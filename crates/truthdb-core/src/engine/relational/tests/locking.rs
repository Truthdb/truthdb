use super::*;

#[test]
fn row_locks_for_point_operations() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("row-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("t");
    let t = table_object_id(&engine, "t");
    let rc = Isolation::ReadCommitted;

    let has_table_x =
        |locks: &[(Resource, LockMode)]| locks.contains(&(Resource::Table(t), LockMode::Exclusive));
    let row_x = |locks: &[(Resource, LockMode)]| -> Option<u64> {
        locks.iter().find_map(|(r, m)| match r {
            Resource::Row(oid, h) if *oid == t && *m == LockMode::Exclusive => Some(*h),
            _ => None,
        })
    };

    // Point UPDATE: Table IX + a single Row X, no Table X.
    let up = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE t SET v = 9 WHERE id = 5",
        rc,
    );
    assert!(up.contains(&(Resource::Table(t), LockMode::IntentExclusive)));
    assert!(
        !has_table_x(&up),
        "point UPDATE must not take Table X: {up:?}"
    );
    let k5 = row_x(&up).expect("point UPDATE row lock");

    // Point DELETE: same row key as the UPDATE of id = 5.
    let del = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DELETE FROM t WHERE id = 5",
        rc,
    );
    assert_eq!(row_x(&del), Some(k5), "DELETE id=5 must lock the same row");

    // A different key → a different row resource (so the two run concurrently).
    let up6 = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE t SET v = 1 WHERE id = 6",
        rc,
    );
    assert_ne!(row_x(&up6), Some(k5));

    // Point INSERT (literal key) row-locks; INSERT ... SELECT does not.
    let ins = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t VALUES (7, 1)",
        rc,
    );
    assert!(row_x(&ins).is_some() && !has_table_x(&ins));
    let ins_sel = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t SELECT id, v FROM t",
        rc,
    );
    assert!(has_table_x(&ins_sel) && row_x(&ins_sel).is_none());

    // Range / OR / partial predicates fall back to Table X.
    for sql in [
        "UPDATE t SET v = 1 WHERE id > 5",
        "DELETE FROM t WHERE id = 5 OR id = 6",
        "UPDATE t SET v = 1",
        "UPDATE t SET id = 2 WHERE id = 5", // key change moves the row
        "DELETE FROM t WHERE id = (SELECT MAX(id) FROM t)",
    ] {
        let locks = engine.analyze_locks(crate::relstore::catalog::DEFAULT_DATABASE_ID, sql, rc);
        assert!(
            has_table_x(&locks) && row_x(&locks).is_none(),
            "table lock for `{sql}`: {locks:?}"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn row_lock_safety_guards() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("row-lock-guards");
    let engine = new_engine(&path);
    // Character PK, a table with a secondary UNIQUE index, and a FLOAT PK.
    engine
        .execute("CREATE TABLE cs (id VARCHAR(10) NOT NULL PRIMARY KEY, v INT)")
        .expect("cs");
    engine
        .execute("CREATE TABLE u (id INT NOT NULL PRIMARY KEY, email VARCHAR(50))")
        .expect("u");
    engine
        .execute("CREATE UNIQUE INDEX ux ON u (email)")
        .expect("ux");
    engine
        .execute("CREATE TABLE f (k FLOAT NOT NULL PRIMARY KEY, v INT)")
        .expect("f");
    let cs = table_object_id(&engine, "cs");
    let u = table_object_id(&engine, "u");
    let f = table_object_id(&engine, "f");
    let rc = Isolation::ReadCommitted;
    let table_x = |locks: &[(Resource, LockMode)], t: u32| {
        locks.contains(&(Resource::Table(t), LockMode::Exclusive))
    };
    let has_row = |locks: &[(Resource, LockMode)], t: u32| {
        locks
            .iter()
            .any(|(r, _)| matches!(r, Resource::Row(o, _) if *o == t))
    };

    // Character PK vs a *string* literal row-locks; vs a *numeric* literal it
    // does not (the executor's string->number match is many-to-one).
    let str_lit = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE cs SET v = 1 WHERE id = '05'",
        rc,
    );
    assert!(has_row(&str_lit, cs));
    let num_lit = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE cs SET v = 1 WHERE id = 5",
        rc,
    );
    assert!(table_x(&num_lit, cs) && !has_row(&num_lit, cs));

    // A table with a secondary UNIQUE index: INSERT/UPDATE keep Table X;
    // DELETE may still row-lock (a delete cannot create a duplicate).
    let ins = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO u VALUES (1, 'a@b.com')",
        rc,
    );
    assert!(table_x(&ins, u) && !has_row(&ins, u));
    let upd = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE u SET email = 'x' WHERE id = 1",
        rc,
    );
    assert!(table_x(&upd, u) && !has_row(&upd, u));
    let del = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DELETE FROM u WHERE id = 1",
        rc,
    );
    assert!(has_row(&del, u) && !table_x(&del, u));

    // FLOAT PK is never row-locked (signed zero / NaN encode ambiguity).
    let fl = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE f SET v = 1 WHERE k = 1.0",
        rc,
    );
    assert!(table_x(&fl, f) && !has_row(&fl, f));

    // A batch that point-writes AND reads the same table must end up with an
    // exclusive table lock (the IX+S -> X combine fix), not a Shared lock.
    let batch = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE cs SET v = 1 WHERE id = '05'; SELECT * FROM cs",
        rc,
    );
    assert!(
        table_x(&batch, cs),
        "point-write + same-table read must hold Table X: {batch:?}"
    );
    assert!(
        !batch.contains(&(Resource::Table(cs), LockMode::Shared)),
        "must not downgrade to Shared: {batch:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn row_locks_require_full_composite_key() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("row-locks-composite");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a, b))")
        .expect("t");
    let t = table_object_id(&engine, "t");
    let rc = Isolation::ReadCommitted;
    let row_x = |locks: &[(Resource, LockMode)]| {
        locks.iter().any(|(r, m)| {
            matches!(r, Resource::Row(oid, _) if *oid == t) && *m == LockMode::Exclusive
        })
    };
    // Both key columns pinned → row lock.
    assert!(row_x(&engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE t SET v = 1 WHERE a = 1 AND b = 2",
        rc
    )));
    // Only one pinned → table lock.
    let partial = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "UPDATE t SET v = 1 WHERE a = 1",
        rc,
    );
    assert!(!row_x(&partial) && partial.contains(&(Resource::Table(t), LockMode::Exclusive)));
    let _ = std::fs::remove_file(path);
}

#[test]
fn foreign_key_insert_locks_parent_shared() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("fk-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY)")
        .expect("p");
    engine
        .execute("CREATE TABLE c (id INT NOT NULL PRIMARY KEY, pid INT REFERENCES p (id))")
        .expect("c");
    let p = table_object_id(&engine, "p");
    let c = table_object_id(&engine, "c");

    // INSERT into the child reads the parent, so it must take a Shared lock
    // on the parent (else it could read an uncommitted parent row). The
    // child is not itself an FK parent, so its point INSERT row-locks:
    // Table IntentExclusive + a Row Exclusive on the inserted key.
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO c VALUES (1, 1)",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(c), LockMode::IntentExclusive)),
        "child IntentExclusive: {locks:?}"
    );
    assert!(
        locks
            .iter()
            .any(|(r, m)| matches!(r, Resource::Row(t, _) if *t == c) && *m == LockMode::Exclusive),
        "child Row Exclusive: {locks:?}"
    );
    assert!(
        locks.contains(&(Resource::Table(p), LockMode::Shared)),
        "parent Shared: {locks:?}"
    );
    // DELETE of the parent reads the child (NO ACTION check) -> child Shared.
    let del = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DELETE FROM p WHERE id = 1",
        Isolation::ReadCommitted,
    );
    assert!(
        del.contains(&(Resource::Table(p), LockMode::Exclusive)),
        "parent Exclusive: {del:?}"
    );
    assert!(
        del.contains(&(Resource::Table(c), LockMode::Shared)),
        "child Shared: {del:?}"
    );
    let _ = std::fs::remove_file(path);
}
