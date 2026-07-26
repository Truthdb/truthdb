use super::*;

#[test]
fn sql_create_insert_select_survive_restart() {
    let path = unique_temp_path("sql-roundtrip");
    let engine = new_engine(&path);

    engine
        .execute(
            "CREATE TABLE products (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50), price FLOAT)",
        )
        .expect("create");
    engine
            .execute("INSERT INTO products VALUES (1, 'Skor', 79.99), (2, 'Kangor', 129.5), (3, 'Sockar', NULL)")
            .expect("insert");

    let (columns, rows) = sql_rows(&engine, "SELECT id, name FROM products ORDER BY id");
    assert_eq!(columns, vec!["id", "name"]);
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("Skor".into())],
            vec![Some("2".into()), Some("Kangor".into())],
            vec![Some("3".into()), Some("Sockar".into())],
        ]
    );
    drop(engine);

    // Restart: schema + rows recovered.
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    let (_, rows) = sql_rows(&engine, "SELECT name FROM products WHERE price IS NULL");
    assert_eq!(rows, vec![vec![Some("Sockar".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_update_and_delete_with_where() {
    let path = unique_temp_path("sql-update-delete");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, n INT, label NVARCHAR(20))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
        .expect("insert");

    // UPDATE a non-key column; SET expression sees the pre-update row.
    engine
        .execute("UPDATE t SET n = n + 5, label = 'x' WHERE id = 2")
        .expect("update");
    let (_, rows) = sql_rows(&engine, "SELECT n, label FROM t WHERE id = 2");
    assert_eq!(rows, vec![vec![Some("25".into()), Some("x".into())]]);

    // DELETE a subset.
    engine
        .execute("DELETE FROM t WHERE n < 20")
        .expect("delete");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_update_primary_key_rekeys() {
    let path = unique_temp_path("sql-update-pk");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .expect("insert");
    // Move row 1 to key 5 (delete + insert under the hood).
    engine
        .execute("UPDATE t SET id = 5 WHERE id = 1")
        .expect("update");
    let (_, rows) = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Some("2".into()), Some("200".into())],
            vec![Some("5".into()), Some("100".into())],
        ]
    );
    // Re-keying onto an existing key collides (2627).
    assert_eq!(
        sql_error_number(&engine, "UPDATE t SET id = 2 WHERE id = 5"),
        2627
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_delete_all_and_update_null_violation() {
    let path = unique_temp_path("sql-del-all");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, n INT NOT NULL)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert");
    // Updating a NOT NULL column to NULL is 515.
    assert_eq!(
        sql_error_number(&engine, "UPDATE t SET n = NULL WHERE id = 1"),
        515
    );
    // DELETE with no WHERE clears the table.
    engine.execute("DELETE FROM t").expect("delete all");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t");
    assert!(rows.is_empty());
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_default_values_applied() {
    let path = unique_temp_path("sql-default");
    let engine = new_engine(&path);
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                 n INT NOT NULL DEFAULT 7, label NVARCHAR(10) DEFAULT 'none')",
        )
        .expect("create");
    // Omit the defaulted columns.
    engine
        .execute("INSERT INTO t (id) VALUES (1)")
        .expect("insert");
    // An explicit NULL into a nullable column is kept (not defaulted).
    engine
        .execute("INSERT INTO t (id, label) VALUES (2, NULL)")
        .expect("insert2");
    let (_, rows) = sql_rows(&engine, "SELECT id, n, label FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("7".into()), Some("none".into())],
            vec![Some("2".into()), Some("7".into()), None],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_identity_assigns_and_survives_restart() {
    let path = unique_temp_path("sql-identity");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY IDENTITY(1,1), name NVARCHAR(10))")
        .expect("create");
    engine
        .execute("INSERT INTO t (name) VALUES ('a')")
        .expect("i1");
    engine
        .execute("INSERT INTO t (name) VALUES ('b'), ('c')")
        .expect("i2");
    // Deleting the max row must not let its identity be reused.
    engine.execute("DELETE FROM t WHERE id = 3").expect("del");
    engine
        .execute("INSERT INTO t (name) VALUES ('d')")
        .expect("i3");
    let (_, rows) = sql_rows(&engine, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("a".into())],
            vec![Some("2".into()), Some("b".into())],
            vec![Some("4".into()), Some("d".into())],
        ]
    );
    // Providing an explicit value for an identity column is rejected.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t (id, name) VALUES (9, 'z')"),
        8101
    );
    // Identity cannot be updated.
    assert_eq!(
        sql_error_number(&engine, "UPDATE t SET id = 100 WHERE id = 1"),
        8102
    );
    drop(engine);

    // Restart: the counter continues from 5, never reusing 3.
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    engine
        .execute("INSERT INTO t (name) VALUES ('e')")
        .expect("i4");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'e'");
    assert_eq!(rows, vec![vec![Some("5".into())]]);
    let _ = std::fs::remove_file(path);
}

/// Runs SQL expected to error and returns the SQL error message.
fn sql_error_message(engine: &Engine, text: &str) -> String {
    let env = sql(engine, text);
    env["error"]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an error envelope, got {env}"))
        .to_string()
}

#[test]
fn sql_check_constraints_enforced_on_insert_and_update() {
    let path = unique_temp_path("sql-check");
    let engine = new_engine(&path);
    engine
        .execute(
            "CREATE TABLE items (\
                   id INT NOT NULL PRIMARY KEY, \
                   qty INT CHECK (qty >= 0), \
                   price INT, \
                   CONSTRAINT ck_price CHECK ((price - qty) > 0))",
        )
        .expect("create");

    // A row satisfying both checks inserts.
    engine
        .execute("INSERT INTO items VALUES (1, 5, 10)")
        .expect("insert ok");

    // Column check violation (qty < 0) → 547.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO items VALUES (2, -1, 10)"),
        547
    );
    // Named table check violation (price <= qty) → 547, name in message.
    let msg = sql_error_message(&engine, "INSERT INTO items VALUES (3, 5, 5)");
    assert!(
        msg.contains("ck_price"),
        "message should name the constraint: {msg}"
    );

    // A NULL in a checked column yields UNKNOWN, which passes.
    engine
        .execute("INSERT INTO items VALUES (4, NULL, 10)")
        .expect("null qty passes check");

    // UPDATE is checked against the new row.
    assert_eq!(
        sql_error_number(&engine, "UPDATE items SET qty = -3 WHERE id = 1"),
        547
    );
    engine
        .execute("UPDATE items SET qty = 2 WHERE id = 1")
        .expect("update ok");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM items ORDER BY id");
    assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("4".into())],]);

    // The constraint survives a restart and still fires.
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO items VALUES (5, -9, 10)"),
        547
    );
    // sys.check_constraints lists both (the auto-named column check and the
    // explicitly named table check).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT name FROM sys.check_constraints ORDER BY name",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("CK__items__1".into())],
            vec![Some("ck_price".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_check_constraint_rejects_unknown_column_and_duplicate_name() {
    let path = unique_temp_path("sql-check-invalid");
    let engine = new_engine(&path);
    // A CHECK referencing a non-existent column is rejected at CREATE (207).
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, CHECK (missing > 0))",
        ),
        207
    );
    // Two constraints with the same explicit name collide (2714).
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                   CONSTRAINT c CHECK (id > 0), CONSTRAINT c CHECK (id < 100))",
        ),
        2714
    );
    // A multi-part (qualified) identifier in a CHECK is rejected at CREATE
    // (4104) rather than producing a table that rejects every INSERT.
    assert_eq!(
        sql_error_number(&engine, "CREATE TABLE t (col INT, CHECK (t.col > 0))",),
        4104
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_insert_select_copies_rows() {
    let path = unique_temp_path("sql-insert-select");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE src (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20), keep BIT)")
        .expect("create src");
    engine
        .execute("INSERT INTO src VALUES (1, 'a', 1), (2, 'b', 0), (3, 'c', 1)")
        .expect("seed src");
    // Target has an IDENTITY and a DEFAULT; the SELECT feeds the two named
    // columns and the rest are server-generated / defaulted.
    engine
        .execute(
            "CREATE TABLE dst (rid INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
                   id INT, label NVARCHAR(20), note NVARCHAR(10) DEFAULT 'copied')",
        )
        .expect("create dst");
    engine
        .execute("INSERT INTO dst (id, label) SELECT id, name FROM src WHERE keep = 1 ORDER BY id")
        .expect("insert select");
    let (_, rows) = sql_rows(&engine, "SELECT rid, id, label, note FROM dst ORDER BY rid");
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".into()),
                Some("1".into()),
                Some("a".into()),
                Some("copied".into())
            ],
            vec![
                Some("2".into()),
                Some("3".into()),
                Some("c".into()),
                Some("copied".into())
            ],
        ]
    );

    // Column-count mismatch between SELECT list and insert list.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO dst (id) SELECT id, name FROM src"),
        121
    );
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO dst (id, label) SELECT id FROM src"),
        120
    );

    // Self-insert is Halloween-safe: the SELECT is fully materialized
    // before any row is inserted, so it doubles the table exactly once.
    engine
        .execute("INSERT INTO dst (id, label) SELECT id, label FROM dst")
        .expect("self insert select");
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM dst");
    assert_eq!(rows, vec![vec![Some("4".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn insert_select_locks_source_table_shared() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("insert-select-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)")
        .expect("t1");
    engine
        .execute("CREATE TABLE t2 (v INT NOT NULL)")
        .expect("t2");
    let t1 = table_object_id(&engine, "t1");
    let t2 = table_object_id(&engine, "t2");

    // The SELECT's source table must be read-locked (Shared) and the target
    // write-locked (Exclusive); without the Shared lock this INSERT could
    // read another transaction's uncommitted rows.
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t2 (v) SELECT v FROM t1",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(t1), LockMode::Shared)),
        "source t1 must be Shared: {locks:?}"
    );
    assert!(
        locks.contains(&(Resource::Table(t2), LockMode::Exclusive)),
        "target t2 must be Exclusive: {locks:?}"
    );

    // A self-insert combines the read and write into a single Exclusive lock.
    let self_locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t1 (id, v) SELECT id, v FROM t1",
        Isolation::ReadCommitted,
    );
    let t1_locks: Vec<_> = self_locks
        .iter()
        .filter(|(r, _)| *r == Resource::Table(t1))
        .collect();
    assert_eq!(
        t1_locks,
        vec![&(Resource::Table(t1), LockMode::Exclusive)],
        "self-insert takes a single Exclusive lock on t1"
    );

    // READ UNCOMMITTED takes no read lock on the source.
    let ru = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t2 (v) SELECT v FROM t1",
        Isolation::ReadUncommitted,
    );
    assert!(
        !ru.iter()
            .any(|(r, m)| *r == Resource::Table(t1) && *m == LockMode::Shared),
        "READ UNCOMMITTED takes no shared lock: {ru:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_alter_table_add_drop_check() {
    let path = unique_temp_path("sql-alter-check");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, qty INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 10)")
        .expect("seed");

    // ADD CONSTRAINT validates existing rows: a constraint every row
    // satisfies is accepted and then enforced on new writes.
    engine
        .execute("ALTER TABLE t ADD CONSTRAINT ck_qty CHECK (qty >= 0)")
        .expect("add check");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t VALUES (3, -1)"),
        547
    );

    // ADD CONSTRAINT that an existing row violates is rejected (547) and
    // not persisted (a later insert violating it still succeeds after DROP).
    assert_eq!(
        sql_error_number(
            &engine,
            "ALTER TABLE t ADD CONSTRAINT ck_big CHECK (qty > 8)"
        ),
        547
    );
    // ck_big was not added, so it is not enforced.
    engine
        .execute("INSERT INTO t VALUES (4, 1)")
        .expect("insert allowed (ck_big not added)");

    // DROP CONSTRAINT removes enforcement.
    engine
        .execute("ALTER TABLE t DROP CONSTRAINT ck_qty")
        .expect("drop check");
    engine
        .execute("INSERT INTO t VALUES (5, -7)")
        .expect("insert allowed after drop");

    // Dropping an unknown constraint errors.
    assert_eq!(
        sql_error_number(&engine, "ALTER TABLE t DROP CONSTRAINT nope"),
        3728
    );
    // ALTER TABLE is DDL and is not allowed inside an explicit transaction
    // (needs a persistent txn context, so run it as one batch).
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRANSACTION; ALTER TABLE t ADD CHECK (qty < 100)",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(226));
    batch(&engine, &mut ctx, "ROLLBACK");

    // A constraint added via ALTER survives a restart.
    engine
        .execute("ALTER TABLE t ADD CONSTRAINT ck_id CHECK (id > 0)")
        .expect("add ck_id");
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT name FROM sys.check_constraints ORDER BY name",
    );
    assert_eq!(rows, vec![vec![Some("ck_id".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_foreign_key_child_and_parent_enforcement() {
    let path = unique_temp_path("sql-fk");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE parent (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
        .expect("parent");
    engine
        .execute("CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT REFERENCES parent (id))")
        .expect("child");
    engine
        .execute("INSERT INTO parent VALUES (1, 'a'), (2, 'b')")
        .expect("seed parent");

    // Child side: a referenced parent must exist; NULL skips enforcement.
    engine
        .execute("INSERT INTO child VALUES (10, 1)")
        .expect("child -> parent 1");
    engine
        .execute("INSERT INTO child VALUES (11, NULL)")
        .expect("NULL fk allowed");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO child VALUES (12, 99)"),
        547
    );

    // Parent side (DELETE, NO ACTION): a referenced parent cannot be deleted.
    assert_eq!(
        sql_error_number(&engine, "DELETE FROM parent WHERE id = 1"),
        547
    );
    engine
        .execute("DELETE FROM parent WHERE id = 2")
        .expect("unreferenced parent deletes");

    // Parent side (UPDATE of the PK): cannot vacate a referenced key; a
    // non-key update is fine.
    assert_eq!(
        sql_error_number(&engine, "UPDATE parent SET id = 5 WHERE id = 1"),
        547
    );
    engine
        .execute("UPDATE parent SET name = 'z' WHERE id = 1")
        .expect("non-key parent update");

    // Child UPDATE re-checks the new value.
    assert_eq!(
        sql_error_number(&engine, "UPDATE child SET pid = 42 WHERE id = 10"),
        547
    );
    engine
        .execute("UPDATE child SET pid = NULL WHERE id = 10")
        .expect("child update to NULL");
    // With no child referencing parent 1, it can now be deleted.
    engine
        .execute("DELETE FROM parent WHERE id = 1")
        .expect("now-unreferenced parent deletes");

    // The constraint is enforced again after a restart.
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO child VALUES (20, 7)"),
        547
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_foreign_key_self_reference() {
    let path = unique_temp_path("sql-fk-self");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE emp (id INT NOT NULL PRIMARY KEY, mgr INT REFERENCES emp (id))")
        .expect("emp");
    // A root has a NULL manager; a subordinate references an existing row.
    engine
        .execute("INSERT INTO emp VALUES (1, NULL)")
        .expect("root");
    engine
        .execute("INSERT INTO emp VALUES (2, 1)")
        .expect("sub");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO emp VALUES (3, 99)"),
        547
    );
    // A batch may reference a sibling row inserted in the same statement
    // (row 4 references 5, which is created alongside it).
    engine
        .execute("INSERT INTO emp VALUES (4, 5), (5, 1)")
        .expect("self-ref batch");
    // A referenced row cannot be deleted while a subordinate remains.
    assert_eq!(
        sql_error_number(&engine, "DELETE FROM emp WHERE id = 1"),
        547
    );

    // A primary-key change that would orphan a self-reference is rejected
    // (row 4 references row 5, so changing id 5 dangles mgr=5). This must be
    // validated against the post-update state, not the stale pre-update row.
    assert_eq!(
        sql_error_number(&engine, "UPDATE emp SET id = 50 WHERE id = 5"),
        547
    );
    // A primary-key change with no dependents is allowed (nothing points at
    // row 2, and its own mgr=1 still exists).
    engine
        .execute("UPDATE emp SET id = 6 WHERE id = 2")
        .expect("unreferenced self-ref pk change");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_constraint_name_unique_across_kinds() {
    let path = unique_temp_path("sql-constraint-names");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY)")
        .expect("p");
    // A CHECK and a FOREIGN KEY cannot share a name within one CREATE.
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TABLE c (x INT, CONSTRAINT dup CHECK (x > 0), \
                   CONSTRAINT dup FOREIGN KEY (x) REFERENCES p (id))",
        ),
        2714
    );
    // Nor across ALTER, in either order.
    engine.execute("CREATE TABLE c (x INT)").expect("c");
    engine
        .execute("ALTER TABLE c ADD CONSTRAINT dup CHECK (x > 0)")
        .expect("add check");
    assert_eq!(
        sql_error_number(
            &engine,
            "ALTER TABLE c ADD CONSTRAINT dup FOREIGN KEY (x) REFERENCES p (id)",
        ),
        2714
    );
    engine
        .execute("ALTER TABLE c ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES p (id)")
        .expect("add fk");
    assert_eq!(
        sql_error_number(&engine, "ALTER TABLE c ADD CONSTRAINT fk1 CHECK (x < 100)"),
        2714
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_foreign_key_alter_drop_and_catalog() {
    let path = unique_temp_path("sql-fk-alter");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY)")
        .expect("p");
    engine
        .execute("CREATE TABLE c (id INT NOT NULL PRIMARY KEY, pid INT)")
        .expect("c");
    engine.execute("INSERT INTO p VALUES (1)").expect("seed p");
    // Row 11 references a missing parent (no FK yet, so it is allowed).
    engine
        .execute("INSERT INTO c VALUES (10, 1), (11, 99)")
        .expect("seed c");

    // ADD FOREIGN KEY validates existing rows: row 11 orphans -> 547.
    assert_eq!(
        sql_error_number(
            &engine,
            "ALTER TABLE c ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES p (id)",
        ),
        547
    );
    // Fix the orphan, then the constraint is added and enforced.
    engine
        .execute("UPDATE c SET pid = 1 WHERE id = 11")
        .expect("fix orphan");
    engine
        .execute("ALTER TABLE c ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES p (id)")
        .expect("add fk");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO c VALUES (12, 77)"),
        547
    );

    // sys.foreign_keys lists it, referencing p.
    let p_oid = table_object_id(&engine, "p");
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT name, referenced_object_id FROM sys.foreign_keys",
    );
    assert_eq!(cols, vec!["name", "referenced_object_id"]);
    assert_eq!(rows, vec![vec![Some("fk".into()), Some(p_oid.to_string())]]);

    // A referenced parent cannot be dropped.
    assert_eq!(sql_error_number(&engine, "DROP TABLE p"), 3726);

    // DROP CONSTRAINT removes enforcement; the FK survives a restart until
    // then. Confirm durability first.
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO c VALUES (13, 55)"),
        547
    );
    engine
        .execute("ALTER TABLE c DROP CONSTRAINT fk")
        .expect("drop fk");
    engine
        .execute("INSERT INTO c VALUES (14, 55)")
        .expect("insert allowed after drop");
    // Now p can be dropped.
    engine.execute("DROP TABLE p").expect("drop unref parent");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_foreign_key_validation_errors() {
    let path = unique_temp_path("sql-fk-invalid");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY, other INT)")
        .expect("p");
    engine
        .execute("CREATE TABLE bignum (id BIGINT NOT NULL PRIMARY KEY)")
        .expect("bignum");
    // Referencing a non-primary-key column of the parent.
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TABLE r1 (id INT NOT NULL PRIMARY KEY, pid INT REFERENCES p (other))",
        ),
        1776
    );
    // Type mismatch between child (INT) and parent PK (BIGINT).
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TABLE r2 (id INT NOT NULL PRIMARY KEY, bid INT REFERENCES bignum (id))",
        ),
        1778
    );
    // Referencing a table that does not exist.
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TABLE r3 (id INT NOT NULL PRIMARY KEY, x INT REFERENCES nope (id))",
        ),
        208
    );
    let _ = std::fs::remove_file(path);
}
