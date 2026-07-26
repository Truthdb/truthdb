use super::*;

#[test]
fn nested_cte_locks_its_base_table() {
    // A CTE whose body itself declares a CTE (`WITH c AS (WITH d AS ...)`)
    // must still Shared-lock the base table the inner CTE reads — directly
    // and through a view — or it dirty-reads under READ COMMITTED.
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("nested-cte-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    let secret = table_object_id(&engine, "secret");

    // Plain query with a nested CTE.
    let direct = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "WITH c AS (WITH d AS (SELECT z FROM secret) SELECT z FROM d) SELECT z FROM c",
        Isolation::ReadCommitted,
    );
    assert!(
        direct.contains(&(Resource::Table(secret), LockMode::Shared)),
        "nested-CTE query must lock secret: {direct:?}"
    );

    // Same through a view.
    engine
            .execute(
                "CREATE VIEW v AS WITH c AS (WITH d AS (SELECT z FROM secret) SELECT z FROM d) SELECT z FROM c",
            )
            .expect("view");
    let via_view = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT z FROM v",
        Isolation::ReadCommitted,
    );
    assert!(
        via_view.contains(&(Resource::Table(secret), LockMode::Shared)),
        "view over a nested-CTE body must lock secret: {via_view:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn selecting_a_view_locks_its_base_tables() {
    // A read through a view must Shared-lock the view's base tables (else a
    // dirty read under READ COMMITTED), including a base table the view body
    // reaches only through its own CTE.
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("view-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE base (x INT NOT NULL PRIMARY KEY)")
        .expect("base");
    engine
        .execute("CREATE VIEW v AS WITH c AS (SELECT x FROM base) SELECT x FROM c")
        .expect("view");
    let base = table_object_id(&engine, "base");

    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT x FROM v",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(base), LockMode::Shared)),
        "a view's base table (via its CTE) must be Shared-locked: {locks:?}"
    );

    // A view over the view must reach the base table through both levels.
    engine
        .execute("CREATE VIEW v2 AS SELECT x FROM v")
        .expect("v2");
    let nested = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT x FROM v2",
        Isolation::ReadCommitted,
    );
    assert!(
        nested.contains(&(Resource::Table(base), LockMode::Shared)),
        "a nested view must Shared-lock the base table through both views: {nested:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn view_with_nested_cte_in_exists_locks_both_tables() {
    // A view whose body has `WHERE EXISTS (WITH d AS (SELECT ... FROM secret)
    // ...)` reads both `base` and `secret`; both must be Shared-locked.
    // EXISTS is the only expression position the parser lets a subquery start
    // with WITH, so it is the nested-CTE-in-expression case for locks.
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("view-exists-cte-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE base (x INT NOT NULL PRIMARY KEY)")
        .expect("base");
    engine
        .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    engine
            .execute("CREATE VIEW v AS SELECT x FROM base WHERE EXISTS (WITH d AS (SELECT z FROM secret) SELECT z FROM d)")
            .expect("view");
    let base = table_object_id(&engine, "base");
    let secret = table_object_id(&engine, "secret");

    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT x FROM v",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(base), LockMode::Shared)),
        "base must be locked: {locks:?}"
    );
    assert!(
        locks.contains(&(Resource::Table(secret), LockMode::Shared)),
        "secret (behind the EXISTS nested CTE) must be locked: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn view_definition_survives_restart() {
    // A view lives in the persisted catalog, so it must be queryable after
    // the engine is reopened.
    let path = unique_temp_path("view-persist");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("storage create");
    let engine = Engine::new(storage).expect("engine create");
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("table");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert");
    engine
        .execute("CREATE VIEW hi AS SELECT id FROM t WHERE v >= 20")
        .expect("view");
    drop(engine);

    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine restart");
    let out = engine.execute("SELECT id FROM hi").expect("query view");
    assert!(out.contains('2'), "view query after restart: {out}");
    let listed = engine
        .execute("SELECT name FROM sys.views")
        .expect("sys.views");
    assert!(listed.contains("hi"), "sys.views after restart: {listed}");
    let _ = std::fs::remove_file(path);
}

#[test]
fn assignment_select_locks_base_table_behind_a_cte_value() {
    // A CTE referenced only inside an assignment SELECT's value subquery
    // must still lock the real base table, or the read could dirty-read a
    // concurrent uncommitted write under READ COMMITTED.
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("assign-cte-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE secret (x INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    let secret = table_object_id(&engine, "secret");

    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DECLARE @v INT; WITH c AS (SELECT x FROM secret) SELECT @v = (SELECT MAX(x) FROM c)",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(secret), LockMode::Shared)),
        "base table behind the CTE-in-value must be Shared-locked: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn order_by_spills_and_matches_in_memory_sort() {
    // A tiny sort budget forces the external merge sort (spill sorted runs
    // to temp extents + k-way merge); its output must be byte-identical to
    // the in-memory sort, ties included.
    let path = unique_temp_path("sort-spill");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, grp INT, tag NVARCHAR(20))")
        .expect("t");
    // 600 rows with many tied `grp` values (exercises stable cross-run ties).
    for i in 0..600 {
        engine
            .execute(&format!(
                "INSERT INTO t VALUES ({i}, {}, 'tag-{}')",
                (i * 7) % 50,
                i % 13
            ))
            .expect("insert");
    }
    let query = "SELECT id, grp, tag FROM t ORDER BY grp, tag, id";

    // Reference: default (in-memory) budget.
    let (_, reference) = sql_rows(&engine, query);

    // Forced spill: a 300-byte budget makes almost every row its own run.
    crate::engine::set_test_sort_budget(Some(300));
    let (_, spilled) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);

    assert_eq!(reference.len(), 600);
    assert_eq!(
        spilled, reference,
        "spilled sort must match the in-memory sort"
    );
    // Sanity: the result really is ordered by (grp, tag, id).
    let key = |r: &Vec<Option<String>>| {
        (
            r[1].clone().unwrap().parse::<i64>().unwrap(),
            r[2].clone().unwrap(),
            r[0].clone().unwrap().parse::<i64>().unwrap(),
        )
    };
    assert!(spilled.windows(2).all(|w| key(&w[0]) <= key(&w[1])));
    let _ = std::fs::remove_file(path);
}

#[test]
fn inner_join_grace_hash_spills_and_matches_in_memory() {
    // A tiny budget forces the grace-hash INNER join (partition both sides by
    // key hash to temp extents, join per partition). Results must match the
    // in-memory hash join — many-to-many keys, NULL keys (never match), and a
    // residual ON predicate.
    let path = unique_temp_path("join-spill");
    let engine = new_engine(&path);
    engine.execute("CREATE TABLE l (k INT, v INT)").expect("l");
    engine.execute("CREATE TABLE r (k INT, w INT)").expect("r");
    for i in 0..300 {
        let lk = if i % 41 == 0 {
            "NULL".into()
        } else {
            (i % 25).to_string()
        };
        engine
            .execute(&format!("INSERT INTO l VALUES ({lk}, {i})"))
            .expect("l ins");
        let rk = if i % 43 == 0 {
            "NULL".into()
        } else {
            (i % 25).to_string()
        };
        engine
            .execute(&format!("INSERT INTO r VALUES ({rk}, {i})"))
            .expect("r ins");
    }
    let query = "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k AND r.w > 100 ORDER BY l.v, r.w";

    let (_, reference) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(Some(500));
    let (_, spilled) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);

    assert!(!reference.is_empty());
    assert_eq!(
        spilled, reference,
        "grace-hash INNER join must match in-memory"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn outer_joins_grace_hash_spill_and_match_in_memory() {
    // A tiny budget forces the grace-hash join for LEFT/RIGHT/FULL. Each must
    // match its in-memory result, including probe-side unmatched rows,
    // build-side unmatched rows (FULL), and NULL-keyed rows on both sides
    // (never match; the outer side's are null-extended, the inner's dropped).
    let path = unique_temp_path("outer-join-spill");
    let engine = new_engine(&path);
    engine.execute("CREATE TABLE l (k INT, v INT)").expect("l");
    engine.execute("CREATE TABLE r (k INT, w INT)").expect("r");
    for i in 0..300 {
        // Disjoint-ish key ranges so both sides have unmatched rows.
        let lk = if i % 41 == 0 {
            "NULL".into()
        } else {
            (i % 30).to_string()
        };
        engine
            .execute(&format!("INSERT INTO l VALUES ({lk}, {i})"))
            .expect("l ins");
        let rk = if i % 43 == 0 {
            "NULL".into()
        } else {
            (i % 25 + 10).to_string()
        };
        engine
            .execute(&format!("INSERT INTO r VALUES ({rk}, {i})"))
            .expect("r ins");
    }
    for kind in ["LEFT", "RIGHT", "FULL"] {
        let query = format!("SELECT l.v, r.w FROM l {kind} JOIN r ON l.k = r.k ORDER BY l.v, r.w");
        let (_, reference) = sql_rows(&engine, &query);
        crate::engine::set_test_sort_budget(Some(500));
        let (_, spilled) = sql_rows(&engine, &query);
        crate::engine::set_test_sort_budget(None);
        assert!(!reference.is_empty(), "{kind}: reference empty");
        assert_eq!(
            spilled, reference,
            "grace-hash {kind} join must match in-memory"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn distinct_grace_hash_spills_and_matches_in_memory() {
    // A tiny budget forces grace-hash DISTINCT (partition rows by key hash to
    // temp extents, dedup each partition). Results must match the in-memory
    // hash DISTINCT — many duplicates, NULLs, and a multi-column key.
    let path = unique_temp_path("distinct-spill");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, b INT)")
        .expect("t");
    for i in 0..900 {
        let a = if i % 53 == 0 {
            "NULL".to_string()
        } else {
            (i % 20).to_string()
        };
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {a}, {})", i % 15))
            .expect("insert");
    }
    let query = "SELECT DISTINCT a, b FROM t ORDER BY a, b";

    let (_, reference) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(Some(400));
    let (_, spilled) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);

    assert!(!reference.is_empty());
    assert_eq!(
        spilled, reference,
        "grace-hash DISTINCT must match in-memory"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn group_by_grace_hash_spills_and_matches_in_memory() {
    // A tiny budget forces grace-hash aggregation (partition rows by
    // group-key hash to temp extents, aggregate each partition). Results
    // must match the in-memory hash aggregate — group keys, SUM, COUNT, and
    // COUNT(DISTINCT), including a NULL group and HAVING.
    let path = unique_temp_path("agg-spill");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, grp INT, amt INT)")
        .expect("t");
    for i in 0..800 {
        let grp = if i % 37 == 0 {
            "NULL".to_string()
        } else {
            (i % 60).to_string()
        };
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {grp}, {})", i % 10))
            .expect("insert");
    }
    let query = "SELECT grp, SUM(amt), COUNT(*), COUNT(DISTINCT amt) FROM t GROUP BY grp HAVING COUNT(*) > 2 ORDER BY grp";

    let (_, reference) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(Some(400));
    let (_, spilled) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);

    assert!(!reference.is_empty());
    assert_eq!(
        spilled, reference,
        "grace-hash aggregate must match in-memory"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn order_by_spills_wide_join_rows() {
    // A join's source row is the concatenation of both tables' columns. Each
    // per-table row fits (< the ~2020 B clustered cell cap), but the joined
    // source row (two ~1950 B strings) exceeds the 3900 B in-row table cap —
    // sorting it (pre-projection) must still spill: the spill codec is
    // cap-free, whereas reusing the table codec would error 1701.
    let path = unique_temp_path("sort-spill-wide");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE a (k INT NOT NULL PRIMARY KEY, s VARCHAR(2000))")
        .expect("a");
    engine
        .execute("CREATE TABLE b (k INT NOT NULL PRIMARY KEY, s VARCHAR(2000))")
        .expect("b");
    for i in 0..40 {
        engine
            .execute(&format!(
                "INSERT INTO a VALUES ({i}, '{}')",
                "x".repeat(1950)
            ))
            .expect("a ins");
        engine
            .execute(&format!(
                "INSERT INTO b VALUES ({i}, '{}')",
                "y".repeat(1950)
            ))
            .expect("b ins");
    }
    let query = "SELECT a.k FROM a JOIN b ON a.k = b.k ORDER BY a.k DESC";
    let (_, reference) = sql_rows(&engine, query);
    assert_eq!(
        reference.len(),
        40,
        "join+sort should return 40 rows in memory"
    );
    // Each joined source row is ~3.9 KB (> 3900) — forced to spill.
    crate::engine::set_test_sort_budget(Some(300));
    let (_, rows) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);
    assert_eq!(
        rows, reference,
        "spilled wide-join sort must match in-memory"
    );
    assert_eq!(rows[0][0].as_deref(), Some("39"));
    assert_eq!(rows[39][0].as_deref(), Some("0"));
    let _ = std::fs::remove_file(path);
}
