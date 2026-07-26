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

#[test]
fn sql_batch_variables() {
    let path = unique_temp_path("sql-vars");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();

    // DECLARE, SET, and read a variable within one batch.
    let out = batch(&engine, &mut ctx, "DECLARE @n INT; SET @n = 42; SELECT @n");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![42]);

    // An initializer may reference an earlier variable in the same DECLARE.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @a INT = 5, @b INT = @a + 1; SELECT @b",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![6]);

    // A variable used in a WHERE clause.
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
    );
    batch(
        &engine,
        &mut ctx,
        "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @min INT; SET @min = 20; SELECT id FROM t WHERE v >= @min ORDER BY id",
    );
    assert_eq!(ids(&out), vec![2, 3]);

    // Using an undeclared variable is error 137 (SET and read).
    assert_eq!(
        batch(&engine, &mut ctx, "SET @nope = 1")
            .error
            .as_ref()
            .map(|e| e.number),
        Some(137)
    );
    assert_eq!(
        batch(&engine, &mut ctx, "SELECT @nope")
            .error
            .as_ref()
            .map(|e| e.number),
        Some(137)
    );

    // Redeclaring within the same batch is error 134.
    assert_eq!(
        batch(&engine, &mut ctx, "DECLARE @d INT; DECLARE @d INT")
            .error
            .as_ref()
            .map(|e| e.number),
        Some(134)
    );

    // Variables are batch-scoped: one declared in a prior batch is gone.
    batch(&engine, &mut ctx, "DECLARE @scoped INT");
    assert_eq!(
        batch(&engine, &mut ctx, "SELECT @scoped")
            .error
            .as_ref()
            .map(|e| e.number),
        Some(137)
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_scalar_in_exists_subqueries() {
    let path = unique_temp_path("sql-subquery");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("nums");
    engine
        .execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
        .expect("seed");
    engine
        .execute("CREATE TABLE picks (id INT NOT NULL PRIMARY KEY, target INT)")
        .expect("picks");
    engine
        .execute("INSERT INTO picks VALUES (1, 2), (2, 3)")
        .expect("seed2");

    // Scalar subquery in WHERE.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE v = (SELECT MAX(v) FROM nums)",
    );
    assert_eq!(rows, vec![vec![Some("3".into())]]);

    // Scalar subquery in the SELECT list (evaluated once).
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT id, (SELECT COUNT(*) FROM picks) AS pc FROM nums ORDER BY id",
    );
    assert_eq!(cols, vec!["id", "pc"]);
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("2".into())],
            vec![Some("2".into()), Some("2".into())],
            vec![Some("3".into()), Some("2".into())],
        ]
    );

    // IN (SELECT) and NOT IN (SELECT).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE id IN (SELECT target FROM picks) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE id NOT IN (SELECT target FROM picks)",
    );
    assert_eq!(rows, vec![vec![Some("1".into())]]);

    // EXISTS / NOT EXISTS (uncorrelated).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE EXISTS (SELECT 1 FROM picks WHERE target = 3) ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
        ]
    );
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE NOT EXISTS (SELECT 1 FROM picks WHERE target = 99)",
    );
    assert_eq!(rows.len(), 3);

    // A scalar subquery with no rows is NULL (so the `=` is unknown).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE v = (SELECT v FROM nums WHERE id = 99)",
    );
    assert!(rows.is_empty());

    // More than one row from a scalar subquery is 512; more than one column
    // is 116.
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT id FROM nums WHERE v = (SELECT v FROM nums)"
        ),
        512
    );
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT id FROM nums WHERE v = (SELECT id, v FROM nums WHERE id = 1)",
        ),
        116
    );
    // Correlated subqueries: the inner query references an outer column and
    // is re-run per outer row (Stage 11).
    // EXISTS: nums with a pick whose target equals the num id -> 2, 3.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE EXISTS (SELECT 1 FROM picks WHERE picks.target = nums.id) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
    // NOT EXISTS is the complement.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE NOT EXISTS (SELECT 1 FROM picks WHERE picks.target = nums.id) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    // Correlated scalar subquery: the pick sharing the num's id has target 2.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE (SELECT target FROM picks WHERE picks.id = nums.id) = 2",
    );
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    // Correlated IN: num id is among the targets of picks sharing that id.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE id IN (SELECT target FROM picks WHERE picks.id = nums.id - 1) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);

    // `NOT IN (empty subquery)` is TRUE for every row, including a NULL
    // outer value — the comparison set is empty, so nothing is unknown.
    engine
        .execute("INSERT INTO nums VALUES (4, NULL)")
        .expect("null row");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE v NOT IN (SELECT target FROM picks WHERE target > 1000) ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
            vec![Some("4".into())],
        ]
    );
    // `IN (empty subquery)` is FALSE for every row (no rows returned).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM nums WHERE v IN (SELECT target FROM picks WHERE target > 1000)",
    );
    assert!(rows.is_empty());
    let _ = std::fs::remove_file(path);
}

#[test]
fn subquery_locks_referenced_tables_shared() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("subquery-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE a (id INT NOT NULL PRIMARY KEY)")
        .expect("a");
    engine
        .execute("CREATE TABLE b (id INT NOT NULL PRIMARY KEY)")
        .expect("b");
    let a = table_object_id(&engine, "a");
    let b = table_object_id(&engine, "b");
    // A subquery over `b` inside `a`'s WHERE reads `b`, so it must take a
    // Shared lock on `b` (else it could read `b`'s uncommitted rows).
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT id FROM a WHERE id IN (SELECT id FROM b)",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(a), LockMode::Shared)),
        "a Shared: {locks:?}"
    );
    assert!(
        locks.contains(&(Resource::Table(b), LockMode::Shared)),
        "b Shared (subquery): {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_common_table_expressions() {
    let path = unique_temp_path("sql-cte");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, dept NVARCHAR(4), amount INT)")
        .expect("create");
    engine
        .execute("INSERT INTO sales VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',50)")
        .expect("seed");

    // A basic CTE referenced in FROM.
    let (cols, rows) = sql_rows(
        &engine,
        "WITH big AS (SELECT id, amount FROM sales WHERE amount >= 20) SELECT id FROM big ORDER BY id",
    );
    assert_eq!(cols, vec!["id"]);
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("4".into())]]);

    // A CTE that aggregates, filtered by the outer query.
    let (_, rows) = sql_rows(
        &engine,
        "WITH s AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) \
               SELECT dept FROM s WHERE total > 30 ORDER BY dept",
    );
    assert_eq!(rows, vec![vec![Some("b".into())]]);

    // A later CTE references an earlier one.
    let (_, rows) = sql_rows(
        &engine,
        "WITH a AS (SELECT id, amount FROM sales WHERE amount >= 10), \
                  b AS (SELECT id FROM a WHERE amount >= 20) \
               SELECT id FROM b ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("4".into())]]);

    // A CTE joined to a base table.
    let (_, rows) = sql_rows(
        &engine,
        "WITH s AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) \
               SELECT t.id, s.total FROM sales t JOIN s ON t.dept = s.dept WHERE t.id = 3",
    );
    assert_eq!(rows, vec![vec![Some("3".into()), Some("55".into())]]);

    // The optional column-rename list is not supported yet.
    assert_eq!(
        sql_error_number(
            &engine,
            "WITH c(x) AS (SELECT id FROM sales) SELECT x FROM c",
        ),
        102
    );
    // A recursive / self-reference resolves as a (non-existent) base table.
    assert_eq!(
        sql_error_number(&engine, "WITH r AS (SELECT id FROM r) SELECT id FROM r"),
        208
    );

    // A CTE is visible to a subquery in the WHERE clause, not just the FROM.
    let (_, rows) = sql_rows(
        &engine,
        "WITH s AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) \
               SELECT id FROM sales WHERE dept IN (SELECT dept FROM s WHERE total > 30) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("3".into())], vec![Some("4".into())]]);

    // Duplicate CTE names are rejected.
    assert_eq!(
        sql_error_number(
            &engine,
            "WITH a AS (SELECT 1 AS x), a AS (SELECT 2 AS x) SELECT x FROM a",
        ),
        460
    );
    // A schema-qualified reference does not match a CTE (dbo.s is a base
    // table name, which here does not exist).
    assert_eq!(
        sql_error_number(&engine, "WITH s AS (SELECT 1 AS v) SELECT v FROM dbo.s"),
        208
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_derived_tables() {
    let path = unique_temp_path("sql-derived");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, dept NVARCHAR(4), amount INT)")
        .expect("create");
    engine
        .execute("INSERT INTO sales VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',50)")
        .expect("seed");

    // A derived table filtered further by the outer query; columns resolve
    // by the derived alias.
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT s.id, s.amount FROM (SELECT id, amount FROM sales WHERE amount >= 10) s \
               WHERE s.id < 3 ORDER BY s.id",
    );
    assert_eq!(cols, vec!["id", "amount"]);
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into()), Some("10".into())],
            vec![Some("2".into()), Some("20".into())],
        ]
    );

    // A derived table may aggregate; the outer query filters on the alias.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT d.dept, d.total FROM (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) d \
               WHERE d.total > 30 ORDER BY d.dept",
    );
    assert_eq!(rows, vec![vec![Some("b".into()), Some("55".into())]]);

    // A derived table joined to a base table.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT t.dept, d.total FROM sales t \
               JOIN (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) d ON t.dept = d.dept \
               WHERE t.id = 1",
    );
    assert_eq!(rows, vec![vec![Some("a".into()), Some("30".into())]]);

    // A derived table must have an alias.
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM (SELECT id FROM sales)"),
        102
    );
    // Every derived column must be named.
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM (SELECT amount + 1 FROM sales) x"),
        8155
    );
    // Duplicate derived column names are rejected.
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT * FROM (SELECT id, amount AS id FROM sales) x",
        ),
        8156
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_sys_default_constraints() {
    let path = unique_temp_path("sql-default-constraints");
    let engine = new_engine(&path);
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                   qty INT DEFAULT 0, note NVARCHAR(10) DEFAULT 'n/a', plain INT)",
        )
        .expect("create");
    // One row per column that carries a DEFAULT (plain has none).
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT name, parent_column_id, definition FROM sys.default_constraints ORDER BY parent_column_id",
    );
    assert_eq!(cols, vec!["name", "parent_column_id", "definition"]);
    assert_eq!(
        rows,
        vec![
            vec![
                Some("DF__t__qty".into()),
                Some("2".into()),
                Some("(0)".into())
            ],
            vec![
                Some("DF__t__note".into()),
                Some("3".into()),
                Some("('n/a')".into()),
            ],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_decimal_arithmetic_and_rendering() {
    let path = unique_temp_path("sql-decimal");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, price DECIMAL(10,2))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 12.50), (2, 3.30)")
        .expect("insert");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT price, price * 2 AS dbl, price + 0.05 AS bump FROM t ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Some("12.50".into()),
                Some("25.00".into()),
                Some("12.55".into())
            ],
            vec![
                Some("3.30".into()),
                Some("6.60".into()),
                Some("3.35".into())
            ],
        ]
    );
    // Division derives scale = max(6, ...) per SQL Server.
    let (_, rows) = sql_rows(&engine, "SELECT price / 3 FROM t WHERE id = 1");
    assert_eq!(rows, vec![vec![Some("4.166667".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_temporal_types_round_trip() {
    let path = unique_temp_path("sql-temporal");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME2)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, '2020-06-15', '2020-06-15 13:45:30.5')")
        .expect("insert");
    let (_, rows) = sql_rows(&engine, "SELECT d, dt FROM t");
    assert_eq!(
        rows,
        vec![vec![
            Some("2020-06-15".into()),
            Some("2020-06-15 13:45:30.5000000".into())
        ]]
    );
    // A character literal implicitly converts to DATE for the comparison.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE d = '2020-06-15'");
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_expression_operators() {
    let path = unique_temp_path("sql-expr-ops");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20), score INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', NULL), (3, 'Carol', 70)")
        .expect("insert");

    // LIKE + IN + BETWEEN combine in a WHERE.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM t WHERE name LIKE 'A%' OR id IN (3) OR score BETWEEN 85 AND 95 ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("3".into())]]);

    // CASE (searched) + ISNULL + a scalar function.
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT UPPER(name) AS u, ISNULL(score, 0) AS s, \
             CASE WHEN score >= 85 THEN 'hi' WHEN score IS NULL THEN 'none' ELSE 'lo' END AS grade \
             FROM t ORDER BY id",
    );
    assert_eq!(cols, vec!["u", "s", "grade"]);
    assert_eq!(
        rows,
        vec![
            vec![Some("ALICE".into()), Some("90".into()), Some("hi".into())],
            vec![Some("BOB".into()), Some("0".into()), Some("none".into())],
            vec![Some("CAROL".into()), Some("70".into()), Some("lo".into())],
        ]
    );

    // CAST and NOT LIKE.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT CAST(score AS NVARCHAR(10)) FROM t WHERE id = 1",
    );
    assert_eq!(rows, vec![vec![Some("90".into())]]);
    let (_, rows) = sql_rows(
        &engine,
        "SELECT id FROM t WHERE name NOT LIKE '%o%' ORDER BY id",
    );
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_swedish_collation_order_by() {
    let path = unique_temp_path("sql-collation");
    let engine = new_engine(&path);
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                 w NVARCHAR(20) COLLATE Finnish_Swedish_CI_AS)",
        )
        .expect("create");
    engine
        .execute(
            "INSERT INTO t VALUES (1, 'öl'), (2, 'apa'), (3, 'åre'), \
                 (4, 'zebra'), (5, 'ängel'), (6, 'björn')",
        )
        .expect("insert");
    // Swedish sorts å, ä, ö after z: apa, björn, zebra, åre, ängel, öl.
    let (_, rows) = sql_rows(&engine, "SELECT w FROM t ORDER BY w");
    let order: Vec<String> = rows.into_iter().map(|r| r[0].clone().unwrap()).collect();
    assert_eq!(order, vec!["apa", "björn", "zebra", "åre", "ängel", "öl"]);
    // The collation is surfaced in sys.columns.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT collation_name FROM sys.columns WHERE name = 'w'",
    );
    assert_eq!(rows, vec![vec![Some("Finnish_Swedish_CI_AS".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_stage5_review_fixes() {
    let path = unique_temp_path("sql-review-fixes");
    let engine = new_engine(&path);
    // CAST decimal/float to int truncates toward zero (not rounds).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT CAST(10.6496 AS INT), CAST(2.9 AS INT), CAST(-10.6496 AS INT)",
    );
    assert_eq!(
        rows,
        vec![vec![
            Some("10".into()),
            Some("2".into()),
            Some("-10".into())
        ]]
    );
    // REPLICATE with a huge count is bounded (no panic / mutex-poison DoS).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT LEN(REPLICATE('abc', 9223372036854775807)) AS n",
    );
    assert_eq!(rows, vec![vec![Some("999999".into())]]);
    // A mixed int/decimal computed column infers enough precision (no 220).
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1), (2)")
        .expect("insert");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT CASE WHEN id = 1 THEN 100000 ELSE 0.5 END AS v FROM t ORDER BY id",
    );
    assert_eq!(
        rows,
        vec![vec![Some("100000.0".into())], vec![Some("0.5".into())]]
    );
    // UPDATE with a duplicated SET column is rejected (264).
    assert_eq!(
        sql_error_number(&engine, "UPDATE t SET id = 3, id = 4 WHERE id = 1"),
        264
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_duplicate_pk_reports_error_2627() {
    let path = unique_temp_path("sql-pk-dup");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    assert_eq!(sql_error_number(&engine, "INSERT INTO t VALUES (1)"), 2627);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_where_order_top_projection() {
    let path = unique_temp_path("sql-select");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE nums (n INT NOT NULL PRIMARY KEY, label NVARCHAR(10))")
        .expect("create");
    for n in 1..=10 {
        engine
            .execute(&format!("INSERT INTO nums VALUES ({n}, 'r{n}')"))
            .expect("insert");
    }
    // WHERE + ORDER DESC + TOP + computed projection.
    let (columns, rows) = sql_rows(
        &engine,
        "SELECT TOP 3 n, n * 10 AS ten FROM nums WHERE n > 4 ORDER BY n DESC",
    );
    assert_eq!(columns, vec!["n", "ten"]);
    assert_eq!(
        rows,
        vec![
            vec![Some("10".into()), Some("100".into())],
            vec![Some("9".into()), Some("90".into())],
            vec![Some("8".into()), Some("80".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_bare_column_alias_is_preserved() {
    let path = unique_temp_path("sql-alias");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE nums (n INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine
        .execute("INSERT INTO nums VALUES (1)")
        .expect("insert");
    // A bare column with an alias must report the alias, not the source
    // column name (regression guard for the typed-projection refactor).
    let (columns, rows) = sql_rows(&engine, "SELECT n AS foo FROM nums");
    assert_eq!(columns, vec!["foo"]);
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_three_valued_where_keeps_only_true_rows() {
    let path = unique_temp_path("sql-3vl");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)")
        .expect("insert");
    // v <> 10 is UNKNOWN for the NULL row, which is filtered out.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE v <> 10 ORDER BY id");
    assert_eq!(rows, vec![vec![Some("3".into())]]);
    // IS NULL is two-valued.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE v IS NULL");
    assert_eq!(rows, vec![vec![Some("2".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_sys_catalog_is_queryable() {
    let path = unique_temp_path("sql-syscat");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE alpha (id INT PRIMARY KEY, name NVARCHAR(20))")
        .expect("create alpha");
    engine
        .execute("CREATE TABLE beta (x BIGINT NOT NULL)")
        .expect("create beta");
    let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.tables ORDER BY name");
    assert_eq!(
        rows,
        vec![vec![Some("alpha".into())], vec![Some("beta".into())]]
    );
    // sys.columns: alpha has two columns.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT name, type FROM sys.columns WHERE object_id = 2 ORDER BY column_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("id".into()), Some("int".into())],
            vec![Some("name".into()), Some("nvarchar(20)".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_drop_table_and_errors() {
    let path = unique_temp_path("sql-drop");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .expect("create");
    // Selecting a missing table -> 208.
    assert_eq!(sql_error_number(&engine, "SELECT * FROM nope"), 208);
    // Duplicate CREATE -> 2714.
    assert_eq!(sql_error_number(&engine, "CREATE TABLE t (id INT)"), 2714);
    // DROP then it's gone; DROP IF EXISTS is a no-op; bare DROP -> 3701.
    engine.execute("DROP TABLE t").expect("drop");
    assert_eq!(sql_error_number(&engine, "SELECT * FROM t"), 208);
    engine
        .execute("DROP TABLE IF EXISTS t")
        .expect("drop if exists");
    assert_eq!(sql_error_number(&engine, "DROP TABLE t"), 3701);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_not_null_violation_reports_515() {
    let path = unique_temp_path("sql-notnull");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(10) NOT NULL)")
        .expect("create");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t (id) VALUES (1)"),
        515
    );
    // String too long -> 8152.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t VALUES (1, 'this is far too long')"),
        8152
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_and_search_share_the_engine() {
    // The SQL front door must not disturb the frozen ES surface.
    let path = unique_temp_path("sql-es-coexist");
    let engine = new_engine(&path);
    engine
        .execute(
            r#"create index docs { "mappings": { "properties": { "body": { "type": "text" } } } }"#,
        )
        .expect("create index");
    engine
        .execute(r#"insert document docs { "body": "hello world" }"#)
        .expect("insert doc");
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .expect("create table");
    engine
        .execute("INSERT INTO t VALUES (42)")
        .expect("insert row");

    let search = sql(
        &engine,
        r#"search docs { "query": { "match": { "body": "hello" } } }"#,
    );
    assert_eq!(search["hits"]["total"], 1);
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t");
    assert_eq!(rows, vec![vec![Some("42".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_bit_column_compares_to_integer_literal() {
    let path = unique_temp_path("sql-bit-cmp");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, active BIT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
        .expect("insert");
    // `active = 1` (BIT vs int) must work, not clash.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE active = 1 ORDER BY id");
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_multi_row_insert_is_atomic() {
    let path = unique_temp_path("sql-insert-atomic");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (5)").expect("seed");
    // The 3rd row duplicates PK 5: the whole INSERT must roll back, so
    // rows 10 and 11 must NOT be present.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t VALUES (10), (11), (5)"),
        2627
    );
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows, vec![vec![Some("5".into())]], "no partial rows");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_batch_keeps_earlier_results_before_an_error() {
    let path = unique_temp_path("sql-batch-partial");
    let engine = new_engine(&path);
    // One batch: a good CREATE + INSERT, then a failing INSERT.
    let env = sql(
        &engine,
        "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);",
    );
    assert_eq!(env["kind"], "sql");
    // Two statements succeeded (done, count) before the error.
    assert_eq!(env["results"].as_array().unwrap().len(), 2);
    assert_eq!(env["results"][1]["rows_affected"], 1);
    assert_eq!(env["error"]["number"], 2627);
    // The first row is durably present.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t");
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}
