use super::*;

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
