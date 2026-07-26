use super::*;

// ---- aggregation, GROUP BY, DISTINCT (Stage 8) ---------------------

fn agg_setup(label: &str) -> (Engine, PathBuf) {
    let path = unique_temp_path(label);
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, dept NVARCHAR(10), amount INT)")
        .expect("create");
    engine
        .execute(
            "INSERT INTO sales VALUES \
                 (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',NULL),(5,'a',20)",
        )
        .expect("insert");
    (engine, path)
}

#[test]
fn sql_aggregates_over_whole_table() {
    let (engine, path) = agg_setup("agg-whole");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM sales",
    );
    // COUNT(*)=5, COUNT(amount)=4 (skips NULL), SUM=80, MIN=10, MAX=30.
    assert_eq!(
        rows,
        vec![vec![
            Some("5".into()),
            Some("4".into()),
            Some("80".into()),
            Some("10".into()),
            Some("30".into()),
        ]]
    );
}

#[test]
fn sql_avg_integer_truncates() {
    let (engine, path) = agg_setup("agg-avg");
    // AVG(amount) = 80/4 = 20 exactly here; use a truncating case too.
    let (_, rows) = sql_rows(&engine, "SELECT AVG(amount) FROM sales WHERE dept = 'a'");
    // dept 'a': 10,20,20 -> sum 50 / 3 = 16 (integer truncation).
    assert_eq!(rows, vec![vec![Some("16".into())]]);
}

#[test]
fn sql_group_by_with_aggregates() {
    let (engine, path) = agg_setup("agg-group");
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT dept, COUNT(*), SUM(amount) FROM sales GROUP BY dept ORDER BY dept",
    );
    assert_eq!(cols[0], "dept");
    assert_eq!(
        rows,
        vec![
            vec![Some("a".into()), Some("3".into()), Some("50".into())],
            vec![Some("b".into()), Some("2".into()), Some("30".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_having_filters_groups() {
    let (engine, path) = agg_setup("agg-having");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 40 ORDER BY dept",
    );
    assert_eq!(rows, vec![vec![Some("a".into()), Some("50".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_count_distinct() {
    let (engine, path) = agg_setup("agg-distinct");
    // amounts: 10,20,30,NULL,20 -> distinct non-null = {10,20,30} = 3.
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(DISTINCT amount) FROM sales");
    assert_eq!(rows, vec![vec![Some("3".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_select_distinct() {
    let (engine, path) = agg_setup("agg-select-distinct");
    let (_, mut rows) = sql_rows(&engine, "SELECT DISTINCT dept FROM sales");
    rows.sort();
    assert_eq!(rows, vec![vec![Some("a".into())], vec![Some("b".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_order_by_ordinal_and_aggregate() {
    let (engine, path) = agg_setup("agg-order");
    // ORDER BY 2 DESC = order by SUM(amount) descending.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT dept, SUM(amount) FROM sales GROUP BY dept ORDER BY 2 DESC",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("a".into()), Some("50".into())],
            vec![Some("b".into()), Some("30".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_count_star_over_empty_is_zero_but_group_by_is_empty_set() {
    let path = unique_temp_path("agg-empty");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    // No rows: COUNT(*) with no GROUP BY = one row (0); SUM = NULL.
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*), SUM(v) FROM t");
    assert_eq!(rows, vec![vec![Some("0".into()), None]]);
    // With GROUP BY, no rows = zero groups.
    let (_, rows) = sql_rows(&engine, "SELECT v, COUNT(*) FROM t GROUP BY v");
    assert!(rows.is_empty());
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_non_grouped_column_is_error_8120() {
    let (engine, path) = agg_setup("agg-8120");
    // `id` is neither grouped nor aggregated.
    assert_eq!(
        sql_error_number(&engine, "SELECT id, dept FROM sales GROUP BY dept"),
        8120
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_aggregate_in_where_is_error_147() {
    let (engine, path) = agg_setup("agg-147");
    assert_eq!(
        sql_error_number(&engine, "SELECT dept FROM sales WHERE COUNT(*) > 1"),
        147
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_group_by_cast_expression_key() {
    let path = unique_temp_path("agg-cast-key");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,10),(3,20)")
        .expect("insert");
    // A CAST group key must match the identical SELECT expression (not
    // wrongly trigger 8120 by recursing into the inner column).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT CAST(v AS BIGINT), COUNT(*) FROM t GROUP BY CAST(v AS BIGINT) ORDER BY 1",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("10".into()), Some("2".into())],
            vec![Some("20".into()), Some("1".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_sum_of_character_column_is_error_8117() {
    let path = unique_temp_path("agg-sum-char");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, s VARCHAR(10))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,'1'),(2,'2'),(3,'3')")
        .expect("insert");
    // SUM/AVG of character data errors (never string-concatenates).
    assert_eq!(sql_error_number(&engine, "SELECT SUM(s) FROM t"), 8117);
    assert_eq!(sql_error_number(&engine, "SELECT AVG(s) FROM t"), 8117);
    let _ = std::fs::remove_file(path);
}

// ---- joins (Stage 8 part 2) ----------------------------------------

fn join_setup(label: &str) -> (Engine, PathBuf) {
    let path = unique_temp_path(label);
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE cust (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
        .expect("create cust");
    engine
        .execute("CREATE TABLE ord (id INT NOT NULL PRIMARY KEY, cust_id INT, amount INT)")
        .expect("create ord");
    engine
        .execute("INSERT INTO cust VALUES (1,'alice'),(2,'bob'),(3,'carol')")
        .expect("insert cust");
    // carol(3) has no orders; order 13 references a missing customer (99).
    engine
        .execute("INSERT INTO ord VALUES (10,1,100),(11,1,200),(12,2,50),(13,99,7)")
        .expect("insert ord");
    (engine, path)
}

fn row_count(engine: &Engine, sql: &str) -> usize {
    sql_rows(engine, sql).1.len()
}

#[test]
fn sql_inner_join() {
    let (engine, path) = join_setup("join-inner");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT c.name, o.amount FROM cust c JOIN ord o ON c.id = o.cust_id ORDER BY o.id",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("alice".into()), Some("100".into())],
            vec![Some("alice".into()), Some("200".into())],
            vec![Some("bob".into()), Some("50".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_left_join_keeps_unmatched_left() {
    let (engine, path) = join_setup("join-left");
    // carol has no orders → one row with NULL amount.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT c.name, o.amount FROM cust c LEFT JOIN ord o ON c.id = o.cust_id \
             ORDER BY c.id, o.id",
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[3], vec![Some("carol".into()), None]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_right_join_keeps_unmatched_right() {
    let (engine, path) = join_setup("join-right");
    // order 13 (cust 99) has no customer → NULL name.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT c.name, o.id FROM cust c RIGHT JOIN ord o ON c.id = o.cust_id ORDER BY o.id",
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[3], vec![None, Some("13".into())]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_full_join_keeps_both_unmatched() {
    let (engine, path) = join_setup("join-full");
    // 3 matched + carol (left-only) + order 13 (right-only) = 5 rows.
    assert_eq!(
        row_count(
            &engine,
            "SELECT c.name, o.id FROM cust c FULL JOIN ord o ON c.id = o.cust_id",
        ),
        5
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_cross_join_and_comma() {
    let (engine, path) = join_setup("join-cross");
    // 3 customers x 4 orders = 12.
    assert_eq!(
        row_count(&engine, "SELECT c.id, o.id FROM cust c CROSS JOIN ord o"),
        12
    );
    assert_eq!(
        row_count(&engine, "SELECT c.id, o.id FROM cust c, ord o"),
        12
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_join_with_where_and_qualified_wildcard() {
    let (engine, path) = join_setup("join-where");
    let (cols, rows) = sql_rows(
        &engine,
        "SELECT c.* FROM cust c JOIN ord o ON c.id = o.cust_id WHERE o.amount > 100 ORDER BY o.id",
    );
    // c.* expands to cust columns; only order 11 (amount 200, alice).
    assert_eq!(cols, vec!["id", "name"]);
    assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_aggregate_over_join() {
    let (engine, path) = join_setup("join-agg");
    // Total amount per customer (inner join).
    let (_, rows) = sql_rows(
        &engine,
        "SELECT c.name, SUM(o.amount) FROM cust c JOIN ord o ON c.id = o.cust_id \
             GROUP BY c.name ORDER BY c.name",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("alice".into()), Some("300".into())],
            vec![Some("bob".into()), Some("50".into())],
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_ambiguous_column_errors() {
    let (engine, path) = join_setup("join-ambig");
    // `id` exists in both cust and ord → ambiguous (SQL Server 209).
    let err = sql_error_number(
        &engine,
        "SELECT id FROM cust c JOIN ord o ON c.id = o.cust_id",
    );
    assert_eq!(err, 209, "ambiguous column should be 209");
    // A genuinely missing column is still 207 (invalid), not 209.
    let missing = sql_error_number(
        &engine,
        "SELECT nope FROM cust c JOIN ord o ON c.id = o.cust_id",
    );
    assert_eq!(missing, 207, "unknown column should be 207");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_grouped_coercion_error_is_not_swallowed() {
    let path = unique_temp_path("agg-coerce");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, g INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,1),(2,123456)")
        .expect("insert");
    // A heterogeneous grouped output (short string in one group, a large
    // integer in another) must raise the truncation error, not mask it as
    // NULL — matching the plain-projection path.
    let plain = sql_error_number(&engine, "SELECT CASE WHEN g = 1 THEN 'x' ELSE g END FROM t");
    let grouped = sql_error_number(
        &engine,
        "SELECT CASE WHEN g = 1 THEN 'x' ELSE g END FROM t GROUP BY g",
    );
    assert_eq!(plain, grouped, "grouped path must raise the same error");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_non_boolean_where_is_rejected_4145() {
    let path = unique_temp_path("sql-where-4145");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    // `WHERE id + 1` is numeric, not boolean.
    assert_eq!(
        sql_error_number(&engine, "SELECT id FROM t WHERE id + 1"),
        4145
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_schema_qualified_names_resolve() {
    let path = unique_temp_path("sql-dbo");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE dbo.products (id INT NOT NULL PRIMARY KEY)")
        .expect("create dbo.");
    engine
        .execute("INSERT INTO dbo.products VALUES (1)")
        .expect("insert dbo.");
    // Reachable by both qualified and bare names.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM products");
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let (_, rows) = sql_rows(&engine, "SELECT id FROM dbo.products");
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_unicode_round_trips_through_insert_and_select() {
    let path = unique_temp_path("sql-unicode");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'café åäö 😀')")
        .expect("insert");
    let (_, rows) = sql_rows(&engine, "SELECT name FROM t");
    assert_eq!(rows, vec![vec![Some("café åäö 😀".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_bigint_overflow_literal_errors_not_saturates() {
    let path = unique_temp_path("sql-bigint-of");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, big BIGINT)")
        .expect("create");
    // 1e30 overflows i64; must error, not silently saturate.
    assert_eq!(
        sql_error_number(
            &engine,
            "INSERT INTO t VALUES (1, 1000000000000000000000000000000)"
        ),
        220
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_table_level_pk_column_is_not_null() {
    let path = unique_temp_path("sql-tablepk");
    let engine = new_engine(&path);
    // A table-level PK on a column with no explicit nullability succeeds
    // (the column is promoted to NOT NULL).
    engine
        .execute("CREATE TABLE t (id INT, v NVARCHAR(10), PRIMARY KEY (id))")
        .expect("create");
    // Inserting NULL into the PK column is then a NOT NULL violation.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t (v) VALUES ('x')"),
        515
    );
    let _ = std::fs::remove_file(path);
}
