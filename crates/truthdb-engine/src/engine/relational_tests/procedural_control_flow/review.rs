use super::*;

#[test]
fn cf_review_raiserror_and_throw_inside_while() {
    // RAISERROR >= 11 outside TRY is statement-scope: the loop keeps
    // running and the error surfaces after the batch finishes. THROW is
    // batch-terminating: it ends the loop AND the batch.
    let path = unique_temp_path("cf-raise-throw-loop");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i = 2 RAISERROR('boom', 16, 1); \
             END; \
             SELECT @i AS n",
    );
    assert_eq!(ids(&out), vec![3], "the loop survived the RAISERROR");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(50000),
        "the RAISERROR still surfaces at batch end"
    );
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i = 2 THROW 50001, 'stop', 1; \
             END; \
             SELECT 9 AS n",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "THROW ended the batch, not just the loop: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_return_unwinds_nested_control_flow() {
    // RETURN inside WHILE exits the batch (not just the loop), and a
    // RETURN nested in WHILE-inside-TRY-inside-BEGIN..END unwinds
    // everything without running any CATCH.
    let path = unique_temp_path("cf-return-unwind");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i = 2 RETURN; \
             END; \
             SELECT 1 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "RETURN exited the batch: the post-loop SELECT never ran: {:?}",
        out.results
    );
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY \
               BEGIN \
                 WHILE 1 = 1 \
                 BEGIN \
                   RETURN; \
                 END \
               END \
             END TRY \
             BEGIN CATCH SELECT 5 AS n; END CATCH; \
             SELECT 6 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "RETURN unwound block+loop+TRY without a CATCH: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_while_condition_resets_last_error() {
    // The WHILE's per-iteration condition evaluation resets @@ERROR (like
    // the IF's) — a body error set on the LAST iteration reads 0 after
    // the final (false) condition evaluation. SQL Server ambiguity noted:
    // the IF analogy (every statement evaluation resets @@ERROR) is what
    // this pins.
    let path = unique_temp_path("cf-while-at-at-error");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             DECLARE @i INT = 0; \
             WHILE @i < 1 \
             BEGIN \
               SET @i = @i + 1; \
               INSERT INTO t VALUES (1); \
             END; \
             SELECT @@ERROR AS n; \
             COMMIT",
    );
    assert_eq!(
        ids(&out),
        vec![0],
        "the final condition evaluation reset @@ERROR"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_parser_edges() {
    let path = unique_temp_path("cf-parser-edges");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    // Dangling ELSE binds to the INNERMOST IF (as in SQL Server): the
    // inner condition is false, so the ELSE runs.
    let out = batch(
        &engine,
        &mut ctx,
        "IF 1 = 1 IF 1 = 2 SELECT 1 AS n ELSE SELECT 2 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![2], "the ELSE belongs to the inner IF");
    // ...so when the OUTER condition is false, nothing runs at all.
    let out = batch(
        &engine,
        &mut ctx,
        "IF 1 = 2 IF 1 = 1 SELECT 1 AS n ELSE SELECT 2 AS n; SELECT 3 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![3]);
    // CASE consumes its own ELSE; the IF grammar is unaffected.
    let out = batch(
        &engine,
        &mut ctx,
        "IF CASE WHEN 1 = 1 THEN 1 ELSE 2 END = 1 SELECT 4 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![4]);
    // A semicolon ends the IF: `; ELSE` is a syntax error, as in T-SQL.
    let out = batch(
        &engine,
        &mut ctx,
        "IF 1 = 1 SELECT 1 AS n; ELSE SELECT 2 AS n",
    );
    assert!(out.error.is_some(), "`; ELSE` must not attach to the IF");
    // A block whose first statement is BEGIN TRAN parses as a block.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN BEGIN TRAN; INSERT INTO t VALUES (21); COMMIT; END",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(!ctx.has_open_transaction());
    // WHILE whose body is a bare BREAK parses and runs zero-or-more times.
    let out = batch(&engine, &mut ctx, "WHILE 1 = 0 BREAK; SELECT 8 AS n");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![8]);
    // BREAK inside EXEC'd text is its own batch scope: compile-time 135
    // surfaces as the EXEC's error even though the EXEC sits in a WHILE.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; \
             WHILE @i < 1 \
             BEGIN \
               SET @i = 1; \
               EXEC sp_executesql N'BREAK'; \
             END",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(135));
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_tsql_fidelity_gaps() {
    let path = unique_temp_path("cf-fidelity");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    // An empty block is a compile-time syntax error in T-SQL ("Incorrect
    // syntax near 'END'") — expectation is the recommended FIXED behavior
    // (3360df1 accepts it as a no-op).
    let out = batch(&engine, &mut ctx, "BEGIN END");
    assert!(
        out.error.is_some(),
        "T-SQL rejects an empty BEGIN END block"
    );
    // RETURN with a string value is context error 178 in a batch, like
    // any RETURN with a value — expectation is the recommended FIXED
    // behavior (3360df1 gives 102 near 'x': the string is not parsed as
    // the RETURN's value).
    let out = batch(&engine, &mut ctx, "RETURN 'x'");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
    // Recorded divergence (pinning CURRENT behavior): SQL Server's
    // IF EXISTS sets @@ROWCOUNT from the probe scan (0 here); TruthDB's
    // condition evaluation leaves @@ROWCOUNT untouched, so the INSERT's
    // count of 1 survives the untaken IF.
    let out = batch(
        &engine,
        &mut ctx,
        "INSERT INTO t VALUES (2); \
             IF EXISTS (SELECT * FROM t WHERE id = 99) SELECT 7 AS n; \
             SELECT CAST(@@ROWCOUNT AS INT) AS n",
    );
    assert_eq!(ids(&out), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_condition_error_shapes() {
    let path = unique_temp_path("cf-cond-errors");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (1), (2)");
    // An undeclared variable in the condition is the usual 137.
    let out = batch(&engine, &mut ctx, "IF @nope = 1 SELECT 1 AS n");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(137));
    // A scalar condition subquery returning two rows is 512.
    let out = batch(&engine, &mut ctx, "IF (SELECT id FROM t) = 1 SELECT 1 AS n");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(512));
    // An assignment SELECT nested in a condition subquery is rejected.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @x INT; IF (SELECT @x = 1) = 1 SELECT 1 AS n",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(141));
    // Nested EXISTS inside the condition's subquery works.
    let out = batch(
        &engine,
        &mut ctx,
        "IF EXISTS (SELECT * FROM t WHERE EXISTS (SELECT * FROM t WHERE id = 2)) \
             SELECT 8 AS n ELSE SELECT 9 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![8]);
    // A condition error outside any transaction ends the batch (same
    // ladder as a failed statement); in a transaction with XACT_ABORT
    // OFF the batch continues past the failed IF, taking no branch.
    let out = batch(
        &engine,
        &mut ctx,
        "IF 1 / 0 = 1 SELECT 1 AS n ELSE SELECT 2 AS n",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(8134));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "neither branch ran: {:?}",
        out.results
    );
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             IF 1 / 0 = 1 SELECT 1 AS n ELSE SELECT 2 AS n; \
             SELECT 99 AS n; \
             COMMIT",
    );
    assert_eq!(ids(&out), vec![99], "no branch ran; the batch continued");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(8134));
    assert!(!ctx.has_open_transaction());
    let _ = std::fs::remove_file(path);
}

#[test]
fn scalar_function_body_tables_locked_up_front() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("udf-lock-seam");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE secret (id INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    engine
        .execute("CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY)")
        .expect("t2");
    engine
        .execute(
            "CREATE FUNCTION dbo.secret_count () RETURNS INT AS \
                 BEGIN RETURN (SELECT COUNT(*) FROM secret) END",
        )
        .expect("fn");
    let secret = table_object_id(&engine, "secret");
    // A query that calls the function must Shared-lock the table its body
    // reads, up front — otherwise the body would read it with no lock held
    // under 2PL (the seam-defect class). Checked in the SELECT list, the
    // WHERE clause, and an IF condition.
    for sql in [
        "SELECT id, dbo.secret_count() FROM t2",
        "SELECT id FROM t2 WHERE dbo.secret_count() > 0",
        "IF dbo.secret_count() > 0 SELECT 1 AS n",
    ] {
        let locks = engine.analyze_locks(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            sql,
            Isolation::ReadCommitted,
        );
        assert!(
            locks.contains(&(Resource::Table(secret), LockMode::Shared)),
            "the function's inner-read table must be Shared-locked for `{sql}`: {locks:?}"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn scalar_function_isolation_error_and_nesting() {
    let path = unique_temp_path("udf-isolation");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    // A function does not see caller locals: a body reference to `@outer`
    // is undeclared inside the function scope (137), never the caller's
    // value.
    batch(
        &engine,
        &mut ctx,
        "CREATE FUNCTION dbo.leak () RETURNS INT AS BEGIN RETURN @outer END",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @outer INT = 5; SELECT dbo.leak() AS n",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(137),
        "a function must not see caller locals: {:?}",
        out.error
    );
    // An error inside the body aborts the calling statement.
    batch(
        &engine,
        &mut ctx,
        "CREATE FUNCTION dbo.divzero (@x INT) RETURNS INT AS BEGIN RETURN 1 / @x END",
    );
    let out = batch(&engine, &mut ctx, "SELECT dbo.divzero(0) AS n");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(8134),
        "divide-by-zero in a function aborts the query: {:?}",
        out.error
    );
    // Unbounded recursion hits the shared nesting cap (217), and unwinds.
    batch(
        &engine,
        &mut ctx,
        "CREATE FUNCTION dbo.recur (@x INT) RETURNS INT AS BEGIN RETURN dbo.recur(@x) END",
    );
    let out = batch(&engine, &mut ctx, "SELECT dbo.recur(1) AS n");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(217),
        "recursion must hit the nesting cap: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn goto_jumps_forward_backward_and_errors_on_missing_label() {
    let path = unique_temp_path("goto");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (n INT NOT NULL PRIMARY KEY)")
        .expect("create");

    // Forward GOTO skips the statement it jumps over.
    engine
        .execute(
            "INSERT INTO t VALUES (1); GOTO skip; INSERT INTO t VALUES (2); \
                 skip: INSERT INTO t VALUES (3)",
        )
        .expect("forward goto");
    assert_eq!(
        sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
        vec![vec![Some("1".into())], vec![Some("3".into())]],
        "forward GOTO skipped VALUES(2)"
    );

    // Backward GOTO from inside an IF drives a counting loop (10, 11, 12).
    engine.execute("DELETE FROM t").expect("clear");
    engine
        .execute(
            "DECLARE @i INT = 10; \
                 loop: INSERT INTO t VALUES (@i); SET @i = @i + 1; IF @i <= 12 GOTO loop",
        )
        .expect("backward goto loop");
    assert_eq!(
        sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
        vec![
            vec![Some("10".into())],
            vec![Some("11".into())],
            vec![Some("12".into())],
        ],
        "backward GOTO from an IF looped"
    );

    // A GOTO to a label defined nowhere in scope errors 133.
    assert_eq!(
        sql_error_number(&engine, "GOTO nowhere"),
        133,
        "a GOTO to an undefined label errors 133"
    );

    // A label inside a BEGIN...END block (no semicolon after the label).
    engine.execute("DELETE FROM t").expect("clear");
    engine
        .execute("BEGIN GOTO d; INSERT INTO t VALUES (7); d: INSERT INTO t VALUES (8) END")
        .expect("label in a block");
    assert_eq!(
        sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
        vec![vec![Some("8".into())]],
        "GOTO skipped VALUES(7) inside the block"
    );

    // A label inside a stored procedure body.
    engine.execute("DELETE FROM t").expect("clear");
    assert!(
            sql(
                &engine,
                "CREATE PROCEDURE fill AS BEGIN GOTO d; INSERT INTO t VALUES (20); d: INSERT INTO t VALUES (21) END"
            )["error"]
                .is_null(),
            "a procedure body with a label creates cleanly"
        );
    engine.execute("EXEC fill").expect("exec proc");
    assert_eq!(
        sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
        vec![vec![Some("21".into())]],
        "GOTO inside a procedure body skipped VALUES(20)"
    );

    // A label repeated in the same list errors 132.
    assert_eq!(
        sql_error_number(&engine, "d: SELECT 1; d: SELECT 2"),
        132,
        "a duplicate label errors 132"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn cross_and_outer_apply_correlate_the_right_side_to_each_left_row() {
    let path = unique_temp_path("apply");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE seq (v INT NOT NULL PRIMARY KEY)")
        .expect("seq");
    for v in 1..=3 {
        engine
            .execute(&format!("INSERT INTO seq VALUES ({v})"))
            .expect("ins seq");
    }
    engine
        .execute("CREATE TABLE t (k INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine.execute("INSERT INTO t VALUES (2)").expect("t2");
    engine.execute("INSERT INTO t VALUES (0)").expect("t0");
    engine
            .execute("CREATE FUNCTION dbo.upto (@n INT) RETURNS TABLE AS RETURN (SELECT v FROM seq WHERE v <= @n)")
            .expect("tvf");

    // CROSS APPLY correlates upto(t.k) to each left row and drops the k=0 row
    // (upto(0) yields no rows).
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT t.k, u.v FROM t CROSS APPLY dbo.upto(t.k) u ORDER BY t.k, u.v"
        )
        .1,
        vec![
            vec![Some("2".into()), Some("1".into())],
            vec![Some("2".into()), Some("2".into())],
        ],
        "CROSS APPLY correlates and drops empty-right rows"
    );

    // OUTER APPLY keeps the k=0 row with NULL for the right columns.
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT t.k, u.v FROM t OUTER APPLY dbo.upto(t.k) u ORDER BY t.k, u.v"
        )
        .1,
        vec![
            vec![Some("0".into()), None],
            vec![Some("2".into()), Some("1".into())],
            vec![Some("2".into()), Some("2".into())],
        ],
        "OUTER APPLY keeps empty-right rows with NULL"
    );

    // A correlated derived table on the right side works too.
    assert_eq!(
            sql_rows(
                &engine,
                "SELECT t.k, d.v FROM t CROSS APPLY (SELECT v FROM seq WHERE v <= t.k) d ORDER BY t.k, d.v"
            )
            .1,
            vec![
                vec![Some("2".into()), Some("1".into())],
                vec![Some("2".into()), Some("2".into())],
            ],
            "CROSS APPLY over a correlated derived table"
        );

    drop(engine);
    let _ = std::fs::remove_file(path);
}
