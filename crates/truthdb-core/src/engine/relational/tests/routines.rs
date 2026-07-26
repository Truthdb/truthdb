use super::*;

#[test]
fn user_scalar_function_works_in_all_query_clause_positions() {
    let path = unique_temp_path("udf-clauses");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    for i in 1..=5 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    engine
        .execute("CREATE FUNCTION dbo.dbl (@x INT) RETURNS INT AS BEGIN RETURN @x * 2 END")
        .expect("create fn");

    // ORDER BY a UDF (descending by dbl(id) == descending by id).
    assert_eq!(
        sql_rows(&engine, "SELECT id FROM t ORDER BY dbo.dbl(id) DESC").1,
        vec![
            vec![Some("5".into())],
            vec![Some("4".into())],
            vec![Some("3".into())],
            vec![Some("2".into())],
            vec![Some("1".into())],
        ],
        "UDF in ORDER BY"
    );

    // GROUP BY a UDF key: five distinct dbl(id) groups.
    assert_eq!(
        sql_rows(&engine, "SELECT COUNT(*) FROM t GROUP BY dbo.dbl(id)")
            .1
            .len(),
        5,
        "UDF in GROUP BY key"
    );

    // A UDF over an aggregate in the grouped SELECT list (dbl(count)=2 per id).
    assert_eq!(
        sql_rows(&engine, "SELECT dbo.dbl(COUNT(*)) FROM t GROUP BY id").1,
        vec![vec![Some("2".into())]; 5],
        "UDF over an aggregate in the grouped output"
    );

    // HAVING a UDF over the grouping column: dbl(id) > 6 keeps id 4 and 5.
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT id FROM t GROUP BY id HAVING dbo.dbl(id) > 6 ORDER BY id"
        )
        .1,
        vec![vec![Some("4".into())], vec![Some("5".into())]],
        "UDF in HAVING"
    );

    // A UDF as an aggregate argument: SUM(dbl(id)) = 2*(1+2+3+4+5) = 30.
    assert_eq!(
        sql_rows(&engine, "SELECT SUM(dbo.dbl(id)) FROM t").1,
        vec![vec![Some("30".into())]],
        "UDF as an aggregate argument"
    );

    // A UDF in a join ON predicate: dbl(t.id) matches u.x for id 2 and 5.
    engine
        .execute("CREATE TABLE u (x INT NOT NULL PRIMARY KEY)")
        .expect("create u");
    engine.execute("INSERT INTO u VALUES (4)").expect("ins u");
    engine.execute("INSERT INTO u VALUES (10)").expect("ins u");
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT t.id FROM t JOIN u ON dbo.dbl(t.id) = u.x ORDER BY t.id"
        )
        .1,
        vec![vec![Some("2".into())], vec![Some("5".into())]],
        "UDF in join ON"
    );

    // A UDF in a CHECK constraint: dbl(v) <= 10 rejects v = 6 (dbl = 12) with 547.
    engine
        .execute("CREATE TABLE c (v INT CHECK (dbo.dbl(v) <= 10))")
        .expect("create c");
    engine
        .execute("INSERT INTO c VALUES (5)")
        .expect("dbl(5)=10 passes the check");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO c VALUES (6)"),
        547,
        "UDF in CHECK: dbl(6)=12 > 10 conflicts (547)"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn scalar_function_snapshot_scope_covers_body_reads() {
    // The snapshot-scope determination must recurse into a called UDF's
    // body exactly as lock analysis does: under SNAPSHOT isolation with
    // snapshot isolation NOT allowed, a statement whose ONLY table access is
    // inside a UDF body must still raise 3952 (the body IS a data access).
    // Before the fix these silently succeeded and read live/unlocked — the
    // "neither lock nor snapshot" seam.
    let path = unique_temp_path("udf-snapshot-scope");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(
        &engine,
        &mut ctx,
        "CREATE FUNCTION dbo.cnt () RETURNS INT AS BEGIN RETURN (SELECT COUNT(*) FROM t) END",
    );
    batch(
        &engine,
        &mut ctx,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    );
    // A SELECT whose only table read is inside the UDF body.
    let out = batch(&engine, &mut ctx, "SELECT dbo.cnt() AS n");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3952),
        "a UDF-only SELECT must arm the snapshot scope: {:?}",
        out.error
    );
    // An IF condition whose only table read is inside the UDF body.
    let out = batch(&engine, &mut ctx, "IF dbo.cnt() > 0 SELECT 1 AS n");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3952),
        "a UDF-only IF condition must arm the snapshot scope: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn scalar_function_in_view_body_is_lock_analyzed() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("udf-view-lock");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE base (x INT NOT NULL PRIMARY KEY)")
        .expect("base");
    engine
        .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    engine
        .execute(
            "CREATE FUNCTION dbo.secret_count () RETURNS INT AS \
                 BEGIN RETURN (SELECT COUNT(*) FROM secret) END",
        )
        .expect("fn");
    engine
        .execute("CREATE VIEW v AS SELECT x, dbo.secret_count() AS sc FROM base")
        .expect("view");
    let secret = table_object_id(&engine, "secret");
    // A UDF reached THROUGH a view must still have its body's table Shared-
    // locked — else the view-nested UDF reads secret unlocked under 2PL.
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT * FROM v",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(secret), LockMode::Shared)),
        "a view-nested UDF's body table must be Shared-locked: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn scalar_function_does_not_shadow_builtin() {
    let path = unique_temp_path("udf-builtin-shadow");
    let engine = new_engine(&path);
    engine
        .execute("CREATE FUNCTION dbo.abs (@x INT) RETURNS INT AS BEGIN RETURN 0 END")
        .expect("fn");
    // A bare call binds to the built-in ABS (5), not the same-named UDF (0).
    let (_, rows) = sql_rows(&engine, "SELECT abs(-5) AS n");
    assert_eq!(
        rows,
        vec![vec![Some("5".into())]],
        "bare abs() must be the built-in"
    );
    // The schema-qualified name still reaches the UDF.
    let (_, rows) = sql_rows(&engine, "SELECT dbo.abs(-5) AS n");
    assert_eq!(
        rows,
        vec![vec![Some("0".into())]],
        "dbo.abs() must be the UDF"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn inline_tvf_body_tables_locked_and_snapshotted() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("tvf-seam");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    engine
        .execute(
            "CREATE FUNCTION dbo.rows_of (@x INT) RETURNS TABLE AS \
                 RETURN (SELECT z FROM secret WHERE z >= @x)",
        )
        .expect("tvf");
    let secret = table_object_id(&engine, "secret");
    // A TVF in FROM must Shared-lock the table its body reads, up front.
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT z FROM dbo.rows_of(1)",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(secret), LockMode::Shared)),
        "a TVF's body table must be Shared-locked: {locks:?}"
    );
    // And it must arm the snapshot scope: under SNAPSHOT-not-allowed a TVF
    // whose body reads a table raises 3952 (the body IS a data access).
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    );
    let out = batch(&engine, &mut ctx, "SELECT z FROM dbo.rows_of(1)");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3952),
        "a TVF-reading SELECT must arm the snapshot scope: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn table_variable_access_takes_no_table_locks() {
    use crate::engine::Isolation;
    use crate::lock::Resource;
    let path = unique_temp_path("tablevar-nolocks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE base (id INT NOT NULL PRIMARY KEY)")
        .expect("base");
    // A table variable is session memory: its name never resolves to a base
    // table, so reads and writes of @t take no table/row locks. (A Database
    // intent lock may still appear; only object locks are asserted absent.)
    let has_object_lock = |sql: &str| {
        engine
            .analyze_locks(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                sql,
                Isolation::ReadCommitted,
            )
            .iter()
            .any(|(r, _)| matches!(r, Resource::Table(_) | Resource::Row(..)))
    };
    assert!(
        !has_object_lock("SELECT * FROM @t"),
        "SELECT FROM @t must take no object locks"
    );
    assert!(
        !has_object_lock("INSERT INTO @t VALUES (1)"),
        "INSERT @t VALUES must take no object locks"
    );
    // But an INSERT @t whose SOURCE reads a real table still locks the
    // source — the seam: the @t target is free, the source read is not.
    let base = table_object_id(&engine, "base");
    // A join of base with @t locks only base; the @t side adds nothing.
    let join_locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT * FROM base AS b JOIN @t AS t ON b.id = t.id",
        Isolation::ReadCommitted,
    );
    assert!(
        !join_locks
            .iter()
            .any(|(r, _)| matches!(r, Resource::Table(id) if *id != base)),
        "the @t side of a join must add no table lock beyond base's: {join_locks:?}"
    );
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO @t SELECT id FROM base",
        Isolation::ReadCommitted,
    );
    assert!(
        locks
            .iter()
            .any(|(r, _)| matches!(r, Resource::Table(id) if *id == base)),
        "INSERT @t SELECT FROM base must lock base: {locks:?}"
    );
    assert!(
        !locks
            .iter()
            .any(|(r, _)| matches!(r, Resource::Table(id) if *id != base)),
        "no phantom lock for @t itself: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn table_variable_read_does_not_arm_snapshot() {
    let path = unique_temp_path("tablevar-nosnapshot");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE base (id INT NOT NULL PRIMARY KEY)")
        .expect("base");
    engine.execute("INSERT INTO base VALUES (7)").expect("seed");
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    );
    // Under SNAPSHOT-not-allowed, a @t-only batch is NOT a data access: it
    // must run to completion, not raise 3952.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (1), (2); SELECT id FROM @t",
    );
    assert!(
        out.error.is_none(),
        "a table-variable-only batch must not raise 3952: {:?}",
        out.error
    );
    assert_eq!(ids(&out), vec![1, 2]);
    // But INSERT @t whose SOURCE reads a real table IS a data access and
    // must raise 3952 under SNAPSHOT-not-allowed (the source read needs the
    // snapshot the database forbids).
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); INSERT INTO @t SELECT id FROM base",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3952),
        "INSERT @t SELECT FROM base must arm the snapshot scope: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn function_body_cannot_read_caller_table_variable() {
    let path = unique_temp_path("tablevar-fn-isolation");
    let engine = new_engine(&path);
    // A scalar UDF and an inline TVF whose bodies reference @t are created
    // without a bind-time check, but at call time each runs with its OWN
    // (empty) table-variable scope — it must NOT see the caller's @t. The
    // body's `FROM @t` therefore errors 1087, not silently reading caller
    // rows. This is the scope seam: the read view armed by the calling
    // statement must be shadowed, not inherited, across the body boundary.
    engine
        .execute(
            "CREATE FUNCTION dbo.cnt () RETURNS INT AS BEGIN RETURN (SELECT COUNT(*) FROM @t) END",
        )
        .expect("create scalar udf");
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (1), (2), (3); SELECT dbo.cnt() AS n",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(1087),
        "a scalar UDF body must not read the caller's table variable: {:?}",
        out.error
    );

    engine
        .execute("CREATE FUNCTION dbo.readt () RETURNS TABLE AS RETURN (SELECT id FROM @t)")
        .expect("create inline tvf");
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (99); SELECT id FROM dbo.readt()",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(1087),
        "an inline TVF body must not read the caller's table variable: {:?}",
        out.error
    );

    // A VIEW body is the same stored-object scope: it must not read the
    // caller's @t either. (SQL Server rejects such a view at CREATE; TruthDB
    // defers name resolution, so the isolation must hold at query time.)
    engine
        .execute("CREATE VIEW dbo.vt AS SELECT id FROM @t")
        .expect("create view over @t");
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (1), (2); SELECT id FROM dbo.vt",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(1087),
        "a view body must not read the caller's table variable: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn recursive_function_lock_analysis_terminates() {
    use crate::engine::Isolation;
    let path = unique_temp_path("udf-recursion-bomb");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    // A self-referencing TVF whose body references itself TWICE (fan-out 2):
    // without the visited-set memoization in collect_read_lock_ids this
    // recurses ~2^32 times and hangs analysis (and, under the scheduler
    // mutex, the whole server). Run in a thread so a regression FAILS
    // cleanly on the timeout rather than hanging the test binary.
    engine
        .execute(
            "CREATE FUNCTION dbo.bomb (@x INT) RETURNS TABLE AS \
                 RETURN (SELECT a.id FROM dbo.bomb(@x) AS a JOIN dbo.bomb(@x) AS b ON a.id = b.id)",
        )
        .expect("bomb");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = engine.analyze_locks(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "SELECT id FROM dbo.bomb(1)",
            Isolation::ReadCommitted,
        );
        let _ = tx.send(());
    });
    assert!(
        rx.recv_timeout(std::time::Duration::from_secs(10)).is_ok(),
        "lock analysis of a recursive function must terminate (memoization)"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn showplan_names_a_table_valued_function() {
    let path = unique_temp_path("tvf-showplan");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, grp INT NOT NULL)")
        .expect("nums");
    engine
        .execute(
            "CREATE FUNCTION dbo.in_group (@g INT) RETURNS TABLE AS \
                 RETURN (SELECT id FROM nums WHERE grp = @g)",
        )
        .expect("tvf");
    // A lone TVF in FROM must not render as a phantom nested-loops join over
    // a base table named after the function.
    let plan = plan_lines(&engine, "SELECT id FROM dbo.in_group(20)");
    assert!(
        plan.iter().any(|l| l.contains("Table-valued Function")),
        "plan names the TVF: {plan:?}"
    );
    assert!(
        !plan.iter().any(|l| l.contains("Nested Loops")),
        "no phantom join: {plan:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn multi_statement_tvf_returns_body_populated_rows() {
    let path = unique_temp_path("multi-tvf-basic");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY)")
        .expect("nums");
    engine
        .execute("INSERT INTO nums VALUES (1),(2),(3),(4),(5),(6)")
        .expect("seed");
    // A multi-statement TVF: its body populates the RETURNS table variable
    // from a real table, and the accumulated rows are the result.
    engine
            .execute(
                "CREATE FUNCTION dbo.evens (@n INT) RETURNS @r TABLE (v INT NOT NULL PRIMARY KEY) \
                 AS BEGIN INSERT INTO @r SELECT id FROM nums WHERE id % 2 = 0 AND id <= @n; RETURN END",
            )
            .expect("create multi-tvf");
    let (_cols, rows) = sql_rows(&engine, "SELECT v FROM dbo.evens(5) ORDER BY v");
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string())], vec![Some("4".to_string())]],
        "the body filtered nums to the evens ≤ 5"
    );
    // A different argument reruns the body.
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM dbo.evens(6)");
    assert_eq!(rows, vec![vec![Some("3".to_string())]], "evens ≤ 6 = 2,4,6");
    // The RETURNS table's PRIMARY KEY is enforced when the body populates it:
    // a duplicate key raises 2627 at call time (the body is not run at CREATE).
    engine
        .execute(
            "CREATE FUNCTION dbo.dup () RETURNS @r TABLE (id INT NOT NULL PRIMARY KEY) \
                 AS BEGIN INSERT INTO @r VALUES (1), (1); RETURN END",
        )
        .expect("create dup TVF");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "SELECT id FROM dbo.dup()");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2627),
        "a duplicate result PRIMARY KEY raises 2627: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn multi_statement_tvf_body_reads_are_locked_and_snapshotted() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("multi-tvf-seam");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
        .expect("secret");
    engine
        .execute(
            "CREATE FUNCTION dbo.copy_secret () RETURNS @r TABLE (z INT NOT NULL PRIMARY KEY) \
                 AS BEGIN INSERT INTO @r SELECT z FROM secret; RETURN END",
        )
        .expect("multi-tvf");
    let secret = table_object_id(&engine, "secret");
    // The body's read of `secret` must be Shared-locked up front, just like
    // an inline TVF or a scalar UDF body — the lock seam.
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT z FROM dbo.copy_secret()",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(secret), LockMode::Shared)),
        "a multi-statement TVF's body table must be Shared-locked: {locks:?}"
    );
    // And it must arm the snapshot scope: under SNAPSHOT-not-allowed the body
    // read is a data access, so the call raises 3952.
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    );
    let out = batch(&engine, &mut ctx, "SELECT z FROM dbo.copy_secret()");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3952),
        "a body-reading multi-statement TVF must arm the snapshot scope: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn multi_statement_tvf_rejects_real_table_dml_at_create() {
    let path = unique_temp_path("multi-tvf-sideeffect");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE log (id INT NOT NULL PRIMARY KEY)")
        .expect("log");
    let mut ctx = TxnContext::default();
    // A multi-statement TVF may DML its result table variable, but writing a
    // real table is a side effect rejected at CREATE (443).
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE FUNCTION dbo.bad () RETURNS @r TABLE (id INT NOT NULL PRIMARY KEY) \
             AS BEGIN INSERT INTO log VALUES (1); RETURN END",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(443),
        "side-effecting DML in a TVF body is 443: {:?}",
        out.error
    );
    // Its body must end in RETURN (455).
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE FUNCTION dbo.noret () RETURNS @r TABLE (id INT NOT NULL PRIMARY KEY) \
             AS BEGIN INSERT INTO @r VALUES (1) END",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(455),
        "a function body must end in RETURN: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_describe_stops_at_control_flow() {
    // sp_describe_first_result_set: a batch whose FIRST possible rowset
    // sits inside an IF must answer "not statically derivable" — skipping
    // the IF and describing a LATER statement would hand a prepared
    // driver the wrong COLMETADATA when the branch streams first.
    let path = unique_temp_path("cf-describe");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v NVARCHAR(10))",
    );
    let described = engine.describe_first_result_set("IF 1 = 1 SELECT id FROM t; SELECT v FROM t");
    assert!(
        described.is_err(),
        "an IF-guarded first rowset is not statically derivable: {described:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_own_txn_writes_visible_in_condition() {
    // A transaction's own uncommitted write is visible to its own IF
    // condition (plain READ COMMITTED, no versioning).
    let path = unique_temp_path("cf-own-writes");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             INSERT INTO t VALUES (5); \
             IF EXISTS (SELECT * FROM t WHERE id = 5) SELECT 1 AS n ELSE SELECT 0 AS n; \
             ROLLBACK",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_exec_inner_return_exits_inner_batch_only() {
    // A RETURN inside EXEC'd text ends the INNER batch; the outer batch
    // continues after the EXEC.
    let path = unique_temp_path("cf-exec-return");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "EXEC sp_executesql N'INSERT INTO t VALUES (1); RETURN; INSERT INTO t VALUES (2)'; \
             INSERT INTO t VALUES (3); \
             SELECT id FROM t ORDER BY id",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        ids(&out),
        vec![1, 3],
        "inner RETURN skipped only the inner tail"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_cancel_lands_mid_while() {
    // An Attention arriving while a WHILE spins aborts the batch with the
    // cancel marker instead of looping forever.
    let path = unique_temp_path("cf-cancel-while");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    crate::engine::set_test_cancel(flag.clone());
    let setter = {
        let flag = flag.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(150));
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        })
    };
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; WHILE 1 = 1 SET @i = @i + 1",
    );
    crate::engine::clear_test_cancel();
    setter.join().expect("setter thread");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3617),
        "the spin died on the Attention"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_condition_cte_executes_but_analysis_misses_it() {
    // Runtime reachability half of the CTE lock hole: the executor
    // inlines a WITH inside an IF condition's subquery and reads the base
    // table (see storage.rs cf_review_analyze_locks_condition_cte for the
    // analysis half — the lock set contains nothing for it).
    let path = unique_temp_path("cf-cond-cte-runtime");
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
        "IF EXISTS (WITH x AS (SELECT id FROM t) SELECT id FROM x) \
             SELECT 1 AS n ELSE SELECT 0 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![1], "the CTE condition read the table");
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_rcsi_condition_read_is_versioned() {
    // Under RCSI a READ COMMITTED read takes no Table S — it relies on
    // the per-statement snapshot instead. The IF condition's subquery
    // must therefore read through a snapshot exactly like a SELECT
    // statement does; reading the raw latest state is a dirty read
    // (expectations here are the FIXED behavior).
    let path = unique_temp_path("cf-rcsi-cond");
    let engine = new_engine(&path);
    let mut admin = TxnContext::default();
    batch(
        &engine,
        &mut admin,
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
    );
    batch(
        &engine,
        &mut admin,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    let mut writer = TxnContext::default();
    let out = batch(&engine, &mut writer, "BEGIN TRAN; INSERT INTO t VALUES (1)");
    assert!(out.error.is_none(), "{:?}", out.error);
    let mut reader = TxnContext::default();
    let out = batch(&engine, &mut reader, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(
        ids(&out),
        vec![0],
        "sanity: a plain SELECT reads the snapshot, not the writer's uncommitted row"
    );
    let out = batch(
        &engine,
        &mut reader,
        "IF EXISTS (SELECT * FROM t WHERE id = 1) SELECT 1 AS n ELSE SELECT 0 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        ids(&out),
        vec![0],
        "the IF condition must read the same snapshot a SELECT would — \
             seeing the writer's uncommitted row is a dirty read"
    );
    batch(&engine, &mut writer, "ROLLBACK");
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_snapshot_isolation_condition_uses_txn_snapshot() {
    // SNAPSHOT isolation: every read in the transaction sees the
    // transaction's snapshot. A commit that lands after the snapshot was
    // established must stay invisible to an IF condition too
    // (expectations here are the FIXED behavior).
    let path = unique_temp_path("cf-snap-cond");
    let engine = new_engine(&path);
    let mut admin = TxnContext::default();
    batch(
        &engine,
        &mut admin,
        "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON",
    );
    batch(
        &engine,
        &mut admin,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut admin, "INSERT INTO t VALUES (1)");
    let mut reader = TxnContext::default();
    batch(
        &engine,
        &mut reader,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
    );
    let out = batch(
        &engine,
        &mut reader,
        "BEGIN TRAN; SELECT COUNT(*) AS n FROM t",
    );
    assert_eq!(ids(&out), vec![1], "the snapshot is established at 1 row");
    let out = batch(&engine, &mut admin, "INSERT INTO t VALUES (2)");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut reader,
        "IF EXISTS (SELECT * FROM t WHERE id = 2) SELECT 1 AS n ELSE SELECT 0 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        ids(&out),
        vec![0],
        "the IF condition must read the transaction's snapshot, \
             not the post-snapshot commit"
    );
    batch(&engine, &mut reader, "COMMIT");
    let _ = std::fs::remove_file(path);
}

#[test]
fn procedure_ddl_round_trips_and_survives_reopen() {
    // CREATE/ALTER/DROP PROCEDURE: catalog persistence (the body is
    // stored text), name collision (2714), the first-statement rule
    // (111), and RETURN <value> legal only inside a body.
    let path = unique_temp_path("proc-ddl");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE add_pair @a INT, @b INT = 7 OUTPUT AS \
             SET @b = @a + @b; RETURN 0",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    // Name collision with any object class.
    let out = batch(&engine, &mut ctx, "CREATE PROC add_pair AS SELECT 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2714));
    // Not the first statement in the batch: 111.
    let out = batch(&engine, &mut ctx, "SELECT 1; CREATE PROC late AS SELECT 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(111));
    // RETURN <value> stays illegal OUTSIDE a body.
    let out = batch(&engine, &mut ctx, "RETURN 3");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
    // ALTER replaces; ALTER of a missing procedure errors.
    let out = batch(
        &engine,
        &mut ctx,
        "ALTER PROCEDURE add_pair @a INT AS RETURN @a",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "ALTER PROC no_such AS SELECT 1");
    assert!(out.error.is_some(), "ALTER of a missing procedure fails");
    drop(engine);

    // The definition survives a reopen; DROP removes it.
    let engine = {
        let storage = Storage::open(path.clone()).expect("reopen");
        Engine::new(storage).expect("engine")
    };
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROC add_pair AS SELECT 1");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2714),
        "the procedure survived the reopen"
    );
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE add_pair");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE add_pair");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3701));
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE IF EXISTS add_pair");
    assert!(out.error.is_none(), "IF EXISTS swallows the miss");
    let _ = std::fs::remove_file(path);
}

#[test]
fn exec_user_procedure_binds_returns_and_copies_output() {
    let path = unique_temp_path("proc-exec");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE ins_and_double @a INT, @b INT = 10, @twice INT OUTPUT AS \
             INSERT INTO t VALUES (@a); \
             SET @twice = (@a + @b) * 2; \
             SELECT @a AS n; \
             RETURN @a + 1",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    // Positional + named + OUTPUT + @rc, default filling @b.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @d INT, @rc INT; \
             EXEC @rc = ins_and_double 5, @twice = @d OUTPUT; \
             SELECT @rc AS n; SELECT @d AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let firsts: Vec<i64> = out
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => Some(i64::from(v)),
                Datum::BigInt(v) => Some(v),
                ref other => panic!("expected int, got {other:?}"),
            },
            _ => None,
        })
        .collect();
    assert_eq!(
        firsts,
        vec![5, 6, 30],
        "body SELECT streamed; @rc = RETURN @a+1; @twice = (5+10)*2"
    );
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(ids(&out), vec![5], "the body's INSERT landed");
    let _ = std::fs::remove_file(path);
}

#[test]
fn exec_user_procedure_argument_errors() {
    let path = unique_temp_path("proc-args");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE p2 @a INT, @o INT OUTPUT AS SET @o = @a",
    );
    let case = |sql: &str| -> Option<i32> {
        let mut c = TxnContext::default();
        batch(&engine, &mut c, sql).error.map(|e| e.number)
    };
    assert_eq!(case("EXEC p2"), Some(201), "missing @a");
    assert_eq!(
        case("DECLARE @x INT; EXEC p2 1, @x OUTPUT, 3"),
        Some(8144),
        "too many arguments"
    );
    assert_eq!(
        case("DECLARE @x INT; EXEC p2 @a = 1, @nope = 2"),
        Some(8145),
        "unknown named parameter"
    );
    assert_eq!(
        case("DECLARE @x INT; EXEC p2 @a = @x OUTPUT, @o = @x"),
        Some(8162),
        "OUTPUT on a non-OUTPUT parameter"
    );
    assert_eq!(
        case("EXEC p2 1, 2 OUTPUT"),
        Some(179),
        "OUTPUT with a constant"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn recursive_procedure_hits_the_nesting_cap() {
    let path = unique_temp_path("proc-recurse");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE recur AS SELECT CAST(@@NESTLEVEL AS INT) AS n; EXEC recur",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC recur");
    // The batch surfaces 217 when the recursion exceeds depth 32...
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(217));
    // ...and the streamed @@NESTLEVEL values count 1, 2, 3, ...
    let levels: Vec<i64> = out
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => Some(i64::from(v)),
                Datum::BigInt(v) => Some(v),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(levels.first(), Some(&1));
    assert_eq!(levels.last(), Some(&32), "depth 32 ran; 33 was refused");
    let _ = std::fs::remove_file(path);
}

#[test]
fn error_procedure_names_the_failing_procedure() {
    let path = unique_temp_path("proc-errproc");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE boomer AS RAISERROR('inside', 16, 1)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC boomer; END TRY \
             BEGIN CATCH SELECT ERROR_PROCEDURE() AS p, ERROR_NUMBER() AS n; END CATCH",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows");
    };
    assert_eq!(
        rowset.rows[0][0],
        Datum::NVarChar("boomer".into()),
        "ERROR_PROCEDURE() names the proc"
    );
    // Outside any procedure, ERROR_PROCEDURE() stays NULL.
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE u (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO u VALUES (1)");
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY INSERT INTO u VALUES (1); END TRY \
             BEGIN CATCH SELECT ERROR_PROCEDURE() AS p; END CATCH",
    );
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows");
    };
    assert_eq!(rowset.rows[0][0], Datum::Null, "NULL in an ad-hoc batch");
    let _ = std::fs::remove_file(path);
}

#[test]
fn output_and_return_skipped_when_the_body_aborts() {
    let path = unique_temp_path("proc-abort-output");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE fails @o INT OUTPUT AS SET @o = 99; THROW 50001, 'die', 1",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 7, @rc INT = -1; \
             EXEC @rc = fails @o = @v OUTPUT; \
             SELECT @v AS n; SELECT @rc AS n",
    );
    // The THROW terminated the batch too, so nothing after the EXEC ran —
    // and neither copy-back nor @rc assignment happened.
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the post-EXEC selects never ran: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

// ---- adversarial review probes: Stage 15 stored procedures ----------

/// First cell of every streamed rowset, as i64 (panics on non-integers).
fn review_first_cells(out: &BatchOutcome) -> Vec<i64> {
    out.results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => Some(i64::from(v)),
                Datum::BigInt(v) => Some(v),
                ref other => panic!("expected int, got {other:?}"),
            },
            _ => None,
        })
        .collect()
}

/// SQL Server refuses a positional argument after a named one (error
/// 119). The current binder accepts it and binds BOTH: the positional
/// value lands on the parameter by position and a named argument for the
/// same parameter is silently discarded — a silent misbind.
#[test]
fn review_poc_positional_after_named_is_refused() {
    let path = unique_temp_path("proc-pos-after-named");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE pan @a INT, @b INT AS SELECT @a * 100 + @b AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC pan @b = 1, 2");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(119),
        "a positional argument after a named one is error 119; instead: \
             error {:?}, streamed {:?}",
        out.error,
        review_first_cells(&out)
    );
    let _ = std::fs::remove_file(path);
}

/// SQL Server coerces each argument to the declared parameter type at
/// bind time ('7' for an INT parameter arrives as int 7; an unconvertible
/// string is a conversion error at the EXEC). The current binder stores
/// the raw value with the declared type tag and never converts, so the
/// body sees a string where it declared an INT.
#[test]
fn review_poc_exec_argument_coerced_to_declared_type() {
    let path = unique_temp_path("proc-arg-coerce");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE echoi @a INT AS SELECT @a AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC echoi '7'");
    assert!(out.error.is_none(), "{:?}", out.error);
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows, got {:?}", out.results);
    };
    assert!(
        matches!(rowset.rows[0][0], Datum::Int(7) | Datum::BigInt(7)),
        "'7' bound to @a INT must arrive as int 7 (DECLARE/SET coerce; \
             EXEC binding must too), got {:?}",
        rowset.rows[0][0]
    );
    // An unconvertible string is a conversion error at the EXEC.
    let out = batch(&engine, &mut ctx, "EXEC echoi 'nope'");
    assert!(
        out.error.is_some(),
        "'nope' for @a INT must fail conversion at bind, streamed {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

/// `EXEC @rc = p` with an UNDECLARED @rc is error 137 in SQL Server. The
/// current code inserts the variable into the caller scope unconditionally,
/// silently creating it.
#[test]
fn review_poc_undeclared_return_status_variable_is_137() {
    let path = unique_temp_path("proc-undeclared-rc");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROCEDURE r4 AS RETURN 4");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC @rc = r4; SELECT @rc AS n");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(137),
        "an undeclared return-status variable must be 137; instead: \
             error {:?}, streamed {:?}",
        out.error,
        review_first_cells(&out)
    );
    let _ = std::fs::remove_file(path);
}

/// A proc error caught by the CALLER's TRY terminated the body: neither
/// OUTPUT copy-back nor the @rc assignment happens, but the batch
/// continues in the CATCH. (The committed abort test cannot observe this
/// — its THROW ends the whole batch before anything reads @v/@rc.)
#[test]
fn review_poc_output_and_rc_skipped_when_proc_error_is_caught() {
    let path = unique_temp_path("proc-caught-output");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE failo @o INT OUTPUT AS \
             SET @o = 99; RAISERROR('die', 16, 1); SET @o = 98",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 7, @rc INT = -1; \
             BEGIN TRY EXEC @rc = failo @o = @v OUTPUT; END TRY \
             BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH; \
             SELECT @v AS n; SELECT @rc AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        review_first_cells(&out),
        vec![50000, 7, -1],
        "caught proc error: no copy-back (7 stays), no @rc (-1 stays)"
    );
    let _ = std::fs::remove_file(path);
}

/// A statement-scope RAISERROR 16 with no TRY anywhere does NOT terminate
/// the proc body: the body runs to completion, so OUTPUT copy-back DOES
/// happen — with the value assigned after the error.
#[test]
fn review_poc_statement_scope_raiserror_completes_body_and_copies_output() {
    let path = unique_temp_path("proc-warn-output");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE warns @o INT OUTPUT AS \
             SET @o = 1; RAISERROR('w', 16, 1); SET @o = 2",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 0; EXEC warns @o = @v OUTPUT; SELECT @v AS n",
    );
    assert_eq!(
        review_first_cells(&out),
        vec![2],
        "the body completed past the statement-scope error; copy-back ran"
    );
    let _ = std::fs::remove_file(path);
}

/// Nested procedures: each frame's RETURN status is its own. An inner
/// EXEC's status never bleeds into the outer frame's `EXEC @rc =`, even
/// when the outer body's LAST action is that inner EXEC.
#[test]
fn review_poc_nested_return_statuses_do_not_bleed() {
    let path = unique_temp_path("proc-nested-rc");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE PROCEDURE five AS RETURN 5",
        "CREATE PROCEDURE seven AS EXEC five; RETURN 7",
        "CREATE PROCEDURE tail AS EXEC five",
        "CREATE PROCEDURE captures AS DECLARE @x INT; EXEC @x = five; SELECT @x AS n",
    ] {
        let out = batch(&engine, &mut ctx, sql);
        assert!(out.error.is_none(), "{sql}: {:?}", out.error);
    }
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @a INT, @b INT, @c INT; \
             EXEC @a = seven; EXEC @b = tail; EXEC @c = captures; \
             SELECT @a AS n; SELECT @b AS n; SELECT @c AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        review_first_cells(&out),
        vec![5, 7, 0, 0],
        "captures streamed inner's 5; then @a=7 (outer RETURN wins), \
             @b=0 (tail's inner EXEC consumed its own status), @c=0"
    );
    let _ = std::fs::remove_file(path);
}

/// The 217 depth-cap error path unwinds EXEC_DEPTH all the way: a
/// subsequent EXEC in the same session starts at nest level 1 again.
#[test]
fn review_poc_exec_depth_unwinds_after_217() {
    let path = unique_temp_path("proc-depth-unwind");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE PROCEDURE recur2 AS EXEC recur2",
        "CREATE PROCEDURE shallow AS SELECT CAST(@@NESTLEVEL AS INT) AS n",
    ] {
        let out = batch(&engine, &mut ctx, sql);
        assert!(out.error.is_none(), "{sql}: {:?}", out.error);
    }
    let out = batch(&engine, &mut ctx, "EXEC recur2");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(217));
    let out = batch(&engine, &mut ctx, "EXEC shallow");
    assert!(
        out.error.is_none(),
        "depth leaked past the 217: {:?}",
        out.error
    );
    assert_eq!(
        review_first_cells(&out),
        vec![1],
        "@@NESTLEVEL restarts at 1 after the failed recursion"
    );
    let _ = std::fs::remove_file(path);
}

/// A parameter named like a caller variable is a separate slot: the body
/// mutates its own @a; the caller's @a is untouched after the EXEC.
#[test]
fn review_poc_param_scope_isolated_from_caller_variable() {
    let path = unique_temp_path("proc-shadow");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE shadow @a INT AS SET @a = @a + 1; SELECT @a AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @a INT = 1; EXEC shadow @a = @a; SELECT @a AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        review_first_cells(&out),
        vec![2, 1],
        "inside: 2; caller's @a unchanged: 1"
    );
    let _ = std::fs::remove_file(path);
}

/// ERROR_PROCEDURE() precision under nested CATCHes: a second error in
/// the ad-hoc CATCH pushes its own frame (procedure NULL, its own
/// number); when that inner CATCH exits, the outer CATCH's ERROR_*()
/// resolve to the procedure error again.
#[test]
fn review_poc_error_procedure_survives_second_error_in_catch() {
    let path = unique_temp_path("proc-errproc-nested");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE boom2 AS RAISERROR('inside', 16, 1)",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC boom2; END TRY \
             BEGIN CATCH \
               SELECT ERROR_PROCEDURE() AS p; \
               BEGIN TRY SELECT 1/0 AS d; END TRY \
               BEGIN CATCH SELECT ERROR_PROCEDURE() AS p; SELECT ERROR_NUMBER() AS n; END CATCH; \
               SELECT ERROR_PROCEDURE() AS p; \
             END CATCH",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let cells: Vec<Datum> = out
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => Some(rowset.rows[0][0].clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        cells,
        vec![
            Datum::NVarChar("boom2".into()),
            Datum::Null,
            Datum::BigInt(8134),
            Datum::NVarChar("boom2".into()),
        ],
        "outer: boom2; inner: NULL + 8134; outer again: boom2"
    );
    let _ = std::fs::remove_file(path);
}

/// DROP TABLE of a procedure must be refused (SQL Server 3701: the
/// procedure namespace is not the table namespace), exactly as DROP TABLE
/// of a view already is. The current arm only guards views, so DROP TABLE
/// silently destroys a procedure.
#[test]
fn review_poc_drop_table_does_not_drop_a_procedure() {
    let path = unique_temp_path("proc-drop-table");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROCEDURE keepp AS SELECT 1 AS n");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "DROP TABLE keepp");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3701),
        "DROP TABLE of a procedure must fail, got {:?}",
        out.error
    );
    let out = batch(&engine, &mut ctx, "EXEC keepp");
    assert!(
        out.error.is_none(),
        "the procedure survived the wrong-type DROP: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

/// DML against a procedure name errors cleanly (the object is not a
/// table); none of these may succeed or panic.
#[test]
fn review_poc_dml_on_a_procedure_name_errors_cleanly() {
    let path = unique_temp_path("proc-dml");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROCEDURE nott AS SELECT 1 AS n");
    assert!(out.error.is_none(), "{:?}", out.error);
    // Observed today: SELECT * silently streams an EMPTY rowset and
    // DELETE silently reports 0 rows affected; only INSERT (110) and
    // UPDATE (207) error, for incidental column reasons.
    for sql in [
        "SELECT * FROM nott",
        "INSERT INTO nott VALUES (1)",
        "UPDATE nott SET x = 1",
        "DELETE FROM nott",
    ] {
        let out = batch(&engine, &mut ctx, sql);
        assert!(
            out.error.is_some(),
            "{sql}: must error (a procedure is not a table), streamed {:?}",
            out.results
        );
    }
    let _ = std::fs::remove_file(path);
}

/// SQL Server requires procedure parameter defaults to be CONSTANTS
/// (literals or NULL). The current code stores the default's source text
/// and evaluates it at EXEC against the CALLER's scope — so `@b INT = @a`
/// captures whatever @a happens to be in each caller, and even a niladic
/// function default drifts per call.
#[test]
fn review_poc_non_constant_parameter_default_rejected_at_create() {
    let path = unique_temp_path("proc-nonconst-default");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE dflt @b INT = @a AS SELECT @b AS n",
    );
    assert!(
        out.error.is_some(),
        "a variable-referencing parameter default must be rejected at \
             CREATE (SQL Server: defaults are constants)"
    );
    let _ = std::fs::remove_file(path);
}

/// The stored body text round-trips exactly through sys.sql_modules:
/// embedded quotes, a line comment, a newline, a trailing statement.
#[test]
fn review_poc_body_text_round_trips_through_sys_sql_modules() {
    let path = unique_temp_path("proc-body-roundtrip");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let body = "SELECT 'it''s' AS s -- trailing comment\n; SELECT 2 AS n";
    let out = batch(
        &engine,
        &mut ctx,
        &format!("CREATE PROCEDURE qbody AS {body}"),
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "SELECT m.definition FROM sys.sql_modules m \
             JOIN sys.procedures p ON m.object_id = p.object_id \
             WHERE p.name = 'qbody'",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows, got {:?}", out.results);
    };
    assert_eq!(
        rowset.rows[0][0],
        Datum::NVarChar(body.into()),
        "the definition is the verbatim body text"
    );
    // And the stored text still executes: both rowsets stream.
    let out = batch(&engine, &mut ctx, "EXEC qbody");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        out.results
            .iter()
            .filter(|r| matches!(r, StatementResult::Rows(_)))
            .count(),
        2,
        "both body statements ran: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

/// CREATE PROCEDURE as dynamic SQL: legal (it is the first statement of
/// the inner batch, as SQL Server requires), and the DDL-in-transaction
/// gate still applies through the dynamic path.
#[test]
fn review_poc_create_procedure_inside_dynamic_sql() {
    let path = unique_temp_path("proc-dyn-create");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "EXEC sp_executesql N'CREATE PROCEDURE dynp AS SELECT 42 AS n'",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC dynp");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(review_first_cells(&out), vec![42]);
    // The DDL-in-txn gate is not bypassed by the dynamic path.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             EXEC sp_executesql N'CREATE PROCEDURE dynp2 AS SELECT 1 AS n';",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(226),
        "DDL inside an explicit transaction stays refused via dynamic SQL"
    );
    batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
    let out = batch(&engine, &mut ctx, "EXEC dynp2");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2812),
        "dynp2 was never created"
    );
    let _ = std::fs::remove_file(path);
}

/// DROP PROCEDURE IF EXISTS of a TABLE name is a silent no-op (the
/// procedure namespace has no such object, IF EXISTS suppresses the
/// miss) and the table survives; without IF EXISTS it is 3701.
#[test]
fn review_poc_drop_procedure_if_exists_of_a_table_is_a_noop() {
    let path = unique_temp_path("proc-die-table");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE keept (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO keept VALUES (1)");
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE IF EXISTS keept");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "SELECT id FROM keept");
    assert_eq!(ids(&out), vec![1], "the table survived");
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE keept");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3701));
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_nested_inner_handles_outer_continues() {
    // The inner CATCH handles the inner error; because it does not re-raise,
    // the outer TRY continues and the outer CATCH never runs.
    let path = unique_temp_path("try-catch-nested");
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
        "BEGIN TRY \
               BEGIN TRY INSERT INTO t VALUES (1); END TRY \
               BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH; \
               SELECT 777 AS n; \
             END TRY \
             BEGIN CATCH SELECT 999 AS n; END CATCH",
    );
    assert!(out.error.is_none());
    assert_eq!(
        all_int_rows(&out),
        vec![vec![2627], vec![777]],
        "inner CATCH ran (2627), outer TRY continued (777), outer CATCH skipped"
    );
    let _ = std::fs::remove_file(path);
}
