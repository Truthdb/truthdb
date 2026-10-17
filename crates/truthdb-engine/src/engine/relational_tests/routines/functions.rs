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
