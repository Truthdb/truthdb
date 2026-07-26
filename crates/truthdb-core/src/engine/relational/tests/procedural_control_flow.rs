use super::*;

#[test]
fn if_else_takes_the_right_branch_including_null() {
    // T-SQL three-valued conditions: TRUE runs THEN; FALSE and NULL
    // (UNKNOWN) take the ELSE.
    let path = unique_temp_path("if-else");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "IF 1 = 1 SELECT 1 AS n ELSE SELECT 2 AS n",
    );
    assert_eq!(ids(&out), vec![1]);
    let out = batch(
        &engine,
        &mut ctx,
        "IF 1 = 2 SELECT 1 AS n ELSE SELECT 2 AS n",
    );
    assert_eq!(ids(&out), vec![2]);
    let out = batch(
        &engine,
        &mut ctx,
        "IF NULL = NULL SELECT 1 AS n ELSE SELECT 2 AS n",
    );
    assert_eq!(ids(&out), vec![2], "UNKNOWN takes the ELSE");
    // ALIASLESS selects: `ELSE` must not be readable as an implicit
    // column alias (`SELECT 1 ELSE` would silently detach the branch).
    let out = batch(&engine, &mut ctx, "IF 1 = 2 SELECT 1 ELSE SELECT 2");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![2], "ELSE binds to the IF, not as an alias");
    // Without an ELSE, an untaken IF runs nothing.
    let out = batch(&engine, &mut ctx, "IF 1 = 2 SELECT 1 AS n; SELECT 3 AS n");
    assert_eq!(ids(&out), vec![3]);
    // A non-boolean condition is 4145.
    let out = batch(&engine, &mut ctx, "IF 7 SELECT 1 AS n");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(4145));
    let _ = std::fs::remove_file(path);
}

#[test]
fn if_exists_subquery_condition_works() {
    // The bread-and-butter SSMS shape: IF EXISTS (SELECT ...) over a real
    // table, both polarities, plus a scalar-subquery comparison.
    let path = unique_temp_path("if-exists");
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
        "IF EXISTS (SELECT * FROM t WHERE id = 1) SELECT 10 AS n ELSE SELECT 20 AS n",
    );
    assert_eq!(ids(&out), vec![10]);
    let out = batch(
        &engine,
        &mut ctx,
        "IF EXISTS (SELECT * FROM t WHERE id = 99) SELECT 10 AS n ELSE SELECT 20 AS n",
    );
    assert_eq!(ids(&out), vec![20]);
    let out = batch(
        &engine,
        &mut ctx,
        "IF (SELECT COUNT(*) FROM t) = 1 SELECT 30 AS n",
    );
    assert_eq!(ids(&out), vec![30], "scalar subquery in the condition");
    let _ = std::fs::remove_file(path);
}

#[test]
fn while_loop_runs_with_break_and_continue() {
    let path = unique_temp_path("while-loop");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    // A counted loop driven by a variable.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 1; \
             WHILE @i <= 5 \
             BEGIN \
               INSERT INTO t VALUES (@i); \
               SET @i = @i + 1; \
             END; \
             SELECT COUNT(*) AS n FROM t",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![5]);
    // CONTINUE skips even ids; BREAK stops at 8.
    batch(&engine, &mut ctx, "DELETE FROM t");
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i >= 8 BREAK; \
               IF @i % 2 = 0 CONTINUE; \
               INSERT INTO t VALUES (@i); \
             END; \
             SELECT id FROM t ORDER BY id",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![1, 3, 5, 7]);
    let _ = std::fs::remove_file(path);
}

/// The first rowset's first row, as `i32`s across its columns.
fn row_ints(outcome: &BatchOutcome) -> Vec<i32> {
    for result in &outcome.results {
        if let StatementResult::Rows(rowset) = result {
            return rowset.rows[0]
                .iter()
                .map(|d| match d {
                    Datum::TinyInt(v) => *v as i32,
                    Datum::SmallInt(v) => *v as i32,
                    Datum::Int(v) => *v,
                    Datum::BigInt(v) => *v as i32,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect();
        }
    }
    panic!("no rowset in outcome: {:?}", outcome.results);
}

#[test]
fn cursors_iterate_scroll_and_report_fetch_status() {
    let path = unique_temp_path("cursors");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, v INT)",
    );
    batch(
        &engine,
        &mut ctx,
        "INSERT INTO nums VALUES (1,10),(2,20),(3,30)",
    );

    // Forward iteration: FETCH INTO drives a @@FETCH_STATUS loop that sums v.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @sum INT = 0; \
             DECLARE @v INT; \
             DECLARE c CURSOR FOR SELECT v FROM nums ORDER BY id; \
             OPEN c; \
             FETCH NEXT FROM c INTO @v; \
             WHILE @@FETCH_STATUS = 0 \
             BEGIN \
               SET @sum = @sum + @v; \
               FETCH NEXT FROM c INTO @v; \
             END; \
             CLOSE c; \
             DEALLOCATE c; \
             SELECT @sum AS total",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![60]);

    // A SCROLL cursor addresses rows by direction. The trailing FETCH PRIOR
    // runs off the start: @@FETCH_STATUS becomes -1 and @v keeps its value.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @last INT; DECLARE @first INT; DECLARE @abs INT; \
             DECLARE @rel INT; DECLARE @v INT; \
             DECLARE c SCROLL CURSOR FOR SELECT v FROM nums ORDER BY id; \
             OPEN c; \
             FETCH LAST FROM c INTO @last; \
             FETCH FIRST FROM c INTO @first; \
             FETCH ABSOLUTE 2 FROM c INTO @abs; \
             FETCH RELATIVE -1 FROM c INTO @rel; \
             SET @v = @rel; \
             FETCH PRIOR FROM c INTO @v; \
             DECLARE @st INT = @@FETCH_STATUS; \
             CLOSE c; DEALLOCATE c; \
             SELECT @last, @first, @abs, @rel, @v, @st",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    // LAST=30, FIRST=10, ABSOLUTE 2=20, RELATIVE -1 (from row 2)=10, @v held
    // at 10 (the off-start FETCH left it), @@FETCH_STATUS=-1.
    assert_eq!(row_ints(&out), vec![30, 10, 20, 10, 10, -1]);

    // FETCH without INTO returns the fetched row to the client.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE c CURSOR FOR SELECT v FROM nums ORDER BY id; \
             OPEN c; FETCH NEXT FROM c; CLOSE c; DEALLOCATE c",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![10]);

    // FETCH on an unopened cursor -> 16917.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE c CURSOR FOR SELECT v FROM nums; FETCH NEXT FROM c",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(16917));

    // A cursor name that was never declared -> 16916.
    let out = batch(&engine, &mut ctx, "OPEN nope");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(16916));

    // Re-declaring an existing cursor name -> 16915.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE c CURSOR FOR SELECT v FROM nums; \
             DECLARE c CURSOR FOR SELECT v FROM nums",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(16915));

    // OPEN of an already-open cursor -> 16905.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE c CURSOR FOR SELECT v FROM nums; OPEN c; OPEN c",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(16905));

    // A FETCH RELATIVE offset near i64::MAX must not overflow the position:
    // it saturates off the end (status -1), leaving @v unchanged. (A checked
    // build would panic without the saturating add.)
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 7; \
             DECLARE c SCROLL CURSOR FOR SELECT v FROM nums WHERE id < 99 ORDER BY id; \
             OPEN c; \
             FETCH NEXT FROM c INTO @v; \
             FETCH RELATIVE 9223372036854775807 FROM c INTO @v; \
             DECLARE @st INT = @@FETCH_STATUS; \
             CLOSE c; DEALLOCATE c; \
             SELECT @v, @st",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(row_ints(&out), vec![10, -1]);

    let _ = std::fs::remove_file(path);
}

#[test]
fn break_crosses_a_try_and_return_exits_the_batch() {
    let path = unique_temp_path("flow-crossings");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    // BREAK inside a TRY leaves the loop without touching the CATCH.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               BEGIN TRY IF @i = 3 BREAK; END TRY BEGIN CATCH SELECT 99 AS n; END CATCH \
             END; \
             SELECT @i AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![3], "the CATCH never ran, the loop broke");
    // RETURN exits the batch mid-way.
    let out = batch(
        &engine,
        &mut ctx,
        "INSERT INTO t VALUES (1); RETURN; INSERT INTO t VALUES (2)",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1], "the post-RETURN INSERT never ran");
    // RETURN with a value is a batch-context error (178), and
    // BREAK/CONTINUE outside a loop are compile-time 135/136.
    let out = batch(&engine, &mut ctx, "RETURN 5");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
    let out = batch(&engine, &mut ctx, "BREAK");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(135));
    let out = batch(&engine, &mut ctx, "CONTINUE");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(136));
    let _ = std::fs::remove_file(path);
}

#[test]
fn if_condition_reads_at_at_error_before_resetting_it() {
    // The canonical pattern: `IF @@ERROR <> 0` sees the failed statement's
    // number (the IF resets @@ERROR only AFTER its condition evaluated).
    let path = unique_temp_path("if-at-at-error");
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
             INSERT INTO t VALUES (1); \
             IF @@ERROR <> 0 SELECT 111 AS n ELSE SELECT 222 AS n; \
             SELECT @@ERROR AS n; \
             COMMIT",
    );
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
        vec![111, 0],
        "the IF saw 2627, then reset @@ERROR to 0"
    );
    // An untaken IF with no ELSE: the IF's own reset is the ONLY one (no
    // branch statement runs to mask it) — @@ERROR reads 0 after it.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             IF 1 = 2 SELECT 999 AS n; \
             SELECT @@ERROR AS n; \
             COMMIT",
    );
    assert_eq!(
        ids(&out),
        vec![0],
        "the IF itself reset @@ERROR though no branch ran"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn doomed_transaction_still_runs_the_canonical_catch_pattern() {
    // IF XACT_STATE() = -1 ROLLBACK — the documented CATCH idiom — must
    // work inside a doomed transaction.
    let path = unique_temp_path("doomed-if-rollback");
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
        "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH \
               IF XACT_STATE() = -1 ROLLBACK; \
             END CATCH; \
             SELECT CAST(@@TRANCOUNT AS INT) AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![0], "the doomed transaction was rolled back");
    assert!(!ctx.has_open_transaction());
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_doomed_gate_condition_reads_branch_writes_gated() {
    // In a doomed transaction's CATCH: an IF condition's subquery READ is
    // legal (SQL Server allows reads in a doomed transaction), but a
    // write inside a taken branch still hits the 3930 gate.
    let path = unique_temp_path("cf-doomed-gate");
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
        "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH \
               IF EXISTS (SELECT * FROM t WHERE id = 1) SELECT 41 AS n ELSE SELECT 40 AS n; \
               IF XACT_STATE() = -1 ROLLBACK; \
             END CATCH; \
             SELECT 42 AS n",
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
        vec![41, 42],
        "the doomed CATCH's condition read saw the txn's own row"
    );
    // A branch WRITE in the doomed CATCH is still rejected with 3930.
    let out = batch(
        &engine,
        &mut ctx,
        "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH \
               IF 1 = 1 INSERT INTO t VALUES (9); \
             END CATCH",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930));
    batch(&engine, &mut ctx, "ROLLBACK");
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_txn_control_inside_while() {
    // BEGIN TRAN / COMMIT (and ROLLBACK) balanced per iteration:
    // @@TRANCOUNT does not drift across iterations.
    let path = unique_temp_path("cf-txn-in-while");
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
        "DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               BEGIN TRAN; INSERT INTO t VALUES (@i); COMMIT; \
               SET @i = @i + 1; \
             END; \
             SELECT CAST(@@TRANCOUNT AS INT) AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![0], "trancount balanced after the loop");
    let out = batch(&engine, &mut ctx, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(ids(&out), vec![3]);
    // Per-iteration ROLLBACK: every iteration's insert is undone.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @i INT = 10; \
             WHILE @i < 13 \
             BEGIN \
               BEGIN TRAN; INSERT INTO t VALUES (@i); ROLLBACK; \
               SET @i = @i + 1; \
             END; \
             SELECT COUNT(*) AS n FROM t WHERE id >= 10",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![0]);
    assert!(!ctx.has_open_transaction());
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_break_inside_catch_inside_while() {
    // BREAK issued from a CATCH block still terminates the enclosing
    // WHILE (the CATCH's flow propagates through the TryCatch arm).
    let path = unique_temp_path("cf-break-in-catch");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    // RETURN issued from a CATCH block exits the batch. This runs FIRST:
    // it fails fast if the CATCH's flow is swallowed, where the BREAK
    // case below would spin forever instead.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH RETURN; END CATCH; \
             SELECT 6 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the CATCH's RETURN exited the batch: {:?}",
        out.results
    );
    let out = batch(
        &engine,
        &mut ctx,
        "WHILE 1 = 1 \
             BEGIN \
               BEGIN TRY INSERT INTO t VALUES (1); END TRY \
               BEGIN CATCH BREAK; END CATCH \
             END; \
             SELECT 77 AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![77], "the CATCH's BREAK ended the loop");
    let _ = std::fs::remove_file(path);
}

#[test]
fn cf_review_loop_body_error_continues_or_dooms() {
    // XACT_ABORT OFF in a transaction: a non-dooming body error rolls
    // back only that statement — the LOOP keeps iterating (it must not
    // swallow the error either: the batch reports it at the end).
    let path = unique_temp_path("cf-loop-body-error");
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
             WHILE @i < 3 \
             BEGIN \
               SET @i = @i + 1; \
               INSERT INTO t VALUES (1); \
             END; \
             SELECT @i AS n; \
             COMMIT",
    );
    assert_eq!(
        ids(&out),
        vec![3],
        "all three iterations ran despite the per-iteration 2627"
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2627),
        "the continued error still surfaces at batch end"
    );
    assert!(!ctx.has_open_transaction(), "the COMMIT went through");
    // XACT_ABORT ON: the first body error dooms and ends the batch
    // mid-loop — the loop must NOT swallow it and keep iterating.
    let out = batch(
        &engine,
        &mut ctx,
        "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               SET @i = @i + 1; \
               INSERT INTO t VALUES (1); \
             END; \
             SELECT @i AS n; \
             COMMIT",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2627));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the batch ended mid-loop: no SELECT ran: {:?}",
        out.results
    );
    assert!(ctx.has_open_transaction(), "doomed transaction stays open");
    batch(&engine, &mut ctx, "ROLLBACK");
    let _ = std::fs::remove_file(path);
}

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
