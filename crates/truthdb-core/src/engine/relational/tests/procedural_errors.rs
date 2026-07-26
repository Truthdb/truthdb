use super::*;

// ---- TRY/CATCH + XACT_STATE() / ERROR_*() (Stage 6) ------------------

#[test]
fn try_catch_error_transfers_to_catch() {
    let path = unique_temp_path("try-catch-basic");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    // The first TRY statement is a duplicate PK (2627); control jumps to the
    // CATCH, so the second INSERT never runs and the batch reports no error.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY \
               INSERT INTO t VALUES (1); \
               INSERT INTO t VALUES (99); \
             END TRY \
             BEGIN CATCH \
               SELECT ERROR_NUMBER() AS n; \
             END CATCH",
    );
    assert!(
        out.error.is_none(),
        "a caught error is not reported: {:?}",
        out.error
    );
    assert_eq!(ids(&out), vec![2627], "CATCH sees the error number");
    // The post-error TRY statement was skipped; only the seed row exists.
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_error_message_and_null_outside() {
    let path = unique_temp_path("try-catch-msg");
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
        "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
    );
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected a rowset");
    };
    let Datum::NVarChar(msg) = &rowset.rows[0][0] else {
        panic!("expected a string, got {:?}", rowset.rows[0][0]);
    };
    assert!(
        msg.contains("PRIMARY KEY") || msg.to_lowercase().contains("duplicate"),
        "ERROR_MESSAGE() text: {msg}"
    );
    // Outside any CATCH block, ERROR_*() are NULL.
    let out = batch(&engine, &mut ctx, "SELECT ERROR_NUMBER() AS n");
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected a rowset");
    };
    assert_eq!(rowset.rows[0][0], Datum::Null);
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_canonical_form_without_terminators_runs() {
    // The way TRY/CATCH is actually written: no `;` before END TRY/END CATCH.
    let path = unique_temp_path("try-catch-canonical");
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
        "BEGIN TRY\n    INSERT INTO t VALUES (1)\nEND TRY\nBEGIN CATCH\n    SELECT ERROR_NUMBER() AS n\nEND CATCH",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![2627]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_success_skips_catch() {
    let path = unique_temp_path("try-catch-ok");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY SELECT 1 AS n; END TRY BEGIN CATCH SELECT 2 AS n; END CATCH",
    );
    assert!(out.error.is_none());
    assert_eq!(all_int_rows(&out), vec![vec![1]], "the CATCH did not run");
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_xact_state_committable() {
    // A caught non-fatal error inside a transaction leaves it committable
    // (XACT_STATE = 1); the transaction survives and COMMIT persists its work.
    let path = unique_temp_path("try-catch-xs1");
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
             INSERT INTO t VALUES (1); \
             BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH; \
             COMMIT;",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(ids(&out), vec![1], "committable (1) inside the CATCH");
    assert!(!ctx.has_open_transaction(), "COMMIT closed the transaction");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(ids(&out), vec![1], "the surviving insert committed");
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_xact_state_doomed_under_xact_abort() {
    // Under SET XACT_ABORT ON, an error inside TRY still transfers to CATCH,
    // but the transaction is doomed (XACT_STATE = -1) and only ROLLBACK works.
    let path = unique_temp_path("try-catch-xs-doomed");
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
             INSERT INTO t VALUES (1); \
             BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH",
    );
    assert!(out.error.is_none(), "the error was caught: {:?}", out.error);
    assert_eq!(ids(&out), vec![-1], "doomed (-1) inside the CATCH");
    assert!(ctx.has_open_transaction(), "still open, awaiting ROLLBACK");
    // A doomed transaction rejects further writes with 3930.
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930));
    // ROLLBACK clears it; nothing persisted.
    batch(&engine, &mut ctx, "ROLLBACK");
    assert!(!ctx.has_open_transaction());
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(ids(&out), Vec::<i32>::new());
    let _ = std::fs::remove_file(path);
}

#[test]
fn raiserror_is_exempt_from_xact_abort() {
    // SQL Server: "errors raised by RAISERROR are not affected by SET
    // XACT_ABORT" — the batch continues and the transaction stays
    // committable even under XACT_ABORT ON.
    let path = unique_temp_path("raiserror-xact-exempt");
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
             INSERT INTO t VALUES (1); \
             RAISERROR('mid-batch', 16, 1); \
             INSERT INTO t VALUES (2); \
             COMMIT",
    );
    // The RAISERROR is reported (the batch's continued error), but both
    // INSERTs ran and the COMMIT succeeded.
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(50000));
    assert!(!ctx.has_open_transaction(), "COMMIT went through");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1, 2], "the batch continued past RAISERROR");
    let _ = std::fs::remove_file(path);
}

#[test]
fn raiserror_in_try_enters_catch_without_dooming() {
    // Inside TRY, RAISERROR >= 11 transfers to CATCH — but even under
    // XACT_ABORT ON the transaction is NOT doomed (the exemption again):
    // XACT_STATE() reads 1 in the CATCH and COMMIT still works.
    let path = unique_temp_path("raiserror-try-undoomed");
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
             INSERT INTO t VALUES (1); \
             BEGIN TRY RAISERROR('caught', 16, 1); END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n, ERROR_NUMBER() AS e; END CATCH; \
             COMMIT",
    );
    assert!(out.error.is_none(), "caught: {:?}", out.error);
    let rowset = out
        .results
        .iter()
        .find_map(|r| match r {
            StatementResult::Rows(rowset) => Some(rowset),
            _ => None,
        })
        .expect("catch rowset");
    assert_eq!(
        rowset.rows[0],
        vec![Datum::BigInt(1), Datum::BigInt(50000)],
        "XACT_STATE 1 (not doomed), ERROR_NUMBER 50000"
    );
    assert!(!ctx.has_open_transaction(), "COMMIT went through");
    let _ = std::fs::remove_file(path);
}

#[test]
fn throw_terminates_the_batch_without_dooming() {
    // THROW ends the batch even when nothing dooms: under XACT_ABORT OFF
    // the transaction stays open and committable from the next batch.
    let path = unique_temp_path("throw-terminates");
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
             INSERT INTO t VALUES (1); \
             THROW 50001, 'stop here', 7; \
             INSERT INTO t VALUES (2)",
    );
    let error = out.error.expect("THROW surfaces");
    assert_eq!(
        (error.number, error.level, error.state),
        (50001, 16, 7),
        "THROW's number/severity/state"
    );
    assert!(ctx.has_open_transaction(), "not doomed, still open");
    let out = batch(&engine, &mut ctx, "COMMIT");
    assert!(out.error.is_none(), "committable: {:?}", out.error);
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1], "the second INSERT never ran");
    let _ = std::fs::remove_file(path);
}

#[test]
fn throw_under_xact_abort_dooms() {
    let path = unique_temp_path("throw-xact-dooms");
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
        "SET XACT_ABORT ON; BEGIN TRAN; INSERT INTO t VALUES (1); THROW 50001, 'x', 1;",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3930),
        "doomed: writes rejected"
    );
    batch(&engine, &mut ctx, "ROLLBACK");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(ids(&out), Vec::<i32>::new(), "nothing survived");
    let _ = std::fs::remove_file(path);
}

#[test]
fn bare_throw_rethrows_the_original_error() {
    // A bare THROW in a CATCH re-raises the caught error verbatim —
    // number, severity and state — and terminates the batch.
    let path = unique_temp_path("bare-throw");
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
        "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH THROW; END CATCH; \
             SELECT 99 AS n",
    );
    let error = out.error.as_ref().expect("re-thrown");
    assert_eq!(error.number, 2627, "the ORIGINAL number");
    assert_eq!(error.level, 14, "the ORIGINAL severity");
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the statement after the construct never ran: {:?}",
        out.results
    );
    // Outside any CATCH, a bare THROW is 10704.
    let out = batch(&engine, &mut ctx, "THROW");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(10704));
    let _ = std::fs::remove_file(path);
}

#[test]
fn throw_number_below_50000_is_35100() {
    let path = unique_temp_path("throw-range");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "THROW 999, 'too low', 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(35100));
    let _ = std::fs::remove_file(path);
}

#[test]
fn at_at_error_tracks_the_previous_statement() {
    let path = unique_temp_path("at-at-error");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    // The duplicate INSERT fails (batch continues under XACT_ABORT OFF,
    // no transaction open): the next statement reads 2627, and the one
    // after reads 0 — reading @@ERROR is itself a statement that resets.
    // Inside a transaction (the batch-continue path; outside one an
    // error terminates the batch — the recorded divergence).
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             SELECT @@ERROR AS n; \
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
    assert_eq!(firsts, vec![2627, 0], "2627 then reset to 0");
    // Capturing into a variable inside the CATCH: the CATCH's first
    // statement still sees the number.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT @@ERROR AS n; END CATCH",
    );
    assert_eq!(ids(&out), vec![2627], "@@ERROR visible in the CATCH");
    let _ = std::fs::remove_file(path);
}

#[test]
fn raiserror_severity_10_is_informational() {
    // Severity <= 10 is a message, not an error: the statement SUCCEEDS,
    // the batch reports no error, and @@ERROR reads 0 — or 50000 under
    // WITH SETERROR.
    let path = unique_temp_path("raiserror-info");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "RAISERROR('just so you know', 10, 1); SELECT @@ERROR AS n",
    );
    assert!(out.error.is_none(), "informational: {:?}", out.error);
    assert_eq!(ids(&out), vec![0], "@@ERROR is 0");
    let out = batch(
        &engine,
        &mut ctx,
        "RAISERROR('noted', 5, 1) WITH SETERROR; SELECT @@ERROR AS n",
    );
    assert!(out.error.is_none());
    assert_eq!(ids(&out), vec![50000], "SETERROR stamps 50000");
    let _ = std::fs::remove_file(path);
}

#[test]
fn raiserror_formats_printf_arguments() {
    let path = unique_temp_path("raiserror-printf");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY RAISERROR('%s took %d ms (0x%x)', 16, 1, 'scan', 42, 255); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
    );
    assert!(out.error.is_none());
    let StatementResult::Rows(rows) = &out.results[0] else {
        panic!("expected rows");
    };
    assert_eq!(
        rows.rows[0][0],
        Datum::NVarChar("scan took 42 ms (0xff)".into())
    );
    // A directive with a wrong-typed argument is 2786; an unsupported
    // directive is 2787.
    let out = batch(&engine, &mut ctx, "RAISERROR('%d', 16, 1, 'not an int')");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2786));
    let out = batch(&engine, &mut ctx, "RAISERROR('%f', 16, 1, 1)");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2787));
    let _ = std::fs::remove_file(path);
}

#[test]
fn raiserror_above_18_requires_with_log() {
    let path = unique_temp_path("raiserror-log-gate");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "RAISERROR('big', 19, 1)");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2754));
    // With the option, 19 raises normally (catchable, not fatal).
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY RAISERROR('big', 19, 1) WITH LOG; END TRY \
             BEGIN CATCH SELECT ERROR_SEVERITY() AS n; END CATCH",
    );
    assert!(out.error.is_none());
    assert_eq!(ids(&out), vec![19]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn severity_20_bypasses_catch_and_dooms() {
    // Severity >= 20 is fatal: no CATCH sees it, the transaction dooms,
    // and (on TDS) the connection closes after delivery — the wire half
    // is pinned in the TDS end-to-end suite.
    let path = unique_temp_path("severity-20");
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
             INSERT INTO t VALUES (1); \
             BEGIN TRY RAISERROR('fatal', 20, 1) WITH LOG; END TRY \
             BEGIN CATCH SELECT 1 AS caught; END CATCH",
    );
    let error = out
        .error
        .as_ref()
        .expect("fatal error surfaces past the CATCH");
    assert_eq!((error.number, error.level), (50000, 20));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the CATCH block never ran: {:?}",
        out.results
    );
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930), "doomed");
    batch(&engine, &mut ctx, "ROLLBACK");
    let _ = std::fs::remove_file(path);
}

#[test]
fn exec_inner_raiserror_in_try_reaches_catch_undoomed() {
    // The seam pin: a RAISERROR inside EXEC'd text, inside a TRY, under
    // XACT_ABORT ON. The doom decision is made where the statement kind
    // is known (the inner run_block) — the TRY and EXEC boundaries must
    // not re-derive it, or the exemption is lost in transit.
    let path = unique_temp_path("exec-raiserror-seam");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY EXEC sp_executesql N'RAISERROR(''inner'', 16, 1)'; END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH; \
             COMMIT",
    );
    assert!(out.error.is_none(), "caught: {:?}", out.error);
    assert_eq!(ids(&out), vec![1], "NOT doomed across both boundaries");
    assert!(!ctx.has_open_transaction(), "COMMIT went through");
    // Contrast: run_exec's OWN error (unknown proc) under the same setup
    // dooms per the ordinary rule — decided at its source.
    let out = batch(
        &engine,
        &mut ctx,
        "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY EXEC no_such_proc; END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH",
    );
    assert!(out.error.is_none(), "caught: {:?}", out.error);
    assert_eq!(ids(&out), vec![-1], "doomed: the EXEC's own failure");
    batch(&engine, &mut ctx, "ROLLBACK");
    let _ = std::fs::remove_file(path);
}

// ---- Stage 15 adversarial-review PoCs (severity/abort truth table) ----

/// REVIEW PoC (defect: THROW loses its batch termination at the EXEC
/// seam). SQL Server: an uncaught THROW terminates the batch — including
/// the calling batch when it fires inside `sp_executesql` — without
/// rolling back the transaction under XACT_ABORT OFF. At e49a515 the
/// EXEC arm's fallback re-derives the continuation decision from
/// severity alone (16, non-dooming), so inside an open transaction the
/// OUTER batch continues past the EXEC. Pins the fixed behavior.
#[test]
fn review_poc_throw_inside_exec_terminates_the_outer_batch() {
    let path = unique_temp_path("review-throw-exec-seam");
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
             INSERT INTO t VALUES (1); \
             EXEC sp_executesql N'THROW 50001, ''from inner'', 1'; \
             INSERT INTO t VALUES (2); \
             COMMIT",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
    // The batch terminated at the THROW: the trailing INSERT and COMMIT
    // never ran, and the (undoomed) transaction is still open.
    assert!(
        ctx.has_open_transaction(),
        "THROW must terminate the batch before the COMMIT"
    );
    let out = batch(&engine, &mut ctx, "COMMIT");
    assert!(out.error.is_none(), "committable: {:?}", out.error);
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1], "the INSERT after the EXEC never ran");
    let _ = std::fs::remove_file(path);
}

/// REVIEW probe (passes): severity >= 20 raised inside EXEC'd text
/// bypasses a CATCH around the EXEC. No committed test covered the EXEC
/// route for a fatal error; this pins it. (The EXEC arm's own fatal
/// branch is behaviorally redundant — the in_try transfer plus the
/// TryCatch arm's fatal filter produce the same outcome — so no mutation
/// of that branch alone can fail a test; this pins the SEMANTICS.)
#[test]
fn review_poc_fatal_inside_exec_bypasses_catch() {
    let path = unique_temp_path("review-exec-fatal");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC sp_executesql N'RAISERROR(''die'', 20, 1) WITH LOG'; END TRY \
             BEGIN CATCH SELECT 1 AS caught; END CATCH",
    );
    let error = out.error.as_ref().expect("fatal surfaces past the CATCH");
    assert_eq!((error.number, error.level), (50000, 20));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the CATCH never ran: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

/// REVIEW teeth probe (passes): @@ERROR is maintained by the EXEC arm's
/// own error exits — the in_try transfer (the CATCH's first statement
/// sees the EXEC's failure) and the batch-continue path (outside TRY,
/// inside an open transaction). Dropping the EXEC arm's
/// `txn_ctx.last_error` maintenance survives the committed suite; this
/// pins it.
#[test]
fn review_poc_at_at_error_after_a_failed_exec() {
    let path = unique_temp_path("review-exec-at-at-error");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; EXEC no_such_proc; SELECT @@ERROR AS n; COMMIT",
    );
    assert_eq!(ids(&out), vec![2812], "@@ERROR after the failed EXEC");
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC also_missing; END TRY \
             BEGIN CATCH SELECT @@ERROR AS n; END CATCH",
    );
    assert_eq!(ids(&out), vec![2812], "@@ERROR visible in the CATCH");
    let _ = std::fs::remove_file(path);
}

/// REVIEW teeth probe (passes): a bare THROW re-raises the INNERMOST
/// CATCH's error. Reading `error_stack.first()` instead of `last()`
/// survives the committed suite (every committed bare-THROW test has a
/// one-deep stack); this pins the nested case.
#[test]
fn review_poc_bare_throw_in_nested_catch_rethrows_the_innermost() {
    let path = unique_temp_path("review-nested-bare-throw");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY THROW 50001, 'outer', 1; END TRY \
             BEGIN CATCH \
               BEGIN TRY THROW 50002, 'inner', 5; END TRY \
               BEGIN CATCH THROW; END CATCH \
             END CATCH",
    );
    let error = out.error.as_ref().expect("re-thrown");
    assert_eq!(
        (error.number, error.level, error.state),
        (50002, 16, 5),
        "the INNERMOST caught error, not the outer one"
    );
    let _ = std::fs::remove_file(path);
}

/// REVIEW teeth probe (passes): RAISERROR state 0 reports as state 1.
/// Dropping the `.max(1)` survives the committed suite (the slt line is
/// a bare `statement ok`); this pins the reported state.
#[test]
fn review_poc_raiserror_state_zero_reports_as_one() {
    let path = unique_temp_path("review-state-zero");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY RAISERROR('x', 16, 0); END TRY \
             BEGIN CATCH SELECT ERROR_STATE() AS n; END CATCH",
    );
    assert!(out.error.is_none(), "caught: {:?}", out.error);
    assert_eq!(ids(&out), vec![1], "state 0 reports as 1");
    let _ = std::fs::remove_file(path);
}

/// REVIEW teeth probe (passes): the flush before a RAISERROR statement.
/// Removing `Statement::RaiseError` from run_block's flush condition
/// survives the committed suite (every committed RAISERROR opens its
/// batch, so no DONE is ever deferred when the INFO fires); with a
/// deferred DONE in flight the mutation trips `BatchRun::info`'s
/// debug_assert. This pins the shape.
#[test]
fn review_poc_info_after_a_write_flushes_the_deferred_dones() {
    let path = unique_temp_path("review-info-flush");
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
        "INSERT INTO t VALUES (1); RAISERROR('fyi', 5, 1); SELECT id FROM t",
    );
    assert!(out.error.is_none(), "informational: {:?}", out.error);
    assert_eq!(ids(&out), vec![1]);
    let _ = std::fs::remove_file(path);
}

/// REVIEW record-only pin: an error raised while executing the RAISERROR
/// statement itself (here 2786, a missing substitution argument) takes
/// RAISERROR's lenient path — exempt from XACT_ABORT dooming, batch
/// continues — because run_block classifies by statement KIND, not by
/// which code produced the error. SQL Server's behavior for RAISERROR's
/// own gate errors under XACT_ABORT ON is not clearly documented;
/// pinned as-is so a change is deliberate.
#[test]
fn review_poc_raiserror_gate_errors_take_the_lenient_path() {
    let path = unique_temp_path("review-gate-lenient");
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
             INSERT INTO t VALUES (1); \
             RAISERROR('%d', 16, 1); \
             INSERT INTO t VALUES (2); \
             COMMIT",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2786));
    assert!(!ctx.has_open_transaction(), "COMMIT went through");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1, 2], "the batch continued past 2786");
    let _ = std::fs::remove_file(path);
}

/// REVIEW PoC (divergence, fix recommended): SQL Server substitutes
/// "(null)" for a NULL substitution argument; e49a515 raises 2786. Pins
/// the SQL Server behavior.
#[test]
fn review_poc_raiserror_null_argument_prints_null_marker() {
    let path = unique_temp_path("review-null-arg");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY RAISERROR('v=%s', 16, 1, NULL); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
    );
    assert!(out.error.is_none(), "caught: {:?}", out.error);
    let StatementResult::Rows(rows) = &out.results[0] else {
        panic!("expected rows, got {:?}", out.results);
    };
    assert_eq!(rows.rows[0][0], Datum::NVarChar("v=(null)".into()));
    let _ = std::fs::remove_file(path);
}

/// REVIEW PoC (divergence, fix recommended): RAISERROR's integer
/// substitution arguments are 32-bit in SQL Server (int is the widest
/// accepted argument type), so %x on -1 prints ffffffff and %u prints
/// 4294967295. e49a515 formats through u64 and prints the 64-bit
/// two's complement. Pins the 32-bit width.
#[test]
fn review_poc_raiserror_hex_width_is_32_bit() {
    let path = unique_temp_path("review-hex-width");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY RAISERROR('%x %X %u', 16, 1, -1, -1, -1); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
    );
    assert!(out.error.is_none(), "caught: {:?}", out.error);
    let StatementResult::Rows(rows) = &out.results[0] else {
        panic!("expected rows, got {:?}", out.results);
    };
    assert_eq!(
        rows.rows[0][0],
        Datum::NVarChar("ffffffff FFFFFFFF 4294967295".into())
    );
    let _ = std::fs::remove_file(path);
}

/// REVIEW probe (passes): THROW's bare-vs-args lookahead on tokens that
/// start neither form. `THROW -1, ...` reads as a bare THROW (Minus is
/// not an argument-start token) followed by junk, so the batch is a 102
/// syntax error — the shape SQL Server reports too (its THROW arguments
/// must be constants or variables).
#[test]
fn review_poc_throw_negative_number_is_a_syntax_error() {
    let path = unique_temp_path("review-throw-negative");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "THROW -1, 'x', 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(102));
    let out = batch(&engine, &mut ctx, "THROW (SELECT 1), 'x', 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(102));
    let _ = std::fs::remove_file(path);
}
