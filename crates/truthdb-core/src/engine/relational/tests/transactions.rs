use super::*;

// ---- explicit transactions (Stage 6, M2) ---------------------------

#[test]
fn txn_commit_is_durable_across_restart() {
    let path = unique_temp_path("txn-commit-durable");
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
        "BEGIN TRANSACTION; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT TRANSACTION;",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(
        !ctx.has_open_transaction(),
        "COMMIT must close the transaction"
    );

    // Reopen: the committed rows must survive ARIES recovery.
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1, 2]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_rollback_discards_all_writes() {
    let path = unique_temp_path("txn-rollback");
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
        "BEGIN TRANSACTION; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); ROLLBACK TRANSACTION;",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(!ctx.has_open_transaction());

    // Only the pre-transaction row 1 remains.
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_trancount_reflects_nesting() {
    let path = unique_temp_path("txn-trancount");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();

    // Outside any transaction, @@TRANCOUNT is 0.
    let out = batch(&engine, &mut ctx, "SELECT @@TRANCOUNT AS n");
    assert_eq!(ids(&out), vec![0]);

    // Nested BEGINs bump the count; only the outermost COMMIT commits.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; BEGIN TRAN; SELECT @@TRANCOUNT AS n;",
    );
    assert_eq!(ids(&out), vec![2]);
    assert!(ctx.has_open_transaction());

    let out = batch(&engine, &mut ctx, "COMMIT; SELECT @@TRANCOUNT AS n;");
    assert_eq!(ids(&out), vec![1], "inner COMMIT only decrements");
    assert!(
        ctx.has_open_transaction(),
        "transaction still open at count 1"
    );

    batch(&engine, &mut ctx, "COMMIT");
    assert!(!ctx.has_open_transaction());
    let out = batch(&engine, &mut ctx, "SELECT @@TRANCOUNT AS n");
    assert_eq!(ids(&out), vec![0]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_statement_error_rolls_back_statement_not_transaction() {
    // SQL Server default (XACT_ABORT OFF): a non-fatal statement error rolls
    // back only that statement; the transaction stays open and the batch
    // continues past it.
    let path = unique_temp_path("txn-stmt-atomic");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    // The middle INSERT is a duplicate PK (2627, severity 14): it rolls back
    // only itself; the surrounding inserts still apply and COMMIT persists them.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2627),
        "the duplicate is reported"
    );
    assert!(
        !ctx.has_open_transaction(),
        "COMMIT ran — the transaction was not doomed"
    );
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(
        ids(&out),
        vec![1, 2],
        "the dup rolled back; 1 and 2 committed"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_partial_multirow_insert_is_atomic() {
    // A multi-row INSERT that fails partway undoes ALL its rows (statement
    // atomicity), leaving no partial write in the surviving transaction.
    let path = unique_temp_path("txn-multirow-atomic");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (5)");
    batch(&engine, &mut ctx, "BEGIN TRAN");
    // (6) inserts, then (5) is a duplicate — the whole statement rolls back.
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (6), (5)");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2627));
    batch(&engine, &mut ctx, "COMMIT");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(
        ids(&out),
        vec![5],
        "the partial (6) was undone with the statement"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_error_dooms_transaction_when_xact_abort_on() {
    // SET XACT_ABORT ON: a statement error dooms the whole transaction — only
    // ROLLBACK is then accepted (error 3930).
    let path = unique_temp_path("txn-doomed");
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
        "SET XACT_ABORT ON; BEGIN TRAN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2627));

    // A doomed transaction rejects further writes with 3930...
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930));

    // ...but a read is still allowed (so a CATCH can inspect state)...
    let out = batch(&engine, &mut ctx, "SELECT 1 AS n");
    assert_eq!(ids(&out), vec![1]);

    // ...and ROLLBACK is allowed and clears the doom.
    let out = batch(&engine, &mut ctx, "ROLLBACK");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(!ctx.has_open_transaction());

    // The table is usable again and holds nothing (the txn rolled back).
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(ids(&out), Vec::<i32>::new());
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_ddl_inside_transaction_is_rejected() {
    let path = unique_temp_path("txn-ddl");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();

    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; CREATE TABLE t (id INT NOT NULL PRIMARY KEY);",
    );
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(226));
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_bare_commit_and_rollback_error() {
    let path = unique_temp_path("txn-bare");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();

    let out = batch(&engine, &mut ctx, "COMMIT TRANSACTION");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3902));

    let out = batch(&engine, &mut ctx, "ROLLBACK TRANSACTION");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3903));
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_abort_on_disconnect_rolls_back() {
    let path = unique_temp_path("txn-disconnect");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();

    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "BEGIN TRAN; INSERT INTO t VALUES (7);");
    assert!(ctx.has_open_transaction());

    // Simulate the session teardown that CloseSession performs.
    engine.abort_session_txn(&mut ctx);
    assert!(!ctx.has_open_transaction());

    let mut ctx2 = TxnContext::default();
    let out = batch(&engine, &mut ctx2, "SELECT id FROM t");
    assert_eq!(
        ids(&out),
        Vec::<i32>::new(),
        "uncommitted insert was rolled back"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_uncommitted_explicit_txn_is_undone_after_crash() {
    let path = unique_temp_path("txn-crash-undo");
    let engine = new_engine(&path);
    batch(
        &engine,
        &mut TxnContext::default(),
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );

    // Session A opens a transaction and inserts 99 but never commits.
    let mut ctx_a = TxnContext::default();
    batch(
        &engine,
        &mut ctx_a,
        "BEGIN TRAN; INSERT INTO t VALUES (99);",
    );
    assert!(ctx_a.has_open_transaction());

    // An autocommit insert commits, forcing the WAL to disk — including
    // A's (earlier, still-uncommitted) log records.
    batch(
        &engine,
        &mut TxnContext::default(),
        "INSERT INTO t VALUES (1)",
    );

    // Crash: drop the engine and A's context without a graceful rollback
    // (StorageTxn has no Drop, so nothing is committed on the way out).
    drop(ctx_a);
    drop(engine);

    // Recovery on reopen redoes history then undoes the loser (A): row 99
    // is gone, the committed row 1 survives.
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    let out = batch(
        &engine,
        &mut TxnContext::default(),
        "SELECT id FROM t ORDER BY id",
    );
    assert_eq!(ids(&out), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn txn_statement_rollback_then_crash_recovers_cleanly() {
    // A statement rolled back to a savepoint writes CLRs; if the transaction
    // then crashes uncommitted, recovery must undo the surviving ops and SKIP
    // the already-compensated statement (follow the CLR chain — never
    // double-undo). This exercises the ARIES correctness of `rollback_to`.
    let path = unique_temp_path("txn-stmt-rollback-crash");
    let engine = new_engine(&path);
    batch(
        &engine,
        &mut TxnContext::default(),
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );

    // Session A: open a transaction, insert 10, hit a duplicate-PK (rolled back
    // to a savepoint under XACT_ABORT OFF), then insert 11 — all uncommitted.
    let mut ctx_a = TxnContext::default();
    batch(
        &engine,
        &mut ctx_a,
        "BEGIN TRAN; INSERT INTO t VALUES (10); INSERT INTO t VALUES (10); INSERT INTO t VALUES (11)",
    );
    assert!(
        ctx_a.has_open_transaction(),
        "the transaction survived the statement error (XACT_ABORT OFF)"
    );

    // An autocommit insert forces A's WAL records — including the compensation
    // CLRs from the rolled-back statement — to disk.
    batch(
        &engine,
        &mut TxnContext::default(),
        "INSERT INTO t VALUES (1)",
    );

    // Crash before A commits.
    drop(ctx_a);
    drop(engine);

    // Recovery undoes A entirely (10 and 11 gone); the compensated duplicate is
    // skipped via its CLR chain (no double-undo / corruption); row 1 survives.
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    let out = batch(
        &engine,
        &mut TxnContext::default(),
        "SELECT id FROM t ORDER BY id",
    );
    assert_eq!(
        ids(&out),
        vec![1],
        "A fully undone; only the committed row survives"
    );
    // The table is writable and 10/11 are free again (fully rolled back).
    batch(
        &engine,
        &mut TxnContext::default(),
        "INSERT INTO t VALUES (10), (11)",
    );
    let out = batch(
        &engine,
        &mut TxnContext::default(),
        "SELECT id FROM t ORDER BY id",
    );
    assert_eq!(ids(&out), vec![1, 10, 11]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn attention_cancel_aborts_a_batch() {
    // A TDS Attention sets the batch's cancel flag; the executor polls it and
    // aborts, returning the internal cancel marker (3617) instead of results.
    // The transaction is not doomed.
    let path = unique_temp_path("attn-cancel");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    for i in 0..10 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("ins");
    }
    // Simulate an Attention arriving: raise the cancel flag for this thread.
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    crate::engine::set_test_cancel(flag);
    let env = sql(&engine, "SELECT id FROM t");
    // Clear before asserting so a panic can't leak the flag to another test.
    crate::engine::clear_test_cancel();
    assert_eq!(
        env["error"]["number"], 3617,
        "a cancelled batch aborts instead of returning rows: {env}"
    );
    // The engine is still usable afterwards.
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM t");
    assert_eq!(rows, vec![vec![Some("10".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn save_transaction_partial_rollback_keeps_earlier_work() {
    // SAVE TRANSACTION + ROLLBACK TRANSACTION <name> undoes only the work done
    // since the savepoint; the transaction stays open and commits the rest.
    let path = unique_temp_path("save-tran");
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
        "BEGIN TRAN; INSERT INTO t VALUES (1); SAVE TRANSACTION sp; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
    );
    // Roll back to the savepoint: 2 and 3 are undone, 1 remains, txn open.
    let out = batch(&engine, &mut ctx, "ROLLBACK TRANSACTION sp");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(ctx.has_open_transaction(), "the transaction stays open");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1], "2 and 3 rolled back; 1 remains");
    // The transaction is still usable and commits the survivors.
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (4); COMMIT");
    let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
    assert_eq!(ids(&out), vec![1, 4]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn rollback_to_unknown_savepoint_errors_3908() {
    let path = unique_temp_path("save-tran-missing");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "BEGIN TRAN; INSERT INTO t VALUES (1)");
    let out = batch(&engine, &mut ctx, "ROLLBACK TRANSACTION nope");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3908),
        "rolling back to an unknown savepoint errors 3908"
    );
    // The transaction is untouched — a full ROLLBACK still works.
    batch(&engine, &mut ctx, "ROLLBACK");
    let _ = std::fs::remove_file(path);
}

#[test]
fn save_transaction_rollback_then_crash_recovers_cleanly() {
    // A ROLLBACK TO savepoint writes CLRs; if the transaction then crashes
    // uncommitted, recovery must undo it all without double-undoing the
    // savepoint-compensated work.
    let path = unique_temp_path("save-tran-crash");
    let engine = new_engine(&path);
    batch(
        &engine,
        &mut TxnContext::default(),
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    let mut ctx_a = TxnContext::default();
    batch(
        &engine,
        &mut ctx_a,
        "BEGIN TRAN; INSERT INTO t VALUES (10); SAVE TRANSACTION sp; INSERT INTO t VALUES (11); ROLLBACK TRANSACTION sp; INSERT INTO t VALUES (12)",
    );
    assert!(ctx_a.has_open_transaction());
    // Force A's WAL (incl. the savepoint-rollback CLRs) to disk.
    batch(
        &engine,
        &mut TxnContext::default(),
        "INSERT INTO t VALUES (1)",
    );
    drop(ctx_a);
    drop(engine);

    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    let out = batch(
        &engine,
        &mut TxnContext::default(),
        "SELECT id FROM t ORDER BY id",
    );
    assert_eq!(
        ids(&out),
        vec![1],
        "A (10/12) fully undone; committed row 1 survives"
    );
    let _ = std::fs::remove_file(path);
}
