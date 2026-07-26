use super::*;

#[test]
fn update_function_reports_touched_columns_in_a_trigger() {
    let path = unique_temp_path("update-fn");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, b INT)")
        .expect("t");
    engine
        .execute("CREATE TABLE tlog (which VARCHAR(20) NOT NULL PRIMARY KEY)")
        .expect("tlog");
    engine
            .execute("CREATE TRIGGER trg ON t AFTER UPDATE AS IF UPDATE(a) INSERT INTO tlog VALUES ('a'); IF UPDATE(b) INSERT INTO tlog VALUES ('b')")
            .expect("trigger");
    engine
        .execute("INSERT INTO t VALUES (1, 10, 20)")
        .expect("seed");

    // Update only column a: UPDATE(a) is true, UPDATE(b) is false.
    engine
        .execute("UPDATE t SET a = 99 WHERE id = 1")
        .expect("update a");
    assert_eq!(
        sql_rows(&engine, "SELECT which FROM tlog ORDER BY which").1,
        vec![vec![Some("a".into())]],
        "only column a is reported updated"
    );

    // Now update column b: UPDATE(b) is true (a is not in this SET list).
    engine
        .execute("UPDATE t SET b = 30 WHERE id = 1")
        .expect("update b");
    assert_eq!(
        sql_rows(&engine, "SELECT which FROM tlog ORDER BY which").1,
        vec![vec![Some("a".into())], vec![Some("b".into())]],
        "column b is now reported updated"
    );

    // Outside a trigger, UPDATE()/COLUMNS_UPDATED() error 4101.
    assert_eq!(
        sql_error_number(&engine, "SELECT UPDATE(a)"),
        4101,
        "UPDATE() outside a trigger errors"
    );
    assert_eq!(
        sql_error_number(&engine, "SELECT COLUMNS_UPDATED()"),
        4101,
        "COLUMNS_UPDATED() outside a trigger errors"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn instead_of_triggers_replace_the_dml() {
    let path = unique_temp_path("instead-of");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE ilog (n INT NOT NULL PRIMARY KEY)")
        .expect("ilog");

    // INSTEAD OF INSERT: the base insert is bypassed; the body logs `inserted`.
    engine
        .execute("CREATE TABLE ti (id INT NOT NULL PRIMARY KEY)")
        .expect("ti");
    engine
            .execute("CREATE TRIGGER trg_i ON ti INSTEAD OF INSERT AS INSERT INTO ilog SELECT id FROM inserted")
            .expect("io insert");
    engine.execute("INSERT INTO ti VALUES (5)").expect("insert");
    assert_eq!(
        sql_rows(&engine, "SELECT COUNT(*) FROM ti").1,
        vec![vec![Some("0".into())]],
        "INSTEAD OF INSERT bypassed the base insert"
    );

    // INSTEAD OF DELETE: base delete bypassed; body logs `deleted` (+100).
    engine
        .execute("CREATE TABLE td (id INT NOT NULL PRIMARY KEY)")
        .expect("td");
    engine
        .execute("INSERT INTO td VALUES (7)")
        .expect("seed td");
    engine
            .execute("CREATE TRIGGER trg_d ON td INSTEAD OF DELETE AS INSERT INTO ilog SELECT id + 100 FROM deleted")
            .expect("io delete");
    engine
        .execute("DELETE FROM td WHERE id = 7")
        .expect("delete");
    assert_eq!(
        sql_rows(&engine, "SELECT COUNT(*) FROM td").1,
        vec![vec![Some("1".into())]],
        "INSTEAD OF DELETE bypassed the base delete"
    );

    // INSTEAD OF UPDATE: base update bypassed; body logs `inserted` (new v).
    engine
        .execute("CREATE TABLE tu (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("tu");
    engine
        .execute("INSERT INTO tu VALUES (1, 3)")
        .expect("seed tu");
    engine
            .execute("CREATE TRIGGER trg_u ON tu INSTEAD OF UPDATE AS INSERT INTO ilog SELECT v FROM inserted")
            .expect("io update");
    engine
        .execute("UPDATE tu SET v = 42 WHERE id = 1")
        .expect("update");
    assert_eq!(
        sql_rows(&engine, "SELECT v FROM tu").1,
        vec![vec![Some("3".into())]],
        "INSTEAD OF UPDATE bypassed the base update"
    );

    // Every INSTEAD OF body ran over the proposed images: 5 (ins), 42 (upd new
    // value), 107 (del id + 100).
    assert_eq!(
        sql_rows(&engine, "SELECT n FROM ilog ORDER BY n").1,
        vec![
            vec![Some("5".into())],
            vec![Some("42".into())],
            vec![Some("107".into())],
        ],
        "the INSTEAD OF bodies ran over inserted/deleted"
    );

    // A second INSTEAD OF trigger for the same action errors 2113.
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE TRIGGER trg_i2 ON ti INSTEAD OF INSERT AS SELECT 1"
        ),
        2113,
        "only one INSTEAD OF trigger per action"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn disable_and_enable_trigger_controls_firing() {
    let path = unique_temp_path("trigger-disable");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create t");
    engine
        .execute("CREATE TABLE log (n INT NOT NULL PRIMARY KEY)")
        .expect("create log");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO log SELECT id FROM inserted")
        .expect("create trigger");
    let log_count = |e: &Engine| sql_rows(e, "SELECT COUNT(*) FROM log").1;

    // Enabled by default: the insert fires the trigger.
    engine
        .execute("INSERT INTO t VALUES (1)")
        .expect("insert 1");
    assert_eq!(
        log_count(&engine),
        vec![vec![Some("1".into())]],
        "trigger fired"
    );

    // DISABLE: the trigger no longer fires.
    engine.execute("DISABLE TRIGGER trg ON t").expect("disable");
    engine
        .execute("INSERT INTO t VALUES (2)")
        .expect("insert 2");
    assert_eq!(
        log_count(&engine),
        vec![vec![Some("1".into())]],
        "a disabled trigger does not fire"
    );

    // ENABLE: it fires again.
    engine.execute("ENABLE TRIGGER trg ON t").expect("enable");
    engine
        .execute("INSERT INTO t VALUES (3)")
        .expect("insert 3");
    assert_eq!(
        log_count(&engine),
        vec![vec![Some("2".into())]],
        "a re-enabled trigger fires"
    );

    // DISABLE TRIGGER ALL ON <table> disables every trigger on the table.
    engine
        .execute("DISABLE TRIGGER ALL ON t")
        .expect("disable all");
    engine
        .execute("INSERT INTO t VALUES (4)")
        .expect("insert 4");
    assert_eq!(
        log_count(&engine),
        vec![vec![Some("2".into())]],
        "DISABLE TRIGGER ALL stopped firing"
    );

    // A trigger that is not on the named table (or does not exist) errors.
    assert_eq!(
        sql_error_number(&engine, "DISABLE TRIGGER nope ON t"),
        3701,
        "a missing trigger errors"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn after_insert_trigger_fires_reading_inserted() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("trg-insert");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("t");
    engine
        .execute("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("audit");
    engine
            .execute("CREATE TRIGGER trg_t ON t AFTER INSERT AS INSERT INTO audit SELECT id, v FROM inserted")
            .expect("trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1, 100), (2, 200)");
    assert!(out.error.is_none(), "insert+trigger: {:?}", out.error);
    let (_c, rows) = sql_rows(&engine, "SELECT id, v FROM audit ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![Some("1".to_string()), Some("100".to_string())],
            vec![Some("2".to_string()), Some("200".to_string())],
        ],
        "the AFTER INSERT trigger copied `inserted` into audit"
    );
    // The lock seam: analyze_locks over the INSERT must hold `audit`'s
    // Exclusive lock up front (the trigger body writes it) — else the body
    // writes unlocked under 2PL.
    let audit = table_object_id(&engine, "audit");
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t VALUES (9, 9)",
        Isolation::ReadCommitted,
    );
    assert!(
        locks.contains(&(Resource::Table(audit), LockMode::Exclusive)),
        "the trigger body's audit write must be X-locked up front: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn after_update_and_delete_triggers_read_deleted_and_inserted() {
    let path = unique_temp_path("trg-upd-del");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("t");
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("seed");
    engine
        .execute("CREATE TABLE log (k INT NOT NULL PRIMARY KEY, oldv INT, newv INT)")
        .expect("log");
    // UPDATE trigger sees both `deleted` (old) and `inserted` (new).
    engine
        .execute(
            "CREATE TRIGGER trg_u ON t AFTER UPDATE AS INSERT INTO log \
                 SELECT i.id, d.v, i.v FROM inserted AS i JOIN deleted AS d ON i.id = d.id",
        )
        .expect("update trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "UPDATE t SET v = 99 WHERE id = 1");
    assert!(out.error.is_none(), "update+trigger: {:?}", out.error);
    let (_c, rows) = sql_rows(&engine, "SELECT k, oldv, newv FROM log");
    assert_eq!(
        rows,
        vec![vec![
            Some("1".to_string()),
            Some("10".to_string()),
            Some("99".to_string())
        ]],
        "UPDATE trigger joined deleted(old) and inserted(new)"
    );
    // DELETE trigger sees `deleted`.
    engine
        .execute("CREATE TABLE gone (id INT NOT NULL PRIMARY KEY)")
        .expect("gone");
    engine
        .execute(
            "CREATE TRIGGER trg_d ON t AFTER DELETE AS INSERT INTO gone SELECT id FROM deleted",
        )
        .expect("delete trigger");
    let out = batch(&engine, &mut ctx, "DELETE FROM t WHERE id = 2");
    assert!(out.error.is_none(), "delete+trigger: {:?}", out.error);
    let (_c, rows) = sql_rows(&engine, "SELECT id FROM gone");
    assert_eq!(
        rows,
        vec![vec![Some("2".to_string())]],
        "DELETE trigger read deleted"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_rollback_raises_3609_and_undoes_the_dml() {
    let path = unique_temp_path("trg-3609");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    // A trigger that rolls back ends the transaction: 3609, and the firing
    // INSERT is undone (atomic under the implicit transaction).
    engine
        .execute("CREATE TRIGGER trg_rb ON t AFTER INSERT AS ROLLBACK")
        .expect("rollback trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3609),
        "a trigger ROLLBACK raises 3609: {:?}",
        out.error
    );
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "the INSERT must be rolled back with the trigger"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn recursive_trigger_does_not_refire_itself() {
    let path = unique_temp_path("trg-recursive");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    // A trigger whose body inserts into its OWN table must not re-fire itself
    // (recursive triggers OFF by default) — otherwise it would loop.
    engine
            .execute(
                "CREATE TRIGGER trg_self ON t AFTER INSERT AS INSERT INTO t SELECT id + 100 FROM inserted WHERE id < 100",
            )
            .expect("self trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    assert!(out.error.is_none(), "recursive-off insert: {:?}", out.error);
    // The original row plus one from the (non-re-firing) trigger body.
    let (_c, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("101".to_string())]],
        "the trigger fired once, did not recurse on its own insert"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_cycle_lock_analysis_terminates() {
    use crate::engine::Isolation;
    let path = unique_temp_path("trg-cycle");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE a (id INT NOT NULL PRIMARY KEY)")
        .expect("a");
    engine
        .execute("CREATE TABLE b (id INT NOT NULL PRIMARY KEY)")
        .expect("b");
    // A trigger cycle: a's trigger writes b, b's trigger writes a. Lock
    // analysis recurses trigger bodies — without the visited-set it would
    // recurse forever and hang under the scheduler mutex. Run in a thread so
    // a regression fails on the timeout rather than hanging the test binary.
    engine
        .execute("CREATE TRIGGER trg_a ON a AFTER INSERT AS INSERT INTO b SELECT id FROM inserted")
        .expect("trg_a");
    engine
        .execute("CREATE TRIGGER trg_b ON b AFTER INSERT AS INSERT INTO a SELECT id FROM inserted")
        .expect("trg_b");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = engine.analyze_locks(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "INSERT INTO a VALUES (1)",
            Isolation::ReadCommitted,
        );
        let _ = tx.send(());
    });
    assert!(
        rx.recv_timeout(std::time::Duration::from_secs(10)).is_ok(),
        "trigger-cycle lock analysis must terminate (visited-set)"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_raiserror_aborts_and_undoes_the_dml() {
    let path = unique_temp_path("trg-validate");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    // A validation trigger: RAISERROR (severity 16) must abort the firing
    // statement and roll it back — not be silently swallowed.
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('rejected by trigger', 16, 1)")
        .expect("trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    assert!(
        out.error.is_some(),
        "a trigger RAISERROR must fail the INSERT, not be swallowed"
    );
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "the rejected INSERT must be rolled back"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_body_exec_and_fk_reads_are_locked_up_front() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("trg-exec-fk-locks");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE w (id INT NOT NULL PRIMARY KEY)")
        .expect("w");
    engine
        .execute("CREATE PROCEDURE do_write AS INSERT INTO w VALUES (1)")
        .expect("proc");
    engine
        .execute("CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)")
        .expect("parent");
    engine
            .execute("CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT NOT NULL REFERENCES parent(id))")
            .expect("child");
    // The body EXECs a proc that writes w, and inserts into child (FK to
    // parent). analyze_locks over the firing INSERT must include w's X lock
    // (the EXEC'd proc's write) AND parent's S lock (the FK integrity read) —
    // the trigger-body analysis now reuses the real lock analysis.
    engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS BEGIN EXEC do_write; INSERT INTO child VALUES (1, 1) END")
            .expect("trigger");
    let w = table_object_id(&engine, "w");
    let parent = table_object_id(&engine, "parent");
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "INSERT INTO t VALUES (1)",
        Isolation::ReadCommitted,
    );
    // The single-row proc INSERT locks w at Table-IX + Row-X; assert w is
    // locked at all (a write lock, IX or X), proving the EXEC was analyzed.
    assert!(
        locks
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(id) if *id == w)
                && matches!(m, LockMode::Exclusive | LockMode::IntentExclusive)),
        "the EXEC'd proc's write to w must be write-locked up front: {locks:?}"
    );
    assert!(
        locks.contains(&(Resource::Table(parent), LockMode::Shared)),
        "the trigger body's FK read of parent must be S-locked up front: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn procedure_called_from_trigger_cannot_read_inserted() {
    let path = unique_temp_path("trg-proc-shadow");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE sink (id INT NOT NULL PRIMARY KEY)")
        .expect("sink");
    engine
        .execute("CREATE PROCEDURE logproc AS INSERT INTO sink SELECT id FROM inserted")
        .expect("proc");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS EXEC logproc")
        .expect("trigger");
    // inserted is visible only in the trigger's OWN statements; a proc it
    // EXECs cannot see it — the reference errors (and aborts the INSERT).
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    assert!(
        out.error.is_some(),
        "a proc called from a trigger must not resolve `inserted`"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_unbalanced_begin_transaction_raises_3609() {
    let path = unique_temp_path("trg-leak-txn");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    // A trigger that opens a transaction without closing it changes
    // @@TRANCOUNT — 3609, and the leaked transaction is rolled back.
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS BEGIN TRANSACTION")
        .expect("trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3609),
        "an unbalanced BEGIN in a trigger raises 3609: {:?}",
        out.error
    );
    assert!(
        !ctx.has_open_transaction(),
        "the leaked transaction must be rolled back"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_name_is_not_a_droppable_or_queryable_table() {
    let path = unique_temp_path("trg-not-a-table");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO t SELECT 0 WHERE 1 = 0")
        .expect("trigger");
    let mut ctx = TxnContext::default();
    // DROP TABLE on the trigger name must NOT silently destroy it (3701).
    let out = batch(&engine, &mut ctx, "DROP TABLE trg");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3701),
        "DROP TABLE must not destroy a trigger: {:?}",
        out.error
    );
    // SELECT FROM the trigger name must error, not heap-scan its root page.
    let out = batch(&engine, &mut ctx, "SELECT * FROM trg");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(208),
        "SELECT FROM a trigger name must be invalid object: {:?}",
        out.error
    );
    // sys.tables must not list the trigger.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.tables WHERE name = 'trg'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "sys.tables excludes triggers"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn indirect_trigger_recursion_is_allowed() {
    let path = unique_temp_path("trg-indirect");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE a (id INT NOT NULL PRIMARY KEY)")
        .expect("a");
    engine
        .execute("CREATE TABLE b (id INT NOT NULL PRIMARY KEY)")
        .expect("b");
    // Recursive-OFF suppresses only DIRECT self-recursion; indirect
    // recursion (a's trigger writes b, b's trigger writes a) is allowed and
    // bounded by the nesting cap. The IF EXISTS guard stops the FIRING (not
    // just the row insert) so the chain terminates — a bare WHERE would still
    // do a 0-row INSERT that fires the next trigger, looping to the cap.
    engine
        .execute("CREATE TRIGGER ta ON a AFTER INSERT AS INSERT INTO b SELECT id FROM inserted")
        .expect("ta");
    engine
            .execute("CREATE TRIGGER tb ON b AFTER INSERT AS IF EXISTS (SELECT 1 FROM inserted WHERE id < 10) INSERT INTO a SELECT id + 10 FROM inserted WHERE id < 10")
            .expect("tb");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "INSERT INTO a VALUES (1)");
    assert!(out.error.is_none(), "indirect recursion: {:?}", out.error);
    // a = {1 (seed), 11 (via a->b->a)}: the indirect path fired once.
    let (_c, rows) = sql_rows(&engine, "SELECT id FROM a ORDER BY id");
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())], vec![Some("11".to_string())]],
        "indirect a->b->a recursion must fire (id 11 came through b)"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_body_read_is_locked_under_inline_isolation_escalation() {
    use crate::engine::Isolation;
    use crate::lock::{LockMode, Resource};
    let path = unique_temp_path("trg-escalation");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE r (id INT NOT NULL PRIMARY KEY)")
        .expect("r");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS SELECT id FROM r")
        .expect("trigger");
    let r = table_object_id(&engine, "r");
    // A batch that escalates the isolation in-line (SET SERIALIZABLE) under a
    // versioned session (Snapshot) must analyze the trigger body's read of r
    // lock-based — Table S — not drop it as a versioned read. The trigger
    // body analysis now forwards the escalation-corrected isolation.
    let locks = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; INSERT INTO t VALUES (1)",
        Isolation::Snapshot,
    );
    assert!(
        locks.contains(&(Resource::Table(r), LockMode::Shared)),
        "the trigger body's read of r must be Table-S locked under escalation: {locks:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_error_rolls_back_the_dml_inside_an_explicit_transaction() {
    let path = unique_temp_path("trg-explicit-txn");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
        .expect("trigger");
    // Inside an explicit transaction, a trigger error dooms it — the COMMIT
    // of the uncommittable transaction fails, so the firing row can never
    // durably commit (it stays staged in the doomed, still-open transaction).
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (1); COMMIT",
    );
    assert!(
        out.error.is_some(),
        "the trigger error (and the failed COMMIT of the doomed txn) must surface"
    );
    // Roll the doomed transaction back; nothing was ever durable.
    batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "the firing row must never durably commit after a trigger error"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_error_dooms_explicit_transaction_caught_by_try_catch() {
    let path = unique_temp_path("trg-doomed-catch");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY)")
        .expect("audit");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
        .expect("trigger");
    // A trigger error inside an explicit transaction DOOMS it (does not tear
    // it down): the CATCH runs under an uncommittable transaction, so its
    // write is rejected (3930), never silently autocommitted. After the
    // ROLLBACK nothing is durable.
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "BEGIN TRANSACTION; BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH INSERT INTO audit VALUES (99); END CATCH; ROLLBACK",
    );
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM audit");
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "the CATCH write must be rejected under the doomed transaction, not autocommitted"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_that_rolls_back_and_errors_does_not_wedge_the_session() {
    let path = unique_temp_path("trg-rollback-error");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE other (id INT NOT NULL PRIMARY KEY)")
        .expect("other");
    // The idiomatic abort-in-trigger pattern: ROLLBACK then RAISERROR. The
    // trigger ends the transaction AND errors — it must abort cleanly (3609
    // path), not doom a torn-down transaction and leave the session wedged.
    engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS BEGIN ROLLBACK; RAISERROR('reject', 16, 1) END")
            .expect("trigger");
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (1)",
    );
    assert!(out.error.is_some(), "the trigger's RAISERROR must surface");
    // The session is not wedged: a subsequent autocommit write succeeds
    // (no leftover doomed state rejecting it with 3930).
    let out = batch(&engine, &mut ctx, "INSERT INTO other VALUES (1)");
    assert!(
        out.error.is_none(),
        "the session must not be wedged after ROLLBACK; RAISERROR: {:?}",
        out.error
    );
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM other");
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())]],
        "the later write committed"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn uncaught_trigger_error_dooms_so_later_writes_are_rejected() {
    let path = unique_temp_path("trg-uncaught-doom");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE other (id INT NOT NULL PRIMARY KEY)")
        .expect("other");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
        .expect("trigger");
    // An uncaught trigger error in an explicit transaction dooms it, so a
    // later write in the same transaction is rejected (3930) — it cannot
    // durably commit new work over the uncommittable transaction.
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (1); INSERT INTO other VALUES (2)",
    );
    batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM other");
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "the doomed transaction must reject the later write, not commit it"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn doomed_transaction_catch_reaches_its_rollback_after_a_benign_error() {
    let path = unique_temp_path("trg-catch-benign");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
        .expect("trigger");
    // A trigger error dooms the explicit transaction and transfers to the
    // CATCH. A benign statement-terminating error inside the CATCH (division
    // by zero) must NOT abort the batch before the CATCH reaches its
    // ROLLBACK — otherwise the uncommittable transaction is left open holding
    // its locks (the wedge class). After the batch the transaction is closed.
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "BEGIN TRANSACTION; BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT 1 / 0 AS x; IF XACT_STATE() <> 0 ROLLBACK; END CATCH",
    );
    assert!(
        !ctx.has_open_transaction(),
        "the CATCH must reach ROLLBACK despite the divide-by-zero; no wedged transaction"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_is_not_an_alter_or_dml_target() {
    let path = unique_temp_path("trg-alter-target");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO t SELECT 0 WHERE 1 = 0")
        .expect("trigger");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "ALTER TABLE trg ADD c INT");
    assert!(out.error.is_some(), "ALTER TABLE on a trigger must error");
    let out = batch(&engine, &mut ctx, "INSERT INTO trg VALUES (1)");
    assert!(out.error.is_some(), "INSERT into a trigger must error");
    let _ = std::fs::remove_file(path);
}

#[test]
fn drop_table_cascade_drops_its_triggers() {
    let path = unique_temp_path("trg-cascade-drop");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("t");
    engine
        .execute("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY)")
        .expect("audit");
    engine
        .execute(
            "CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO audit SELECT id FROM inserted",
        )
        .expect("trigger");
    engine.execute("DROP TABLE t").expect("drop t");
    // The trigger is gone (not orphaned): its name is free to reuse.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.objects WHERE name = 'trg'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "DROP TABLE must cascade-drop its triggers"
    );
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE TABLE trg (x INT)");
    assert!(
        out.error.is_none(),
        "the orphaned trigger name must be reusable: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn trigger_writes_are_fully_undone_after_a_crash() {
    let path = unique_temp_path("trg-crash-undo");
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
        "CREATE TABLE audit (id INT NOT NULL PRIMARY KEY)",
    );
    batch(
        &engine,
        &mut ctx,
        "CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO audit SELECT id FROM inserted",
    );

    // Session A: an explicit transaction whose INSERT fires the trigger
    // (writing `audit`), never committed. The firing row and the trigger's
    // write both stage on A's transaction.
    let mut ctx_a = TxnContext::default();
    batch(&engine, &mut ctx_a, "BEGIN TRAN; INSERT INTO t VALUES (99)");
    assert!(ctx_a.has_open_transaction());
    // A committed autocommit insert forces the WAL (including A's
    // uncommitted records) to disk.
    batch(
        &engine,
        &mut TxnContext::default(),
        "INSERT INTO audit VALUES (1)",
    );

    // Crash: no graceful rollback.
    drop(ctx_a);
    drop(engine);

    // Recovery undoes the loser A entirely — the whole statement, DML AND
    // its trigger's write, is atomic: t=99 is gone and the trigger's
    // audit=99 is gone; the separately-committed audit=1 survives.
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    let out_t = batch(
        &engine,
        &mut TxnContext::default(),
        "SELECT id FROM t ORDER BY id",
    );
    assert!(
        ids(&out_t).is_empty(),
        "the firing row must be undone after the crash"
    );
    let out_a = batch(
        &engine,
        &mut TxnContext::default(),
        "SELECT id FROM audit ORDER BY id",
    );
    assert_eq!(
        ids(&out_a),
        vec![1],
        "the trigger's write must be undone with the statement; the committed row survives"
    );
    let _ = std::fs::remove_file(path);
}
