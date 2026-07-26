use super::*;

/// ALTER TABLE ADD survives a restart — the widened schema and the frozen
/// fills are one durable statement — and a failure mid-rewrite rolls the
/// whole ALTER back: old schema, old rows, fully readable.
#[test]
fn alter_add_column_is_durable_and_atomic() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("alter-add");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))",
        "INSERT INTO t VALUES (1, 'one'), (2, 'two')",
        "ALTER TABLE t ADD score INT DEFAULT 7",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    drop(storage);

    // Reopen: the widened schema and the frozen fill survived.
    let storage = Storage::open(path.clone()).expect("reopen");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT id, score FROM t ORDER BY id", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => assert_eq!(
            rowset.rows,
            vec![
                vec![Datum::Int(1), Datum::Int(7)],
                vec![Datum::Int(2), Datum::Int(7)],
            ]
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    // A failure mid-rewrite (fault injection inside the ALTER's statement)
    // rolls the whole thing back: the new column does not exist and the
    // old rows are intact.
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(Some(1)));
    let outcome = execute_batch(&storage, "ALTER TABLE t ADD flag BIT DEFAULT 1", &mut ctx);
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(None));
    assert!(outcome.error.is_some(), "the injected failure must surface");

    let outcome = execute_batch(&storage, "SELECT flag FROM t", &mut ctx);
    assert!(
        outcome.error.is_some(),
        "the rolled-back column must not exist: {:?}",
        outcome.results
    );
    let outcome = execute_batch(&storage, "SELECT id, score FROM t ORDER BY id", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => assert_eq!(rowset.rows.len(), 2),
        other => panic!("expected rows, got {other:?}"),
    }

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The EXEC text path's two load-bearing lock properties, pinned: a
/// variable @stmt cannot be analyzed up front and takes the conservative
/// database-exclusive lock, and isolation escalation crosses the EXEC
/// boundary in both directions (mutating either previously went green).
#[test]
fn exec_lock_analysis_never_under_locks() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("exec-locks");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // A variable statement text is unknowable before it runs: the batch
    // locks the database exclusively rather than ever under-locking.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DECLARE @s NVARCHAR(50) = N'SELECT v FROM t'; EXEC sp_executesql @s",
        Isolation::ReadCommitted,
    );
    assert!(
        needs.contains(&(Resource::Database, LockMode::Exclusive)),
        "variable @stmt must take Database X: {needs:?}"
    );

    // Direction 1: a SET raise BEFORE the EXEC locks the inner reads.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; EXEC sp_executesql N'SELECT v FROM t'",
        Isolation::ReadUncommitted,
    );
    assert!(
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
        "escalated isolation must lock the inner SELECT: {needs:?}"
    );

    // Direction 2 (intra-EXEC): a SET raise INSIDE the literal locks the
    // statements after it inside the same literal.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC sp_executesql N'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT v FROM t'",
        Isolation::ReadUncommitted,
    );
    assert!(
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
        "an inner SET raise must lock the inner reads: {needs:?}"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The Stage 13 database options round-trip the superblock: they survive
/// a reopen, and a checkpoint — which rebuilds both superblocks from
/// scratch — must carry them forward rather than silently resetting them.
#[test]
fn db_options_persist_across_reopen_and_checkpoint() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("db-options");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    assert!(!storage.rcsi_enabled());
    assert!(!storage.snapshot_isolation_allowed());
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON, ALLOW_SNAPSHOT_ISOLATION ON",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert!(storage.rcsi_enabled());
    assert!(storage.snapshot_isolation_allowed());
    drop(storage);

    let storage = Storage::open(path.clone()).expect("reopen");
    assert!(storage.rcsi_enabled(), "RCSI survives a restart");
    assert!(storage.snapshot_isolation_allowed());

    // One option off, then a checkpoint, then a reopen: the checkpoint's
    // fresh superblocks must keep the surviving option.
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    storage
        .write_checkpoint(b"cp", 1, 2, 1)
        .expect("checkpoint");
    drop(storage);
    let storage = Storage::open(path.clone()).expect("reopen after checkpoint");
    assert!(
        storage.rcsi_enabled(),
        "the checkpoint must not reset the option"
    );
    assert!(!storage.snapshot_isolation_allowed());
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// A READ COMMITTED SELECT under RCSI takes only Database IS — no Table
/// S — which is the entire readers-don't-block mechanism; and the other
/// levels are untouched by the option.
/// Lock analysis descends control-flow bodies: a WHILE body's INSERT and
/// an IF condition's EXISTS table are in the batch's up-front lock set.
/// EXEC of a user procedure locks the STORED BODY's tables up front —
/// parsed with the in-procedure grammar (a plain parse would 178 on
/// `RETURN <value>`, yield no locks, and the body would run unlocked).
/// Recursive procedures terminate analysis via the visited set.
#[test]
fn analyze_locks_covers_procedure_bodies() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("proc-locks");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE plt (id INT NOT NULL PRIMARY KEY)",
        "CREATE PROCEDURE writer @v INT AS INSERT INTO plt VALUES (@v); \
         EXEC writer @v; RETURN 5",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC writer 1",
        Isolation::ReadCommitted,
    );
    assert!(
        needs.iter().any(
            |(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive
                || matches!(r, Resource::Row(..))
        ),
        "the recursive body's INSERT locks its table (and analysis \
         terminated): {needs:?}"
    );
    // An unknown procedure contributes no locks (2812 at execution).
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC no_such_proc",
        Isolation::ReadCommitted,
    );
    assert!(
        !needs
            .iter()
            .any(|(r, _)| matches!(r, Resource::Table(_) | Resource::Row(..))),
        "unknown proc: {needs:?}"
    );
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Adversarial review probe: the visited set dedups a procedure's lock
/// contribution, but a body's lock set DEPENDS on the effective
/// isolation. Under RCSI, `EXEC pread` analyzed first contributes only
/// Database IS (versioned read); a later `EXEC pser` — whose body raises
/// to SERIALIZABLE and EXECs pread — finds pread already visited and
/// skips it, so the lock-based re-analysis (Table S) is dropped. At
/// execution the SET is live inside pser and pread's SELECT reads
/// lock-based with no Table S held: the 2PL under-lock class.
#[test]
fn review_poc_analyze_locks_procedure_reanalyzed_under_escalated_isolation() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("proc-visited-isolation");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE PROCEDURE pread AS SELECT v FROM t",
        "CREATE PROCEDURE pser AS \
         SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; EXEC pread",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let table_s = |needs: &[(Resource, LockMode)]| {
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared)
    };
    // Control: analyzed alone, the escalated body read-locks.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC pser",
        Isolation::ReadCommitted,
    );
    assert!(
        table_s(&needs),
        "control: pser's escalated body takes Table S: {needs:?}"
    );
    // The seam: pread analyzed first under the versioned regime, then
    // pser's escalated re-entry is dropped by the visited set.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC pread; EXEC pser",
        Isolation::ReadCommitted,
    );
    assert!(
        table_s(&needs),
        "pser still runs pread's SELECT under SERIALIZABLE — Table S \
         must be in the up-front set: {needs:?}"
    );
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Adversarial review probe: a stored procedure EXEC'd from INSIDE a
/// dynamic-SQL literal is still resolved by lock analysis (the literal
/// recursion's Exec arm hits the procedure branch).
#[test]
fn review_poc_analyze_locks_procedure_via_dynamic_sql() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("proc-dyn-locks");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        "CREATE PROCEDURE wtr @v INT AS INSERT INTO t VALUES (@v)",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC sp_executesql N'EXEC wtr 1'",
        Isolation::ReadCommitted,
    );
    assert!(
        needs.iter().any(|(r, m)| matches!(r, Resource::Table(_))
            && matches!(m, LockMode::IntentExclusive | LockMode::Exclusive)
            || matches!(r, Resource::Row(..))),
        "the proc body's INSERT is in the up-front set via the literal \
         path: {needs:?}"
    );
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Adversarial review probe: analysis resolves the catalog FIRST, but
/// execution checks the sp_executesql builtin FIRST. A user procedure
/// named sp_executesql makes the two disagree: analysis analyzes the
/// user body, execution runs the builtin over the literal — whose locks
/// were never analyzed (under-lock). Either the CREATE must be refused
/// or the analysis must mirror execution's builtin-first order.
#[test]
fn review_poc_user_procedure_named_sp_executesql_cannot_shadow_builtin() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("proc-spexec-shadow");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let outcome = execute_batch(
        &storage,
        "CREATE PROCEDURE sp_executesql AS SELECT 1 AS n",
        &mut ctx,
    );
    if outcome.error.is_none() {
        // The shadow exists: analysis and execution must still agree.
        // Execution runs the BUILTIN (its name check comes first), so the
        // literal's INSERT locks must be in the analyzed set.
        let needs = crate::engine::analyze_locks(
            &storage,
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "EXEC sp_executesql N'INSERT INTO t VALUES (1)'",
            Isolation::ReadCommitted,
        );
        assert!(
            needs.iter().any(|(r, m)| matches!(r, Resource::Table(_))
                && matches!(m, LockMode::IntentExclusive | LockMode::Exclusive)
                || matches!(r, Resource::Row(..))
                || (matches!(r, Resource::Database) && *m == LockMode::Exclusive)),
            "execution runs the builtin INSERT; analysis followed the \
             user proc's body instead: {needs:?}"
        );
    }
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn analyze_locks_descends_control_flow() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("flow-locks");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE locked_t (id INT NOT NULL PRIMARY KEY)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DECLARE @i INT = 0; WHILE @i < 3 BEGIN INSERT INTO locked_t VALUES (@i); \
         SET @i = @i + 1; END",
        Isolation::ReadCommitted,
    );
    assert!(
        needs.iter().any(
            |(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive
                || matches!(r, Resource::Row(..))
        ),
        "the WHILE body's INSERT locks its table: {needs:?}"
    );

    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "IF EXISTS (SELECT * FROM locked_t) SELECT 1",
        Isolation::ReadCommitted,
    );
    assert!(
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
        "the IF condition's EXISTS table takes Table S: {needs:?}"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Adversarial probes (control-flow review): condition shapes whose table
/// reads must be in the up-front lock set — a WHILE condition, a derived
/// table and an IN-subquery inside the condition, a CASE-wrapped EXISTS,
/// a view, an untaken ELSE branch's write, and an EXEC literal inside a
/// WHILE body.
#[test]
fn cf_review_analyze_locks_condition_shapes() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("cf-flow-lock-shapes");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE lt (id INT NOT NULL PRIMARY KEY)",
        "CREATE VIEW lv AS SELECT id FROM lt",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let table_s = |needs: &[(Resource, LockMode)]| {
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared)
    };
    let table_write = |needs: &[(Resource, LockMode)]| {
        needs.iter().any(|(r, m)| {
            matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive
                || matches!(r, Resource::Row(..))
        })
    };
    for sql in [
        "WHILE EXISTS (SELECT * FROM lt) SELECT 1",
        "IF EXISTS (SELECT * FROM (SELECT id FROM lt) d) SELECT 1",
        "IF 1 IN (SELECT id FROM lt) SELECT 1",
        "IF CASE WHEN EXISTS (SELECT * FROM lt) THEN 1 ELSE 0 END = 1 SELECT 1",
        "IF EXISTS (SELECT * FROM lv) SELECT 1",
        "IF (SELECT COUNT(*) FROM lt) = 0 SELECT 1",
    ] {
        let needs = crate::engine::analyze_locks(
            &storage,
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            sql,
            Isolation::ReadCommitted,
        );
        assert!(
            table_s(&needs),
            "{sql}: condition read takes Table S: {needs:?}"
        );
    }
    // Both IF branches analyze — an untaken ELSE's INSERT is still locked.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "IF 1 = 2 SELECT 1 ELSE INSERT INTO lt VALUES (9)",
        Isolation::ReadCommitted,
    );
    assert!(
        table_write(&needs),
        "the ELSE branch's INSERT locks its table: {needs:?}"
    );
    // An EXEC literal inside a WHILE body analyzes through the Exec arm.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "DECLARE @i INT = 0; WHILE @i < 1 BEGIN \
         EXEC sp_executesql N'INSERT INTO lt VALUES (7)'; SET @i = @i + 1; END",
        Isolation::ReadCommitted,
    );
    assert!(
        table_write(&needs),
        "the EXEC'd INSERT inside the loop locks its table: {needs:?}"
    );
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// A CTE inside an IF condition's subquery: the executor inlines it and
/// reads the base table (engine.rs pins that half), so analysis must lock
/// that table like the Select arm does — the expectation here is the
/// FIXED behavior.
#[test]
fn cf_review_analyze_locks_condition_cte() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("cf-flow-lock-cte");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE lt (id INT NOT NULL PRIMARY KEY)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "IF EXISTS (WITH x AS (SELECT id FROM lt) SELECT id FROM x) SELECT 1",
        Isolation::ReadCommitted,
    );
    assert!(
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
        "the CTE's base table is read at runtime and must be locked: {needs:?}"
    );
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn analyze_locks_drops_table_s_under_rcsi() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("rcsi-locks");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    let table_s = |needs: &[(Resource, LockMode)]| {
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared)
    };

    // Off: the SELECT read-locks, as ever.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT v FROM t",
        Isolation::ReadCommitted,
    );
    assert!(
        table_s(&needs),
        "without RCSI a RC SELECT takes Table S: {needs:?}"
    );

    let outcome = execute_batch(
        &storage,
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // On: Database IS only — the DDL fence — and no Table S.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT v FROM t",
        Isolation::ReadCommitted,
    );
    assert!(
        !table_s(&needs),
        "under RCSI a RC SELECT takes no Table S: {needs:?}"
    );
    assert!(
        needs.contains(&(Resource::Database, LockMode::IntentShared)),
        "the Database IS fence stays: {needs:?}"
    );

    // The other levels are untouched: RR still read-locks, RU still
    // takes nothing, and a batch that raises isolation falls back to
    // locking even though the session level is RC.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT v FROM t",
        Isolation::RepeatableRead,
    );
    assert!(table_s(&needs), "RR is not versioned: {needs:?}");
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT v FROM t",
        Isolation::ReadUncommitted,
    );
    assert!(needs.is_empty(), "RU takes no locks at all: {needs:?}");
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT v FROM t",
        Isolation::ReadCommitted,
    );
    assert!(
        table_s(&needs),
        "a raising SET disables the snapshot path: {needs:?}"
    );

    // SNAPSHOT isolation is versioned regardless of RCSI: Database IS
    // only, and the EXEC-literal recursion preserves the level (the
    // #120 review's collapse bug, from the other direction).
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SELECT v FROM t",
        Isolation::Snapshot,
    );
    assert!(
        !table_s(&needs),
        "SNAPSHOT reads take no Table S: {needs:?}"
    );
    assert!(needs.contains(&(Resource::Database, LockMode::IntentShared)));
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC sp_executesql N'SELECT v FROM t'",
        Isolation::Snapshot,
    );
    assert!(
        !table_s(&needs),
        "the recursion must not turn SNAPSHOT into a locking level: {needs:?}"
    );
    // ...and a SET SNAPSHOT inside a batch is not a lock-escalating
    // raise, but the batch still holds the Database IS fence.
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT v FROM t",
        Isolation::ReadUncommitted,
    );
    assert!(
        needs.contains(&(Resource::Database, LockMode::IntentShared)),
        "SET SNAPSHOT from RU keeps the DDL fence: {needs:?}"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}
