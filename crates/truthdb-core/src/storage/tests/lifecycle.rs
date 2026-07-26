use super::*;

/// Offline growth (Stage 14): the data region extends, everything in it
/// survives, and the grown space is allocatable. Includes a second grow
/// (the re-run shape an interrupted grow needs).
#[test]
fn offline_grow_extends_the_data_region() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("grow");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, v NVARCHAR(MAX))",
        "INSERT INTO g VALUES (1, N'before growth')",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let old_pages = storage.lock().layout.data_size / PAGE_SIZE as u64;
    drop(ctx);
    drop(storage);

    let new_pages = Storage::grow_data_region(&path, 1).expect("grow");
    assert_eq!(
        new_pages,
        old_pages + (1u64 << 30) / PAGE_SIZE as u64,
        "one GiB of new data pages"
    );

    let storage = Storage::open(path.clone()).expect("reopen after grow");
    assert_eq!(
        storage.lock().layout.data_size / PAGE_SIZE as u64,
        new_pages
    );
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT v FROM g WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::NVarChar("before growth".into()));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let outcome = execute_batch(&storage, "INSERT INTO g VALUES (2, N'after')", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    drop(ctx);
    drop(storage);

    // Growing again works (the re-run an interrupted grow performs).
    let newer = Storage::grow_data_region(&path, 1).expect("second grow");
    assert_eq!(newer, new_pages + (1u64 << 30) / PAGE_SIZE as u64);
    let storage = Storage::open(path.clone()).expect("reopen after second grow");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT COUNT(*) FROM g", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The safe minimum: a delta below the tail span is refused with the
/// minimum named; nothing is touched.
#[test]
fn grow_refuses_below_the_safe_minimum() {
    let path = unique_temp_path("grow-min");
    // 4 GiB file: the tail regions span ~1.16 GiB, so +1 GiB is unsafe.
    let mut opts = test_storage_options();
    opts.size_gib = 4;
    let storage = Storage::create(path.clone(), opts).expect("create");
    drop(storage);
    let err = Storage::grow_data_region(&path, 1).expect_err("must refuse");
    assert!(
        err.to_string().contains("safe minimum"),
        "names the floor: {err}"
    );
    let storage = Storage::open(path.clone()).expect("file untouched");
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Growth with a pending WAL (a crash-interrupted transaction) is safe:
/// the WAL sits before the data region and replays against the moved
/// bitmap exactly as it would have.
#[test]
fn grow_with_pending_wal_recovers_cleanly() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("grow-wal");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO g VALUES (1, 10)",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    // An open transaction dies with the "crash" (drop without commit).
    let outcome = execute_batch(
        &storage,
        "BEGIN TRAN; UPDATE g SET v = 999 WHERE id = 1;",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    drop(ctx);
    drop(storage);

    Storage::grow_data_region(&path, 1).expect("grow with pending WAL");
    let storage = Storage::open(path.clone()).expect("recovery after grow");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT v FROM g WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::Int(10), "the loser is undone");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The advisory file lock fences grow against a running server: grow
/// refuses while the store is open, and works after it closes. (flock is
/// per open file description, so two opens in one process conflict too.)
#[test]
fn grow_refuses_while_the_store_is_open() {
    let path = unique_temp_path("grow-flock");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let err = Storage::grow_data_region(&path, 2).expect_err("must refuse while open");
    assert!(
        err.to_string()
            .contains("locked by another TruthDB process"),
        "names the lock: {err}"
    );
    drop(storage);
    Storage::grow_data_region(&path, 2).expect("grow after close");
    let storage = Storage::open(path.clone()).expect("reopen");
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// REVIEW POC: a crash after the extension writes but BEFORE the header
/// stamp leaves a longer file under the old header. The file must open
/// under the old layout, and a re-run of the grow must complete it.
/// Simulated by saving the original header page before the grow and
/// writing it back afterwards.
#[test]
fn grow_crash_before_header_stamp_is_recoverable() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("grow-crash");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO g VALUES (1, 42)",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    // A real checkpoint makes the descriptor and bitmap copies
    // load-bearing (fresh files have neither on disk).
    storage
        .write_checkpoint(b"crash-window-probe", 1, 2, 1)
        .expect("checkpoint");
    drop(ctx);
    drop(storage);

    // Save the pre-grow header page (the commit point the "crash" loses).
    let old_header = {
        let mut f = std::fs::File::open(&path).expect("open for header save");
        let mut buf = vec![0u8; FILE_HEADER_SIZE];
        f.read_exact(&mut buf).expect("read header");
        buf
    };

    let old_pages = {
        let storage = Storage::open(path.clone()).expect("preflight open");
        let pages = storage.lock().layout.data_size / PAGE_SIZE as u64;
        drop(storage);
        pages
    };

    Storage::grow_data_region(&path, 1).expect("grow");

    // Crash simulation: the extension writes are durable, the header
    // stamp is not.
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for header restore");
        f.seek(SeekFrom::Start(0)).expect("seek");
        f.write_all(&old_header).expect("restore old header");
        f.sync_all().expect("sync");
    }

    // The old layout must be fully valid: data readable, snapshot loads.
    let storage = Storage::open(path.clone()).expect("open under the OLD header");
    assert_eq!(
        storage.lock().layout.data_size / PAGE_SIZE as u64,
        old_pages,
        "still the old layout"
    );
    let snap = storage
        .load_snapshot()
        .expect("load snapshot under old header")
        .expect("snapshot present");
    assert_eq!(snap.data, b"crash-window-probe");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT v FROM g WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::Int(42));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    drop(ctx);
    drop(storage);

    // Re-running the grow completes it.
    let new_pages = Storage::grow_data_region(&path, 1).expect("re-run grow");
    assert_eq!(new_pages, old_pages + (1u64 << 30) / PAGE_SIZE as u64);
    let storage = Storage::open(path.clone()).expect("open after completed grow");
    assert_eq!(
        storage.lock().layout.data_size / PAGE_SIZE as u64,
        new_pages
    );
    let snap = storage
        .load_snapshot()
        .expect("load snapshot after grow")
        .expect("snapshot survived");
    assert_eq!(snap.data, b"crash-window-probe");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "INSERT INTO g VALUES (2, 43)", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// REVIEW POC (teeth): the checkpointed search snapshot must survive a
/// grow — the descriptor pages are the only pointer to it, and they move.
/// Fails if the grow skips the descriptor copy.
#[test]
fn grow_preserves_the_checkpointed_snapshot() {
    let path = unique_temp_path("grow-snap");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    storage
        .write_checkpoint(b"survives-the-move", 1, 2, 1)
        .expect("checkpoint");
    drop(storage);

    Storage::grow_data_region(&path, 1).expect("grow");

    let storage = Storage::open(path.clone()).expect("reopen");
    let snap = storage
        .load_snapshot()
        .expect("load")
        .expect("snapshot survived the grow");
    assert_eq!(snap.data, b"survives-the-move");
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// REVIEW POC (teeth): after a checkpoint the persisted bitmap is the
/// ONLY record of table extents (the WAL head has advanced past their
/// alloc records). If the grow loses the bitmap, a reopen sees every page
/// free and new allocations clobber existing tables. Fails if the grow
/// skips the bitmap copy.
#[test]
fn grow_preserves_the_persisted_allocator_bitmap() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("grow-bitmap");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v NVARCHAR(60))",
        "INSERT INTO t1 VALUES (1, N'pre-grow row')",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    drop(ctx);
    // The checkpoint persists the bitmap and advances the WAL head past
    // t1's extent-alloc records.
    storage
        .write_checkpoint(b"bitmap-probe", 1, 2, 1)
        .expect("checkpoint");
    drop(storage);

    Storage::grow_data_region(&path, 1).expect("grow");

    // Reopen and allocate heavily; with a lost bitmap these allocations
    // land on t1's pages.
    let storage = Storage::open(path.clone()).expect("reopen");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v NVARCHAR(200))",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    for batch in 0..20 {
        let mut sql = String::from("INSERT INTO t2 VALUES ");
        for i in 0..50 {
            if i > 0 {
                sql.push(',');
            }
            let id = batch * 50 + i;
            sql.push_str(&format!("({id}, N'filler row {id} {}')", "x".repeat(120)));
        }
        let outcome = execute_batch(&storage, &sql, &mut ctx);
        assert!(
            outcome.error.is_none(),
            "batch {batch}: {:?}",
            outcome.error
        );
    }
    let outcome = execute_batch(&storage, "SELECT v FROM t1 WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(
                rowset.rows[0][0],
                Datum::NVarChar("pre-grow row".into()),
                "t1 must not be clobbered by post-grow allocations"
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Publishing versions changes nothing about crash recovery: the store
/// (chains and commit table) is memory-only, so a kill-and-reopen with
/// RCSI on recovers exactly the committed state, options intact, chains
/// empty.
#[test]
fn rcsi_survives_a_crash_with_clean_recovery() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("rcsi-crash");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
        "UPDATE t SET v = 11 WHERE id = 1",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    // An open transaction with published versions dies with the crash.
    let outcome = execute_batch(
        &storage,
        "BEGIN TRAN; UPDATE t SET v = 999 WHERE id = 2;",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    // Kill without checkpoint: drop the handle mid-transaction.
    let _ = ctx;
    drop(storage);

    let storage = Storage::open(path.clone()).expect("recovery");
    assert!(storage.rcsi_enabled());
    assert_eq!(
        storage.version_chain_count("t"),
        0,
        "chains do not survive a restart"
    );
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT v FROM t ORDER BY id", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => assert_eq!(
            rows_i32(&rowset.rows),
            vec![11, 20],
            "committed wins, the in-flight update is undone"
        ),
        other => panic!("expected rows, got {other:?}"),
    }
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Row-lock escalation (Stage 12), pinned at its actual threshold: a
/// statement naming more than 1000 row keys takes ONE table lock instead
/// of flooding the lock table; at or below the threshold it takes row
/// locks. (The plan sketched ~5000; 1000 is the shipped value — this
/// test is the record of that divergence.)
#[test]
fn row_locks_escalate_past_the_threshold() {
    use crate::engine::{Isolation, TxnContext, execute_batch};
    use crate::lock::{LockMode, Resource};

    let path = unique_temp_path("escalation");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    let insert = |n: usize| {
        let tuples: Vec<String> = (0..n).map(|i| format!("({i}, 0)")).collect();
        format!("INSERT INTO t VALUES {}", tuples.join(", "))
    };

    // At the threshold: per-row locks (plus the table intent).
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        &insert(1000),
        Isolation::ReadCommitted,
    );
    let rows = needs
        .iter()
        .filter(|(r, _)| matches!(r, Resource::Row(_, _)))
        .count();
    assert_eq!(rows, 1000, "at the threshold every key gets a row lock");
    assert!(
        !needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive),
        "no table X below escalation: {:?}",
        needs.len()
    );

    // A single statement past the threshold: the per-statement cap
    // declines to enumerate 1001 row hashes and the INSERT falls back to
    // one table-exclusive lock. (Reachable since the node budget became
    // per-expression — a 1001-tuple INSERT parses now.)
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        &insert(1001),
        Isolation::ReadCommitted,
    );
    assert!(
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive),
        "a single over-threshold statement takes table X: {needs:?}"
    );
    assert_eq!(
        needs
            .iter()
            .filter(|(r, _)| matches!(r, Resource::Row(_, _)))
            .count(),
        0
    );

    // Past it — summed across the WHOLE BATCH: a 1000-tuple INSERT plus
    // 20 point DELETEs on DISTINCT keys wants 1020 row locks on one
    // table (the needs map dedups by key hash, so overlapping keys would
    // not count twice), and the batch-level pass replaces them all with
    // one table-exclusive lock.
    let deletes: Vec<String> = (2000..2020)
        .map(|i| format!("DELETE FROM t WHERE id = {i}"))
        .collect();
    let over = format!("{}; {}", insert(1000), deletes.join("; "));
    let needs = crate::engine::analyze_locks(
        &storage,
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        &over,
        Isolation::ReadCommitted,
    );
    assert!(
        needs
            .iter()
            .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive),
        "past the threshold the statement takes table X: {needs:?}"
    );
    assert_eq!(
        needs
            .iter()
            .filter(|(r, _)| matches!(r, Resource::Row(_, _)))
            .count(),
        0,
        "row locks are replaced, not added to"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// EXEC scope semantics, pinned: outer variables are restored after the
/// inner batch (a regression previously went green), and SET options
/// revert at scope exit as SQL Server reverts them — an inner
/// `SET XACT_ABORT ON` must not doom the outer batch's transaction.
#[test]
fn exec_scope_restores_variables_and_set_options() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("exec-scope");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // Inner @o shadows; the outer @o is intact after the EXEC returns.
    let outcome = execute_batch(
        &storage,
        "DECLARE @o INT = 1; EXEC sp_executesql N'DECLARE @o INT = 99; SELECT @o AS inner_o'; SELECT @o AS outer_o",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let values: Vec<i64> = outcome
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rs) => match rs.rows[0][0] {
                Datum::Int(v) => Some(v as i64),
                Datum::BigInt(v) => Some(v),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(values, [99, 1], "{:?}", outcome.results);

    // An inner SET XACT_ABORT ON reverts at EXEC exit: the later duplicate
    // key fails its own statement but the batch continues and commits.
    let outcome = execute_batch(
        &storage,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (1); \
         EXEC sp_executesql N'SET XACT_ABORT ON'; \
         INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT",
        &mut ctx,
    );
    assert!(outcome.error.is_some(), "the dup insert reports its error");
    let outcome = execute_batch(&storage, "SELECT COUNT(*) FROM t", &mut ctx);
    match &outcome.results[0] {
        StatementResult::Rows(rs) => assert_eq!(
            rs.rows[0][0],
            Datum::BigInt(2),
            "XACT_ABORT must revert at scope exit; the transaction commits"
        ),
        other => panic!("expected rows, got {other:?}"),
    }

    drop(storage);
    let _ = std::fs::remove_file(&path);
}
