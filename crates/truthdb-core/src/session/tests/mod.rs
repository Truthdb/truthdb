use super::*;
use crate::engine::Engine;
use crate::engine::StatementResult;
use crate::relstore::types::Datum;
use crate::storage::{Storage, StorageOptions};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

fn graph(edges: &[(u64, &[u64])]) -> HashMap<u64, HashSet<u64>> {
    edges
        .iter()
        .map(|(from, to)| (*from, to.iter().copied().collect()))
        .collect()
}

fn test_storage_options() -> StorageOptions {
    StorageOptions {
        size_gib: 1,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
        default_collation: None,
    }
}

fn unique_temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    path.push(format!("truthdb-lock-{label}-{nanos}.db"));
    path
}

/// A running engine plus the temp file backing it (removed on drop).
struct Harness {
    handle: EngineHandle,
    path: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.handle.shutdown();
        let _ = std::fs::remove_file(&self.path);
    }
}

fn start(timeout: Duration) -> Harness {
    let path = unique_temp_path("engine");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let engine = Engine::new(storage).expect("engine");
    let (handle, _join) = spawn_engine_with_timeout(engine, timeout);
    Harness { handle, path }
}

/// A single-worker harness. With one worker the loop's wait is the only
/// thing deciding when the sweep runs; with several, a sibling that
/// snapshotted an earlier deadline can wake and sweep on another's behalf,
/// masking a wait that is too long.
fn start_single_worker(idle: Option<Duration>) -> Harness {
    let path = unique_temp_path("engine-1worker");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let engine = Engine::new(storage).expect("engine");
    let (handle, _join) = spawn_engine_pool(engine, LOCK_WAIT_TIMEOUT, idle, 1);
    Harness { handle, path }
}

/// A harness whose idle-transaction reaper fires after `idle` (or never,
/// when `None`), so the reaper is testable without a real 10 min wait.
fn start_with_idle(idle: Option<Duration>) -> Harness {
    let path = unique_temp_path("engine-idle");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let engine = Engine::new(storage).expect("engine");
    let (handle, _join) = spawn_engine_with_idle_timeout(engine, idle);
    Harness { handle, path }
}

/// The `id` column (column 0) of the first rowset, as i64s.
fn ids(reply: &BatchReply) -> Vec<i64> {
    for result in &reply.outcome.results {
        if let StatementResult::Rows(rowset) = result {
            return rowset
                .rows
                .iter()
                .map(|row| match row[0] {
                    Datum::TinyInt(v) => v as i64,
                    Datum::SmallInt(v) => v as i64,
                    Datum::Int(v) => v as i64,
                    Datum::BigInt(v) => v,
                    ref other => panic!("expected integer id, got {other:?}"),
                })
                .collect();
        }
    }
    panic!("no rowset in outcome");
}

fn error_number(reply: &BatchReply) -> Option<i32> {
    reply.outcome.error.as_ref().map(|e| e.number)
}

// ---- idle-transaction reaper ----------------------------------------

/// Drains a batch's event stream through its terminal event.
async fn drain_events(mut rx: mpsc::UnboundedReceiver<BatchEvent>) -> Vec<BatchEvent> {
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        let terminal = matches!(event, BatchEvent::Complete { .. } | BatchEvent::Failed(_));
        events.push(event);
        if terminal {
            break;
        }
    }
    events
}

fn no_cancel() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// The `in_transaction` flag of every StatementDone, in stream order.
fn done_flags(events: &[BatchEvent]) -> Vec<bool> {
    events
        .iter()
        .filter_map(|event| match event {
            BatchEvent::StatementDone { in_transaction, .. } => Some(*in_transaction),
            _ => None,
        })
        .collect()
}

/// Fills `t` with `1..=n` single-column PK rows.
async fn fill(h: &Harness, s: SessionId, n: usize) {
    h.handle
        .run_batch(s, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    // Batched into 500-tuple inserts: the per-expression node budget
    // keeps giant single statements bounded, and chunks stay far from
    // every limit.
    let ids: Vec<usize> = (1..=n).collect();
    for chunk in ids.chunks(500) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i})")).collect();
        let reply = h
            .handle
            .run_batch(s, format!("INSERT INTO t VALUES {}", values.join(", ")))
            .await
            .unwrap();
        assert!(reply.outcome.error.is_none(), "{:?}", reply.outcome.error);
    }
}

/// A bare Scheduler + Engine, for pinning the sweep's guards directly
/// instead of racing real timers.
fn bare(label: &str, idle: Option<Duration>) -> (Engine, Scheduler, PathBuf) {
    let path = unique_temp_path(label);
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let engine = Engine::new(storage).expect("engine");
    (engine, Scheduler::new(LOCK_WAIT_TIMEOUT, idle), path)
}

/// Adversarial review probe: ALTER PROCEDURE bumps the lock-analysis
/// epoch, so a batch parked with a lock set analyzed over the OLD body is
/// re-analyzed at grant time against the NEW body — otherwise the new
/// body's writes would run under the old body's read locks (the stale
/// twice-derived-decision class from the Stage 13 review).

/// The worker pool's core correctness stress test: many sessions run money
/// transfers concurrently, some rolled back at random, and the total across
/// all accounts must be exactly conserved — no lost updates, no torn
/// transactions, no money created or destroyed by the concurrent plumbing
/// (take/return of session context, parking, waking, draining).

/// Like the conservation test, but each transfer is a *multi-batch*
/// transaction across two tables, so locks are taken incrementally and two
/// transfers in opposite table order can genuinely deadlock. A 1205 victim
/// rolls back and retries. Exercises cross-batch lock holding, the deadlock
/// detector, and victim retry all under concurrent load — the total must
/// still be conserved.

/// A single waiter blocked on a legitimately-held lock (no cycle, so the
/// graph detector finds nothing) must still be freed by the lock-wait
/// timeout even when the pool then goes completely quiet — no further call
/// arrives to wake a worker. Regression: a stale `earliest_deadline`
/// snapshot used to let a worker block in an untimed `recv` holding the
/// single-consumer rx mutex, disabling the reaper during quiescence and
/// hanging the waiter indefinitely instead of timing it out.

/// Stage 12's exit case: the engine stays responsive during a large
/// scan. A protocol heartbeat never touches the engine (the dispatcher
/// answers it), so the meaningful half is a native search completing
/// WHILE a worker is mid-scan holding the batch-long read gate — pinned
/// by synchronizing on the scan's first Columns event (the worker is
/// provably inside the batch) and asserting the search returns before
/// the scan finishes. A regression to an exclusive gate blocks the
/// search behind the whole scan.

// ---- sp_prepare handle family -----------------------------------------

/// The PreparedHandle event's value, if the stream carried one.
fn handle_of(events: &[BatchEvent]) -> Option<i32> {
    events.iter().find_map(|event| match event {
        BatchEvent::PreparedHandle(h) => Some(*h),
        _ => None,
    })
}

/// The first Error event's number, if any.
fn event_error(events: &[BatchEvent]) -> Option<i32> {
    events.iter().find_map(|event| match event {
        BatchEvent::Error(e) => Some(e.number),
        _ => None,
    })
}

fn int_param(value: i32) -> crate::engine::RpcParam {
    crate::engine::RpcParam {
        name: String::new(),
        column_type: crate::relstore::types::ColumnType::Int,
        value: Datum::Int(value),
    }
}

/// All streamed rows, flattened.
fn rows_of(events: &[BatchEvent]) -> Vec<Vec<Datum>> {
    events
        .iter()
        .filter_map(|event| match event {
            BatchEvent::Rows(rows) => Some(rows.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

// ---- READ_COMMITTED_SNAPSHOT (Stage 13) ------------------------------

/// A harness with RCSI on and `t (id PK, v)` seeded with (1,10), (2,20).
async fn rcsi_harness() -> (Harness, SessionId, SessionId) {
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    ] {
        let reply = h.handle.run_batch(a, sql.into()).await.unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "{sql}: {:?}",
            reply.outcome.error
        );
    }
    (h, a, b)
}

/// Runs a batch under a 2 s timeout: an RCSI reader must never park
/// behind a writer, so a hang here is the failure being tested for.
async fn must_not_block(h: &Harness, s: SessionId, sql: &str) -> BatchReply {
    tokio::time::timeout(Duration::from_secs(2), h.handle.run_batch(s, sql.into()))
        .await
        .expect("an RCSI read must not block on a writer")
        .unwrap()
}

/// Column 0 of the first rowset as i64s, asserting the batch succeeded.
fn values(reply: &BatchReply) -> Vec<i64> {
    assert_eq!(error_number(reply), None, "{:?}", reply.outcome.error);
    ids(reply)
}

// The next six tests came out of the adversarial review: the first two
// pin its confirmed findings (both reproduced as dirty reads before the
// fixes), the rest are its passing probes kept as regressions.

// ---- SNAPSHOT isolation (Stage 13) -----------------------------------

/// ALLOW_SNAPSHOT_ISOLATION on (RCSI deliberately off — SNAPSHOT must
/// stand alone), `t (id PK, v)` = (1,10), (2,20); session B is SNAPSHOT.
async fn si_harness() -> (Harness, SessionId, SessionId) {
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    for sql in [
        "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    ] {
        let reply = h.handle.run_batch(a, sql.into()).await.unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "{sql}: {:?}",
            reply.outcome.error
        );
    }
    h.handle
        .run_batch(b, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT".into())
        .await
        .unwrap();
    (h, a, b)
}

// ---- SI review PoC tests ---------------------------------------------

mod locking;
mod maintenance;
mod prepared;
mod rcsi;
mod snapshot;
/// Attack A: a row re-keyed by a snapshot-invisible writer. Targeting the
/// snapshot row by its OLD key must be 3960 (its current state was
/// produced by an invisible writer); targeting the NEW key must affect
/// nothing (the snapshot has no such row) and must not touch the current
/// row.

/// Attack E: an index created AFTER the snapshot only contains
/// post-snapshot states. A seek through it must still produce exactly the
/// snapshot's rows: old values found via chain images, new values
/// filtered out.

/// Attack C: EXEC of a literal shares the transaction's snapshot - the
/// inner statement both CAPTURES it (first access inside an EXEC) and
/// REUSES it, and 3952 fires at the inner statement's access when the
/// option is off.

/// Attack C/D: without ALLOW_SNAPSHOT_ISOLATION, the 3952 fires at the
/// INNER statement of an EXEC too, and dooms the open transaction.

/// Attack B / leak audit: nested BEGIN and a savepoint rollback keep the
/// transaction - and must keep its snapshot.

/// Attack E (option toggle): ALTER DATABASE SET ALLOW_SNAPSHOT_ISOLATION
/// OFF while a SNAPSHOT transaction holds a registered snapshot between
/// batches (it holds no locks then, so Database X does not exclude it).
/// Expected correct behavior: the toggle succeeds (or waits), the
/// snapshot transaction gets 3952 at its next access, and the session
/// recovers. Today the version store's reset debug_asserts that no
/// snapshot is live - which is false here.

/// A statement-scoped (autocommit) 3952 does not wedge the session.

/// 3961 completeness: DROP TABLE + CREATE TABLE of the same name between
/// a SNAPSHOT transaction's batches. SQL Server raises 3961 (metadata is
/// not versioned); at minimum the reader must not silently see the NEW
/// table's contents as if they were its snapshot.

/// The heap arm of the SI DML target scan: conflicts through synthesized
/// RID locators (no primary key anywhere).

/// Full-table DML under SI: DELETE with no WHERE keeps every snapshot
/// row; if ANY of them was changed since the snapshot, 3960.
mod streaming;
