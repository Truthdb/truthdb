//! The engine actor: a dedicated OS thread owns the [`Engine`] and a
//! [`SessionManager`], and serves [`EngineCall`]s over a channel. This replaces
//! the shared `Arc<Mutex<Engine>>` — it serializes engine access (as the mutex
//! did), carries per-connection sessions (transaction/isolation state) and
//! moves the engine's synchronous io_uring work off the async reactor onto its
//! own thread.
//!
//! ## Locking without blocking the actor
//!
//! The actor runs one call at a time, so it must never block in place waiting
//! for a lock — the lock's holder could only release by having its own call
//! processed, which the blocked thread would prevent (self-deadlock). Instead
//! a batch acquires *all* the table/database locks it needs up front (see
//! [`crate::rel::analyze_locks`]) before running any statement. If one
//! conflicts, the whole [`EngineCall::RunBatch`] is *parked* — its reply
//! deferred — and the actor moves on. Releasing locks (commit / rollback /
//! disconnect) wakes parked batches in FIFO order and re-attempts them; since
//! a parked batch never ran, restarting it is exact. A 5 s deadline reaps a
//! batch stuck behind a deadlock, rolling it back as the victim (error 1205).

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use truthdb_sql::error::SqlError;

use crate::engine::{Engine, EngineError};
use crate::lock::{LockManager, LockMode, Resource};
use crate::rel::{BatchOutcome, Isolation, TxnContext};

/// How long a batch may wait on a lock before it is treated as a deadlock
/// victim and rolled back (SQL Server-style, plan: "5 s wait timeout →
/// abort youngest").
const LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// The result of running a batch for a session: its typed outcome plus
/// whether the connection is still inside a transaction afterwards (so the
/// TDS gateway can set `DONE_INXACT`).
pub struct BatchReply {
    pub outcome: BatchOutcome,
    pub in_transaction: bool,
}

/// Identifies a connection's session on the engine thread.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(u64);

impl SessionId {
    /// The raw id used as the lock-manager owner key.
    fn raw(self) -> u64 {
        self.0
    }
}

/// Per-connection engine-side state: the transaction context carried across a
/// connection's batches (open transaction, `@@TRANCOUNT`, isolation, SET
/// options).
#[derive(Default)]
struct Session {
    txn_ctx: TxnContext,
}

struct SessionManager {
    sessions: HashMap<SessionId, Session>,
    next_id: u64,
}

impl SessionManager {
    fn new() -> Self {
        SessionManager {
            sessions: HashMap::new(),
            next_id: 1,
        }
    }

    fn open(&mut self) -> SessionId {
        let id = SessionId(self.next_id);
        self.next_id += 1;
        self.sessions.insert(id, Session::default());
        id
    }

    fn close(&mut self, id: SessionId) -> Option<Session> {
        self.sessions.remove(&id)
    }

    fn get(&self, id: SessionId) -> Option<&Session> {
        self.sessions.get(&id)
    }

    fn get_mut(&mut self, id: SessionId) -> Option<&mut Session> {
        self.sessions.get_mut(&id)
    }
}

/// A message to the engine thread. Each carries a one-shot reply channel the
/// async caller awaits.
enum EngineCall {
    OpenSession {
        reply: oneshot::Sender<SessionId>,
    },
    /// A SQL batch on behalf of a session (TDS path): typed results.
    RunBatch {
        session: SessionId,
        sql: String,
        reply: oneshot::Sender<Result<BatchReply, EngineError>>,
    },
    /// A native-protocol command (ES or SQL): rendered text.
    RunNative {
        command: String,
        reply: oneshot::Sender<Result<String, EngineError>>,
    },
    CloseSession {
        session: SessionId,
    },
    Shutdown,
}

/// A cloneable handle to the engine thread. Cheap to clone (shares the sender).
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineCall>,
}

impl EngineHandle {
    /// Opens a session; returns its id (or a placeholder if the engine is gone).
    pub async fn open_session(&self) -> SessionId {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(EngineCall::OpenSession { reply }).is_err() {
            return SessionId(0);
        }
        rx.await.unwrap_or(SessionId(0))
    }

    /// Runs a SQL batch for a session and returns its typed outcome plus the
    /// connection's post-batch transaction state.
    pub async fn run_batch(
        &self,
        session: SessionId,
        sql: String,
    ) -> Result<BatchReply, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(EngineCall::RunBatch {
                session,
                sql,
                reply,
            })
            .map_err(|_| EngineError::Unavailable)?;
        rx.await.map_err(|_| EngineError::Unavailable)?
    }

    /// Runs a native-protocol command (ES or SQL) and returns rendered text.
    pub async fn run_native(&self, command: String) -> Result<String, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(EngineCall::RunNative { command, reply })
            .map_err(|_| EngineError::Unavailable)?;
        rx.await.map_err(|_| EngineError::Unavailable)?
    }

    /// Closes a session (rolling back any open transaction — later milestone).
    pub fn close_session(&self, session: SessionId) {
        let _ = self.tx.send(EngineCall::CloseSession { session });
    }

    /// Asks the engine thread to stop after draining queued calls.
    pub fn shutdown(&self) {
        let _ = self.tx.send(EngineCall::Shutdown);
    }
}

/// Spawns the engine thread and returns a handle plus its join handle.
pub fn spawn_engine(engine: Engine) -> (EngineHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let join = std::thread::Builder::new()
        .name("truthdb-engine".to_string())
        .spawn(move || EngineLoop::new(engine).run(rx))
        .expect("spawn engine thread");
    (EngineHandle { tx }, join)
}

/// Like [`spawn_engine`] but with a custom lock-wait timeout, so tests can
/// exercise the deadlock reaper without a real 5 s wait.
#[cfg(test)]
fn spawn_engine_with_timeout(engine: Engine, timeout: Duration) -> (EngineHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let join = std::thread::Builder::new()
        .name("truthdb-engine-test".to_string())
        .spawn(move || {
            let mut engine_loop = EngineLoop::new(engine);
            engine_loop.lock_wait_timeout = timeout;
            engine_loop.run(rx);
        })
        .expect("spawn engine thread");
    (EngineHandle { tx }, join)
}

/// A SQL batch waiting for locks: its request, the locks it needs, and the
/// deadline past which it is treated as a deadlock victim.
struct Parked {
    session: SessionId,
    sql: String,
    reply: oneshot::Sender<Result<BatchReply, EngineError>>,
    needs: Vec<(Resource, LockMode)>,
    deadline: Instant,
}

/// The engine actor's mutable world: the engine, its sessions, the lock
/// manager, and the FIFO queue of batches parked on locks.
struct EngineLoop {
    engine: Engine,
    sessions: SessionManager,
    locks: LockManager,
    parked: VecDeque<Parked>,
    lock_wait_timeout: Duration,
}

impl EngineLoop {
    fn new(engine: Engine) -> Self {
        EngineLoop {
            engine,
            sessions: SessionManager::new(),
            locks: LockManager::new(),
            parked: VecDeque::new(),
            lock_wait_timeout: LOCK_WAIT_TIMEOUT,
        }
    }

    fn run(mut self, rx: mpsc::Receiver<EngineCall>) {
        loop {
            // Wake at the earliest parked deadline so deadlocked waiters are
            // reaped even if no new call arrives.
            let call = match self.earliest_deadline() {
                Some(deadline) => {
                    let wait = deadline.saturating_duration_since(Instant::now());
                    match rx.recv_timeout(wait) {
                        Ok(call) => Some(call),
                        Err(mpsc::RecvTimeoutError::Timeout) => None,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                None => match rx.recv() {
                    Ok(call) => Some(call),
                    Err(_) => break,
                },
            };
            self.reap_expired();
            match call {
                Some(EngineCall::OpenSession { reply }) => {
                    let _ = reply.send(self.sessions.open());
                }
                Some(EngineCall::RunBatch {
                    session,
                    sql,
                    reply,
                }) => self.dispatch_batch(session, sql, reply),
                Some(EngineCall::RunNative { command, reply }) => {
                    let _ = reply.send(self.engine.execute(&command));
                }
                Some(EngineCall::CloseSession { session }) => self.close_session(session),
                Some(EngineCall::Shutdown) => break,
                None => {}
            }
        }
        // Draining: fail any batches still parked so their callers unblock.
        for parked in self.parked.drain(..) {
            let _ = parked.reply.send(Err(EngineError::Unavailable));
        }
    }

    fn earliest_deadline(&self) -> Option<Instant> {
        self.parked.iter().map(|p| p.deadline).min()
    }

    /// Acquires a batch's locks and runs it, or parks it behind a conflict.
    fn dispatch_batch(
        &mut self,
        session: SessionId,
        sql: String,
        reply: oneshot::Sender<Result<BatchReply, EngineError>>,
    ) {
        let isolation = self
            .sessions
            .get(session)
            .map(|s| s.txn_ctx.isolation())
            .unwrap_or_default();
        let needs = self.engine.analyze_locks(&sql, isolation);
        if self.try_acquire(session.raw(), &needs, true) {
            self.run_and_reply(session, &sql, reply);
            self.wake_parked();
        } else {
            self.parked.push_back(Parked {
                session,
                sql,
                reply,
                needs,
                deadline: Instant::now() + self.lock_wait_timeout,
            });
        }
    }

    /// Tries to grant every lock in `needs` to `owner` atomically. When
    /// `respect_queue` is set, an incoming batch also yields to any resource a
    /// parked waiter (of another owner) is already queued for — FIFO fairness,
    /// no barging. A resource the owner ALREADY holds is exempt from that
    /// yield: re-acquiring or upgrading a held lock is not queue-jumping, and
    /// yielding there would make a transaction wait on its own lock (a waiter
    /// parked behind that lock can never release it), a false self-deadlock.
    /// Returns whether all locks were granted.
    fn try_acquire(
        &mut self,
        owner: u64,
        needs: &[(Resource, LockMode)],
        respect_queue: bool,
    ) -> bool {
        let blocked = needs.iter().any(|(resource, mode)| {
            let queued = respect_queue
                && !self.locks.holds(owner, *resource)
                && self.parked.iter().any(|p| {
                    p.session.raw() != owner && p.needs.iter().any(|(r, _)| r == resource)
                });
            queued || self.locks.conflict(owner, *resource, *mode).is_some()
        });
        if blocked {
            return false;
        }
        for (resource, mode) in needs {
            self.locks.grant(owner, *resource, *mode);
        }
        true
    }

    /// Runs a batch whose locks are already held, replies, then releases the
    /// locks that do not outlive it (all of them once the transaction closes;
    /// read locks after each statement under READ COMMITTED).
    fn run_and_reply(
        &mut self,
        session: SessionId,
        sql: &str,
        reply: oneshot::Sender<Result<BatchReply, EngineError>>,
    ) {
        let owner = session.raw();
        let outcome = match self.sessions.get_mut(session) {
            Some(state) => self.engine.sql_batch(sql, &mut state.txn_ctx),
            None => {
                // Unknown session: one-shot autocommit, hold no locks after.
                let mut txn_ctx = TxnContext::default();
                let out = self.engine.sql_batch(sql, &mut txn_ctx);
                self.engine.abort_session_txn(&mut txn_ctx);
                out
            }
        };
        let in_transaction = match self.sessions.get(session) {
            Some(state) if state.txn_ctx.has_open_transaction() => {
                // Transaction still open: keep write locks. Under READ
                // COMMITTED shared locks do not survive the statement.
                if matches!(state.txn_ctx.isolation(), Isolation::ReadCommitted) {
                    self.locks.release_read_locks(owner);
                }
                true
            }
            // Transaction closed (autocommit or COMMIT/ROLLBACK) or unknown
            // session: drop every lock the batch acquired.
            _ => {
                self.locks.release_all(owner);
                false
            }
        };
        let _ = reply.send(outcome.map(|outcome| BatchReply {
            outcome,
            in_transaction,
        }));
    }

    /// Re-attempts parked batches in FIFO order until none can proceed. A
    /// woken batch may itself release locks (autocommit / commit), so this
    /// re-scans from the front after each grant.
    fn wake_parked(&mut self) {
        loop {
            let mut ran = false;
            let mut i = 0;
            while i < self.parked.len() {
                let owner = self.parked[i].session.raw();
                // Only waiters ahead in the queue have priority (FIFO); a
                // waiter never yields to itself or to those behind it.
                let ahead: Vec<(Resource, LockMode)> = self
                    .parked
                    .iter()
                    .take(i)
                    .filter(|p| p.session.raw() != owner)
                    .flat_map(|p| p.needs.iter().copied())
                    .collect();
                let grantable = self.parked[i].needs.iter().all(|(resource, mode)| {
                    // A resource the waiter already holds is exempt from the
                    // FIFO yield (it is not jumping the queue for it), matching
                    // try_acquire.
                    (self.locks.holds(owner, *resource)
                        || !ahead.iter().any(|(r, _)| r == resource))
                        && self.locks.conflict(owner, *resource, *mode).is_none()
                });
                if grantable {
                    let parked = self.parked.remove(i).expect("index in bounds");
                    for (resource, mode) in &parked.needs {
                        self.locks.grant(owner, *resource, *mode);
                    }
                    self.run_and_reply(parked.session, &parked.sql, parked.reply);
                    ran = true;
                    break; // re-scan from the front (locks may have changed)
                }
                i += 1;
            }
            if !ran {
                break;
            }
        }
    }

    /// Rolls back the single earliest-deadline batch whose wait has expired
    /// (the deadlock victim), then wakes anyone its released locks unblock —
    /// which typically rescues its deadlock partner before that partner is
    /// itself reaped. Any further expired waiters are handled on the next
    /// loop iteration.
    fn reap_expired(&mut self) {
        let now = Instant::now();
        let victim_idx = self
            .parked
            .iter()
            .enumerate()
            .filter(|(_, p)| p.deadline <= now)
            .min_by_key(|(_, p)| p.deadline)
            .map(|(i, _)| i);
        let Some(idx) = victim_idx else {
            return;
        };
        let victim = self.parked.remove(idx).expect("index in bounds");
        if let Some(state) = self.sessions.get_mut(victim.session) {
            self.engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(victim.session.raw());
        let _ = victim.reply.send(Ok(deadlock_victim_reply()));
        self.wake_parked();
    }

    /// Handles a disconnect: roll back any open transaction, release the
    /// session's locks, and wake anyone that was waiting on them.
    fn close_session(&mut self, session: SessionId) {
        if let Some(mut state) = self.sessions.close(session)
            && state.txn_ctx.has_open_transaction()
        {
            self.engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(session.raw());
        self.wake_parked();
    }
}

/// The reply delivered to a deadlock victim: no results, error 1205, and the
/// transaction is over (it was rolled back).
fn deadlock_victim_reply() -> BatchReply {
    BatchReply {
        outcome: BatchOutcome {
            results: Vec::new(),
            error: Some(SqlError::new(
                1205,
                13,
                51,
                "Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.",
            )),
        },
        in_transaction: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::rel::StatementResult;
    use crate::relstore::types::Datum;
    use crate::storage::{Storage, StorageOptions};
    use std::path::PathBuf;

    fn test_storage_options() -> StorageOptions {
        StorageOptions {
            size_gib: 1,
            wal_ratio: 0.05,
            metadata_ratio: 0.08,
            snapshot_ratio: 0.02,
            allocator_ratio: 0.02,
            reserved_ratio: 0.17,
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

    #[tokio::test]
    async fn writer_blocks_reader_until_commit() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;

        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        // A opens a transaction and writes, holding X on t.
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();

        // B's read must block (READ COMMITTED cannot read A's uncommitted row).
        let handle_b = h.handle.clone();
        let read = tokio::spawn(async move {
            handle_b
                .run_batch(b, "SELECT id FROM t".into())
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !read.is_finished(),
            "reader should be blocked by the writer"
        );

        // A commits → releases X → B unblocks and sees the committed row.
        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("reader should unblock after commit")
            .unwrap();
        assert_eq!(ids(&out), vec![1]);
    }

    #[tokio::test]
    async fn read_uncommitted_sees_uncommitted_rows_without_blocking() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;

        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (7);".into())
            .await
            .unwrap();

        // B under READ UNCOMMITTED takes no read lock → dirty-reads A's row.
        h.handle
            .run_batch(b, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED".into())
            .await
            .unwrap();
        let out = tokio::time::timeout(
            Duration::from_secs(2),
            h.handle.run_batch(b, "SELECT id FROM t".into()),
        )
        .await
        .expect("READ UNCOMMITTED must not block")
        .unwrap();
        assert_eq!(ids(&out), vec![7], "dirty read sees the uncommitted row");

        h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_releases_locks_and_wakes_waiter() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;

        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();

        let handle_b = h.handle.clone();
        let read = tokio::spawn(async move {
            handle_b
                .run_batch(b, "SELECT id FROM t".into())
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!read.is_finished(), "reader blocked by open writer txn");

        // A disconnects mid-transaction: rollback releases the lock, waking B,
        // which now sees an empty table (the insert was undone).
        h.handle.close_session(a);
        let out = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("reader should unblock on disconnect")
            .unwrap();
        assert_eq!(ids(&out), Vec::<i64>::new());
    }

    #[tokio::test]
    async fn deadlock_is_broken_by_timeout_with_1205() {
        // Short timeout so the reaper fires quickly.
        let h = start(Duration::from_millis(300));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;

        for stmt in [
            "CREATE TABLE a (id INT NOT NULL PRIMARY KEY)",
            "CREATE TABLE b (id INT NOT NULL PRIMARY KEY)",
            "INSERT INTO a VALUES (1)",
            "INSERT INTO b VALUES (1)",
        ] {
            h.handle.run_batch(a, stmt.into()).await.unwrap();
        }

        // A locks table a; B locks table b (each in its own transaction).
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE a SET id = id".into())
            .await
            .unwrap();
        h.handle
            .run_batch(b, "BEGIN TRAN; UPDATE b SET id = id".into())
            .await
            .unwrap();

        // Now each waits for the other's table → deadlock.
        let ha = h.handle.clone();
        let a_waits =
            tokio::spawn(async move { ha.run_batch(a, "UPDATE b SET id = id".into()).await });
        let hb = h.handle.clone();
        let b_waits =
            tokio::spawn(async move { hb.run_batch(b, "UPDATE a SET id = id".into()).await });

        let a_out = tokio::time::timeout(Duration::from_secs(5), a_waits)
            .await
            .expect("a_waits resolved")
            .unwrap()
            .unwrap();
        let b_out = tokio::time::timeout(Duration::from_secs(5), b_waits)
            .await
            .expect("b_waits resolved")
            .unwrap()
            .unwrap();

        // Exactly one is the deadlock victim (1205); the other succeeds.
        let victims = [&a_out, &b_out]
            .iter()
            .filter(|o| error_number(o) == Some(1205))
            .count();
        assert_eq!(victims, 1, "exactly one transaction is the deadlock victim");
    }

    #[tokio::test]
    async fn repeatable_read_holds_shared_lock_and_blocks_a_writer() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "INSERT INTO t VALUES (1)".into())
            .await
            .unwrap();

        // A reads under REPEATABLE READ inside a transaction → holds S on t.
        h.handle
            .run_batch(a, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; SELECT id FROM t;".into())
            .await
            .unwrap();

        // B's write must block on A's retained shared lock.
        let hb = h.handle.clone();
        let write = tokio::spawn(async move {
            hb.run_batch(b, "UPDATE t SET id = id".into())
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !write.is_finished(),
            "REPEATABLE READ keeps the shared lock, blocking the writer"
        );

        // A commits → releases S → B proceeds.
        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(5), write)
            .await
            .expect("writer unblocks after reader commits")
            .unwrap();
        assert!(out.outcome.error.is_none(), "{:?}", out.outcome.error);
    }

    #[tokio::test]
    async fn read_committed_releases_shared_lock_so_a_writer_proceeds() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "INSERT INTO t VALUES (1)".into())
            .await
            .unwrap();

        // A reads under READ COMMITTED (the default) inside a transaction; its
        // shared lock is dropped at statement end even though the txn stays open.
        h.handle
            .run_batch(a, "BEGIN TRAN; SELECT id FROM t;".into())
            .await
            .unwrap();

        // B's write is NOT blocked — unlike REPEATABLE READ above.
        let out = tokio::time::timeout(
            Duration::from_secs(2),
            h.handle.run_batch(b, "UPDATE t SET id = id".into()),
        )
        .await
        .expect("READ COMMITTED releases the shared lock, so the writer runs")
        .unwrap();
        assert!(out.outcome.error.is_none(), "{:?}", out.outcome.error);

        h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    }

    #[tokio::test]
    async fn isolation_escalation_within_a_batch_locks_the_read() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "INSERT INTO t VALUES (1)".into())
            .await
            .unwrap();

        // A holds X on t.
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t SET id = id;".into())
            .await
            .unwrap();

        // B is READ UNCOMMITTED, so a plain read would take no lock and could
        // dirty-read. But B raises the level to SERIALIZABLE in the SAME batch
        // as the read, which must lock the read → it blocks on A's writer.
        h.handle
            .run_batch(b, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED".into())
            .await
            .unwrap();
        let hb = h.handle.clone();
        let read = tokio::spawn(async move {
            hb.run_batch(
                b,
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT id FROM t;".into(),
            )
            .await
            .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !read.is_finished(),
            "an escalated read must lock and block on the uncommitted writer"
        );

        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("escalated read unblocks after commit")
            .unwrap();
        assert_eq!(ids(&out), vec![1]);
    }

    #[tokio::test]
    async fn holder_is_not_blocked_by_a_waiter_on_its_own_lock() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "INSERT INTO t VALUES (1)".into())
            .await
            .unwrap();

        // A holds X on t via an open transaction.
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t SET id = id;".into())
            .await
            .unwrap();

        // B blocks on t and parks in the queue.
        let hb = h.handle.clone();
        let b_read =
            tokio::spawn(async move { hb.run_batch(b, "SELECT id FROM t".into()).await.unwrap() });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!b_read.is_finished(), "B parks on A's lock");

        // A re-touches t in a new batch. Because A already holds the lock, it
        // must NOT yield to the queued waiter B — doing so would deadlock A on
        // its own lock. This completes promptly.
        let again = tokio::time::timeout(
            Duration::from_secs(2),
            h.handle.run_batch(a, "UPDATE t SET id = id".into()),
        )
        .await
        .expect("holder must not self-deadlock on a waiter behind its own lock")
        .unwrap();
        assert!(again.outcome.error.is_none(), "{:?}", again.outcome.error);

        // A commits → B finally proceeds.
        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(5), b_read)
            .await
            .expect("B unblocks after A commits")
            .unwrap();
        assert_eq!(ids(&out), vec![1]);
    }

    #[tokio::test]
    async fn autocommit_reads_run_concurrently() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session().await;
        let b = h.handle.open_session().await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "INSERT INTO t VALUES (1)".into())
            .await
            .unwrap();

        // Two shared readers never block each other.
        let out_a = h
            .handle
            .run_batch(a, "SELECT id FROM t".into())
            .await
            .unwrap();
        let out_b = tokio::time::timeout(
            Duration::from_secs(2),
            h.handle.run_batch(b, "SELECT id FROM t".into()),
        )
        .await
        .expect("concurrent shared reads must not block")
        .unwrap();
        assert_eq!(ids(&out_a), vec![1]);
        assert_eq!(ids(&out_b), vec![1]);
    }
}
