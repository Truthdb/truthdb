//! The engine worker pool: a bank of OS threads shares an `Arc<Engine>` and a
//! [`Scheduler`] (sessions + lock table + parked queue behind one mutex) and
//! serves [`EngineCall`]s off a channel. Workers hold the scheduler mutex only
//! to make lock decisions (acquire / park / release / wake); a batch's actual
//! execution runs with the mutex *released*, so non-conflicting batches run
//! concurrently. Per-connection session state (transaction / isolation) lives
//! in the [`SessionManager`], and the synchronous io_uring work runs off the
//! async reactor on these threads.
//!
//! ## Locking without blocking a worker
//!
//! A worker must never block in place waiting for a lock — the lock's holder
//! could only release by having its own work processed, and while workers exist
//! to do that, a batch that parked mid-execution could not be restarted
//! cleanly. Instead a batch acquires *all* the table/database locks it needs up
//! front (see [`crate::rel::analyze_locks`]) before running any statement, so a
//! running batch never blocks on a lock. If a lock conflicts, the whole
//! [`EngineCall::RunBatch`] is *parked* — its reply deferred — and the worker
//! moves on. Releasing locks (commit / rollback / disconnect) makes parked
//! batches grantable; the releasing worker drains them in FIFO order, running
//! each. Since a parked batch never ran, restarting it is exact.
//!
//! Because a running batch never waits on a lock, only *parked* batches can
//! form a lock-wait cycle. A deadlock is broken by a waits-for-graph cycle
//! detector that runs the instant a parking batch closes a cycle: the youngest
//! transaction in the cycle is rolled back as the victim (error 1205). A 5 s
//! per-wait deadline remains as a backstop for any stall the graph does not
//! model.
//!
//! ## Thread-safety of shared state
//!
//! Two locks, always taken in this order (never the reverse), so they cannot
//! deadlock: the **scheduler** mutex (lock decisions) may briefly take the
//! **storage** mutex under it (catalog lookup in `analyze_locks`, rollback in
//! `abort`); batch execution takes only storage (and the engine's execution
//! gate), never the scheduler. See [`Engine`] for the execution gate that keeps
//! the native path from observing a relational batch's torn writes.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
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

/// How long a session may sit idle with a transaction still open before the
/// engine rolls it back and releases its locks.
///
/// This is a deliberate divergence from SQL Server, which never reaps an idle
/// transaction: a client that dies without closing its TCP connection would
/// otherwise hold its locks until the OS notices, which can take hours, and
/// every conflicting batch fails with 1205 in the meantime. Ten minutes is far
/// longer than any interactive transaction should stay idle, so a legitimate
/// client is never reaped. `spawn_engine_pool` takes it as `Option`, so it can
/// be disabled outright.
const IDLE_TXN_TIMEOUT: Duration = Duration::from_secs(600);

/// Floor on the maintenance thread's sleep, so no configuration of the timeouts
/// can turn its loop into a spin.
const MIN_SWEEP_INTERVAL: Duration = Duration::from_millis(10);

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
struct Session {
    txn_ctx: TxnContext,
    /// When this session last started or finished a batch. Only meaningful
    /// while the session is idle: a running batch's context is moved out by
    /// [`Scheduler::take_ctx`], so a session mid-batch reports no open
    /// transaction and is never a reap candidate regardless of this stamp.
    last_activity: Instant,
}

impl Default for Session {
    fn default() -> Self {
        Session {
            txn_ctx: TxnContext::default(),
            last_activity: Instant::now(),
        }
    }
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

    fn open(&mut self, database: String, login: String) -> SessionId {
        let id = SessionId(self.next_id);
        self.next_id += 1;
        let mut session = Session::default();
        session
            .txn_ctx
            .set_session_identity(database, login, id.0 as i32);
        self.sessions.insert(id, session);
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

    fn iter(&self) -> impl Iterator<Item = (&SessionId, &Session)> {
        self.sessions.iter()
    }

    /// Marks a session as active now, so the idle reaper does not count time
    /// spent running a batch against it.
    fn touch(&mut self, id: SessionId) {
        if let Some(state) = self.sessions.get_mut(&id) {
            state.last_activity = Instant::now();
        }
    }
}

/// A message to the engine thread. Each carries a one-shot reply channel the
/// async caller awaits.
enum EngineCall {
    OpenSession {
        database: String,
        login: String,
        reply: oneshot::Sender<SessionId>,
    },
    /// A SQL batch on behalf of a session (TDS path): typed results. `params`
    /// is empty for a plain batch, or the `sp_executesql` parameters seeded as
    /// batch variables before the statement runs (RPC path).
    RunBatch {
        session: SessionId,
        sql: String,
        params: Vec<crate::rel::RpcParam>,
        /// Set by the connection on a TDS Attention to abort the batch mid-flight.
        cancel: Arc<AtomicBool>,
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

/// A cloneable handle to the engine's worker pool. Cheap to clone (shares the
/// sender).
#[derive(Clone)]
pub struct EngineHandle {
    tx: mpsc::Sender<EngineCall>,
    /// Number of worker threads, so [`Self::shutdown`] can send one poison
    /// pill per worker.
    workers: usize,
}

impl EngineHandle {
    /// Opens a session for a connection, recording its database and login for
    /// session intrinsics. Returns its id (or a placeholder if the engine is
    /// gone).
    pub async fn open_session(&self, database: String, login: String) -> SessionId {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(EngineCall::OpenSession {
                database,
                login,
                reply,
            })
            .is_err()
        {
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
        self.run_rpc(session, sql, Vec::new()).await
    }

    /// Runs an `sp_executesql` statement with decoded parameters seeded as
    /// batch variables. Same lock/parking path as [`Self::run_batch`].
    pub async fn run_rpc(
        &self,
        session: SessionId,
        sql: String,
        params: Vec<crate::rel::RpcParam>,
    ) -> Result<BatchReply, EngineError> {
        self.run_rpc_cancellable(session, sql, params, Arc::new(AtomicBool::new(false)))
            .await
    }

    /// Like [`Self::run_batch`] but the caller holds `cancel`: setting it (on a
    /// TDS Attention) aborts the running statement mid-flight (the executor polls
    /// it). Pass a shared clone to the connection's Attention handler.
    pub async fn run_batch_cancellable(
        &self,
        session: SessionId,
        sql: String,
        cancel: Arc<AtomicBool>,
    ) -> Result<BatchReply, EngineError> {
        self.run_rpc_cancellable(session, sql, Vec::new(), cancel)
            .await
    }

    /// Like [`Self::run_rpc`] but cancellable via `cancel` (see
    /// [`Self::run_batch_cancellable`]).
    pub async fn run_rpc_cancellable(
        &self,
        session: SessionId,
        sql: String,
        params: Vec<crate::rel::RpcParam>,
        cancel: Arc<AtomicBool>,
    ) -> Result<BatchReply, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(EngineCall::RunBatch {
                session,
                sql,
                params,
                cancel,
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

    /// Asks the worker pool to stop. One poison pill per worker wakes every
    /// thread (even those blocked on `recv`); each consumes one and exits.
    pub fn shutdown(&self) {
        for _ in 0..self.workers {
            let _ = self.tx.send(EngineCall::Shutdown);
        }
    }
}

/// Worker-thread count for the pool: one per core (minus a couple reserved for
/// the async listeners), at least two so reads can genuinely overlap.
fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(2))
        .unwrap_or(2)
        .max(2)
}

/// Spawns the engine worker pool and returns a handle plus a join handle for a
/// supervisor thread that outlives every worker.
pub fn spawn_engine(engine: Engine) -> (EngineHandle, JoinHandle<()>) {
    spawn_engine_pool(
        engine,
        LOCK_WAIT_TIMEOUT,
        Some(IDLE_TXN_TIMEOUT),
        worker_count(),
    )
}

/// Like [`spawn_engine`] but with a custom lock-wait timeout, so tests can
/// exercise the deadlock reaper without a real 5 s wait.
#[cfg(test)]
fn spawn_engine_with_timeout(engine: Engine, timeout: Duration) -> (EngineHandle, JoinHandle<()>) {
    spawn_engine_pool(engine, timeout, Some(IDLE_TXN_TIMEOUT), worker_count())
}

/// Like [`spawn_engine`] but with a custom idle-transaction timeout, so tests
/// can exercise the idle reaper without a real 10 min wait.
#[cfg(test)]
fn spawn_engine_with_idle_timeout(
    engine: Engine,
    idle: Option<Duration>,
) -> (EngineHandle, JoinHandle<()>) {
    spawn_engine_pool(engine, LOCK_WAIT_TIMEOUT, idle, worker_count())
}

fn spawn_engine_pool(
    engine: Engine,
    timeout: Duration,
    idle_txn_timeout: Option<Duration>,
    workers: usize,
) -> (EngineHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let shared = Arc::new(Shared {
        engine: Arc::new(engine),
        scheduler: Mutex::new(Scheduler::new(timeout, idle_txn_timeout)),
        rx: Mutex::new(rx),
        stop: AtomicBool::new(false),
        idle: Mutex::new(()),
        wake: Condvar::new(),
    });
    // A supervisor thread spawns the workers and joins them; its handle is what
    // callers join at shutdown. When all workers have exited, any batch still
    // parked is failed so its caller unblocks.
    let supervisor = Arc::clone(&shared);
    let join = std::thread::Builder::new()
        .name("truthdb-engine".to_string())
        .spawn(move || {
            let keeper = Arc::clone(&supervisor);
            let maintenance = std::thread::Builder::new()
                .name("truthdb-maintenance".to_string())
                .spawn(move || maintenance_loop(&keeper))
                .expect("spawn maintenance thread");
            let handles: Vec<_> = (0..workers)
                .map(|i| {
                    let shared = Arc::clone(&supervisor);
                    std::thread::Builder::new()
                        .name(format!("truthdb-worker-{i}"))
                        .spawn(move || worker_loop(&shared))
                        .expect("spawn worker thread")
                })
                .collect();
            for handle in handles {
                let _ = handle.join();
            }
            // The workers are gone — either on a Shutdown pill, or because the
            // last handle dropped and the channel disconnected, which sets no
            // flag. Tell the maintenance thread either way, or it would outlive
            // the pool. Setting the flag under `idle` is what makes the wake
            // reliable rather than a race against its next sleep.
            {
                let _idle = supervisor.idle.lock().expect("idle mutex poisoned");
                supervisor.stop.store(true, Ordering::Release);
            }
            supervisor.wake.notify_all();
            let _ = maintenance.join();
            let mut sched = supervisor.scheduler.lock().expect("scheduler poisoned");
            for parked in sched.parked.drain(..) {
                let _ = parked.reply.send(Err(EngineError::Unavailable));
            }
        })
        .expect("spawn engine supervisor");
    (EngineHandle { tx, workers }, join)
}

/// State shared by every worker thread.
struct Shared {
    /// The database engine. `&self` throughout, so the pool shares one `Arc`.
    engine: Arc<Engine>,
    /// Sessions + lock table + parked queue. Held only for lock decisions.
    scheduler: Mutex<Scheduler>,
    /// Inbound calls. Behind a mutex because `mpsc::Receiver` has a single
    /// consumer: a worker locks it only for the brief `recv`, then releases it
    /// so a sibling can take the next call while this one runs its batch.
    rx: Mutex<mpsc::Receiver<EngineCall>>,
    /// Set when a `Shutdown` is seen, so a worker between calls exits promptly
    /// rather than picking up more work.
    stop: AtomicBool,
    /// Companion mutex for [`Self::wake`]. Guards nothing — a `Condvar` needs
    /// one.
    idle: Mutex<()>,
    /// Wakes the maintenance thread out of its sleep at shutdown, so the pool
    /// does not wait out a whole sweep interval before exiting.
    wake: Condvar,
}

// The pool shares `Arc<Engine>` across worker threads, so the engine — and thus
// the whole shared state — must be Send + Sync. Assert it here rather than
// discovering it via a distant `thread::spawn` error.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Shared>();
};

/// A parked batch that has become grantable: its locks are already held and its
/// session's transaction context has been taken out for the worker to run with.
struct Runnable {
    session: SessionId,
    sql: String,
    params: Vec<crate::rel::RpcParam>,
    cancel: Arc<AtomicBool>,
    reply: oneshot::Sender<Result<BatchReply, EngineError>>,
    txn_ctx: TxnContext,
}

/// Counts maintenance threads that have started, so a test can prove the pool
/// actually spawns one — the reaping itself is pinned against a hand-built
/// `Shared`, which would not notice the supervisor forgetting to wire it up.
#[cfg(test)]
static MAINTENANCE_STARTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Counts maintenance sweeps, so a test can prove the thread sleeps between
/// them rather than spinning.
#[cfg(test)]
static MAINTENANCE_SWEEPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The idle-transaction reaper, on a thread that never runs a batch.
///
/// It used to run only on the workers, between calls, which made it exactly as
/// punctual as the pool was free — and the pool is `cores-2` threads, two on a
/// four-core box. A few long scans deferred it for as long as they ran, which
/// is backwards: it exists to release the locks of a client that has stopped
/// responding, and a loaded engine is when that matters most. Nothing a client
/// does can delay this thread, because it never executes anything on anyone's
/// behalf.
///
/// **Only the idle reaper lives here.** The deadlock backstop
/// ([`Scheduler::reap_expired`]) stays on the workers, because it is coupled to
/// draining: reaping a victim releases locks that rescue the waiters behind it,
/// and its contract is that the same pass then runs them. A sweeper that reaps
/// without draining is worse than none — it re-reaps the waiters a drain would
/// have rescued, and (since a waiter it must *not* reap keeps a deadline in the
/// past) it spins doing so. The idle reaper has no such coupling: it is purely
/// a function of `last_activity`, and the locks it frees are drained by
/// whichever worker gets there, exactly as before.
///
/// Its cadence therefore never depends on the parked queue — see
/// [`Scheduler::sweep_interval`].
fn maintenance_loop(shared: &Arc<Shared>) {
    #[cfg(test)]
    MAINTENANCE_STARTS.fetch_add(1, Ordering::Relaxed);
    while !shared.stop.load(Ordering::Acquire) {
        let wait = shared
            .scheduler
            .lock()
            .expect("scheduler poisoned")
            .sweep_interval();
        {
            // `stop` is read and written under this mutex, so a shutdown that
            // lands between the check and the wait cannot be missed — it would
            // otherwise be slept through for a whole interval, and the
            // supervisor is joining this thread.
            let idle = shared.idle.lock().expect("idle mutex poisoned");
            if shared.stop.load(Ordering::Acquire) {
                break;
            }
            let _ = shared
                .wake
                .wait_timeout(idle, wait)
                .expect("idle mutex poisoned");
        }
        #[cfg(test)]
        MAINTENANCE_SWEEPS.fetch_add(1, Ordering::Relaxed);
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        sched.reap_idle_txns(&shared.engine);
    }
}

/// One worker thread: pull a call, dispatch it, repeat until shutdown.
fn worker_loop(shared: &Arc<Shared>) {
    while !shared.stop.load(Ordering::Acquire) {
        // Block for the next call, waking at the earliest parked deadline so a
        // stalled waiter is reaped even if no new call arrives. The rx mutex has
        // a single consumer, so only one worker is in `recv` at a time; the
        // deadline snapshot is taken before contending for it and can go stale
        // (another worker may park a batch while we wait for the mutex). To keep
        // the reaper live we NEVER block indefinitely: with nothing parked we
        // still cap the wait at `lock_wait_timeout` and re-evaluate. Since a
        // parked batch's own deadline is exactly that far out, this cap
        // guarantees a worker re-reads the queue no later than the first
        // deadline, so it is reaped on time. (Only the brief `recv` holds rx.)
        let (deadline, cap) = {
            let sched = shared.scheduler.lock().expect("scheduler poisoned");
            (sched.earliest_deadline(), sched.wake_cap())
        };
        let wait = match deadline {
            // Never sleep past the periodic cap, even when the nearest parked
            // deadline is further out: the idle sweep still has to run.
            Some(deadline) => deadline.saturating_duration_since(Instant::now()).min(cap),
            None => cap,
        };
        let call = {
            let rx = shared.rx.lock().expect("rx mutex poisoned");
            match rx.recv_timeout(wait) {
                Ok(call) => Some(call),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        };
        // Reap any expired waiter (a deadlock backstop). A reap releases the
        // victim's locks, which may unblock its parked partner, so drain before
        // handling the call — otherwise, if the system then goes quiet, that
        // partner could sit grantable until its own deadline and be reaped as a
        // false victim. (dispatch_batch / close_session drain again after their
        // own releases.)
        {
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            // A call we just dequeued proves its session is not idle. Stamp it
            // before sweeping: the sweep runs in this same iteration, *before*
            // the call below is dispatched, and would otherwise reap the
            // transaction of a session whose next batch is already in hand.
            if let Some(EngineCall::RunBatch { session, .. }) = &call {
                sched.sessions.touch(*session);
            }
            sched.reap_expired(&shared.engine);
            // The loop above never blocks longer than `wake_cap`, so this sweep
            // runs regularly even on a completely idle server.
            sched.reap_idle_txns(&shared.engine);
        }
        drain_ready(shared);
        match call {
            None => {}
            Some(EngineCall::OpenSession {
                database,
                login,
                reply,
            }) => {
                let id = shared
                    .scheduler
                    .lock()
                    .expect("scheduler poisoned")
                    .sessions
                    .open(database, login);
                let _ = reply.send(id);
            }
            Some(EngineCall::RunBatch {
                session,
                sql,
                params,
                cancel,
                reply,
            }) => dispatch_batch(shared, session, sql, params, cancel, reply),
            Some(EngineCall::RunNative { command, reply }) => {
                let _ = reply.send(shared.engine.execute(&command));
            }
            Some(EngineCall::CloseSession { session }) => {
                shared
                    .scheduler
                    .lock()
                    .expect("scheduler poisoned")
                    .close_session(&shared.engine, session);
                drain_ready(shared);
            }
            Some(EngineCall::Shutdown) => {
                shared.stop.store(true, Ordering::Release);
                break;
            }
        }
    }
}

/// Acquires a batch's locks and runs it, or parks it behind a conflict. Either
/// way, drains anything the batch's completion (or a deadlock abort) unblocked.
fn dispatch_batch(
    shared: &Arc<Shared>,
    session: SessionId,
    sql: String,
    params: Vec<crate::rel::RpcParam>,
    cancel: Arc<AtomicBool>,
    reply: oneshot::Sender<Result<BatchReply, EngineError>>,
) {
    let runnable = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        let isolation = sched.isolation(session);
        // Parameters are values, not statements, so they never change which
        // locks the batch needs — analyse the statement text as usual.
        let needs = shared.engine.analyze_locks(&sql, isolation);
        if sched.try_acquire(session.raw(), &needs, true) {
            sched.sessions.touch(session);
            let txn_ctx = sched.take_ctx(session);
            Some(Runnable {
                session,
                sql,
                params,
                cancel,
                reply,
                txn_ctx,
            })
        } else {
            let deadline = Instant::now() + sched.lock_wait_timeout;
            sched.parked.push_back(Parked {
                session,
                sql,
                params,
                cancel,
                reply,
                needs,
                deadline,
            });
            // The new waiter may have closed a lock-wait cycle; break it now
            // rather than waiting for the deadline backstop.
            sched.detect_deadlock(&shared.engine);
            None
        }
    };
    if let Some(work) = runnable {
        run_and_finish(shared, work);
    }
    drain_ready(shared);
}

/// Runs a batch whose locks are already held (execution holds no scheduler
/// lock, so batches run concurrently), then re-locks the scheduler to return
/// the session's transaction context and release the locks that do not outlive
/// the batch.
fn run_and_finish(shared: &Arc<Shared>, work: Runnable) {
    let Runnable {
        session,
        sql,
        params,
        cancel,
        reply,
        mut txn_ctx,
    } = work;
    // Bind the cancel flag to this worker thread for the batch, so the executor's
    // `check_cancelled` polls see a TDS Attention; the guard clears it on return.
    let _cancel_guard = crate::rel::CancelScope::enter(cancel);
    let outcome = shared
        .engine
        .sql_batch_with_params(&sql, &mut txn_ctx, &params);
    let in_transaction = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        sched.finish(&shared.engine, session, txn_ctx)
    };
    let _ = reply.send(outcome.map(|outcome| BatchReply {
        outcome,
        in_transaction,
    }));
}

/// Runs every parked batch whose locks are now grantable, in FIFO order, until
/// none remain. Each finished batch may release locks that unblock the next, so
/// this re-checks after every one.
fn drain_ready(shared: &Arc<Shared>) {
    loop {
        let work = {
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            sched.next_grantable()
        };
        match work {
            Some(work) => run_and_finish(shared, work),
            None => break,
        }
    }
}

/// A SQL batch waiting for locks: its request, the locks it needs, and the
/// deadline past which it is treated as a deadlock victim.
struct Parked {
    session: SessionId,
    sql: String,
    params: Vec<crate::rel::RpcParam>,
    cancel: Arc<AtomicBool>,
    reply: oneshot::Sender<Result<BatchReply, EngineError>>,
    needs: Vec<(Resource, LockMode)>,
    deadline: Instant,
}

/// The scheduler's mutable world: the sessions, the lock manager, and the FIFO
/// queue of batches parked on locks. One [`Mutex`] guards all three; a worker
/// holds it only to make lock decisions, never while running a batch.
struct Scheduler {
    sessions: SessionManager,
    locks: LockManager,
    parked: VecDeque<Parked>,
    lock_wait_timeout: Duration,
    /// How long a session may sit idle *with a transaction open* before that
    /// transaction is rolled back and its locks released. `None` disables the
    /// reaper.
    idle_txn_timeout: Option<Duration>,
}

impl Scheduler {
    fn new(lock_wait_timeout: Duration, idle_txn_timeout: Option<Duration>) -> Self {
        Scheduler {
            sessions: SessionManager::new(),
            locks: LockManager::new(),
            parked: VecDeque::new(),
            lock_wait_timeout,
            idle_txn_timeout,
        }
    }

    fn earliest_deadline(&self) -> Option<Instant> {
        self.parked.iter().map(|p| p.deadline).min()
    }

    /// The longest a worker may block before re-evaluating. A worker has two
    /// periodic duties — reaping a parked batch at its deadline and reaping a
    /// transaction left idle — so it must wake for whichever comes first. With
    /// the default timeouts the lock-wait bound (5 s) dominates the idle one
    /// (10 min) and this is unchanged; a deployment (or test) that sets a short
    /// idle timeout gets a correspondingly shorter cap instead of a reaper that
    /// silently runs late.
    /// How long the maintenance thread sleeps between idle sweeps.
    ///
    /// Deliberately a function of the timeouts alone, never of the parked
    /// queue: a deadline in the past — which [`Self::reap_expired`] leaves
    /// there on purpose, for a waiter that is grantable and so must not be
    /// reaped — would drive the wait to zero and spin a core against the
    /// scheduler mutex. The idle reaper has no business with parked deadlines
    /// anyway.
    ///
    /// Floored, because `idle_txn_timeout` is a tuning knob and tests already
    /// pass `Duration::ZERO`; without it, that setting alone would peg a core.
    fn sweep_interval(&self) -> Duration {
        match self.idle_txn_timeout {
            Some(idle) => idle.min(self.lock_wait_timeout),
            // The reaper is disabled; there is nothing to be prompt for.
            None => self.lock_wait_timeout,
        }
        .max(MIN_SWEEP_INTERVAL)
    }

    fn wake_cap(&self) -> Duration {
        match self.idle_txn_timeout {
            Some(idle) => self.lock_wait_timeout.min(idle),
            None => self.lock_wait_timeout,
        }
    }

    /// A session's current isolation level (default if the session is unknown).
    fn isolation(&self, session: SessionId) -> Isolation {
        self.sessions
            .get(session)
            .map(|s| s.txn_ctx.isolation())
            .unwrap_or_default()
    }

    /// Takes a session's transaction context out for a worker to run a batch
    /// with (a `Default` placeholder is left behind; [`Self::finish`] returns
    /// the real one). A session has at most one in-flight batch and no close
    /// races it — the connection is request/response — so the placeholder is
    /// never observed. Unknown session: a transient context, rolled back after.
    fn take_ctx(&mut self, session: SessionId) -> TxnContext {
        self.sessions
            .get_mut(session)
            .map(|state| std::mem::take(&mut state.txn_ctx))
            .unwrap_or_default()
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

    /// Returns a finished batch's transaction context to its session and
    /// releases the locks that do not outlive it: all of them once the
    /// transaction closes; read locks after each statement under READ
    /// COMMITTED. Returns whether the connection is still in a transaction.
    /// (Execution ran in [`run_and_finish`], with the scheduler lock released.)
    fn finish(&mut self, engine: &Engine, session: SessionId, txn_ctx: TxnContext) -> bool {
        let owner = session.raw();
        match self.sessions.get_mut(session) {
            Some(state) => {
                state.txn_ctx = txn_ctx;
                // The idle clock restarts when the batch finishes: time spent
                // running is not time spent idle.
                state.last_activity = Instant::now();
                let open = state.txn_ctx.has_open_transaction();
                let is_read_committed =
                    matches!(state.txn_ctx.isolation(), Isolation::ReadCommitted);
                if open {
                    // Transaction still open: keep write locks. Under READ
                    // COMMITTED shared locks do not survive the statement.
                    if is_read_committed {
                        self.locks.release_read_locks(owner);
                    }
                    true
                } else {
                    // Transaction closed (autocommit or COMMIT/ROLLBACK): drop
                    // every lock the batch acquired.
                    self.locks.release_all(owner);
                    false
                }
            }
            None => {
                // Session closed while the batch ran, or unknown: roll back the
                // taken context and hold no locks.
                let mut txn_ctx = txn_ctx;
                engine.abort_session_txn(&mut txn_ctx);
                self.locks.release_all(owner);
                false
            }
        }
    }

    /// Whether the parked batch at `i` could take every lock it needs right
    /// now — i.e. it is waiting for a worker to pick it up, not for a lock.
    fn grantable(&self, i: usize) -> bool {
        let owner = self.parked[i].session.raw();
        // Only waiters ahead in the queue have priority (FIFO); a waiter never
        // yields to itself or to those behind it.
        let ahead: Vec<(Resource, LockMode)> = self
            .parked
            .iter()
            .take(i)
            .filter(|p| p.session.raw() != owner)
            .flat_map(|p| p.needs.iter().copied())
            .collect();
        self.parked[i].needs.iter().all(|(resource, mode)| {
            // A resource the waiter already holds is exempt from the FIFO yield
            // (it is not jumping the queue for it), matching try_acquire.
            (self.locks.holds(owner, *resource) || !ahead.iter().any(|(r, _)| r == resource))
                && self.locks.conflict(owner, *resource, *mode).is_none()
        })
    }

    /// Removes and returns the first parked batch (FIFO) whose locks are now
    /// grantable, having granted them and taken its session's transaction
    /// context out to run with. `None` if none can proceed. The caller runs it
    /// with the scheduler lock released, then calls again.
    fn next_grantable(&mut self) -> Option<Runnable> {
        let mut i = 0;
        while i < self.parked.len() {
            let owner = self.parked[i].session.raw();
            if self.grantable(i) {
                let parked = self.parked.remove(i).expect("index in bounds");
                for (resource, mode) in &parked.needs {
                    self.locks.grant(owner, *resource, *mode);
                }
                let txn_ctx = self.take_ctx(parked.session);
                return Some(Runnable {
                    session: parked.session,
                    sql: parked.sql,
                    params: parked.params,
                    cancel: parked.cancel,
                    reply: parked.reply,
                    txn_ctx,
                });
            }
            i += 1;
        }
        None
    }

    /// Rolls back the single earliest-deadline batch whose wait has expired
    /// (the deadlock backstop victim). The caller then drains anyone its
    /// released locks unblock — typically rescuing its deadlock partner before
    /// that partner is itself reaped. Any further expired waiters are handled on
    /// the next loop iteration.
    ///
    /// A waiter whose locks are already grantable is never a victim, however
    /// long it has sat there: it is not blocked on anyone, it is waiting for a
    /// worker to run it, and killing it would report a lock conflict (1205)
    /// that does not exist. The gap is narrow today — the worker that releases
    /// the locks drains the queue microseconds later, so only an unlucky
    /// deschedule between the two exposes it — but it widens as soon as
    /// anything can delay a worker between releasing locks and draining, which
    /// is exactly what pushing result rows to a client will do. The reaper's
    /// contract is about lock waits either way.
    fn reap_expired(&mut self, engine: &Engine) {
        let now = Instant::now();
        let victim_idx = self
            .parked
            .iter()
            .enumerate()
            // `grantable` is only consulted for a waiter that has actually
            // expired, so the common case (nothing expired) does no extra work.
            .filter(|(i, p)| p.deadline <= now && !self.grantable(*i))
            .min_by_key(|(_, p)| p.deadline)
            .map(|(i, _)| i);
        if let Some(idx) = victim_idx {
            self.abort_parked_victim(engine, idx);
        }
    }

    /// Rolls back transactions abandoned by idle sessions, releasing their
    /// locks; returns whether anything was released (so the caller drains the
    /// batches those locks unblock).
    ///
    /// A connection that opens a transaction and then goes silent *without
    /// disconnecting* — a crashed client, a severed network — would otherwise
    /// hold its locks indefinitely: the connection-drop path only covers a
    /// connection that actually closed, and TCP may not notice for hours.
    ///
    /// Only genuinely idle sessions are candidates. A session running a batch
    /// has had its context moved out by [`Self::take_ctx`], so it reports no
    /// open transaction and cannot be reaped mid-batch; a session with a parked
    /// batch is skipped explicitly, since that batch is only waiting on locks
    /// (and its own deadline reaps it) rather than being abandoned.
    fn reap_idle_txns(&mut self, engine: &Engine) -> bool {
        let Some(timeout) = self.idle_txn_timeout else {
            return false;
        };
        let now = Instant::now();
        let parked: Vec<SessionId> = self.parked.iter().map(|p| p.session).collect();
        let victims: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(id, state)| {
                state.txn_ctx.has_open_transaction()
                    && now.duration_since(state.last_activity) >= timeout
                    && !parked.contains(id)
            })
            .map(|(id, _)| *id)
            .collect();
        for session in &victims {
            if let Some(state) = self.sessions.get_mut(*session) {
                // The session survives, so the rollback is recorded: its next
                // batch is told the transaction is gone rather than silently
                // autocommitting statements the client means to be
                // transactional.
                engine.abort_idle_session_txn(&mut state.txn_ctx);
                state.last_activity = now;
            }
            self.locks.release_all(session.raw());
        }
        !victims.is_empty()
    }

    /// Aborts the parked batch at `idx` as a deadlock victim: rolls back its
    /// transaction, releases its locks, and replies with error 1205. The caller
    /// drains any batches the released locks unblock.
    fn abort_parked_victim(&mut self, engine: &Engine, idx: usize) {
        let victim = self.parked.remove(idx).expect("index in bounds");
        if let Some(state) = self.sessions.get_mut(victim.session) {
            engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(victim.session.raw());
        let _ = victim.reply.send(Ok(deadlock_victim_reply()));
    }

    /// Detects lock-wait *cycles* among the parked batches — a waits-for graph
    /// over the lock manager — and aborts the youngest transaction in each cycle
    /// (error 1205). A cycle can only form when a batch parks, so this runs the
    /// instant one does, breaking the deadlock immediately rather than after the
    /// wait-timeout backstop. Aborts victims until the graph is acyclic.
    fn detect_deadlock(&mut self, engine: &Engine) {
        while let Some(idx) = self.find_deadlock_victim() {
            self.abort_parked_victim(engine, idx);
        }
    }

    /// The parked-queue index of a deadlock victim, or `None` if no cycle exists.
    /// Edge O -> H: a parked owner O waits for every current holder H of a
    /// resource O needs but cannot acquire. The victim is the cycle member that
    /// parked most recently (the youngest wait — the least work to roll back).
    fn find_deadlock_victim(&self) -> Option<usize> {
        use std::collections::{HashMap, HashSet};
        // Assumes at most one parked batch per session (a session is
        // request/response, so it has at most one in-flight batch). If pipelining
        // is ever added, the per-owner edge merge and single-index abort below
        // must be revisited.
        let mut waits_for: HashMap<u64, HashSet<u64>> = HashMap::new();
        for (index, parked) in self.parked.iter().enumerate() {
            let owner = parked.session.raw();
            let edges = waits_for.entry(owner).or_default();
            for (resource, mode) in &parked.needs {
                // Held-conflict edges: owners holding a conflicting lock.
                for holder in self.locks.conflicting_holders(owner, *resource, *mode) {
                    edges.insert(holder);
                }
                // FIFO anti-barging edges: a batch yields a free resource to any
                // waiter parked ahead of it that needs the same resource (the
                // `wake_parked` grant rule), unless it already holds it. Without
                // these a deadlock routed through a queue yield would be missed.
                if !self.locks.holds(owner, *resource) {
                    for ahead in self.parked.iter().take(index) {
                        if ahead.session.raw() != owner
                            && ahead.needs.iter().any(|(r, _)| r == resource)
                        {
                            edges.insert(ahead.session.raw());
                        }
                    }
                }
            }
        }
        let cycle = find_cycle(&waits_for)?;
        self.parked
            .iter()
            .enumerate()
            .filter(|(_, p)| cycle.contains(&p.session.raw()))
            .max_by_key(|(_, p)| p.deadline)
            .map(|(i, _)| i)
    }

    /// Handles a disconnect: roll back any open transaction and release the
    /// session's locks. The caller drains anyone that was waiting on them.
    fn close_session(&mut self, engine: &Engine, session: SessionId) {
        if let Some(mut state) = self.sessions.close(session)
            && state.txn_ctx.has_open_transaction()
        {
            engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(session.raw());
    }
}

/// The reply delivered to a deadlock victim: no results, error 1205, and the
/// transaction is over (it was rolled back).
/// Finds one cycle in a waits-for graph (owner -> owners it waits for), or
/// `None` if acyclic. Iterative colored DFS (white/gray/black); a back-edge to a
/// gray node on the current path is a cycle, returned as the owners composing
/// it. Nodes with no outgoing edges (a lock holder that is not itself waiting)
/// are dead ends and cannot close a cycle.
fn find_cycle(
    graph: &std::collections::HashMap<u64, std::collections::HashSet<u64>>,
) -> Option<Vec<u64>> {
    const WHITE: u8 = 0;
    const GRAY: u8 = 1;
    const BLACK: u8 = 2;
    // Pre-seed every graph node WHITE. A neighbor absent from this map is a lock
    // holder that is not itself waiting (no outgoing edges) — a dead end, so it
    // defaults to BLACK below and cannot extend a path.
    let mut color: std::collections::HashMap<u64, u8> = graph.keys().map(|&k| (k, WHITE)).collect();
    for &root in graph.keys() {
        if color.get(&root).copied().unwrap_or(WHITE) != WHITE {
            continue;
        }
        let mut path: Vec<u64> = vec![root];
        let neighbors = |n: u64| -> std::vec::IntoIter<u64> {
            graph
                .get(&n)
                .map(|s| s.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default()
                .into_iter()
        };
        let mut iters: Vec<std::vec::IntoIter<u64>> = vec![neighbors(root)];
        color.insert(root, GRAY);
        while !iters.is_empty() {
            let next = iters.last_mut().expect("non-empty").next();
            match next {
                Some(next) => match color.get(&next).copied().unwrap_or(BLACK) {
                    WHITE => {
                        color.insert(next, GRAY);
                        path.push(next);
                        iters.push(neighbors(next));
                    }
                    GRAY => {
                        let start = path.iter().position(|&x| x == next).expect("gray on path");
                        return Some(path[start..].to_vec());
                    }
                    _ => {}
                },
                None => {
                    let done = path.pop().expect("path non-empty");
                    color.insert(done, BLACK);
                    iters.pop();
                }
            }
        }
    }
    None
}

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
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    fn graph(edges: &[(u64, &[u64])]) -> HashMap<u64, HashSet<u64>> {
        edges
            .iter()
            .map(|(from, to)| (*from, to.iter().copied().collect()))
            .collect()
    }

    #[test]
    fn find_cycle_detects_and_ignores_cycles() {
        // No edges / acyclic chain / DAG -> None.
        assert!(find_cycle(&graph(&[])).is_none());
        assert!(find_cycle(&graph(&[(1, &[2]), (2, &[3]), (3, &[])])).is_none());
        assert!(find_cycle(&graph(&[(1, &[2, 3]), (2, &[3]), (3, &[])])).is_none());

        // A 2-cycle where the second node is reached as a neighbor before it is
        // colored — the case that regressed when unvisited nodes defaulted to
        // "done" instead of "unvisited".
        let c2 = find_cycle(&graph(&[(1, &[2]), (2, &[1])])).expect("2-cycle");
        assert_eq!(c2.iter().copied().collect::<HashSet<_>>(), [1, 2].into());

        // A 3-cycle with a dead-end branch (4 holds a lock but is not waiting).
        let c3 = find_cycle(&graph(&[(1, &[2, 4]), (2, &[3]), (3, &[1])])).expect("3-cycle");
        assert_eq!(c3.iter().copied().collect::<HashSet<_>>(), [1, 2, 3].into());

        // A self-loop (a transaction waiting on itself should never happen, but
        // the detector must not miss it).
        assert!(find_cycle(&graph(&[(1, &[1])])).is_some());
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

    #[tokio::test]
    async fn idle_transaction_is_reaped_and_its_locks_released() {
        // A client that opens a transaction and goes silent without
        // disconnecting must not hold its locks forever.
        let h = start_with_idle(Some(Duration::from_millis(150)));
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        let b = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        // Session A takes a write lock and then abandons the transaction.
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();

        // B blocks on A's lock while A still holds it, so wait for the reaper.
        // Once it fires, A's write is rolled back and B sees an empty table.
        let reply = h
            .handle
            .run_batch(b, "SELECT id FROM t".into())
            .await
            .unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "B must proceed once the abandoned transaction is reaped: {:?}",
            reply.outcome.error
        );
        assert_eq!(ids(&reply), Vec::<i64>::new(), "the reaped write is undone");

        // A is told its transaction was reaped rather than left to discover it
        // at a COMMIT that fails for a confusing reason.
        let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        assert_eq!(error_number(&reply), Some(1205));
        // The signal fires once; the now-transactionless COMMIT then reports the
        // ordinary 3902.
        let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        assert_eq!(error_number(&reply), Some(3902));
    }

    #[tokio::test]
    async fn the_sweep_runs_on_time_even_with_a_batch_parked_further_out() {
        // A worker must not sleep past the sweep just because the nearest parked
        // deadline is further out: the abandoned transaction the parked batch is
        // waiting on would not be reaped until that deadline, and the waiter
        // would die of its own timeout (1205) first — the very lock it was
        // waiting for having been reclaimable the whole time.
        //
        // Single-worker, because with several workers a sibling holding an
        // earlier deadline snapshot can wake and sweep anyway, hiding this.
        let h = start_single_worker(Some(Duration::from_millis(150)));
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        let b = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        // A abandons a transaction holding a write lock.
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();
        // B parks on A's lock with a 5 s deadline — far beyond the 150 ms sweep.
        let reply = h
            .handle
            .run_batch(b, "SELECT id FROM t".into())
            .await
            .unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "B must be unblocked by the sweep, not killed at its own deadline: {:?}",
            reply.outcome.error
        );
        assert_eq!(ids(&reply), Vec::<i64>::new(), "A's write was rolled back");
    }

    #[tokio::test]
    async fn active_transaction_is_not_reaped() {
        // The reaper must only touch *idle* sessions: a session that keeps
        // working keeps its transaction, however long it stays open.
        let h = start_with_idle(Some(Duration::from_millis(150)));
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();
        // Keep touching the session across more than the idle timeout.
        for i in 2..=6 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let reply = h
                .handle
                .run_batch(a, format!("INSERT INTO t VALUES ({i})"))
                .await
                .unwrap();
            assert_eq!(
                error_number(&reply),
                None,
                "an active session must keep its transaction"
            );
        }
        // The transaction survived and commits everything it wrote.
        let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
        let reply = h
            .handle
            .run_batch(a, "SELECT id FROM t ORDER BY id".into())
            .await
            .unwrap();
        assert_eq!(ids(&reply), vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn idle_reaper_can_be_disabled() {
        // With the reaper off, an idle transaction is left alone.
        let h = start_with_idle(None);
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        // The transaction is still open and still commits.
        let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
        let reply = h
            .handle
            .run_batch(a, "SELECT id FROM t".into())
            .await
            .unwrap();
        assert_eq!(ids(&reply), vec![1]);
    }

    /// A bare Scheduler + Engine, for pinning the sweep's guards directly
    /// instead of racing real timers.
    fn bare(label: &str, idle: Option<Duration>) -> (Engine, Scheduler, PathBuf) {
        let path = unique_temp_path(label);
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let engine = Engine::new(storage).expect("engine");
        (engine, Scheduler::new(LOCK_WAIT_TIMEOUT, idle), path)
    }

    #[test]
    fn a_running_batch_is_never_reaped_however_idle_the_session_looks() {
        // The reaper's whole safety argument: while a batch runs its context has
        // been moved out by take_ctx, so the session reports no open
        // transaction and the sweep cannot select it. Pinned directly, with a
        // zero idle timeout so that guard is the *only* thing protecting it.
        let (engine, mut sched, path) = bare("reap-running", Some(Duration::ZERO));
        let id = sched.sessions.open("truthdb".into(), "sa".into());
        {
            let ctx = &mut sched.sessions.get_mut(id).expect("session").txn_ctx;
            engine
                .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", ctx)
                .expect("create");
            engine
                .sql_batch("BEGIN TRAN; INSERT INTO t VALUES (1);", ctx)
                .expect("begin");
        }
        assert!(
            sched
                .sessions
                .get(id)
                .expect("session")
                .txn_ctx
                .has_open_transaction(),
            "precondition: the session holds an open transaction"
        );

        // Simulate the batch being dispatched: the context moves to the worker.
        let ctx = sched.take_ctx(id);
        assert!(
            !sched
                .sessions
                .get(id)
                .expect("session")
                .txn_ctx
                .has_open_transaction(),
            "take_ctx must leave the session reporting no open transaction"
        );
        assert!(
            !sched.reap_idle_txns(&engine),
            "a session whose batch is running must never be reaped"
        );

        // Restoring it makes the very same session reapable — proving the test
        // above is not passing merely because nothing is ever reapable.
        sched.finish(&engine, id, ctx);
        assert!(
            sched.reap_idle_txns(&engine),
            "once idle again, the transaction is reaped"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_session_with_a_parked_batch_is_not_reaped() {
        // A parked batch is waiting on locks, not abandoned; its own deadline
        // reaps it. Reaping its transaction underneath it would run the batch
        // against a rolled-back transaction.
        let (engine, mut sched, path) = bare("reap-parked", Some(Duration::ZERO));
        let id = sched.sessions.open("truthdb".into(), "sa".into());
        {
            let ctx = &mut sched.sessions.get_mut(id).expect("session").txn_ctx;
            engine
                .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", ctx)
                .expect("create");
            engine
                .sql_batch("BEGIN TRAN; INSERT INTO t VALUES (1);", ctx)
                .expect("begin");
        }
        let (reply, _rx) = oneshot::channel();
        sched.parked.push_back(Parked {
            session: id,
            sql: "SELECT id FROM t".into(),
            params: Vec::new(),
            cancel: Arc::new(AtomicBool::new(false)),
            reply,
            needs: Vec::new(),
            deadline: Instant::now() + Duration::from_secs(5),
        });
        assert!(
            !sched.reap_idle_txns(&engine),
            "a session with a parked batch must not be reaped"
        );

        // Drop the parked entry and the same session is reaped — the guard, not
        // some other condition, is what protected it.
        sched.parked.clear();
        assert!(
            sched.reap_idle_txns(&engine),
            "with nothing parked, the idle transaction is reaped"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn the_pool_actually_spawns_a_maintenance_thread() {
        // `housekeeping_runs_with_no_worker_free_to_do_it` builds `Shared` by
        // hand and starts the thread itself, so it would pass just as happily
        // if the supervisor never spawned one. This covers the wiring: run the
        // real `spawn_engine` path and wait for a maintenance thread to report
        // in. (A sibling test's pool satisfying this is fine — it is the same
        // supervisor code either way; what fails is nobody spawning one at all.)
        let h = start_with_idle(Some(Duration::from_millis(150)));
        let deadline = Instant::now() + Duration::from_secs(5);
        while MAINTENANCE_STARTS.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            MAINTENANCE_STARTS.load(Ordering::Relaxed) > 0,
            "the pool spawns a maintenance thread"
        );
        drop(h);
    }

    #[test]
    fn the_maintenance_thread_sleeps_between_sweeps_whatever_is_parked() {
        // An expired waiter that `reap_expired` must NOT reap (its locks are
        // free, so it is queued for a worker rather than blocked) keeps a
        // deadline in the past for as long as it sits there. A sweeper that
        // derived its sleep from that deadline would compute zero and spin,
        // taking the scheduler mutex thousands of times a second — and would do
        // it precisely while every worker was busy, which is the case this
        // thread exists for.
        let (engine, mut sched, path) = bare("no-spin", Some(Duration::from_millis(50)));
        let id = sched.sessions.open("truthdb".into(), "sa".into());
        let (reply, _rx) = oneshot::channel();
        sched.parked.push_back(Parked {
            session: id,
            sql: "SELECT 1".into(),
            params: Vec::new(),
            cancel: Arc::new(AtomicBool::new(false)),
            reply,
            needs: vec![(Resource::Table(1), LockMode::Shared)],
            deadline: Instant::now() - Duration::from_secs(60),
        });
        let (_tx, rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            engine: Arc::new(engine),
            scheduler: Mutex::new(sched),
            rx: Mutex::new(rx),
            stop: AtomicBool::new(false),
            idle: Mutex::new(()),
            wake: Condvar::new(),
        });
        MAINTENANCE_SWEEPS.store(0, Ordering::Relaxed);
        let keeper = Arc::clone(&shared);
        let maintenance = std::thread::spawn(move || maintenance_loop(&keeper));
        std::thread::sleep(Duration::from_millis(200));
        let sweeps = MAINTENANCE_SWEEPS.load(Ordering::Relaxed);
        {
            let _idle = shared.idle.lock().expect("idle mutex poisoned");
            shared.stop.store(true, Ordering::Release);
        }
        shared.wake.notify_all();
        maintenance.join().expect("maintenance thread");

        // 200ms at a 50ms interval is ~4 sweeps. A spin measures in thousands,
        // so the bound is loose enough not to be a timing test.
        assert!(
            sweeps <= 20,
            "the thread slept between sweeps; it ran {sweeps} in 200ms"
        );
        assert!(
            shared
                .scheduler
                .lock()
                .expect("scheduler poisoned")
                .parked
                .len()
                == 1,
            "and it left the grantable waiter for a worker, rather than reaping it"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn housekeeping_runs_with_no_worker_free_to_do_it() {
        // The reapers are the engine's safety valves and must not be hostage to
        // the pool: a worker only sweeps between batches, so a busy pool used to
        // mean no sweep. Pinned in its strongest form — a pool with *no workers
        // at all*, which no amount of load can distinguish itself from — where
        // only the maintenance thread can do it.
        let (engine, mut sched, path) = bare("maintenance", Some(Duration::from_millis(50)));
        let id = sched.sessions.open("truthdb".into(), "sa".into());
        {
            let ctx = &mut sched.sessions.get_mut(id).expect("session").txn_ctx;
            engine
                .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", ctx)
                .expect("create");
            engine
                .sql_batch("BEGIN TRAN; INSERT INTO t VALUES (1);", ctx)
                .expect("begin");
            assert!(
                ctx.has_open_transaction(),
                "the session starts with an open txn"
            );
        }
        // Keep the sender alive so the (worker-less) channel stays open.
        let (_tx, rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            engine: Arc::new(engine),
            scheduler: Mutex::new(sched),
            rx: Mutex::new(rx),
            stop: AtomicBool::new(false),
            idle: Mutex::new(()),
            wake: Condvar::new(),
        });
        let keeper = Arc::clone(&shared);
        let maintenance = std::thread::spawn(move || maintenance_loop(&keeper));

        let deadline = Instant::now() + Duration::from_secs(5);
        let reaped = loop {
            {
                let sched = shared.scheduler.lock().expect("scheduler poisoned");
                if !sched
                    .sessions
                    .get(id)
                    .expect("session")
                    .txn_ctx
                    .has_open_transaction()
                {
                    break true;
                }
            }
            if Instant::now() > deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        {
            let _idle = shared.idle.lock().expect("idle mutex poisoned");
            shared.stop.store(true, Ordering::Release);
        }
        shared.wake.notify_all();
        maintenance.join().expect("maintenance thread");
        assert!(
            reaped,
            "the idle transaction was reaped with no worker to do it"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_parked_batch_whose_locks_are_free_is_never_reaped_as_a_victim() {
        // The deadline is a backstop for a batch stuck behind someone else's
        // lock. A waiter whose locks are free is not stuck — it is queued for a
        // worker — so reaping it would report a conflict (1205) that never
        // existed.
        let (engine, mut sched, path) = bare("reap-grantable", None);
        let id = sched.sessions.open("truthdb".into(), "sa".into());
        let (reply, _rx) = oneshot::channel();
        // Parked, deadline long gone, and nothing holds the lock it wants.
        sched.parked.push_back(Parked {
            session: id,
            sql: "SELECT 1".into(),
            params: Vec::new(),
            cancel: Arc::new(AtomicBool::new(false)),
            reply,
            needs: vec![(Resource::Table(1), LockMode::Shared)],
            deadline: Instant::now() - Duration::from_secs(60),
        });
        sched.reap_expired(&engine);
        assert_eq!(
            sched.parked.len(),
            1,
            "an expired waiter whose locks are free is not a deadlock victim"
        );

        // The same waiter, once something actually blocks it, is reaped — so it
        // is grantability doing the work above, not a dead reaper.
        sched
            .locks
            .grant(999, Resource::Table(1), LockMode::Exclusive);
        sched.reap_expired(&engine);
        assert!(
            sched.parked.is_empty(),
            "an expired waiter that is genuinely blocked is still the victim"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn reaped_session_is_told_instead_of_silently_autocommitting() {
        // A client that comes back believing it is still in a transaction must
        // not have its statements silently autocommit.
        let h = start_with_idle(Some(Duration::from_millis(150)));
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        // The next batch is told the transaction is gone, and does not run.
        let reply = h
            .handle
            .run_batch(a, "INSERT INTO t VALUES (2)".into())
            .await
            .unwrap();
        assert_eq!(
            error_number(&reply),
            Some(1205),
            "the reap must be reported, not swallowed"
        );

        // The signal fires once; the session is usable again afterwards.
        let reply = h
            .handle
            .run_batch(a, "SELECT id FROM t".into())
            .await
            .unwrap();
        assert_eq!(error_number(&reply), None);
        assert_eq!(
            ids(&reply),
            Vec::<i64>::new(),
            "the reaped write is undone, and the rejected INSERT never applied"
        );
    }

    #[tokio::test]
    async fn a_reaped_transaction_leaves_no_savepoints_behind() {
        // A savepoint holds the undo-log offset of the transaction that recorded
        // it. One surviving a reap would let ROLLBACK TRANSACTION find a stale
        // entry in the session's NEXT transaction and hand a dead offset to the
        // undo log — silently discarding committed work, or panicking.
        let h = start_with_idle(Some(Duration::from_millis(150)));
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(
                a,
                "BEGIN TRAN; INSERT INTO t VALUES (1); SAVE TRANSACTION sp1;".into(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Drain the one-shot reap signal.
        let reply = h.handle.run_batch(a, "SELECT 1".into()).await.unwrap();
        assert_eq!(error_number(&reply), Some(1205));

        // sp1 belonged to the reaped transaction: rolling back to it in a new
        // transaction must error 3908, not silently truncate this one's work.
        let reply = h
            .handle
            .run_batch(
                a,
                "BEGIN TRAN; INSERT INTO t VALUES (7); INSERT INTO t VALUES (8); ROLLBACK TRANSACTION sp1;"
                    .into(),
            )
            .await
            .unwrap();
        assert_eq!(
            error_number(&reply),
            Some(3908),
            "a savepoint from the reaped transaction must not survive"
        );
        h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();

        // And the new transaction's work was never silently discarded.
        h.handle
            .run_batch(
                a,
                "BEGIN TRAN; INSERT INTO t VALUES (7); INSERT INTO t VALUES (8); COMMIT;".into(),
            )
            .await
            .unwrap();
        let reply = h
            .handle
            .run_batch(a, "SELECT id FROM t ORDER BY id".into())
            .await
            .unwrap();
        assert_eq!(ids(&reply), vec![7, 8], "both committed rows survive");
    }

    #[tokio::test]
    async fn writer_blocks_reader_until_commit() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;

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
    async fn point_writers_to_different_rows_run_concurrently() {
        // The Stage 12 row-lock win: two transactions updating *different* rows
        // of one table no longer serialize (Table IX + distinct Row X locks).
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
        h.handle
            .run_batch(
                a,
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT); INSERT INTO t VALUES (1,0),(2,0);".into(),
            )
            .await
            .unwrap();

        // A holds Row X on id = 1 inside an open transaction.
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 10 WHERE id = 1;".into())
            .await
            .unwrap();

        // B updates id = 2 — a different row — and must complete without waiting
        // for A's commit.
        let out = tokio::time::timeout(
            Duration::from_secs(3),
            h.handle
                .run_batch(b, "UPDATE t SET v = 20 WHERE id = 2".into()),
        )
        .await
        .expect("a point write to a different row must not block")
        .unwrap();
        assert!(error_number(&out).is_none(), "{:?}", out.outcome.error);

        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    }

    #[tokio::test]
    async fn point_writers_to_the_same_row_serialize() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
        h.handle
            .run_batch(
                a,
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT); INSERT INTO t VALUES (1,0);"
                    .into(),
            )
            .await
            .unwrap();
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 10 WHERE id = 1;".into())
            .await
            .unwrap();

        // B updates the *same* row (id = 1): it must block on A's Row X.
        let handle_b = h.handle.clone();
        let write = tokio::spawn(async move {
            handle_b
                .run_batch(b, "UPDATE t SET v = 20 WHERE id = 1".into())
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!write.is_finished(), "same-row writer must block");

        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(5), write)
            .await
            .expect("writer unblocks after commit")
            .unwrap();
        assert!(error_number(&out).is_none(), "{:?}", out.outcome.error);
    }

    #[tokio::test]
    async fn read_uncommitted_sees_uncommitted_rows_without_blocking() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;

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
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;

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
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;

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
    async fn deadlock_is_broken_by_the_waits_for_graph_not_the_timeout() {
        // A 30 s wait timeout: if the deadlock were only broken by the timeout
        // backstop this would not resolve for 30 s. The waits-for-graph detector
        // must break it the instant the cycle closes, so the whole thing
        // finishes well under the timeout.
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
        for stmt in [
            "CREATE TABLE a (id INT NOT NULL PRIMARY KEY)",
            "CREATE TABLE b (id INT NOT NULL PRIMARY KEY)",
            "INSERT INTO a VALUES (1)",
            "INSERT INTO b VALUES (1)",
        ] {
            h.handle.run_batch(a, stmt.into()).await.unwrap();
        }
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE a SET id = id".into())
            .await
            .unwrap();
        h.handle
            .run_batch(b, "BEGIN TRAN; UPDATE b SET id = id".into())
            .await
            .unwrap();

        let ha = h.handle.clone();
        let a_waits =
            tokio::spawn(async move { ha.run_batch(a, "UPDATE b SET id = id".into()).await });
        let hb = h.handle.clone();
        let b_waits =
            tokio::spawn(async move { hb.run_batch(b, "UPDATE a SET id = id".into()).await });

        // Both resolve far sooner than the 30 s timeout — proving graph detection.
        let a_out = tokio::time::timeout(Duration::from_secs(3), a_waits)
            .await
            .expect("graph must break the deadlock well under the timeout")
            .unwrap()
            .unwrap();
        let b_out = tokio::time::timeout(Duration::from_secs(3), b_waits)
            .await
            .expect("graph must break the deadlock well under the timeout")
            .unwrap()
            .unwrap();

        let victims = [&a_out, &b_out]
            .iter()
            .filter(|o| error_number(o) == Some(1205))
            .count();
        assert_eq!(victims, 1, "exactly one transaction is the deadlock victim");
    }

    #[tokio::test]
    async fn deadlock_through_a_fifo_yield_is_detected_by_the_graph() {
        // A deadlock whose cycle passes through a FIFO anti-barging yield (not a
        // held-lock conflict): A holds t1; C parks wanting t1+t2; A then wants
        // the *free* t2 but yields to C, which is queued ahead for it. The graph
        // must model that yield edge and break the cycle under the 30 s timeout.
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let c = h.handle.open_session(String::new(), String::new()).await;
        for stmt in [
            "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
            "CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY)",
            "INSERT INTO t1 VALUES (1)",
            "INSERT INTO t2 VALUES (1)",
        ] {
            h.handle.run_batch(a, stmt.into()).await.unwrap();
        }
        // A holds X(t1).
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t1 SET id = id".into())
            .await
            .unwrap();
        // C wants t2 then t1 (held by A) → parks, now queued ahead for t2.
        let hc = h.handle.clone();
        let c_waits = tokio::spawn(async move {
            hc.run_batch(
                c,
                "BEGIN TRAN; UPDATE t2 SET id = id; UPDATE t1 SET id = id".into(),
            )
            .await
        });
        // Ensure C is parked before A asks for t2, so A queues behind it.
        tokio::time::sleep(Duration::from_millis(250)).await;
        // A wants the free t2 but yields to C (ahead) → parks → FIFO cycle.
        let ha = h.handle.clone();
        let a_waits =
            tokio::spawn(async move { ha.run_batch(a, "UPDATE t2 SET id = id".into()).await });

        let a_out = tokio::time::timeout(Duration::from_secs(3), a_waits)
            .await
            .expect("graph must break the FIFO deadlock well under the timeout")
            .unwrap()
            .unwrap();
        let c_out = tokio::time::timeout(Duration::from_secs(3), c_waits)
            .await
            .expect("graph must break the FIFO deadlock well under the timeout")
            .unwrap()
            .unwrap();
        let victims = [&a_out, &c_out]
            .iter()
            .filter(|o| error_number(o) == Some(1205))
            .count();
        assert_eq!(victims, 1, "exactly one transaction is the deadlock victim");
    }

    #[tokio::test]
    async fn repeatable_read_holds_shared_lock_and_blocks_a_writer() {
        let h = start(Duration::from_secs(30));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
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
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
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
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
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
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
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
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;
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

    /// The worker pool's core correctness stress test: many sessions run money
    /// transfers concurrently, some rolled back at random, and the total across
    /// all accounts must be exactly conserved — no lost updates, no torn
    /// transactions, no money created or destroyed by the concurrent plumbing
    /// (take/return of session context, parking, waking, draining).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_transfers_conserve_the_total() {
        const ACCOUNTS: i64 = 8;
        const TASKS: usize = 16;
        const TRANSFERS: usize = 25;
        const INITIAL: i64 = 1000;

        let h = start(Duration::from_secs(30));
        let setup = h.handle.open_session(String::new(), String::new()).await;
        h.handle
            .run_batch(
                setup,
                "CREATE TABLE accounts (id INT NOT NULL PRIMARY KEY, balance INT NOT NULL)".into(),
            )
            .await
            .unwrap();
        for id in 1..=ACCOUNTS {
            h.handle
                .run_batch(
                    setup,
                    format!("INSERT INTO accounts VALUES ({id}, {INITIAL})"),
                )
                .await
                .unwrap();
        }

        let mut tasks = Vec::new();
        for t in 0..TASKS {
            let handle = h.handle.clone();
            tasks.push(tokio::spawn(async move {
                let session = handle.open_session(String::new(), String::new()).await;
                // A deterministic per-task PRNG (an LCG) — reproducible, no dep.
                let mut rng: u64 =
                    0x9E37_79B9_7F4A_7C15 ^ (t as u64).wrapping_mul(0x2545_F491_4F6C_DD1D);
                let mut next = move || {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    rng >> 33
                };
                for _ in 0..TRANSFERS {
                    let a = (next() % ACCOUNTS as u64) as i64 + 1;
                    // Force a distinct counterparty: (a % N) + 1 is never a.
                    let b = (a % ACCOUNTS) + 1;
                    let amount = (next() % 50) as i64 + 1;
                    // One in ten transactions rolls back; conservation must hold
                    // either way.
                    let close = if next() % 10 == 0 {
                        "ROLLBACK"
                    } else {
                        "COMMIT"
                    };
                    let sql = format!(
                        "BEGIN TRAN; \
                         UPDATE accounts SET balance = balance - {amount} WHERE id = {a}; \
                         UPDATE accounts SET balance = balance + {amount} WHERE id = {b}; \
                         {close};"
                    );
                    // A deadlock victim (1205) rolls back cleanly; just retry it.
                    loop {
                        let reply = handle.run_batch(session, sql.clone()).await.unwrap();
                        match error_number(&reply) {
                            Some(1205) => continue,
                            Some(other) => panic!("unexpected error {other} on transfer"),
                            None => break,
                        }
                    }
                }
                handle.close_session(session);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let reply = h
            .handle
            .run_batch(setup, "SELECT balance FROM accounts".into())
            .await
            .unwrap();
        let total: i64 = ids(&reply).iter().sum();
        assert_eq!(
            total,
            ACCOUNTS * INITIAL,
            "money was created or destroyed under concurrency"
        );
    }

    /// Like the conservation test, but each transfer is a *multi-batch*
    /// transaction across two tables, so locks are taken incrementally and two
    /// transfers in opposite table order can genuinely deadlock. A 1205 victim
    /// rolls back and retries. Exercises cross-batch lock holding, the deadlock
    /// detector, and victim retry all under concurrent load — the total must
    /// still be conserved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_multi_table_transfers_survive_deadlocks() {
        const ACCOUNTS: i64 = 3;
        const TASKS: usize = 10;
        const TRANSFERS: usize = 12;
        const INITIAL: i64 = 1000;
        const TABLES: [&str; 2] = ["checking", "savings"];

        let h = start(Duration::from_secs(30));
        let setup = h.handle.open_session(String::new(), String::new()).await;
        for table in TABLES {
            h.handle
                .run_batch(
                    setup,
                    format!(
                        "CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, balance INT NOT NULL)"
                    ),
                )
                .await
                .unwrap();
            for id in 1..=ACCOUNTS {
                h.handle
                    .run_batch(
                        setup,
                        format!("INSERT INTO {table} VALUES ({id}, {INITIAL})"),
                    )
                    .await
                    .unwrap();
            }
        }

        let mut tasks = Vec::new();
        for t in 0..TASKS {
            let handle = h.handle.clone();
            tasks.push(tokio::spawn(async move {
                let session = handle.open_session(String::new(), String::new()).await;
                let mut rng: u64 =
                    0xDEAD_BEEF_0000_0001 ^ (t as u64).wrapping_mul(0x2545_F491_4F6C_DD1D);
                let mut next = move || {
                    rng = rng
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    rng >> 33
                };
                for _ in 0..TRANSFERS {
                    let src = TABLES[(next() % 2) as usize];
                    let dst = TABLES[(next() % 2) as usize];
                    let a = (next() % ACCOUNTS as u64) as i64 + 1;
                    let b = (next() % ACCOUNTS as u64) as i64 + 1;
                    let amount = (next() % 40) as i64 + 1;
                    let close = if next() % 8 == 0 {
                        "ROLLBACK"
                    } else {
                        "COMMIT"
                    };
                    // Deadlock victims only ever land on the two UPDATEs; BEGIN
                    // and COMMIT/ROLLBACK take no new locks. Retry the whole
                    // transaction (already rolled back) from the top.
                    'attempt: loop {
                        let steps = [
                            "BEGIN TRAN".to_string(),
                            format!("UPDATE {src} SET balance = balance - {amount} WHERE id = {a}"),
                            format!("UPDATE {dst} SET balance = balance + {amount} WHERE id = {b}"),
                            close.to_string(),
                        ];
                        for step in steps {
                            let reply = handle.run_batch(session, step).await.unwrap();
                            match error_number(&reply) {
                                Some(1205) => continue 'attempt,
                                Some(other) => panic!("unexpected error {other}"),
                                None => {}
                            }
                        }
                        break;
                    }
                }
                handle.close_session(session);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let mut total = 0i64;
        for table in TABLES {
            let reply = h
                .handle
                .run_batch(setup, format!("SELECT balance FROM {table}"))
                .await
                .unwrap();
            total += ids(&reply).iter().sum::<i64>();
        }
        assert_eq!(
            total,
            2 * ACCOUNTS * INITIAL,
            "money not conserved across tables under deadlock retries"
        );
    }

    /// A single waiter blocked on a legitimately-held lock (no cycle, so the
    /// graph detector finds nothing) must still be freed by the lock-wait
    /// timeout even when the pool then goes completely quiet — no further call
    /// arrives to wake a worker. Regression: a stale `earliest_deadline`
    /// snapshot used to let a worker block in an untimed `recv` holding the
    /// single-consumer rx mutex, disabling the reaper during quiescence and
    /// hanging the waiter indefinitely instead of timing it out.
    #[tokio::test]
    async fn lone_waiter_is_reaped_by_timeout_when_pool_goes_idle() {
        let h = start(Duration::from_millis(300));
        let a = h.handle.open_session(String::new(), String::new()).await;
        let b = h.handle.open_session(String::new(), String::new()).await;

        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        h.handle
            .run_batch(a, "INSERT INTO t VALUES (1)".into())
            .await
            .unwrap();
        // A holds X on t and stays idle inside its transaction (never commits).
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t SET id = id".into())
            .await
            .unwrap();

        // B's read conflicts with A's uncommitted write and parks. There is no
        // cycle (A waits on nothing), so only the timeout backstop can free it,
        // and no further calls arrive to wake a worker.
        let out = tokio::time::timeout(
            Duration::from_secs(5),
            h.handle.run_batch(b, "SELECT id FROM t".into()),
        )
        .await
        .expect("lone waiter must be reaped by the timeout, not hang forever")
        .unwrap();
        assert_eq!(
            error_number(&out),
            Some(1205),
            "the lone waiter should time out as a 1205 victim"
        );
    }
}
