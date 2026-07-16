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
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use truthdb_sql::error::SqlError;

use crate::engine::{Engine, EngineError};
use crate::lock::{LockManager, LockMode, Resource};
use crate::rel::{BatchOutcome, Isolation, ResultColumn, RowSet, StatementResult, TxnContext};
use crate::relstore::types::Datum;

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

/// How many rows one [`BatchEvent::Rows`] carries.
const EVENT_ROWS: usize = 256;

/// One event in a batch's reply.
///
/// A batch emits, per statement, either `Columns` followed by zero or more
/// `Rows` chunks and then `StatementDone`, or a bare `StatementDone` (DDL, a
/// row count). A batch-stopping `Error` may follow the statements that ran.
/// Every stream ends with exactly one terminal event — `Complete` or `Failed` —
/// unless the receiver is dropped first.
#[derive(Debug)]
pub enum BatchEvent {
    /// Starts a result set: its column metadata.
    Columns(Vec<ResultColumn>),
    /// A chunk of rows for the result set the last `Columns` opened.
    Rows(Vec<Vec<Datum>>),
    /// Ends one statement. `count` is its row count / rows-affected, or `None`
    /// for a statement that reports neither (DDL).
    StatementDone {
        count: Option<u64>,
        /// The transaction state to stamp on this statement's DONE
        /// (`DONE_INXACT`).
        in_transaction: bool,
        /// The DONE's `CurCmd` class — mssql-jdbc drops the count without it.
        command: crate::rel::DoneCommand,
    },
    /// Ends a statement that failed after its result set had begun streaming:
    /// closes the set (with a clean DONE — an error-flagged DONE without an
    /// ERROR token reads as "severe failure" to real drivers) so the stream
    /// stays framed for the statements that follow. The error itself travels
    /// separately — in the batch-final [`BatchEvent::Error`] for a continued
    /// error, or not at all for one a `CATCH` handled.
    StatementAborted { in_transaction: bool },
    /// A SQL error that stopped the batch. The statements before it kept their
    /// results, which were already sent.
    Error(SqlError),
    /// The handle `sp_prepare`/`sp_prepexec` allocated, reported to the client
    /// as a RETURNVALUE token. Sent after every statement's events, just
    /// before `Complete` — where SQL Server puts return values.
    PreparedHandle(i32),
    /// Terminal: the batch ended. Carries the batch's *final* transaction
    /// state, which is what the TDS transaction-manager path needs.
    Complete { in_transaction: bool },
    /// Terminal: the engine could not run the batch at all.
    Failed(EngineError),
}

/// A prepared-statement RPC (the `sp_prepare` handle family). `Prepare` and
/// `Unprepare` touch only session state; `Execute` and `PrepExec` run the
/// statement through the ordinary batch path (locks, parking, streaming).
pub enum PreparedRpc {
    Prepare {
        decls: String,
        stmt: String,
    },
    Execute {
        handle: i32,
        values: Vec<crate::rel::RpcParam>,
    },
    PrepExec {
        decls: String,
        stmt: String,
        values: Vec<crate::rel::RpcParam>,
    },
    Unprepare {
        handle: i32,
    },
    /// `sp_describe_first_result_set`: metadata discovery, no execution.
    Describe {
        tsql: String,
    },
}

/// A statement a session prepared: its text and parameter declarations, both
/// verbatim from `sp_prepare`. There is no cached plan to go stale — every
/// execution re-parses and re-binds against the live catalog, exactly like
/// `sp_executesql`, so DDL between prepare and execute behaves like SQL
/// Server's recompile-on-schema-change with no invalidation machinery.
struct PreparedStatement {
    decls: String,
    text: String,
}

/// The reply channel for one batch.
///
/// **Unbounded, deliberately.** A bounded queue would make the worker block
/// once the connection fell behind — and a worker blocks *inside* the batch,
/// which is to say while it still holds the batch's table locks (`finish` runs
/// after the batch returns). A client reading slowly would then hold `Table(t)`
/// S for as long as it liked, and `LOCK_WAIT_TIMEOUT` is 5 s, so every other
/// session touching that table would be reaped with a 1205 naming a deadlock
/// that never happened. Nothing in this module can abort a *running* batch —
/// victims come only from the parked queue — so the engine could not even
/// respond. A hard cap on a reply's memory needs a reader that holds no S
/// locks (Stage 13's RCSI); until then, not blocking the worker is worth more
/// than the cap.
///
/// What it costs: a connection that never drains lets its reply accumulate. The
/// ceiling is the whole result — which is exactly what the non-streaming path
/// held *unconditionally*, for every client — so this is never worse, and for
/// any client that keeps up it is bounded by what is in flight.
pub struct BatchSink {
    tx: mpsc::UnboundedSender<BatchEvent>,
    /// A handle `sp_prepexec` allocated, reported as a `PreparedHandle` event
    /// just before `Complete` — return values follow every result set.
    prepared_handle: Option<i32>,
}

impl BatchSink {
    fn new(tx: mpsc::UnboundedSender<BatchEvent>) -> BatchSink {
        BatchSink {
            tx,
            prepared_handle: None,
        }
    }

    /// Sends one event. Never blocks: `UnboundedSender::send` is a plain
    /// function, which is the point (see the type's docs). `false` once the
    /// receiver is gone — the client disconnected, or its connection task was
    /// dropped — which is the producer's signal to stop.
    fn send(&self, event: BatchEvent) -> bool {
        if matches!(event, BatchEvent::Complete { .. })
            && let Some(handle) = self.prepared_handle
        {
            let _ = self.tx.send(BatchEvent::PreparedHandle(handle));
        }
        self.tx.send(event).is_ok()
    }

    /// Sends a finished outcome as events — the reply of a batch that never
    /// ran (the parked deadlock victim) and the tests' shorthand. A batch that
    /// runs streams through the [`crate::rel::BatchEmitter`] impl below
    /// instead, stamping each DONE with its own statement's state; here every
    /// DONE carries the one final state, which is all an error-only reply has.
    fn send_outcome(&self, outcome: BatchOutcome, in_transaction: bool) {
        for result in outcome.results {
            let sent = match result {
                StatementResult::Rows(rowset) => self.send_rowset(rowset, in_transaction),
                StatementResult::RowsAffected(n) => self.send(BatchEvent::StatementDone {
                    count: Some(n),
                    in_transaction,
                    command: crate::rel::DoneCommand::Other,
                }),
                StatementResult::Done => self.send(BatchEvent::StatementDone {
                    count: None,
                    in_transaction,
                    command: crate::rel::DoneCommand::Other,
                }),
            };
            if !sent {
                return;
            }
        }
        if let Some(error) = outcome.error
            && !self.send(BatchEvent::Error(error))
        {
            return;
        }
        self.send(BatchEvent::Complete { in_transaction });
    }

    /// Sends one result set: metadata, then rows in [`EVENT_ROWS`] chunks.
    fn send_rowset(&self, rowset: RowSet, in_transaction: bool) -> bool {
        let count = rowset.rows.len() as u64;
        if !self.send(BatchEvent::Columns(rowset.columns)) {
            return false;
        }
        // Taken from the front through the iterator, not `split_off`: splitting
        // hands back the *remainder* each time, so every chunk memmoves what is
        // left and a large result costs O(n²). This moves each row once.
        let mut rows = rowset.rows.into_iter();
        loop {
            let chunk: Vec<Vec<Datum>> = rows.by_ref().take(EVENT_ROWS).collect();
            if chunk.is_empty() {
                break;
            }
            if !self.send(BatchEvent::Rows(chunk)) {
                return false;
            }
        }
        self.send(BatchEvent::StatementDone {
            count: Some(count),
            in_transaction,
            command: crate::rel::DoneCommand::Select,
        })
    }

    /// Sends a batch's terminal events: its error, if it ended with one, then
    /// `Complete` with the post-batch transaction state (which the TDS
    /// transaction-manager path reads — it stays batch-final by design).
    fn send_tail(&self, error: Option<truthdb_sql::error::SqlError>, in_transaction: bool) {
        if let Some(error) = error
            && !self.send(BatchEvent::Error(error))
        {
            return;
        }
        self.send(BatchEvent::Complete { in_transaction });
    }
}

/// The worker-side face of the reply channel: the executor emits each
/// statement's results through this as it runs, which is what puts rows on
/// the wire while the batch still executes. Send failures mean the client is
/// gone; the batch still runs to completion (its effects do not depend on
/// anyone listening) and the disconnect path's cancel flag stops it early.
impl crate::rel::BatchEmitter for BatchSink {
    fn columns(&mut self, columns: Vec<ResultColumn>) {
        self.send(BatchEvent::Columns(columns));
    }

    fn rows(&mut self, rows: Vec<Vec<Datum>>) {
        self.send(BatchEvent::Rows(rows));
    }

    fn statement_done(
        &mut self,
        count: Option<u64>,
        in_transaction: bool,
        command: crate::rel::DoneCommand,
    ) {
        self.send(BatchEvent::StatementDone {
            count,
            in_transaction,
            command,
        });
    }

    fn statement_aborted(&mut self, in_transaction: bool) {
        self.send(BatchEvent::StatementAborted { in_transaction });
    }
}

/// Reassembles an event stream into a whole [`BatchReply`] — the shape every
/// caller that wants the entire result still asks for (the transaction-manager
/// path, the tests). Draining as the worker produces is what keeps this from
/// being a second copy on top of the first.
async fn collect_reply(
    events: &mut mpsc::UnboundedReceiver<BatchEvent>,
) -> Result<BatchReply, EngineError> {
    let mut results: Vec<StatementResult> = Vec::new();
    let mut error = None;
    // The result set currently streaming, if this statement opened one.
    let mut open: Option<RowSet> = None;
    while let Some(event) = events.recv().await {
        match event {
            BatchEvent::Columns(columns) => {
                open = Some(RowSet {
                    columns,
                    rows: Vec::new(),
                });
            }
            BatchEvent::Rows(mut rows) => {
                if let Some(rowset) = open.as_mut() {
                    rowset.rows.append(&mut rows);
                }
            }
            BatchEvent::StatementDone { count, .. } => results.push(match open.take() {
                Some(rowset) => StatementResult::Rows(rowset),
                None => match count {
                    Some(n) => StatementResult::RowsAffected(n),
                    None => StatementResult::Done,
                },
            }),
            // The aborted statement contributes no result; its partly-streamed
            // rowset is dropped, which is what the buffered path returned too.
            BatchEvent::StatementAborted { .. } => open = None,
            // A whole-reply caller has nowhere to carry a prepared handle —
            // only the TDS renderer (RETURNVALUE) consumes it.
            BatchEvent::PreparedHandle(_) => {}
            BatchEvent::Error(err) => error = Some(err),
            BatchEvent::Complete { in_transaction } => {
                return Ok(BatchReply {
                    outcome: BatchOutcome { results, error },
                    in_transaction,
                });
            }
            BatchEvent::Failed(err) => return Err(err),
        }
    }
    // The stream ended without a terminal event: the worker pool is gone.
    Err(EngineError::Unavailable)
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
    /// Statements prepared over the `sp_prepare` family, by handle. Dropped
    /// with the session (SQL Server scopes prepared handles the same way).
    prepared: HashMap<i32, PreparedStatement>,
    /// The next handle to allocate. Handles are opaque to the client and never
    /// reused within a session.
    next_prepared_handle: i32,
}

impl Default for Session {
    fn default() -> Self {
        Session {
            txn_ctx: TxnContext::default(),
            last_activity: Instant::now(),
            prepared: HashMap::new(),
            next_prepared_handle: 1,
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

    /// Stores a prepared statement for the session and returns its handle.
    fn prepare(&mut self, id: SessionId, decls: String, text: String) -> i32 {
        let session = self.sessions.entry(id).or_default();
        let handle = session.next_prepared_handle;
        session.next_prepared_handle += 1;
        session
            .prepared
            .insert(handle, PreparedStatement { decls, text });
        handle
    }

    /// Looks up a session's prepared statement by handle.
    fn prepared(&self, id: SessionId, handle: i32) -> Option<(String, String)> {
        self.sessions
            .get(&id)?
            .prepared
            .get(&handle)
            .map(|p| (p.decls.clone(), p.text.clone()))
    }

    /// Drops a prepared handle. `false` if the session never prepared it.
    fn unprepare(&mut self, id: SessionId, handle: i32) -> bool {
        self.sessions
            .get_mut(&id)
            .is_some_and(|s| s.prepared.remove(&handle).is_some())
    }

    /// Whether the session has an open explicit transaction — the state an
    /// immediate (no-batch) reply's DONE stamps as `DONE_INXACT`.
    fn in_transaction(&self, id: SessionId) -> bool {
        self.sessions
            .get(&id)
            .is_some_and(|s| s.txn_ctx.in_transaction())
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
        reply: BatchSink,
    },
    /// A prepared-statement RPC (`sp_prepare` handle family) on behalf of a
    /// session. `Execute`/`PrepExec` re-enter the ordinary batch path once the
    /// handle is resolved; `Prepare`/`Unprepare` answer immediately.
    RunRpc {
        session: SessionId,
        command: PreparedRpc,
        cancel: Arc<AtomicBool>,
        reply: BatchSink,
    },
    /// A native-protocol command (ES or SQL): rendered text.
    RunNative {
        command: String,
        reply: oneshot::Sender<Result<String, EngineError>>,
    },
    CloseSession {
        session: SessionId,
    },
}

/// What a worker took off the [`Inbox`].
enum Work {
    /// A call to dispatch.
    Call(EngineCall),
    /// No call — parked work may have become grantable, so drain.
    Drain,
}

/// The pool's inbound queue: calls waiting for a worker, and a nudge saying
/// parked work may now be grantable.
///
/// Not an `mpsc`, because a worker has to wait for *either* of those and a
/// channel receiver can only wait for a call. That was what pinned the deadlock
/// backstop to the worker threads: whoever reaps a victim releases locks that
/// rescue the waiters behind it, and nothing could reach a worker parked in
/// `recv` to say so — so the reaping had to happen on a worker, between
/// batches, which is to say only as often as the pool was free.
struct Inbox {
    state: Mutex<InboxState>,
    /// Signalled on a new call, a drain nudge, and on close.
    ready: Condvar,
}

struct InboxState {
    calls: VecDeque<EngineCall>,
    /// A pending drain. One flag rather than a count: draining is idempotent
    /// and runs everything grantable, so two nudges are one drain.
    drain: bool,
    /// No more calls will come — every handle is gone, or `shutdown` was
    /// called. Workers finish what is queued and exit.
    closed: bool,
}

impl Inbox {
    fn new() -> Self {
        Inbox {
            state: Mutex::new(InboxState {
                calls: VecDeque::new(),
                drain: false,
                closed: false,
            }),
            ready: Condvar::new(),
        }
    }

    /// Queues a call. Dropped on the floor once closed — the pool is going away
    /// and the caller's reply channel dies with it, which is how a caller finds
    /// out.
    fn send(&self, call: EngineCall) {
        let mut state = self.state.lock().expect("inbox poisoned");
        if state.closed {
            return;
        }
        state.calls.push_back(call);
        drop(state);
        self.ready.notify_one();
    }

    /// Asks some worker to look at the parked queue: locks were released and
    /// whatever they unblock still needs a thread to run it.
    fn nudge(&self) {
        let mut state = self.state.lock().expect("inbox poisoned");
        if state.closed {
            return;
        }
        state.drain = true;
        drop(state);
        self.ready.notify_one();
    }

    /// Closes the inbox and wakes every worker.
    fn close(&self) {
        let mut state = self.state.lock().expect("inbox poisoned");
        state.closed = true;
        drop(state);
        self.ready.notify_all();
    }

    /// Blocks until there is something to do. `None` once the inbox is closed
    /// and drained, which is a worker's signal to exit.
    fn next(&self) -> Option<Work> {
        let mut state = self.state.lock().expect("inbox poisoned");
        loop {
            if let Some(call) = state.calls.pop_front() {
                return Some(Work::Call(call));
            }
            if std::mem::take(&mut state.drain) {
                return Some(Work::Drain);
            }
            if state.closed {
                return None;
            }
            state = self.ready.wait(state).expect("inbox poisoned");
        }
    }
}

/// Closes the [`Inbox`] when the last [`EngineHandle`] goes.
///
/// Both shutdown paths matter: the server calls [`EngineHandle::shutdown`]
/// explicitly, while tests just drop the handle. An `Arc<Inbox>` the workers
/// also hold could never reach zero to signal the second, so this token — held
/// only by handles — does: its count reaching zero means exactly "no more calls
/// will ever arrive".
struct HandleToken(Arc<Inbox>);

impl Drop for HandleToken {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A cloneable handle to the engine's worker pool. Cheap to clone (shares the
/// sender).
#[derive(Clone)]
pub struct EngineHandle {
    inbox: Arc<Inbox>,
    /// Dropped with the last handle, which closes the inbox.
    _token: Arc<HandleToken>,
}

impl EngineHandle {
    /// Opens a session for a connection, recording its database and login for
    /// session intrinsics. Returns its id (or a placeholder if the engine is
    /// gone).
    pub async fn open_session(&self, database: String, login: String) -> SessionId {
        let (reply, rx) = oneshot::channel();
        self.inbox.send(EngineCall::OpenSession {
            database,
            login,
            reply,
        });
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
    ///
    /// Collects the whole reply, so it costs the memory the result needs. A
    /// caller that writes the rows straight out — the TDS gateway — should use
    /// [`Self::stream_rpc`] instead.
    pub async fn run_rpc_cancellable(
        &self,
        session: SessionId,
        sql: String,
        params: Vec<crate::rel::RpcParam>,
        cancel: Arc<AtomicBool>,
    ) -> Result<BatchReply, EngineError> {
        let mut events = self.stream_rpc(session, sql, String::new(), params, cancel);
        collect_reply(&mut events).await
    }

    /// Runs a SQL batch and returns its reply as a stream (see
    /// [`Self::stream_rpc`]).
    pub fn stream_batch(
        &self,
        session: SessionId,
        sql: String,
        cancel: Arc<AtomicBool>,
    ) -> mpsc::UnboundedReceiver<BatchEvent> {
        self.stream_rpc(session, sql, String::new(), Vec::new(), cancel)
    }

    /// Runs a batch and returns its reply as a stream of [`BatchEvent`]s, so a
    /// caller can write each chunk of rows out as it arrives instead of holding
    /// the whole result.
    ///
    /// Drain it until a terminal event, or drop it — dropping tells the worker
    /// to stop producing rows nobody will read. The worker never waits on the
    /// receiver (see [`BatchSink`]), so a slow reader costs only its own memory.
    ///
    /// An engine that is already gone comes back as a `Failed` event rather
    /// than a separate error return, so a caller has one shape to render.
    pub fn stream_rpc(
        &self,
        session: SessionId,
        sql: String,
        decls: String,
        params: Vec<crate::rel::RpcParam>,
        cancel: Arc<AtomicBool>,
    ) -> mpsc::UnboundedReceiver<BatchEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        // Unnamed values take the declaration list's names (mssql-jdbc sends
        // them unnamed), exactly as sp_execute binds them.
        let params = match bind_decl_names(&decls, params) {
            Ok(params) => params,
            Err(error) => {
                let sink = BatchSink::new(tx);
                sink.send(BatchEvent::Error(error));
                sink.send(BatchEvent::Complete {
                    in_transaction: false,
                });
                return rx;
            }
        };
        self.inbox.send(EngineCall::RunBatch {
            session,
            sql,
            params,
            cancel,
            reply: BatchSink::new(tx),
        });
        // A closed inbox drops the call, taking the sink with it, so the stream
        // ends with no terminal event — which every reader here turns into the
        // same "the engine is gone" the dead oneshot used to mean.
        rx
    }

    /// Runs a prepared-statement RPC (the `sp_prepare` handle family),
    /// streaming its reply exactly like [`Self::stream_rpc`].
    pub fn stream_prepared(
        &self,
        session: SessionId,
        command: PreparedRpc,
        cancel: Arc<AtomicBool>,
    ) -> mpsc::UnboundedReceiver<BatchEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inbox.send(EngineCall::RunRpc {
            session,
            command,
            cancel,
            reply: BatchSink::new(tx),
        });
        rx
    }

    /// Runs a native-protocol command (ES or SQL) and returns rendered text.
    pub async fn run_native(&self, command: String) -> Result<String, EngineError> {
        let (reply, rx) = oneshot::channel();
        self.inbox.send(EngineCall::RunNative { command, reply });
        rx.await.map_err(|_| EngineError::Unavailable)?
    }

    /// Closes a session (rolling back any open transaction — later milestone).
    pub fn close_session(&self, session: SessionId) {
        self.inbox.send(EngineCall::CloseSession { session });
    }

    /// Asks the worker pool to stop: the inbox closes and every worker wakes,
    /// finishes what is queued, and exits. Dropping the last handle does the
    /// same thing.
    pub fn shutdown(&self) {
        self.inbox.close();
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
    let inbox = Arc::new(Inbox::new());
    let shared = Arc::new(Shared {
        engine: Arc::new(engine),
        scheduler: Mutex::new(Scheduler::new(timeout, idle_txn_timeout)),
        inbox: Arc::clone(&inbox),
        stop: AtomicBool::new(false),
        idle: Mutex::new(()),
        wake: Condvar::new(),
        #[cfg(test)]
        sweeps: std::sync::atomic::AtomicUsize::new(0),
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
            // The workers are gone, and neither way of getting here sets the
            // flag: `shutdown` and the last handle dropping both just close the
            // inbox. Tell the maintenance thread, or it would outlive the pool.
            // Setting the flag under `idle` is what makes the wake reliable
            // rather than a race against its next sleep.
            {
                let _idle = supervisor.idle.lock().expect("idle mutex poisoned");
                supervisor.stop.store(true, Ordering::Release);
            }
            supervisor.wake.notify_all();
            let _ = maintenance.join();
            let mut sched = supervisor.scheduler.lock().expect("scheduler poisoned");
            for parked in sched.parked.drain(..) {
                parked
                    .reply
                    .send(BatchEvent::Failed(EngineError::Unavailable));
            }
        })
        .expect("spawn engine supervisor");
    (
        EngineHandle {
            _token: Arc::new(HandleToken(Arc::clone(&inbox))),
            inbox,
        },
        join,
    )
}

/// State shared by every worker thread.
struct Shared {
    /// The database engine. `&self` throughout, so the pool shares one `Arc`.
    engine: Arc<Engine>,
    /// Sessions + lock table + parked queue. Held only for lock decisions.
    scheduler: Mutex<Scheduler>,
    /// Inbound calls, plus the drain nudge a releaser uses to hand parked work
    /// to whichever worker is free.
    inbox: Arc<Inbox>,
    /// Set when a `Shutdown` is seen, so a worker between calls exits promptly
    /// rather than picking up more work.
    stop: AtomicBool,
    /// Companion mutex for [`Self::wake`]. Guards nothing — a `Condvar` needs
    /// one.
    idle: Mutex<()>,
    /// Wakes the maintenance thread out of its sleep at shutdown, so the pool
    /// does not wait out a whole sweep interval before exiting.
    wake: Condvar,
    /// This pool's maintenance sweeps, so a test can prove the thread sleeps
    /// between them rather than spinning. Per-pool, not global: the tests run
    /// in parallel in one binary, and a global counter measures every other
    /// pool's sweeps too.
    #[cfg(test)]
    sweeps: std::sync::atomic::AtomicUsize,
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
    reply: BatchSink,
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

/// The engine's housekeeping, on a thread that never runs a batch: the deadlock
/// backstop and the idle-transaction reaper.
///
/// Both used to run only on the workers, between calls, which made them exactly
/// as punctual as the pool was free — and the pool is `cores-2` threads, two on
/// a four-core box. A few long scans deferred them for as long as they ran,
/// which is backwards: the idle reaper exists to release the locks of a client
/// that has stopped responding, and a loaded engine is when that matters most.
/// Nothing a client does can delay this thread, because it never executes
/// anything on anyone's behalf.
///
/// It only *releases* locks. Running what that unblocks still needs a worker,
/// so it nudges the [`Inbox`] rather than doing it here — the pairing the old
/// worker sweep got for free by simply calling `drain_ready` next.
fn maintenance_loop(shared: &Arc<Shared>) {
    #[cfg(test)]
    MAINTENANCE_STARTS.fetch_add(1, Ordering::Relaxed);
    while !shared.stop.load(Ordering::Acquire) {
        // Sleep until the nearest deadline the reaper could act on, capped by
        // the idle sweep's interval and floored so no arrangement of parked
        // work or tuning knobs can turn this loop into a spin.
        let wait = {
            let sched = shared.scheduler.lock().expect("scheduler poisoned");
            let cap = sched.sweep_interval();
            match sched.earliest_reapable_deadline() {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()).min(cap),
                None => cap,
            }
            .max(MIN_SWEEP_INTERVAL)
        };
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
        shared.sweeps.fetch_add(1, Ordering::Relaxed);
        {
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            // One victim per sweep, and the floor on the sleep means a worker
            // has a chance to run what that release rescued before the next
            // one. The ordering matters: reaping a victim makes the waiter
            // behind it grantable, and sweeping again without letting that
            // happen would take a second victim where one would do.
            sched.reap_expired(&shared.engine);
            sched.reap_idle_txns(&shared.engine);
        }
        // Unconditionally, not just when something was released. Workers now
        // block indefinitely on the inbox, so a nudge that should have been
        // sent and was not is a batch parked forever rather than a batch
        // started late — this periodic one is the backstop the workers' old
        // `recv_timeout(wake_cap)` used to be, at the same cost: one wakeup per
        // interval on an idle engine, which finds nothing and sleeps again.
        shared.inbox.nudge();
    }
}

/// One worker thread: pull a call, dispatch it, repeat until shutdown.
fn worker_loop(shared: &Arc<Shared>) {
    while !shared.stop.load(Ordering::Acquire) {
        // Block until there is a call to run, or a releaser nudged us to look
        // at the parked queue. `None` means the inbox is closed and drained.
        let work = match shared.inbox.next() {
            Some(work) => work,
            None => break,
        };
        {
            // A call we just dequeued proves its session is not idle. Stamp it
            // before anything else looks: the maintenance thread sweeps at
            // arbitrary times and would otherwise reap the transaction of a
            // session whose next batch is already in hand.
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            match &work {
                Work::Call(EngineCall::RunBatch { session, .. })
                | Work::Call(EngineCall::RunRpc { session, .. }) => {
                    sched.sessions.touch(*session);
                }
                _ => {}
            }
        }
        drain_ready(shared);
        match work {
            Work::Drain => {}
            Work::Call(EngineCall::OpenSession {
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
            Work::Call(EngineCall::RunBatch {
                session,
                sql,
                params,
                cancel,
                reply,
            }) => dispatch_batch(shared, session, sql, params, cancel, reply),
            Work::Call(EngineCall::RunRpc {
                session,
                command,
                cancel,
                reply,
            }) => dispatch_rpc(shared, session, command, cancel, reply),
            Work::Call(EngineCall::RunNative { command, reply }) => {
                let _ = reply.send(shared.engine.execute(&command));
            }
            Work::Call(EngineCall::CloseSession { session }) => {
                shared
                    .scheduler
                    .lock()
                    .expect("scheduler poisoned")
                    .close_session(&shared.engine, session);
                drain_ready(shared);
            }
        }
    }
}

/// Resolves a prepared-statement RPC. `Prepare`/`Unprepare` touch only the
/// session's handle table and answer immediately; `Execute`/`PrepExec`
/// re-enter [`dispatch_batch`] with the resolved statement text, so locks,
/// parking and streaming behave exactly as for a plain batch. There is no
/// cached plan: execution re-parses and re-binds against the live catalog
/// (like `sp_executesql`), so DDL between prepare and execute needs no
/// invalidation — the next execute simply sees the new schema.
fn dispatch_rpc(
    shared: &Arc<Shared>,
    session: SessionId,
    command: PreparedRpc,
    cancel: Arc<AtomicBool>,
    mut reply: BatchSink,
) {
    // The immediate replies (no batch runs) still stamp DONE_INXACT from the
    // session's real transaction state.
    let immediate = |reply: &BatchSink, error: Option<SqlError>| {
        let in_transaction = {
            let sched = shared.scheduler.lock().expect("scheduler poisoned");
            sched.sessions.in_transaction(session)
        };
        if let Some(error) = error {
            reply.send(BatchEvent::Error(error));
        }
        reply.send(BatchEvent::Complete { in_transaction });
    };
    let missing_handle = |handle: i32| {
        SqlError::new(
            8179,
            16,
            1,
            format!("Could not find prepared statement with handle {handle}."),
        )
    };
    match command {
        PreparedRpc::Prepare { decls, stmt } => {
            // Parse now so a syntax error surfaces at prepare time, as SQL
            // Server's compile does. Binding stays at execute (names resolve
            // against the live catalog there) — a divergence: an unknown
            // table or column errors at execute, not prepare.
            if let Err(error) = truthdb_sql::parse(&stmt) {
                immediate(&reply, Some(error));
                return;
            }
            let handle = {
                let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.prepare(session, decls, stmt)
            };
            reply.send(BatchEvent::PreparedHandle(handle));
            immediate(&reply, None);
        }
        PreparedRpc::Unprepare { handle } => {
            let dropped = {
                let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.unprepare(session, handle)
            };
            immediate(&reply, (!dropped).then(|| missing_handle(handle)));
        }
        PreparedRpc::Describe { tsql } => match shared.engine.describe_first_result_set(&tsql) {
            Ok(rowset) => {
                let count = rowset.rows.len() as u64;
                let in_transaction = {
                    let sched = shared.scheduler.lock().expect("scheduler poisoned");
                    sched.sessions.in_transaction(session)
                };
                reply.send(BatchEvent::Columns(rowset.columns));
                reply.send(BatchEvent::Rows(rowset.rows));
                reply.send(BatchEvent::StatementDone {
                    count: Some(count),
                    in_transaction,
                    command: crate::rel::DoneCommand::Select,
                });
                reply.send(BatchEvent::Complete { in_transaction });
            }
            Err(error) => immediate(&reply, Some(error)),
        },
        PreparedRpc::Execute { handle, values } => {
            let resolved = {
                let sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.prepared(session, handle)
            };
            let Some((decls, text)) = resolved else {
                immediate(&reply, Some(missing_handle(handle)));
                return;
            };
            let values = match bind_decl_names(&decls, values) {
                Ok(values) => values,
                Err(error) => {
                    immediate(&reply, Some(error));
                    return;
                }
            };
            dispatch_batch(shared, session, text, values, cancel, reply);
        }
        PreparedRpc::PrepExec {
            decls,
            stmt,
            values,
        } => {
            if let Err(error) = truthdb_sql::parse(&stmt) {
                immediate(&reply, Some(error));
                return;
            }
            let handle = {
                let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.prepare(session, decls.clone(), stmt.clone())
            };
            reply.prepared_handle = Some(handle);
            let values = match bind_decl_names(&decls, values) {
                Ok(values) => values,
                Err(error) => {
                    immediate(&reply, Some(error));
                    return;
                }
            };
            dispatch_batch(shared, session, stmt, values, cancel, reply);
        }
    }
}

/// Names any unnamed value parameters from the declaration list, in order —
/// `sp_execute` values arrive unnamed on the wire, and seeding a batch
/// variable needs its name. A value that already has a name keeps it.
fn bind_decl_names(
    decls: &str,
    mut values: Vec<crate::rel::RpcParam>,
) -> Result<Vec<crate::rel::RpcParam>, SqlError> {
    let names = crate::rel::decl_names(decls);
    // An unnamed value with no declaration to name it is SQL Server's 8144.
    // (Fewer values than declarations is legal — a declared parameter the
    // statement never reads goes unmissed, and one it does read errors when
    // the variable lookup fails at execution. Extra NAMED values pass
    // through: they seed variables by their own names, which keeps the
    // `run_rpc` wrappers' seed-named-params contract intact.)
    if values
        .iter()
        .skip(names.len())
        .any(|value| value.name.is_empty())
    {
        return Err(SqlError::new(
            8144,
            16,
            2,
            "Procedure or function has too many arguments specified.",
        ));
    }
    for (value, name) in values.iter_mut().zip(names) {
        if value.name.is_empty() {
            value.name = name;
        }
    }
    Ok(values)
}

/// Acquires a batch's locks and runs it, or parks it behind a conflict. Either
/// way, drains anything the batch's completion (or a deadlock abort) unblocked.
fn dispatch_batch(
    shared: &Arc<Shared>,
    session: SessionId,
    sql: String,
    params: Vec<crate::rel::RpcParam>,
    cancel: Arc<AtomicBool>,
    reply: BatchSink,
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
    // Statement events stream out *while the batch runs* — the executor emits
    // each result as it is produced, and the send never blocks, so a client
    // that reads slowly delays neither this worker nor the locks it holds.
    let mut reply = reply;
    let outcome = shared
        .engine
        .sql_batch_streamed(&sql, &mut txn_ctx, &params, &mut reply);
    let in_transaction = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        sched.finish(&shared.engine, session, txn_ctx)
    };
    // The terminal events wait for the locks to settle: `Complete` carries the
    // post-batch transaction state, which only `finish` knows.
    match outcome {
        Ok(error) => reply.send_tail(error, in_transaction),
        Err(err) => {
            reply.send(BatchEvent::Failed(err));
        }
    }
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
    reply: BatchSink,
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

    /// The earliest deadline the reaper could actually act on.
    ///
    /// A waiter that is grantable is skipped, and that is load-bearing rather
    /// than an optimisation: [`Self::reap_expired`] refuses to reap one (it is
    /// queued for a worker, not blocked), so its deadline stays in the past for
    /// as long as it sits there. Letting that drive the sleep computes zero and
    /// spins a core against the scheduler mutex — and only while every worker
    /// is busy, since a free one drains the waiter away in microseconds.
    fn earliest_reapable_deadline(&self) -> Option<Instant> {
        (0..self.parked.len())
            .filter(|i| !self.grantable(*i))
            .map(|i| self.parked[i].deadline)
            .min()
    }

    /// The longest the maintenance thread may sleep: often enough to notice an
    /// idle transaction, and floored so no setting of a tuning knob can turn
    /// its loop into a spin (a test already passes `Duration::ZERO`).
    fn sweep_interval(&self) -> Duration {
        match self.idle_txn_timeout {
            Some(idle) => idle.min(self.lock_wait_timeout),
            // The reaper is disabled; there is nothing to be prompt for.
            None => self.lock_wait_timeout,
        }
        .max(MIN_SWEEP_INTERVAL)
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
            // An expired wait behind a LIVE holder is not a deadlock: SQL
            // Server raises 1205 only for real cycles and 1222 for a lock
            // wait that timed out. Reporting a false deadlock sends drivers
            // into retry loops for a condition retrying cannot fix.
            self.abort_parked_victim(engine, idx, lock_timeout_error());
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
    fn abort_parked_victim(&mut self, engine: &Engine, idx: usize, error: SqlError) {
        let victim = self.parked.remove(idx).expect("index in bounds");
        if let Some(state) = self.sessions.get_mut(victim.session) {
            engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(victim.session.raw());
        victim.reply.send_outcome(
            BatchOutcome {
                results: Vec::new(),
                error: Some(error),
            },
            false,
        );
    }

    /// Detects lock-wait *cycles* among the parked batches — a waits-for graph
    /// over the lock manager — and aborts the youngest transaction in each cycle
    /// (error 1205). A cycle can only form when a batch parks, so this runs the
    /// instant one does, breaking the deadlock immediately rather than after the
    /// wait-timeout backstop. Aborts victims until the graph is acyclic.
    fn detect_deadlock(&mut self, engine: &Engine) {
        while let Some(idx) = self.find_deadlock_victim() {
            self.abort_parked_victim(engine, idx, deadlock_victim_error());
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

fn deadlock_victim_error() -> SqlError {
    SqlError::new(
        1205,
        13,
        51,
        "Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.",
    )
}

/// A lock wait that outlived the timeout behind a LIVE holder — no cycle.
/// SQL Server's number for an expired lock wait is 1222.
fn lock_timeout_error() -> SqlError {
    SqlError::new(1222, 16, 56, "Lock request time out period exceeded.")
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

    #[tokio::test]
    async fn statement_dones_carry_their_own_transaction_state() {
        // Each DONE reports the transaction state after *its own* statement —
        // BEGIN and the SELECT inside the transaction say so, the COMMIT says
        // it ended — instead of the batch's final state stamped on all three.
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        let events = drain_events(h.handle.stream_batch(
            s,
            "BEGIN TRANSACTION; SELECT 1 AS one; COMMIT".into(),
            no_cancel(),
        ))
        .await;
        assert_eq!(done_flags(&events), [true, true, false]);
        assert!(
            matches!(
                events.last(),
                Some(BatchEvent::Complete {
                    in_transaction: false
                })
            ),
            "Complete carries the batch-final state: {events:?}"
        );
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

    #[tokio::test]
    async fn a_mid_scan_error_keeps_the_rows_already_streamed() {
        // A streamed SELECT that fails part-way has already emitted the rows
        // that preceded the failure — rows leave while the statement is still
        // running. The buffered path emitted nothing for a failed statement,
        // so any Columns/Rows here prove the stream is real.
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 600).await;
        // The WHERE divides by zero at id = 600, after 599 kept rows: two full
        // 256-row chunks are already out, the partial third is dropped.
        let events = drain_events(h.handle.stream_batch(
            s,
            "SELECT id FROM t WHERE 10 / (id - 600) > -100".into(),
            no_cancel(),
        ))
        .await;
        let streamed: usize = events
            .iter()
            .map(|event| match event {
                BatchEvent::Rows(rows) => rows.len(),
                _ => 0,
            })
            .sum();
        assert_eq!(streamed, 512, "two full chunks precede the failure");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, BatchEvent::Error(err) if err.number == 8134)),
            "the divide-by-zero still reaches the client: {events:?}"
        );
    }

    #[tokio::test]
    async fn a_caught_mid_scan_error_closes_the_open_rowset() {
        // A TRY/CATCH swallows the error, but the failed SELECT's result set
        // had already started streaming — it must be closed (StatementAborted)
        // before the CATCH's own result set opens, and no Error event follows.
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 300).await;
        let events = drain_events(
            h.handle.stream_batch(
                s,
                "BEGIN TRY SELECT id FROM t WHERE 10 / (id - 300) > -100 END TRY \
             BEGIN CATCH SELECT 99 AS caught END CATCH"
                    .into(),
                no_cancel(),
            ),
        )
        .await;
        let aborted = events
            .iter()
            .position(|e| matches!(e, BatchEvent::StatementAborted { .. }))
            .expect("the failed SELECT's rowset is closed");
        let caught = events
            .iter()
            .position(
                |e| matches!(e, BatchEvent::Columns(cols) if cols.first().is_some_and(|c| c.name == "caught")),
            )
            .expect("the CATCH's rowset follows");
        assert!(aborted < caught, "close before reopening: {events:?}");
        assert!(
            !events.iter().any(|e| matches!(e, BatchEvent::Error(_))),
            "a caught error never surfaces: {events:?}"
        );
    }

    #[tokio::test]
    async fn a_continued_mid_scan_error_closes_the_rowset_and_reports_last() {
        // Under XACT_ABORT OFF a non-fatal in-transaction error rolls back only
        // its statement and the batch continues — so the half-streamed rowset
        // closes, the following statements run, and the error is reported at
        // the end of the batch, exactly where the buffered path put it.
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 300).await;
        let events = drain_events(
            h.handle.stream_batch(
                s,
                "BEGIN TRANSACTION; SELECT id FROM t WHERE 10 / (id - 300) > -100; \
             SELECT 7 AS after; COMMIT"
                    .into(),
                no_cancel(),
            ),
        )
        .await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                BatchEvent::StatementAborted {
                    in_transaction: true
                }
            )),
            "the failed SELECT's rowset closes, still in-transaction: {events:?}"
        );
        // BEGIN, the surviving SELECT, then COMMIT — each DONE with its own state.
        assert_eq!(done_flags(&events), [true, true, false]);
        let error = events
            .iter()
            .position(|e| matches!(e, BatchEvent::Error(err) if err.number == 8134))
            .expect("the continued error is still reported");
        assert_eq!(error, events.len() - 2, "after every result: {events:?}");
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
        let (tx, _rx) = mpsc::unbounded_channel();
        let reply = BatchSink::new(tx);
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
    fn both_ways_of_shutting_the_pool_down_stop_every_thread() {
        // The server calls `shutdown` and then joins; the tests just drop the
        // handle. Only the first closes the inbox by itself — the second relies
        // on the handle token, since the `Arc<Inbox>` the workers hold can never
        // reach zero. Joining the supervisor is what proves it: it joins the
        // workers and the maintenance thread first, so it only returns if every
        // one of them noticed.
        for explicit in [true, false] {
            let path = unique_temp_path("shutdown");
            let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
            let engine = Engine::new(storage).expect("engine");
            let (handle, join) = spawn_engine(engine);
            if explicit {
                handle.shutdown();
                drop(handle);
            } else {
                drop(handle);
            }
            join.join().expect("the pool shut down");
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test]
    async fn a_batch_the_idle_reaper_unblocks_is_handed_to_a_worker() {
        // The reaper runs off-worker now, so releasing a lock and running what
        // that unblocks happen on different threads. Workers block on the inbox
        // indefinitely — there is no timeout to fall back on — so a reap that
        // did not nudge would leave this batch parked forever rather than late.
        let h = start_with_idle(Some(Duration::from_millis(150)));
        let a = h.handle.open_session("truthdb".into(), "sa".into()).await;
        let b = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
            .await
            .unwrap();
        // A abandons a write lock; B parks behind it and can only be rescued by
        // the maintenance thread reaping A and then nudging a worker.
        h.handle
            .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
            .await
            .unwrap();
        let reply = tokio::time::timeout(
            Duration::from_secs(10),
            h.handle.run_batch(b, "SELECT id FROM t".into()),
        )
        .await
        .expect("the rescued batch was handed to a worker, not left parked")
        .unwrap();
        assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
        assert_eq!(ids(&reply), Vec::<i64>::new(), "the reaped write is undone");
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
        let (tx, _rx) = mpsc::unbounded_channel();
        let reply = BatchSink::new(tx);
        sched.parked.push_back(Parked {
            session: id,
            sql: "SELECT 1".into(),
            params: Vec::new(),
            cancel: Arc::new(AtomicBool::new(false)),
            reply,
            needs: vec![(Resource::Table(1), LockMode::Shared)],
            deadline: Instant::now() - Duration::from_secs(60),
        });
        let shared = Arc::new(Shared {
            engine: Arc::new(engine),
            scheduler: Mutex::new(sched),
            inbox: Arc::new(Inbox::new()),
            stop: AtomicBool::new(false),
            idle: Mutex::new(()),
            wake: Condvar::new(),
            sweeps: std::sync::atomic::AtomicUsize::new(0),
        });
        let keeper = Arc::clone(&shared);
        let maintenance = std::thread::spawn(move || maintenance_loop(&keeper));
        std::thread::sleep(Duration::from_millis(200));
        let sweeps = shared.sweeps.load(Ordering::Relaxed);
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
        let shared = Arc::new(Shared {
            engine: Arc::new(engine),
            scheduler: Mutex::new(sched),
            inbox: Arc::new(Inbox::new()),
            stop: AtomicBool::new(false),
            idle: Mutex::new(()),
            wake: Condvar::new(),
            sweeps: std::sync::atomic::AtomicUsize::new(0),
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
        let (tx, _rx) = mpsc::unbounded_channel();
        let reply = BatchSink::new(tx);
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
        // Wide enough that BOTH conflicting batches park (forming the cycle)
        // before any deadline expires: since the 1222 split, an expired
        // waiter with no cycle is reaped as a lock timeout, and a loaded
        // runner delaying the second park past the deadline would otherwise
        // turn this test's deadlock into a 1222.
        let h = start(Duration::from_secs(2));
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
            Some(1222),
            "a lone waiter behind a live holder times out as 1222 — there is \
             no cycle, so 1205 would report a deadlock that never happened"
        );
    }

    /// Stage 12's exit case: the engine stays responsive during a large
    /// scan. A protocol heartbeat never touches the engine (the dispatcher
    /// answers it), so the meaningful half is a native search completing
    /// WHILE a worker is mid-scan holding the batch-long read gate — pinned
    /// by synchronizing on the scan's first Columns event (the worker is
    /// provably inside the batch) and asserting the search returns before
    /// the scan finishes. A regression to an exclusive gate blocks the
    /// search behind the whole scan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_search_is_answered_while_a_large_scan_runs() {
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 60_000).await;
        h.handle
            .run_native(
                r#"create index bench { "mappings": { "properties": { "title": { "type": "text" } } } }"#
                    .to_string(),
            )
            .await
            .expect("create index");
        h.handle
            .run_native(r#"insert document bench { "title": "hello world" }"#.to_string())
            .await
            .expect("insert doc");

        let mut events = h
            .handle
            .stream_batch(s, "SELECT id FROM t".into(), no_cancel());
        // The first Columns event: the worker is inside the batch, gate held.
        loop {
            match events.recv().await.expect("stream lives") {
                BatchEvent::Columns(_) => break,
                BatchEvent::Failed(err) => panic!("scan failed to start: {err}"),
                _ => {}
            }
        }

        let search = h
            .handle
            .run_native(r#"search bench {"query": {"match": {"title": "hello"}}}"#.to_string())
            .await
            .expect("search");
        assert!(search.contains("hello world"), "search answered: {search}");
        // The scan must have STILL BEEN RUNNING when the search returned.
        // The channel is unbounded, so "the next event" proves nothing —
        // instead drain everything already buffered at this instant without
        // blocking: if the batch's terminal event is among it, the batch
        // finished before the search did (an exclusive gate makes the search
        // wait out the whole scan, and this assertion catches exactly that).
        let mut already_finished = false;
        while let Ok(event) = events.try_recv() {
            if matches!(event, BatchEvent::Complete { .. } | BatchEvent::Failed(_)) {
                already_finished = true;
            }
        }
        assert!(
            !already_finished,
            "the search must complete while the scan is mid-stream, not after it"
        );
        drain_events(events).await;
    }

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

    fn int_param(value: i32) -> crate::rel::RpcParam {
        crate::rel::RpcParam {
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

    #[tokio::test]
    async fn prepare_execute_unprepare_roundtrip() {
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 5).await;

        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Prepare {
                decls: "@p1 int".into(),
                stmt: "SELECT id FROM t WHERE id = @p1".into(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");
        let handle = handle_of(&events).expect("prepare returns a handle");

        // Execute twice with different values: the same handle re-binds each
        // time, and the unnamed wire value takes the declaration's name.
        for wanted in [3, 5] {
            let events = drain_events(h.handle.stream_prepared(
                s,
                PreparedRpc::Execute {
                    handle,
                    values: vec![int_param(wanted)],
                },
                no_cancel(),
            ))
            .await;
            assert_eq!(event_error(&events), None, "{events:?}");
            assert_eq!(rows_of(&events), vec![vec![Datum::Int(wanted)]]);
        }

        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Unprepare { handle },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");

        // The dropped handle is gone: 8179, SQL Server's number for it.
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Execute {
                handle,
                values: vec![int_param(1)],
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), Some(8179), "{events:?}");
    }

    #[tokio::test]
    async fn an_unknown_handle_answers_8179() {
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Execute {
                handle: 42,
                values: Vec::new(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), Some(8179));
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Unprepare { handle: 42 },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), Some(8179));
    }

    #[tokio::test]
    async fn a_parse_error_at_prepare_allocates_no_handle() {
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Prepare {
                decls: String::new(),
                stmt: "SELEC oops".into(),
            },
            no_cancel(),
        ))
        .await;
        assert!(event_error(&events).is_some(), "{events:?}");
        assert_eq!(handle_of(&events), None, "no handle on a failed prepare");

        // The failed prepare consumed nothing: the next handle is still 1.
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Prepare {
                decls: String::new(),
                stmt: "SELECT 1 AS one".into(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(handle_of(&events), Some(1));
    }

    #[tokio::test]
    async fn ddl_between_prepare_and_execute_sees_the_new_schema() {
        // There is no cached plan: every execute re-binds against the live
        // catalog, so DDL between prepare and execute needs no invalidation —
        // the same handle simply sees the new schema (the plan's
        // catalog_version/rebind machinery is moot by design).
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 3).await;
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Prepare {
                decls: String::new(),
                stmt: "SELECT * FROM t ORDER BY id".into(),
            },
            no_cancel(),
        ))
        .await;
        let handle = handle_of(&events).expect("prepare returns a handle");

        h.handle
            .run_batch(
                s,
                "DROP TABLE t; CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL); INSERT INTO t VALUES (7, 70)".into(),
            )
            .await
            .unwrap();

        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Execute {
                handle,
                values: Vec::new(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");
        // The wildcard expands against the NEW table: two columns, new row.
        assert_eq!(rows_of(&events), vec![vec![Datum::Int(7), Datum::Int(70)]]);
    }

    #[tokio::test]
    async fn prepexec_reports_the_handle_after_the_results() {
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        fill(&h, s, 4).await;
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::PrepExec {
                decls: "@p1 int".into(),
                stmt: "SELECT id FROM t WHERE id > @p1 ORDER BY id".into(),
                values: vec![int_param(2)],
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");
        assert_eq!(
            rows_of(&events),
            vec![vec![Datum::Int(3)], vec![Datum::Int(4)]]
        );
        // Return values follow every result set: the handle event comes after
        // the statement's DONE, immediately before Complete.
        let positions: Vec<&str> = events
            .iter()
            .map(|e| match e {
                BatchEvent::Columns(_) => "columns",
                BatchEvent::Rows(_) => "rows",
                BatchEvent::StatementDone { .. } => "done",
                BatchEvent::PreparedHandle(_) => "handle",
                BatchEvent::Complete { .. } => "complete",
                _ => "other",
            })
            .collect();
        assert_eq!(
            positions.iter().rev().take(2).copied().collect::<Vec<_>>(),
            ["complete", "handle"],
            "{positions:?}"
        );
        // And the stored handle is executable afterwards.
        let handle = handle_of(&events).expect("prepexec returns a handle");
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Execute {
                handle,
                values: vec![int_param(3)],
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(rows_of(&events), vec![vec![Datum::Int(4)]]);
    }

    #[tokio::test]
    async fn describe_first_result_set_covers_static_shapes_only() {
        let h = start(LOCK_WAIT_TIMEOUT);
        let s = h.handle.open_session("truthdb".into(), "sa".into()).await;
        h.handle
            .run_batch(
                s,
                "CREATE TABLE d (id INT NOT NULL PRIMARY KEY, name NVARCHAR(40))".into(),
            )
            .await
            .unwrap();

        // A parameterized single-table SELECT describes without executing.
        // The @p1 is unresolvable in describe's default context — the planner
        // swallows that and falls back to a scan (non-sargable predicate);
        // the columns come from the table schema either way.
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Describe {
                tsql: "SELECT id, name FROM d WHERE id = @p1".into(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");
        let rows = rows_of(&events);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Datum::Int(1)); // column_ordinal
        assert_eq!(rows[0][2], Datum::NVarChar("id".into()));
        assert_eq!(rows[0][4], Datum::Int(56)); // system_type_id: int
        assert_eq!(rows[1][2], Datum::NVarChar("name".into()));
        assert_eq!(rows[1][5], Datum::NVarChar("nvarchar(40)".into()));

        // A statement producing no result set describes as zero rows — and
        // describing it executes nothing: the table stays empty.
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Describe {
                tsql: "INSERT INTO d VALUES (1, 'x')".into(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), None, "{events:?}");
        assert_eq!(rows_of(&events), Vec::<Vec<Datum>>::new());
        let reply = h
            .handle
            .run_batch(s, "SELECT COUNT(*) FROM d".into())
            .await
            .unwrap();
        match &reply.outcome.results[0] {
            StatementResult::Rows(rowset) => assert_eq!(
                rowset.rows[0][0],
                Datum::BigInt(0),
                "describe must not execute the INSERT"
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // The contract is the first RESULT SET, not the first statement:
        // `INSERT; SELECT` (and a SELECT inside a TRY block) describe the
        // SELECT, and `TOP 0` — the standard metadata-discovery idiom —
        // describes like any other TOP.
        for tsql in [
            "INSERT INTO d VALUES (9, 'y'); SELECT id FROM d",
            "BEGIN TRY SELECT id FROM d END TRY BEGIN CATCH END CATCH",
            "SELECT TOP 0 id FROM d",
        ] {
            let events = drain_events(h.handle.stream_prepared(
                s,
                PreparedRpc::Describe { tsql: tsql.into() },
                no_cancel(),
            ))
            .await;
            assert_eq!(event_error(&events), None, "{tsql}: {events:?}");
            let rows = rows_of(&events);
            assert_eq!(rows.len(), 1, "{tsql}");
            assert_eq!(rows[0][2], Datum::NVarChar("id".into()), "{tsql}");
        }

        // A shape whose types are only known by executing answers 11514.
        let events = drain_events(h.handle.stream_prepared(
            s,
            PreparedRpc::Describe {
                tsql: "SELECT a.id FROM d a JOIN d b ON a.id = b.id".into(),
            },
            no_cancel(),
        ))
        .await;
        assert_eq!(event_error(&events), Some(11514), "{events:?}");
    }

    #[test]
    fn decl_names_splits_top_level_commas_only() {
        assert_eq!(
            crate::rel::decl_names("@p1 int, @p2 nvarchar(10), @p3 decimal(10,2)"),
            ["@p1", "@p2", "@p3"]
        );
        assert_eq!(crate::rel::decl_names(""), Vec::<String>::new());
        assert_eq!(crate::rel::decl_names("@a int"), ["@a"]);
        // A quoted default may contain commas and parens; a doubled ''
        // escape stays inside the string.
        assert_eq!(
            crate::rel::decl_names(
                "@p1 varchar(10) = 'a,b', @p2 int, @p3 varchar(5) = 'it''s, ok'"
            ),
            ["@p1", "@p2", "@p3"]
        );
    }

    #[test]
    fn bind_decl_names_keeps_existing_names() {
        let mut named = int_param(1);
        named.name = "@mine".into();
        let bound = bind_decl_names("@p1 int, @p2 int", vec![named, int_param(2)]).expect("bind");
        assert_eq!(bound[0].name, "@mine");
        assert_eq!(bound[1].name, "@p2");
    }

    #[test]
    fn more_values_than_declarations_is_8144() {
        // SQL Server rejects extra arguments rather than silently ignoring
        // them; without this an unmatched value seeded nothing and vanished.
        let err = bind_decl_names("@p1 int", vec![int_param(1), int_param(2)])
            .expect_err("extra value must be rejected");
        assert_eq!(err.number, 8144);
        let err = bind_decl_names("", vec![int_param(1)])
            .expect_err("values against an empty declaration list must be rejected");
        assert_eq!(err.number, 8144);
        // Fewer values than declarations stays legal (an unread declared
        // parameter goes unmissed).
        assert!(bind_decl_names("@p1 int, @p2 int", vec![int_param(1)]).is_ok());
        // Extra NAMED values pass through — they seed variables by their own
        // names (the run_rpc wrappers' contract).
        let mut named = int_param(9);
        named.name = "@extra".into();
        assert!(bind_decl_names("", vec![named]).is_ok());
    }
}
