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
//! front (see [`crate::engine::analyze_locks`]) before running any statement, so a
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

use crate::engine::{BatchOutcome, Isolation, ResultColumn, RowSet, StatementResult, TxnContext};
use crate::engine::{Engine, EngineError};
use crate::lock::{LockManager, LockMode, Resource};
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
        command: crate::engine::DoneCommand,
    },
    /// Ends a statement that failed after its result set had begun streaming:
    /// closes the set (with a clean DONE — an error-flagged DONE without an
    /// ERROR token reads as "severe failure" to real drivers) so the stream
    /// stays framed for the statements that follow. The error itself travels
    /// separately — in the batch-final [`BatchEvent::Error`] for a continued
    /// error, or not at all for one a `CATCH` handled.
    StatementAborted { in_transaction: bool },
    /// The session's database context was (re-)established (`USE`): the TDS
    /// layer renders the ENVCHANGE + 5701 INFO clients (SSMS) expect.
    DatabaseContext { database: String },
    /// A SQL error that stopped the batch. The statements before it kept their
    /// results, which were already sent.
    Error(SqlError),
    /// An informational message (RAISERROR severity <= 10): TDS renders an
    /// INFO token in-stream; it is not an error and stops nothing.
    Info(SqlError),
    /// A procedure's RETURN status for an RPC-by-name call: the RETURNSTATUS
    /// token's value (hardcoded 0 before this event existed).
    ReturnStatus(i32),
    /// A procedure OUTPUT parameter's final value for an RPC-by-name call,
    /// rendered as a typed RETURNVALUE token after RETURNSTATUS. `ordinal` is
    /// the parameter's 0-based position in the RPC call (ordinal-keyed drivers
    /// place the value there).
    ReturnValue {
        ordinal: u16,
        name: String,
        column_type: crate::relstore::types::ColumnType,
        value: Datum,
    },
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
        values: Vec<crate::engine::RpcParam>,
    },
    PrepExec {
        decls: String,
        stmt: String,
        values: Vec<crate::engine::RpcParam>,
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
    /// runs streams through the [`crate::engine::BatchEmitter`] impl below
    /// instead, stamping each DONE with its own statement's state; here every
    /// DONE carries the one final state, which is all an error-only reply has.
    fn send_outcome(&self, outcome: BatchOutcome, in_transaction: bool) {
        for result in outcome.results {
            let sent = match result {
                StatementResult::Rows(rowset) => self.send_rowset(rowset, in_transaction),
                StatementResult::RowsAffected(n) => self.send(BatchEvent::StatementDone {
                    count: Some(n),
                    in_transaction,
                    command: crate::engine::DoneCommand::Other,
                }),
                StatementResult::Done => self.send(BatchEvent::StatementDone {
                    count: None,
                    in_transaction,
                    command: crate::engine::DoneCommand::Other,
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
            command: crate::engine::DoneCommand::Select,
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
impl crate::engine::BatchEmitter for BatchSink {
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
        command: crate::engine::DoneCommand,
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

    fn database_context(&mut self, database: &str) {
        self.send(BatchEvent::DatabaseContext {
            database: database.to_string(),
        });
    }

    fn info(&mut self, error: &truthdb_sql::error::SqlError) {
        self.send(BatchEvent::Info(error.clone()));
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
            // only the TDS renderer (RETURNVALUE) consumes it. Same for a
            // database-context change (the ENVCHANGE is wire-only).
            BatchEvent::PreparedHandle(_) => {}
            BatchEvent::DatabaseContext { .. } => {}
            // An informational message (RAISERROR <= 10) is wire-only too:
            // it is not an error and carries no result.
            BatchEvent::Info(_) => {}
            // Return status/values are wire-only (RETURNSTATUS/RETURNVALUE).
            BatchEvent::ReturnStatus(_) | BatchEvent::ReturnValue { .. } => {}
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

    #[allow(clippy::too_many_arguments)]
    fn open(
        &mut self,
        database: String,
        database_id: u32,
        login: String,
        login_sid: u32,
        user: String,
        user_sid: u32,
    ) -> SessionId {
        let id = SessionId(self.next_id);
        self.next_id += 1;
        let mut session = Session::default();
        session.txn_ctx.set_session_identity(
            database,
            database_id,
            login,
            id.0 as i32,
            user,
            login_sid,
            user_sid,
        );
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
/// The response-tail descriptor for an RPC-by-name call of a user procedure.
///
/// The call is executed as a synthesized `EXEC @<status_var> = [proc] @p = @p
/// [OUTPUT]…` batch: the wire parameters are seeded as batch variables, so once
/// the batch completes the procedure's OUTPUT parameters have been copied back
/// into their caller-scope variables and the RETURN status into `status_var`.
/// The worker reads them off the context (before it is handed back to the
/// scheduler) and emits the RETURNSTATUS / RETURNVALUE events the wire renderer
/// turns into tokens — but only when the batch completed without error, which is
/// exactly when the copy-back and status assignment happened.
#[derive(Clone)]
struct ProcRpcTail {
    /// The seeded variable holding the RETURN status. It is seeded NULL and the
    /// procedure overwrites it with an Int only if it completes; a still-NULL
    /// value at read time means the procedure aborted, so no tail is emitted.
    status_var: String,
    /// One entry per OUTPUT parameter, in call order: the caller-scope variable
    /// to read the final value back from, the name the RETURNVALUE token
    /// carries, and the parameter's 0-based position in the RPC call (the
    /// RETURNVALUE ParamOrdinal — ordinal-keyed drivers place values by it). For
    /// a named argument the read/token names are the wire name (`@out`); for a
    /// positional one (JDBC `{call p(?)}`) the read variable is a synthetic seed
    /// and the token name is empty.
    output_vars: Vec<(String, String, u16)>,
}

enum EngineCall {
    OpenSession {
        database: String,
        login: String,
        /// The login's server principal_id (0 if none — the native path); the
        /// worker resolves it to the database user before the session opens.
        login_sid: u32,
        /// `Ok((session, canonical database name))`, or `Err(())` when the
        /// requested database does not exist (the TDS gateway answers 4060).
        reply: oneshot::Sender<Result<(SessionId, String), ()>>,
    },
    /// A SQL batch on behalf of a session (TDS path): typed results. `params`
    /// is empty for a plain batch, or the `sp_executesql` parameters seeded as
    /// batch variables before the statement runs (RPC path).
    RunBatch {
        session: SessionId,
        sql: String,
        params: Vec<crate::engine::RpcParam>,
        /// For an RPC-by-name call of a user procedure: the OUTPUT parameters
        /// and return-status variable to read back off the context once the
        /// synthesized `EXEC` batch completes. `None` for an ordinary batch.
        proc_tail: Option<ProcRpcTail>,
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
    /// A session-less catalog read: fetch a login's credential for the TDS
    /// handshake, which runs BEFORE any session/transaction exists. The reply
    /// carries only the stored blob — PBKDF2 verification runs off the worker.
    LookupLogin {
        name: String,
        reply: oneshot::Sender<Option<LoginRecord>>,
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

/// A login's stored authentication data, read by the TDS handshake before any
/// session exists. The credential is verified in the TDS task (off the worker
/// pool) so a ~30 ms PBKDF2 does not occupy a worker thread.
#[derive(Debug, Clone)]
pub struct LoginRecord {
    pub principal_id: u32,
    /// The stored login name in its canonical casing (for `SUSER_SNAME()`).
    pub name: String,
    pub password_blob: String,
    pub is_disabled: bool,
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
    /// Opens a session for a connection, recording its database, login, and the
    /// login's server principal_id (for the database-user and role resolution)
    /// for session intrinsics. Returns its id (or a placeholder if the engine is
    /// gone).
    pub async fn open_session(
        &self,
        database: String,
        login: String,
        login_sid: u32,
    ) -> Result<(SessionId, String), ()> {
        let (reply, rx) = oneshot::channel();
        self.inbox.send(EngineCall::OpenSession {
            database,
            login,
            login_sid,
            reply,
        });
        rx.await.unwrap_or(Err(()))
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
        params: Vec<crate::engine::RpcParam>,
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
        params: Vec<crate::engine::RpcParam>,
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
        params: Vec<crate::engine::RpcParam>,
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
            proc_tail: None,
            cancel,
            reply: BatchSink::new(tx),
        });
        // A closed inbox drops the call, taking the sink with it, so the stream
        // ends with no terminal event — which every reader here turns into the
        // same "the engine is gone" the dead oneshot used to mean.
        rx
    }

    /// Runs an RPC-by-name call of a USER procedure: synthesizes the equivalent
    /// `EXEC` batch with the wire parameters seeded as variables (named binding,
    /// OUTPUT flags carried) and streams the reply like any batch. Lock analysis
    /// resolves the procedure body through the EXEC arm.
    ///
    /// The RETURN status is captured with `EXEC @<rc> = …` into a synthetic
    /// variable seeded from an extra Int parameter — no wire token, no extra
    /// statement — and the OUTPUT parameters land back in their caller-scope
    /// variables. A [`ProcRpcTail`] tells the worker which variables to read off
    /// the context once the batch completes, so it can emit the real RETURNSTATUS
    /// and typed RETURNVALUEs.
    pub fn stream_proc_rpc(
        &self,
        session: SessionId,
        name: String,
        params: Vec<crate::engine::RpcParam>,
        outputs: Vec<bool>,
        cancel: Arc<AtomicBool>,
    ) -> mpsc::UnboundedReceiver<BatchEvent> {
        // `@__truthdb_rc` is seeded (not a proc argument), so `EXEC @__truthdb_rc
        // = …` finds it declared and stores the RETURN status there. The name is
        // one no user variable would collide with in the synthesized batch.
        let status_var = "__truthdb_rc".to_string();
        let mut sql = format!("EXEC @{status_var} = [{}]", name.replace(']', "]]"));
        let mut output_vars = Vec::new();
        let mut seeded: Vec<crate::engine::RpcParam> = Vec::with_capacity(params.len() + 1);
        for (index, mut param) in params.into_iter().enumerate() {
            let sep = if index == 0 { " " } else { ", " };
            let is_output = outputs.get(index).copied().unwrap_or(false);
            let output_kw = if is_output { " OUTPUT" } else { "" };
            let ordinal = index as u16;
            if param.name.trim_start_matches('@').is_empty() {
                // Positional (unnamed) argument — a JDBC `{call p(?, ?)}` shape.
                // Seed under a synthetic variable and pass it positionally; the
                // RETURNVALUE carries no name, which drivers match by ordinal.
                let var = format!("__truthdb_arg{index}");
                sql.push_str(&format!("{sep}@{var}{output_kw}"));
                if is_output {
                    output_vars.push((format!("@{var}"), String::new(), ordinal));
                }
                param.name = format!("@{var}");
            } else {
                let bare = param.name.trim_start_matches('@').to_string();
                sql.push_str(&format!("{sep}@{bare} = @{bare}{output_kw}"));
                if is_output {
                    output_vars.push((param.name.clone(), param.name.clone(), ordinal));
                }
            }
            seeded.push(param);
        }
        // Seed the return-status variable NULL: the procedure overwrites it with
        // an Int only on completion, so a still-NULL read means it aborted (see
        // read_proc_tail). Seeding it also satisfies `EXEC @rc =`'s declared-var
        // requirement (137).
        seeded.push(crate::engine::RpcParam {
            name: format!("@{status_var}"),
            column_type: crate::relstore::types::ColumnType::Int,
            value: Datum::Null,
        });
        let proc_tail = ProcRpcTail {
            status_var,
            output_vars,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        self.inbox.send(EngineCall::RunBatch {
            session,
            sql,
            params: seeded,
            proc_tail: Some(proc_tail),
            cancel,
            reply: BatchSink::new(tx),
        });
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

    /// Fetches a login's stored credential for the TDS handshake. Returns
    /// `None` if the login does not exist or the worker pool is gone.
    pub async fn lookup_login(&self, name: String) -> Option<LoginRecord> {
        let (reply, rx) = oneshot::channel();
        self.inbox.send(EngineCall::LookupLogin { name, reply });
        rx.await.ok().flatten()
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
    params: Vec<crate::engine::RpcParam>,
    proc_tail: Option<ProcRpcTail>,
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
        // Version-store upkeep (Stage 13): drop row-version history no live
        // snapshot can need. Outside the scheduler lock — it takes the
        // storage lock, and nothing here depends on lock-table state.
        shared.engine.version_prune();
        // Standby ring upkeep (Stage 18): a replication standby cannot
        // checkpoint, so this periodic restartpoint is what reclaims its WAL
        // ring (up to the primary-shipped undo floor). A no-op elsewhere.
        if let Err(err) = shared.engine.standby_restartpoint_if_needed() {
            eprintln!("standby restartpoint failed: {err}");
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
                login_sid,
                reply,
            }) => {
                // The requested database must exist (the caller answers 4060
                // otherwise); the session records the CATALOG's spelling and
                // the id, resolved once, here — the same derivation USE runs.
                // NOT an early return: this arm runs inside the worker loop,
                // and returning would kill the thread.
                if let Some((db_id, canonical)) = shared.engine.resolve_database(&database) {
                    // Resolve the login to its database user here (the worker
                    // has the catalog); the session records both for
                    // USER_NAME() and role membership.
                    let (user, user_sid) = shared.engine.resolve_session_user(&login, login_sid);
                    let id = shared
                        .scheduler
                        .lock()
                        .expect("scheduler poisoned")
                        .sessions
                        .open(canonical.clone(), db_id, login, login_sid, user, user_sid);
                    let _ = reply.send(Ok((id, canonical)));
                } else {
                    let _ = reply.send(Err(()));
                }
            }
            Work::Call(EngineCall::RunBatch {
                session,
                sql,
                params,
                proc_tail,
                cancel,
                reply,
            }) => dispatch_batch(shared, session, sql, params, proc_tail, cancel, reply),
            Work::Call(EngineCall::RunRpc {
                session,
                command,
                cancel,
                reply,
            }) => dispatch_rpc(shared, session, command, cancel, reply),
            Work::Call(EngineCall::RunNative { command, reply }) => {
                let _ = reply.send(shared.engine.execute(&command));
            }
            Work::Call(EngineCall::LookupLogin { name, reply }) => {
                let _ = reply.send(shared.engine.lookup_login(&name));
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
                    command: crate::engine::DoneCommand::Select,
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
            dispatch_batch(shared, session, text, values, None, cancel, reply);
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
            dispatch_batch(shared, session, stmt, values, None, cancel, reply);
        }
    }
}

/// Names any unnamed value parameters from the declaration list, in order —
/// `sp_execute` values arrive unnamed on the wire, and seeding a batch
/// variable needs its name. A value that already has a name keeps it.
fn bind_decl_names(
    decls: &str,
    mut values: Vec<crate::engine::RpcParam>,
) -> Result<Vec<crate::engine::RpcParam>, SqlError> {
    let names = crate::engine::decl_names(decls);
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
    params: Vec<crate::engine::RpcParam>,
    proc_tail: Option<ProcRpcTail>,
    cancel: Arc<AtomicBool>,
    reply: BatchSink,
) {
    let runnable = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        let isolation = sched.isolation(session);
        // Parameters are values, not statements, so they never change which
        // locks the batch needs — analyse the statement text as usual.
        let epoch = shared.engine.lock_analysis_epoch();
        let needs = shared
            .engine
            .analyze_locks(sched.session_db(session), &sql, isolation);
        if sched.try_acquire(session.raw(), &needs, true) {
            sched.sessions.touch(session);
            let txn_ctx = sched.take_ctx(session);
            Some(Runnable {
                session,
                sql,
                params,
                proc_tail,
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
                proc_tail,
                cancel,
                reply,
                needs,
                deadline,
                epoch,
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
        proc_tail,
        cancel,
        reply,
        mut txn_ctx,
    } = work;
    // Bind the cancel flag to this worker thread for the batch, so the executor's
    // `check_cancelled` polls see a TDS Attention; the guard clears it on return.
    let _cancel_guard = crate::engine::CancelScope::enter(cancel);
    // Statement events stream out *while the batch runs* — the executor emits
    // each result as it is produced, and the send never blocks, so a client
    // that reads slowly delays neither this worker nor the locks it holds.
    let mut reply = reply;
    let outcome = shared
        .engine
        .sql_batch_streamed(&sql, &mut txn_ctx, &params, &mut reply);
    // RPC-by-name tail: a procedure copies its OUTPUT parameters back and stores
    // its RETURN status only when it *completed* — which is not the same as the
    // *batch* running clean. A procedure that raises a continued (non-dooming)
    // error and then returns completes, yet the batch surfaces that error
    // (`Ok(Some(err))`); SQL Server still sends the tail there. So read the tail
    // on any `Ok` outcome and let `read_proc_tail` decide from the completion
    // signal (the status variable is non-NULL only if the procedure completed).
    // Read it now, before `finish` hands the context back to the scheduler.
    let tail_events = match (&proc_tail, &outcome) {
        (Some(tail), Ok(_)) => Some(read_proc_tail(&txn_ctx, tail)),
        _ => None,
    };
    let in_transaction = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        sched.finish(&shared.engine, session, txn_ctx)
    };
    // The terminal events wait for the locks to settle: `Complete` carries the
    // post-batch transaction state, which only `finish` knows.
    match outcome {
        Ok(error) => {
            if let Some(events) = tail_events {
                for event in events {
                    if !reply.send(event) {
                        return;
                    }
                }
            }
            reply.send_tail(error, in_transaction);
        }
        Err(err) => {
            reply.send(BatchEvent::Failed(err));
        }
    }
}

/// Reads an RPC-by-name call's response tail off the finished context: the
/// RETURN status, then each OUTPUT parameter as a typed RETURNVALUE.
///
/// The status variable is the completion signal. It was seeded NULL, and
/// `run_user_procedure` overwrites it with an Int (and copies OUTPUT parameters
/// back) only when the body completes — both under the one `result.is_ok()`
/// gate. So a NULL status here means the procedure aborted: emit nothing, which
/// is what SQL Server does for a failed procedure. A non-NULL Int status means
/// it completed (possibly after a continued, non-dooming error), and the OUTPUT
/// copy-back happened too, so the whole tail is emitted. This single observable
/// keeps the emission decision in agreement with the copy-back decision.
fn read_proc_tail(txn_ctx: &TxnContext, tail: &ProcRpcTail) -> Vec<BatchEvent> {
    let status = match txn_ctx.variable_datum(&tail.status_var) {
        Some((_, Datum::Int(status))) => status,
        _ => return Vec::new(),
    };
    let mut events = Vec::with_capacity(tail.output_vars.len() + 1);
    events.push(BatchEvent::ReturnStatus(status));
    for (read_var, wire_name, ordinal) in &tail.output_vars {
        if let Some((column_type, value)) = txn_ctx.variable_datum(read_var) {
            events.push(BatchEvent::ReturnValue {
                ordinal: *ordinal,
                name: wire_name.clone(),
                column_type,
                value,
            });
        }
    }
    events
}

/// Runs every parked batch whose locks are now grantable, in FIFO order, until
/// none remain. Each finished batch may release locks that unblock the next, so
/// this re-checks after every one.
fn drain_ready(shared: &Arc<Shared>) {
    loop {
        let work = {
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            sched.next_grantable(&shared.engine)
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
    params: Vec<crate::engine::RpcParam>,
    proc_tail: Option<ProcRpcTail>,
    cancel: Arc<AtomicBool>,
    reply: BatchSink,
    needs: Vec<(Resource, LockMode)>,
    deadline: Instant,
    /// The lock-analysis epoch `needs` was computed under. A pending `ALTER
    /// DATABASE` option flip bumps the epoch; a parked batch whose epoch is
    /// stale is re-analyzed before it can be granted, so the lock set it
    /// runs with always matches the versioning regime it will execute under
    /// (analyzed-versioned-but-executes-lock-based was a dirty read).
    epoch: u64,
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

    /// The session's current database id — the namespace its batch's lock
    /// analysis must resolve names in (the same one execution will use). The
    /// default database for an unknown session or one whose context is
    /// momentarily taken (a session has at most one in-flight batch, so its
    /// own analysis never observes the placeholder).
    fn session_db(&self, session: SessionId) -> u32 {
        self.sessions
            .get(session)
            .map(|s| s.txn_ctx.database_id())
            .unwrap_or(crate::relstore::catalog::DEFAULT_DATABASE_ID)
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
                // READ COMMITTED shared locks do not survive the batch, and a
                // SNAPSHOT transaction holds no read locks between batches at
                // all (its Database IS is the running batch's DDL fence; the
                // snapshot itself is what protects its reads).
                let releases_read_locks = matches!(
                    state.txn_ctx.isolation(),
                    Isolation::ReadCommitted | Isolation::Snapshot
                );
                if open {
                    // Transaction still open: keep write locks.
                    if releases_read_locks {
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
    fn next_grantable(&mut self, engine: &Engine) -> Option<Runnable> {
        let current_epoch = engine.lock_analysis_epoch();
        let mut i = 0;
        while i < self.parked.len() {
            // An ALTER DATABASE option flip since this batch was analyzed may
            // have changed which locks its reads need (versioned vs Table S):
            // re-analyze before considering the grant, so the lock set always
            // matches the regime the batch will execute under. The deadlock
            // graph may see one sweep of stale needs; the grant path never
            // does.
            if self.parked[i].epoch != current_epoch {
                let isolation = self.isolation(self.parked[i].session);
                self.parked[i].needs = engine.analyze_locks(
                    self.session_db(self.parked[i].session),
                    &self.parked[i].sql,
                    isolation,
                );
                self.parked[i].epoch = current_epoch;
            }
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
                    proc_tail: parked.proc_tail,
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
mod tests;
