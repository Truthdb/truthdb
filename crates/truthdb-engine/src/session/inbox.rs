use super::*;

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
pub(super) struct ProcRpcTail {
    /// The seeded variable holding the RETURN status. It is seeded NULL and the
    /// procedure overwrites it with an Int only if it completes; a still-NULL
    /// value at read time means the procedure aborted, so no tail is emitted.
    pub(super) status_var: String,
    /// One entry per OUTPUT parameter, in call order: the caller-scope variable
    /// to read the final value back from, the name the RETURNVALUE token
    /// carries, and the parameter's 0-based position in the RPC call (the
    /// RETURNVALUE ParamOrdinal — ordinal-keyed drivers place values by it). For
    /// a named argument the read/token names are the wire name (`@out`); for a
    /// positional one (JDBC `{call p(?)}`) the read variable is a synthetic seed
    /// and the token name is empty.
    pub(super) output_vars: Vec<(String, String, u16)>,
}

pub(super) enum EngineCall {
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
pub(super) enum Work {
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
pub(super) struct Inbox {
    pub(super) state: Mutex<InboxState>,
    /// Signalled on a new call, a drain nudge, and on close.
    pub(super) ready: Condvar,
}

pub(super) struct InboxState {
    pub(super) calls: VecDeque<EngineCall>,
    /// A pending drain. One flag rather than a count: draining is idempotent
    /// and runs everything grantable, so two nudges are one drain.
    pub(super) drain: bool,
    /// No more calls will come — every handle is gone, or `shutdown` was
    /// called. Workers finish what is queued and exit.
    pub(super) closed: bool,
}

impl Inbox {
    pub(super) fn new() -> Self {
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
    pub(super) fn send(&self, call: EngineCall) {
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
    pub(super) fn nudge(&self) {
        let mut state = self.state.lock().expect("inbox poisoned");
        if state.closed {
            return;
        }
        state.drain = true;
        drop(state);
        self.ready.notify_one();
    }

    /// Closes the inbox and wakes every worker.
    pub(super) fn close(&self) {
        let mut state = self.state.lock().expect("inbox poisoned");
        state.closed = true;
        drop(state);
        self.ready.notify_all();
    }

    /// Blocks until there is something to do. `None` once the inbox is closed
    /// and drained, which is a worker's signal to exit.
    pub(super) fn next(&self) -> Option<Work> {
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
pub(super) struct HandleToken(pub(super) Arc<Inbox>);

impl Drop for HandleToken {
    fn drop(&mut self) {
        self.0.close();
    }
}
