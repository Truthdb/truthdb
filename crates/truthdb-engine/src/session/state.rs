use super::*;

/// A statement a session prepared: its text and parameter declarations, both
/// verbatim from `sp_prepare`. There is no cached plan to go stale — every
/// execution re-parses and re-binds against the live catalog, exactly like
/// `sp_executesql`, so DDL between prepare and execute behaves like SQL
/// Server's recompile-on-schema-change with no invalidation machinery.
struct PreparedStatement {
    decls: String,
    text: String,
}

/// Per-connection engine-side state: the transaction context carried across a
/// connection's batches (open transaction, `@@TRANCOUNT`, isolation, SET
/// options).
pub(super) struct Session {
    pub(super) txn_ctx: TxnContext,
    /// When this session last started or finished a batch. Only meaningful
    /// while the session is idle: a running batch's context is moved out by
    /// [`Scheduler::take_ctx`], so a session mid-batch reports no open
    /// transaction and is never a reap candidate regardless of this stamp.
    pub(super) last_activity: Instant,
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

pub(super) struct SessionManager {
    sessions: HashMap<SessionId, Session>,
    next_id: u64,
}

impl SessionManager {
    pub(super) fn new() -> Self {
        SessionManager {
            sessions: HashMap::new(),
            next_id: 1,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn open(
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

    pub(super) fn close(&mut self, id: SessionId) -> Option<Session> {
        self.sessions.remove(&id)
    }

    pub(super) fn get(&self, id: SessionId) -> Option<&Session> {
        self.sessions.get(&id)
    }

    pub(super) fn get_mut(&mut self, id: SessionId) -> Option<&mut Session> {
        self.sessions.get_mut(&id)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&SessionId, &Session)> {
        self.sessions.iter()
    }

    /// Marks a session as active now, so the idle reaper does not count time
    /// spent running a batch against it.
    pub(super) fn touch(&mut self, id: SessionId) {
        if let Some(state) = self.sessions.get_mut(&id) {
            state.last_activity = Instant::now();
        }
    }

    /// Stores a prepared statement for the session and returns its handle.
    pub(super) fn prepare(&mut self, id: SessionId, decls: String, text: String) -> i32 {
        let session = self.sessions.entry(id).or_default();
        let handle = session.next_prepared_handle;
        session.next_prepared_handle += 1;
        session
            .prepared
            .insert(handle, PreparedStatement { decls, text });
        handle
    }

    /// Looks up a session's prepared statement by handle.
    pub(super) fn prepared(&self, id: SessionId, handle: i32) -> Option<(String, String)> {
        self.sessions
            .get(&id)?
            .prepared
            .get(&handle)
            .map(|p| (p.decls.clone(), p.text.clone()))
    }

    /// Drops a prepared handle. `false` if the session never prepared it.
    pub(super) fn unprepare(&mut self, id: SessionId, handle: i32) -> bool {
        self.sessions
            .get_mut(&id)
            .is_some_and(|s| s.prepared.remove(&handle).is_some())
    }

    /// Whether the session has an open explicit transaction — the state an
    /// immediate (no-batch) reply's DONE stamps as `DONE_INXACT`.
    pub(super) fn in_transaction(&self, id: SessionId) -> bool {
        self.sessions
            .get(&id)
            .is_some_and(|s| s.txn_ctx.in_transaction())
    }
}
