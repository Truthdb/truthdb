use super::*;

/// A cloneable handle to the engine's worker pool. Cheap to clone (shares the
/// sender).
#[derive(Clone)]
pub struct EngineHandle {
    pub(super) inbox: Arc<Inbox>,
    /// Dropped with the last handle, which closes the inbox.
    pub(super) _token: Arc<HandleToken>,
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
