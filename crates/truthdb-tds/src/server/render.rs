use super::*;

/// Streams a batch's reply to the client — writing packets as rows arrive —
/// while concurrently watching the connection for a TDS Attention. `Ok(false)`
/// if the client disconnected mid-batch.
///
/// Reading and writing at once needs both halves of the stream, so it is split;
/// nothing else touches it for the batch's duration, so the split's internal
/// lock is never contended.
///
/// An Attention sets `cancel` (the executor's `check_cancelled` polls see it)
/// and the rest of the reply is dropped rather than rendered: the client is
/// answered with `DONE(attention)`, which per MS-TDS terminates the response.
/// Rows already written stay written — unlike the buffered path this replaces,
/// which discarded the whole result. Streaming means some of it has already
/// left, and drivers discard a cancelled response's rows on seeing the
/// attention DONE.
///
/// `AsyncReadExt::read` is cancellation-safe, so bytes read into `hdr` survive a
/// `select!` iteration that resolves to an event instead; any partial header
/// left when the batch ends is completed afterwards so packet framing is intact.
/// What a response renders: one already-started reply stream (a SQL batch, or
/// the render tests' RPC framing), or an RPC request's one-or-more RPCs — each
/// issued only after the previous one's terminal event, since a session runs
/// one call at a time.
pub(super) enum ReplySource<'a> {
    Single {
        events: Option<mpsc::UnboundedReceiver<BatchEvent>>,
        rpc: bool,
    },
    Rpcs {
        engine: &'a EngineHandle,
        session: SessionId,
        requests: std::vec::IntoIter<rpc::RpcRequest>,
    },
}

impl ReplySource<'_> {
    fn is_rpc(&self) -> bool {
        match self {
            ReplySource::Single { rpc, .. } => *rpc,
            ReplySource::Rpcs { .. } => true,
        }
    }

    /// Whether more replies follow the one about to render — what sets
    /// `DONE_MORE` on a non-last RPC's DONEPROC.
    fn has_more(&self) -> bool {
        match self {
            ReplySource::Single { .. } => false,
            ReplySource::Rpcs { requests, .. } => requests.len() > 0,
        }
    }

    /// Starts the next reply, or `None` when the response is complete. `Err`
    /// is a decode error to render in-frame (no engine call was made).
    fn next(
        &mut self,
        cancel: &Arc<AtomicBool>,
    ) -> Option<Result<mpsc::UnboundedReceiver<BatchEvent>, (i32, String)>> {
        match self {
            ReplySource::Single { events, .. } => events.take().map(Ok),
            ReplySource::Rpcs {
                engine,
                session,
                requests,
            } => {
                let request = requests.next()?;
                Some(start_rpc(engine, *session, request, cancel.clone()))
            }
        }
    }
}

pub(super) async fn stream_reply<S>(
    stream: &mut S,
    mut source: ReplySource<'_>,
    cancel: Arc<AtomicBool>,
    packet_size: usize,
) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut out = MessageWriter::new(&mut wr, PKT_TABULAR_RESULT, packet_size);
    let rpc = source.is_rpc();
    let mut hdr = [0u8; HEADER_LEN];
    let mut got = 0usize;
    let mut attention = false;
    let mut fatal = false;
    'replies: while let Some(start) = source.next(&cancel) {
        let mut render = BatchRender {
            rpc,
            more_responses: source.has_more(),
            ..BatchRender::default()
        };
        let mut events = match start {
            Ok(events) => events,
            // A decode error: render it in-frame so the multi-RPC response
            // stays token-legal, and keep going — each RPC in a request
            // answers independently, like statements in a batch.
            Err((number, message)) => {
                if attention {
                    continue;
                }
                render
                    .event(
                        &mut out,
                        BatchEvent::Error(truthdb_sql::error::SqlError::new(
                            number, 16, 1, message,
                        )),
                    )
                    .await?;
                render
                    .event(
                        &mut out,
                        BatchEvent::Complete {
                            in_transaction: false,
                        },
                    )
                    .await?;
                continue;
            }
        };
        loop {
            tokio::select! {
                event = events.recv() => match event {
                    // Render until the terminal event. After an Attention the
                    // remaining events are drained without rendering: they would
                    // otherwise put the executor's internal "query was canceled"
                    // error on the wire, which the buffered path never showed.
                    Some(event) => {
                        if attention {
                            continue;
                        }
                        match render.event(&mut out, event).await {
                            Ok(terminal) => {
                                if terminal {
                                    break;
                                }
                            }
                            // A socket write failed mid-batch — the ordinary way a
                            // client dies while a result streams. The batch is
                            // still RUNNING and holds its locks, and the caller
                            // will close the session the moment this returns —
                            // which releases those locks out from under it. Same
                            // contract as every other early exit: cancel the
                            // batch, wait for it to actually end, then leave.
                            Err(err) => {
                                cancel.store(true, Ordering::Relaxed);
                                drain_to_end(&mut events).await;
                                return Err(err);
                            }
                        }
                    }
                    // The stream ended without a terminal event: the worker panicked,
                    // or the pool dropped the call at shutdown. Falling through here
                    // would emit a message with no DONE at all — an empty EOM packet
                    // that leaves the client waiting for a result that never
                    // terminates. The buffered path turned a dead reply channel into
                    // a clean 50000, so render exactly that, flushing any DONE still
                    // held back on the way (a stream that died between
                    // `StatementDone` and `Complete` would otherwise leave its result
                    // set unterminated).
                    None => {
                        if !attention {
                            render
                                .event(&mut out, BatchEvent::Failed(EngineError::Unavailable))
                                .await?;
                        }
                        break;
                    }
                },
                read = rd.read(&mut hdr[got..]) => match read {
                    // A read error is a disconnect with an errno: same treatment
                    // as the clean EOF below, or the still-running batch would
                    // have its locks released by the caller's close_session.
                    Err(err) => {
                        cancel.store(true, Ordering::Relaxed);
                        drain_to_end(&mut events).await;
                        return Err(err);
                    }
                    Ok(0) => {
                        // Client disconnected mid-batch.
                        cancel.store(true, Ordering::Relaxed);
                        drain_to_end(&mut events).await;
                        return Ok(false);
                    }
                    Ok(n) => {
                        got += n;
                        if got == HEADER_LEN {
                            // Only an Attention (a header-only packet) is legal during
                            // a running batch — TDS is request/response with no MARS.
                            if hdr[0] == PKT_ATTENTION {
                                attention = true;
                                cancel.store(true, Ordering::Relaxed);
                                got = 0;
                            } else {
                                // A pipelined non-Attention packet: fail loud rather
                                // than silently ignore its (undrained) body and misframe
                                // the next read. Abort the batch, then error out.
                                cancel.store(true, Ordering::Relaxed);
                                drain_to_end(&mut events).await;
                                return Err(protocol_err(
                                    "unexpected TDS packet during a running batch",
                                ));
                            }
                        }
                    }
                },
            }
        }
        if render.fatal {
            // A fatal-severity error (>= 20): the reply is complete, and the
            // connection dies with it — any RPCs not yet issued never run.
            fatal = true;
            break 'replies;
        }
        if attention {
            // Everything after the Attention is discarded; the RPCs not yet
            // issued never run.
            break 'replies;
        }
    }
    // The batch finished before a header fully arrived: complete it so the next
    // read stays framed (and still honour a late Attention).
    let mut late_attention = false;
    if got > 0 {
        rd.read_exact(&mut hdr[got..]).await?;
        if hdr[0] == PKT_ATTENTION {
            late_attention = true;
        } else {
            return Err(protocol_err("unexpected TDS packet during a running batch"));
        }
    }
    if attention {
        // Mid-batch: everything after the Attention was drained unrendered, so
        // this is the message's only final DONE.
        let mut done = Vec::new();
        token::done_attention(&mut done);
        out.write(&done).await?;
    }
    out.finish().await?;
    if late_attention && !attention {
        // The Attention landed after the batch had already rendered its own
        // final DONE, so the acknowledgement cannot go in that message: a client
        // stops reading at the first DONE with DONE_MORE clear and never parses
        // a second one behind it — go-mssqldb's `processSingleResponse` returns
        // there, `readCancelConfirmation` then reports no ack, and it blocks
        // forever in `io.ReadFull` waiting for one, with no connection timeout
        // by default. Its own source comment describes this race and expects a
        // *separate* response.
        //
        // So the ack gets its own message, which is what this file already does
        // for an Attention that arrives with no batch in flight, and what the
        // buffered path achieved by discarding the result and sending the ack
        // alone. Streaming cannot take the result back, but it can still put the
        // ack where the client is looking for it.
        write_attention_ack(&mut wr, packet_size).await?;
    }
    Ok(!fatal)
}

#[cfg(test)]
mod render_tests;

/// Starts one RPC on the engine, returning its reply stream.
///
/// A malformed or unsupported procedure never reaches the engine and comes
/// back as an error `(number, message)` the caller renders in-frame, so the
/// connection stays usable and a multi-RPC response stays token-legal.
fn start_rpc(
    engine: &EngineHandle,
    session: SessionId,
    request: rpc::RpcRequest,
    cancel: Arc<AtomicBool>,
) -> Result<mpsc::UnboundedReceiver<BatchEvent>, (i32, String)> {
    let rpc_error = |message: String| (50000, message);
    match request.proc {
        RpcProc::SpExecuteSql => {
            let (sql, decls, params) = rpc::split_sp_executesql(request.params)
                .map_err(|err| rpc_error(err.to_string()))?;
            Ok(engine.stream_rpc(session, sql, decls, params, cancel))
        }
        RpcProc::SpPrepare => {
            let (decls, stmt) =
                rpc::split_sp_prepare(request.params).map_err(|err| rpc_error(err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Prepare { decls, stmt }, cancel))
        }
        RpcProc::SpExecute => {
            let (handle, values) =
                rpc::split_sp_execute(request.params).map_err(|err| rpc_error(err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Execute { handle, values }, cancel))
        }
        RpcProc::SpPrepExec => {
            let (decls, stmt, values) =
                rpc::split_sp_prepexec(request.params).map_err(|err| rpc_error(err.to_string()))?;
            Ok(engine.stream_prepared(
                session,
                PreparedRpc::PrepExec {
                    decls,
                    stmt,
                    values,
                },
                cancel,
            ))
        }
        RpcProc::SpUnprepare => {
            let handle = rpc::split_sp_unprepare(request.params)
                .map_err(|err| rpc_error(err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Unprepare { handle }, cancel))
        }
        RpcProc::SpDescribeFirstResultSet => {
            let tsql =
                rpc::split_sp_describe(request.params).map_err(|err| rpc_error(err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Describe { tsql }, cancel))
        }
        // Server-side cursors are not implemented; say so rather than "not
        // found" so a driver's fallback logic gets an honest signal.
        RpcProc::SpCursor(name) => Err((
            40510,
            format!(
                "The stored procedure '{name}' is not supported (server-side cursors are not implemented)."
            ),
        )),
        // Error 2812 is SQL Server's "Could not find stored procedure".
        // A user procedure: the engine resolves the name (2812 if absent),
        // binds the named params, and emits the real RETURNSTATUS and typed
        // RETURNVALUEs for OUTPUT parameters.
        RpcProc::Other(name) => {
            Ok(engine.stream_proc_rpc(session, name, request.params, request.outputs, cancel))
        }
    }
}

/// Renders a batch's [`BatchEvent`]s into a TDS token stream as they arrive.
///
/// The only state it carries is the current result set's column metadata (ROW
/// encoding needs it) and one deferred DONE, so a response of any size renders
/// in constant memory.
#[derive(Default)]
struct BatchRender {
    /// Columns of the result set the last COLMETADATA opened.
    columns: Vec<ResultColumn>,
    /// A finished statement's DONE, held back until the next event says whether
    /// anything follows it: `DONE_MORE` means "not the last token group of this
    /// response", which the statement itself cannot know. The buffered renderer
    /// knew the last statement by its index; a stream has to wait and see.
    pending: Option<PendingDone>,
    /// Set once a batch-stopping ERROR has been written, so the final DONE
    /// carries `DONE_ERROR`.
    errored: bool,
    /// Set when the written ERROR's severity was fatal (>= 20): the reply
    /// finishes, then the connection closes, as SQL Server does.
    fatal: bool,
    /// RPC response framing: per-statement DONEs render as DONEINPROC, and the
    /// response ends RETURNSTATUS → RETURNVALUE(s) → DONEPROC, as SQL Server
    /// frames a procedure's reply. A SQL batch keeps plain DONEs (the #97
    /// oracle pins those bytes).
    rpc: bool,
    /// A prepared handle to report as a RETURNVALUE just before the final
    /// DONEPROC — held so RETURNSTATUS precedes it, SQL Server's order.
    pending_handle: Option<i32>,
    /// The procedure's real RETURN status (RPC-by-name); None keeps the
    /// legacy 0.
    return_status: Option<i32>,
    /// OUTPUT parameter values (ParamOrdinal, name, type, value), held for the
    /// response tail after RETURNSTATUS, before DONEPROC — SQL Server's order.
    return_values: Vec<(
        u16,
        String,
        truthdb_core::relstore::types::ColumnType,
        truthdb_core::relstore::types::Datum,
    )>,
    /// More replies follow this one in the same response (a multi-RPC
    /// request): the final DONEPROC keeps `DONE_MORE` instead of `DONE_FINAL`.
    more_responses: bool,
    /// Scratch for encoding tokens, reused so a row costs no allocation here.
    buf: Vec<u8>,
}

/// A statement's DONE, minus the `more` bit that only the next event settles.
struct PendingDone {
    count: Option<u64>,
    in_transaction: bool,
    curcmd: u16,
}

/// The DONE `CurCmd` value for a statement's command class.
fn curcmd_of(command: truthdb_core::engine::DoneCommand) -> u16 {
    use truthdb_core::engine::DoneCommand;
    match command {
        DoneCommand::Select => token::CMD_SELECT,
        DoneCommand::Insert => token::CMD_INSERT,
        DoneCommand::Update => token::CMD_UPDATE,
        DoneCommand::Delete => token::CMD_DELETE,
        DoneCommand::Other => 0,
    }
}

impl BatchRender {
    /// Renders one event. Returns whether it was the batch's terminal event.
    async fn event<W: AsyncWrite + Unpin>(
        &mut self,
        out: &mut MessageWriter<'_, W>,
        event: BatchEvent,
    ) -> io::Result<bool> {
        match event {
            BatchEvent::Columns(columns) => {
                self.flush_pending(out, true).await?;
                self.columns = columns;
                self.buf.clear();
                token::colmetadata(&mut self.buf, &self.columns);
                out.write(&self.buf).await?;
            }
            BatchEvent::Rows(rows) => {
                self.buf.clear();
                for row in &rows {
                    token::row(&mut self.buf, row, &self.columns);
                    if self.buf.len() >= ROW_FLUSH_BYTES {
                        out.write(&self.buf).await?;
                        self.buf.clear();
                    }
                }
                out.write(&self.buf).await?;
            }
            BatchEvent::StatementDone {
                count,
                in_transaction,
                command,
            } => {
                self.flush_pending(out, true).await?;
                self.pending = Some(PendingDone {
                    count,
                    in_transaction,
                    curcmd: curcmd_of(command),
                });
            }
            BatchEvent::StatementAborted { in_transaction } => {
                // Closes a result set whose statement failed mid-stream — with
                // a CLEAN done, deliberately. Setting `DONE_ERROR` here without
                // a preceding ERROR token reads as "severe failure, discard
                // results" to every real driver: pytds raises "Request failed,
                // server didn't send error message" (`process_end` raises on
                // the flag with no accumulated messages), go-mssqldb v1.8.0
                // synthesizes "Request failed but didn't provide reason" and
                // strands the result sets behind it, and SqlClient's
                // equivalent branch is documented as covering server aborts.
                // The error itself, when the client gets one at all, travels
                // as the batch-final ERROR token exactly as before; a caught
                // (TRY/CATCH) error never surfaces at all.
                self.flush_pending(out, true).await?;
                self.pending = Some(PendingDone {
                    count: None,
                    in_transaction,
                    curcmd: 0,
                });
            }
            BatchEvent::DatabaseContext { database } => {
                // `USE` succeeded: the database-context ENVCHANGE plus the
                // 5701 INFO, exactly what the login sequence emits and what
                // SSMS listens for. The statement's own DONE follows.
                self.flush_pending(out, true).await?;
                token::envchange_database(&mut self.buf, &database);
                token::info(
                    &mut self.buf,
                    5701,
                    2,
                    0,
                    &format!("Changed database context to '{database}'."),
                );
                out.write(&self.buf).await?;
                self.buf.clear();
            }
            BatchEvent::PreparedHandle(handle) => {
                // Held for the response tail: RETURNSTATUS precedes the
                // RETURNVALUE, which precedes the final DONEPROC.
                self.pending_handle = Some(handle);
            }
            BatchEvent::ReturnStatus(status) => {
                self.return_status = Some(status);
            }
            BatchEvent::ReturnValue {
                ordinal,
                name,
                column_type,
                value,
            } => {
                self.return_values.push((ordinal, name, column_type, value));
            }
            BatchEvent::Info(info) => {
                // RAISERROR severity <= 10: an INFO token, not an error — the
                // batch continues and no DONE flag changes. Pending DONEs go
                // out first so stream order holds.
                self.flush_pending(out, true).await?;
                self.buf.clear();
                token::info(
                    &mut self.buf,
                    info.number,
                    info.state,
                    info.level,
                    &info.message,
                );
                out.write(&self.buf).await?;
                self.buf.clear();
            }
            BatchEvent::Error(error) => {
                self.flush_pending(out, true).await?;
                self.buf.clear();
                token::error(
                    &mut self.buf,
                    error.number,
                    error.state,
                    error.level,
                    &error.message,
                );
                out.write(&self.buf).await?;
                self.errored = true;
                // Severity >= 20 kills the connection, as SQL Server does:
                // the reply still finishes (error + final DONE) and then the
                // connection loop closes the stream.
                if error.level >= truthdb_core::engine::FATAL_SEVERITY {
                    self.fatal = true;
                }
            }
            BatchEvent::Complete { in_transaction } => {
                if self.rpc {
                    // The last statement's DONEINPROC keeps DONE_MORE — the
                    // RETURNSTATUS/RETURNVALUE/DONEPROC tail always follows.
                    self.flush_pending(out, true).await?;
                    self.buf.clear();
                    // RETURNSTATUS: a procedure that completed reports its real
                    // status — the engine sends the event only on completion,
                    // even under a continued error, so a warned-but-completed
                    // proc still reports it. Any other clean RPC reply
                    // (sp_executesql, a prepared handle) keeps the status-0
                    // default. An aborted procedure or a failed RPC sends no
                    // status event and is errored, so nothing is emitted.
                    if let Some(status) = self.return_status {
                        token::return_status(&mut self.buf, status);
                    } else if !self.errored {
                        token::return_status(&mut self.buf, 0);
                    }
                    if let Some(handle) = self.pending_handle.take() {
                        token::return_value_int(&mut self.buf, "handle", handle);
                    }
                    // OUTPUT parameters: populated only when the procedure
                    // completed, so emit whatever the engine sent.
                    for (ordinal, name, column_type, value) in self.return_values.drain(..) {
                        token::return_value(&mut self.buf, ordinal, &name, &column_type, &value);
                    }
                    out.write(&self.buf).await?;
                    self.final_done(out, self.errored, in_transaction).await?;
                } else if self.errored {
                    // The error's own final DONE, after the last statement's.
                    self.flush_pending(out, true).await?;
                    self.final_done(out, true, in_transaction).await?;
                } else if self.pending.is_some() {
                    self.flush_pending(out, false).await?;
                } else {
                    // A batch with no statements at all (e.g. only comments):
                    // one final DONE.
                    self.final_done(out, false, in_transaction).await?;
                }
                return Ok(true);
            }
            BatchEvent::Failed(err) => {
                // A genuine engine/storage failure, not a SQL-level error.
                self.flush_pending(out, true).await?;
                self.buf.clear();
                token::error(&mut self.buf, 50000, 1, 16, &err.to_string());
                out.write(&self.buf).await?;
                self.final_done(out, true, false).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Writes the deferred DONE, if any, now that `more` is known.
    async fn flush_pending<W: AsyncWrite + Unpin>(
        &mut self,
        out: &mut MessageWriter<'_, W>,
        more: bool,
    ) -> io::Result<()> {
        if let Some(done) = self.pending.take() {
            self.done(
                out,
                more,
                false,
                done.in_transaction,
                done.count,
                done.curcmd,
            )
            .await?;
        }
        Ok(())
    }

    /// A statement's DONE: DONEINPROC inside an RPC response, plain DONE in a
    /// batch (where `more = false` makes it the batch's final).
    async fn done<W: AsyncWrite + Unpin>(
        &mut self,
        out: &mut MessageWriter<'_, W>,
        more: bool,
        error: bool,
        in_transaction: bool,
        count: Option<u64>,
        curcmd: u16,
    ) -> io::Result<()> {
        self.buf.clear();
        if self.rpc {
            token::done_in_proc(&mut self.buf, more, error, in_transaction, count, curcmd);
        } else {
            token::done(&mut self.buf, more, error, in_transaction, count, curcmd);
        }
        out.write(&self.buf).await
    }

    /// The reply's final DONE: DONEPROC for an RPC (keeping `DONE_MORE` when
    /// more RPCs follow in the same response), plain final DONE for a batch.
    async fn final_done<W: AsyncWrite + Unpin>(
        &mut self,
        out: &mut MessageWriter<'_, W>,
        error: bool,
        in_transaction: bool,
    ) -> io::Result<()> {
        self.buf.clear();
        if self.rpc {
            token::done_proc(
                &mut self.buf,
                self.more_responses,
                error,
                in_transaction,
                None,
            );
        } else {
            token::done(&mut self.buf, false, error, in_transaction, None, 0);
        }
        out.write(&self.buf).await
    }
}
