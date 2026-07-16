//! TDS connection handler: PRELOGIN -> LOGIN7 (auth) -> SQLBatch loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::mpsc;
use truthdb_core::engine::EngineError;
use truthdb_core::rel::ResultColumn;
use truthdb_core::session::{BatchEvent, EngineHandle, PreparedRpc, SessionId};

use crate::login::{self, parse_login7};
use crate::packet::{
    self, DEFAULT_PACKET_SIZE, HEADER_LEN, Message, MessageWriter, PKT_ATTENTION, PKT_LOGIN7,
    PKT_PRELOGIN, PKT_RPC, PKT_SQL_BATCH, PKT_TABULAR_RESULT, PKT_TRANSACTION_MANAGER,
    read_message, write_message,
};
use crate::rpc::{self, RpcProc};
use crate::token;

/// How many bytes of encoded rows may build up before the buffer is handed to
/// the message writer. Only a bound on the scratch buffer: the writer packetizes
/// whatever it is given, so this does not have to relate to the packet size.
const ROW_FLUSH_BYTES: usize = 8 * 1024;

// Transaction Manager request types (MS-TDS 2.2.6.9).
const TM_BEGIN_XACT: u16 = 5;
const TM_COMMIT_XACT: u16 = 7;
const TM_ROLLBACK_XACT: u16 = 8;

/// What the server does about encryption on a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encryption {
    /// Never encrypt: TLS is not offered even if certificates are configured.
    Off,
    /// Encrypt when the client asks to (the default, and what SQL Server calls
    /// opportunistic): a client that does not ask is served in plaintext.
    #[default]
    Optional,
    /// Refuse any client that will not encrypt. The server advertises
    /// `ENCRYPT_REQ`, so a compliant client either encrypts or gives up, and a
    /// client that says it cannot encrypt is dropped rather than served.
    Required,
}

/// Server-side TDS configuration: the login users, the reported database,
/// optional TLS, and the encryption policy.
#[derive(Debug, Clone)]
pub struct TdsConfig {
    /// username -> password (plaintext config auth for Stage 4).
    pub users: HashMap<String, String>,
    /// The single database name reported to clients.
    pub database: String,
    /// TLS certificate/key. When present, the server offers encryption to
    /// clients that request it (a tunneled TLS handshake, then an encrypted
    /// session).
    pub tls: Option<crate::tls::TlsConfig>,
    /// Whether encryption is off, opportunistic, or mandatory. `Required`
    /// needs `tls` to be configured; the server refuses to start otherwise,
    /// since it could satisfy no one.
    pub encryption: Encryption,
}

/// Handles one TDS connection to completion (or disconnect).
pub async fn serve_connection<S>(
    mut stream: S,
    engine: EngineHandle,
    config: Arc<TdsConfig>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut packet_size = DEFAULT_PACKET_SIZE;

    // --- PRELOGIN ---
    let prelogin = read_message(&mut stream).await?;
    if prelogin.kind != PKT_PRELOGIN {
        return Err(protocol_err("expected PRELOGIN"));
    }
    // Encryption policy. `Optional` (the default) is opportunistic: encrypt
    // only when configured AND the client asks (ENCRYPT_ON/REQ). `Off` never
    // encrypts. `Required` advertises ENCRYPT_REQ so a compliant client knows it
    // must encrypt — and a client that answered "cannot" is dropped below rather
    // than quietly served in plaintext. We encrypt the whole session, not the
    // login-only mode.
    let client_enc = login::prelogin_client_encryption(&prelogin.payload);
    let (advertised, offer_tls) =
        negotiate_encryption(config.encryption, config.tls.is_some(), client_enc);
    // The PRELOGIN *response* is sent as a REPLY (Tabular Result) packet;
    // clients (pytds/go-mssqldb) expect type 0x04 here, not 0x12.
    write_message(
        &mut stream,
        PKT_TABULAR_RESULT,
        &login::prelogin_response(advertised),
        packet_size,
    )
    .await?;
    // The client was told encryption is mandatory but says it cannot: answer
    // the PRELOGIN (so it learns why) and then refuse, rather than fall back to
    // plaintext — a fallback would silently defeat the whole setting.
    if config.encryption == Encryption::Required
        && !matches!(client_enc, login::ENCRYPT_ON | login::ENCRYPT_REQ)
    {
        return Err(protocol_err(
            "the server requires encryption; the client does not support it",
        ));
    }

    // If encryption was negotiated, complete the tunneled TLS handshake and run
    // the rest of the session over the encrypted stream.
    let mut stream = if offer_tls {
        let tls = config.tls.as_ref().expect("tls configured");
        crate::tls::MaybeTlsStream::Tls(Box::new(tls.accept(stream).await?))
    } else {
        crate::tls::MaybeTlsStream::Plain(stream)
    };

    // --- LOGIN7 ---
    let login_msg = read_message(&mut stream).await?;
    if login_msg.kind != PKT_LOGIN7 {
        return Err(protocol_err("expected LOGIN7"));
    }
    let login = parse_login7(&login_msg.payload).map_err(|e| protocol_err(e.0))?;

    if !authenticate(&config, &login.username, &login.password) {
        let mut out = Vec::new();
        // 18456: login failed for user.
        token::error(
            &mut out,
            18456,
            1,
            14,
            &format!("Login failed for user '{}'.", login.username),
        );
        token::done(&mut out, false, true, false, None);
        write_message(&mut stream, PKT_TABULAR_RESULT, &out, packet_size).await?;
        return Ok(());
    }

    // Negotiate packet size if the client requested a valid one.
    let requested = login.packet_size as usize;
    if requested >= packet::MIN_PACKET_SIZE {
        packet_size = requested.min(packet::MAX_PACKET_SIZE);
    }

    // Login response token stream.
    let database = if login.database.is_empty() {
        config.database.clone()
    } else {
        login.database.clone()
    };
    let mut out = Vec::new();
    token::loginack(&mut out);
    token::envchange_database(&mut out, &database);
    token::envchange_packet_size(&mut out, packet_size, DEFAULT_PACKET_SIZE);
    token::info(
        &mut out,
        5701,
        2,
        10,
        &format!("Changed database context to '{database}'."),
    );
    token::done(&mut out, false, false, false, None);
    write_message(&mut stream, PKT_TABULAR_RESULT, &out, packet_size).await?;

    // Each connection gets an engine-side session; it is closed (rolling back
    // any open transaction) whenever the connection ends, cleanly or not. The
    // database and login are recorded for session intrinsics (DB_NAME() etc.).
    let session = engine
        .open_session(database.clone(), login.username.clone())
        .await;
    let result = request_loop(&mut stream, &engine, session, packet_size).await;
    engine.close_session(session);
    result
}

async fn request_loop<S>(
    stream: &mut S,
    engine: &EngineHandle,
    session: SessionId,
    packet_size: usize,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // The connection's current transaction descriptor (0 = none). A fresh
    // value is minted per BEGIN so the client can echo it in ALL_HEADERS.
    let mut tran_descriptor: u64 = 0;
    let mut next_descriptor: u64 = 1;
    loop {
        let message = match read_message(stream).await {
            Ok(message) => message,
            // Clean disconnect.
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err),
        };
        match message.kind {
            PKT_SQL_BATCH => {
                let headers = parse_all_headers(&message.payload)?;
                check_transaction_descriptor(headers.transaction_descriptor, tran_descriptor)?;
                let sql = batch_sql(headers.body)?;
                let cancel = Arc::new(AtomicBool::new(false));
                let events = engine.stream_batch(session, sql, cancel.clone());
                if !stream_reply(stream, events, cancel, packet_size).await? {
                    return Ok(());
                }
            }
            PKT_RPC => {
                let headers = parse_all_headers(&message.payload)?;
                check_transaction_descriptor(headers.transaction_descriptor, tran_descriptor)?;
                let cancel = Arc::new(AtomicBool::new(false));
                match start_rpc(engine, session, headers.body, cancel.clone()) {
                    Ok(events) => {
                        if !stream_reply(stream, events, cancel, packet_size).await? {
                            return Ok(());
                        }
                    }
                    // A malformed or unsupported request never reached the
                    // engine, so there is no stream — just the error tokens.
                    Err(tokens) => {
                        write_message(stream, PKT_TABULAR_RESULT, &tokens, packet_size).await?
                    }
                }
            }
            PKT_TRANSACTION_MANAGER => {
                let headers = parse_all_headers(&message.payload)?;
                let response = handle_tm_request(
                    engine,
                    session,
                    headers.body,
                    headers.transaction_descriptor,
                    &mut tran_descriptor,
                    &mut next_descriptor,
                )
                .await?;
                write_message(stream, PKT_TABULAR_RESULT, &response, packet_size).await?;
            }
            PKT_ATTENTION => {
                // An Attention arriving with no batch in flight (the batch it
                // targeted already finished): just acknowledge it. Mid-batch
                // Attentions are handled by `stream_reply` above.
                write_attention_ack(stream, packet_size).await?;
            }
            _ => return Err(protocol_err("unexpected TDS message type")),
        }
    }
}

/// Handles a Transaction Manager request (`db.BeginTx()` / `Commit` /
/// `Rollback` in drivers). Translates it to the equivalent SQL transaction
/// statement, runs it through the session, and answers with the matching
/// transaction ENVCHANGE (types 8/9/10) plus a final DONE.
async fn handle_tm_request(
    engine: &EngineHandle,
    session: SessionId,
    body: &[u8],
    claimed_descriptor: Option<u64>,
    tran_descriptor: &mut u64,
    next_descriptor: &mut u64,
) -> io::Result<Vec<u8>> {
    let (request_type, isolation) = parse_tm_request(body)?;
    // Only COMMIT/ROLLBACK are validated: they name the transaction they end,
    // so a mismatch means the client is ending a transaction it is not in. A
    // BEGIN's descriptor references no transaction yet — it is a placeholder,
    // and real drivers treat it as one: go-mssqldb hardcodes 0 on BEGIN while
    // using the live descriptor everywhere else. Validating it would kill the
    // connection of a perfectly correct client.
    if request_type != TM_BEGIN_XACT {
        check_transaction_descriptor(claimed_descriptor, *tran_descriptor)?;
    }
    let sql = match request_type {
        TM_BEGIN_XACT => match isolation_set_clause(isolation) {
            Some(set) => format!("{set}; BEGIN TRANSACTION"),
            None => "BEGIN TRANSACTION".to_string(),
        },
        TM_COMMIT_XACT => "COMMIT TRANSACTION".to_string(),
        TM_ROLLBACK_XACT => "ROLLBACK TRANSACTION".to_string(),
        other => {
            return Err(protocol_err(&format!(
                "unsupported transaction manager request type {other}"
            )));
        }
    };

    let reply = match engine.run_batch(session, sql).await {
        Ok(reply) => reply,
        Err(err) => {
            let mut out = Vec::new();
            token::error(&mut out, 50000, 1, 16, &err.to_string());
            token::done(&mut out, false, true, false, None);
            return Ok(out);
        }
    };

    let mut out = Vec::new();
    // A SQL-level failure (e.g. COMMIT with no matching BEGIN) is reported as
    // an ERROR with no ENVCHANGE; the descriptor is left unchanged.
    if let Some(error) = &reply.outcome.error {
        token::error(
            &mut out,
            error.number,
            error.state,
            error.level,
            &error.message,
        );
        token::done(&mut out, false, true, reply.in_transaction, None);
        return Ok(out);
    }

    match request_type {
        TM_BEGIN_XACT => {
            let descriptor = *next_descriptor;
            *next_descriptor += 1;
            *tran_descriptor = descriptor;
            token::envchange_begin_tran(&mut out, descriptor);
        }
        // A nested COMMIT (@@TRANCOUNT 2 -> 1) does not end the transaction, so
        // it emits no terminating ENVCHANGE and keeps the descriptor: the
        // transaction the client is in has not changed. Announcing the end of a
        // still-open transaction would contradict this reply's own DONE(INXACT)
        // and, now that the descriptor is validated, desynchronise the client.
        TM_COMMIT_XACT => {
            if !reply.in_transaction {
                token::envchange_commit_tran(&mut out, *tran_descriptor);
                *tran_descriptor = 0;
            }
        }
        TM_ROLLBACK_XACT => {
            if !reply.in_transaction {
                token::envchange_rollback_tran(&mut out, *tran_descriptor);
                *tran_descriptor = 0;
            }
        }
        _ => unreachable!("request type validated above"),
    }
    token::done(&mut out, false, false, reply.in_transaction, None);
    Ok(out)
}

/// Parses a Transaction Manager request body (the bytes *after* ALL_HEADERS): a
/// `RequestType` (u16 LE), then for `TM_BEGIN_XACT` an isolation-level byte.
/// Returns the request type and (for BEGIN) the isolation byte.
fn parse_tm_request(body: &[u8]) -> io::Result<(u16, u8)> {
    if body.len() < 2 {
        return Err(protocol_err("transaction manager request too short"));
    }
    let request_type = u16::from_le_bytes([body[0], body[1]]);
    // TM_BEGIN_XACT carries: IsolationLevel (u8), then a B_VARCHAR name.
    let isolation = if request_type == TM_BEGIN_XACT {
        body.get(2).copied().unwrap_or(0)
    } else {
        0
    };
    Ok((request_type, isolation))
}

/// Maps a TDS isolation-level byte to the equivalent `SET TRANSACTION
/// ISOLATION LEVEL` statement, or `None` to keep the session default.
fn isolation_set_clause(isolation: u8) -> Option<&'static str> {
    match isolation {
        1 => Some("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"),
        2 => Some("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
        3 => Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        4 => Some("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
        // 0 (unspecified) and 5 (snapshot, unsupported) keep the default.
        _ => None,
    }
}

fn authenticate(config: &TdsConfig, username: &str, password: &str) -> bool {
    config
        .users
        .get(username)
        .is_some_and(|expected| expected == password)
}

/// Waits for a cancelled batch to finish, discarding the rest of its reply.
///
/// Every path that leaves `stream_reply` early ends the connection, and
/// `serve_connection` closes the session on its way out — `close_session`
/// releases *the session's* locks, and a batch that is still running still holds
/// them and may still be writing. Releasing them under it would let the next
/// session read rows mid-statement. So the batch is cancelled and then waited
/// for, exactly as the buffered path this replaces did with its
/// `let _ = (&mut work).await`.
///
/// The wait is bounded by the batch, not by the client: `check_cancelled` stops
/// it at its next row, and the worker never blocks on this channel.
async fn drain_to_end(events: &mut mpsc::UnboundedReceiver<BatchEvent>) {
    while events.recv().await.is_some() {}
}

/// Acknowledges an Attention that arrived with no batch in flight:
/// `DONE(attention)` and nothing else.
async fn write_attention_ack<S: AsyncWrite + Unpin>(
    stream: &mut S,
    packet_size: usize,
) -> io::Result<()> {
    let mut out = Vec::new();
    token::done_attention(&mut out);
    write_message(stream, PKT_TABULAR_RESULT, &out, packet_size).await
}

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
async fn stream_reply<S>(
    stream: &mut S,
    mut events: mpsc::UnboundedReceiver<BatchEvent>,
    cancel: Arc<AtomicBool>,
    packet_size: usize,
) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut out = MessageWriter::new(&mut wr, PKT_TABULAR_RESULT, packet_size);
    let mut render = BatchRender::default();
    let mut hdr = [0u8; HEADER_LEN];
    let mut got = 0usize;
    let mut attention = false;
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
    Ok(true)
}

/// Starts an RPC request on the engine, returning its reply stream.
///
/// Supports `sp_executesql` — decode its statement and typed parameters and run
/// them, streaming exactly what a batch would. Any other procedure, or a
/// malformed request, never reaches the engine and comes back as ready-made
/// error tokens (`Err`) plus a final DONE, so the connection stays usable.
fn start_rpc(
    engine: &EngineHandle,
    session: SessionId,
    body: &[u8],
    cancel: Arc<AtomicBool>,
) -> Result<mpsc::UnboundedReceiver<BatchEvent>, Vec<u8>> {
    let request = rpc::parse_rpc_request(body)
        .map_err(|err| rpc_error(&format!("malformed RPC request: {err}")))?;
    match request.proc {
        RpcProc::SpExecuteSql => {
            let (sql, params) = rpc::split_sp_executesql(request.params)
                .map_err(|err| rpc_error(&err.to_string()))?;
            Ok(engine.stream_rpc(session, sql, params, cancel))
        }
        RpcProc::SpPrepare => {
            let (decls, stmt) = rpc::split_sp_prepare(request.params)
                .map_err(|err| rpc_error(&err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Prepare { decls, stmt }, cancel))
        }
        RpcProc::SpExecute => {
            let (handle, values) = rpc::split_sp_execute(request.params)
                .map_err(|err| rpc_error(&err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Execute { handle, values }, cancel))
        }
        RpcProc::SpPrepExec => {
            let (decls, stmt, values) = rpc::split_sp_prepexec(request.params)
                .map_err(|err| rpc_error(&err.to_string()))?;
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
                .map_err(|err| rpc_error(&err.to_string()))?;
            Ok(engine.stream_prepared(session, PreparedRpc::Unprepare { handle }, cancel))
        }
        // Server-side cursors are not implemented; say so rather than "not
        // found" so a driver's fallback logic gets an honest signal.
        RpcProc::SpCursor(name) => Err(rpc_error_num(
            40510,
            &format!("The stored procedure '{name}' is not supported (server-side cursors are not implemented)."),
        )),
        // Error 2812 is SQL Server's "Could not find stored procedure".
        RpcProc::Other(name) => Err(rpc_error_num(
            2812,
            &format!("Could not find stored procedure '{name}'."),
        )),
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
    /// Scratch for encoding tokens, reused so a row costs no allocation here.
    buf: Vec<u8>,
}

/// A statement's DONE, minus the `more` bit that only the next event settles.
struct PendingDone {
    count: Option<u64>,
    in_transaction: bool,
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
            } => {
                self.flush_pending(out, true).await?;
                self.pending = Some(PendingDone {
                    count,
                    in_transaction,
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
                });
            }
            BatchEvent::PreparedHandle(handle) => {
                // The handle rides a RETURNVALUE token, after every result
                // set (return values follow results in an RPC response).
                self.flush_pending(out, true).await?;
                self.buf.clear();
                token::return_value_int(&mut self.buf, "handle", handle);
                out.write(&self.buf).await?;
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
            }
            BatchEvent::Complete { in_transaction } => {
                if self.errored {
                    // The error's own final DONE, after the last statement's.
                    self.flush_pending(out, true).await?;
                    self.done(out, false, true, in_transaction, None).await?;
                } else if self.pending.is_some() {
                    self.flush_pending(out, false).await?;
                } else {
                    // A batch with no statements at all (e.g. only comments):
                    // one final DONE.
                    self.done(out, false, false, in_transaction, None).await?;
                }
                return Ok(true);
            }
            BatchEvent::Failed(err) => {
                // A genuine engine/storage failure, not a SQL-level error.
                self.flush_pending(out, true).await?;
                self.buf.clear();
                token::error(&mut self.buf, 50000, 1, 16, &err.to_string());
                out.write(&self.buf).await?;
                self.done(out, false, true, false, None).await?;
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
            self.done(out, more, false, done.in_transaction, done.count)
                .await?;
        }
        Ok(())
    }

    async fn done<W: AsyncWrite + Unpin>(
        &mut self,
        out: &mut MessageWriter<'_, W>,
        more: bool,
        error: bool,
        in_transaction: bool,
        count: Option<u64>,
    ) -> io::Result<()> {
        self.buf.clear();
        token::done(&mut self.buf, more, error, in_transaction, count);
        out.write(&self.buf).await
    }
}

fn rpc_error(message: &str) -> Vec<u8> {
    rpc_error_num(50000, message)
}

fn rpc_error_num(number: i32, message: &str) -> Vec<u8> {
    let mut out = Vec::new();
    token::error(&mut out, number, 1, 16, message);
    token::done(&mut out, false, true, false, None);
    out
}

/// The Transaction Descriptor header (MS-TDS 2.2.5.3.1): the descriptor the
/// server handed out via ENVCHANGE 8, plus an outstanding-request count.
const HEADER_TRANSACTION_DESCRIPTOR: u16 = 0x0002;

/// A request's parsed ALL_HEADERS block.
struct AllHeaders<'a> {
    /// The descriptor from the Transaction Descriptor header, if the client
    /// sent one (the header is mandatory in MS-TDS, but absence is tolerated —
    /// see [`check_transaction_descriptor`]).
    transaction_descriptor: Option<u64>,
    /// The request body following the header block.
    body: &'a [u8],
}

/// Parses a request payload's ALL_HEADERS block (MS-TDS 2.2.5.2): a
/// `TotalLength u32` covering the whole block (including itself), then headers
/// of `HeaderLength u32 | HeaderType u16 | data`. Unknown header types are
/// skipped (forward compatibility).
///
/// A malformed block is a protocol error rather than being silently taken as
/// request data: treating a bad `TotalLength` as "no headers" would hand the
/// header bytes to the SQL/RPC decoder as if they were the request.
fn parse_all_headers(payload: &[u8]) -> io::Result<AllHeaders<'_>> {
    if payload.len() < 4 {
        return Err(protocol_err("request is missing its ALL_HEADERS block"));
    }
    let total = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if !(4..=payload.len()).contains(&total) {
        return Err(protocol_err("ALL_HEADERS TotalLength is out of range"));
    }
    let mut rest = &payload[4..total];
    let mut transaction_descriptor = None;
    while !rest.is_empty() {
        // Each entry is at least its own length field plus a type.
        if rest.len() < 6 {
            return Err(protocol_err("ALL_HEADERS entry is truncated"));
        }
        let len = u32::from_le_bytes(rest[0..4].try_into().unwrap()) as usize;
        // `len` must cover the header itself and stay inside the block — a zero
        // or oversized length would otherwise stall or overrun the walk.
        if !(6..=rest.len()).contains(&len) {
            return Err(protocol_err("ALL_HEADERS HeaderLength is out of range"));
        }
        if u16::from_le_bytes([rest[4], rest[5]]) == HEADER_TRANSACTION_DESCRIPTOR {
            // TransactionDescriptor u64, then OutstandingRequestCount u32.
            let data = &rest[6..len];
            if data.len() < 12 {
                return Err(protocol_err("transaction descriptor header is truncated"));
            }
            transaction_descriptor = Some(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        }
        rest = &rest[len..];
    }
    Ok(AllHeaders {
        transaction_descriptor,
        body: &payload[total..],
    })
}

/// Rejects a request whose transaction descriptor disagrees with the
/// transaction this connection is actually in — the client's view has
/// desynchronised from the server's, so running the request would apply it to
/// the wrong transaction.
///
/// A client that sends no descriptor header is not checked: the header is only
/// meaningful when present, and refusing requests without one would break
/// clients that omit it (MS-TDS mandates it, but tolerating absence costs
/// nothing here — a client that never sends one never desynchronises).
///
/// Nor is a `TM_BEGIN_XACT` checked — see [`handle_tm_request`].
fn check_transaction_descriptor(claimed: Option<u64>, current: u64) -> io::Result<()> {
    match claimed {
        Some(descriptor) if descriptor != current => Err(protocol_err(&format!(
            "transaction descriptor {descriptor} does not match this connection's transaction {current}"
        ))),
        _ => Ok(()),
    }
}

/// Decodes a SQLBatch body (the UCS-2LE query text after ALL_HEADERS).
fn batch_sql(text: &[u8]) -> io::Result<String> {
    if !text.len().is_multiple_of(2) {
        return Err(protocol_err("SQLBatch text is not UCS-2 aligned"));
    }
    let units: Vec<u16> = text
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16(&units).map_err(|_| protocol_err("SQLBatch text is not valid UTF-16"))
}

/// Decides what to advertise in PRELOGIN, and whether to run a TLS handshake.
///
/// Split out from the connection flow because the interesting cases are the
/// combinations — notably `Off` while a certificate *is* configured, which an
/// end-to-end test cannot reach without shipping a key.
fn negotiate_encryption(policy: Encryption, tls_configured: bool, client: u8) -> (u8, bool) {
    let client_wants_tls = matches!(client, login::ENCRYPT_ON | login::ENCRYPT_REQ);
    match policy {
        // Never encrypt, whatever is configured or asked for.
        Encryption::Off => (login::ENCRYPT_NOT_SUP, false),
        Encryption::Optional if tls_configured && client_wants_tls => (login::ENCRYPT_ON, true),
        Encryption::Optional => (login::ENCRYPT_NOT_SUP, false),
        // Startup refuses `Required` without a certificate, so offering here is
        // always satisfiable; a client that will not encrypt is refused after
        // the response tells it why.
        Encryption::Required => (login::ENCRYPT_REQ, true),
    }
}

fn protocol_err(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Test hook: the raw message reader (used by the in-process TDS client
/// integration test).
#[doc(hidden)]
pub async fn read_raw_message<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Message> {
    read_message(reader).await
}

/// Pins the incremental renderer against the buffered one it replaced.
///
/// The compatibility claim is that a driver cannot tell the difference, so the
/// oracle is the old `build_batch_tokens` + `write_message` pair — kept
/// verbatim — and every batch shape must reach the wire as the same bytes.
/// Rendering now flushes mid-result (at `ROW_FLUSH_BYTES`, and at every packet
/// boundary inside the writer), and a flush landing mid-token or mid-row is
/// exactly how this would break.
#[cfg(test)]
mod render_tests {
    use super::*;
    use crate::packet::{MIN_PACKET_SIZE, read_message};
    use truthdb_core::rel::{BatchOutcome, ResultColumn, RowSet, StatementResult};
    use truthdb_core::relstore::types::{ColumnType, Datum};
    use truthdb_sql::error::SqlError;

    /// The pre-streaming renderer, verbatim. Do not "fix" this to agree with
    /// the new code: it is the oracle, and its job is to disagree.
    fn build_batch_tokens(outcome: &BatchOutcome, in_transaction: bool) -> Vec<u8> {
        let mut out = Vec::new();
        let has_error = outcome.error.is_some();
        let last_index = outcome.results.len().saturating_sub(1);
        for (index, result) in outcome.results.iter().enumerate() {
            let more = index != last_index || has_error;
            match result {
                StatementResult::Rows(rowset) => {
                    token::colmetadata(&mut out, &rowset.columns);
                    for row in &rowset.rows {
                        token::row(&mut out, row, &rowset.columns);
                    }
                    token::done(
                        &mut out,
                        more,
                        false,
                        in_transaction,
                        Some(rowset.rows.len() as u64),
                    );
                }
                StatementResult::RowsAffected(n) => {
                    token::done(&mut out, more, false, in_transaction, Some(*n));
                }
                StatementResult::Done => {
                    token::done(&mut out, more, false, in_transaction, None);
                }
            }
        }
        if let Some(error) = &outcome.error {
            token::error(
                &mut out,
                error.number,
                error.state,
                error.level,
                &error.message,
            );
            token::done(&mut out, false, true, in_transaction, None);
        } else if outcome.results.is_empty() {
            token::done(&mut out, false, false, in_transaction, None);
        }
        out
    }

    /// Renders through the real write path and reassembles the message.
    /// Renders `outcome` through the real path: as the event stream a worker
    /// would produce, drained by `stream_reply` exactly as a connection drains
    /// it. Nothing is short-circuited — the events go through a real channel and
    /// the deferred DONE has to settle `DONE_MORE` by lookahead, which is the
    /// part most likely to disagree with the oracle.
    async fn rendered(outcome: &BatchOutcome, in_xact: bool, packet_size: usize) -> Vec<u8> {
        rendered_events(events_for(outcome, in_xact), packet_size).await
    }

    /// Renders an event stream through the real `stream_reply` and reassembles
    /// the message payload.
    async fn rendered_events(
        events: mpsc::UnboundedReceiver<BatchEvent>,
        packet_size: usize,
    ) -> Vec<u8> {
        let mut wire = Duplex {
            read: std::io::Cursor::new(Vec::new()),
            written: Vec::new(),
        };
        let kept = stream_reply(
            &mut wire,
            events,
            Arc::new(AtomicBool::new(false)),
            packet_size,
        )
        .await
        .expect("stream");
        assert!(kept, "the client did not disconnect");
        let mut cursor = std::io::Cursor::new(wire.written);
        read_message(&mut cursor).await.expect("message").payload
    }

    /// The events a worker's `send_outcome` would produce for `outcome`, in a
    /// closed channel ready to drain.
    fn events_for(outcome: &BatchOutcome, in_xact: bool) -> mpsc::UnboundedReceiver<BatchEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        for result in &outcome.results {
            match result {
                StatementResult::Rows(rowset) => {
                    tx.send(BatchEvent::Columns(rowset.columns.clone()))
                        .unwrap();
                    // Chunked exactly as the sink chunks, so a boundary landing
                    // mid-result is exercised here too.
                    for chunk in rowset.rows.chunks(256) {
                        tx.send(BatchEvent::Rows(chunk.to_vec())).unwrap();
                    }
                    tx.send(BatchEvent::StatementDone {
                        count: Some(rowset.rows.len() as u64),
                        in_transaction: in_xact,
                    })
                    .unwrap();
                }
                StatementResult::RowsAffected(n) => tx
                    .send(BatchEvent::StatementDone {
                        count: Some(*n),
                        in_transaction: in_xact,
                    })
                    .unwrap(),
                StatementResult::Done => tx
                    .send(BatchEvent::StatementDone {
                        count: None,
                        in_transaction: in_xact,
                    })
                    .unwrap(),
            }
        }
        if let Some(error) = &outcome.error {
            tx.send(BatchEvent::Error(error.clone())).unwrap();
        }
        tx.send(BatchEvent::Complete {
            in_transaction: in_xact,
        })
        .unwrap();
        rx
    }

    /// Each DONE carries the transaction state of *its own* statement on the
    /// wire — `BEGIN TRAN; SELECT ...; COMMIT` reads INXACT 1, 1, 0 — instead
    /// of the batch's final state stamped on all of them retroactively.
    #[tokio::test]
    async fn done_inxact_is_per_statement() {
        let (tx, rx) = mpsc::unbounded_channel();
        for (count, in_transaction) in [(None, true), (Some(1), true), (None, false)] {
            tx.send(BatchEvent::StatementDone {
                count,
                in_transaction,
            })
            .unwrap();
        }
        tx.send(BatchEvent::Complete {
            in_transaction: false,
        })
        .unwrap();
        drop(tx);

        let mut expected = Vec::new();
        token::done(&mut expected, true, false, true, None);
        token::done(&mut expected, true, false, true, Some(1));
        token::done(&mut expected, false, false, false, None);
        assert_eq!(rendered_events(rx, 4096).await, expected);
    }

    /// A statement that fails after its result set began streaming closes the
    /// set with a CLEAN done — never `DONE_ERROR` without an ERROR token,
    /// which pytds and go-mssqldb both turn into a synthesized "request
    /// failed" error that strands every result set behind it — and the stream
    /// stays framed for what follows, here a CATCH block's own result set.
    #[tokio::test]
    async fn an_aborted_statement_closes_its_rowset_with_a_clean_done() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(BatchEvent::Columns(columns())).unwrap();
        tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
            .unwrap();
        tx.send(BatchEvent::StatementAborted {
            in_transaction: false,
        })
        .unwrap();
        tx.send(BatchEvent::Columns(columns())).unwrap();
        tx.send(BatchEvent::Rows(vec![vec![Datum::Int(9)]]))
            .unwrap();
        tx.send(BatchEvent::StatementDone {
            count: Some(1),
            in_transaction: false,
        })
        .unwrap();
        tx.send(BatchEvent::Complete {
            in_transaction: false,
        })
        .unwrap();
        drop(tx);

        let mut expected = Vec::new();
        token::colmetadata(&mut expected, &columns());
        token::row(&mut expected, &[Datum::Int(1)], &columns());
        token::done(&mut expected, true, false, false, None);
        token::colmetadata(&mut expected, &columns());
        token::row(&mut expected, &[Datum::Int(9)], &columns());
        token::done(&mut expected, false, false, false, Some(1));
        assert_eq!(rendered_events(rx, 4096).await, expected);
    }

    /// An abort as the batch's LAST statement event (an empty CATCH at the end
    /// of the batch): its pending DONE becomes the batch-final DONE and must
    /// be clean — the batch succeeded, its one error was caught. The buffered
    /// path sent a single clean final DONE for this batch; a final
    /// `DONE_ERROR` with no ERROR token would read as a failed batch.
    #[tokio::test]
    async fn an_abort_ending_the_batch_leaves_the_final_done_clean() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(BatchEvent::Columns(columns())).unwrap();
        tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
            .unwrap();
        tx.send(BatchEvent::StatementAborted {
            in_transaction: false,
        })
        .unwrap();
        tx.send(BatchEvent::Complete {
            in_transaction: false,
        })
        .unwrap();
        drop(tx);

        let mut expected = Vec::new();
        token::colmetadata(&mut expected, &columns());
        token::row(&mut expected, &[Datum::Int(1)], &columns());
        token::done(&mut expected, false, false, false, None);
        assert_eq!(rendered_events(rx, 4096).await, expected);
    }

    /// A write half that fails on the first socket write, read half pending
    /// forever — a client that died while a result was streaming to it.
    struct FailingWrite;

    impl AsyncRead for FailingWrite {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    impl AsyncWrite for FailingWrite {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A socket write failure mid-stream must cancel the batch and wait for
    /// it to end before returning — the caller closes the session the moment
    /// `stream_reply` returns, which releases the batch's locks, so returning
    /// while the batch still runs would let another session read its
    /// uncommitted writes. `stream_reply` returning at all proves the drain
    /// ran (it waits for the channel to close), and the sender task below
    /// only closes the channel once it observes the cancel flag.
    #[tokio::test]
    async fn a_write_error_mid_stream_cancels_and_drains_the_batch() {
        let (tx, rx) = mpsc::unbounded_channel();
        // Enough rows that rendering must write a packet to the (dead) socket.
        tx.send(BatchEvent::Columns(columns())).unwrap();
        tx.send(BatchEvent::Rows(
            (0..200).map(|i| vec![Datum::Int(i)]).collect(),
        ))
        .unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let observed = cancel.clone();
        // The "worker": keeps the batch open until it sees the cancel, then
        // ends it — as the real executor's check_cancelled poll does.
        let worker = tokio::spawn(async move {
            while !observed.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            let _ = tx.send(BatchEvent::Complete {
                in_transaction: false,
            });
            drop(tx);
        });

        let mut wire = FailingWrite;
        let result = stream_reply(&mut wire, rx, cancel.clone(), MIN_PACKET_SIZE).await;
        assert!(result.is_err(), "the write error surfaces");
        assert!(
            cancel.load(Ordering::Relaxed),
            "the running batch was cancelled before stream_reply returned"
        );
        worker.await.expect("worker");
    }

    /// A stream whose read half never yields (no Attention ever arrives) and
    /// whose write half collects the bytes. `stream_reply` splits it, so it
    /// needs both halves on one object.
    struct Duplex {
        read: std::io::Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl AsyncRead for Duplex {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            // Never ready: a client that sends nothing during its batch. Not
            // `Ok(())` with zero bytes, which `stream_reply` reads as a
            // disconnect.
            std::task::Poll::Pending
        }
    }

    impl AsyncWrite for Duplex {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            self.written.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn columns() -> Vec<ResultColumn> {
        vec![ResultColumn {
            name: "id".into(),
            column_type: ColumnType::Int,
        }]
    }

    fn rowset(n: i32) -> RowSet {
        RowSet {
            columns: columns(),
            rows: (0..n).map(|i| vec![Datum::Int(i)]).collect(),
        }
    }

    fn err() -> SqlError {
        SqlError::new(2627, 14, 1, "Violation of PRIMARY KEY constraint.")
    }

    fn outcome(results: Vec<StatementResult>, error: Option<SqlError>) -> BatchOutcome {
        BatchOutcome { results, error }
    }

    #[tokio::test]
    async fn every_batch_shape_reaches_the_wire_as_the_buffered_path_did() {
        let cases: Vec<(&str, BatchOutcome)> = vec![
            // A batch with no statements at all: one final DONE.
            ("empty batch", outcome(Vec::new(), None)),
            // A rowset, including the zero-row case whose DONE still carries 0,
            // and one big enough to cross ROW_FLUSH_BYTES mid-result.
            (
                "zero-row rowset",
                outcome(vec![StatementResult::Rows(rowset(0))], None),
            ),
            (
                "small rowset",
                outcome(vec![StatementResult::Rows(rowset(3))], None),
            ),
            (
                "large rowset",
                outcome(vec![StatementResult::Rows(rowset(5000))], None),
            ),
            // A row count (DML) and a bare DONE (DDL).
            (
                "rows affected",
                outcome(vec![StatementResult::RowsAffected(5)], None),
            ),
            ("ddl", outcome(vec![StatementResult::Done], None)),
            // Several statements: every DONE but the last says MORE.
            (
                "three statements",
                outcome(
                    vec![
                        StatementResult::Rows(rowset(2)),
                        StatementResult::Rows(rowset(1)),
                        StatementResult::RowsAffected(9),
                    ],
                    None,
                ),
            ),
            // With an error the last statement's DONE says MORE too, because
            // the error's DONE is the final one.
            (
                "results then error",
                outcome(vec![StatementResult::Rows(rowset(2))], Some(err())),
            ),
            ("error only", outcome(Vec::new(), Some(err()))),
        ];
        for (case, batch) in &cases {
            for in_xact in [false, true] {
                // Small packets so a large result is split many times, and the
                // default so the common path is covered too.
                for packet_size in [MIN_PACKET_SIZE, DEFAULT_PACKET_SIZE] {
                    assert_eq!(
                        rendered(batch, in_xact, packet_size).await,
                        build_batch_tokens(batch, in_xact),
                        "{case} (in_transaction={in_xact}, packet_size={packet_size})"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn a_large_result_is_split_into_many_packets_but_one_message() {
        // The point of the writer: the bytes leave as they are rendered rather
        // than accumulating into one buffer. A 5000-row result at the minimum
        // packet size is hundreds of packets and exactly one EOM.
        let batch = outcome(vec![StatementResult::Rows(rowset(5000))], None);
        let events = events_for(&batch, false);
        let mut duplex = Duplex {
            read: std::io::Cursor::new(Vec::new()),
            written: Vec::new(),
        };
        stream_reply(
            &mut duplex,
            events,
            Arc::new(AtomicBool::new(false)),
            MIN_PACKET_SIZE,
        )
        .await
        .expect("stream");
        let wire = duplex.written;
        let mut packets = 0;
        let mut eom = 0;
        let mut offset = 0;
        while offset < wire.len() {
            let length = u16::from_be_bytes([wire[offset + 2], wire[offset + 3]]) as usize;
            if wire[offset + 1] & 0x01 != 0 {
                eom += 1;
            }
            packets += 1;
            offset += length;
        }
        // 5000 rows of one INT is ~25 KB of ROW tokens, so ~50 packets of 504
        // payload bytes. The bound is deliberately loose — the claim is "many
        // packets, still one message", not an exact encoding size.
        assert!(
            packets > 30,
            "a big result spans many packets, got {packets}"
        );
        assert_eq!(eom, 1, "exactly one packet ends the message");
        assert_eq!(offset, wire.len(), "packet lengths tile the stream exactly");
    }
}

#[cfg(test)]
mod attention_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use truthdb_core::rel::ResultColumn;
    use truthdb_core::relstore::types::{ColumnType, Datum};

    /// An Attention header-only packet.
    const ATTN: [u8; 8] = [PKT_ATTENTION, 0x01, 0x00, 0x08, 0, 0, 0, 0];

    fn columns() -> Vec<ResultColumn> {
        vec![ResultColumn {
            name: "id".to_string(),
            column_type: ColumnType::Int,
        }]
    }

    /// A batch that produces nothing until its cancel flag is set — the
    /// executor polling `check_cancelled` before its first row. Emitting
    /// nothing up front is what makes the test deterministic: `select!` picks
    /// randomly among ready branches, so a batch with rows waiting would
    /// sometimes render them before the Attention and sometimes not.
    fn cancellable_batch(cancel: Arc<AtomicBool>) -> mpsc::UnboundedReceiver<BatchEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while !cancel.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            // A cancelled batch reports the executor's internal cancel error,
            // which the renderer must not put on the wire.
            tx.send(BatchEvent::Error(
                truthdb_sql::error::SqlError::message_only(3617, "The query was canceled."),
            ))
            .ok();
            tx.send(BatchEvent::Complete {
                in_transaction: false,
            })
            .ok();
        });
        rx
    }

    #[tokio::test]
    async fn attention_during_batch_cancels_and_acks() {
        let (mut client, server) = tokio::io::duplex(4096);
        let mut server = server;
        let cancel = Arc::new(AtomicBool::new(false));
        let events = cancellable_batch(cancel.clone());
        // The client sends an Attention (header-only packet) during the batch.
        client.write_all(&ATTN).await.expect("send attention");
        let kept = stream_reply(&mut server, events, cancel.clone(), 4096)
            .await
            .expect("io ok");
        assert!(kept, "the client is still connected");
        assert!(
            cancel.load(Ordering::Relaxed),
            "the Attention set the cancel flag"
        );

        let mut cursor = std::io::Cursor::new(read_client_bytes(&mut client).await);
        let payload = crate::packet::read_message(&mut cursor)
            .await
            .expect("message")
            .payload;
        // DONE(attention) and nothing else. The executor's internal 3617 is
        // never rendered — the buffered path did not show it, and a client must
        // not see it either — even though the batch reported it.
        let mut expected = Vec::new();
        token::done_attention(&mut expected);
        assert_eq!(payload, expected);
    }

    #[tokio::test]
    async fn no_attention_renders_the_batch_normally() {
        let (mut client, server) = tokio::io::duplex(4096);
        let mut server = server;
        let cancel = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(BatchEvent::Columns(columns())).unwrap();
        tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
            .unwrap();
        tx.send(BatchEvent::StatementDone {
            count: Some(1),
            in_transaction: false,
        })
        .unwrap();
        tx.send(BatchEvent::Complete {
            in_transaction: false,
        })
        .unwrap();
        drop(tx);

        let kept = stream_reply(&mut server, rx, cancel.clone(), 4096)
            .await
            .expect("io ok");
        assert!(kept);
        assert!(!cancel.load(Ordering::Relaxed));
        let mut cursor = std::io::Cursor::new(read_client_bytes(&mut client).await);
        let payload = crate::packet::read_message(&mut cursor)
            .await
            .expect("message")
            .payload;
        // One final DONE carrying the row count, and no attention bit.
        let mut expected = Vec::new();
        token::colmetadata(&mut expected, &columns());
        token::row(&mut expected, &[Datum::Int(1)], &columns());
        token::done(&mut expected, false, false, false, Some(1));
        assert_eq!(payload, expected);
    }

    #[tokio::test]
    async fn a_reply_that_ends_without_a_terminal_event_is_a_clean_error() {
        // The worker panicked, or the pool dropped the call at shutdown. Falling
        // through here would emit a message with NO DONE at all, leaving the
        // client waiting on a result that never terminates. The buffered path
        // turned a dead reply channel into a clean 50000.
        let (mut client, server) = tokio::io::duplex(4096);
        let mut server = server;
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(BatchEvent::Columns(columns())).unwrap();
        tx.send(BatchEvent::Rows(vec![vec![Datum::Int(1)]]))
            .unwrap();
        // A DONE is still deferred here — it must not be lost either.
        tx.send(BatchEvent::StatementDone {
            count: Some(1),
            in_transaction: false,
        })
        .unwrap();
        drop(tx);

        let kept = stream_reply(&mut server, rx, Arc::new(AtomicBool::new(false)), 4096)
            .await
            .expect("io ok");
        assert!(kept);
        let mut cursor = std::io::Cursor::new(read_client_bytes(&mut client).await);
        let payload = crate::packet::read_message(&mut cursor)
            .await
            .expect("message")
            .payload;
        // The deferred DONE is flushed with DONE_MORE (something does follow
        // it), then the error and its final DONE_ERROR. Without the fix this
        // message has no DONE at all.
        let mut expected = Vec::new();
        token::colmetadata(&mut expected, &columns());
        token::row(&mut expected, &[Datum::Int(1)], &columns());
        token::done(&mut expected, true, false, false, Some(1));
        token::error(
            &mut expected,
            50000,
            1,
            16,
            &EngineError::Unavailable.to_string(),
        );
        token::done(&mut expected, false, true, false, None);
        assert_eq!(payload, expected);
    }

    #[tokio::test]
    async fn an_early_exit_waits_for_the_batch_it_cancelled_to_finish() {
        // `serve_connection` closes the session the moment this returns, and
        // `close_session` releases *the session's* locks. A batch that is still
        // running still holds them and may still be writing, so returning early
        // hands its locks to the next session mid-statement — a dirty read the
        // engine has no defence against, since nothing can abort a running
        // batch. The buffered path awaited the batch here for this reason.
        let (client, server) = tokio::io::duplex(4096);
        let mut server = server;
        drop(client);
        let cancel = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));

        let (tx, rx) = mpsc::unbounded_channel();
        let batch_cancel = cancel.clone();
        let batch_finished = finished.clone();
        tokio::spawn(async move {
            // The batch notices its cancel flag, then still has work to do
            // before it returns and its locks are released.
            while !batch_cancel.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
            batch_finished.store(true, Ordering::Relaxed);
            tx.send(BatchEvent::Complete {
                in_transaction: false,
            })
            .ok();
            // `tx` drops here: the sink dying is what says the batch is over.
        });

        let kept = stream_reply(&mut server, rx, cancel.clone(), 4096)
            .await
            .expect("io ok");
        assert!(!kept, "the client disconnected");
        assert!(
            cancel.load(Ordering::Relaxed),
            "a vanished client cancels the batch it left running"
        );
        assert!(
            finished.load(Ordering::Relaxed),
            "returned while the batch was still running: the caller would now \
             release its locks out from under it"
        );
    }

    #[tokio::test]
    async fn a_disconnect_mid_batch_is_reported_as_such() {
        let (client, server) = tokio::io::duplex(4096);
        let mut server = server;
        drop(client);
        let cancel = Arc::new(AtomicBool::new(false));
        let events = cancellable_batch(cancel.clone());
        let kept = stream_reply(&mut server, events, cancel.clone(), 4096)
            .await
            .expect("io ok");
        assert!(!kept, "the client disconnected");
        assert!(
            cancel.load(Ordering::Relaxed),
            "a vanished client still cancels the batch it left running"
        );
    }

    /// Everything the server has written so far.
    async fn read_client_bytes(client: &mut tokio::io::DuplexStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        // The server half is still open, so read what is buffered and stop.
        loop {
            let mut chunk = [0u8; 4096];
            match tokio::time::timeout(
                std::time::Duration::from_millis(50),
                client.read(&mut chunk),
            )
            .await
            {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
                Ok(Err(_)) => break,
            }
        }
        buf
    }
}
