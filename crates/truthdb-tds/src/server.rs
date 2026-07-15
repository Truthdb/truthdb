//! TDS connection handler: PRELOGIN -> LOGIN7 (auth) -> SQLBatch loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::mpsc;
use truthdb_core::rel::ResultColumn;
use truthdb_core::session::{BatchEvent, EngineHandle, SessionId};

use crate::login::{self, parse_login7};
use crate::packet::{
    self, DEFAULT_PACKET_SIZE, HEADER_LEN, Message, MessageWriter, PKT_ATTENTION, PKT_LOGIN7,
    PKT_PRELOGIN, PKT_RPC, PKT_SQL_BATCH, PKT_TABULAR_RESULT, PKT_TRANSACTION_MANAGER,
    read_message, write_message,
};
use crate::rpc::{self, RpcProc};
use crate::token;

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
                    // A malformed/unsupported request never reached the engine,
                    // so there is no stream to write — just the error tokens.
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
                // Attentions are handled by `run_watching_attention` above.
                let mut out = Vec::new();
                token::done_attention(&mut out);
                write_message(stream, PKT_TABULAR_RESULT, &out, packet_size).await?;
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

/// Streams a batch's reply to the client — writing packets as rows arrive —
/// while concurrently watching the connection for a TDS Attention. Returns
/// `Ok(false)` if the client disconnected mid-batch.
///
/// Reading and writing at once needs both halves of the stream, so it is split;
/// nothing else touches it for the batch's duration, so the split's internal
/// lock is never contended.
///
/// An Attention sets `cancel` (the executor's `check_cancelled` polls see it)
/// and the rest of the reply is dropped rather than rendered: the client is
/// answered with `DONE(attention)`, which per MS-TDS terminates the response.
/// Rows already written stay written — unlike the buffered path this replaces,
/// which could still discard the whole response, streaming means some of it has
/// already left. That is what SQL Server does too, and drivers discard a
/// cancelled response's rows on seeing the attention DONE.
///
/// `AsyncReadExt::read` is cancellation-safe, so bytes read into `hdr` survive a
/// `select!` iteration that resolves to an event instead; any partial header
/// left when the batch ends is completed afterwards so packet framing is intact.
async fn stream_reply<S>(
    stream: &mut S,
    mut events: mpsc::Receiver<BatchEvent>,
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
                    if render.event(&mut out, event).await? {
                        break;
                    }
                }
                // The worker pool vanished without a terminal event.
                None => break,
            },
            read = rd.read(&mut hdr[got..]) => match read? {
                0 => {
                    // Client disconnected mid-batch: abort, then drop the
                    // reply. Dropping the receiver also stops the worker
                    // producing rows nobody will read.
                    cancel.store(true, Ordering::Relaxed);
                    return Ok(false);
                }
                n => {
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
    if got > 0 {
        rd.read_exact(&mut hdr[got..]).await?;
        if hdr[0] == PKT_ATTENTION {
            attention = true;
        } else {
            return Err(protocol_err("unexpected TDS packet during a running batch"));
        }
    }
    if attention {
        let mut done = Vec::new();
        token::done_attention(&mut done);
        out.write(&done).await?;
    }
    out.finish().await?;
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
) -> Result<mpsc::Receiver<BatchEvent>, Vec<u8>> {
    let request = rpc::parse_rpc_request(body)
        .map_err(|err| rpc_error(&format!("malformed RPC request: {err}")))?;
    match request.proc {
        RpcProc::SpExecuteSql => {
            let (sql, params) = rpc::split_sp_executesql(request.params)
                .map_err(|err| rpc_error(&err.to_string()))?;
            Ok(engine.stream_rpc(session, sql, params, cancel))
        }
        // Error 2812 is SQL Server's "Could not find stored procedure".
        RpcProc::Other(name) => Err(rpc_error_num(
            2812,
            &format!("Could not find stored procedure '{name}'."),
        )),
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
    /// response", which the statement itself cannot know.
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

#[cfg(test)]
mod attention_tests {
    use super::*;
    use crate::packet::read_message;
    use tokio::io::AsyncWriteExt;
    use truthdb_core::relstore::types::{ColumnType, Datum};
    use truthdb_sql::error::SqlError;

    /// An Attention header-only packet.
    const ATTN: [u8; 8] = [PKT_ATTENTION, 0x01, 0x00, 0x08, 0, 0, 0, 0];

    #[tokio::test]
    async fn attention_during_batch_cancels_and_acks() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let cancel = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(4);
        let cwork = cancel.clone();
        // A "batch" that emits nothing until its cancel flag is set (like the
        // executor polling `check_cancelled`), then reports the internal error
        // cancellation raises — which the client must never be shown.
        tokio::spawn(async move {
            while !cwork.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            let _ = tx
                .send(BatchEvent::Error(SqlError::message_only(
                    3617,
                    "The query was canceled.",
                )))
                .await;
            let _ = tx
                .send(BatchEvent::Complete {
                    in_transaction: false,
                })
                .await;
        });
        client.write_all(&ATTN).await.expect("send attention");
        assert!(
            stream_reply(&mut server, rx, cancel.clone(), 4096)
                .await
                .expect("io ok"),
            "not disconnected"
        );
        assert!(
            cancel.load(Ordering::Relaxed),
            "the Attention set the cancel flag"
        );
        let response = read_message(&mut client).await.expect("a response");
        let mut expected = Vec::new();
        token::done_attention(&mut expected);
        assert_eq!(
            response.payload, expected,
            "response is DONE(attention), not the cancellation error"
        );
    }

    /// Reads one packet, returning its body and whether it ended the message.
    async fn read_packet<R: AsyncRead + Unpin>(reader: &mut R) -> (Vec<u8>, bool) {
        let mut hdr = [0u8; HEADER_LEN];
        reader.read_exact(&mut hdr).await.expect("packet header");
        let length = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
        let mut body = vec![0u8; length - HEADER_LEN];
        reader.read_exact(&mut body).await.expect("packet body");
        (body, hdr[1] & 0x01 != 0)
    }

    #[tokio::test]
    async fn rows_already_streamed_before_an_attention_stay_written() {
        // Unlike the buffered path this replaced, a cancelled batch cannot
        // un-send what already left: the client keeps the rows it was already
        // given, and DONE(attention) is what tells it to discard them.
        let (mut client, mut server) = tokio::io::duplex(8192);
        let cancel = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(4);
        let columns = vec![ResultColumn {
            name: "id".into(),
            column_type: ColumnType::Int,
        }];
        // Enough rows to overflow the 512-byte packet size below several times
        // over, so packets genuinely reach the client mid-batch — a result that
        // fits one packet would only be written by `finish`, and could not show
        // the difference.
        let rows: Vec<Vec<Datum>> = (0..200).map(|i| vec![Datum::Int(i)]).collect();
        let (sent_cols, sent_rows) = (columns.clone(), rows.clone());
        let cwork = cancel.clone();
        tokio::spawn(async move {
            let _ = tx.send(BatchEvent::Columns(sent_cols)).await;
            let _ = tx.send(BatchEvent::Rows(sent_rows)).await;
            while !cwork.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            let _ = tx
                .send(BatchEvent::Complete {
                    in_transaction: false,
                })
                .await;
        });
        let ccancel = cancel.clone();
        let serving = tokio::spawn(async move {
            stream_reply(&mut server, rx, ccancel, 512)
                .await
                .expect("io ok")
        });
        // Take a whole packet off the wire first: the batch is now provably
        // mid-reply when the Attention lands.
        let (first, eom) = read_packet(&mut client).await;
        assert!(!eom, "the reply is still in flight");
        client.write_all(&ATTN).await.expect("send attention");
        assert!(serving.await.expect("serving task"), "not disconnected");

        let mut payload = first;
        payload.extend(read_message(&mut client).await.expect("rest").payload);
        let mut expected = Vec::new();
        token::colmetadata(&mut expected, &columns);
        for row in &rows {
            token::row(&mut expected, row, &columns);
        }
        token::done_attention(&mut expected);
        assert_eq!(
            payload, expected,
            "the rows already sent, then DONE(attention)"
        );
    }

    #[tokio::test]
    async fn no_attention_renders_the_batch_result() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let cancel = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel(4);
        // The batch completes on its own; no Attention is ever sent.
        tokio::spawn(async move {
            let _ = tx
                .send(BatchEvent::StatementDone {
                    count: Some(3),
                    in_transaction: false,
                })
                .await;
            let _ = tx
                .send(BatchEvent::Complete {
                    in_transaction: false,
                })
                .await;
        });
        assert!(
            stream_reply(&mut server, rx, cancel.clone(), 4096)
                .await
                .expect("io ok")
        );
        assert!(!cancel.load(Ordering::Relaxed));
        let response = read_message(&mut client).await.expect("a response");
        let mut expected = Vec::new();
        token::done(&mut expected, false, false, false, Some(3));
        assert_eq!(
            response.payload, expected,
            "no attention -> the real result"
        );
    }
}

#[cfg(test)]
mod encryption_tests {
    use super::*;

    const ON: u8 = login::ENCRYPT_ON;
    const NOT_SUP: u8 = login::ENCRYPT_NOT_SUP;
    const REQ: u8 = login::ENCRYPT_REQ;

    #[test]
    fn off_never_offers_tls_even_with_a_certificate_configured() {
        // The setting overrides the certificate: this is the case the setting
        // exists for, and the one an end-to-end test cannot reach.
        for client in [ON, NOT_SUP, REQ] {
            assert_eq!(
                negotiate_encryption(Encryption::Off, true, client),
                (NOT_SUP, false),
                "client {client}"
            );
        }
    }

    #[test]
    fn optional_encrypts_only_when_configured_and_asked() {
        assert_eq!(
            negotiate_encryption(Encryption::Optional, true, ON),
            (ON, true)
        );
        assert_eq!(
            negotiate_encryption(Encryption::Optional, true, REQ),
            (ON, true)
        );
        // Asked for, but no certificate to offer.
        assert_eq!(
            negotiate_encryption(Encryption::Optional, false, ON),
            (NOT_SUP, false)
        );
        // Configured, but the client does not want it.
        assert_eq!(
            negotiate_encryption(Encryption::Optional, true, NOT_SUP),
            (NOT_SUP, false)
        );
    }

    #[test]
    fn required_always_demands_encryption() {
        for client in [ON, NOT_SUP, REQ] {
            assert_eq!(
                negotiate_encryption(Encryption::Required, true, client),
                (REQ, true),
                "client {client}"
            );
        }
    }
}
