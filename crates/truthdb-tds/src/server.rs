//! TDS connection handler: PRELOGIN -> LOGIN7 (auth) -> SQLBatch loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite};
use truthdb_core::engine::EngineError;
use truthdb_core::rel::{BatchOutcome, StatementResult};
use truthdb_core::session::{BatchReply, EngineHandle, SessionId};

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
                let work = run_batch(engine, session, sql, cancel.clone());
                match run_watching_attention(stream, cancel, work).await? {
                    Watched::Finished(reply) => write_reply(stream, reply, packet_size).await?,
                    Watched::Attention => write_attention_ack(stream, packet_size).await?,
                    Watched::Disconnected => return Ok(()),
                }
            }
            PKT_RPC => {
                let headers = parse_all_headers(&message.payload)?;
                check_transaction_descriptor(headers.transaction_descriptor, tran_descriptor)?;
                let cancel = Arc::new(AtomicBool::new(false));
                let work = run_rpc(engine, session, headers.body, cancel.clone());
                match run_watching_attention(stream, cancel, work).await? {
                    Watched::Finished(reply) => write_reply(stream, reply, packet_size).await?,
                    Watched::Attention => write_attention_ack(stream, packet_size).await?,
                    Watched::Disconnected => return Ok(()),
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

/// Runs `work` (a batch/RPC that respects `cancel`) while concurrently watching
/// the connection for a TDS Attention. If one arrives, `cancel` is set so the
/// executor aborts the statement, and the caller answers with `DONE(attention)`
/// instead of the (discarded) result.
///
/// `AsyncReadExt::read` is cancellation-safe, so bytes read into `hdr` survive a
/// `select!` iteration that resolves to the batch instead; any partial header
/// left when the batch wins is completed afterwards so packet framing is intact.
async fn run_watching_attention<S, F, T>(
    stream: &mut S,
    cancel: Arc<AtomicBool>,
    work: F,
) -> io::Result<Watched<T>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: std::future::Future<Output = T>,
{
    let mut work = std::pin::pin!(work);
    let mut hdr = [0u8; HEADER_LEN];
    let mut got = 0usize;
    let mut attention = false;
    let result = loop {
        tokio::select! {
            resp = &mut work => break resp,
            read = stream.read(&mut hdr[got..]) => match read? {
                0 => {
                    // Client disconnected mid-batch: abort and drain, then drop.
                    cancel.store(true, Ordering::Relaxed);
                    let _ = (&mut work).await;
                    return Ok(Watched::Disconnected);
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
                            let _ = (&mut work).await;
                            return Err(protocol_err(
                                "unexpected TDS packet during a running batch",
                            ));
                        }
                    }
                }
            },
        }
    };
    // The batch finished before a header fully arrived: complete it so the next
    // read stays framed (and still honour a late Attention).
    if got > 0 {
        stream.read_exact(&mut hdr[got..]).await?;
        if hdr[0] == PKT_ATTENTION {
            attention = true;
        } else {
            return Err(protocol_err("unexpected TDS packet during a running batch"));
        }
    }
    if attention {
        Ok(Watched::Attention)
    } else {
        Ok(Watched::Finished(result))
    }
}

/// How a batch ended, from the connection's point of view.
enum Watched<T> {
    /// It ran to completion; nothing interrupted it.
    Finished(T),
    /// A TDS Attention arrived: it was cancelled and its result discarded.
    Attention,
    /// The client disconnected mid-batch.
    Disconnected,
}

/// Acknowledges a cancelled batch: `DONE(attention)` and nothing else.
async fn write_attention_ack<S: AsyncWrite + Unpin>(
    stream: &mut S,
    packet_size: usize,
) -> io::Result<()> {
    let mut out = Vec::new();
    token::done_attention(&mut out);
    write_message(stream, PKT_TABULAR_RESULT, &out, packet_size).await
}

/// A reply ready to go to the client: a batch's outcome, or ready-made tokens
/// for a request that never reached the engine (a malformed or unsupported
/// RPC), which carry error numbers an `EngineError` cannot express.
enum Reply {
    Batch(Result<BatchReply, EngineError>),
    Tokens(Vec<u8>),
}

/// Writes a reply as one TDS message, rendering tokens straight into packets as
/// they are produced.
///
/// The reply is still a whole `BatchOutcome` — the executor materializes it —
/// but rendering no longer doubles that: rather than encoding every token into
/// one `Vec<u8>` and chunking it afterwards, each packet goes to the socket as
/// it fills, so rendering costs one packet's worth of bytes however many rows
/// the result carries.
async fn write_reply<S: AsyncWrite + Unpin>(
    stream: &mut S,
    reply: Reply,
    packet_size: usize,
) -> io::Result<()> {
    let mut out = MessageWriter::new(stream, PKT_TABULAR_RESULT, packet_size);
    let mut buf = Vec::new();
    match reply {
        Reply::Batch(Ok(reply)) => {
            write_batch_tokens(&mut out, &mut buf, &reply.outcome, reply.in_transaction).await?
        }
        Reply::Batch(Err(err)) => {
            // A genuine engine/storage failure (not a SQL-level error).
            token::error(&mut buf, 50000, 1, 16, &err.to_string());
            token::done(&mut buf, false, true, false, None);
            out.write(&buf).await?;
        }
        Reply::Tokens(tokens) => out.write(&tokens).await?,
    }
    out.finish().await
}

/// Writes the COLMETADATA/ROW/DONE/ERROR token stream for a batch outcome.
/// `in_transaction` sets `DONE_INXACT` so the client knows the connection is
/// still inside a transaction.
///
/// `buf` is scratch each token is encoded into before being appended to the
/// message. It is handed over and cleared once a packet's worth has built up,
/// so a million-row result never assembles into one buffer.
async fn write_batch_tokens<W: AsyncWrite + Unpin>(
    out: &mut MessageWriter<'_, W>,
    buf: &mut Vec<u8>,
    outcome: &BatchOutcome,
    in_transaction: bool,
) -> io::Result<()> {
    let has_error = outcome.error.is_some();
    let last_index = outcome.results.len().saturating_sub(1);
    for (index, result) in outcome.results.iter().enumerate() {
        // A DONE is final only when it is the last token group of the whole
        // response (no more results and no trailing error).
        let more = index != last_index || has_error;
        buf.clear();
        match result {
            StatementResult::Rows(rowset) => {
                token::colmetadata(buf, &rowset.columns);
                for row in &rowset.rows {
                    token::row(buf, row, &rowset.columns);
                    if buf.len() >= ROW_FLUSH_BYTES {
                        out.write(buf).await?;
                        buf.clear();
                    }
                }
                token::done(
                    buf,
                    more,
                    false,
                    in_transaction,
                    Some(rowset.rows.len() as u64),
                );
            }
            StatementResult::RowsAffected(n) => {
                token::done(buf, more, false, in_transaction, Some(*n));
            }
            StatementResult::Done => {
                token::done(buf, more, false, in_transaction, None);
            }
        }
        out.write(buf).await?;
    }
    buf.clear();
    if let Some(error) = &outcome.error {
        token::error(buf, error.number, error.state, error.level, &error.message);
        token::done(buf, false, true, in_transaction, None);
    } else if outcome.results.is_empty() {
        // Empty batch (e.g. only comments): a single final DONE.
        token::done(buf, false, false, in_transaction, None);
    }
    out.write(buf).await
}

/// Runs a SQL batch through the engine actor. The batch is cancellable: setting
/// `cancel` (on a TDS Attention) aborts it.
async fn run_batch(
    engine: &EngineHandle,
    session: SessionId,
    sql: String,
    cancel: Arc<AtomicBool>,
) -> Reply {
    Reply::Batch(engine.run_batch_cancellable(session, sql, cancel).await)
}

/// Handles an RPC request. Supports `sp_executesql` — decode its statement and
/// typed parameters, run them, and reply exactly as a batch would. Any other
/// procedure, or a malformed request, is answered with an error token plus a
/// final DONE so the connection stays usable.
async fn run_rpc(
    engine: &EngineHandle,
    session: SessionId,
    body: &[u8],
    cancel: Arc<AtomicBool>,
) -> Reply {
    let request = match rpc::parse_rpc_request(body) {
        Ok(request) => request,
        Err(err) => return Reply::Tokens(rpc_error(&format!("malformed RPC request: {err}"))),
    };
    match request.proc {
        RpcProc::SpExecuteSql => {
            let (sql, params) = match rpc::split_sp_executesql(request.params) {
                Ok(split) => split,
                Err(err) => return Reply::Tokens(rpc_error(&err.to_string())),
            };
            Reply::Batch(
                engine
                    .run_rpc_cancellable(session, sql, params, cancel)
                    .await,
            )
        }
        // Error 2812 is SQL Server's "Could not find stored procedure".
        RpcProc::Other(name) => Reply::Tokens(rpc_error_num(
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
    use truthdb_core::rel::{ResultColumn, RowSet};
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
    async fn rendered(outcome: &BatchOutcome, in_xact: bool, packet_size: usize) -> Vec<u8> {
        let mut wire = Vec::new();
        write_reply(
            &mut wire,
            Reply::Batch(Ok(BatchReply {
                outcome: BatchOutcome {
                    results: outcome.results.clone(),
                    error: outcome.error.clone(),
                },
                in_transaction: in_xact,
            })),
            packet_size,
        )
        .await
        .expect("write");
        let mut cursor = std::io::Cursor::new(wire);
        read_message(&mut cursor).await.expect("message").payload
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
        let mut wire = Vec::new();
        write_reply(
            &mut wire,
            Reply::Batch(Ok(BatchReply {
                outcome: batch,
                in_transaction: false,
            })),
            MIN_PACKET_SIZE,
        )
        .await
        .expect("write");
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

    /// An Attention header-only packet.
    const ATTN: [u8; 8] = [PKT_ATTENTION, 0x01, 0x00, 0x08, 0, 0, 0, 0];

    #[tokio::test]
    async fn attention_during_batch_cancels_and_acks() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let cancel = Arc::new(AtomicBool::new(false));
        let cwork = cancel.clone();
        // A "batch" that runs until its cancel flag is set (like the executor
        // polling `check_cancelled`), then would return a normal result.
        let work = async move {
            loop {
                if cwork.load(Ordering::Relaxed) {
                    return b"RESULT".to_vec();
                }
                tokio::task::yield_now().await;
            }
        };
        // The client sends an Attention (header-only packet) during the batch.
        client.write_all(&ATTN).await.expect("send attention");
        let watched = run_watching_attention(&mut server, cancel.clone(), work)
            .await
            .expect("io ok");
        assert!(
            cancel.load(Ordering::Relaxed),
            "the Attention set the cancel flag"
        );
        assert!(
            matches!(watched, Watched::Attention),
            "the result is discarded for a DONE(attention)"
        );
    }

    #[tokio::test]
    async fn no_attention_returns_the_batch_result() {
        let (_client, mut server) = tokio::io::duplex(64);
        let cancel = Arc::new(AtomicBool::new(false));
        // The batch completes on its own; no Attention is ever sent.
        let work = async move { b"RESULT".to_vec() };
        let watched = run_watching_attention(&mut server, cancel.clone(), work)
            .await
            .expect("io ok");
        assert!(!cancel.load(Ordering::Relaxed));
        match watched {
            Watched::Finished(out) => assert_eq!(out, b"RESULT".to_vec()),
            _ => panic!("no attention -> the real result"),
        }
    }

    #[tokio::test]
    async fn a_disconnect_mid_batch_is_reported_as_such() {
        let (client, mut server) = tokio::io::duplex(64);
        drop(client);
        let cancel = Arc::new(AtomicBool::new(false));
        let cwork = cancel.clone();
        let work = async move {
            loop {
                if cwork.load(Ordering::Relaxed) {
                    return b"RESULT".to_vec();
                }
                tokio::task::yield_now().await;
            }
        };
        let watched = run_watching_attention(&mut server, cancel.clone(), work)
            .await
            .expect("io ok");
        assert!(matches!(watched, Watched::Disconnected));
        assert!(
            cancel.load(Ordering::Relaxed),
            "a vanished client still cancels the batch it left running"
        );
    }
}
