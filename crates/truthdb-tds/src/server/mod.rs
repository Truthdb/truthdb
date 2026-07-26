//! TDS connection handler: PRELOGIN -> LOGIN7 (auth) -> SQLBatch loop.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::mpsc;
use truthdb_core::auth;
use truthdb_core::engine::EngineError;
use truthdb_core::engine::ResultColumn;
use truthdb_core::session::{BatchEvent, EngineHandle, PreparedRpc, SessionId};

use crate::login::{self, parse_login7};
use crate::packet::{
    self, DEFAULT_PACKET_SIZE, HEADER_LEN, Message, MessageWriter, PKT_ATTENTION, PKT_LOGIN7,
    PKT_PRELOGIN, PKT_RPC, PKT_SQL_BATCH, PKT_TABULAR_RESULT, PKT_TRANSACTION_MANAGER,
    read_message, write_message,
};
use crate::rpc::{self, RpcProc};
use crate::throttle::{self, LoginThrottle};
use crate::token;

mod render;

use render::{ReplySource, stream_reply};

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

/// Server-side TDS configuration: the reported database, optional TLS, and the
/// encryption policy. Login credentials no longer live here — authentication is
/// against catalog logins (see [`crate::throttle`] and `serve_connection`); the
/// config's `[tds.auth]` users are migrated into the catalog at first boot.
#[derive(Debug, Clone)]
pub struct TdsConfig {
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
    throttle: LoginThrottle,
    peer: IpAddr,
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

    // Catalog-backed authentication. The throttle counts this attempt up front
    // (atomically, so a parallel burst cannot slip past the free window) and
    // either imposes a growing delay or, past a hard cap, refuses without
    // checking the credential. The PBKDF2 verification then runs off both the
    // reactor and the engine worker pool (`spawn_blocking`). Not-found, disabled
    // and bad-password all fail identically — same wire response AND same CPU
    // time (a dummy hash is verified when the login is absent) — so nothing
    // reveals which usernames exist.
    match throttle.note_attempt(&login.username, peer) {
        throttle::Decision::Reject => {
            deny_login(&mut stream, &login.username, packet_size).await?;
            return Ok(());
        }
        throttle::Decision::Proceed(delay) => {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
    }
    let record = engine.lookup_login(login.username.clone()).await;
    let blob = record
        .as_ref()
        .map(|rec| rec.password_blob.clone())
        .unwrap_or_else(|| auth::DUMMY_BLOB.to_string());
    let password = login.password.clone();
    let verified = tokio::task::spawn_blocking(move || auth::verify_password(&blob, &password))
        .await
        .map_err(|_| protocol_err("password verification task panicked"))?;
    let (canonical_login, login_sid) = match &record {
        Some(rec) if !rec.is_disabled && verified == auth::VerifyOutcome::Ok => {
            (rec.name.clone(), rec.principal_id)
        }
        _ => {
            // The attempt was already counted by note_attempt above.
            deny_login(&mut stream, &login.username, packet_size).await?;
            return Ok(());
        }
    };
    throttle.record_success(&login.username, peer);

    // Negotiate packet size if the client requested a valid one.
    let requested = login.packet_size as usize;
    if requested >= packet::MIN_PACKET_SIZE {
        packet_size = requested.min(packet::MAX_PACKET_SIZE);
    }

    // The requested database must exist (SQL Server 4060); an empty request
    // falls back to the configured default. The session opens FIRST so the
    // login response can carry the catalog's canonical spelling.
    let database = if login.database.is_empty() {
        config.database.clone()
    } else {
        login.database.clone()
    };
    // Each connection gets an engine-side session; it is closed (rolling back
    // any open transaction) whenever the connection ends, cleanly or not. The
    // database and login are recorded for session intrinsics (DB_NAME() etc.).
    let Ok((session, database)) = engine
        .open_session(database.clone(), canonical_login, login_sid)
        .await
    else {
        return deny_database(&mut stream, &database, packet_size).await;
    };

    // Login response token stream. From here on the session exists, so
    // every exit — including a failed response write — must close it.
    let mut out = Vec::new();
    token::loginack(&mut out);
    token::envchange_database(&mut out, &database);
    token::envchange_sql_collation(&mut out);
    token::envchange_packet_size(&mut out, packet_size, DEFAULT_PACKET_SIZE);
    token::info(
        &mut out,
        5701,
        2,
        10,
        &format!("Changed database context to '{database}'."),
    );
    token::done(&mut out, false, false, false, None, 0);
    if let Err(err) = write_message(&mut stream, PKT_TABULAR_RESULT, &out, packet_size).await {
        engine.close_session(session);
        return Err(err);
    }
    let result = request_loop(&mut stream, &engine, session, packet_size).await;
    engine.close_session(session);
    result
}

/// Writes the single generic login-failure response: error 18456, level 14,
/// state 1, and a DONE. Every failure kind (unknown login, wrong password,
/// disabled, throttle lockout) uses this exact response so nothing on the wire
/// distinguishes them — SQL Server sanitizes the state to 1 for the same reason
/// (a client must not be able to enumerate valid usernames).
/// Refuses a login whose requested database does not exist: SQL Server's
/// 4060 (severity 11), then DONE.
async fn deny_database<S: AsyncWrite + Unpin>(
    stream: &mut S,
    database: &str,
    packet_size: usize,
) -> io::Result<()> {
    let mut out = Vec::new();
    token::error(
        &mut out,
        4060,
        1,
        11,
        &format!("Cannot open database \"{database}\" requested by the login. The login failed."),
    );
    token::done(&mut out, false, true, false, None, 0);
    write_message(stream, PKT_TABULAR_RESULT, &out, packet_size).await
}

/// Writes the single generic login-failure response: error 18456, level 14,
/// state 1, and a DONE. Every failure kind (unknown login, wrong password,
/// disabled, throttle lockout) uses this exact response so nothing on the wire
/// distinguishes them.
async fn deny_login<S: AsyncWrite + Unpin>(
    stream: &mut S,
    username: &str,
    packet_size: usize,
) -> io::Result<()> {
    let mut out = Vec::new();
    token::error(
        &mut out,
        18456,
        1,
        14,
        &format!("Login failed for user '{username}'."),
    );
    token::done(&mut out, false, true, false, None, 0);
    write_message(stream, PKT_TABULAR_RESULT, &out, packet_size).await
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
                let source = ReplySource::Single {
                    events: Some(events),
                    rpc: false,
                };
                if !stream_reply(stream, source, cancel, packet_size).await? {
                    return Ok(());
                }
            }
            PKT_RPC => {
                let headers = parse_all_headers(&message.payload)?;
                check_transaction_descriptor(headers.transaction_descriptor, tran_descriptor)?;
                let cancel = Arc::new(AtomicBool::new(false));
                match rpc::parse_rpc_requests(headers.body) {
                    Ok(requests) => {
                        let source = ReplySource::Rpcs {
                            engine,
                            session,
                            requests: requests.into_iter(),
                        };
                        if !stream_reply(stream, source, cancel, packet_size).await? {
                            return Ok(());
                        }
                    }
                    // An undecodable body never identified an RPC to run —
                    // just the error tokens.
                    Err(err) => {
                        let tokens = rpc_error(&format!("malformed RPC request: {err}"));
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
            token::done(&mut out, false, true, false, None, 0);
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
        token::done(&mut out, false, true, reply.in_transaction, None, 0);
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
    token::done(&mut out, false, false, reply.in_transaction, None, 0);
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
        5 => Some("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"),
        // 0 (unspecified) keeps the default.
        _ => None,
    }
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

fn rpc_error(message: &str) -> Vec<u8> {
    let mut out = Vec::new();
    token::error(&mut out, 50000, 1, 16, message);
    token::done(&mut out, false, true, false, None, 0);
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

#[cfg(test)]
mod attention_tests;
