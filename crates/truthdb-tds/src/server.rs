//! TDS connection handler: PRELOGIN -> LOGIN7 (auth) -> SQLBatch loop.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite};
use truthdb_core::rel::{BatchOutcome, StatementResult};
use truthdb_core::session::{EngineHandle, SessionId};

use crate::login::{self, parse_login7};
use crate::packet::{
    self, DEFAULT_PACKET_SIZE, HEADER_LEN, Message, PKT_ATTENTION, PKT_LOGIN7, PKT_PRELOGIN,
    PKT_RPC, PKT_SQL_BATCH, PKT_TABULAR_RESULT, PKT_TRANSACTION_MANAGER, read_message,
    write_message,
};
use crate::rpc::{self, RpcProc};
use crate::token;

// Transaction Manager request types (MS-TDS 2.2.6.9).
const TM_BEGIN_XACT: u16 = 5;
const TM_COMMIT_XACT: u16 = 7;
const TM_ROLLBACK_XACT: u16 = 8;

/// Server-side TDS configuration: the login users, the reported database, and
/// optional TLS.
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
    // Offer TLS only when configured AND the client asks to encrypt (ENCRYPT_ON
    // /REQ). We encrypt the whole session, not the login-only mode.
    let client_enc = login::prelogin_client_encryption(&prelogin.payload);
    let offer_tls =
        config.tls.is_some() && matches!(client_enc, login::ENCRYPT_ON | login::ENCRYPT_REQ);
    // The PRELOGIN *response* is sent as a REPLY (Tabular Result) packet;
    // clients (pytds/go-mssqldb) expect type 0x04 here, not 0x12.
    write_message(
        &mut stream,
        PKT_TABULAR_RESULT,
        &login::prelogin_response(offer_tls),
        packet_size,
    )
    .await?;

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
                let sql = batch_sql(&message.payload)?;
                let cancel = Arc::new(AtomicBool::new(false));
                let work = run_batch(engine, session, &sql, cancel.clone());
                match run_watching_attention(stream, cancel, work).await? {
                    Some(response) => {
                        write_message(stream, PKT_TABULAR_RESULT, &response, packet_size).await?
                    }
                    None => return Ok(()),
                }
            }
            PKT_RPC => {
                let cancel = Arc::new(AtomicBool::new(false));
                let work = run_rpc(engine, session, &message.payload, cancel.clone());
                match run_watching_attention(stream, cancel, work).await? {
                    Some(response) => {
                        write_message(stream, PKT_TABULAR_RESULT, &response, packet_size).await?
                    }
                    None => return Ok(()),
                }
            }
            PKT_TRANSACTION_MANAGER => {
                let response = handle_tm_request(
                    engine,
                    session,
                    &message.payload,
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
    payload: &[u8],
    tran_descriptor: &mut u64,
    next_descriptor: &mut u64,
) -> io::Result<Vec<u8>> {
    let (request_type, isolation) = parse_tm_request(payload)?;
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
        TM_COMMIT_XACT => {
            token::envchange_commit_tran(&mut out, *tran_descriptor);
            *tran_descriptor = 0;
        }
        TM_ROLLBACK_XACT => {
            token::envchange_rollback_tran(&mut out, *tran_descriptor);
            *tran_descriptor = 0;
        }
        _ => unreachable!("request type validated above"),
    }
    token::done(&mut out, false, false, reply.in_transaction, None);
    Ok(out)
}

/// Parses a Transaction Manager request payload: an ALL_HEADERS block, then a
/// `RequestType` (u16 LE), then for `TM_BEGIN_XACT` an isolation-level byte.
/// Returns the request type and (for BEGIN) the isolation byte.
fn parse_tm_request(payload: &[u8]) -> io::Result<(u16, u8)> {
    let body = skip_all_headers(payload);
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
/// executor aborts the statement, and the client is answered with a
/// `DONE(attention)` instead of the (partial) result. Returns `Ok(None)` if the
/// client disconnected mid-batch.
///
/// `AsyncReadExt::read` is cancellation-safe, so bytes read into `hdr` survive a
/// `select!` iteration that resolves to the batch instead; any partial header
/// left when the batch wins is completed afterwards so packet framing is intact.
async fn run_watching_attention<S, F>(
    stream: &mut S,
    cancel: Arc<AtomicBool>,
    work: F,
) -> io::Result<Option<Vec<u8>>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: std::future::Future<Output = Vec<u8>>,
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
                    return Ok(None);
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
        let mut out = Vec::new();
        token::done_attention(&mut out);
        Ok(Some(out))
    } else {
        Ok(Some(result))
    }
}

/// Runs a SQL batch through the engine actor and builds its token stream. The
/// batch is cancellable: setting `cancel` (on a TDS Attention) aborts it.
async fn run_batch(
    engine: &EngineHandle,
    session: SessionId,
    sql: &str,
    cancel: Arc<AtomicBool>,
) -> Vec<u8> {
    match engine
        .run_batch_cancellable(session, sql.to_string(), cancel)
        .await
    {
        Ok(reply) => build_batch_tokens(&reply.outcome, reply.in_transaction),
        Err(err) => {
            // A genuine engine/storage failure (not a SQL-level error).
            let mut out = Vec::new();
            token::error(&mut out, 50000, 1, 16, &err.to_string());
            token::done(&mut out, false, true, false, None);
            out
        }
    }
}

/// Handles an RPC request. Supports `sp_executesql` — decode its statement and
/// typed parameters, run them, and return the same token stream a batch would.
/// Any other procedure, or a malformed request, is answered with an error token
/// plus a final DONE so the connection stays usable.
async fn run_rpc(
    engine: &EngineHandle,
    session: SessionId,
    payload: &[u8],
    cancel: Arc<AtomicBool>,
) -> Vec<u8> {
    let request = match rpc::parse_rpc_request(skip_all_headers(payload)) {
        Ok(request) => request,
        Err(err) => return rpc_error(&format!("malformed RPC request: {err}")),
    };
    match request.proc {
        RpcProc::SpExecuteSql => {
            let (sql, params) = match rpc::split_sp_executesql(request.params) {
                Ok(split) => split,
                Err(err) => return rpc_error(&err.to_string()),
            };
            match engine
                .run_rpc_cancellable(session, sql, params, cancel)
                .await
            {
                Ok(reply) => build_batch_tokens(&reply.outcome, reply.in_transaction),
                Err(err) => rpc_error(&err.to_string()),
            }
        }
        // Error 2812 is SQL Server's "Could not find stored procedure".
        RpcProc::Other(name) => {
            rpc_error_num(2812, &format!("Could not find stored procedure '{name}'."))
        }
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

/// Builds the COLMETADATA/ROW/DONE/ERROR token stream for a batch outcome.
/// `in_transaction` sets `DONE_INXACT` so the client knows the connection is
/// still inside a transaction.
fn build_batch_tokens(outcome: &BatchOutcome, in_transaction: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let has_error = outcome.error.is_some();
    let last_index = outcome.results.len().saturating_sub(1);
    for (index, result) in outcome.results.iter().enumerate() {
        // A DONE is final only when it is the last token group of the whole
        // response (no more results and no trailing error).
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
        // Empty batch (e.g. only comments): a single final DONE.
        token::done(&mut out, false, false, in_transaction, None);
    }
    out
}

/// Skips a request payload's ALL_HEADERS block, returning the bytes after it.
/// ALL_HEADERS starts with a `TotalLength u32` covering the whole block (incl.
/// itself); a missing or malformed block means the payload has no headers.
fn skip_all_headers(payload: &[u8]) -> &[u8] {
    let start = if payload.len() >= 4 {
        let total = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        if (4..=payload.len()).contains(&total) {
            total
        } else {
            0
        }
    } else {
        0
    };
    &payload[start..]
}

/// Extracts the SQL text from a SQLBatch payload: an ALL_HEADERS block
/// (`TotalLength u32` covering the headers) followed by the UCS-2LE query.
fn batch_sql(payload: &[u8]) -> io::Result<String> {
    let text = skip_all_headers(payload);
    if !text.len().is_multiple_of(2) {
        return Err(protocol_err("SQLBatch text is not UCS-2 aligned"));
    }
    let units: Vec<u16> = text
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16(&units).map_err(|_| protocol_err("SQLBatch text is not valid UTF-16"))
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
    use tokio::io::AsyncWriteExt;

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
        let attn = [PKT_ATTENTION, 0x01, 0x00, 0x08, 0, 0, 0, 0];
        client.write_all(&attn).await.expect("send attention");
        let out = run_watching_attention(&mut server, cancel.clone(), work)
            .await
            .expect("io ok")
            .expect("not disconnected");
        assert!(
            cancel.load(Ordering::Relaxed),
            "the Attention set the cancel flag"
        );
        let mut expected = Vec::new();
        token::done_attention(&mut expected);
        assert_eq!(out, expected, "response is DONE(attention), not the result");
    }

    #[tokio::test]
    async fn no_attention_returns_the_batch_result() {
        let (_client, mut server) = tokio::io::duplex(64);
        let cancel = Arc::new(AtomicBool::new(false));
        // The batch completes on its own; no Attention is ever sent.
        let work = async move { b"RESULT".to_vec() };
        let out = run_watching_attention(&mut server, cancel.clone(), work)
            .await
            .expect("io ok")
            .expect("not disconnected");
        assert!(!cancel.load(Ordering::Relaxed));
        assert_eq!(out, b"RESULT".to_vec(), "no attention -> the real result");
    }
}
