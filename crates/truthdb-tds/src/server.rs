//! TDS connection handler: PRELOGIN -> LOGIN7 (auth) -> SQLBatch loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{self, AsyncRead, AsyncWrite};
use truthdb_core::engine::Engine;
use truthdb_core::rel::{BatchOutcome, StatementResult};

use crate::login::{self, parse_login7};
use crate::packet::{
    self, DEFAULT_PACKET_SIZE, Message, PKT_ATTENTION, PKT_LOGIN7, PKT_PRELOGIN, PKT_SQL_BATCH,
    PKT_TABULAR_RESULT, read_message, write_message,
};
use crate::token;

/// Server-side TDS configuration: the login users and the reported database.
#[derive(Debug, Clone)]
pub struct TdsConfig {
    /// username -> password (plaintext config auth for Stage 4).
    pub users: HashMap<String, String>,
    /// The single database name reported to clients.
    pub database: String,
}

/// Handles one TDS connection to completion (or disconnect).
pub async fn serve_connection<S>(
    mut stream: S,
    engine: Arc<Mutex<Engine>>,
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
    // The PRELOGIN *response* is sent as a REPLY (Tabular Result) packet;
    // clients (pytds/go-mssqldb) expect type 0x04 here, not 0x12.
    write_message(
        &mut stream,
        PKT_TABULAR_RESULT,
        &login::prelogin_response(),
        packet_size,
    )
    .await?;

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
        token::done(&mut out, false, true, None);
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
    token::done(&mut out, false, false, None);
    write_message(&mut stream, PKT_TABULAR_RESULT, &out, packet_size).await?;

    // --- request loop ---
    loop {
        let message = match read_message(&mut stream).await {
            Ok(message) => message,
            // Clean disconnect.
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err),
        };
        match message.kind {
            PKT_SQL_BATCH => {
                let sql = batch_sql(&message.payload)?;
                let response = run_batch(&engine, &sql);
                write_message(&mut stream, PKT_TABULAR_RESULT, &response, packet_size).await?;
            }
            PKT_ATTENTION => {
                // Acknowledge cancel with a DONE(attention). True mid-batch
                // interruption arrives in a later stage; batches here run to
                // completion.
                let mut out = Vec::new();
                token::done_attention(&mut out);
                write_message(&mut stream, PKT_TABULAR_RESULT, &out, packet_size).await?;
            }
            _ => return Err(protocol_err("unexpected TDS message type")),
        }
    }
}

fn authenticate(config: &TdsConfig, username: &str, password: &str) -> bool {
    config
        .users
        .get(username)
        .is_some_and(|expected| expected == password)
}

/// Runs a SQL batch through the engine and builds its token stream.
fn run_batch(engine: &Arc<Mutex<Engine>>, sql: &str) -> Vec<u8> {
    let outcome = {
        let mut engine = engine.lock().expect("engine mutex poisoned");
        engine.sql_batch(sql)
    };
    match outcome {
        Ok(outcome) => build_batch_tokens(&outcome),
        Err(err) => {
            // A genuine engine/storage failure (not a SQL-level error).
            let mut out = Vec::new();
            token::error(&mut out, 50000, 1, 16, &err.to_string());
            token::done(&mut out, false, true, None);
            out
        }
    }
}

/// Builds the COLMETADATA/ROW/DONE/ERROR token stream for a batch outcome.
fn build_batch_tokens(outcome: &BatchOutcome) -> Vec<u8> {
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
                token::done(&mut out, more, false, Some(rowset.rows.len() as u64));
            }
            StatementResult::RowsAffected(n) => {
                token::done(&mut out, more, false, Some(*n));
            }
            StatementResult::Done => {
                token::done(&mut out, more, false, None);
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
        token::done(&mut out, false, true, None);
    } else if outcome.results.is_empty() {
        // Empty batch (e.g. only comments): a single final DONE.
        token::done(&mut out, false, false, None);
    }
    out
}

/// Extracts the SQL text from a SQLBatch payload: an ALL_HEADERS block
/// (`TotalLength u32` covering the headers) followed by the UCS-2LE query.
fn batch_sql(payload: &[u8]) -> io::Result<String> {
    let sql_start = if payload.len() >= 4 {
        let total = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        if (4..=payload.len()).contains(&total) {
            total
        } else {
            0 // No (or malformed) ALL_HEADERS: treat the whole payload as SQL.
        }
    } else {
        0
    };
    let text = &payload[sql_start..];
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
