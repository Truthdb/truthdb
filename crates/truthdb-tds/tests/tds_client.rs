//! End-to-end TDS test: a minimal in-process TDS client drives the full
//! handshake and query flow against `serve_connection` over an in-memory
//! duplex stream, then decodes the token stream. This exercises every byte
//! path a real driver would (PRELOGIN, LOGIN7, SQLBatch, COLMETADATA, ROW,
//! DONE, ERROR) without needing an external SQL Server driver.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use truthdb_core::engine::Engine;
use truthdb_core::session::{EngineHandle, spawn_engine};
use truthdb_core::storage::{Storage, StorageOptions};
use truthdb_tds::server::{TdsConfig, serve_connection};

// Packet types.
const PKT_SQL_BATCH: u8 = 0x01;
const PKT_TRANSACTION_MANAGER: u8 = 0x0e;
const PKT_LOGIN7: u8 = 0x10;
const PKT_PRELOGIN: u8 = 0x12;

fn temp_path(label: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("truthdb-tds-{label}-{nanos}.db"));
    path
}

fn engine(path: &std::path::Path) -> EngineHandle {
    let opts = StorageOptions {
        size_gib: 1,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
        default_collation: None,
    };
    let storage = Storage::create(path.to_path_buf(), opts).expect("storage");
    // The JoinHandle is dropped; the engine thread exits when the last
    // EngineHandle drops at end of test.
    spawn_engine(Engine::new(storage).expect("engine")).0
}

fn config() -> TdsConfig {
    let mut users = HashMap::new();
    users.insert("sa".to_string(), "secret".to_string());
    TdsConfig {
        users,
        database: "truthdb".to_string(),
        tls: None,
        encryption: truthdb_tds::Encryption::default(),
    }
}

fn ucs2le(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
}

/// A minimal client that speaks just enough TDS to test the server.
struct Client {
    stream: DuplexStream,
    /// The connection's current transaction descriptor, learned from ENVCHANGE
    /// 8 (begin) and cleared by 9/10 (commit/rollback). Real drivers echo this
    /// in every request's ALL_HEADERS, and the server validates it.
    tran_descriptor: u64,
}

impl Client {
    async fn write_packet(&mut self, kind: u8, payload: &[u8]) {
        let length = (8 + payload.len()) as u16;
        let header = [
            kind,
            0x01, // EOM
            (length >> 8) as u8,
            (length & 0xff) as u8,
            0,
            0,
            1,
            0,
        ];
        self.stream.write_all(&header).await.unwrap();
        self.stream.write_all(payload).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Reads a message, or None if the server closed the connection (which is
    /// how a protocol error surfaces to the client).
    async fn try_read_message(&mut self) -> Option<(u8, Vec<u8>)> {
        let mut header = [0u8; 8];
        self.stream.read_exact(&mut header).await.ok()?;
        let kind = header[0];
        let mut payload = self.read_body(&header).await;
        let mut status = header[1];
        while status & 0x01 == 0 {
            self.stream.read_exact(&mut header).await.ok()?;
            status = header[1];
            payload.extend(self.read_body(&header).await);
        }
        Some((kind, payload))
    }

    /// Sends a SQLBatch with a caller-supplied ALL_HEADERS block (to exercise
    /// malformed / mismatched headers the normal `batch` path cannot produce).
    async fn raw_batch(&mut self, headers_block: &[u8], sql: &str) {
        let mut payload = Vec::new();
        payload.extend_from_slice(headers_block);
        payload.extend(ucs2le(sql));
        self.write_packet(PKT_SQL_BATCH, &payload).await;
    }

    /// Reads a full message (packets until EOM) -> (kind, payload).
    async fn read_message(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 8];
        self.stream.read_exact(&mut header).await.unwrap();
        let kind = header[0];
        let mut payload = self.read_body(&header).await;
        let mut status = header[1];
        while status & 0x01 == 0 {
            self.stream.read_exact(&mut header).await.unwrap();
            status = header[1];
            payload.extend(self.read_body(&header).await);
        }
        (kind, payload)
    }

    async fn read_body(&mut self, header: &[u8; 8]) -> Vec<u8> {
        let length = u16::from_be_bytes([header[2], header[3]]) as usize;
        let mut body = vec![0u8; length - 8];
        self.stream.read_exact(&mut body).await.unwrap();
        body
    }

    async fn prelogin(&mut self) {
        // Minimal PRELOGIN: just a terminator (server ignores the contents).
        self.write_packet(PKT_PRELOGIN, &[0xff]).await;
        let (kind, _) = self.read_message().await;
        assert!(kind == 0x04 || kind == PKT_PRELOGIN);
    }

    /// A PRELOGIN carrying an ENCRYPTION option, returning the byte the server
    /// advertises back (or None if it hung up).
    async fn prelogin_with_encryption(&mut self, client: u8) -> Option<u8> {
        // One ENCRYPTION option: token | offset u16 BE | length u16 BE, then
        // the terminator, then the data.
        let mut payload = vec![0x01u8];
        payload.extend_from_slice(&6u16.to_be_bytes());
        payload.extend_from_slice(&1u16.to_be_bytes());
        payload.push(0xff);
        payload.push(client);
        self.write_packet(PKT_PRELOGIN, &payload).await;
        let (_, response) = self.try_read_message().await?;
        Some(read_encryption_option(&response))
    }

    async fn login(&mut self, user: &str, password: &str) -> Vec<Token> {
        self.write_packet(PKT_LOGIN7, &build_login7(user, password, "truthdb"))
            .await;
        let (_, payload) = self.read_message().await;
        parse_tokens(&payload)
    }

    /// Sends a PKT_RPC message with the given (post-headers) body and
    /// returns the response tokens.
    async fn rpc(&mut self, body: &[u8]) -> Vec<Token> {
        let mut payload = Vec::new();
        let headers = all_headers(self.tran_descriptor);
        let total = 4 + headers.len();
        payload.extend_from_slice(&(total as u32).to_le_bytes());
        payload.extend_from_slice(&headers);
        payload.extend_from_slice(body);
        self.write_packet(0x03, &payload).await;
        let (_, response) = self.read_message().await;
        parse_tokens(&response)
    }

    async fn batch(&mut self, sql: &str) -> Vec<Token> {
        let mut payload = Vec::new();
        // ALL_HEADERS: TotalLength includes itself (the 4-byte field) plus
        // the header block; the SQL text starts right after.
        let headers = all_headers(self.tran_descriptor);
        let total = 4 + headers.len();
        payload.extend_from_slice(&(total as u32).to_le_bytes());
        payload.extend_from_slice(&headers);
        payload.extend(ucs2le(sql));
        self.write_packet(PKT_SQL_BATCH, &payload).await;
        let (_, response) = self.read_message().await;
        let tokens = parse_tokens(&response);
        self.track_descriptor(&tokens);
        tokens
    }

    /// Applies any transaction ENVCHANGE in a response to the tracked
    /// descriptor, exactly as a real driver would.
    fn track_descriptor(&mut self, tokens: &[Token]) {
        for token in tokens {
            if let Token::EnvChange { kind, descriptor } = token {
                match kind {
                    8 => self.tran_descriptor = *descriptor,
                    9 | 10 => self.tran_descriptor = 0,
                    _ => {}
                }
            }
        }
    }

    /// Sends a Transaction Manager request (request type + optional isolation
    /// byte for BEGIN) and returns the decoded response tokens.
    async fn tm_request(&mut self, request_type: u16, isolation: u8) -> Vec<Token> {
        let mut payload = Vec::new();
        // Mirror go-mssqldb: a BEGIN carries a placeholder 0 descriptor (it
        // names no transaction yet), while COMMIT/ROLLBACK carry the live one.
        let descriptor = if request_type == TM_BEGIN_XACT {
            0
        } else {
            self.tran_descriptor
        };
        let headers = all_headers(descriptor);
        let total = 4 + headers.len();
        payload.extend_from_slice(&(total as u32).to_le_bytes());
        payload.extend_from_slice(&headers);
        payload.extend_from_slice(&request_type.to_le_bytes());
        if request_type == 5 {
            payload.push(isolation); // IsolationLevel
            payload.push(0); // name length (B_VARCHAR, empty)
        }
        self.write_packet(PKT_TRANSACTION_MANAGER, &payload).await;
        let (_, response) = self.read_message().await;
        let tokens = parse_tokens(&response);
        self.track_descriptor(&tokens);
        tokens
    }
}

/// The transaction descriptor carried by an ENVCHANGE body, or 0 if there is
/// none. Body = `type u8 | NewValue B_VARBYTE | OldValue B_VARBYTE`; type 8
/// (begin) puts the new descriptor in NewValue, types 9/10 (commit/rollback)
/// leave NewValue empty and put the ending descriptor in OldValue.
fn envchange_descriptor(body: &[u8]) -> u64 {
    let read_varbyte = |at: usize| -> Option<(u64, usize)> {
        let len = *body.get(at)? as usize;
        if len == 8 && body.len() >= at + 1 + 8 {
            let bytes: [u8; 8] = body[at + 1..at + 9].try_into().ok()?;
            Some((u64::from_le_bytes(bytes), at + 1 + len))
        } else {
            Some((0, at + 1 + len))
        }
    };
    // NewValue first; if it was empty, fall through to OldValue.
    match read_varbyte(1) {
        Some((value, _)) if value != 0 => value,
        Some((_, next)) => read_varbyte(next).map(|(v, _)| v).unwrap_or(0),
        None => 0,
    }
}

/// A minimal ALL_HEADERS with a transaction-descriptor header (type 2),
/// carrying the connection's current descriptor (0 = no transaction).
fn all_headers(descriptor: u64) -> Vec<u8> {
    // Header: length u32 | type u16 | transaction descriptor u64 | request count u32
    let mut header = Vec::new();
    let body_len = 4 + 2 + 8 + 4;
    header.extend_from_slice(&(body_len as u32).to_le_bytes());
    header.extend_from_slice(&2u16.to_le_bytes()); // transaction descriptor
    header.extend_from_slice(&descriptor.to_le_bytes());
    header.extend_from_slice(&1u32.to_le_bytes());
    header
}

fn build_login7(user: &str, password: &str, database: &str) -> Vec<u8> {
    let mut payload = vec![0u8; 94];
    payload[8..12].copy_from_slice(&4096u32.to_le_bytes());
    let mut data = Vec::new();
    let obfuscate = |s: &str| -> Vec<u8> {
        ucs2le(s)
            .into_iter()
            .map(|b| b.rotate_left(4) ^ 0xa5)
            .collect()
    };
    let add = |payload: &mut Vec<u8>, data: &mut Vec<u8>, at: usize, bytes: &[u8]| {
        let offset = 94 + data.len();
        payload[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        payload[at + 2..at + 4].copy_from_slice(&((bytes.len() / 2) as u16).to_le_bytes());
        data.extend_from_slice(bytes);
    };
    add(&mut payload, &mut data, 40, &ucs2le(user));
    add(&mut payload, &mut data, 44, &obfuscate(password));
    add(&mut payload, &mut data, 68, &ucs2le(database));
    payload.extend(data);
    payload
}

/// A decoded token relevant to the tests.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    LoginAck,
    ColMetadata(Vec<ColType>),
    Row(Vec<Cell>),
    Error {
        number: i32,
    },
    Info {
        number: i32,
    },
    EnvChange {
        kind: u8,
        descriptor: u64,
    },
    Done {
        count: Option<u64>,
        in_xact: bool,
        cmd: u16,
    },
    DoneInProc {
        count: Option<u64>,
        cmd: u16,
    },
    DoneProc {
        more: bool,
        error: bool,
        cmd: u16,
    },
    ReturnStatus(i32),
    ReturnValue,
    Other(u8),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ColType {
    Int,
    Bit,
    Float,
    NVarChar,
    VarChar,
}

#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    Int(i64),
    Bool(bool),
    Float(f64),
    Str(String),
}

/// Parses a server token stream into decodable tokens (covers only what the
/// tests need: the Stage 4 type set).
fn parse_tokens(payload: &[u8]) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut i = 0;
    let mut last_meta: Vec<ColType> = Vec::new();
    while i < payload.len() {
        let token = payload[i];
        i += 1;
        match token {
            0xad => {
                // LOGINACK: length-prefixed, skip body.
                let len = u16::from_le_bytes([payload[i], payload[i + 1]]) as usize;
                i += 2 + len;
                tokens.push(Token::LoginAck);
            }
            0xe3 | 0xab | 0xaa => {
                // ENVCHANGE / INFO / ERROR: length-prefixed.
                let len = u16::from_le_bytes([payload[i], payload[i + 1]]) as usize;
                let body = &payload[i + 2..i + 2 + len];
                if token == 0xaa {
                    let number = i32::from_le_bytes(body[0..4].try_into().unwrap());
                    tokens.push(Token::Error { number });
                } else if token == 0xab {
                    let number = i32::from_le_bytes(body[0..4].try_into().unwrap());
                    tokens.push(Token::Info { number });
                } else if token == 0xe3 {
                    // Transaction ENVCHANGEs carry the descriptor as a
                    // B_VARBYTE: type 8 (begin) in NewValue, types 9/10
                    // (commit/rollback) in OldValue (NewValue empty).
                    tokens.push(Token::EnvChange {
                        kind: body[0],
                        descriptor: envchange_descriptor(body),
                    });
                }
                i += 2 + len;
            }
            0x81 => {
                let (meta, consumed) = parse_colmetadata(&payload[i..]);
                i += consumed;
                last_meta = meta.clone();
                tokens.push(Token::ColMetadata(meta));
            }
            0xd1 => {
                let (cells, consumed) = parse_row(&payload[i..], &last_meta);
                i += consumed;
                tokens.push(Token::Row(cells));
            }
            0xfd..=0xff => {
                // DONE / DONEPROC / DONEINPROC: status u16, curcmd u16, count u64.
                let status = u16::from_le_bytes([payload[i], payload[i + 1]]);
                let cmd = u16::from_le_bytes([payload[i + 2], payload[i + 3]]);
                let count = u64::from_le_bytes(payload[i + 4..i + 12].try_into().unwrap());
                let has_count = status & 0x0010 != 0;
                let in_xact = status & 0x0004 != 0;
                i += 12;
                tokens.push(match token {
                    0xfd => Token::Done {
                        count: has_count.then_some(count),
                        in_xact,
                        cmd,
                    },
                    0xfe => Token::DoneProc {
                        more: status & 0x0001 != 0,
                        error: status & 0x0002 != 0,
                        cmd,
                    },
                    _ => Token::DoneInProc {
                        count: has_count.then_some(count),
                        cmd,
                    },
                });
            }
            0x79 => {
                let value = i32::from_le_bytes(payload[i..i + 4].try_into().unwrap());
                i += 4;
                tokens.push(Token::ReturnStatus(value));
            }
            0xac => {
                // RETURNVALUE: ordinal u16, B_VARCHAR name, status u8,
                // usertype u32, flags u16, TYPE_INFO(INTN,4), value.
                i += 2;
                let name_chars = payload[i] as usize;
                i += 1 + name_chars * 2;
                i += 1 + 4 + 2; // status, usertype, flags
                i += 2; // INTN + max len
                let len = payload[i] as usize;
                i += 1 + len;
                tokens.push(Token::ReturnValue);
            }
            other => {
                tokens.push(Token::Other(other));
                break; // unknown token: stop to avoid misparsing
            }
        }
    }
    tokens
}

fn parse_colmetadata(bytes: &[u8]) -> (Vec<ColType>, usize) {
    let count = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let mut i = 2;
    let mut cols = Vec::with_capacity(count);
    for _ in 0..count {
        i += 4; // usertype
        i += 2; // flags
        let type_token = bytes[i];
        i += 1;
        let col_type = match type_token {
            0x26 => {
                i += 1; // max len byte
                ColType::Int
            }
            0x68 => {
                i += 1;
                ColType::Bit
            }
            0x6d => {
                i += 1;
                ColType::Float
            }
            0xe7 => {
                i += 2 + 5; // max len u16 + collation
                ColType::NVarChar
            }
            0xa7 => {
                i += 2 + 5;
                ColType::VarChar
            }
            other => panic!("unhandled type token {other:#x}"),
        };
        // ColName: b_varchar (char count then UCS-2).
        let name_len = bytes[i] as usize;
        i += 1 + name_len * 2;
        cols.push(col_type);
    }
    (cols, i)
}

fn parse_row(bytes: &[u8], meta: &[ColType]) -> (Vec<Cell>, usize) {
    let mut i = 0;
    let mut cells = Vec::with_capacity(meta.len());
    for col in meta {
        match col {
            ColType::Int => {
                let len = bytes[i] as usize;
                i += 1;
                if len == 0 {
                    cells.push(Cell::Null);
                } else {
                    let mut v = [0u8; 8];
                    v[..len].copy_from_slice(&bytes[i..i + len]);
                    // Sign-extend from the actual width.
                    let mut n = i64::from_le_bytes(v);
                    let bits = len * 8;
                    if bits < 64 && (n >> (bits - 1)) & 1 == 1 {
                        n |= -1i64 << bits;
                    }
                    cells.push(Cell::Int(n));
                    i += len;
                }
            }
            ColType::Bit => {
                let len = bytes[i] as usize;
                i += 1;
                if len == 0 {
                    cells.push(Cell::Null);
                } else {
                    cells.push(Cell::Bool(bytes[i] != 0));
                    i += len;
                }
            }
            ColType::Float => {
                let len = bytes[i] as usize;
                i += 1;
                match len {
                    0 => cells.push(Cell::Null),
                    4 => {
                        let v = f32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
                        cells.push(Cell::Float(v as f64));
                        i += 4;
                    }
                    8 => {
                        let v = f64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
                        cells.push(Cell::Float(v));
                        i += 8;
                    }
                    other => panic!("bad float len {other}"),
                }
            }
            ColType::NVarChar => {
                let len = u16::from_le_bytes([bytes[i], bytes[i + 1]]);
                i += 2;
                if len == 0xffff {
                    cells.push(Cell::Null);
                } else {
                    let len = len as usize;
                    let units: Vec<u16> = bytes[i..i + len]
                        .chunks_exact(2)
                        .map(|c| u16::from_le_bytes([c[0], c[1]]))
                        .collect();
                    cells.push(Cell::Str(String::from_utf16(&units).unwrap()));
                    i += len;
                }
            }
            ColType::VarChar => {
                let len = u16::from_le_bytes([bytes[i], bytes[i + 1]]);
                i += 2;
                if len == 0xffff {
                    cells.push(Cell::Null);
                } else {
                    let len = len as usize;
                    cells.push(Cell::Str(
                        String::from_utf8_lossy(&bytes[i..i + len]).into_owned(),
                    ));
                    i += len;
                }
            }
        }
    }
    (cells, i)
}

async fn connect_with(engine: EngineHandle, cfg: TdsConfig) -> Client {
    let (client_half, server_half) = tokio::io::duplex(64 * 1024);
    let cfg = Arc::new(cfg);
    tokio::spawn(async move {
        let _ = serve_connection(server_half, engine, cfg).await;
    });
    Client {
        stream: client_half,
        tran_descriptor: 0,
    }
}

async fn connect(engine: EngineHandle) -> Client {
    let (client_half, server_half) = tokio::io::duplex(64 * 1024);
    let cfg = Arc::new(config());
    tokio::spawn(async move {
        let _ = serve_connection(server_half, engine, cfg).await;
    });
    Client {
        stream: client_half,
        tran_descriptor: 0,
    }
}

/// Reads the ENCRYPTION option out of a PRELOGIN response.
fn read_encryption_option(payload: &[u8]) -> u8 {
    let mut i = 0;
    while i + 4 < payload.len() {
        let token = payload[i];
        if token == 0xff {
            break;
        }
        let offset = u16::from_be_bytes([payload[i + 1], payload[i + 2]]) as usize;
        if token == 0x01 {
            return payload[offset];
        }
        i += 5;
    }
    panic!("no ENCRYPTION option in PRELOGIN response: {payload:?}");
}

#[tokio::test]
async fn encryption_off_never_offers_tls_even_to_a_client_that_asks() {
    let path = temp_path("enc-off");
    let engine = engine(&path);
    let mut cfg = config();
    cfg.encryption = truthdb_tds::Encryption::Off;
    let mut client = connect_with(engine, cfg).await;
    let advertised = client
        .prelogin_with_encryption(0x01) // ENCRYPT_ON: the client wants TLS
        .await
        .expect("server answered");
    assert_eq!(advertised, 0x02, "must advertise NOT_SUP");
    // ...and the session continues in plaintext.
    client.login("sa", "secret").await;
    let rows = client.batch("SELECT 1 AS n").await;
    assert!(rows.iter().any(|t| matches!(t, Token::Row(_))), "{rows:?}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn encryption_optional_serves_a_plaintext_client() {
    // The default: a client that does not ask to encrypt is served as before.
    let path = temp_path("enc-optional");
    let engine = engine(&path);
    let mut client = connect_with(engine, config()).await;
    let advertised = client
        .prelogin_with_encryption(0x02) // NOT_SUP: the client will not encrypt
        .await
        .expect("server answered");
    assert_eq!(advertised, 0x02);
    client.login("sa", "secret").await;
    let rows = client.batch("SELECT 1 AS n").await;
    assert!(rows.iter().any(|t| matches!(t, Token::Row(_))), "{rows:?}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn encryption_required_refuses_a_client_that_cannot_encrypt() {
    // The server must say encryption is mandatory and then refuse — falling
    // back to plaintext would silently defeat the setting.
    let path = temp_path("enc-required");
    let engine = engine(&path);
    let mut cfg = config();
    cfg.encryption = truthdb_tds::Encryption::Required;
    let mut client = connect_with(engine, cfg).await;
    let advertised = client
        .prelogin_with_encryption(0x02) // NOT_SUP
        .await
        .expect("server answers the PRELOGIN first");
    assert_eq!(
        advertised, 0x03,
        "must advertise REQ so the client learns why"
    );
    // The connection is then dropped rather than served in plaintext.
    assert!(
        client.try_read_message().await.is_none(),
        "a client that cannot encrypt must not be served"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn full_handshake_query_and_error() {
    let path = temp_path("e2e");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;

    client.prelogin().await;
    let login = client.login("sa", "secret").await;
    assert!(login.contains(&Token::LoginAck), "login tokens: {login:?}");
    assert!(!login.iter().any(|t| matches!(t, Token::Error { .. })));

    // DDL + insert.
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50), active BIT)")
        .await;
    let insert = client
        .batch("INSERT INTO t VALUES (1, 'Skor', 1), (2, 'Kangor', 0), (3, NULL, NULL)")
        .await;
    assert!(
        insert
            .iter()
            .any(|t| matches!(t, Token::Done { count: Some(3), .. })),
        "insert tokens: {insert:?}"
    );

    // SELECT: typed COLMETADATA + ROWs.
    let select = client
        .batch("SELECT id, name, active FROM t ORDER BY id")
        .await;
    let rows: Vec<&Vec<Cell>> = select
        .iter()
        .filter_map(|t| match t {
            Token::Row(cells) => Some(cells),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 3, "tokens: {select:?}");
    assert_eq!(
        *rows[0],
        vec![Cell::Int(1), Cell::Str("Skor".into()), Cell::Bool(true)]
    );
    assert_eq!(
        *rows[1],
        vec![Cell::Int(2), Cell::Str("Kangor".into()), Cell::Bool(false)]
    );
    assert_eq!(*rows[2], vec![Cell::Int(3), Cell::Null, Cell::Null]);

    // Error path: duplicate PK -> 2627 in the token stream.
    let dup = client.batch("INSERT INTO t VALUES (1, 'x', 1)").await;
    assert!(
        dup.iter()
            .any(|t| matches!(t, Token::Error { number: 2627 })),
        "dup tokens: {dup:?}"
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn login_failure_reports_18456() {
    let path = temp_path("login-fail");
    let engine = engine(&path);
    let mut client = connect(engine).await;

    client.prelogin().await;
    let tokens = client.login("sa", "wrong-password").await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 18456 })),
        "tokens: {tokens:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn computed_columns_and_constant_select() {
    let path = temp_path("computed");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;
    client.batch("INSERT INTO t VALUES (10), (20)").await;
    let select = client.batch("SELECT id, id * 2 FROM t ORDER BY id").await;
    let rows: Vec<&Vec<Cell>> = select
        .iter()
        .filter_map(|t| match t {
            Token::Row(cells) => Some(cells),
            _ => None,
        })
        .collect();
    assert_eq!(*rows[0], vec![Cell::Int(10), Cell::Int(20)]);
    assert_eq!(*rows[1], vec![Cell::Int(20), Cell::Int(40)]);
    let _ = std::fs::remove_file(path);
}

// Transaction Manager request types (MS-TDS 2.2.6.9).
const TM_BEGIN_XACT: u16 = 5;
const TM_COMMIT_XACT: u16 = 7;
const TM_ROLLBACK_XACT: u16 = 8;

/// One sp_executesql RPC (by ProcID) with a single unnamed @stmt param.
fn sp_executesql_rpc(sql: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&0xffffu16.to_le_bytes()); // ProcID sentinel
    b.extend_from_slice(&10u16.to_le_bytes()); // sp_executesql
    b.extend_from_slice(&0u16.to_le_bytes()); // option flags
    b.push(0); // empty param name
    b.push(0); // status
    b.push(0xe7); // NVARCHAR
    b.extend_from_slice(&8000u16.to_le_bytes());
    b.extend_from_slice(&[0x09, 0x04, 0xd0, 0x00, 0x34]); // collation
    let bytes = ucs2le(sql);
    b.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    b.extend_from_slice(&bytes);
    b
}

/// A multi-RPC request (mssql-jdbc's default flow batches sp_unprepare with
/// the next sp_prepexec this way): each RPC answers its own DONEPROC-framed
/// reply — DONE_MORE on every DONEPROC but the last — inside one response.
#[tokio::test]
async fn a_multi_rpc_request_answers_each_rpc_in_one_response() {
    let path = temp_path("multirpc");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let mut body = sp_executesql_rpc("SELECT 1 AS a");
    body.push(0xff); // batch separator
    body.extend(sp_executesql_rpc("SELECT 2 AS b"));
    let tokens = client.rpc(&body).await;

    let ints: Vec<i64> = row_ints(&tokens);
    assert_eq!(ints, [1, 2], "tokens: {tokens:?}");
    let procs: Vec<(bool, bool)> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { more, error, .. } => Some((*more, *error)),
            _ => None,
        })
        .collect();
    assert_eq!(
        procs,
        [(true, false), (false, false)],
        "every DONEPROC but the last carries DONE_MORE: {tokens:?}"
    );
    assert_eq!(
        tokens
            .iter()
            .filter(|t| matches!(t, Token::ReturnStatus(0)))
            .count(),
        2,
        "one RETURNSTATUS per RPC: {tokens:?}"
    );

    // An erroring RPC does not take the rest of the request with it: the
    // second RPC still runs and the response stays framed.
    let mut body = Vec::new();
    body.extend_from_slice(&0xffffu16.to_le_bytes());
    body.extend_from_slice(&15u16.to_le_bytes()); // sp_unprepare
    body.extend_from_slice(&0u16.to_le_bytes());
    body.push(0); // empty param name
    body.push(0); // status
    b_int(&mut body, 42); // a handle that was never prepared
    body.push(0xff);
    body.extend(sp_executesql_rpc("SELECT 3 AS c"));
    let tokens = client.rpc(&body).await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 8179 })),
        "tokens: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [3], "tokens: {tokens:?}");
    let procs: Vec<(bool, bool)> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { more, error, .. } => Some((*more, *error)),
            _ => None,
        })
        .collect();
    assert_eq!(procs, [(true, true), (false, false)], "tokens: {tokens:?}");

    // A decode-level error mid-request (an unknown procedure never reaches
    // the engine) renders in-frame and the RPCs after it still run.
    let mut body = Vec::new();
    body.extend_from_slice(&5u16.to_le_bytes()); // name length in chars
    body.extend(ucs2le("sp_no"));
    body.extend_from_slice(&0u16.to_le_bytes()); // option flags
    body.push(0xff);
    body.extend(sp_executesql_rpc("SELECT 4 AS d"));
    let tokens = client.rpc(&body).await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 2812 })),
        "tokens: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [4], "tokens: {tokens:?}");

    let _ = std::fs::remove_file(&path);
}

/// The login response advertises the connection's default SQL collation
/// (ENVCHANGE 7) — mssql-jdbc dereferences it to encode every NVARCHAR RPC
/// parameter and NPEs client-side without it — and every DONE stamps its
/// statement's command class in CurCmd, which the same driver requires
/// before it accepts a DONE's row count (executeUpdate returns -1 without
/// it). Both regressions previously survived every in-repo test.
#[tokio::test]
async fn login_advertises_collation_and_dones_carry_their_command_class() {
    let path = temp_path("curcmd");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    let login = client.login("sa", "secret").await;
    assert!(
        has_envchange(&login, 7),
        "login must carry ENVCHANGE 7 (SQL collation): {login:?}"
    );

    client
        .batch("CREATE TABLE cc (id INT NOT NULL PRIMARY KEY)")
        .await;
    // The REAL engine's statement→command mapping, batch path: INSERT 0xC3,
    // UPDATE 0xC5, SELECT 0xC1, DELETE 0xC4 on the statement's own DONE.
    for (sql, want) in [
        ("INSERT INTO cc VALUES (1), (2)", 0xc3u16),
        ("UPDATE cc SET id = 3 WHERE id = 1", 0xc5),
        ("SELECT id FROM cc ORDER BY id", 0xc1),
        ("DELETE FROM cc WHERE id = 3", 0xc4),
    ] {
        let tokens = client.batch(sql).await;
        let cmds: Vec<u16> = tokens
            .iter()
            .filter_map(|t| match t {
                Token::Done { cmd, .. } => Some(*cmd),
                _ => None,
            })
            .collect();
        assert_eq!(cmds, [want], "{sql}: {tokens:?}");
    }

    // The RPC path: the DONEINPROC carries the statement's class, the final
    // DONEPROC carries EXECUTE (0xE0).
    let tokens = client
        .rpc(&sp_executesql_rpc("INSERT INTO cc VALUES (9)"))
        .await;
    let inproc: Vec<u16> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneInProc { cmd, .. } => Some(*cmd),
            _ => None,
        })
        .collect();
    assert_eq!(inproc, [0xc3], "tokens: {tokens:?}");
    let procs: Vec<u16> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { cmd, .. } => Some(*cmd),
            _ => None,
        })
        .collect();
    assert_eq!(procs, [0xe0], "tokens: {tokens:?}");

    let _ = std::fs::remove_file(&path);
}

/// `USE` answers with the database-context ENVCHANGE (type 1) and the 5701
/// INFO — the exact tokens SSMS listens for — before the statement's DONE;
/// a wrong database is 911 and emits neither.
#[tokio::test]
async fn use_statement_emits_the_database_envchange() {
    let path = temp_path("use-envchange");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client.batch("USE truthdb").await;
    assert!(
        has_envchange(&tokens, 1),
        "USE must emit ENVCHANGE 1 (database): {tokens:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Info { number: 5701 })),
        "USE must emit INFO 5701: {tokens:?}"
    );

    let tokens = client.batch("USE somewhere_else").await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 911 })),
        "a wrong database is 911: {tokens:?}"
    );
    assert!(
        !has_envchange(&tokens, 1),
        "a failed USE must not announce a context change: {tokens:?}"
    );

    let _ = std::fs::remove_file(&path);
}

/// RAISERROR severity <= 10 arrives as an INFO token (0xAB), not an ERROR:
/// the batch succeeds and its DONE is clean.
#[tokio::test]
async fn raiserror_low_severity_is_an_info_token() {
    let path = temp_path("raiserror-info");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .batch("RAISERROR('for your information', 5, 1)")
        .await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Info { number: 50000 })),
        "an INFO 50000 token: {tokens:?}"
    );
    assert!(
        !tokens.iter().any(|t| matches!(t, Token::Error { .. })),
        "no ERROR token: {tokens:?}"
    );
    // The connection stays healthy.
    let tokens = client.batch("SELECT 1 AS one").await;
    assert!(tokens.iter().any(|t| matches!(t, Token::Row(_))));

    let _ = std::fs::remove_file(&path);
}

/// Severity >= 20 is fatal: the error and its DONE are delivered, then the
/// server closes the connection — as SQL Server drops the session.
#[tokio::test]
async fn fatal_severity_closes_the_connection() {
    use tokio::io::AsyncReadExt;

    let path = temp_path("raiserror-fatal");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .batch("RAISERROR('going down', 20, 1) WITH LOG")
        .await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000 })),
        "the fatal error is delivered first: {tokens:?}"
    );
    // The server hangs up: the next read is EOF (no reply will ever come).
    // Bounded, so a regression that keeps the connection open FAILS here in
    // seconds instead of hanging the suite on a read that never returns.
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.stream.read(&mut byte),
    )
    .await
    .expect("the server must close the connection after a severity-20 error")
    .expect("read after fatal");
    assert_eq!(
        read, 0,
        "the connection is closed after a severity-20 error"
    );

    let _ = std::fs::remove_file(&path);
}

/// REVIEW probe (passes): severity >= 20 inside an RPC (sp_executesql)
/// delivers the error inside the DONEPROC-framed reply and then closes the
/// connection — the RPC reply path honors the same fatal flag as a batch.
#[tokio::test]
async fn review_poc_fatal_inside_an_rpc_closes_the_connection() {
    use tokio::io::AsyncReadExt;

    let path = temp_path("review-rpc-fatal");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .rpc(&sp_executesql_rpc("RAISERROR('die', 20, 1) WITH LOG"))
        .await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000 })),
        "the fatal error is delivered: {tokens:?}"
    );
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.stream.read(&mut byte),
    )
    .await
    .expect("the server must close the connection after a fatal RPC error")
    .expect("read after fatal");
    assert_eq!(read, 0, "closed after a severity-20 error in an RPC");

    let _ = std::fs::remove_file(&path);
}

/// REVIEW probe (passes): a fatal error in the FIRST RPC of a multi-RPC
/// request finishes that reply, never runs the remaining RPCs, and closes
/// the connection.
#[tokio::test]
async fn review_poc_fatal_in_a_multi_rpc_request_skips_the_rest() {
    use tokio::io::AsyncReadExt;

    let path = temp_path("review-multirpc-fatal");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let mut body = sp_executesql_rpc("RAISERROR('die', 20, 1) WITH LOG");
    body.push(0xff); // batch separator
    body.extend(sp_executesql_rpc("SELECT 1 AS a"));
    let tokens = client.rpc(&body).await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000 })),
        "the fatal error is delivered: {tokens:?}"
    );
    assert!(
        !tokens.iter().any(|t| matches!(t, Token::Row(_))),
        "the RPC after the fatal one never ran: {tokens:?}"
    );
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.stream.read(&mut byte),
    )
    .await
    .expect("the server must close the connection after a fatal error")
    .expect("read after fatal");
    assert_eq!(read, 0, "closed after the fatal RPC's reply");

    let _ = std::fs::remove_file(&path);
}

/// REVIEW probe (passes): a mid-batch RAISERROR <= 10 puts its INFO token
/// between the preceding statement's DONE and the next result set's
/// COLMETADATA — the stream-order rule the flush-before-RAISERROR exists
/// for.
#[tokio::test]
async fn review_poc_info_token_orders_between_result_sets() {
    let path = temp_path("review-info-order");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .batch("SELECT 1 AS a; RAISERROR('fyi', 5, 1); SELECT 2 AS b")
        .await;
    let info = tokens
        .iter()
        .position(|t| matches!(t, Token::Info { number: 50000 }))
        .unwrap_or_else(|| panic!("an INFO token: {tokens:?}"));
    let first_done = tokens
        .iter()
        .position(|t| matches!(t, Token::Done { .. }))
        .unwrap_or_else(|| panic!("a DONE token: {tokens:?}"));
    let second_colmeta = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| matches!(t, Token::ColMetadata(_)))
        .map(|(i, _)| i)
        .nth(1)
        .unwrap_or_else(|| panic!("a second COLMETADATA: {tokens:?}"));
    assert!(
        first_done < info,
        "the first SELECT's DONE precedes the INFO: {tokens:?}"
    );
    assert!(
        info < second_colmeta,
        "the INFO precedes the second result set: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [1, 2], "both SELECTs ran: {tokens:?}");

    let _ = std::fs::remove_file(&path);
}

/// `SET NOCOUNT ON` drops DONE_COUNT from statement DONEs on the wire —
/// results are untouched — and OFF restores it.
#[tokio::test]
async fn set_nocount_suppresses_done_counts_on_the_wire() {
    let path = temp_path("nocount");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE nc (id INT NOT NULL PRIMARY KEY)")
        .await;

    let counts = |tokens: &[Token]| -> Vec<Option<u64>> {
        tokens
            .iter()
            .filter_map(|t| match t {
                Token::Done { count, .. } => Some(*count),
                _ => None,
            })
            .collect()
    };

    let tokens = client
        .batch("SET NOCOUNT ON; INSERT INTO nc VALUES (1), (2); SELECT id FROM nc")
        .await;
    // SET, INSERT, SELECT: three DONEs, none carrying a count — but the rows
    // themselves still arrive.
    assert_eq!(
        counts(&tokens),
        [None, None, None],
        "NOCOUNT must drop every DONE count: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [1, 2], "results are untouched");

    // The option is session-durable and OFF restores the counts.
    let tokens = client.batch("INSERT INTO nc VALUES (3)").await;
    assert_eq!(counts(&tokens), [None], "still on across batches");
    let tokens = client
        .batch("SET NOCOUNT OFF; INSERT INTO nc VALUES (4)")
        .await;
    assert_eq!(
        counts(&tokens),
        [None, Some(1)],
        "OFF restores the count: {tokens:?}"
    );

    let _ = std::fs::remove_file(&path);
}

/// An INTN(4) parameter value.
fn b_int(b: &mut Vec<u8>, value: i32) {
    b.push(0x26); // INTN
    b.push(4);
    b.push(4);
    b.extend_from_slice(&value.to_le_bytes());
}

fn has_envchange(tokens: &[Token], kind: u8) -> bool {
    tokens
        .iter()
        .any(|t| matches!(t, Token::EnvChange { kind: k, .. } if *k == kind))
}

fn row_ints(tokens: &[Token]) -> Vec<i64> {
    tokens
        .iter()
        .filter_map(|t| match t {
            Token::Row(cells) => match cells.first() {
                Some(Cell::Int(v)) => Some(*v),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Wraps a header block with its ALL_HEADERS TotalLength (which includes itself).
fn headers_block(headers: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&((4 + headers.len()) as u32).to_le_bytes());
    out.extend_from_slice(headers);
    out
}

#[tokio::test]
async fn malformed_all_headers_is_rejected() {
    // A TotalLength that runs past the payload must be a protocol error. It was
    // previously treated as "no headers", handing the header bytes to the SQL
    // decoder as if they were the query.
    for bad in [
        // TotalLength beyond the payload.
        {
            let mut p = 9999u32.to_le_bytes().to_vec();
            p.extend(ucs2le("SELECT 1"));
            p
        },
        // TotalLength smaller than the field itself.
        {
            let mut p = 2u32.to_le_bytes().to_vec();
            p.extend(ucs2le("SELECT 1"));
            p
        },
        // A header whose HeaderLength is 0 (would stall the walk).
        {
            let mut h = 0u32.to_le_bytes().to_vec();
            h.extend_from_slice(&2u16.to_le_bytes());
            headers_block(&h)
        },
        // A header whose HeaderLength overruns the block.
        {
            let mut h = 999u32.to_le_bytes().to_vec();
            h.extend_from_slice(&2u16.to_le_bytes());
            headers_block(&h)
        },
        // A transaction-descriptor header with truncated data.
        {
            let mut h = (4u32 + 2 + 3).to_le_bytes().to_vec();
            h.extend_from_slice(&2u16.to_le_bytes());
            h.extend_from_slice(&[0, 0, 0]);
            headers_block(&h)
        },
    ] {
        let path = temp_path("bad-headers");
        let engine = engine(&path);
        let mut client = connect(engine).await;
        client.prelogin().await;
        client.login("sa", "secret").await;
        client.write_packet(PKT_SQL_BATCH, &bad).await;
        assert!(
            client.try_read_message().await.is_none(),
            "malformed ALL_HEADERS must close the connection, not answer"
        );
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn mismatched_transaction_descriptor_is_rejected() {
    // A descriptor the server never handed out means the client's transaction
    // view has desynchronised: the request must not run.
    let path = temp_path("bad-descriptor");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    // No transaction is open, so the connection's descriptor is 0; claim 42.
    client
        .raw_batch(&headers_block(&all_headers(42)), "SELECT 1")
        .await;
    assert!(
        client.try_read_message().await.is_none(),
        "a mismatched transaction descriptor must close the connection"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn transaction_descriptor_round_trips_through_envchange() {
    // The server mints a descriptor on TM begin, the client echoes it on the
    // next request, and it returns to 0 after commit. This is what the
    // validation above enforces, so pin the values rather than only the flow.
    let path = temp_path("descriptor-roundtrip");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    assert_eq!(client.tran_descriptor, 0, "no transaction at login");

    client.tm_request(TM_BEGIN_XACT, 0).await;
    let in_txn = client.tran_descriptor;
    assert_ne!(in_txn, 0, "begin must mint a non-zero descriptor");

    // The next request echoes it and is accepted (the server validates it).
    let rows = client.batch("SELECT 1").await;
    assert!(
        rows.iter().any(|t| matches!(t, Token::Row(_))),
        "echoing the descriptor must be accepted: {rows:?}"
    );
    assert_eq!(
        client.tran_descriptor, in_txn,
        "descriptor is stable in-txn"
    );

    client.tm_request(TM_COMMIT_XACT, 0).await;
    assert_eq!(client.tran_descriptor, 0, "commit clears the descriptor");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn begin_after_a_batch_commit_is_accepted_with_a_placeholder_descriptor() {
    // Regression: go-mssqldb hardcodes descriptor 0 on TM begin while using the
    // live descriptor everywhere else. Committing via a SQL batch leaves the
    // server's descriptor non-zero (the batch path emits no ENVCHANGE), so a
    // following begin arrives claiming 0 against a non-zero descriptor.
    // Validating a begin would kill this correct client's connection.
    let path = temp_path("begin-placeholder");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;

    // Begin via TM, then commit via a SQL batch: the server's descriptor stays
    // non-zero because only TM requests move it.
    client.tm_request(TM_BEGIN_XACT, 0).await;
    assert_ne!(client.tran_descriptor, 0, "begin minted a descriptor");
    client.batch("INSERT INTO t VALUES (1)").await;
    client.batch("COMMIT TRANSACTION").await;

    // A second begin, carrying go-mssqldb's placeholder 0, must be accepted.
    let begin = client.tm_request(TM_BEGIN_XACT, 0).await;
    assert!(
        has_envchange(&begin, 8),
        "a begin with a placeholder descriptor must be accepted: {begin:?}"
    );
    let rows = client.batch("SELECT id FROM t").await;
    assert!(
        rows.iter().any(|t| matches!(t, Token::Row(_))),
        "the connection is still usable: {rows:?}"
    );
    client.tm_request(TM_ROLLBACK_XACT, 0).await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn nested_tm_commit_keeps_the_transaction_and_its_descriptor() {
    // A nested COMMIT (@@TRANCOUNT 2 -> 1) does not end the transaction, so it
    // must not announce one ending: emitting ENVCHANGE 9 here would contradict
    // the same reply's DONE(INXACT) and zero a descriptor the client is still
    // meant to send.
    let path = temp_path("nested-tm-commit");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;

    client.tm_request(TM_BEGIN_XACT, 0).await;
    let descriptor = client.tran_descriptor;
    assert_ne!(descriptor, 0);
    // Nest a second transaction via SQL: @@TRANCOUNT is now 2.
    client.batch("BEGIN TRANSACTION").await;

    // The inner commit only decrements @@TRANCOUNT: still in a transaction.
    let commit = client.tm_request(TM_COMMIT_XACT, 0).await;
    assert!(
        !has_envchange(&commit, 9),
        "a nested commit must not announce the transaction ending: {commit:?}"
    );
    assert!(
        commit
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: true, .. })),
        "still in a transaction: {commit:?}"
    );
    assert_eq!(
        client.tran_descriptor, descriptor,
        "the descriptor survives a nested commit"
    );

    // The outer commit ends it: now the ENVCHANGE fires and clears it.
    let commit = client.tm_request(TM_COMMIT_XACT, 0).await;
    assert!(
        has_envchange(&commit, 9),
        "outer commit ends it: {commit:?}"
    );
    assert_eq!(client.tran_descriptor, 0);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn tm_begin_commit_persists_and_emits_envchanges() {
    let path = temp_path("tm-commit");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;

    // db.BeginTx(): a TM begin request → ENVCHANGE(8) + DONE(INXACT).
    let begin = client.tm_request(TM_BEGIN_XACT, 0).await;
    assert!(has_envchange(&begin, 8), "begin tokens: {begin:?}");
    assert!(
        begin
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: true, .. })),
        "begin DONE must set INXACT: {begin:?}"
    );

    // A statement inside the transaction reports it is still in a transaction.
    let insert = client.batch("INSERT INTO t VALUES (1)").await;
    assert!(
        insert
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: true, .. })),
        "in-txn statement DONE must set INXACT: {insert:?}"
    );

    // Commit → ENVCHANGE(9) + DONE without INXACT.
    let commit = client.tm_request(TM_COMMIT_XACT, 0).await;
    assert!(has_envchange(&commit, 9), "commit tokens: {commit:?}");
    assert!(
        commit
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: false, .. })),
        "commit DONE must clear INXACT: {commit:?}"
    );

    // The committed row is durable and visible after the transaction.
    let select = client.batch("SELECT id FROM t").await;
    assert_eq!(row_ints(&select), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn tm_begin_rollback_discards_writes() {
    let path = temp_path("tm-rollback");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;
    client.batch("INSERT INTO t VALUES (1)").await;

    client.tm_request(TM_BEGIN_XACT, 0).await;
    client.batch("INSERT INTO t VALUES (2)").await;

    // Rollback → ENVCHANGE(10); the second insert is discarded.
    let rollback = client.tm_request(TM_ROLLBACK_XACT, 0).await;
    assert!(
        has_envchange(&rollback, 10),
        "rollback tokens: {rollback:?}"
    );

    let select = client.batch("SELECT id FROM t ORDER BY id").await;
    assert_eq!(row_ints(&select), vec![1], "only the pre-txn row survives");
    let _ = std::fs::remove_file(path);
}

/// REVIEW PoC: SQL Server emits a prior statement's DONE before a later
/// USE's ENVCHANGE. TruthDB's core-side DONE deferral lets the ENVCHANGE
/// jump the queue: for "INSERT ...; USE truthdb" the ENVCHANGE (and INFO
/// 5701) reach the wire before the INSERT's DONE.
#[tokio::test]
async fn use_envchange_follows_prior_statement_done() {
    let path = temp_path("use-order");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE uo (id INT NOT NULL PRIMARY KEY)")
        .await;

    let tokens = client.batch("INSERT INTO uo VALUES (1); USE truthdb").await;
    let done_at = tokens
        .iter()
        .position(|t| matches!(t, Token::Done { count: Some(1), .. }))
        .expect("the INSERT's DONE");
    let env_at = tokens
        .iter()
        .position(|t| matches!(t, Token::EnvChange { kind: 1, .. }))
        .expect("the USE's ENVCHANGE");
    assert!(
        done_at < env_at,
        "SQL Server order: DONE(insert) before ENVCHANGE(database); got {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// REVIEW PoC: `sp_executesql N'USE truthdb'` over RPC — ENVCHANGE 1 + INFO
/// 5701 arrive, the statement's DONE is a DONEINPROC, and the RPC tail
/// (RETURNSTATUS, DONEPROC) stays framed.
#[tokio::test]
async fn use_inside_exec_rpc_keeps_doneinproc_framing() {
    let path = temp_path("use-rpc");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client.rpc(&sp_executesql_rpc("USE truthdb")).await;
    assert!(has_envchange(&tokens, 1), "ENVCHANGE 1: {tokens:?}");
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Info { number: 5701 })),
        "INFO 5701: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| matches!(t, Token::DoneInProc { .. })),
        "the USE's DONE renders as DONEINPROC: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| matches!(t, Token::ReturnStatus(0))),
        "RETURNSTATUS: {tokens:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::DoneProc { error: false, .. })),
        "DONEPROC: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// REVIEW PoC: two USEs in one batch — each emits its own ENVCHANGE + INFO.
#[tokio::test]
async fn two_uses_in_one_batch_emit_two_envchanges() {
    let path = temp_path("use-two");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client.batch("USE truthdb; USE truthdb").await;
    let envs = tokens
        .iter()
        .filter(|t| matches!(t, Token::EnvChange { kind: 1, .. }))
        .count();
    let infos = tokens
        .iter()
        .filter(|t| matches!(t, Token::Info { number: 5701 }))
        .count();
    assert_eq!((envs, infos), (2, 2), "tokens: {tokens:?}");
    let _ = std::fs::remove_file(&path);
}
