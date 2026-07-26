//! End-to-end TDS test: a minimal in-process TDS client drives the full
//! handshake and query flow against `serve_connection` over an in-memory
//! duplex stream, then decodes the token stream. This exercises every byte
//! path a real driver would (PRELOGIN, LOGIN7, SQLBatch, COLMETADATA, ROW,
//! DONE, ERROR) without needing an external SQL Server driver.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use truthdb_core::engine::Engine;
use truthdb_core::session::{EngineHandle, spawn_engine};
use truthdb_core::storage::{Storage, StorageOptions};
use truthdb_tds::LoginThrottle;
use truthdb_tds::server::{TdsConfig, serve_connection};

/// The loopback IP the in-process test client presents to the server.
const TEST_PEER: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

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
    // Seed the login the tests authenticate as (sa/secret) into the catalog
    // before the engine thread starts — auth is catalog-backed now, not from
    // config. The JoinHandle is dropped; the engine thread exits when the last
    // EngineHandle drops at end of test.
    let engine = Engine::new(storage).expect("engine");
    let mut users = BTreeMap::new();
    users.insert("sa".to_string(), "secret".to_string());
    engine.migrate_logins(&users).expect("seed logins");
    spawn_engine(engine).0
}

fn config() -> TdsConfig {
    TdsConfig {
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
        state: u8,
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
    ReturnValue {
        ordinal: u16,
        name: String,
        value: Cell,
    },
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
                    // ERROR token (MS-TDS 2.2.7.10): Number(4), State(1), Class(1)…
                    let state = body[4];
                    tokens.push(Token::Error { number, state });
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
                // usertype u32, flags u16, TYPE_INFO, then the value in that
                // type's row encoding.
                let ordinal = u16::from_le_bytes([payload[i], payload[i + 1]]);
                i += 2; // ParamOrdinal
                let name_chars = payload[i] as usize;
                i += 1;
                let units: Vec<u16> = payload[i..i + name_chars * 2]
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect();
                let name = String::from_utf16(&units).unwrap();
                i += name_chars * 2;
                i += 1 + 4 + 2; // status, usertype, flags
                let (col_type, consumed) = parse_type_info(&payload[i..]);
                i += consumed;
                let (cells, consumed) = parse_row(&payload[i..], &[col_type]);
                i += consumed;
                tokens.push(Token::ReturnValue {
                    ordinal,
                    name,
                    value: cells.into_iter().next().unwrap(),
                });
            }
            other => {
                tokens.push(Token::Other(other));
                break; // unknown token: stop to avoid misparsing
            }
        }
    }
    tokens
}

/// Parses one TYPE_INFO (the type token and its type-specific bytes), shared by
/// COLMETADATA columns and the RETURNVALUE token. Returns the type and the
/// number of bytes consumed.
fn parse_type_info(bytes: &[u8]) -> (ColType, usize) {
    let type_token = bytes[0];
    let mut i = 1;
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
    (col_type, i)
}

fn parse_colmetadata(bytes: &[u8]) -> (Vec<ColType>, usize) {
    let count = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let mut i = 2;
    let mut cols = Vec::with_capacity(count);
    for _ in 0..count {
        i += 4; // usertype
        i += 2; // flags
        let (col_type, consumed) = parse_type_info(&bytes[i..]);
        i += consumed;
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
    connect_with_throttle(engine, cfg, LoginThrottle::new()).await
}

async fn connect_with_throttle(
    engine: EngineHandle,
    cfg: TdsConfig,
    throttle: LoginThrottle,
) -> Client {
    let (client_half, server_half) = tokio::io::duplex(64 * 1024);
    let cfg = Arc::new(cfg);
    tokio::spawn(async move {
        let _ = serve_connection(server_half, engine, cfg, throttle, TEST_PEER).await;
    });
    Client {
        stream: client_half,
        tran_descriptor: 0,
    }
}

async fn connect(engine: EngineHandle) -> Client {
    connect_with(engine, config()).await
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

/// The single 18456 error a login failure must produce: number 18456, wire
/// state 1, and NO LoginAck. Returns the matched error's state for equality
/// checks across failure kinds.
fn assert_login_denied(tokens: &[Token]) -> u8 {
    assert!(
        !tokens.iter().any(|t| matches!(t, Token::LoginAck)),
        "a denied login must not ack: {tokens:?}"
    );
    let state = tokens.iter().find_map(|t| match t {
        Token::Error {
            number: 18456,
            state,
        } => Some(*state),
        _ => None,
    });
    state.unwrap_or_else(|| panic!("expected an 18456 error: {tokens:?}"))
}
// Transaction Manager request types (MS-TDS 2.2.6.9).
const TM_BEGIN_XACT: u16 = 5;
const TM_COMMIT_XACT: u16 = 7;
const TM_ROLLBACK_XACT: u16 = 8;

/// An RPC argument value, encoded as an input parameter (TYPE_INFO + value).
enum RpcArg {
    Int(i32),
    IntNull,
    NVarChar(String),
}

impl RpcArg {
    fn encode(&self, b: &mut Vec<u8>) {
        match self {
            RpcArg::Int(v) => {
                b.push(0x26); // INTN
                b.push(4); // max len
                b.push(4); // value len
                b.extend_from_slice(&v.to_le_bytes());
            }
            RpcArg::IntNull => {
                b.push(0x26);
                b.push(4);
                b.push(0); // NULL: zero-length value
            }
            RpcArg::NVarChar(s) => {
                b.push(0xe7); // NVARCHAR
                b.extend_from_slice(&8000u16.to_le_bytes());
                b.extend_from_slice(&[0x09, 0x04, 0xd0, 0x00, 0x34]); // collation
                let bytes = ucs2le(s);
                b.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                b.extend_from_slice(&bytes);
            }
        }
    }
}

/// One RPC-by-name call of a user procedure. Each parameter is (name, value,
/// output): an empty name is a positional argument; `output` sets the
/// fByRefValue status bit (an OUTPUT argument returned as a RETURNVALUE).
fn proc_rpc(name: &str, params: &[(&str, RpcArg, bool)]) -> Vec<u8> {
    let mut b = Vec::new();
    let name_units = ucs2le(name);
    b.extend_from_slice(&((name_units.len() / 2) as u16).to_le_bytes()); // NameLen (chars)
    b.extend_from_slice(&name_units);
    b.extend_from_slice(&0u16.to_le_bytes()); // option flags
    for (pname, value, output) in params {
        let pn = ucs2le(pname);
        b.push((pn.len() / 2) as u8); // param-name char count (0 = positional)
        b.extend_from_slice(&pn);
        b.push(if *output { 0x01 } else { 0x00 }); // StatusFlags: fByRefValue
        value.encode(&mut b);
    }
    b
}

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

/// Extracts the (ordinal, name, value) of every RETURNVALUE token in order.
fn return_values(tokens: &[Token]) -> Vec<(u16, String, Cell)> {
    tokens
        .iter()
        .filter_map(|t| match t {
            Token::ReturnValue {
                ordinal,
                name,
                value,
            } => Some((*ordinal, name.clone(), value.clone())),
            _ => None,
        })
        .collect()
}

/// The one RETURNSTATUS value in a reply.
fn return_status(tokens: &[Token]) -> Option<i32> {
    tokens.iter().find_map(|t| match t {
        Token::ReturnStatus(v) => Some(*v),
        _ => None,
    })
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

mod authentication;
mod envchange;
mod errors;
mod protocol;
mod rpc;
mod transaction_manager;
