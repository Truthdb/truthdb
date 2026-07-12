//! End-to-end TDS test: a minimal in-process TDS client drives the full
//! handshake and query flow against `serve_connection` over an in-memory
//! duplex stream, then decodes the token stream. This exercises every byte
//! path a real driver would (PRELOGIN, LOGIN7, SQLBatch, COLMETADATA, ROW,
//! DONE, ERROR) without needing an external SQL Server driver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use truthdb_core::engine::Engine;
use truthdb_core::storage::{Storage, StorageOptions};
use truthdb_tds::server::{TdsConfig, serve_connection};

// Packet types.
const PKT_SQL_BATCH: u8 = 0x01;
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

fn engine(path: &std::path::Path) -> Arc<Mutex<Engine>> {
    let opts = StorageOptions {
        size_gib: 1,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
    };
    let storage = Storage::create(path.to_path_buf(), opts).expect("storage");
    Arc::new(Mutex::new(Engine::new(storage).expect("engine")))
}

fn config() -> TdsConfig {
    let mut users = HashMap::new();
    users.insert("sa".to_string(), "secret".to_string());
    TdsConfig {
        users,
        database: "truthdb".to_string(),
    }
}

fn ucs2le(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
}

/// A minimal client that speaks just enough TDS to test the server.
struct Client {
    stream: DuplexStream,
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

    async fn login(&mut self, user: &str, password: &str) -> Vec<Token> {
        self.write_packet(PKT_LOGIN7, &build_login7(user, password, "truthdb"))
            .await;
        let (_, payload) = self.read_message().await;
        parse_tokens(&payload)
    }

    async fn batch(&mut self, sql: &str) -> Vec<Token> {
        let mut payload = Vec::new();
        // ALL_HEADERS: TotalLength includes itself (the 4-byte field) plus
        // the header block; the SQL text starts right after.
        let headers = all_headers();
        let total = 4 + headers.len();
        payload.extend_from_slice(&(total as u32).to_le_bytes());
        payload.extend_from_slice(&headers);
        payload.extend(ucs2le(sql));
        self.write_packet(PKT_SQL_BATCH, &payload).await;
        let (_, response) = self.read_message().await;
        parse_tokens(&response)
    }
}

/// A minimal ALL_HEADERS with a transaction-descriptor header (type 2).
fn all_headers() -> Vec<u8> {
    // Header: length u32 | type u16 | transaction descriptor u64 | request count u32
    let mut header = Vec::new();
    let body_len = 4 + 2 + 8 + 4;
    header.extend_from_slice(&(body_len as u32).to_le_bytes());
    header.extend_from_slice(&2u16.to_le_bytes()); // transaction descriptor
    header.extend_from_slice(&0u64.to_le_bytes());
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
    Error { number: i32 },
    Done { count: Option<u64> },
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
                let count = u64::from_le_bytes(payload[i + 4..i + 12].try_into().unwrap());
                let has_count = status & 0x0010 != 0;
                i += 12;
                tokens.push(Token::Done {
                    count: has_count.then_some(count),
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

async fn connect(engine: Arc<Mutex<Engine>>) -> Client {
    let (client_half, server_half) = tokio::io::duplex(64 * 1024);
    let cfg = Arc::new(config());
    tokio::spawn(async move {
        let _ = serve_connection(server_half, engine, cfg).await;
    });
    Client {
        stream: client_half,
    }
}

#[tokio::test]
async fn full_handshake_query_and_error() {
    let path = temp_path("e2e");
    let engine = engine(&path);
    let mut client = connect(Arc::clone(&engine)).await;

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
            .any(|t| matches!(t, Token::Done { count: Some(3) })),
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
