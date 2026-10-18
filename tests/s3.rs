//! The S3 API against a real server process: access keys come from the
//! native protocol, every request is authenticated with SigV4, and errors
//! come back as S3 XML.

#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use truthdb_net::{read_frame, write_frame};
use truthdb_proto::{
    CommandReq, CommandResp, Frame, HelloReq, HelloResp, MsgType, PROTOCOL_VERSION, decode_message,
    encode_message,
};
use truthdb_s3::sigv4;

const DEADLINE: Duration = Duration::from_secs(60);

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

struct Server {
    child: Child,
    dir: PathBuf,
    native_port: u16,
    s3_port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn start_server() -> Server {
    let dir = std::env::temp_dir().join(format!(
        "truthdb-s3-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let (native_port, s3_port) = (free_port(), free_port());
    let config = dir.join("truthdb.toml");
    std::fs::write(
        &config,
        format!(
            "[network]\naddr = \"127.0.0.1\"\nport = {native_port}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n\n[s3]\nenabled = true\naddr = \"127.0.0.1\"\nport = {s3_port}\n",
            dir.join("truth.db").display()
        ),
    )
    .expect("config");
    let log = std::fs::File::create(dir.join("server.log")).expect("log");
    let child = Command::new(env!("CARGO_BIN_EXE_truthdb"))
        .env("TRUTHDB_CONFIG", &config)
        .stdout(Stdio::from(log.try_clone().expect("clone")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn server");
    Server {
        child,
        dir,
        native_port,
        s3_port,
    }
}

/// Sends one native-protocol command and returns its JSON reply.
fn native(port: u16, command: &str) -> serde_json::Value {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let deadline = Instant::now() + DEADLINE;
        let mut stream = loop {
            match tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                Ok(stream) => break stream,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(err) => panic!("server never came up: {err}"),
            }
        };
        let hello = HelloReq {
            protocol_version: PROTOCOL_VERSION,
            client_name: "s3-test".into(),
            client_version: "0".into(),
        };
        let frame = Frame {
            msg_type: MsgType::HelloReq,
            flags: 0,
            payload: encode_message(&hello).expect("encode"),
        };
        write_frame(&mut stream, &frame).await.expect("hello");
        let resp = read_frame(&mut stream).await.expect("hello resp");
        let _: HelloResp = decode_message(&resp.payload).expect("hello decode");
        let req = CommandReq {
            id: 1,
            command: command.to_string(),
        };
        let frame = Frame {
            msg_type: MsgType::CommandReq,
            flags: 0,
            payload: encode_message(&req).expect("encode"),
        };
        write_frame(&mut stream, &frame).await.expect("send");
        let resp = read_frame(&mut stream).await.expect("recv");
        let resp: CommandResp = decode_message(&resp.payload).expect("decode");
        serde_json::from_str(&resp.message).expect("json reply")
    })
}

fn amz_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since 1970 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Sends a signed GET (signing with `secret`, or unsigned when `None`) and
/// returns the status and body.
fn get(port: u16, path: &str, key: &str, secret: Option<&str>) -> (u16, String) {
    let amz_date = amz_now();
    let host = format!("127.0.0.1:{port}");
    let payload = sigv4::sha256_hex(b"");
    let headers = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nx-amz-content-sha256: {payload}\r\nx-amz-date: {amz_date}\r\nConnection: close\r\n"
    );
    if let Some(secret) = secret {
        let credential = sigv4::Credential {
            access_key: key.to_string(),
            date: amz_date[..8].to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        };
        let signed = vec![
            "host".to_string(),
            "x-amz-content-sha256".to_string(),
            "x-amz-date".to_string(),
        ];
        let canonical =
            sigv4::canonical_request("GET", path, "", &headers, &signed, &payload).unwrap();
        let signature = sigv4::sign(
            &sigv4::signing_key(secret, &credential),
            &sigv4::string_to_sign(&amz_date, &credential.scope(), &canonical),
        );
        request.push_str(&format!(
            "Authorization: AWS4-HMAC-SHA256 Credential={key}/{}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}\r\n",
            credential.scope()
        ));
    }
    request.push_str("\r\n");
    let deadline = Instant::now() + DEADLINE;
    let mut stream = loop {
        match std::net::TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => break stream,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(err) => panic!("S3 listener never came up: {err}"),
        }
    };
    stream.write_all(request.as_bytes()).expect("send");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

#[test]
fn requests_are_authenticated_with_sigv4_against_native_access_keys() {
    let server = start_server();
    let created = native(server.native_port, "s3 create access key");
    let key = created["access_key_id"].as_str().expect("id").to_string();
    let secret = created["secret_access_key"]
        .as_str()
        .expect("secret")
        .to_string();

    // A correctly signed request passes authentication and reaches a route
    // that does not exist yet.
    let (status, body) = get(server.s3_port, "/bucket/key", &key, Some(&secret));
    assert_eq!(status, 501, "{body}");
    assert!(body.contains("<Code>NotImplemented</Code>"), "{body}");

    let (status, body) = get(server.s3_port, "/bucket/key", &key, Some("wrong-secret"));
    assert_eq!(status, 403, "{body}");
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{body}"
    );

    let (status, body) = get(server.s3_port, "/bucket/key", "TDBNOSUCHKEY", Some(&secret));
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>InvalidAccessKeyId</Code>"), "{body}");

    let (status, body) = get(server.s3_port, "/bucket/key", &key, None);
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}
