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

/// Three distinct free ports: all are held until all are chosen, so none
/// can be one released before.
fn free_ports() -> (u16, u16, u16) {
    let bind = || std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let (first, second, third) = (bind(), bind(), bind());
    let port = |listener: &std::net::TcpListener| listener.local_addr().expect("addr").port();
    (port(&first), port(&second), port(&third))
}

struct Server {
    child: Child,
    dir: PathBuf,
    native_port: u16,
    s3_port: u16,
    /// The S3 listener's certificate, when it serves TLS.
    cert: Option<Vec<u8>>,
    /// The Kafka listener's port, when it runs (with the S3 listener's
    /// certificate).
    kafka_port: Option<u16>,
    /// The admin listener's port, when it runs (with `ADMIN_TOKEN`; in
    /// place of the Kafka listener).
    admin_port: Option<u16>,
}

/// The admin listener's token in the test servers.
const ADMIN_TOKEN: &str = "admin-test-token";

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Starts the server binary on fresh ports and waits until both listeners
/// answer. A port can still be taken by another process between choosing it
/// and the server binding it; the server then exits, and it is started again
/// on new ports.
fn start_server() -> Server {
    start_server_with(false)
}

/// [`start_server`]; `encrypted`: with a master key configured, so the
/// database it creates is encrypted at rest.
fn start_server_with(encrypted: bool) -> Server {
    start_server_tls(encrypted, false)
}

/// [`start_server_with`]; `tls`: the S3 listener serves HTTPS with a
/// self-signed certificate (see [`send_tls`]).
fn start_server_tls(encrypted: bool, tls: bool) -> Server {
    start_server_full(encrypted, tls, false)
}

/// [`start_server_tls`]; `kafka`: the Kafka listener runs too (it needs
/// `tls`).
fn start_server_full(encrypted: bool, tls: bool, kafka: bool) -> Server {
    for _ in 0..5 {
        let server = spawn_server(encrypted, tls, kafka);
        let deadline = Instant::now() + DEADLINE;
        let mut server = Some(server);
        while Instant::now() < deadline {
            let running = server.as_mut().unwrap();
            if running.child.try_wait().expect("poll the server").is_some() {
                break;
            }
            // Listening on both ports itself: another test's server may have
            // taken a port between choosing it and this one binding it.
            let pid = running.child.id();
            if [running.native_port, running.s3_port]
                .into_iter()
                .chain(running.kafka_port)
                .chain(running.admin_port)
                .all(|port| listens(pid, port))
            {
                return server.take().unwrap();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        drop(server);
    }
    panic!("the server never came up");
}

/// Whether process `pid` holds the socket listening on IPv4 `port`: an
/// entry of /proc/net/tcp in state LISTEN whose inode is one of the
/// process's file descriptors.
fn listens(pid: u32, port: u16) -> bool {
    let Ok(table) = std::fs::read_to_string("/proc/net/tcp") else {
        return false;
    };
    let inodes: Vec<String> = table
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (_, local_port) = fields.get(1)?.split_once(':')?;
            (u16::from_str_radix(local_port, 16).ok()? == port && *fields.get(3)? == "0A")
                .then(|| format!("socket:[{}]", fields.get(9).unwrap_or(&"")))
        })
        .collect();
    let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) else {
        return false;
    };
    fds.filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .any(|target| {
            inodes
                .iter()
                .any(|inode| target.to_string_lossy() == *inode)
        })
}

/// How long the test servers take to restore an archived object, per tier:
/// Bulk restores stay under way for a while.
const RESTORE_SECONDS: &str = "restore_seconds = { expedited = 0, standard = 0, bulk = 2 }\n";

fn spawn_server(encrypted: bool, tls: bool, kafka: bool) -> Server {
    let dir = std::env::temp_dir().join(format!(
        "truthdb-s3-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let (native_port, s3_port, kafka_port) = free_ports();
    let config = dir.join("truthdb.toml");
    let encryption = if encrypted {
        use std::os::unix::fs::PermissionsExt;
        let keys = dir.join("master.keys");
        std::fs::write(&keys, format!("main {}\n", "4f".repeat(32))).expect("key file");
        std::fs::set_permissions(&keys, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        format!("\n[encryption]\nmaster_key_file = \"{}\"\n", keys.display())
    } else {
        String::new()
    };
    let (cert, tls_config) = if tls {
        let certified = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .expect("a cert");
        let (cert_path, key_path) = (dir.join("s3.crt"), dir.join("s3.key"));
        std::fs::write(&cert_path, certified.cert.pem()).expect("cert");
        std::fs::write(&key_path, certified.key_pair.serialize_pem()).expect("key");
        let tls_files = format!(
            "tls_cert = \"{}\"\ntls_key = \"{}\"\n",
            cert_path.display(),
            key_path.display()
        );
        let kafka_config = if kafka {
            format!(
                "\n[kafka]\nenabled = true\naddr = \"127.0.0.1\"\nport = {kafka_port}\nadvertised_host = \"127.0.0.1\"\n{tls_files}"
            )
        } else {
            String::new()
        };
        (
            Some(certified.cert.der().to_vec()),
            format!("{tls_files}{kafka_config}"),
        )
    } else {
        (None, String::new())
    };
    // The admin listener, on the port the Kafka listener would take.
    let admin_config = if kafka {
        String::new()
    } else {
        format!(
            "\n[admin]\nenabled = true\naddr = \"127.0.0.1\"\nport = {kafka_port}\ntoken = \"{ADMIN_TOKEN}\"\n"
        )
    };
    std::fs::write(
        &config,
        format!(
            "[network]\naddr = \"127.0.0.1\"\nport = {native_port}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n\n[s3]\nenabled = true\naddr = \"127.0.0.1\"\nport = {s3_port}\ndomains = [\"s3.test\"]\nmetrics_flush_seconds = 1\naccess_log_flush_seconds = 1\nactivity_flush_seconds = 1\ntrail_flush_seconds = 1\n{RESTORE_SECONDS}{tls_config}{encryption}{admin_config}",
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
        cert,
        kafka_port: kafka.then_some(kafka_port),
        admin_port: (!kafka).then_some(kafka_port),
    }
}

/// Sends one native-protocol command and returns its JSON reply.
fn native(port: u16, command: &str) -> serde_json::Value {
    let reply = native_text(port, command);
    serde_json::from_str(&reply).unwrap_or_else(|_| panic!("not a JSON reply: {reply}"))
}

/// Sends one native-protocol command and returns its reply as text (an
/// error is not JSON).
fn native_text(port: u16, command: &str) -> String {
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
        resp.message
    })
}

fn amz_now() -> String {
    sigv4_date(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
}

/// Seconds since the epoch as an `x-amz-date`.
fn sigv4_date(secs: u64) -> String {
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

/// Sends a request with `body`, signed with `secret` (unsigned when `None`),
/// and returns the status and body.
fn send(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    key: &str,
    secret: Option<&str>,
) -> (u16, String) {
    send_with(port, method, path, query, &[], body, key, secret).1
}

/// [`send`] with extra (unsigned) headers; returns the response headers too.
#[allow(clippy::too_many_arguments)]
fn send_with(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: &[u8],
    key: &str,
    secret: Option<&str>,
) -> (String, (u16, String)) {
    send_paused(
        port,
        method,
        path,
        query,
        extra,
        body,
        key,
        secret,
        body.len(),
        || {},
    )
}

/// [`send_with`], sending the first `split` bytes of the body, then running
/// `pause`, then sending the rest: the request is signed and its head
/// received before `pause` acts.
#[allow(clippy::too_many_arguments)]
fn send_paused(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: &[u8],
    key: &str,
    secret: Option<&str>,
    split: usize,
    pause: impl FnOnce(),
) -> (String, (u16, String)) {
    // A request paused within its body sends `Expect: 100-continue`: the
    // server answers 100 Continue once its handler reads the body, so the
    // head has been acted on before `pause`.
    let paused = split < body.len();
    let mut extra = extra.to_vec();
    if paused {
        extra.push(("expect", "100-continue"));
    }
    let request = signed_request(port, method, path, query, &extra, body, key, secret);
    let deadline = Instant::now() + DEADLINE;
    let mut stream = loop {
        match std::net::TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => break stream,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(err) => panic!("S3 listener never came up: {err}"),
        }
    };
    stream.write_all(request.as_bytes()).expect("send");
    stream.flush().expect("flush");
    if paused {
        let mut interim = Vec::new();
        let mut byte = [0u8];
        while !interim.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).expect("read 100 Continue");
            interim.push(byte[0]);
        }
        assert!(
            interim.starts_with(b"HTTP/1.1 100"),
            "no 100 Continue: {}",
            String::from_utf8_lossy(&interim)
        );
    }
    stream.write_all(&body[..split]).expect("send body");
    stream.flush().expect("flush");
    pause();
    stream.write_all(&body[split..]).expect("send body");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    parse_response(&raw)
}

/// [`send_with`] over TLS to `server`'s HTTPS listener (trusting its
/// self-signed certificate).
#[allow(clippy::too_many_arguments)]
fn send_tls(
    server: &Server,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: &[u8],
    key: &str,
    secret: Option<&str>,
) -> (String, (u16, String)) {
    let mut stream = tls_stream(server);
    let request = signed_request(
        server.s3_port,
        method,
        path,
        query,
        extra,
        body,
        key,
        secret,
    );
    stream.write_all(request.as_bytes()).expect("send");
    // The server may answer (and close) before the whole body is sent.
    let _ = stream.write_all(body).and_then(|()| stream.flush());
    // It may close without a TLS close_notify: what arrived counts.
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    parse_response(&raw)
}

/// A TLS connection to `server`'s S3 listener, trusting its certificate.
fn tls_stream(
    server: &Server,
) -> rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream> {
    tls_stream_to(server, server.s3_port)
}

/// [`tls_stream`] to another of `server`'s TLS listeners.
fn tls_stream_to(
    server: &Server,
    port: u16,
) -> rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(
            server.cert.clone().expect("a TLS server"),
        ))
        .expect("a root");
    let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").expect("a name");
    let connection =
        rustls::ClientConnection::new(std::sync::Arc::new(config), name).expect("a connection");
    let tcp = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    rustls::StreamOwned::new(connection, tcp)
}

/// A multipart/form-data body of `fields` and `file`, and its boundary.
fn form_body(fields: &[(String, String)], filename: &str, file: &[u8]) -> (&'static str, Vec<u8>) {
    let boundary = "----truthdbFormBoundary7MA4YWxk";
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(
        format!("\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"submit\"\r\n\r\nUpload\r\n--{boundary}--\r\n").as_bytes(),
    );
    (boundary, body)
}

/// [`post_form`] over TLS to `server`'s HTTPS listener.
fn post_form_tls(
    server: &Server,
    bucket: &str,
    fields: &[(String, String)],
    filename: &str,
    file: &[u8],
) -> (String, (u16, String)) {
    let (boundary, body) = form_body(fields, filename, file);
    let mut stream = tls_stream(server);
    let head = format!(
        "POST /{bucket} HTTP/1.1\r\nHost: localhost\r\nContent-Type: multipart/form-data; boundary={boundary}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).expect("send");
    let _ = stream.write_all(&body).and_then(|()| stream.flush());
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    parse_response(&raw)
}

/// A SigV4-signed request's head (the body follows it).
#[allow(clippy::too_many_arguments)]
fn signed_request(
    port: u16,
    method: &str,
    path: &str,
    query: &str,
    extra: &[(&str, &str)],
    body: &[u8],
    key: &str,
    secret: Option<&str>,
) -> String {
    let amz_date = amz_now();
    // A `host` among the extra headers replaces the listener's address.
    let host = extra
        .iter()
        .find(|(name, _)| *name == "host")
        .map_or_else(|| format!("127.0.0.1:{port}"), |(_, host)| host.to_string());
    let extra: Vec<&(&str, &str)> = extra.iter().filter(|(name, _)| *name != "host").collect();
    let payload = sigv4::sha256_hex(body);
    // Extra headers are signed too (SigV4 lists signed headers sorted), except
    // the ones named `unsigned-<name>`, sent as `<name>` without a signature.
    let mut headers = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    headers.extend(
        extra
            .iter()
            .filter(|(name, _)| !name.starts_with("unsigned-"))
            // SigV4 signs header names in lowercase.
            .map(|(name, value)| (name.to_ascii_lowercase(), value.to_string())),
    );
    headers.sort();
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let mut request = format!(
        "{method} {target} HTTP/1.1\r\nHost: {host}\r\nx-amz-content-sha256: {payload}\r\nx-amz-date: {amz_date}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra {
        let name = name.strip_prefix("unsigned-").unwrap_or(name);
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(secret) = secret {
        let credential = sigv4::Credential {
            access_key: key.to_string(),
            date: amz_date[..8].to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        };
        let signed: Vec<String> = headers.iter().map(|(name, _)| name.clone()).collect();
        let canonical =
            sigv4::canonical_request(method, path, query, &headers, &signed, &payload).unwrap();
        let signature = sigv4::sign(
            &sigv4::signing_key(secret, &credential),
            &sigv4::string_to_sign(&amz_date, &credential.scope(), &canonical),
        );
        request.push_str(&format!(
            "Authorization: AWS4-HMAC-SHA256 Credential={key}/{}, SignedHeaders={}, Signature={signature}\r\n",
            credential.scope(),
            signed.join(";")
        ));
    }
    request.push_str("\r\n");
    request
}

/// A raw response's head, status and body.
fn parse_response(raw: &[u8]) -> (String, (u16, String)) {
    let response = latin1(raw);
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .map(|(head, body)| (head.to_string(), body.to_string()))
        .unwrap_or_default();
    (head, (status, body))
}

fn get(port: u16, path: &str, key: &str, secret: Option<&str>) -> (u16, String) {
    send(port, "GET", path, "", b"", key, secret)
}

/// A server plus an access key on it.
fn server_with_key() -> (Server, String, String) {
    let server = start_server();
    let created = native(server.native_port, "s3 create access key");
    let key = created["access_key_id"].as_str().expect("id").to_string();
    let secret = created["secret_access_key"]
        .as_str()
        .expect("secret")
        .to_string();
    (server, key, secret)
}

/// A new account on the server, and a root access key of it.
fn account_with_key(server: &Server, name: &str) -> (String, String, String) {
    let account = native(server.native_port, &format!("s3 create account {name}"));
    let account = account["account_id"].as_str().expect("id").to_string();
    let created = native(
        server.native_port,
        &format!("s3 create access key {account}"),
    );
    (
        account,
        created["access_key_id"].as_str().expect("id").to_string(),
        created["secret_access_key"]
            .as_str()
            .expect("secret")
            .to_string(),
    )
}

#[test]
fn each_account_sees_and_touches_only_its_own_buckets() {
    let (server, key_a, secret_a) = server_with_key();
    let port = server.s3_port;
    let (account_b, key_b, secret_b) = account_with_key(&server, "bee");
    let a = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(port, method, path, query, body, &key_a, Some(&secret_a))
    };
    let b = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(port, method, path, query, body, &key_b, Some(&secret_b))
    };
    assert_eq!(a("PUT", "/shared", "", b"").0, 200);
    assert_eq!(a("PUT", "/shared/doc", "", b"hello").0, 200);

    // Account B can neither read, write, list nor delete A's bucket, nor
    // take its name.
    for (method, path, query) in [
        ("GET", "/shared", "list-type=2"),
        ("HEAD", "/shared", ""),
        ("GET", "/shared/doc", ""),
        ("PUT", "/shared/other", ""),
        ("DELETE", "/shared/doc", ""),
        ("GET", "/shared", "versions"),
        ("PUT", "/shared", "versioning"),
        ("DELETE", "/shared", ""),
    ] {
        let (status, body) = b(method, path, query, b"");
        assert_eq!(status, 403, "{method} {path}?{query}: {body}");
    }
    let (status, body) = b("PUT", "/shared", "", b"");
    assert_eq!(status, 409);
    assert!(body.contains("BucketAlreadyExists"), "{body}");
    assert_eq!(a("GET", "/shared/doc", "", b"").1, "hello", "untouched");

    // B's own bucket, which A cannot copy into nor B copy out of A's.
    assert_eq!(b("PUT", "/bees", "", b"").0, 200);
    let (_, (status, body)) = send_with(
        port,
        "PUT",
        "/bees/stolen",
        "",
        &[("x-amz-copy-source", "/shared/doc")],
        b"",
        &key_b,
        Some(&secret_b),
    );
    assert_eq!(status, 403, "{body}");
    assert_eq!(a("PUT", "/bees/x", "", b"").0, 403);
    // A copy source header on anything but a copy is not a copy.
    assert_eq!(b("PUT", "/bees/own", "", b"mine").0, 200);
    let (_, (status, body)) = send_with(
        port,
        "GET",
        "/bees/own",
        "",
        &[("x-amz-copy-source", "/shared/doc")],
        b"",
        &key_b,
        Some(&secret_b),
    );
    assert_eq!((status, body.as_str()), (200, "mine"));

    // Each lists its own buckets, as their owner.
    let listed = |list: &str| -> Vec<String> {
        list.split("<Name>")
            .skip(1)
            .map(|rest| rest.split('<').next().unwrap().to_string())
            .collect()
    };
    let (status, list_a) = a("GET", "/", "", b"");
    assert_eq!(status, 200);
    assert_eq!(listed(&list_a), ["shared"]);
    assert!(list_a.contains(&format!("<ID>{}</ID>", sigv4::sha256_hex(b"000000000000"))));
    let (_, list_b) = b("GET", "/", "", b"");
    assert_eq!(listed(&list_b), ["bees"]);
    assert!(list_b.contains(&format!(
        "<ID>{}</ID>",
        sigv4::sha256_hex(account_b.as_bytes())
    )));

    // A key of one of B's users acts for B, as far as the user's policies
    // allow.
    native(
        server.native_port,
        &format!("s3 create user {account_b} carol"),
    );
    let carol = native(
        server.native_port,
        &format!("s3 create access key {account_b} carol"),
    );
    let carol_lists = || {
        send(
            port,
            "GET",
            "/bees",
            "list-type=2",
            b"",
            carol["access_key_id"].as_str().unwrap(),
            carol["secret_access_key"].as_str(),
        )
        .0
    };
    assert_eq!(carol_lists(), 403, "a user with no policy may do nothing");
    native(
        server.native_port,
        &format!(
            r#"s3 put user policy {account_b} carol list {{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::bees"}}}}"#
        ),
    );
    assert_eq!(carol_lists(), 200);

    // Vector buckets too are their owner's.
    use serde_json::json;
    let (status, _, _) = vectors(
        port,
        &key_b,
        &secret_b,
        "CreateVectorBucket",
        json!({"vectorBucketName": "bee-vectors"}),
    );
    assert_eq!(status, 200);
    let (status, kind, _) = vectors(
        port,
        &key_a,
        &secret_a,
        "GetVectorBucket",
        json!({"vectorBucketName": "bee-vectors"}),
    );
    assert_eq!(
        (status, kind.as_deref()),
        (403, Some("AccessDeniedException"))
    );
    let (status, _, listed) = vectors(port, &key_a, &secret_a, "ListVectorBuckets", json!({}));
    assert_eq!(status, 200);
    assert_eq!(listed["vectorBuckets"], json!([]));
    let (_, _, got) = vectors(
        port,
        &key_b,
        &secret_b,
        "GetVectorBucket",
        json!({"vectorBucketName": "bee-vectors"}),
    );
    assert_eq!(
        got["vectorBucket"]["vectorBucketArn"],
        json!(format!(
            "arn:aws:s3vectors:us-east-1:{account_b}:bucket/bee-vectors"
        ))
    );
}

#[test]
fn a_request_is_checked_again_when_its_operation_runs() {
    let (server, key_a, secret_a) = server_with_key();
    let port = server.s3_port;
    let (_, key_b, secret_b) = account_with_key(&server, "bee");
    let a = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(port, method, path, query, body, &key_a, Some(&secret_a))
    };
    let pause = || std::thread::sleep(Duration::from_millis(300));

    // B starts deleting from a bucket that does not exist yet; A creates it
    // and stores an object before B's body arrives.
    let delete = b"<Delete><Object><Key>doc</Key></Object></Delete>";
    let md5 = {
        use base64::Engine as _;
        use md5::Digest;
        base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(delete))
    };
    let (_, (status, body)) = send_paused(
        port,
        "POST",
        "/victim",
        "delete",
        &[("content-md5", &md5)],
        delete,
        &key_b,
        Some(&secret_b),
        5,
        || {
            pause();
            assert_eq!(a("PUT", "/victim", "", b"").0, 200);
            assert_eq!(a("PUT", "/victim/doc", "", b"kept").0, 200);
        },
    );
    assert_eq!(status, 403, "{body}");
    assert_eq!(a("GET", "/victim/doc", "", b"").1, "kept");

    // B starts a put into its own bucket, gives the bucket up, and A takes
    // the name before B's body arrives.
    assert_eq!(
        send(port, "PUT", "/grab", "", b"", &key_b, Some(&secret_b)).0,
        200
    );
    let (_, (status, body)) = send_paused(
        port,
        "PUT",
        "/grab/evil",
        "",
        &[],
        b"planted by B",
        &key_b,
        Some(&secret_b),
        3,
        || {
            pause();
            assert_eq!(
                send(port, "DELETE", "/grab", "", b"", &key_b, Some(&secret_b)).0,
                204
            );
            assert_eq!(a("PUT", "/grab", "", b"").0, 200);
        },
    );
    assert_eq!(status, 403, "{body}");
    assert_eq!(a("GET", "/grab/evil", "", b"").0, 404);

    // An account dropped while its request is in flight creates nothing.
    let (account_c, key_c, secret_c) = account_with_key(&server, "sea");
    let config = b"<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>";
    let (_, (status, _)) = send_paused(
        port,
        "PUT",
        "/orphan",
        "",
        &[],
        config,
        &key_c,
        Some(&secret_c),
        10,
        || {
            pause();
            native(server.native_port, &format!("s3 drop access key {key_c}"));
            native(server.native_port, &format!("s3 drop account {account_c}"));
        },
    );
    assert_eq!(status, 403);
    assert_eq!(a("PUT", "/orphan", "", b"").0, 200, "the name is free");
}

/// A user of `account` with an access key, and (when given) a policy.
fn user_with_key(
    server: &Server,
    account: &str,
    name: &str,
    policy: Option<&str>,
) -> (String, String) {
    native(
        server.native_port,
        &format!("s3 create user {account} {name}"),
    );
    if let Some(policy) = policy {
        let reply = native(
            server.native_port,
            &format!("s3 put user policy {account} {name} p {policy}"),
        );
        assert_eq!(reply["policy_name"], "p", "{reply}");
    }
    let key = native(
        server.native_port,
        &format!("s3 create access key {account} {name}"),
    );
    (
        key["access_key_id"].as_str().unwrap().to_string(),
        key["secret_access_key"].as_str().unwrap().to_string(),
    )
}

#[test]
fn a_user_without_policies_is_refused_every_operation() {
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "acme");
    let root = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(
            port,
            method,
            path,
            query,
            body,
            &root_key,
            Some(&root_secret),
        )
    };
    assert_eq!(root("PUT", "/cov", "", b"").0, 200);
    assert_eq!(root("PUT", "/cov/o", "", b"data").0, 200);
    let (_, upload) = root("POST", "/cov/up", "uploads", b"");
    let upload_id = upload
        .split("<UploadId>")
        .nth(1)
        .and_then(|rest| rest.split('<').next())
        .expect("an upload id")
        .to_string();
    let (key, secret) = user_with_key(&server, &account, "nobody", None);
    let delete_body = b"<Delete><Object><Key>o</Key></Object></Delete>";
    let md5 = {
        use base64::Engine as _;
        use md5::Digest;
        base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(delete_body))
    };
    let tagging = b"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>";
    // Well-formed lock documents: these are authorized once their body is
    // read.
    let retention = b"<Retention/>";
    let retention_md5 = content_md5(retention);
    let retention_headers = [("content-md5", retention_md5.as_str())];
    let legal_hold = b"<LegalHold><Status>ON</Status></LegalHold>";
    let legal_hold_md5 = content_md5(legal_hold);
    let legal_hold_headers = [("content-md5", legal_hold_md5.as_str())];
    let upload_part = format!("uploadId={upload_id}&partNumber=1");
    let upload_query = format!("uploadId={upload_id}");
    // Method, path, query, body, extra headers.
    type Probe<'a> = (
        &'a str,
        &'a str,
        &'a str,
        &'a [u8],
        &'a [(&'a str, &'a str)],
    );
    let requests: Vec<Probe<'_>> = vec![
        ("GET", "/", "", b"", &[]),
        ("PUT", "/cov2", "", b"", &[]),
        ("DELETE", "/cov", "", b"", &[]),
        ("HEAD", "/cov", "", b"", &[]),
        ("GET", "/cov", "list-type=2", b"", &[]),
        ("GET", "/cov", "versions", b"", &[]),
        ("GET", "/cov", "location", b"", &[]),
        ("GET", "/cov", "versioning", b"", &[]),
        (
            "PUT",
            "/cov",
            "versioning",
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
            &[],
        ),
        ("GET", "/cov", "policy", b"", &[]),
        ("PUT", "/cov", "policy", b"{}", &[]),
        ("DELETE", "/cov", "policy", b"", &[]),
        ("GET", "/cov", "policyStatus", b"", &[]),
        ("GET", "/cov", "encryption", b"", &[]),
        (
            "PUT",
            "/cov",
            "encryption",
            b"<ServerSideEncryptionConfiguration/>",
            &[],
        ),
        ("DELETE", "/cov", "encryption", b"", &[]),
        ("GET", "/cov", "acl", b"", &[]),
        ("PUT", "/cov", "acl", b"", &[]),
        ("GET", "/cov", "ownershipControls", b"", &[]),
        ("PUT", "/cov", "ownershipControls", b"", &[]),
        ("GET", "/cov", "uploads", b"", &[]),
        ("GET", "/cov", "object-lock", b"", &[]),
        ("GET", "/cov", "lifecycle", b"", &[]),
        (
            "PUT",
            "/cov",
            "lifecycle",
            b"<LifecycleConfiguration/>",
            &[],
        ),
        ("DELETE", "/cov", "lifecycle", b"", &[]),
        ("GET", "/cov", "cors", b"", &[]),
        ("PUT", "/cov", "cors", b"<CORSConfiguration/>", &[]),
        ("DELETE", "/cov", "cors", b"", &[]),
        ("GET", "/cov", "metrics", b"", &[]),
        ("GET", "/cov", "metrics&id=a", b"", &[]),
        (
            "PUT",
            "/cov",
            "metrics&id=a",
            b"<MetricsConfiguration><Id>a</Id></MetricsConfiguration>",
            &[],
        ),
        ("DELETE", "/cov", "metrics&id=a", b"", &[]),
        ("GET", "/cov", "logging", b"", &[]),
        ("PUT", "/cov", "logging", b"<BucketLoggingStatus/>", &[]),
        ("GET", "/cov", "website", b"", &[]),
        (
            "PUT",
            "/cov",
            "website",
            b"<WebsiteConfiguration><IndexDocument><Suffix>i</Suffix></IndexDocument></WebsiteConfiguration>",
            &[],
        ),
        ("DELETE", "/cov", "website", b"", &[]),
        ("GET", "/cov", "notification", b"", &[]),
        (
            "PUT",
            "/cov",
            "notification",
            b"<NotificationConfiguration/>",
            &[],
        ),
        ("PUT", "/cov", "object-lock", b"", &[]),
        ("GET", "/cov/o", "retention", b"", &[]),
        ("PUT", "/cov/o", "retention", retention, &retention_headers),
        ("GET", "/cov/o", "legal-hold", b"", &[]),
        (
            "PUT",
            "/cov/o",
            "legal-hold",
            legal_hold,
            &legal_hold_headers,
        ),
        ("PUT", "/cov/o", "", b"x", &[]),
        ("GET", "/cov/o", "", b"", &[]),
        ("HEAD", "/cov/o", "", b"", &[]),
        ("DELETE", "/cov/o", "", b"", &[]),
        ("GET", "/cov/o", "tagging", b"", &[]),
        ("PUT", "/cov/o", "tagging", tagging, &[]),
        ("DELETE", "/cov/o", "tagging", b"", &[]),
        ("GET", "/cov/o", "acl", b"", &[]),
        ("PUT", "/cov/o", "acl", b"", &[]),
        (
            "POST",
            "/cov/o",
            "restore",
            b"<RestoreRequest><Days>1</Days></RestoreRequest>",
            &[],
        ),
        ("POST", "/cov/up", "uploads", b"", &[]),
        ("PUT", "/cov/up", &upload_part, b"part", &[]),
        ("GET", "/cov/up", &upload_query, b"", &[]),
        (
            "POST",
            "/cov/up",
            &upload_query,
            b"<CompleteMultipartUpload></CompleteMultipartUpload>",
            &[],
        ),
        ("DELETE", "/cov/up", &upload_query, b"", &[]),
        (
            "PUT",
            "/cov/copy",
            "",
            b"",
            &[("x-amz-copy-source", "/cov/o")],
        ),
        (
            "PUT",
            "/cov/up",
            &upload_part,
            b"",
            &[("x-amz-copy-source", "/cov/o")],
        ),
    ];
    for (method, path, query, body, extra) in requests {
        let (_, (status, reply)) =
            send_with(port, method, path, query, extra, body, &key, Some(&secret));
        assert_eq!(status, 403, "{method} {path}?{query}: {reply}");
    }
    // DeleteObjects reports each key's denial.
    let (_, (status, reply)) = send_with(
        port,
        "POST",
        "/cov",
        "delete",
        &[("content-md5", &md5)],
        delete_body,
        &key,
        Some(&secret),
    );
    assert_eq!(status, 200);
    assert!(reply.contains("<Code>AccessDenied</Code>"), "{reply}");
    assert_eq!(root("GET", "/cov/o", "", b"").1, "data", "nothing changed");
}

#[test]
fn bucket_and_user_policies_grant_and_deny_as_aws_does() {
    let server = start_server();
    let port = server.s3_port;
    let (owner, owner_key, owner_secret) = account_with_key(&server, "owner");
    let (other, other_key, other_secret) = account_with_key(&server, "other");
    let as_owner = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(
            port,
            method,
            path,
            query,
            body,
            &owner_key,
            Some(&owner_secret),
        )
    };
    assert_eq!(as_owner("PUT", "/shared", "", b"").0, 200);
    for key in ["public/a", "private/b", "home/alice/c", "home/bob/d"] {
        assert_eq!(as_owner("PUT", &format!("/shared/{key}"), "", b"x").0, 200);
    }
    // A malformed policy is refused; a valid one kept and returned.
    let (status, reply) = as_owner("PUT", "/shared", "policy", b"{\"Statement\": []}");
    assert_eq!(status, 400);
    assert!(reply.contains("MalformedPolicy"), "{reply}");
    let policy = format!(
        r#"{{"Version": "2012-10-17", "Statement": [
          {{"Effect": "Allow", "Principal": {{"AWS": "{other}"}}, "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::shared/public/*"}},
          {{"Effect": "Deny", "Principal": {{"AWS": "arn:aws:iam::{other}:user/mallory"}},
            "Action": "s3:*", "Resource": "arn:aws:s3:::shared/*"}}
        ]}}"#
    );
    assert_eq!(
        as_owner("PUT", "/shared", "policy", policy.as_bytes()).0,
        204
    );
    assert_eq!(
        as_owner("GET", "/shared", "policy", b""),
        (200, policy.clone())
    );
    assert!(
        as_owner("GET", "/shared", "policyStatus", b"")
            .1
            .contains("<IsPublic>false</IsPublic>")
    );

    // The other account's root reads public/ only; its user needs its own
    // policy too; an explicit deny wins.
    let other_root =
        |path: &str| send(port, "GET", path, "", b"", &other_key, Some(&other_secret)).0;
    assert_eq!(other_root("/shared/public/a"), 200);
    assert_eq!(other_root("/shared/private/b"), 403);
    let read_all = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}}"#;
    let (bob_key, bob_secret) = user_with_key(&server, &other, "bob", None);
    let (eve_key, eve_secret) = user_with_key(&server, &other, "eve", Some(read_all));
    let (mal_key, mal_secret) = user_with_key(&server, &other, "mallory", Some(read_all));
    let get =
        |key: &str, secret: &str, path: &str| send(port, "GET", path, "", b"", key, Some(secret)).0;
    assert_eq!(get(&bob_key, &bob_secret, "/shared/public/a"), 403);
    assert_eq!(get(&eve_key, &eve_secret, "/shared/public/a"), 200);
    assert_eq!(get(&eve_key, &eve_secret, "/shared/private/b"), 403);
    assert_eq!(get(&mal_key, &mal_secret, "/shared/public/a"), 403);

    // In the owner's account, a user policy with ${aws:username} scopes each
    // user to its home.
    let home = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
         "Resource": "arn:aws:s3:::shared/home/${aws:username}/*"},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::shared",
         "Condition": {"StringLike": {"s3:prefix": "home/${aws:username}/*"}}}]}"#;
    let (alice_key, alice_secret) = user_with_key(&server, &owner, "alice", Some(home));
    let alice = |method: &str, path: &str, query: &str| {
        send(
            port,
            method,
            path,
            query,
            b"y",
            &alice_key,
            Some(&alice_secret),
        )
        .0
    };
    assert_eq!(alice("GET", "/shared/home/alice/c", ""), 200);
    assert_eq!(alice("PUT", "/shared/home/alice/new", ""), 200);
    assert_eq!(alice("GET", "/shared/home/bob/d", ""), 403);
    assert_eq!(
        alice("GET", "/shared", "list-type=2&prefix=home/alice/"),
        200
    );
    assert_eq!(alice("GET", "/shared", "list-type=2&prefix=home/bob/"), 403);
    assert_eq!(alice("GET", "/shared", "list-type=2"), 403);

    // A policy that denies everyone everything locks the owner out of the
    // objects, but never out of the policy itself.
    let lockout = r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:*",
        "Resource": ["arn:aws:s3:::shared", "arn:aws:s3:::shared/*"]}}"#;
    assert_eq!(
        as_owner("PUT", "/shared", "policy", lockout.as_bytes()).0,
        204
    );
    assert_eq!(as_owner("GET", "/shared/public/a", "", b"").0, 403);
    assert_eq!(as_owner("DELETE", "/shared", "policy", b"").0, 204);
    assert_eq!(as_owner("GET", "/shared/public/a", "", b"").0, 200);
    assert_eq!(as_owner("GET", "/shared", "policy", b"").0, 404);

    // A grant to anyone makes the policy public; Block Public Access (on
    // for a new bucket) must be off to allow one.
    let public = r#"{"Statement": {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::shared/public/*"}}"#;
    assert_eq!(
        as_owner("PUT", "/shared", "policy", public.as_bytes()).0,
        403
    );
    let open = b"<PublicAccessBlockConfiguration><BlockPublicPolicy>false</BlockPublicPolicy></PublicAccessBlockConfiguration>";
    assert_eq!(as_owner("PUT", "/shared", "publicAccessBlock", open).0, 200);
    assert_eq!(
        as_owner("PUT", "/shared", "policy", public.as_bytes()).0,
        204
    );
    assert!(
        as_owner("GET", "/shared", "policyStatus", b"")
            .1
            .contains("<IsPublic>true</IsPublic>")
    );
    assert_eq!(
        get(&bob_key, &bob_secret, "/shared/public/a"),
        403,
        "still needs its own policy"
    );
    assert_eq!(other_root("/shared/public/a"), 200);

    // A condition on the transport: this listener is plain HTTP.
    let tls_only = r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::shared/*", "Condition": {"Bool": {"aws:SecureTransport": "false"}}}}"#;
    assert_eq!(
        as_owner("PUT", "/shared", "policy", tls_only.as_bytes()).0,
        204
    );
    assert_eq!(as_owner("GET", "/shared/public/a", "", b"").0, 403);
}

#[test]
fn conditions_see_what_the_operation_acts_on() {
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "acme");
    let root = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            path,
            query,
            extra,
            body,
            &root_key,
            Some(&root_secret),
        )
        .1
    };
    assert_eq!(root("PUT", "/tgs", "", &[], b"").0, 200);
    assert_eq!(
        root(
            "PUT",
            "/tgs/secret",
            "",
            &[("x-amz-tagging", "class=secret")],
            b"data"
        )
        .0,
        200
    );
    // A copy reads its source under the source's own tags.
    let no_secrets = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:GetObject", "Resource": "*",
         "Condition": {"StringEquals": {"s3:ExistingObjectTag/class": "secret"}}}]}"#;
    let (key, secret) = user_with_key(&server, &account, "reader", Some(no_secrets));
    let user = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    assert_eq!(user("GET", "/tgs/secret", "", &[], b"").0, 403);
    assert_eq!(
        user(
            "PUT",
            "/tgs/copy",
            "",
            &[
                ("x-amz-copy-source", "/tgs/secret"),
                ("x-amz-tagging-directive", "REPLACE")
            ],
            b""
        )
        .0,
        403
    );
    assert_eq!(root("GET", "/tgs/copy", "", &[], b"").0, 404);

    // Request tags are those the operation stores, and storing them needs
    // s3:PutObjectTagging.
    let public_tags_only = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:PutObjectTagging", "s3:PutObject"], "Resource": "*",
         "Condition": {"StringEquals": {"s3:RequestObjectTag/class": "public"}}},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectTagging"], "Resource": "*"}]}"#;
    let (key, secret) = user_with_key(&server, &account, "tagger", Some(public_tags_only));
    let tagger = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    let tagging =
        b"<Tagging><TagSet><Tag><Key>owner</Key><Value>mallory</Value></Tag></TagSet></Tagging>";
    assert_eq!(
        tagger(
            "PUT",
            "/tgs/secret",
            "tagging",
            &[("x-amz-tagging", "class=public")],
            tagging
        )
        .0,
        403,
        "the header's tags are not the body's"
    );
    assert_eq!(
        tagger(
            "PUT",
            "/tgs/dst",
            "",
            &[
                ("x-amz-copy-source", "/tgs/secret"),
                ("x-amz-tagging", "class=public")
            ],
            b""
        )
        .0,
        403,
        "a copy without REPLACE stores the source's tags, not the header's"
    );
    assert_eq!(
        tagger(
            "PUT",
            "/tgs/new",
            "",
            &[("x-amz-tagging", "class=public")],
            b"x"
        )
        .0,
        200
    );
    let no_tagging = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObjectTagging"], "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:PutObjectTagging", "Resource": "*"}]}"#;
    let (key, secret) = user_with_key(&server, &account, "untagged", Some(no_tagging));
    let untagged = |extra: &[(&str, &str)]| {
        send_with(port, "PUT", "/tgs/u", "", extra, b"x", &key, Some(&secret))
            .1
            .0
    };
    assert_eq!(untagged(&[]), 200);
    assert_eq!(untagged(&[("x-amz-tagging", "class=public")]), 403);

    // The version a condition sees is the version served.
    assert_eq!(root("PUT", "/vvv", "", &[], b"").0, 200);
    assert_eq!(
        root(
            "PUT",
            "/vvv",
            "versioning",
            &[],
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
        )
        .0,
        200
    );
    let version_of = |body: &[u8]| {
        let (head, (status, _)) = send_with(
            port,
            "PUT",
            "/vvv/o",
            "",
            &[],
            body,
            &root_key,
            Some(&root_secret),
        );
        assert_eq!(status, 200);
        response_header(&head, "x-amz-version-id").expect("a version")
    };
    let (v1, v2) = (version_of(b"public-version"), version_of(b"secret-version"));
    let one_version = format!(
        r#"{{"Version": "2012-10-17", "Statement": [
            {{"Effect": "Allow", "Action": "s3:PutObject", "Resource": "*"}},
            {{"Effect": "Allow", "Action": "s3:GetObjectVersion",
              "Resource": "*", "Condition": {{"StringEquals": {{"s3:VersionId": "{v1}"}}}}}}]}}"#
    );
    let (key, secret) = user_with_key(&server, &account, "versioned", Some(&one_version));
    let get = |query: &str| send(port, "GET", "/vvv/o", query, b"", &key, Some(&secret));
    assert_eq!(get(&format!("versionId={v1}")).0, 200);
    assert_eq!(get(&format!("versionId={v2}")).0, 403);
    let (status, body) = get(&format!("version%49d={v1}&versionId={v2}"));
    assert_ne!((status, body.as_str()), (200, "secret-version"));
    let deny_v2 = format!(
        r#"{{"Version": "2012-10-17", "Statement": [
            {{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}},
            {{"Effect": "Deny", "Action": "s3:GetObjectVersion",
              "Resource": "*", "Condition": {{"StringEquals": {{"s3:VersionId": "{v2}"}}}}}}]}}"#
    );
    let (deny_key, deny_secret) = user_with_key(&server, &account, "denied", Some(&deny_v2));
    let denied = |query: &str| {
        send(
            port,
            "GET",
            "/vvv/o",
            query,
            b"",
            &deny_key,
            Some(&deny_secret),
        )
        .0
    };
    assert_eq!(denied(&format!("versionId={v1}")), 200);
    assert_eq!(
        denied(&format!("versionId=00{v2}")),
        403,
        "a version in another spelling"
    );
    // A copy's source version is the copy source's, not the query's.
    let copy = |query: &str, source: &str| {
        send_with(
            port,
            "PUT",
            "/vvv/c",
            query,
            &[("x-amz-copy-source", source)],
            b"",
            &key,
            Some(&secret),
        )
        .1
        .0
    };
    assert_eq!(
        copy(
            &format!("versionId={v1}"),
            &format!("/vvv/o?versionId={v2}")
        ),
        403
    );

    // DeleteObjects reads a key's version as it deletes it.
    let deny_delete = format!(
        r#"{{"Version": "2012-10-17", "Statement": [
            {{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}},
            {{"Effect": "Deny", "Action": "s3:DeleteObjectVersion",
              "Resource": "*", "Condition": {{"StringEquals": {{"s3:VersionId": "{v1}"}}}}}},
            {{"Effect": "Deny", "Action": "s3:PutObjectTagging", "Resource": "*",
              "Condition": {{"StringEquals": {{"s3:ExistingObjectTag/locked": "true"}}}}}}]}}"#
    );
    let (del_key, del_secret) = user_with_key(&server, &account, "deleter", Some(&deny_delete));
    let body =
        format!("<Delete><Object><Key>o</Key><VersionId>00{v1}</VersionId></Object></Delete>");
    let md5 = {
        use base64::Engine as _;
        use md5::Digest;
        base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
    };
    let (_, (status, reply)) = send_with(
        port,
        "POST",
        "/vvv",
        "delete",
        &[("content-md5", &md5)],
        body.as_bytes(),
        &del_key,
        Some(&del_secret),
    );
    assert_eq!(status, 200);
    assert!(reply.contains("AccessDenied"), "{reply}");
    assert_eq!(
        send(
            port,
            "GET",
            "/vvv/o",
            &format!("versionId={v1}"),
            b"",
            &root_key,
            Some(&root_secret)
        )
        .0,
        200
    );
    // PutObjectTagging with a copy source header is not a copy: the target's
    // own tags hold.
    let locked =
        b"<Tagging><TagSet><Tag><Key>locked</Key><Value>true</Value></Tag></TagSet></Tagging>";
    assert_eq!(
        send(
            port,
            "PUT",
            "/vvv/o",
            "tagging",
            locked,
            &root_key,
            Some(&root_secret)
        )
        .0,
        200
    );
    let unlock =
        b"<Tagging><TagSet><Tag><Key>locked</Key><Value>false</Value></Tag></TagSet></Tagging>";
    let retag = |extra: &[(&str, &str)]| {
        send_with(
            port,
            "PUT",
            "/vvv/o",
            "tagging",
            extra,
            unlock,
            &del_key,
            Some(&del_secret),
        )
        .1
        .0
    };
    assert_eq!(retag(&[]), 403);
    assert_eq!(
        retag(&[("x-amz-copy-source", &format!("/vvv/o?versionId={v1}"))]),
        403
    );

    // An ACL body is read as XML: a spaced tag grants no more than a plain
    // one.
    let id = sigv4::sha256_hex(account.as_bytes());
    let acl = format!(
        "<AccessControlPolicy><AccessControlList><Grant><Grantee><ID>{id}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant ><Grantee><URI >http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission >READ</Permission></Grant></AccessControlList></AccessControlPolicy>"
    );
    assert_eq!(root("PUT", "/tgs", "acl", &[], acl.as_bytes()).0, 400);
    let own = format!(
        "<AccessControlPolicy><AccessControlList><Grant><Grantee><ID>{id}</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>"
    );
    assert_eq!(root("PUT", "/tgs", "acl", &[], own.as_bytes()).0, 200);
}

#[test]
fn anonymous_access_follows_bucket_policies_and_block_public_access() {
    let server = start_server();
    let port = server.s3_port;
    let (owner, owner_key, owner_secret) = account_with_key(&server, "owner");
    let (other, other_key, other_secret) = account_with_key(&server, "other");
    let as_owner = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            path,
            query,
            extra,
            body,
            &owner_key,
            Some(&owner_secret),
        )
        .1
    };
    let anonymous = |method: &str, path: &str| send(port, method, path, "", b"x", "", None).0;
    assert_eq!(as_owner("PUT", "/site", "", &[], b"").0, 200);
    for (key, tags) in [
        ("public/index.html", ""),
        ("private/key", ""),
        ("tagged", "public=true"),
        ("untagged", ""),
    ] {
        let extra: Vec<(&str, &str)> = if tags.is_empty() {
            vec![]
        } else {
            vec![("x-amz-tagging", tags)]
        };
        assert_eq!(
            as_owner("PUT", &format!("/site/{key}"), "", &extra, b"page").0,
            200
        );
    }
    // A new bucket blocks public access.
    let (status, settings) = as_owner("GET", "/site", "publicAccessBlock", &[], b"");
    assert_eq!(status, 200);
    for name in [
        "BlockPublicAcls",
        "IgnorePublicAcls",
        "BlockPublicPolicy",
        "RestrictPublicBuckets",
    ] {
        assert!(
            settings.contains(&format!("<{name}>true</{name}>")),
            "{settings}"
        );
    }
    let public = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/public/*"},
        {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/*",
         "Condition": {"StringEquals": {"s3:ExistingObjectTag/public": "true"}}},
        {"Effect": "Allow", "Principal": {"AWS": "OTHER"}, "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/private/*"}]}"#
        .replace("OTHER", &other);
    let (status, reply) = as_owner("PUT", "/site", "policy", &[], public.as_bytes());
    assert_eq!(status, 403, "BlockPublicPolicy: {reply}");
    let off = b"<PublicAccessBlockConfiguration/>";
    assert_eq!(
        as_owner("PUT", "/site", "publicAccessBlock", &[], off).0,
        200
    );
    assert_eq!(
        as_owner("PUT", "/site", "policy", &[], public.as_bytes()).0,
        204
    );

    // Anonymous reads: by prefix, by tag; nothing else.
    assert_eq!(anonymous("GET", "/site/public/index.html"), 200);
    assert_eq!(anonymous("GET", "/site/tagged"), 200);
    assert_eq!(anonymous("GET", "/site/untagged"), 403);
    assert_eq!(anonymous("GET", "/site/private/key"), 403);
    assert_eq!(anonymous("PUT", "/site/public/new"), 403);
    assert_eq!(anonymous("GET", "/"), 403);
    assert_eq!(anonymous("PUT", "/anon-bucket"), 403);
    let other_get =
        |path: &str| send(port, "GET", path, "", b"", &other_key, Some(&other_secret)).0;
    assert_eq!(other_get("/site/private/key"), 200);

    // RestrictPublicBuckets: with the policy public, only the owner's account
    // keeps access; the grant to the other account goes too.
    let restrict = b"<PublicAccessBlockConfiguration><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>";
    assert_eq!(
        as_owner("PUT", "/site", "publicAccessBlock", &[], restrict).0,
        200
    );
    assert_eq!(anonymous("GET", "/site/public/index.html"), 403);
    assert_eq!(other_get("/site/private/key"), 403);
    assert_eq!(as_owner("GET", "/site/private/key", "", &[], b"").0, 200);
    assert_eq!(
        as_owner("DELETE", "/site", "publicAccessBlock", &[], b"").0,
        204
    );
    assert_eq!(
        as_owner("GET", "/site", "publicAccessBlock", &[], b"").0,
        404
    );
    assert_eq!(anonymous("GET", "/site/public/index.html"), 200);

    // The account's settings apply to all its buckets (S3 Control's API).
    let control = |method: &str, account: &str, body: &[u8]| {
        send_with(
            port,
            method,
            "/v20180820/configuration/publicAccessBlock",
            "",
            &[("x-amz-account-id", account)],
            body,
            &owner_key,
            Some(&owner_secret),
        )
        .1
    };
    assert_eq!(control("GET", &owner, b"").0, 404);
    assert_eq!(control("PUT", &owner, restrict).0, 200);
    assert!(
        control("GET", &owner, b"")
            .1
            .contains("<RestrictPublicBuckets>true</RestrictPublicBuckets>")
    );
    assert_eq!(anonymous("GET", "/site/public/index.html"), 403);
    assert_eq!(
        control("PUT", &other, restrict).0,
        403,
        "another account's settings"
    );
    assert_eq!(control("DELETE", &owner, b"").0, 204);
    assert_eq!(anonymous("GET", "/site/public/index.html"), 200);
    // And the admin command sets them too.
    native(
        server.native_port,
        &format!("s3 put public access block {owner} false false false true"),
    );
    assert_eq!(anonymous("GET", "/site/public/index.html"), 403);

    // A raw request, to send what a client library would not.
    let raw = |request: String| -> String {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.write_all(request.as_bytes()).expect("send");
        let mut out = Vec::new();
        stream.read_to_end(&mut out).expect("read");
        latin1(&out)
    };
    native(
        server.native_port,
        &format!("s3 put public access block {owner} false false false false"),
    );
    assert_eq!(as_owner("PUT", "/open", "", &[], b"").0, 200);
    assert_eq!(as_owner("PUT", "/open/k", "", &[], b"data").0, 200);
    assert_eq!(
        as_owner("PUT", "/open", "publicAccessBlock", &[], off).0,
        200
    );
    // An anonymous caller has no signature keys: a negated condition on them
    // holds for it.
    let signed_only = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::open/*"},
        {"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::open/*",
         "Condition": {"StringNotEquals": {"s3:signatureversion": "AWS4-HMAC-SHA256"}}}]}"#;
    assert_eq!(
        as_owner("PUT", "/open", "policy", &[], signed_only.as_bytes()).0,
        204
    );
    assert_eq!(anonymous("GET", "/open/k"), 403);
    // An anonymous body must match the hash it claims.
    let reply = raw(format!(
        "PUT /open/forged HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nx-amz-content-sha256: {}\r\nContent-Length: 8\r\nConnection: close\r\n\r\ntampered",
        "0".repeat(64)
    ));
    assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
    assert_eq!(as_owner("GET", "/open/forged", "", &[], b"").0, 404);
    // An unreadable Authorization header, or a signature parameter in another
    // spelling, does not make a request anonymous.
    let reply = raw(format!(
        "GET /open/k HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: AWS4-HMAC-SHA256 Credential=\u{e9}\r\nConnection: close\r\n\r\n"
    ));
    assert!(!reply.starts_with("HTTP/1.1 200"), "{reply}");
    assert_eq!(
        send(
            port,
            "GET",
            "/site/public/index.html",
            "x-amz-algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=00",
            b"",
            "",
            None
        )
        .0,
        400
    );
    // A grant pinned to the anonymous caller's own userid is public.
    let userid = r#"{"Statement": {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::open/*", "Condition": {"StringEquals": {"aws:userid": "anonymous"}}}}"#;
    let block_policy = b"<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>";
    assert_eq!(
        as_owner("PUT", "/open", "publicAccessBlock", &[], block_policy).0,
        200
    );
    assert_eq!(
        as_owner("PUT", "/open", "policy", &[], userid.as_bytes()).0,
        403
    );
    // Under RestrictPublicBuckets the public grants count for the owner's
    // users too: they need their own policies.
    assert_eq!(
        as_owner("PUT", "/open", "publicAccessBlock", &[], off).0,
        200
    );
    assert_eq!(
        as_owner("PUT", "/open", "policy", &[], userid.as_bytes()).0,
        204
    );
    assert!(
        as_owner("GET", "/open", "policyStatus", &[], b"")
            .1
            .contains("<IsPublic>true</IsPublic>")
    );
    assert_eq!(
        as_owner("PUT", "/open", "publicAccessBlock", &[], restrict).0,
        200
    );
    let (user_key, user_secret) = user_with_key(&server, &owner, "staff", None);
    assert_eq!(
        send(
            port,
            "GET",
            "/open/k",
            "",
            b"",
            &user_key,
            Some(&user_secret)
        )
        .0,
        403
    );
    assert_eq!(anonymous("GET", "/open/k"), 403);
}

/// A presigned URL's path and query for `method` on `path`: SigV4 in the
/// query string, as the SDKs' presigners build it, signed at `amz_date` for
/// `expires` seconds. `extra` are more (unsigned-by-name) query parameters.
#[allow(clippy::too_many_arguments)]
fn presign(
    port: u16,
    method: &str,
    path: &str,
    extra: &str,
    key: &str,
    secret: &str,
    amz_date: &str,
    expires: u64,
) -> String {
    presign_payload(
        port,
        method,
        path,
        extra,
        key,
        secret,
        amz_date,
        expires,
        sigv4::UNSIGNED_PAYLOAD,
        &[],
    )
}

/// [`presign`] signing `payload_hash` (a hash the URL fixes) and the
/// headers `signed` besides the host.
#[allow(clippy::too_many_arguments)]
fn presign_payload(
    port: u16,
    method: &str,
    path: &str,
    extra: &str,
    key: &str,
    secret: &str,
    amz_date: &str,
    expires: u64,
    payload_hash: &str,
    signed: &[(&str, &str)],
) -> String {
    let credential = sigv4::Credential {
        access_key: key.to_string(),
        date: amz_date[..8].to_string(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };
    let encode = |text: &str| {
        percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
    };
    let mut headers = vec![("host".to_string(), format!("127.0.0.1:{port}"))];
    headers.extend(
        signed
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let mut names: Vec<String> = headers.iter().map(|(name, _)| name.clone()).collect();
    names.sort();
    let mut query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={amz_date}&X-Amz-Expires={expires}&X-Amz-SignedHeaders={}",
        encode(&format!("{key}/{}", credential.scope())),
        encode(&names.join(";"))
    );
    if !extra.is_empty() {
        query = format!("{extra}&{query}");
    }
    let canonical =
        sigv4::canonical_request(method, path, &query, &headers, &names, payload_hash).unwrap();
    let signature = sigv4::sign(
        &sigv4::signing_key(secret, &credential),
        &sigv4::string_to_sign(amz_date, &credential.scope(), &canonical),
    );
    format!("{path}?{query}&X-Amz-Signature={signature}")
}

/// Sends a request to a presigned `target` (path and query), with no
/// Authorization header; returns the response head and the status and body.
fn send_presigned(port: u16, method: &str, target: &str, body: &[u8]) -> (String, (u16, String)) {
    send_presigned_with(port, method, target, &[], body)
}

/// [`send_presigned`] with extra (unsigned) headers.
fn send_presigned_with(
    port: u16,
    method: &str,
    target: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> (String, (u16, String)) {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let extra: String = extra
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("send");
    stream.write_all(body).expect("send body");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let response = latin1(&raw);
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .map(|(head, body)| (head.to_string(), body.to_string()))
        .unwrap_or_default();
    (head, (status, body))
}

#[test]
fn presigned_urls_grant_their_request_until_they_expire() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    assert_eq!(
        send(port, "PUT", "/shr", "", b"", &key, Some(&secret)).0,
        200
    );
    assert_eq!(
        send(
            port,
            "PUT",
            "/shr/doc.txt",
            "",
            b"shared",
            &key,
            Some(&secret)
        )
        .0,
        200
    );
    let now = amz_now();

    // A download, with response headers chosen by the URL.
    let url = presign(
        port,
        "GET",
        "/shr/doc.txt",
        "response-content-type=text%2Fplain&response-content-disposition=attachment",
        &key,
        &secret,
        &now,
        3600,
    );
    let (head, (status, body)) = send_presigned(port, "GET", &url, b"");
    assert_eq!((status, body.as_str()), (200, "shared"), "{head}");
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        response_header(&head, "content-disposition").as_deref(),
        Some("attachment")
    );
    // An upload.
    let url = presign(
        port,
        "PUT",
        "/shr/upload.bin",
        "",
        &key,
        &secret,
        &now,
        3600,
    );
    assert_eq!(send_presigned(port, "PUT", &url, b"uploaded").1.0, 200);
    assert_eq!(
        get(port, "/shr/upload.bin", &key, Some(&secret)),
        (200, "uploaded".to_string())
    );

    // The URL is for its request only: another method, path or parameter
    // does not verify.
    let url = presign(port, "GET", "/shr/doc.txt", "", &key, &secret, &now, 3600);
    assert_eq!(send_presigned(port, "DELETE", &url, b"").1.0, 403);
    assert_eq!(
        send_presigned(port, "GET", &url.replace("doc.txt", "upload.bin"), b"")
            .1
            .0,
        403
    );
    assert_eq!(
        send_presigned(port, "GET", &format!("{url}&response-content-type=x"), b"")
            .1
            .0,
        403
    );

    // A parameter name written encoded is refused, rather than signed as one
    // operation and served as another (`%74agging` is `tagging` signed).
    let url = presign(
        port,
        "DELETE",
        "/shr/doc.txt",
        "tagging=",
        &key,
        &secret,
        &now,
        3600,
    );
    let (status, _) = send_presigned(
        port,
        "DELETE",
        &url.replacen("tagging=", "%74agging=", 1),
        b"",
    )
    .1;
    assert_eq!(status, 400);
    assert_eq!(
        get(port, "/shr/doc.txt", &key, Some(&secret)).0,
        200,
        "not deleted"
    );
    // An override must be a valid header value.
    let url = presign(
        port,
        "GET",
        "/shr/doc.txt",
        "response-content-type=a%0D%0AX-Injected%3A%201",
        &key,
        &secret,
        &now,
        3600,
    );
    let (status, body) = send_presigned(port, "GET", &url, b"").1;
    assert_eq!(status, 400, "{body}");
    // A payload hash the URL fixes must be one.
    let url = presign(
        port,
        "PUT",
        "/shr/x",
        "X-Amz-Content-Sha256=STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        &key,
        &secret,
        &now,
        3600,
    );
    assert_eq!(send_presigned(port, "PUT", &url, b"x").1.0, 400);

    // Time bounds: expired, not yet valid, longer than seven days.
    let epoch = sigv4_date;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let old = epoch(now_secs - 7200);
    let url = presign(port, "GET", "/shr/doc.txt", "", &key, &secret, &old, 3600);
    let (status, body) = send_presigned(port, "GET", &url, b"").1;
    assert_eq!(status, 403);
    assert!(body.contains("expired"), "{body}");
    let ahead = epoch(now_secs + 3600);
    let url = presign(port, "GET", "/shr/doc.txt", "", &key, &secret, &ahead, 3600);
    assert_eq!(send_presigned(port, "GET", &url, b"").1.0, 403);
    let url = presign(
        port,
        "GET",
        "/shr/doc.txt",
        "",
        &key,
        &secret,
        &now,
        604_801,
    );
    assert_eq!(send_presigned(port, "GET", &url, b"").1.0, 400);

    // A policy can refuse presigned access; header-signed requests go on.
    let (account, owner_key, owner_secret) = account_with_key(&server, "acme");
    assert_eq!(
        send(
            port,
            "PUT",
            "/acme-docs",
            "",
            b"",
            &owner_key,
            Some(&owner_secret)
        )
        .0,
        200
    );
    assert_eq!(
        send(
            port,
            "PUT",
            "/acme-docs/d",
            "",
            b"x",
            &owner_key,
            Some(&owner_secret)
        )
        .0,
        200
    );
    let no_urls = r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": "arn:aws:s3:::acme-docs/*",
        "Condition": {"StringEquals": {"s3:authType": "REST-QUERY-STRING"}}}}"#;
    assert_eq!(
        send(
            port,
            "PUT",
            "/acme-docs",
            "policy",
            no_urls.as_bytes(),
            &owner_key,
            Some(&owner_secret)
        )
        .0,
        204
    );
    let url = presign(
        port,
        "GET",
        "/acme-docs/d",
        "",
        &owner_key,
        &owner_secret,
        &now,
        600,
    );
    assert_eq!(send_presigned(port, "GET", &url, b"").1.0, 403);
    assert_eq!(
        get(port, "/acme-docs/d", &owner_key, Some(&owner_secret)).0,
        200
    );

    // Dropping the key revokes its URLs.
    let url = presign(port, "GET", "/shr/doc.txt", "", &key, &secret, &now, 3600);
    assert_eq!(send_presigned(port, "GET", &url, b"").1.0, 200);
    native(server.native_port, &format!("s3 drop access key {key}"));
    assert_eq!(send_presigned(port, "GET", &url, b"").1.0, 403);
    // And an anonymous request may not choose response headers, even for
    // an object it may read.
    let _ = account;
    let open = b"<PublicAccessBlockConfiguration/>";
    let public = r#"{"Statement": {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::acme-docs/*"}}"#;
    assert_eq!(
        send(
            port,
            "PUT",
            "/acme-docs",
            "publicAccessBlock",
            open,
            &owner_key,
            Some(&owner_secret)
        )
        .0,
        200
    );
    assert_eq!(
        send(
            port,
            "PUT",
            "/acme-docs",
            "policy",
            public.as_bytes(),
            &owner_key,
            Some(&owner_secret)
        )
        .0,
        204
    );
    assert_eq!(get(port, "/acme-docs/d", "", None).0, 200);
    assert_eq!(
        get(port, "/acme-docs/d?response-content-type=x", "", None).0,
        400
    );
}

/// A browser form's body: `fields`, then the file part (sent in pieces of
/// `piece` bytes, so the server sees the file across many reads).
fn post_form(
    port: u16,
    bucket: &str,
    fields: &[(String, String)],
    filename: &str,
    file: &[u8],
    piece: usize,
) -> (String, (u16, String)) {
    let (boundary, body) = form_body(fields, filename, file);
    post_raw(port, bucket, boundary, &body, piece, &[])
}

/// Sends a form `body` (in pieces of `piece` bytes) with extra request
/// `headers`; returns the response head and the status and body.
fn post_raw(
    port: u16,
    bucket: &str,
    boundary: &str,
    body: &[u8],
    piece: usize,
    headers: &[(&str, &str)],
) -> (String, (u16, String)) {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.set_nodelay(true).expect("nodelay");
    let mut head = format!(
        "POST /{bucket} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: multipart/form-data; boundary={boundary}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("send");
    for chunk in body.chunks(piece) {
        // The server may answer (and close) before the whole body is sent.
        if stream.write_all(chunk).is_err() {
            break;
        }
        stream.flush().expect("flush");
    }
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let response = latin1(&raw);
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .map(|(head, body)| (head.to_string(), body.to_string()))
        .unwrap_or_default();
    (head, (status, body))
}

/// The fields of a form signed with `secret`, as the SDKs' presigned POST
/// builds them: `fields`, then the policy (`conditions` plus the signature
/// fields' own) expiring at `expiration`, and its signature.
fn signed_form(
    key: &str,
    secret: &str,
    expiration: &str,
    conditions: &[serde_json::Value],
    fields: &[(&str, &str)],
) -> Vec<(String, String)> {
    use base64::Engine as _;
    let amz_date = amz_now();
    let credential = sigv4::Credential {
        access_key: key.to_string(),
        date: amz_date[..8].to_string(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };
    let credential_field = format!("{key}/{}", credential.scope());
    let mut all: Vec<serde_json::Value> = conditions.to_vec();
    all.push(serde_json::json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}));
    all.push(serde_json::json!({"x-amz-credential": credential_field}));
    all.push(serde_json::json!({"x-amz-date": amz_date}));
    let policy = serde_json::json!({"expiration": expiration, "conditions": all}).to_string();
    let policy = base64::engine::general_purpose::STANDARD.encode(policy);
    let signature = sigv4::sign(&sigv4::signing_key(secret, &credential), &policy);
    let mut out: Vec<(String, String)> = fields
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    out.extend([
        (
            "x-amz-algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        ("x-amz-credential".to_string(), credential_field),
        ("x-amz-date".to_string(), amz_date),
        ("policy".to_string(), policy),
        ("x-amz-signature".to_string(), signature),
    ]);
    out
}

#[test]
fn form_uploads_store_objects_within_their_policy() {
    use serde_json::json;
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let as_owner = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(port, method, path, query, body, &key, Some(&secret))
    };
    assert_eq!(as_owner("PUT", "/forms", "", b"").0, 200);
    let later = "2099-01-01T00:00:00.000Z";
    let conditions = [
        json!({"bucket": "forms"}),
        json!(["starts-with", "$key", "up/"]),
        json!(["content-length-range", 1, 100]),
        json!(["starts-with", "$Content-Type", "text/"]),
        json!({"x-amz-meta-note": "hi"}),
        json!({"success_action_status": "201"}),
        json!({"tagging": "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"}),
    ];
    let fields = [
        ("key", "up/${filename}"),
        ("Content-Type", "text/plain"),
        ("x-amz-meta-note", "hi"),
        ("success_action_status", "201"),
        (
            "tagging",
            "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>",
        ),
    ];
    // The file holds what looks like the start of a delimiter, and arrives
    // three bytes at a time.
    let file = b"line one\r\n------truthdb\r\n--\r\nend";
    let form = signed_form(&key, &secret, later, &conditions, &fields);
    let (head, (status, reply)) = post_form(port, "forms", &form, "a.txt", file, 3);
    assert_eq!(status, 201, "{reply}");
    assert!(reply.contains("<Key>up/a.txt</Key>"), "{reply}");
    assert!(reply.contains("<Bucket>forms</Bucket>"), "{reply}");
    let etag = response_header(&head, "etag").expect("an ETag");
    assert_eq!(etag, format!("\"{}\"", md5_hex(file)));
    let (head, (status, body)) = send_with(
        port,
        "GET",
        "/forms/up/a.txt",
        "",
        &[],
        b"",
        &key,
        Some(&secret),
    );
    assert_eq!((status, body.as_bytes()), (200, file.as_slice()));
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        response_header(&head, "x-amz-meta-note").as_deref(),
        Some("hi")
    );
    let (_, tags) = as_owner("GET", "/forms/up/a.txt", "tagging", b"");
    assert!(tags.contains("<Key>k</Key><Value>v</Value>"), "{tags}");

    // Without success_action_status: 204; with a redirect: 303 to it.
    let plain = [
        json!({"bucket": "forms"}),
        json!(["starts-with", "$key", ""]),
    ];
    let form = signed_form(&key, &secret, later, &plain, &[("key", "b.txt")]);
    assert_eq!(post_form(port, "forms", &form, "x", b"b", 1000).1.0, 204);
    let redirect = [
        json!({"bucket": "forms"}),
        json!({"key": "c.txt"}),
        json!({"success_action_redirect": "https://example.com/done"}),
    ];
    let form = signed_form(
        &key,
        &secret,
        later,
        &redirect,
        &[
            ("key", "c.txt"),
            ("success_action_redirect", "https://example.com/done"),
        ],
    );
    let (head, (status, _)) = post_form(port, "forms", &form, "x", b"c", 1000);
    assert_eq!(status, 303);
    let location = response_header(&head, "location").expect("a location");
    assert!(
        location.starts_with("https://example.com/done?bucket=forms&key=c%2Etxt&etag="),
        "{location}"
    );

    // What the policy does not allow is refused, and nothing is stored.
    let refused = |form: Vec<(String, String)>, file: &[u8], code: &str, status: u16| {
        let (_, (got, reply)) = post_form(port, "forms", &form, "a.txt", file, 1000);
        assert_eq!(got, status, "{code}: {reply}");
        assert!(reply.contains(code), "{code}: {reply}");
        assert_eq!(as_owner("GET", "/forms/up/refused", "", b"").0, 404);
    };
    let bounded = [
        json!({"bucket": "forms"}),
        json!({"key": "up/refused"}),
        json!(["content-length-range", 2, 5]),
    ];
    let key_only = [("key", "up/refused")];
    refused(
        signed_form(&key, &secret, later, &bounded, &key_only),
        b"too long",
        "EntityTooLarge",
        400,
    );
    refused(
        signed_form(&key, &secret, later, &bounded, &key_only),
        b"1",
        "EntityTooSmall",
        400,
    );
    let with_extra = [("key", "up/refused"), ("Content-Type", "text/html")];
    refused(
        signed_form(&key, &secret, later, &bounded, &with_extra),
        b"abc",
        "Extra input fields: content-type",
        403,
    );
    let prefixed = [
        json!({"bucket": "forms"}),
        json!(["starts-with", "$key", "elsewhere/"]),
    ];
    refused(
        signed_form(&key, &secret, later, &prefixed, &key_only),
        b"abc",
        "Policy Condition failed",
        403,
    );
    refused(
        signed_form(&key, &secret, "2001-01-01T00:00:00Z", &bounded, &key_only),
        b"abc",
        "Policy expired",
        403,
    );
    let mut forged = signed_form(&key, &secret, later, &bounded, &key_only);
    forged
        .iter_mut()
        .find(|(name, _)| name == "x-amz-signature")
        .unwrap()
        .1 = "0".repeat(64);
    refused(forged, b"abc", "SignatureDoesNotMatch", 403);
    let mut huge = signed_form(&key, &secret, later, &bounded, &key_only);
    huge.insert(0, ("x-ignore-pad".to_string(), "p".repeat(30 * 1024)));
    refused(huge, b"abc", "MaxPostPreDataLengthExceeded", 400);

    // An unsigned form is anonymous: refused until a bucket policy lets
    // anyone upload there.
    let anonymous = vec![("key".to_string(), "up/anon".to_string())];
    let (_, (status, reply)) = post_form(port, "forms", &anonymous, "a", b"anon", 1000);
    assert_eq!(status, 403, "{reply}");
    assert_eq!(
        as_owner(
            "PUT",
            "/forms",
            "publicAccessBlock",
            b"<PublicAccessBlockConfiguration/>"
        )
        .0,
        200
    );
    let public = r#"{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*",
        "Action": "s3:PutObject", "Resource": "arn:aws:s3:::forms/up/*"}]}"#;
    assert_eq!(
        as_owner("PUT", "/forms", "policy", public.as_bytes()).0,
        204
    );
    assert_eq!(
        post_form(port, "forms", &anonymous, "a", b"anon", 1000).1.0,
        204
    );
    assert_eq!(
        as_owner("GET", "/forms/up/anon", "", b""),
        (200, "anon".to_string())
    );
    let outside = vec![("key".to_string(), "down/anon".to_string())];
    assert_eq!(
        post_form(port, "forms", &outside, "a", b"anon", 1000).1.0,
        403
    );
}

#[test]
fn form_uploads_decide_each_field_once_and_parse_the_form_strictly() {
    use serde_json::json;
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let as_owner = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(port, method, path, query, body, &key, Some(&secret))
    };
    assert_eq!(as_owner("PUT", "/strict", "", b"").0, 200);
    let later = "2099-01-01T00:00:00.000Z";
    let absent = |path: &str| assert_eq!(as_owner("GET", path, "", b"").0, 404, "{path}");

    // A field given twice (in any case) is refused: the policy and the
    // stored object must see the same value.
    let conditions = [
        json!({"bucket": "strict"}),
        json!({"key": "twice"}),
        json!(["eq", "$Content-Type", "image/png"]),
    ];
    let mut form = signed_form(
        &key,
        &secret,
        later,
        &conditions,
        &[("key", "twice"), ("Content-Type", "image/png")],
    );
    form.push(("content-type".to_string(), "text/html".to_string()));
    let (_, (status, reply)) = post_form(port, "strict", &form, "a", b"<script>", 1000);
    assert_eq!(status, 400, "{reply}");
    absent("/strict/twice");

    // A redirect that is not a valid header value is refused before
    // anything is stored; a key with a control byte is answered with its
    // location encoded.
    let open = [
        json!({"bucket": "strict"}),
        json!(["starts-with", "$key", ""]),
        json!(["starts-with", "$success_action_redirect", "https://site/"]),
    ];
    let form = signed_form(
        &key,
        &secret,
        later,
        &open,
        &[
            ("key", "redirected"),
            (
                "success_action_redirect",
                "https://site/\r\nSet-Cookie: a=b",
            ),
        ],
    );
    let (_, (status, reply)) = post_form(port, "strict", &form, "a", b"x", 1000);
    assert_eq!(status, 400, "{reply}");
    absent("/strict/redirected");
    let keyed = [
        json!({"bucket": "strict"}),
        json!(["starts-with", "$key", ""]),
    ];
    let form = signed_form(&key, &secret, later, &keyed, &[("key", "ctl\u{1}key")]);
    let (head, (status, reply)) = post_form(port, "strict", &form, "a", b"x", 1000);
    assert_eq!(status, 204, "{reply}");
    assert_eq!(
        response_header(&head, "location").as_deref(),
        Some("/strict/ctl%01key")
    );

    // A form to a bucket that does not exist is refused before its file is
    // read (this form ends inside the file).
    let elsewhere = [
        json!({"bucket": "missing"}),
        json!(["starts-with", "$key", ""]),
    ];
    let form = signed_form(&key, &secret, later, &elsewhere, &[("key", "k")]);
    let boundary = "B0undary";
    let mut body = Vec::new();
    for (name, value) in &form {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\npartial")
            .as_bytes(),
    );
    let (_, (status, reply)) = post_raw(port, "missing", boundary, &body, 1000, &[]);
    assert_eq!(status, 404, "{reply}");
    assert!(reply.contains("NoSuchBucket"), "{reply}");

    // Quoted parameters may hold `;` and `=`; a boundary followed by
    // anything but CRLF or `--` is part of the file.
    let form = signed_form(&key, &secret, later, &keyed, &[("key", "k")]);
    let file = format!("one\r\n--{boundary}X two");
    let mut body = Vec::new();
    for (name, value) in &form {
        let value = if name == "key" {
            "up/${filename}"
        } else {
            value.as_str()
        };
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; filename=\"q;name=key\"; name=\"file\"\r\n\r\n{file}\r\n--{boundary}--\r\n").as_bytes(),
    );
    let (_, (status, reply)) = post_raw(port, "strict", boundary, &body, 7, &[]);
    assert_eq!(status, 204, "{reply}");
    assert_eq!(
        as_owner("GET", "/strict/up/q;name=key", "", b""),
        (200, file)
    );

    // The request's own headers are in the policy context.
    assert_eq!(
        as_owner(
            "PUT",
            "/strict",
            "publicAccessBlock",
            b"<PublicAccessBlockConfiguration/>"
        )
        .0,
        200
    );
    let public = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::strict/*"},
        {"Effect": "Deny", "Principal": "*", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::strict/*",
         "Condition": {"StringLike": {"aws:Referer": "*evil*"}}}]}"#;
    assert_eq!(
        as_owner("PUT", "/strict", "policy", public.as_bytes()).0,
        204
    );
    let anonymous = [("key".to_string(), "anon".to_string())];
    let mut body = Vec::new();
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nanon\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nx\r\n--{boundary}--\r\n").as_bytes(),
    );
    let referer = [("Referer", "https://evil.example/")];
    assert_eq!(
        post_raw(port, "strict", boundary, &body, 1000, &referer)
            .1
            .0,
        403
    );
    absent("/strict/anon");
    assert_eq!(
        post_form(port, "strict", &anonymous, "a", b"x", 1000).1.0,
        204
    );

    // Only what the form acts on reaches the policy: a copy source header
    // (which a form never copies from) does not, the acl field does.
    let conditional = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": "*", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::strict/copied/*",
         "Condition": {"StringEquals": {"s3:x-amz-copy-source": "strict/template"}}},
        {"Effect": "Allow", "Principal": "*", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::strict/owned/*",
         "Condition": {"StringEquals": {"s3:x-amz-acl": "bucket-owner-full-control"}}}]}"#;
    assert_eq!(
        as_owner("PUT", "/strict", "policy", conditional.as_bytes()).0,
        204
    );
    let form_of = |fields: &[(&str, &str)]| {
        let mut body = Vec::new();
        for (name, value) in fields {
            body.extend_from_slice(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes(),
            );
        }
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a\\b.txt\"\r\n\r\nx\r\n--{boundary}--\r\n").as_bytes(),
        );
        body
    };
    let copy_source = [("x-amz-copy-source", "strict/template")];
    let body = form_of(&[("key", "copied/evil")]);
    assert_eq!(
        post_raw(port, "strict", boundary, &body, 1000, &copy_source)
            .1
            .0,
        403
    );
    absent("/strict/copied/evil");
    let body = form_of(&[
        ("key", "owned/${filename}"),
        ("acl", "bucket-owner-full-control"),
    ]);
    let (_, (status, reply)) = post_raw(port, "strict", boundary, &body, 1000, &[]);
    assert_eq!(status, 204, "{reply}");
    // A file name is taken as the browser sends it: `\` is not an escape.
    assert_eq!(as_owner("GET", "/strict/owned/a%5Cb.txt", "", b"").0, 200);

    // A boundary is at most 70 characters (RFC 2046).
    let long = "b".repeat(71);
    let mut body = Vec::new();
    body.extend_from_slice(
        format!("--{long}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nk\r\n--{long}\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nx\r\n--{long}--\r\n").as_bytes(),
    );
    assert_eq!(post_raw(port, "strict", &long, &body, 1000, &[]).1.0, 400);
}

#[test]
fn acls_are_disabled_and_ownership_is_enforced() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    assert_eq!(call("PUT", "/acl", "", &[], b"").0, 200);
    assert_eq!(
        call("PUT", "/acl/o", "", &[("x-amz-acl", "private")], b"x").0,
        200
    );
    assert_eq!(
        call(
            "PUT",
            "/acl/o",
            "",
            &[("x-amz-acl", "bucket-owner-full-control")],
            b"x"
        )
        .0,
        200
    );
    let (status, reply) = call("PUT", "/acl/o", "", &[("x-amz-acl", "public-read")], b"x");
    assert_eq!(status, 400);
    assert!(reply.contains("AccessControlListNotSupported"), "{reply}");
    assert_eq!(
        call("PUT", "/acl/o", "", &[("x-amz-grant-read", "uri=x")], b"x").0,
        400
    );
    for path in ["/acl", "/acl/o"] {
        let (status, reply) = call("GET", path, "acl", &[], b"");
        assert_eq!(status, 200);
        assert!(
            reply.contains("<Permission>FULL_CONTROL</Permission>"),
            "{reply}"
        );
        assert!(
            reply.contains(&sigv4::sha256_hex(b"000000000000")),
            "{reply}"
        );
    }
    assert_eq!(
        call("PUT", "/acl", "acl", &[("x-amz-acl", "private")], b"").0,
        200
    );
    assert_eq!(
        call("PUT", "/acl", "acl", &[("x-amz-acl", "public-read")], b"").0,
        400
    );
    let (status, reply) = call("GET", "/acl", "ownershipControls", &[], b"");
    assert_eq!(status, 200);
    assert!(reply.contains("BucketOwnerEnforced"), "{reply}");
    let preferred = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>";
    assert_eq!(
        call("PUT", "/acl", "ownershipControls", &[], preferred).0,
        400
    );
}

#[test]
fn s3_vectors_operations_follow_the_user_policies() {
    use serde_json::json;
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "vec");
    let (none_key, none_secret) = user_with_key(&server, &account, "none", None);
    let all = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "s3vectors:*", "Resource": "*"}}"#;
    let (all_key, all_secret) = user_with_key(&server, &account, "all", Some(all));
    let (status, _, _) = vectors(
        port,
        &root_key,
        &root_secret,
        "CreateVectorBucket",
        json!({"vectorBucketName": "vbk"}),
    );
    assert_eq!(status, 200);
    for (operation, body) in [
        ("ListVectorBuckets", json!({})),
        ("GetVectorBucket", json!({"vectorBucketName": "vbk"})),
        (
            "CreateIndex",
            json!({"vectorBucketName": "vbk", "indexName": "idx", "dataType": "float32", "dimension": 2, "distanceMetric": "cosine"}),
        ),
    ] {
        let (status, kind, _) = vectors(port, &none_key, &none_secret, operation, body.clone());
        assert_eq!(
            (status, kind.as_deref()),
            (403, Some("AccessDeniedException")),
            "{operation}"
        );
        let (status, _, reply) = vectors(port, &all_key, &all_secret, operation, body);
        assert_eq!(status, 200, "{operation}: {reply}");
    }
}

#[test]
fn requests_are_authenticated_with_sigv4_against_native_access_keys() {
    let (server, key, secret) = server_with_key();

    // A correctly signed request passes authentication and reaches the
    // operation (here: a bucket that does not exist).
    let (status, body) = get(server.s3_port, "/bucket/key", &key, Some(&secret));
    assert_eq!(status, 404, "{body}");
    assert!(body.contains("<Code>NoSuchBucket</Code>"), "{body}");

    let (status, body) = get(server.s3_port, "/bucket/key", &key, Some("wrong-secret"));
    assert_eq!(status, 403, "{body}");
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{body}"
    );

    let (_, (status, body)) = send_with(
        server.s3_port,
        "GET",
        "/bucket/key",
        "",
        &[("unsigned-x-amz-meta-sneaky", "1")],
        b"",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 403, "{body}");
    assert!(
        body.contains("must be signed"),
        "an unsigned x-amz-* header: {body}"
    );

    let (status, body) = get(server.s3_port, "/bucket/key", "TDBNOSUCHKEY", Some(&secret));
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>InvalidAccessKeyId</Code>"), "{body}");

    // An unsigned request is anonymous: a missing bucket is 404, as in AWS;
    // a private one refuses it.
    let (status, body) = get(server.s3_port, "/bucket/key", &key, None);
    assert_eq!(status, 404, "{body}");
    assert_eq!(get(server.s3_port, "/bucket", &key, Some(&secret)).0, 404);
    assert_eq!(
        send(
            server.s3_port,
            "PUT",
            "/private",
            "",
            b"",
            &key,
            Some(&secret)
        )
        .0,
        200
    );
    let (status, body) = get(server.s3_port, "/private/key", &key, None);
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

#[test]
fn buckets_are_created_listed_located_headed_and_deleted() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str| {
        send(port, method, path, query, b"", &key, Some(&secret))
    };

    assert_eq!(call("PUT", "/alpha", "").0, 200);
    // us-east-1 answers success when the owner creates a bucket again.
    let (status, body) = call("PUT", "/alpha", "");
    assert_eq!(status, 200, "{body}");
    let (status, body) = send(
        port,
        "PUT",
        "/gamma",
        "",
        b"<CreateBucketConfiguration><LocationConstraint>",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("MalformedXML"), "{body}");
    let (status, body) = call("PUT", "/Not_Valid", "");
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("InvalidBucketName"), "{body}");
    assert_eq!(call("PUT", "/beta", "").0, 200);

    let (status, body) = call("GET", "/", "");
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("<Name>alpha</Name>") && body.contains("<Name>beta</Name>"),
        "{body}"
    );
    assert!(body.contains("<CreationDate>"), "{body}");

    assert_eq!(call("HEAD", "/alpha", "").0, 200);
    assert_eq!(call("HEAD", "/gamma", "").0, 404);
    let (status, body) = call("GET", "/alpha", "location");
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<LocationConstraint"), "{body}");

    assert_eq!(call("DELETE", "/beta", "").0, 204);
    let (status, body) = call("DELETE", "/beta", "");
    assert_eq!(status, 404, "{body}");
    assert!(body.contains("NoSuchBucket"), "{body}");
    let (_, body) = call("GET", "/", "");
    assert!(!body.contains("<Name>beta</Name>"), "{body}");
}

/// A response header's value (case-insensitive name).
fn response_header(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

#[test]
fn objects_are_put_read_by_range_and_conditionally_and_overwritten() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, "", extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/objs", &[], b"").1.0, 200);

    let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let (head, (status, body)) = call(
        "PUT",
        "/objs/dir/a.bin",
        &[("content-type", "text/x-test"), ("x-amz-meta-owner", "me")],
        &data,
    );
    assert_eq!(status, 200, "{body}");
    let etag = response_header(&head, "etag").expect("etag");
    assert_eq!(etag, format!("\"{}\"", md5_hex(&data)));

    let (head, (status, body)) = call("GET", "/objs/dir/a.bin", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(body.as_bytes(), latin1(&data).as_bytes());
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/x-test")
    );
    assert_eq!(
        response_header(&head, "x-amz-meta-owner").as_deref(),
        Some("me")
    );

    let (head, (status, _)) = call("HEAD", "/objs/dir/a.bin", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "content-length").as_deref(),
        Some("300000")
    );

    let (head, (status, body)) = call("GET", "/objs/dir/a.bin", &[("range", "bytes=10-19")], b"");
    assert_eq!(status, 206);
    assert_eq!(body.as_bytes(), latin1(&data[10..20]).as_bytes());
    assert_eq!(
        response_header(&head, "content-range").as_deref(),
        Some("bytes 10-19/300000")
    );
    assert_eq!(
        call("GET", "/objs/dir/a.bin", &[("range", "bytes=400000-")], b"")
            .1
            .0,
        416
    );
    assert_eq!(
        call("GET", "/objs/dir/a.bin", &[("if-none-match", &etag)], b"")
            .1
            .0,
        304
    );
    let (status, body) = call("GET", "/objs/dir/a.bin", &[("if-match", "\"nope\"")], b"").1;
    assert_eq!(status, 412, "{body}");
    assert!(body.contains("PreconditionFailed"), "{body}");

    assert_eq!(call("PUT", "/objs/dir/a.bin", &[], b"v2").1.0, 200);
    assert_eq!(call("GET", "/objs/dir/a.bin", &[], b"").1.1, "v2");
    let (status, body) = call("GET", "/objs/missing", &[], b"").1;
    assert_eq!(status, 404, "{body}");
    assert!(body.contains("NoSuchKey"), "{body}");
    let (status, body) = call(
        "PUT",
        "/objs/k",
        &[("content-md5", "AAAAAAAAAAAAAAAAAAAAAA==")],
        b"x",
    )
    .1;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("BadDigest"), "{body}");
    let (status, body) = call("PUT", "/objs/k", &[("content-md5", "not-a-digest")], b"x").1;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("InvalidDigest"), "{body}");
    // An attribute too long to store is refused with the header section.
    let long = "t".repeat(70_000);
    let (status, body) = call("PUT", "/objs/k", &[("content-type", &long)], b"x").1;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("RequestHeaderSectionTooLarge"), "{body}");
    assert_eq!(call("GET", "/objs/k", &[], b"").1.0, 404);
}

/// Bytes as the test's String bodies carry them (one char per byte).
fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect::<String>()
}

fn md5_hex(bytes: &[u8]) -> String {
    use md5::Digest;
    md5::Md5::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// PUTs `data` as a signed aws-chunked body (`chunk` bytes per chunk);
/// `tamper` flips a byte of the first chunk after signing.
fn put_chunked(
    port: u16,
    path: &str,
    data: &[u8],
    chunk: usize,
    key: &str,
    secret: &str,
    tamper: bool,
) -> (u16, String) {
    let amz_date = amz_now();
    let host = format!("127.0.0.1:{port}");
    let credential = sigv4::Credential {
        access_key: key.to_string(),
        date: amz_date[..8].to_string(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };
    let signing_key = sigv4::signing_key(secret, &credential);
    let scope = credential.scope();
    // Frame the body first: its length is a signed header.
    let pieces: Vec<&[u8]> = data.chunks(chunk).chain(std::iter::once(&[][..])).collect();
    let encoded_len: usize = pieces
        .iter()
        .map(|piece| format!("{:x};chunk-signature=", piece.len()).len() + 64 + 2 + piece.len() + 2)
        .sum();
    let decoded = data.len().to_string();
    let content_length = encoded_len.to_string();
    let headers = vec![
        ("content-encoding".to_string(), "aws-chunked".to_string()),
        ("content-length".to_string(), content_length.clone()),
        ("host".to_string(), host.clone()),
        (
            "x-amz-content-sha256".to_string(),
            sigv4::STREAMING_PAYLOAD.to_string(),
        ),
        ("x-amz-date".to_string(), amz_date.clone()),
        ("x-amz-decoded-content-length".to_string(), decoded.clone()),
    ];
    let signed: Vec<String> = headers.iter().map(|(name, _)| name.clone()).collect();
    let canonical =
        sigv4::canonical_request("PUT", path, "", &headers, &signed, sigv4::STREAMING_PAYLOAD)
            .unwrap();
    let seed = sigv4::sign(
        &signing_key,
        &sigv4::string_to_sign(&amz_date, &scope, &canonical),
    );
    let mut body = Vec::new();
    let mut previous = seed.clone();
    for piece in &pieces {
        let signature = sigv4::sign(
            &signing_key,
            &sigv4::chunk_string_to_sign(&amz_date, &scope, &previous, piece),
        );
        body.extend_from_slice(
            format!("{:x};chunk-signature={signature}\r\n", piece.len()).as_bytes(),
        );
        body.extend_from_slice(piece);
        body.extend_from_slice(b"\r\n");
        previous = signature;
    }
    assert_eq!(body.len(), encoded_len);
    if tamper {
        let at = body.iter().position(|&b| b == b'\n').unwrap() + 1;
        body[at] ^= 1;
    }
    let mut request = format!("PUT {path} HTTP/1.1\r\nConnection: close\r\n");
    for (name, value) in &headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!(
        "Authorization: AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={}, Signature={seed}\r\n\r\n",
        signed.join(";")
    ));
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.write_all(request.as_bytes()).expect("send");
    stream.write_all(&body).expect("send body");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let response = latin1(&raw);
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
fn a_signed_chunked_upload_is_verified_chunk_by_chunk() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    assert_eq!(
        send(port, "PUT", "/chunks", "", b"", &key, Some(&secret)).0,
        200
    );
    let data: Vec<u8> = (0..200_000u32).map(|i| (i % 249) as u8).collect();

    let (status, body) = put_chunked(port, "/chunks/obj", &data, 65_536, &key, &secret, false);
    assert_eq!(status, 200, "{body}");
    let (status, body) = send(port, "GET", "/chunks/obj", "", b"", &key, Some(&secret));
    assert_eq!(status, 200);
    assert_eq!(body, latin1(&data));

    let (status, body) = put_chunked(port, "/chunks/bad", &data, 65_536, &key, &secret, true);
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("SignatureDoesNotMatch"), "{body}");
    assert_eq!(
        send(port, "GET", "/chunks/bad", "", b"", &key, Some(&secret)).0,
        404,
        "a refused upload leaves no object"
    );
}

#[test]
fn listings_page_through_keys_and_common_prefixes() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(port, method, path, query, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/lst", "", b"").0, 200);
    for name in ["a", "a/b", "a/c", "b/x/1", "b/y", "c"] {
        assert_eq!(
            call("PUT", &format!("/lst/{name}"), "", name.as_bytes()).0,
            200
        );
    }
    // A sub-resource that is not implemented is refused, not answered with
    // a listing or an object body.
    for (path, query) in [
        ("/lst/a", "website"),
        ("/lst/a", "cors"),
        ("/lst/a", "logging"),
        ("/lst/a", "restore"),
    ] {
        let (status, body) = call("GET", path, query, b"");
        assert_eq!(status, 501, "{path}?{query}: {body}");
    }
    let keys_of = |xml: &str| -> Vec<String> {
        xml.split("<Key>")
            .skip(1)
            .map(|rest| rest.split("</Key>").next().unwrap().to_string())
            .collect()
    };
    let prefixes_of = |xml: &str| -> Vec<String> {
        xml.split("<CommonPrefixes><Prefix>")
            .skip(1)
            .map(|rest| rest.split("</Prefix>").next().unwrap().to_string())
            .collect()
    };

    let (status, xml) = call("GET", "/lst", "list-type=2", b"");
    assert_eq!(status, 200, "{xml}");
    assert_eq!(keys_of(&xml), ["a", "a/b", "a/c", "b/x/1", "b/y", "c"]);

    let (_, xml) = call("GET", "/lst", "delimiter=%2F&list-type=2", b"");
    assert_eq!(keys_of(&xml), ["a", "c"]);
    assert_eq!(prefixes_of(&xml), ["a/", "b/"]);

    // Pages of two, following NextContinuationToken.
    let mut seen = Vec::new();
    let mut query = "list-type=2&max-keys=2".to_string();
    loop {
        let (status, xml) = call("GET", "/lst", &query, b"");
        assert_eq!(status, 200, "{xml}");
        seen.extend(keys_of(&xml));
        let Some(next) = xml
            .split("<NextContinuationToken>")
            .nth(1)
            .map(|rest| rest.split('<').next().unwrap().to_string())
        else {
            assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
            break;
        };
        query = format!("continuation-token={next}&list-type=2&max-keys=2");
    }
    assert_eq!(seen, ["a", "a/b", "a/c", "b/x/1", "b/y", "c"]);

    let (_, xml) = call("GET", "/lst", "marker=b%2Fx%2F1", b"");
    assert_eq!(keys_of(&xml), ["b/y", "c"], "v1 resumes after its marker");
    let (status, xml) = call("GET", "/nope", "list-type=2", b"");
    assert_eq!(status, 404, "{xml}");
}

#[test]
fn objects_are_deleted_one_by_one_and_in_batches() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    assert_eq!(call("PUT", "/del", "", &[], b"").0, 200);
    for name in ["a", "b&c", "d"] {
        let path = format!("/del/{}", name.replace('&', "%26"));
        assert_eq!(call("PUT", &path, "", &[], b"x").0, 200);
    }
    assert_eq!(call("DELETE", "/del/a", "", &[], b"").0, 204);
    assert_eq!(
        call("DELETE", "/del/a", "", &[], b"").0,
        204,
        "deleting again succeeds"
    );
    assert_eq!(call("GET", "/del/a", "", &[], b"").0, 404);

    let batch =
        b"<Delete><Object><Key>b&amp;c</Key></Object><Object><Key>d</Key></Object></Delete>";
    let (status, body) = call("POST", "/del", "delete", &[], batch);
    assert_eq!(status, 400, "an integrity header is required: {body}");
    let (status, body) = call(
        "POST",
        "/del",
        "delete",
        &[("x-amz-checksum-algorithm", "CRC32")],
        batch,
    );
    assert_eq!(status, 400, "naming an algorithm is not a checksum: {body}");
    use base64::Engine as _;
    use md5::Digest;
    let digest = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(batch));
    let (status, body) = call(
        "POST",
        "/nobucket",
        "delete",
        &[("content-md5", &digest)],
        batch,
    );
    assert_eq!(status, 404, "a missing bucket fails the request: {body}");
    assert!(body.contains("NoSuchBucket"), "{body}");
    let (status, body) = call("POST", "/del", "delete", &[("content-md5", &digest)], batch);
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("<Deleted><Key>b&amp;c</Key></Deleted>") && body.contains("<Key>d</Key>"),
        "{body}"
    );
    assert_eq!(
        call("DELETE", "/del", "", &[], b"").0,
        204,
        "the bucket is empty"
    );
}

#[test]
fn a_multipart_upload_completes_lists_and_aborts() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(port, method, path, query, &[], body, &key, Some(&secret))
    };
    let tag_of = |xml: &str, tag: &str| -> String {
        xml.split(&format!("<{tag}>"))
            .nth(1)
            .and_then(|rest| rest.split(&format!("</{tag}>")).next())
            .unwrap_or_default()
            .to_string()
    };
    assert_eq!(call("PUT", "/mpu", "", b"").1.0, 200);
    let (_, (status, xml)) = call("POST", "/mpu/big.bin", "uploads", b"");
    assert_eq!(status, 200, "{xml}");
    let upload = tag_of(&xml, "UploadId");

    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = b"tail".to_vec();
    let mut etags = Vec::new();
    for (number, data) in [(1, &part1), (2, &part2)] {
        let (head, (status, body)) = call(
            "PUT",
            "/mpu/big.bin",
            &format!("partNumber={number}&uploadId={upload}"),
            data,
        );
        assert_eq!(status, 200, "{body}");
        etags.push(response_header(&head, "etag").expect("etag"));
    }
    let (_, (_, xml)) = call("GET", "/mpu/big.bin", &format!("uploadId={upload}"), b"");
    assert!(
        xml.contains("<PartNumber>1</PartNumber>") && xml.contains("<Size>4</Size>"),
        "{xml}"
    );
    let (_, (_, xml)) = call("GET", "/mpu", "uploads", b"");
    assert!(xml.contains("<Key>big.bin</Key>"), "{xml}");

    let complete = |parts: &[(u32, &str)]| {
        let mut body = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            body.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        call(
            "POST",
            "/mpu/big.bin",
            &format!("uploadId={upload}"),
            body.as_bytes(),
        )
    };
    let (_, (status, xml)) = complete(&[(2, &etags[1]), (1, &etags[0])]);
    assert_eq!(status, 400, "{xml}");
    assert!(xml.contains("InvalidPartOrder"), "{xml}");
    let (_, (status, xml)) = complete(&[(1, &etags[0]), (2, &etags[1])]);
    assert_eq!(status, 200, "{xml}");
    let expected_etag = {
        use md5::Digest;
        let mut joined = Vec::new();
        joined.extend_from_slice(&md5::Md5::digest(&part1));
        joined.extend_from_slice(&md5::Md5::digest(&part2));
        format!("\"{}-2\"", md5_hex(&joined))
    };
    assert_eq!(tag_of(&xml, "ETag").replace("&quot;", "\""), expected_etag);

    let (head, (status, body)) = call("GET", "/mpu/big.bin", "", b"");
    assert_eq!(status, 200);
    assert_eq!(body.len(), part1.len() + 4);
    assert!(body.ends_with("aaaatail"));
    assert_eq!(
        response_header(&head, "etag").as_deref(),
        Some(expected_etag.as_str())
    );

    // A small part that is not the last is refused; abort releases the upload.
    let (_, (_, xml)) = call("POST", "/mpu/small", "uploads", b"");
    let small = tag_of(&xml, "UploadId");
    let mut small_etags = Vec::new();
    for number in [1, 2] {
        let (head, _) = call(
            "PUT",
            "/mpu/small",
            &format!("partNumber={number}&uploadId={small}"),
            b"tiny",
        );
        small_etags.push(response_header(&head, "etag").expect("etag"));
    }
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        small_etags[0], small_etags[1]
    );
    let (_, (status, xml)) = call(
        "POST",
        "/mpu/small",
        &format!("uploadId={small}"),
        body.as_bytes(),
    );
    assert_eq!(status, 400, "{xml}");
    assert!(xml.contains("EntityTooSmall"), "{xml}");
    assert_eq!(
        call("DELETE", "/mpu/small", &format!("uploadId={small}"), b"")
            .1
            .0,
        204
    );
    let (_, (status, xml)) = call("DELETE", "/mpu/small", &format!("uploadId={small}"), b"");
    assert_eq!(status, 404, "{xml}");
    assert!(xml.contains("NoSuchUpload"), "{xml}");
}

#[test]
fn objects_and_parts_are_copied() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    let tag_of = |xml: &str, tag: &str| -> String {
        xml.split(&format!("<{tag}>"))
            .nth(1)
            .and_then(|rest| rest.split(&format!("</{tag}>")).next())
            .unwrap_or_default()
            .to_string()
    };
    for bucket in ["/copysrc", "/copydst"] {
        assert_eq!(call("PUT", bucket, "", &[], b"").1.0, 200);
    }
    let (head, (status, _)) = call(
        "PUT",
        "/copysrc/orig",
        "",
        &[("content-type", "text/plain"), ("x-amz-meta-color", "blue")],
        b"copy me",
    );
    assert_eq!(status, 200);
    let etag = response_header(&head, "etag").expect("etag");

    // A copy keeps the metadata and the ETag.
    let (_, (status, xml)) = call(
        "PUT",
        "/copydst/a%20copy",
        "",
        &[("x-amz-copy-source", "/copysrc/orig")],
        b"",
    );
    assert_eq!(status, 200, "{xml}");
    assert!(xml.contains("<CopyObjectResult"), "{xml}");
    assert_eq!(tag_of(&xml, "ETag").replace("&quot;", "\""), etag);
    let (head, (status, body)) = call("GET", "/copydst/a%20copy", "", &[], b"");
    assert_eq!((status, body.as_str()), (200, "copy me"));
    assert_eq!(
        response_header(&head, "x-amz-meta-color").as_deref(),
        Some("blue")
    );
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/plain")
    );

    // Onto itself only with new metadata.
    let to_self = |directive: &str| {
        call(
            "PUT",
            "/copysrc/orig",
            "",
            &[
                ("x-amz-copy-source", "copysrc/orig"),
                ("x-amz-metadata-directive", directive),
                ("content-type", "text/csv"),
            ],
            b"",
        )
        .1
    };
    let (status, xml) = to_self("COPY");
    assert_eq!(status, 400, "{xml}");
    assert!(xml.contains("InvalidRequest"), "{xml}");
    assert_eq!(to_self("SIDEWAYS").0, 400);
    assert_eq!(to_self("REPLACE").0, 200);
    let (head, (_, body)) = call("GET", "/copysrc/orig", "", &[], b"");
    assert_eq!(body, "copy me");
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/csv")
    );
    assert_eq!(response_header(&head, "x-amz-meta-color"), None);

    // The source's conditions, and a missing source.
    let (_, (status, xml)) = call(
        "PUT",
        "/copydst/b",
        "",
        &[
            ("x-amz-copy-source", "/copysrc/orig"),
            ("x-amz-copy-source-if-match", "\"0123\""),
        ],
        b"",
    );
    assert_eq!(status, 412, "{xml}");
    let (_, (status, xml)) = call(
        "PUT",
        "/copydst/b",
        "",
        &[("x-amz-copy-source", "/copysrc/missing")],
        b"",
    );
    assert_eq!(status, 404, "{xml}");
    assert!(xml.contains("NoSuchKey"), "{xml}");

    // The copy outlives its source.
    assert_eq!(call("DELETE", "/copysrc/orig", "", &[], b"").1.0, 204);
    assert_eq!(
        call("GET", "/copydst/a%20copy", "", &[], b"").1,
        (200, "copy me".to_string())
    );

    // Parts copied whole (shared), by range, and from a multipart object.
    let big = "x".repeat(5 * 1024 * 1024);
    assert_eq!(
        call("PUT", "/copysrc/big", "", &[], big.as_bytes()).1.0,
        200
    );
    let upload_into = |path: &str, sources: &[(&str, Option<&str>)]| {
        let (_, (_, xml)) = call("POST", path, "uploads", &[], b"");
        let upload = tag_of(&xml, "UploadId");
        let mut parts = String::from("<CompleteMultipartUpload>");
        for (index, (source, range)) in sources.iter().enumerate() {
            let number = index + 1;
            let mut extra = vec![("x-amz-copy-source", *source)];
            if let Some(range) = range {
                extra.push(("x-amz-copy-source-range", *range));
            }
            let (_, (status, xml)) = call(
                "PUT",
                path,
                &format!("partNumber={number}&uploadId={upload}"),
                &extra,
                b"",
            );
            assert_eq!(status, 200, "{xml}");
            assert!(xml.contains("<CopyPartResult"), "{xml}");
            parts.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                tag_of(&xml, "ETag")
            ));
        }
        parts.push_str("</CompleteMultipartUpload>");
        let (_, (status, xml)) = call(
            "POST",
            path,
            &format!("uploadId={upload}"),
            &[],
            parts.as_bytes(),
        );
        assert_eq!(status, 200, "{xml}");
    };
    upload_into(
        "/copydst/joined",
        &[
            ("/copysrc/big", None),
            ("/copydst/a%20copy", Some("bytes=5-6")),
        ],
    );
    let (_, (status, body)) = call("GET", "/copydst/joined", "", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(body, format!("{big}me"));
    upload_into("/copydst/again", &[("/copydst/joined", None)]);
    let (_, (status, body)) = call("GET", "/copydst/again", "", &[], b"");
    assert_eq!((status, body.len()), (200, big.len() + 2));
    assert!(body.ends_with("xme"));

    // A range past the source's end is refused.
    let (_, (_, xml)) = call("POST", "/copydst/c", "uploads", &[], b"");
    let upload = tag_of(&xml, "UploadId");
    let (_, (status, xml)) = call(
        "PUT",
        "/copydst/c",
        &format!("partNumber=1&uploadId={upload}"),
        &[
            ("x-amz-copy-source", "/copysrc/big"),
            ("x-amz-copy-source-range", "bytes=0-99999999"),
        ],
        b"",
    );
    assert_eq!(status, 400, "{xml}");
    assert!(xml.contains("InvalidArgument"), "{xml}");
}

#[test]
fn objects_are_tagged_at_put_and_by_the_tagging_api() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/tagged", "", &[], b"").1.0, 200);
    let (_, (status, body)) = call(
        "PUT",
        "/tagged/obj",
        "",
        &[("x-amz-tagging", "team=blue+sky&empty=")],
        b"data",
    );
    assert_eq!(status, 200, "{body}");
    let (head, (status, _)) = call("HEAD", "/tagged/obj", "", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "x-amz-tagging-count").as_deref(),
        Some("2")
    );
    let (_, (status, xml)) = call("GET", "/tagged/obj", "tagging", &[], b"");
    assert_eq!(status, 200, "{xml}");
    assert!(
        xml.contains("<Tag><Key>team</Key><Value>blue sky</Value></Tag><Tag><Key>empty</Key><Value></Value></Tag>"),
        "{xml}"
    );

    let set = |body: &str| call("PUT", "/tagged/obj", "tagging", &[], body.as_bytes()).1;
    assert_eq!(
        set("<Tagging><TagSet><Tag><Key>k&amp;1</Key><Value>v</Value></Tag></TagSet></Tagging>").0,
        200
    );
    let (_, (_, xml)) = call("GET", "/tagged/obj", "tagging", &[], b"");
    assert!(xml.contains("<Key>k&amp;1</Key><Value>v</Value>"), "{xml}");
    let (status, xml) = set(
        "<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>",
    );
    assert_eq!(status, 400, "{xml}");
    assert!(xml.contains("InvalidTag"), "{xml}");
    for bad in ["<Tagging><TagSet>", "", "<Foo/>"] {
        let (status, xml) = set(bad);
        assert_eq!(status, 400, "{xml}");
        assert!(xml.contains("MalformedXML"), "{xml}");
    }
    let (_, (_, xml)) = call("GET", "/tagged/obj", "tagging", &[], b"");
    assert!(
        xml.contains("<Key>k&amp;1</Key>"),
        "a refused body keeps the tags: {xml}"
    );
    let (status, xml) = call(
        "PUT",
        "/tagged/none",
        "tagging",
        &[],
        b"<Tagging><TagSet></TagSet></Tagging>",
    )
    .1;
    assert_eq!(status, 404, "{xml}");
    assert!(xml.contains("NoSuchKey"), "{xml}");

    // A copy keeps the tags unless told to replace them.
    let (_, (status, _)) = call(
        "PUT",
        "/tagged/kept",
        "",
        &[("x-amz-copy-source", "/tagged/obj")],
        b"",
    );
    assert_eq!(status, 200);
    let (_, (_, xml)) = call("GET", "/tagged/kept", "tagging", &[], b"");
    assert!(xml.contains("<Key>k&amp;1</Key>"), "{xml}");
    let (_, (status, _)) = call(
        "PUT",
        "/tagged/replaced",
        "",
        &[
            ("x-amz-copy-source", "/tagged/obj"),
            ("x-amz-tagging-directive", "REPLACE"),
            ("x-amz-tagging", "new=1"),
        ],
        b"",
    );
    assert_eq!(status, 200);
    let (_, (_, xml)) = call("GET", "/tagged/replaced", "tagging", &[], b"");
    assert!(
        xml.contains("<TagSet><Tag><Key>new</Key><Value>1</Value></Tag></TagSet>"),
        "{xml}"
    );

    assert_eq!(call("DELETE", "/tagged/obj", "tagging", &[], b"").1.0, 204);
    let (_, (_, xml)) = call("GET", "/tagged/obj", "tagging", &[], b"");
    assert!(xml.contains("<TagSet></TagSet>"), "{xml}");
    let (head, _) = call("HEAD", "/tagged/obj", "", &[], b"");
    assert_eq!(response_header(&head, "x-amz-tagging-count"), None);
    assert_eq!(
        call("GET", "/tagged/obj", "", &[], b"").1,
        (200, "data".to_string()),
        "the body is untouched"
    );
}

#[test]
fn versioned_buckets_keep_versions_markers_and_null_versions() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(port, method, path, query, &[], body, &key, Some(&secret))
    };
    let tag_all = |xml: &str, tag: &str| -> Vec<String> {
        xml.split(&format!("<{tag}>"))
            .skip(1)
            .filter_map(|rest| rest.split(&format!("</{tag}>")).next())
            .map(str::to_string)
            .collect()
    };
    let versioning = |status: &str| {
        call(
            "PUT",
            "/vers",
            "versioning",
            format!("<VersioningConfiguration><Status>{status}</Status></VersioningConfiguration>")
                .as_bytes(),
        )
        .1
    };
    assert_eq!(call("PUT", "/vers", "", b"").1.0, 200);
    let (_, (_, xml)) = call("GET", "/vers", "versioning", b"");
    assert!(!xml.contains("<Status>"), "{xml}");
    // An unversioned bucket sends no version ids.
    let (head, _) = call("PUT", "/vers/k", "", b"zero");
    assert_eq!(response_header(&head, "x-amz-version-id"), None);
    assert_eq!(versioning("Sideways").0, 400);
    assert_eq!(versioning("Enabled").0, 200);
    let (_, (_, xml)) = call("GET", "/vers", "versioning", b"");
    assert!(xml.contains("<Status>Enabled</Status>"), "{xml}");

    let put = |body: &[u8]| {
        let (head, (status, _)) = call("PUT", "/vers/k", "", body);
        assert_eq!(status, 200);
        response_header(&head, "x-amz-version-id").expect("a version id")
    };
    let v1 = put(b"one");
    let v2 = put(b"two");
    assert_ne!(v1, v2);
    assert_eq!(call("GET", "/vers/k", "", b"").1, (200, "two".to_string()));
    assert_eq!(
        call("GET", "/vers/k", &format!("versionId={v1}"), b"").1,
        (200, "one".to_string())
    );
    assert_eq!(
        call("GET", "/vers/k", "versionId=null", b"").1,
        (200, "zero".to_string()),
        "the version written before versioning is the null version"
    );

    // A delete adds a marker: the key is gone, its versions are not.
    let (head, (status, _)) = call("DELETE", "/vers/k", "", b"");
    assert_eq!(status, 204);
    assert_eq!(
        response_header(&head, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    let marker = response_header(&head, "x-amz-version-id").expect("the marker's id");
    let (head, (status, xml)) = call("GET", "/vers/k", "", b"");
    assert_eq!(status, 404, "{xml}");
    assert!(xml.contains("NoSuchKey"), "{xml}");
    assert_eq!(
        response_header(&head, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    let (_, (status, xml)) = call("GET", "/vers/k", &format!("versionId={marker}"), b"");
    assert_eq!(status, 405, "{xml}");
    let (_, (_, xml)) = call("GET", "/vers", "versions", b"");
    assert_eq!(
        tag_all(&xml, "VersionId"),
        [marker.clone(), v2.clone(), v1.clone(), "null".to_string()]
    );
    assert_eq!(
        tag_all(&xml, "IsLatest"),
        ["true", "false", "false", "false"]
    );
    assert_eq!(tag_all(&xml, "DeleteMarker").len(), 1, "{xml}");

    // Paging one at a time walks every version.
    let mut seen = Vec::new();
    let mut query = "versions&max-keys=1".to_string();
    loop {
        let (_, (_, xml)) = call("GET", "/vers", &query, b"");
        seen.extend(tag_all(&xml, "VersionId"));
        if !xml.contains("<IsTruncated>true</IsTruncated>") {
            break;
        }
        let key_marker = tag_all(&xml, "NextKeyMarker").remove(0);
        let version_marker = tag_all(&xml, "NextVersionIdMarker").remove(0);
        query = format!(
            "key-marker={key_marker}&max-keys=1&version-id-marker={version_marker}&versions"
        );
    }
    assert_eq!(
        seen,
        [marker.clone(), v2.clone(), v1.clone(), "null".to_string()]
    );

    // Removing the marker brings the object back.
    let (head, (status, _)) = call("DELETE", "/vers/k", &format!("versionId={marker}"), b"");
    assert_eq!(status, 204);
    assert_eq!(
        response_header(&head, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    assert_eq!(call("GET", "/vers/k", "", b"").1, (200, "two".to_string()));

    // Suspended: a PUT replaces the null version and keeps the others.
    assert_eq!(versioning("Suspended").0, 200);
    let (head, _) = call("PUT", "/vers/k", "", b"suspended");
    assert_eq!(
        response_header(&head, "x-amz-version-id").as_deref(),
        Some("null")
    );
    let (_, (_, xml)) = call("GET", "/vers", "versions", b"");
    assert_eq!(
        tag_all(&xml, "VersionId"),
        ["null", v2.as_str(), v1.as_str()]
    );
    assert_eq!(
        call("GET", "/vers/k", "versionId=null", b"").1,
        (200, "suspended".to_string())
    );

    // Sub-resources that are not implemented are refused, not misrouted.
    assert_eq!(call("PUT", "/vers", "tagging", b"").1.0, 501);
    assert_eq!(call("GET", "/vers/k", "torrent", b"").1.0, 501);
}

#[test]
fn objects_are_found_by_exact_key_and_by_prefix() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(port, method, path, query, &[], body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/find", "", b"").1.0, 200);
    for name in [
        "caf%C3%A9/a.txt",
        "caf%C3%A9/%C3%A4.txt",
        "cafe/x",
        "caf%C3%A9s",
    ] {
        assert_eq!(call("PUT", &format!("/find/{name}"), "", b"x").1.0, 200);
    }

    // Exact lookup: the key that exists, and one that does not (a HEAD
    // answer carries no body).
    assert_eq!(call("HEAD", "/find/caf%C3%A9/a.txt", "", b"").1.0, 200);
    let (_, (status, body)) = call("HEAD", "/find/caf%C3%A9/missing", "", b"");
    assert_eq!((status, body.as_str()), (404, ""));
    assert_eq!(call("GET", "/find/cafe/a.txt", "", b"").1.0, 404);

    // Prefix search: exactly the keys under the prefix, in UTF-8 order.
    // The test client reads bodies one char per byte: back to UTF-8.
    let keys_of = |xml: &str| -> Vec<String> {
        let bytes: Vec<u8> = xml.chars().map(|c| c as u8).collect();
        String::from_utf8(bytes)
            .unwrap()
            .split("<Key>")
            .skip(1)
            .filter_map(|rest| rest.split("</Key>").next())
            .map(str::to_string)
            .collect()
    };
    let (_, (status, xml)) = call("GET", "/find", "list-type=2&prefix=caf%C3%A9%2F", b"");
    assert_eq!(status, 200, "{xml}");
    assert_eq!(keys_of(&xml), ["café/a.txt", "café/ä.txt"]);
    let (_, (_, xml)) = call("GET", "/find", "list-type=2&prefix=caf", b"");
    assert_eq!(
        keys_of(&xml),
        ["cafe/x", "café/a.txt", "café/ä.txt", "cafés"]
    );

    // A key whose newest version is a delete marker is not found, and says so.
    let versioning = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(call("PUT", "/find", "versioning", versioning).1.0, 200);
    assert_eq!(call("DELETE", "/find/cafe/x", "", b"").1.0, 204);
    let (head, (status, _)) = call("HEAD", "/find/cafe/x", "", b"");
    assert_eq!(status, 404);
    assert_eq!(
        response_header(&head, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    let (_, (_, xml)) = call("GET", "/find", "list-type=2&prefix=cafe", b"");
    assert!(keys_of(&xml).is_empty(), "{xml}");
}

/// An S3 Vectors request: `POST /<operation>` with a JSON body, signed as
/// AWS's SDKs sign it for the `s3vectors` service (content type, host and
/// date signed; the body's hash in the signature, no x-amz-content-sha256).
/// Returns the status, the error type (if any) and the JSON reply.
fn vectors(
    port: u16,
    key: &str,
    secret: &str,
    operation: &str,
    body: serde_json::Value,
) -> (u16, Option<String>, serde_json::Value) {
    let body = body.to_string();
    let amz_date = amz_now();
    let host = format!("127.0.0.1:{port}");
    let payload = sigv4::sha256_hex(body.as_bytes());
    let path = format!("/{operation}");
    let headers = vec![
        ("content-type".to_string(), "application/json".to_string()),
        ("host".to_string(), host.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    let credential = sigv4::Credential {
        access_key: key.to_string(),
        date: amz_date[..8].to_string(),
        region: "us-east-1".to_string(),
        service: "s3vectors".to_string(),
    };
    let signed: Vec<String> = headers.iter().map(|(name, _)| name.clone()).collect();
    let canonical =
        sigv4::canonical_request_for("s3vectors", "POST", &path, "", &headers, &signed, &payload)
            .unwrap();
    let signature = sigv4::sign(
        &sigv4::signing_key(secret, &credential),
        &sigv4::string_to_sign(&amz_date, &credential.scope(), &canonical),
    );
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nx-amz-date: {amz_date}\r\nContent-Length: {}\r\nConnection: close\r\nAuthorization: AWS4-HMAC-SHA256 Credential={key}/{}, SignedHeaders={}, Signature={signature}\r\n\r\n{body}",
        body.len(),
        credential.scope(),
        signed.join(";")
    );
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.write_all(request.as_bytes()).expect("send");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let response = String::from_utf8(raw).expect("utf-8");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let (head, body) = response.split_once("\r\n\r\n").expect("a head");
    let kind = response_header(head, "x-amzn-errortype");
    (
        status,
        kind,
        serde_json::from_str(body).expect("a JSON reply"),
    )
}

/// An IAM or STS Query-protocol request: `POST /` with a form body, signed
/// as AWS's SDKs sign it (for us-east-1 and `service`). Returns the status
/// and the XML reply.
fn query_api(
    port: u16,
    key: &str,
    secret: &str,
    service: &str,
    params: &[(&str, &str)],
) -> (u16, String) {
    let encode = |text: &str| {
        percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
    };
    let body: String = params
        .iter()
        .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let amz_date = amz_now();
    let host = format!("127.0.0.1:{port}");
    let payload = sigv4::sha256_hex(body.as_bytes());
    let headers = vec![
        (
            "content-type".to_string(),
            "application/x-www-form-urlencoded; charset=utf-8".to_string(),
        ),
        ("host".to_string(), host.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    let credential = sigv4::Credential {
        access_key: key.to_string(),
        date: amz_date[..8].to_string(),
        region: "us-east-1".to_string(),
        service: service.to_string(),
    };
    let signed: Vec<String> = headers.iter().map(|(name, _)| name.clone()).collect();
    let canonical =
        sigv4::canonical_request_for(service, "POST", "/", "", &headers, &signed, &payload)
            .unwrap();
    let signature = sigv4::sign(
        &sigv4::signing_key(secret, &credential),
        &sigv4::string_to_sign(&amz_date, &credential.scope(), &canonical),
    );
    let request = format!(
        "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-www-form-urlencoded; charset=utf-8\r\nx-amz-date: {amz_date}\r\nContent-Length: {}\r\nConnection: close\r\nAuthorization: AWS4-HMAC-SHA256 Credential={key}/{}, SignedHeaders={}, Signature={signature}\r\n\r\n{body}",
        body.len(),
        credential.scope(),
        signed.join(";")
    );
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.write_all(request.as_bytes()).expect("send");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let response = String::from_utf8(raw).expect("utf-8");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status line");
    let (_, body) = response.split_once("\r\n\r\n").expect("a head");
    (status, body.to_string())
}

/// The text of the first `<tag>` in `xml`.
fn xml_value(xml: &str, tag: &str) -> String {
    xml.split(&format!("<{tag}>"))
        .nth(1)
        .and_then(|rest| rest.split(&format!("</{tag}>")).next())
        .unwrap_or_else(|| panic!("no <{tag}> in {xml}"))
        .to_string()
}

#[test]
fn the_iam_api_manages_users_keys_and_policies() {
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "acme");
    let root = |params: &[(&str, &str)]| query_api(port, &root_key, &root_secret, "iam", params);
    let ok = |reply: (u16, String)| {
        assert_eq!(reply.0, 200, "{}", reply.1);
        reply.1
    };

    // Users, by the account's root.
    let created = ok(root(&[
        ("Action", "CreateUser"),
        ("UserName", "alice"),
        ("Version", "2010-05-08"),
    ]));
    assert!(created.contains("<CreateUserResponse"), "{created}");
    assert_eq!(
        xml_value(&created, "Arn"),
        format!("arn:aws:iam::{account}:user/alice")
    );
    assert!(xml_value(&created, "UserId").starts_with("AIDA"));
    let (status, reply) = root(&[("Action", "CreateUser"), ("UserName", "ALICE")]);
    assert_eq!(status, 409);
    assert!(
        reply.contains("<Code>EntityAlreadyExists</Code>"),
        "{reply}"
    );
    assert!(ok(root(&[("Action", "ListUsers")])).contains("<UserName>alice</UserName>"));
    let (status, reply) = root(&[("Action", "GetUser"), ("UserName", "nobody")]);
    assert_eq!(status, 404);
    assert!(reply.contains("<Code>NoSuchEntity</Code>"), "{reply}");

    // A key of the user, which acts for it.
    let key = ok(root(&[
        ("Action", "CreateAccessKey"),
        ("UserName", "alice"),
    ]));
    let (alice_key, alice_secret) = (
        xml_value(&key, "AccessKeyId"),
        xml_value(&key, "SecretAccessKey"),
    );
    let alice = |params: &[(&str, &str)]| query_api(port, &alice_key, &alice_secret, "iam", params);
    let identity = query_api(
        port,
        &alice_key,
        &alice_secret,
        "sts",
        &[("Action", "GetCallerIdentity"), ("Version", "2011-06-15")],
    );
    assert_eq!(identity.0, 200, "{}", identity.1);
    assert_eq!(
        xml_value(&identity.1, "Arn"),
        format!("arn:aws:iam::{account}:user/alice")
    );
    assert_eq!(xml_value(&identity.1, "Account"), account);

    // Without policies she may do nothing; an inline policy lets her list
    // buckets.
    let (status, reply) = alice(&[("Action", "CreateUser"), ("UserName", "bob")]);
    assert_eq!(status, 403);
    assert!(reply.contains("<Code>AccessDenied</Code>"), "{reply}");
    assert_eq!(get(port, "/", &alice_key, Some(&alice_secret)).0, 403);
    let list_buckets = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"}}"#;
    ok(root(&[
        ("Action", "PutUserPolicy"),
        ("UserName", "alice"),
        ("PolicyName", "list"),
        ("PolicyDocument", list_buckets),
    ]));
    assert_eq!(get(port, "/", &alice_key, Some(&alice_secret)).0, 200);
    let fetched = ok(root(&[
        ("Action", "GetUserPolicy"),
        ("UserName", "alice"),
        ("PolicyName", "list"),
    ]));
    let document = percent_encoding::percent_decode_str(&xml_value(&fetched, "PolicyDocument"))
        .decode_utf8()
        .unwrap()
        .into_owned();
    assert_eq!(document, list_buckets, "returned URL-encoded, as written");
    assert!(
        ok(root(&[
            ("Action", "ListUserPolicies"),
            ("UserName", "alice")
        ]))
        .contains("<member>list</member>")
    );
    let (status, reply) = root(&[
        ("Action", "PutUserPolicy"),
        ("UserName", "alice"),
        ("PolicyName", "bad"),
        ("PolicyDocument", "{}"),
    ]);
    assert_eq!(status, 400);
    assert!(
        reply.contains("<Code>MalformedPolicyDocument</Code>"),
        "{reply}"
    );

    // A managed policy attached to her makes her an IAM administrator.
    let admin = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "iam:*", "Resource": "*"}}"#;
    let created = ok(root(&[
        ("Action", "CreatePolicy"),
        ("PolicyName", "iam-admin"),
        ("PolicyDocument", admin),
    ]));
    let arn = xml_value(&created, "Arn");
    assert_eq!(arn, format!("arn:aws:iam::{account}:policy/iam-admin"));
    ok(root(&[
        ("Action", "AttachUserPolicy"),
        ("UserName", "alice"),
        ("PolicyArn", &arn),
    ]));
    assert!(
        ok(root(&[
            ("Action", "ListAttachedUserPolicies"),
            ("UserName", "alice")
        ]))
        .contains(&arn)
    );
    assert_eq!(
        xml_value(
            &ok(root(&[("Action", "GetPolicy"), ("PolicyArn", &arn)])),
            "AttachmentCount"
        ),
        "1"
    );
    let version = ok(root(&[
        ("Action", "GetPolicyVersion"),
        ("PolicyArn", &arn),
        ("VersionId", "v1"),
    ]));
    assert!(
        xml_value(&version, "Document").contains("iam%3A"),
        "{version}"
    );
    assert!(
        ok(root(&[("Action", "ListPolicies"), ("Scope", "Local")]))
            .contains("<PolicyName>iam-admin</PolicyName>")
    );
    ok(alice(&[("Action", "CreateUser"), ("UserName", "bob")]));
    let (status, reply) = root(&[("Action", "DeletePolicy"), ("PolicyArn", &arn)]);
    assert_eq!(status, 409);
    assert!(reply.contains("<Code>DeleteConflict</Code>"), "{reply}");

    // Her own keys: listed, deactivated (and then refused), deleted.
    let listed = ok(alice(&[("Action", "ListAccessKeys")]));
    assert!(listed.contains(&alice_key), "{listed}");
    ok(root(&[
        ("Action", "UpdateAccessKey"),
        ("UserName", "alice"),
        ("AccessKeyId", &alice_key),
        ("Status", "Inactive"),
    ]));
    assert_eq!(get(port, "/", &alice_key, Some(&alice_secret)).0, 403);
    assert!(
        ok(root(&[("Action", "ListAccessKeys"), ("UserName", "alice")]))
            .contains("<Status>Inactive</Status>")
    );
    let (status, _) = root(&[
        ("Action", "DeleteAccessKey"),
        ("UserName", "bob"),
        ("AccessKeyId", &alice_key),
    ]);
    assert_eq!(status, 404, "not bob's key");

    // A user goes only once its keys and policies have.
    let (status, reply) = root(&[("Action", "DeleteUser"), ("UserName", "alice")]);
    assert_eq!(status, 409);
    assert!(reply.contains("<Code>DeleteConflict</Code>"), "{reply}");
    ok(root(&[
        ("Action", "DeleteAccessKey"),
        ("UserName", "alice"),
        ("AccessKeyId", &alice_key),
    ]));
    ok(root(&[
        ("Action", "DeleteUserPolicy"),
        ("UserName", "alice"),
        ("PolicyName", "list"),
    ]));
    ok(root(&[
        ("Action", "DetachUserPolicy"),
        ("UserName", "alice"),
        ("PolicyArn", &arn),
    ]));
    ok(root(&[("Action", "DeleteUser"), ("UserName", "alice")]));
    ok(root(&[("Action", "DeletePolicy"), ("PolicyArn", &arn)]));
    let (status, reply) = root(&[("Action", "CreateGroup"), ("GroupName", "g")]);
    assert_eq!(status, 400);
    assert!(reply.contains("<Code>InvalidAction</Code>"), "{reply}");
    // Another account's policy is not this one's.
    let (status, _) = root(&[
        ("Action", "GetPolicy"),
        ("PolicyArn", "arn:aws:iam::222222222222:policy/iam-admin"),
    ]);
    assert_eq!(status, 404);
}

#[test]
fn iam_names_are_checked_before_they_reach_storage() {
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "acme");
    let root = |params: &[(&str, &str)]| query_api(port, &root_key, &root_secret, "iam", params);
    let ok = |reply: (u16, String)| {
        assert_eq!(reply.0, 200, "{}", reply.1);
        reply.1
    };
    ok(root(&[("Action", "CreateUser"), ("UserName", "alice")]));
    // A policy longer than one storage chunk, attached to alice.
    let statements: Vec<String> = (0..40)
        .map(|i| format!(r#"{{"Sid": "s{i}", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::bucket-{i}/*"}}"#))
        .collect();
    let big = format!(
        r#"{{"Version": "2012-10-17", "Statement": [{}]}}"#,
        statements.join(", ")
    );
    assert!(big.len() > 1500);
    let inline_big = format!(
        r#"{{"Version": "2012-10-17", "Statement": [{}]}}"#,
        statements[..18].join(",")
    );
    assert!(inline_big.len() > 1500 && inline_big.len() < 2048);
    let arn = xml_value(
        &ok(root(&[
            ("Action", "CreatePolicy"),
            ("PolicyName", "big"),
            ("PolicyDocument", &big),
        ])),
        "Arn",
    );
    ok(root(&[
        ("Action", "AttachUserPolicy"),
        ("UserName", "alice"),
        ("PolicyArn", &arn),
    ]));
    ok(root(&[
        ("Action", "PutUserPolicy"),
        ("UserName", "alice"),
        ("PolicyName", "bigin"),
        ("PolicyDocument", &inline_big),
    ]));

    // A 0 byte in a name is refused, and nothing changes.
    let nul_arn = format!("{arn}\0");
    assert_eq!(
        root(&[("Action", "DeletePolicy"), ("PolicyArn", &nul_arn)]).0,
        404
    );
    assert_eq!(
        root(&[
            ("Action", "AttachUserPolicy"),
            ("UserName", "alice"),
            ("PolicyArn", &format!("{arn}2\0"))
        ])
        .0,
        404
    );
    assert_eq!(
        root(&[
            ("Action", "DeleteUserPolicy"),
            ("UserName", "alice"),
            ("PolicyName", "bigin\0")
        ])
        .0,
        400
    );
    assert_eq!(
        root(&[
            ("Action", "GetUserPolicy"),
            ("UserName", "alice\0p"),
            ("PolicyName", "bigin")
        ])
        .0,
        400
    );
    let version = ok(root(&[
        ("Action", "GetPolicyVersion"),
        ("PolicyArn", &arn),
        ("VersionId", "v1"),
    ]));
    let document = percent_encoding::percent_decode_str(&xml_value(&version, "Document"))
        .decode_utf8()
        .unwrap()
        .into_owned();
    assert_eq!(document, big, "the attached policy is whole");
    let inline = ok(root(&[
        ("Action", "GetUserPolicy"),
        ("UserName", "alice"),
        ("PolicyName", "bigin"),
    ]));
    assert_eq!(
        percent_encoding::percent_decode_str(&xml_value(&inline, "PolicyDocument"))
            .decode_utf8()
            .unwrap(),
        inline_big
    );
    assert!(
        ok(root(&[
            ("Action", "ListAttachedUserPolicies"),
            ("UserName", "alice")
        ]))
        .matches("<member>")
        .count()
            == 1
    );

    // Policy names compare without case; OnlyAttached lists attached ones.
    let small = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}}"#;
    assert_eq!(
        root(&[
            ("Action", "CreatePolicy"),
            ("PolicyName", "BIG"),
            ("PolicyDocument", small)
        ])
        .0,
        409
    );
    ok(root(&[
        ("Action", "CreatePolicy"),
        ("PolicyName", "unused"),
        ("PolicyDocument", small),
    ]));
    let attached = ok(root(&[
        ("Action", "ListPolicies"),
        ("OnlyAttached", "true"),
    ]));
    assert!(
        attached.contains("<PolicyName>big</PolicyName>") && !attached.contains("unused"),
        "{attached}"
    );

    // A wrong secret is SignatureDoesNotMatch, as in AWS.
    let (status, reply) = query_api(port, &root_key, "wrong", "iam", &[("Action", "ListUsers")]);
    assert_eq!(status, 403);
    assert!(
        reply.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{reply}"
    );
    let _ = account;
}

#[test]
fn the_s3_vectors_api_manages_buckets_indexes_and_vectors_and_queries_them() {
    use serde_json::json;
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call =
        |operation: &str, body: serde_json::Value| vectors(port, &key, &secret, operation, body);
    let ok = |operation: &str, body: serde_json::Value| {
        let (status, kind, reply) = call(operation, body);
        assert_eq!(
            (status, kind.as_deref()),
            (200, None),
            "{operation}: {reply}"
        );
        reply
    };

    // Buckets.
    let created = ok("CreateVectorBucket", json!({"vectorBucketName": "vb1"}));
    assert_eq!(
        created["vectorBucketArn"],
        json!("arn:aws:s3vectors:us-east-1:000000000000:bucket/vb1")
    );
    ok("CreateVectorBucket", json!({"vectorBucketName": "vb2"}));
    let (status, kind, _) = call("CreateVectorBucket", json!({"vectorBucketName": "vb1"}));
    assert_eq!((status, kind.as_deref()), (409, Some("ConflictException")));
    let (status, kind, reply) = call(
        "CreateVectorBucket",
        json!({"vectorBucketName": "Bad_Name"}),
    );
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );
    assert!(reply["fieldList"].is_array());
    let got = ok(
        "GetVectorBucket",
        json!({"vectorBucketArn": created["vectorBucketArn"]}),
    );
    assert_eq!(got["vectorBucket"]["vectorBucketName"], json!("vb1"));
    assert!(got["vectorBucket"]["creationTime"].as_f64().unwrap() > 1.7e9);
    let page = ok("ListVectorBuckets", json!({"maxResults": 1}));
    assert_eq!(page["vectorBuckets"][0]["vectorBucketName"], json!("vb1"));
    let page = ok(
        "ListVectorBuckets",
        json!({"maxResults": 1, "nextToken": page["nextToken"]}),
    );
    assert_eq!(page["vectorBuckets"][0]["vectorBucketName"], json!("vb2"));
    assert!(page.get("nextToken").is_none());
    assert_eq!(
        ok("ListVectorBuckets", json!({"prefix": "vb2"}))["vectorBuckets"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // An index, by name and by ARN.
    let (status, kind, _) = call(
        "CreateIndex",
        json!({"vectorBucketName": "vb1", "indexName": "idx", "dataType": "float32", "dimension": 5000, "distanceMetric": "cosine"}),
    );
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );
    let index = ok(
        "CreateIndex",
        json!({
            "vectorBucketName": "vb1", "indexName": "idx", "dataType": "float32",
            "dimension": 3, "distanceMetric": "euclidean",
            "metadataConfiguration": {"nonFilterableMetadataKeys": ["text"]}
        }),
    );
    let arn = index["indexArn"].clone();
    assert_eq!(
        arn,
        json!("arn:aws:s3vectors:us-east-1:000000000000:bucket/vb1/index/idx")
    );
    let got = ok("GetIndex", json!({"indexArn": arn}));
    assert_eq!(got["index"]["dimension"], json!(3));
    assert_eq!(got["index"]["distanceMetric"], json!("euclidean"));
    assert_eq!(
        got["index"]["metadataConfiguration"]["nonFilterableMetadataKeys"],
        json!(["text"])
    );
    assert_eq!(
        ok("ListIndexes", json!({"vectorBucketName": "vb1"}))["indexes"][0]["indexName"],
        json!("idx")
    );
    let (status, kind, _) = call(
        "GetIndex",
        json!({"vectorBucketName": "vb1", "indexName": "nope"}),
    );
    assert_eq!((status, kind.as_deref()), (404, Some("NotFoundException")));
    // Refused: an ARN of another region or account, a bucket named twice,
    // an empty prefix, an empty list of non-filterable keys.
    for (operation, body) in [
        (
            "GetIndex",
            json!({"indexArn": "arn:aws:s3vectors:eu-west-1:000000000000:bucket/vb1/index/idx"}),
        ),
        (
            "GetIndex",
            json!({"indexArn": "arn:aws:s3vectors:us-east-1:123456789012:bucket/vb1/index/idx"}),
        ),
        ("GetIndex", json!({"indexArn": arn, "indexName": "idx"})),
        (
            "GetVectorBucket",
            json!({"vectorBucketName": "vb2", "vectorBucketArn": created["vectorBucketArn"]}),
        ),
        ("ListVectorBuckets", json!({"prefix": ""})),
        (
            "CreateIndex",
            json!({
                "vectorBucketName": "vb1", "indexName": "idx2", "dataType": "float32",
                "dimension": 3, "distanceMetric": "cosine", "metadataConfiguration": "x"
            }),
        ),
        (
            "CreateIndex",
            json!({
                "vectorBucketName": "vb1", "indexName": "idx2", "dataType": "float32",
                "dimension": 3, "distanceMetric": "cosine",
                "metadataConfiguration": {"nonFilterableMetadataKeys": []}
            }),
        ),
    ] {
        let (status, kind, reply) = call(operation, body.clone());
        assert_eq!(
            (status, kind.as_deref()),
            (400, Some("ValidationException")),
            "{operation} {body}: {reply}"
        );
    }

    // Vectors.
    let batch: Vec<serde_json::Value> = (0..150)
        .map(|n| {
            json!({
                "key": format!("k{n:03}"),
                "data": {"float32": [n as f32, 0.0, 1.0]},
                "metadata": {"genre": if n % 2 == 0 { "even" } else { "odd" }, "n": n, "text": "long"}
            })
        })
        .collect();
    ok("PutVectors", json!({"indexArn": arn, "vectors": batch}));
    let (status, kind, _) = call(
        "PutVectors",
        json!({"indexArn": arn, "vectors": [{"key": "bad", "data": {"float32": [1.0]}}]}),
    );
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );
    let got = ok(
        "GetVectors",
        json!({"indexArn": arn, "keys": ["k007", "missing"], "returnData": true, "returnMetadata": true}),
    );
    assert_eq!(got["vectors"].as_array().unwrap().len(), 1);
    assert_eq!(got["vectors"][0]["data"]["float32"], json!([7.0, 0.0, 1.0]));
    assert_eq!(got["vectors"][0]["metadata"]["text"], json!("long"));
    // Listed in pages and in segments: every key exactly once.
    let mut listed = Vec::new();
    for segment in 0..3 {
        let mut token = serde_json::Value::Null;
        loop {
            let page = ok(
                "ListVectors",
                json!({"indexArn": arn, "maxResults": 40, "segmentCount": 3, "segmentIndex": segment, "nextToken": token}),
            );
            listed.extend(
                page["vectors"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v["key"].as_str().unwrap().to_string()),
            );
            match page.get("nextToken") {
                Some(next) => token = next.clone(),
                None => break,
            }
        }
    }
    listed.sort();
    assert_eq!(listed.len(), 150);
    listed.dedup();
    assert_eq!(listed.len(), 150);
    // A segment's page carries the data and metadata of its keys; a segment
    // count needs its index.
    let page = ok(
        "ListVectors",
        json!({"indexArn": arn, "maxResults": 5, "segmentCount": 2, "segmentIndex": 1,
               "returnData": true, "returnMetadata": true}),
    );
    for vector in page["vectors"].as_array().unwrap() {
        let n: f64 = vector["key"].as_str().unwrap()[1..].parse().unwrap();
        assert_eq!(vector["data"]["float32"][0], json!(n));
        assert_eq!(vector["metadata"]["n"], json!(n as u64));
    }
    let (status, kind, _) = call("ListVectors", json!({"indexArn": arn, "segmentCount": 2}));
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );

    // Queries: nearest first, filtered, with distances and metadata, every
    // result in one response.
    let found = ok(
        "QueryVectors",
        json!({"indexArn": arn, "topK": 3, "queryVector": {"float32": [10.0, 0.0, 1.0]},
               "filter": {"genre": "even"}, "returnDistance": true, "returnMetadata": true}),
    );
    let keys: Vec<&str> = found["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["k010", "k008", "k012"]);
    assert_eq!(found["distanceMetric"], json!("euclidean"));
    assert_eq!(found["vectors"][1]["distance"], json!(2.0));
    assert_eq!(found["vectors"][0]["metadata"]["n"], json!(10));
    let all = ok(
        "QueryVectors",
        json!({"indexArn": arn, "topK": 150, "queryVector": {"float32": [0.0, 0.0, 1.0]}}),
    );
    assert_eq!(all["vectors"].as_array().unwrap().len(), 150);
    assert_eq!(all["vectors"][100]["key"], json!("k100"));
    assert!(all["vectors"][0].get("distance").is_none());
    assert!(all.get("nextToken").is_none());
    let (status, kind, _) = call(
        "QueryVectors",
        json!({"indexArn": arn, "topK": 150, "queryVector": {"float32": [0.0, 0.0, 1.0]}, "nextToken": "MTAw"}),
    );
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );
    let (status, kind, _) = call(
        "QueryVectors",
        json!({"indexArn": arn, "topK": 3, "queryVector": {"float32": [1.0, 0.0, 0.0]}, "filter": {"n": {"$regex": "x"}}}),
    );
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );

    // Deleting: vectors, then the bucket only once its index is gone.
    ok(
        "DeleteVectors",
        json!({"indexArn": arn, "keys": ["k010", "k008"]}),
    );
    let found = ok(
        "QueryVectors",
        json!({"indexArn": arn, "topK": 1, "queryVector": {"float32": [10.0, 0.0, 1.0]}, "filter": {"genre": "even"}}),
    );
    assert_ne!(found["vectors"][0]["key"], json!("k010"));
    let (status, kind, _) = call("DeleteVectorBucket", json!({"vectorBucketName": "vb1"}));
    assert_eq!((status, kind.as_deref()), (409, Some("ConflictException")));
    ok("DeleteIndex", json!({"indexArn": arn}));
    ok("DeleteVectorBucket", json!({"vectorBucketName": "vb1"}));
    let (status, kind, _) = call("GetVectorBucket", json!({"vectorBucketName": "vb1"}));
    assert_eq!((status, kind.as_deref()), (404, Some("NotFoundException")));

    // A wrong secret, or an operation this server does not serve.
    let (status, kind, _) = vectors(port, &key, "wrong", "ListVectorBuckets", json!({}));
    assert_eq!(
        (status, kind.as_deref()),
        (403, Some("AccessDeniedException"))
    );
    let (status, kind, _) = call("PutVectorBucketPolicy", json!({"vectorBucketName": "vb2"}));
    assert_eq!((status, kind.as_deref()), (501, Some("NotImplemented")));
    // An unknown key is refused before its body is read: the reply comes
    // although the promised megabyte never arrives.
    let amz_date = amz_now();
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(20)))
        .unwrap();
    let request = format!(
        "POST /ListVectorBuckets HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nx-amz-date: {amz_date}\r\nContent-Length: 1000000\r\nAuthorization: AWS4-HMAC-SHA256 Credential=NOSUCHKEY/{}/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature={}\r\n\r\n{{",
        &amz_date[..8],
        "0".repeat(64)
    );
    stream.write_all(request.as_bytes()).expect("send");
    let mut head = [0u8; 12];
    stream
        .read_exact(&mut head)
        .expect("a reply before the body");
    assert_eq!(&head, b"HTTP/1.1 403");

    // Clients that know the key id but not its secret, holding bodies open,
    // turn no signed request away.
    let slow: Vec<std::net::TcpStream> = (0..20)
        .map(|_| {
            let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
            let request = format!(
                "POST /ListVectorBuckets HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nx-amz-date: {amz_date}\r\nContent-Length: 1000\r\nAuthorization: AWS4-HMAC-SHA256 Credential={key}/{}/us-east-1/s3vectors/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature={}\r\n\r\n{{",
                &amz_date[..8],
                "0".repeat(64)
            );
            stream.write_all(request.as_bytes()).expect("send");
            stream
        })
        .collect();
    std::thread::sleep(std::time::Duration::from_millis(500));
    let (status, kind, reply) = call("ListVectorBuckets", json!({}));
    assert_eq!((status, kind.as_deref()), (200, None), "{reply}");
    drop(slow);

    // A body of more values than any request needs is refused unparsed.
    let (status, kind, _) = call("ListVectorBuckets", json!({"junk": vec![0u8; 5 << 20]}));
    assert_eq!(
        (status, kind.as_deref()),
        (400, Some("ValidationException"))
    );

    // The S3 API on the same port is unchanged.
    assert_eq!(get(port, "/", &key, Some(&secret)).0, 200);
}

/// The XML of a ServerSideEncryptionConfiguration with one rule.
fn encryption_config(rule: &str) -> Vec<u8> {
    format!(
        "<ServerSideEncryptionConfiguration><Rule>{rule}</Rule></ServerSideEncryptionConfiguration>"
    )
    .into_bytes()
}

#[test]
fn server_side_encryption_is_the_databases_encryption_at_rest() {
    const SSE: &str = "x-amz-server-side-encryption";
    let aes_default = encryption_config(
        "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
         </ApplyServerSideEncryptionByDefault>",
    );
    let block_sse_c = encryption_config(
        "<BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes>",
    );

    // A plain database: nothing is encrypted, and nothing says it is.
    let server = start_server_with(false);
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "plain");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/plain", "", &[], b"").1.0, 200);
    let (_, (status, reply)) = call("PUT", "/plain/o", "", &[(SSE, "AES256")], b"x");
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("InvalidArgument"), "{reply}");
    let (head, (status, _)) = call("PUT", "/plain/o", "", &[], b"x");
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, SSE), None);
    let (_, (status, reply)) = call("GET", "/plain", "encryption", &[], b"");
    assert_eq!(status, 400);
    assert!(
        reply.contains("ServerSideEncryptionConfigurationNotFoundError"),
        "{reply}"
    );
    assert_eq!(
        call("PUT", "/plain", "encryption", &[], &aes_default).1.0,
        400
    );
    assert_eq!(
        call("PUT", "/plain", "encryption", &[], &block_sse_c).1.0,
        200
    );
    let (_, (status, reply)) = call("GET", "/plain", "encryption", &[], b"");
    assert_eq!(status, 200);
    assert!(
        reply.contains("<EncryptionType>SSE-C</EncryptionType>"),
        "{reply}"
    );
    assert!(!reply.contains("SSEAlgorithm"), "{reply}");
    assert_eq!(call("DELETE", "/plain", "encryption", &[], b"").1.0, 204);
    assert_eq!(call("GET", "/plain", "encryption", &[], b"").1.0, 400);
    // A read names no encryption.
    assert_eq!(
        call("GET", "/plain/o", "", &[(SSE, "AES256")], b"").1.0,
        400
    );
    // Customer-provided keys are refused over plain HTTP, never ignored
    // (the object would be stored, and served to anyone, in the clear).
    let customer = [("x-amz-server-side-encryption-customer-algorithm", "AES256")];
    let copy_customer = [
        ("x-amz-copy-source", "/plain/o"),
        (
            "x-amz-copy-source-server-side-encryption-customer-algorithm",
            "AES256",
        ),
    ];
    for (method, path, query, extra) in [
        ("PUT", "/plain/c", "", &customer[..]),
        ("GET", "/plain/o", "", &customer[..]),
        ("HEAD", "/plain/o", "", &customer[..]),
        ("POST", "/plain/c", "uploads", &customer[..]),
        ("PUT", "/plain/c2", "", &copy_customer[..]),
    ] {
        let (_, (status, reply)) = call(method, path, query, extra, b"x");
        assert_eq!(status, 400, "{method} {path}?{query}: {reply}");
    }
    assert_eq!(call("GET", "/plain/c", "", &[], b"").1.0, 404);
    let form = signed_form(
        &key,
        &secret,
        "2099-01-01T00:00:00.000Z",
        &[
            serde_json::json!({"bucket": "plain"}),
            serde_json::json!(["starts-with", "$key", "f"]),
            serde_json::json!({"x-amz-server-side-encryption-customer-algorithm": "AES256"}),
        ],
        &[
            ("key", "f"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        ],
    );
    assert_eq!(post_form(port, "plain", &form, "f", b"x", 64).1.0, 400);
    assert_eq!(call("GET", "/plain/f", "", &[], b"").1.0, 404);
    drop(server);

    // An encrypted database: every object is AES256, and says so.
    let server = start_server_with(true);
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "sealed");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/sealed", "", &[], b"").1.0, 200);
    for extra in [&[(SSE, "AES256")][..], &[]] {
        let (head, (status, reply)) = call("PUT", "/sealed/o", "", extra, b"secret");
        assert_eq!(status, 200, "{reply}");
        assert_eq!(response_header(&head, SSE).as_deref(), Some("AES256"));
    }
    for method in ["GET", "HEAD"] {
        let (head, (status, _)) = call(method, "/sealed/o", "", &[], b"");
        assert_eq!(status, 200);
        assert_eq!(
            response_header(&head, SSE).as_deref(),
            Some("AES256"),
            "{method}"
        );
    }
    let (_, (status, reply)) = call("PUT", "/sealed/k", "", &[(SSE, "aws:kms")], b"x");
    assert_eq!(status, 400, "{reply}");
    let (head, (status, _)) = call(
        "PUT",
        "/sealed/c",
        "",
        &[("x-amz-copy-source", "/sealed/o")],
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, SSE).as_deref(),
        Some("AES256"),
        "copy"
    );
    let (head, (status, created)) = call("POST", "/sealed/m", "uploads", &[(SSE, "AES256")], b"");
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, SSE).as_deref(),
        Some("AES256"),
        "create upload"
    );
    let upload = xml_value(&created, "UploadId");
    let (head, (status, _)) = call(
        "PUT",
        "/sealed/m",
        &format!("uploadId={upload}&partNumber=1"),
        &[],
        b"part",
    );
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, SSE).as_deref(),
        Some("AES256"),
        "upload part"
    );
    let etag = response_header(&head, "etag").expect("etag");
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let (head, (status, reply)) = call(
        "POST",
        "/sealed/m",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(
        response_header(&head, SSE).as_deref(),
        Some("AES256"),
        "complete"
    );
    // The bucket's default is the database's: AES256.
    let (_, (status, reply)) = call("GET", "/sealed", "encryption", &[], b"");
    assert_eq!(status, 200);
    assert!(
        reply.contains("<SSEAlgorithm>AES256</SSEAlgorithm>"),
        "{reply}"
    );
    assert!(
        reply.contains("<EncryptionType>NONE</EncryptionType>"),
        "{reply}"
    );
    assert_eq!(
        call("PUT", "/sealed", "encryption", &[], &aes_default).1.0,
        200
    );
    let kms = encryption_config(
        "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>\
         </ApplyServerSideEncryptionByDefault>",
    );
    assert_eq!(call("PUT", "/sealed", "encryption", &[], &kms).1.0, 400);
    assert_eq!(call("DELETE", "/sealed", "encryption", &[], b"").1.0, 204);
    let (_, (status, reply)) = call("GET", "/sealed", "encryption", &[], b"");
    assert_eq!(status, 200);
    assert!(
        reply.contains("<SSEAlgorithm>AES256</SSEAlgorithm>"),
        "{reply}"
    );

    // An object has no ?encryption: refused, never taken for the plain
    // object operation.
    for method in ["GET", "PUT", "DELETE"] {
        let (_, (status, reply)) = call(method, "/sealed/o", "encryption", &[], b"over");
        assert_eq!(status, 501, "{method}: {reply}");
    }
    assert_eq!(call("GET", "/sealed/o", "", &[], b"").1.1, "secret");

    // A policy requiring AES256 sees a form's encryption field.
    let require = r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::sealed/*",
        "Condition": {"StringNotEquals": {"s3:x-amz-server-side-encryption": "AES256"}}}}"#;
    assert_eq!(
        call("PUT", "/sealed", "policy", &[], require.as_bytes())
            .1
            .0,
        204
    );
    assert_eq!(call("PUT", "/sealed/p", "", &[], b"x").1.0, 403);
    assert_eq!(
        call("PUT", "/sealed/p", "", &[(SSE, "AES256")], b"x").1.0,
        200
    );
    let form = |encryption: Option<&str>| {
        let mut conditions = vec![
            serde_json::json!({"bucket": "sealed"}),
            serde_json::json!(["starts-with", "$key", "f"]),
        ];
        let mut fields = vec![("key", "f")];
        if let Some(value) = encryption {
            conditions.push(serde_json::json!({SSE: value}));
            fields.push((SSE, value));
        }
        signed_form(
            &key,
            &secret,
            "2099-01-01T00:00:00.000Z",
            &conditions,
            &fields,
        )
    };
    let (head, (status, reply)) = post_form(port, "sealed", &form(Some("AES256")), "f", b"x", 64);
    assert_eq!(status, 204, "{reply}");
    assert_eq!(
        response_header(&head, SSE).as_deref(),
        Some("AES256"),
        "post"
    );
    let (_, (status, reply)) = post_form(port, "sealed", &form(None), "f", b"x", 64);
    assert_eq!(status, 403, "{reply}");
}

#[test]
fn a_sub_resource_with_a_method_or_target_it_has_no_route_for_is_refused() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str| {
        send(port, method, path, query, b"body", &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/subs", "").0, 200);
    assert_eq!(call("PUT", "/subs/o", "").0, 200);
    for (method, path, query) in [
        ("DELETE", "/subs", "versioning"),
        ("DELETE", "/subs", "uploads"),
        ("DELETE", "/subs", "location"),
        ("PUT", "/subs", "versions"),
        ("HEAD", "/subs", "policyStatus"),
        ("GET", "/subs", "delete"),
        ("DELETE", "/subs/o", "uploads"),
        ("DELETE", "/subs/o", "policy"),
        ("GET", "/subs/o", "policy"),
        ("GET", "/subs/o", "publicAccessBlock"),
        ("PUT", "/subs/o", "versioning"),
        ("PUT", "/subs/o", "ownershipControls"),
        ("HEAD", "/subs/o", "tagging"),
        ("DELETE", "/subs", "metadataTable"),
        ("DELETE", "/subs", "metadataConfiguration"),
        ("PUT", "/subs/o", "renameObject"),
        ("PUT", "/subs/o", "annotation"),
        ("DELETE", "/subs/o", "annotation"),
        ("DELETE", "/subs", "object-lock"),
        ("GET", "/subs/o", "object-lock"),
        ("DELETE", "/subs/o", "object-lock"),
        ("PUT", "/subs", "retention"),
        ("DELETE", "/subs/o", "retention"),
        ("GET", "/subs", "legal-hold"),
        ("DELETE", "/subs/o", "legal-hold"),
        ("HEAD", "/subs/o", "retention"),
        ("PUT", "/new", "abac"),
        ("PUT", "/new", "metadataAnnotationTable"),
        ("PUT", "/new", "metadataInventoryTable"),
        ("PUT", "/new", "metadataJournalTable"),
        // An operation this server does not know at all: a write naming
        // it is never run as the plain write.
        ("PUT", "/new", "someFutureOperation"),
        ("DELETE", "/subs", "someFutureOperation"),
        ("PUT", "/subs/o", "someFutureOperation"),
        ("DELETE", "/subs/o", "someFutureOperation=1"),
    ] {
        let (status, reply) = call(method, path, query);
        assert_eq!(status, 501, "{method} {path}?{query}: {reply}");
    }
    // One naming two sub-resources is refused before any route.
    for (method, path, query) in [
        ("GET", "/subs", "uploads&delete"),
        ("POST", "/subs/o", "uploads&policy"),
        ("GET", "/subs/o", "uploadId=1&versioning"),
        ("DELETE", "/subs/o", "uploadId=1&versioning"),
        ("PUT", "/subs", "policy&logging"),
        ("GET", "/subs", "policy&analytics"),
    ] {
        let (status, reply) = call(method, path, query);
        assert_eq!(status, 400, "{method} {path}?{query}: {reply}");
        assert_eq!(xml_value(&reply, "Code"), "InvalidArgument");
    }
    // Neither is any other write naming an unknown operation, whichever
    // route its other parameters select.
    let (_, created) = call("POST", "/subs/keep", "uploads");
    let kept = xml_value(&created, "UploadId");
    for (method, path, query) in [
        (
            "DELETE",
            "/subs/keep",
            format!("uploadId={kept}&someFutureOperation"),
        ),
        (
            "POST",
            "/subs/keep",
            format!("uploadId={kept}&someFutureOperation"),
        ),
        ("PUT", "/subs", "versioning&someFutureOperation".to_string()),
        ("POST", "/subs", "delete&someFutureOperation".to_string()),
    ] {
        let (status, reply) = call(method, path, &query);
        assert_eq!(status, 501, "{method} {path}?{query}: {reply}");
    }
    assert_eq!(
        call("GET", "/subs/keep", &format!("uploadId={kept}")).0,
        200,
        "the upload is still there"
    );
    // The SDKs' x-id passes.
    assert_eq!(call("PUT", "/subs/x", "x-id=PutObject").0, 200);
    assert_eq!(call("DELETE", "/subs/x", "x-id=DeleteObject").0, 204);
    // A copy naming another sub-resource is refused, not taken for
    // UploadPart.
    let (_, created) = call("POST", "/subs/up", "uploads");
    let upload = xml_value(&created, "UploadId");
    let (_, (status, reply)) = send_with(
        port,
        "PUT",
        "/subs/up",
        &format!("partNumber=1&uploadId={upload}&versioning"),
        &[("x-amz-copy-source", "/subs/o")],
        b"",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 400, "{reply}");
    assert_eq!(xml_value(&reply, "Code"), "InvalidArgument");
    // Nothing was deleted, created or overwritten.
    assert_eq!(call("HEAD", "/subs", "").0, 200);
    assert_eq!(call("HEAD", "/new", "").0, 404);
    assert_eq!(call("GET", "/subs/o", ""), (200, "body".to_string()));
    // The plain operations still route.
    assert_eq!(call("GET", "/subs", "list-type=2").0, 200);
    assert_eq!(call("DELETE", "/subs/o", "").0, 204);
}

/// The three headers naming customer-provided key `key` (`prefix`
/// `x-amz-`; a copy source's with `x-amz-copy-source-`).
fn customer_headers(prefix: &str, key: [u8; 32]) -> Vec<(String, String)> {
    use base64::Engine as _;
    use md5::Digest;
    let base64 = base64::engine::general_purpose::STANDARD;
    vec![
        (
            format!("{prefix}server-side-encryption-customer-algorithm"),
            "AES256".to_string(),
        ),
        (
            format!("{prefix}server-side-encryption-customer-key"),
            base64.encode(key),
        ),
        (
            format!("{prefix}server-side-encryption-customer-key-MD5"),
            base64.encode(md5::Md5::digest(key)),
        ),
    ]
}

/// `headers` as the request helpers take them.
fn header_refs(headers: &[(String, String)]) -> Vec<(&str, &str)> {
    headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect()
}

/// Every file under `dir`, read whole.
fn files_hold(dir: &std::path::Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir).expect("dir").any(|entry| {
        let path = entry.expect("entry").path();
        path.is_file()
            && std::fs::read(&path)
                .is_ok_and(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
    })
}

#[test]
fn customer_provided_keys_encrypt_objects_only_their_key_reads() {
    const ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
    const KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-MD5";
    let (right, wrong) = ([0x41u8; 32], [0x42u8; 32]);
    let key_a = customer_headers("x-amz-", right);
    let key_b = customer_headers("x-amz-", wrong);

    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "keys");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    assert_eq!(call("PUT", "/vault", "", &[], b"").1.0, 200);

    // A body over several chunks, with a marker that must not reach disk.
    let marker = b"CUSTOMER-KEY-CANARY";
    let mut body = Vec::new();
    while body.len() < 3 * 65536 + 100 {
        body.extend_from_slice(marker);
        body.extend_from_slice(&(body.len() as u64).to_le_bytes());
    }
    let (head, (status, reply)) = call("PUT", "/vault/o", "", &header_refs(&key_a), &body);
    assert_eq!(status, 200, "{reply}");
    assert_eq!(response_header(&head, ALGORITHM).as_deref(), Some("AES256"));
    assert_eq!(
        response_header(&head, KEY_MD5).as_deref(),
        Some(key_a[2].1.as_str())
    );
    assert_eq!(response_header(&head, "x-amz-server-side-encryption"), None);
    let etag = response_header(&head, "etag").expect("etag");
    assert_ne!(
        etag.trim_matches('"'),
        md5_hex(&body),
        "not the MD5 of the data"
    );

    // Read with its key only.
    let (_, (status, reply)) = call("GET", "/vault/o", "", &[], b"");
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("InvalidRequest"), "{reply}");
    assert_eq!(
        call("GET", "/vault/o", "", &header_refs(&key_b), b"").1.0,
        403
    );
    let (head, (status, got)) = call("GET", "/vault/o", "", &header_refs(&key_a), b"");
    assert_eq!(status, 200);
    assert_eq!(got.as_bytes(), latin1(&body).as_bytes());
    assert_eq!(
        response_header(&head, KEY_MD5).as_deref(),
        Some(key_a[2].1.as_str())
    );
    let (head, (status, _)) = call("HEAD", "/vault/o", "", &header_refs(&key_a), b"");
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "content-length"),
        Some(body.len().to_string())
    );
    assert_eq!(call("HEAD", "/vault/o", "", &[], b"").1.0, 400);
    // Ranges across chunk boundaries, and a suffix.
    for (range, from, to) in [
        ("bytes=65530-65545", 65530, 65546),
        ("bytes=0-0", 0, 1),
        ("bytes=131000-200000", 131000, 200001),
    ] {
        let mut extra = header_refs(&key_a);
        extra.push(("range", range));
        let (_, (status, got)) = call("GET", "/vault/o", "", &extra, b"");
        assert_eq!(status, 206, "{range}");
        assert_eq!(got, latin1(&body[from..to.min(body.len())]), "{range}");
    }
    let mut extra = header_refs(&key_a);
    extra.push(("range", "bytes=-7"));
    assert_eq!(
        call("GET", "/vault/o", "", &extra, b"").1.1,
        latin1(&body[body.len() - 7..])
    );
    // A key for an object without one.
    assert_eq!(call("PUT", "/vault/plain", "", &[], b"open").1.0, 200);
    assert_eq!(
        call("GET", "/vault/plain", "", &header_refs(&key_a), b"")
            .1
            .0,
        400
    );

    // Multipart: every part with the upload's key.
    let (head, (status, created)) = call("POST", "/vault/m", "uploads", &header_refs(&key_a), b"");
    assert_eq!(status, 200, "{created}");
    assert_eq!(response_header(&head, ALGORITHM).as_deref(), Some("AES256"));
    let upload = xml_value(&created, "UploadId");
    let part = |number: u32, extra: &[(&str, &str)], data: &[u8]| {
        call(
            "PUT",
            "/vault/m",
            &format!("uploadId={upload}&partNumber={number}"),
            extra,
            data,
        )
    };
    let first: Vec<u8> = (0..5 * 1024 * 1024).map(|n| (n % 253) as u8).collect();
    let second = b"the last part".to_vec();
    assert_eq!(part(1, &[], b"x").1.0, 400, "a part without the key");
    assert_eq!(
        part(1, &header_refs(&key_b), b"x").1.0,
        403,
        "a part with another key"
    );
    let mut etags = Vec::new();
    for (number, data) in [(1, &first), (2, &second)] {
        let (head, (status, reply)) = part(number, &header_refs(&key_a), data);
        assert_eq!(status, 200, "{reply}");
        assert_eq!(response_header(&head, ALGORITHM).as_deref(), Some("AES256"));
        etags.push(response_header(&head, "etag").expect("etag"));
    }
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let (head, (status, reply)) = call(
        "POST",
        "/vault/m",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(response_header(&head, ALGORITHM), None, "not on completion");
    let whole: Vec<u8> = [first.clone(), second.clone()].concat();
    assert_eq!(call("GET", "/vault/m", "", &[], b"").1.0, 400);
    let (_, (status, got)) = call("GET", "/vault/m", "", &header_refs(&key_a), b"");
    assert_eq!(status, 200);
    assert_eq!(got, latin1(&whole));
    let mut extra = header_refs(&key_a);
    let across = format!("bytes={}-{}", first.len() - 3, first.len() + 4);
    extra.push(("range", &across));
    assert_eq!(
        call("GET", "/vault/m", "", &extra, b"").1.1,
        latin1(&whole[first.len() - 3..first.len() + 5])
    );
    // By part: each part's plaintext, at its plaintext offset.
    let (head, (status, got)) = call("GET", "/vault/m", "partNumber=2", &header_refs(&key_a), b"");
    assert_eq!(status, 206);
    assert_eq!(got, latin1(&second));
    assert_eq!(
        response_header(&head, "content-range"),
        Some(format!(
            "bytes {}-{}/{}",
            first.len(),
            whole.len() - 1,
            whole.len()
        ))
    );
    // A multipart upload without a key refuses a part with one.
    let (_, (_, created)) = call("POST", "/vault/n", "uploads", &[], b"");
    let plain_upload = xml_value(&created, "UploadId");
    assert_eq!(
        call(
            "PUT",
            "/vault/n",
            &format!("uploadId={plain_upload}&partNumber=1"),
            &header_refs(&key_a),
            b"x",
        )
        .1
        .0,
        400
    );

    // A copy needs the source's key (see the copy test).
    let (_, (status, reply)) = call(
        "PUT",
        "/vault/c",
        "",
        &[("x-amz-copy-source", "/vault/o")],
        b"",
    );
    assert_eq!(status, 400, "{reply}");

    // A bucket blocking SSE-C refuses new writes with keys; what it holds
    // stays readable.
    let block = b"<ServerSideEncryptionConfiguration><Rule><BlockedEncryptionTypes>\
        <EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes></Rule>\
        </ServerSideEncryptionConfiguration>";
    assert_eq!(call("PUT", "/vault", "encryption", &[], block).1.0, 200);
    assert_eq!(
        call("PUT", "/vault/p", "", &header_refs(&key_a), b"x").1.0,
        403
    );
    assert_eq!(
        call("POST", "/vault/p", "uploads", &header_refs(&key_a), b"")
            .1
            .0,
        403
    );
    assert_eq!(
        call("GET", "/vault/o", "", &header_refs(&key_a), b"").1.0,
        200
    );

    // Nothing of the plaintext reached the server's files (the database is
    // not encrypted at rest: only the customer's key hides it).
    assert!(!files_hold(&server.dir, marker));
}

#[test]
fn customer_provided_keys_need_tls() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    assert_eq!(
        send(port, "PUT", "/clear", "", b"", &key, Some(&secret)).0,
        200
    );
    let headers = customer_headers("x-amz-", [7u8; 32]);
    let extra = header_refs(&headers);
    let (_, (status, reply)) = send_with(
        port,
        "PUT",
        "/clear/o",
        "",
        &extra,
        b"x",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("secure connection"), "{reply}");
}

#[test]
fn an_object_with_a_customer_provided_key_is_not_reported_aes256() {
    // Encrypted at rest too: every other object says AES256.
    let server = start_server_tls(true, true);
    let (_, key, secret) = account_with_key(&server, "both");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    const SSE: &str = "x-amz-server-side-encryption";
    let customer = customer_headers("x-amz-", [9u8; 32]);
    assert_eq!(call("PUT", "/both", "", &[], b"").1.0, 200);
    let (head, _) = call("PUT", "/both/plain", "", &[], b"x");
    assert_eq!(response_header(&head, SSE).as_deref(), Some("AES256"));
    let (head, (status, _)) = call("PUT", "/both/o", "", &header_refs(&customer), b"x");
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, SSE), None, "put");
    let (head, (_, created)) = call("POST", "/both/m", "uploads", &header_refs(&customer), b"");
    assert_eq!(response_header(&head, SSE), None, "create upload");
    let upload = xml_value(&created, "UploadId");
    let (head, (status, _)) = call(
        "PUT",
        "/both/m",
        &format!("uploadId={upload}&partNumber=1"),
        &header_refs(&customer),
        b"only part",
    );
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, SSE), None, "upload part");
    let etag = response_header(&head, "etag").expect("etag");
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
         </CompleteMultipartUpload>"
    );
    let (head, (status, reply)) = call(
        "POST",
        "/both/m",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(response_header(&head, SSE), None, "complete");
    let (head, (status, _)) = call("HEAD", "/both/m", "", &header_refs(&customer), b"");
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, SSE), None, "head");
}

#[test]
fn policies_see_a_form_uploads_customer_provided_key() {
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "forms");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    assert_eq!(call("PUT", "/nossec", "", &[], b"").1.0, 200);
    let deny = r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::nossec/*", "Condition": {"StringEquals":
        {"s3:x-amz-server-side-encryption-customer-algorithm": "AES256"}}}}"#;
    assert_eq!(
        call("PUT", "/nossec", "policy", &[], deny.as_bytes()).1.0,
        204
    );
    let customer = customer_headers("x-amz-", [5u8; 32]);
    assert_eq!(
        call("PUT", "/nossec/p", "", &header_refs(&customer), b"x")
            .1
            .0,
        403
    );
    let form = |with_key: bool| {
        let mut conditions = vec![
            serde_json::json!({"bucket": "nossec"}),
            serde_json::json!(["starts-with", "$key", "f"]),
        ];
        let mut fields = vec![("key", "f")];
        if with_key {
            for (name, value) in &customer {
                conditions.push(serde_json::json!({name.clone(): value.clone()}));
                fields.push((name.as_str(), value.as_str()));
            }
        }
        signed_form(
            &key,
            &secret,
            "2099-01-01T00:00:00.000Z",
            &conditions,
            &fields,
        )
    };
    let (_, (status, reply)) = post_form_tls(&server, "nossec", &form(true), "f", b"x");
    assert_eq!(status, 403, "{reply}");
    assert_eq!(
        call("HEAD", "/nossec/f", "", &header_refs(&customer), b"")
            .1
            .0,
        404
    );
    let (_, (status, reply)) = post_form_tls(&server, "nossec", &form(false), "f", b"x");
    assert_eq!(status, 204, "{reply}");
}

#[test]
fn a_presigned_url_signs_the_customer_keys_algorithm_only() {
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "presign");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    assert_eq!(call("PUT", "/pre", "", &[], b"").1.0, 200);
    let customer = customer_headers("x-amz-", [6u8; 32]);
    assert_eq!(
        call("PUT", "/pre/o", "", &header_refs(&customer), b"sealed")
            .1
            .0,
        200
    );
    // As S3 documents: the algorithm in the signature, the key and its MD5
    // sent with the request.
    let get = |signed: &[&str]| {
        let amz_date = amz_now();
        let credential = sigv4::Credential {
            access_key: key.clone(),
            date: amz_date[..8].to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        };
        let encode = |text: &str| {
            percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC)
                .to_string()
        };
        let host = format!("127.0.0.1:{}", server.s3_port);
        let mut headers = vec![("host".to_string(), host.clone())];
        for name in signed {
            let value = customer
                .iter()
                .find(|(header, _)| header.eq_ignore_ascii_case(name))
                .expect("a customer header")
                .1
                .clone();
            headers.push((name.to_ascii_lowercase(), value));
        }
        headers.sort();
        let names: Vec<String> = headers.iter().map(|(name, _)| name.clone()).collect();
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={amz_date}&X-Amz-Expires=300&X-Amz-SignedHeaders={}",
            encode(&format!("{key}/{}", credential.scope())),
            encode(&names.join(";"))
        );
        let canonical = sigv4::canonical_request(
            "GET",
            "/pre/o",
            &query,
            &headers,
            &names,
            sigv4::UNSIGNED_PAYLOAD,
        )
        .unwrap();
        let signature = sigv4::sign(
            &sigv4::signing_key(&secret, &credential),
            &sigv4::string_to_sign(&amz_date, &credential.scope(), &canonical),
        );
        let mut request = format!(
            "GET /pre/o?{query}&X-Amz-Signature={signature} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n"
        );
        for (name, value) in &customer {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        let mut stream = tls_stream(&server);
        stream.write_all(request.as_bytes()).expect("send");
        let mut raw = Vec::new();
        let _ = stream.read_to_end(&mut raw);
        parse_response(&raw)
    };
    let (_, (status, reply)) = get(&["x-amz-server-side-encryption-customer-algorithm"]);
    assert_eq!((status, reply.as_str()), (200, "sealed"));
    // The algorithm itself must be signed.
    let (_, (status, reply)) = get(&[]);
    assert_eq!(status, 403, "{reply}");
}

#[test]
fn a_bucket_blocking_customer_keys_refuses_a_part_with_one() {
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "block");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    assert_eq!(call("PUT", "/blk", "", &[], b"").1.0, 200);
    let customer = customer_headers("x-amz-", [8u8; 32]);
    let (_, (_, created)) = call("POST", "/blk/m", "uploads", &header_refs(&customer), b"");
    let upload = xml_value(&created, "UploadId");
    let block = b"<ServerSideEncryptionConfiguration><Rule><BlockedEncryptionTypes>\
        <EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes></Rule>\
        </ServerSideEncryptionConfiguration>";
    assert_eq!(call("PUT", "/blk", "encryption", &[], block).1.0, 200);
    let (_, (status, reply)) = call(
        "PUT",
        "/blk/m",
        &format!("uploadId={upload}&partNumber=1"),
        &header_refs(&customer),
        b"part",
    );
    assert_eq!(status, 403, "{reply}");
}

#[test]
fn copies_decrypt_and_encrypt_again_as_their_keys_say() {
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "copier");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    let (a, b) = ([0xa1u8; 32], [0xb2u8; 32]);
    let target = |key: [u8; 32]| customer_headers("x-amz-", key);
    let source = |key: [u8; 32]| customer_headers("x-amz-copy-source-", key);
    // Several chunks, so the copies cross chunk boundaries.
    let data: Vec<u8> = (0..200_000u32).map(|n| (n % 241) as u8).collect();
    assert_eq!(call("PUT", "/copies", "", &[], b"").1.0, 200);
    assert_eq!(
        call("PUT", "/copies/o", "", &header_refs(&target(a)), &data)
            .1
            .0,
        200
    );
    assert_eq!(call("PUT", "/copies/plain", "", &[], &data).1.0, 200);
    let copy = |to: &str, from: &str, headers: &[Vec<(String, String)>]| {
        let mut extra: Vec<(&str, &str)> = vec![("x-amz-copy-source", from)];
        for set in headers {
            extra.extend(header_refs(set));
        }
        call("PUT", to, "", &extra, b"")
    };
    let read = |path: &str, key: Option<[u8; 32]>| {
        let headers = key.map(target).unwrap_or_default();
        call("GET", path, "", &header_refs(&headers), b"").1
    };
    let whole = latin1(&data);

    // Keyed to plain, plain to keyed, one key to another, the same key.
    let (_, (status, reply)) = copy("/copies/c1", "/copies/o", &[source(a)]);
    assert_eq!(status, 200, "{reply}");
    assert_eq!(read("/copies/c1", None), (200, whole.clone()));
    assert_eq!(read("/copies/c1", Some(a)).0, 400);
    let (head, (status, _)) = copy("/copies/c2", "/copies/plain", &[target(b)]);
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "x-amz-server-side-encryption-customer-key-MD5").as_deref(),
        Some(target(b)[2].1.as_str())
    );
    assert_eq!(read("/copies/c2", Some(b)), (200, whole.clone()));
    assert_eq!(read("/copies/c2", None).0, 400);
    assert_eq!(
        copy("/copies/c3", "/copies/o", &[source(a), target(b)]).1.0,
        200
    );
    assert_eq!(read("/copies/c3", Some(b)), (200, whole.clone()));
    assert_eq!(read("/copies/c3", Some(a)).0, 403);
    assert_eq!(
        copy("/copies/c4", "/copies/o", &[source(a), target(a)]).1.0,
        200
    );
    assert_eq!(read("/copies/c4", Some(a)), (200, whole.clone()));
    // The source's key: required, and the right one.
    assert_eq!(copy("/copies/c5", "/copies/o", &[]).1.0, 400);
    assert_eq!(copy("/copies/c5", "/copies/o", &[source(b)]).1.0, 403);
    // A copy onto itself with another key rotates it; with the same, and
    // nothing else changed, it is refused.
    assert_eq!(
        copy("/copies/o", "/copies/o", &[source(a), target(a)]).1.0,
        400
    );
    assert_eq!(
        copy("/copies/o", "/copies/o", &[source(a), target(b)]).1.0,
        200
    );
    assert_eq!(read("/copies/o", Some(b)), (200, whole.clone()));
    assert_eq!(read("/copies/o", Some(a)).0, 403);

    // Parts copied into an upload with a key: ranges of a keyed and a plain
    // source, each encrypted with the upload's key.
    let (_, (_, created)) = call(
        "POST",
        "/copies/m",
        "uploads",
        &header_refs(&target(a)),
        b"",
    );
    let upload = xml_value(&created, "UploadId");
    let part = |number: u32, from: &str, range: &str, headers: &[Vec<(String, String)>]| {
        let mut extra: Vec<(&str, &str)> = vec![
            ("x-amz-copy-source", from),
            ("x-amz-copy-source-range", range),
        ];
        for set in headers {
            extra.extend(header_refs(set));
        }
        call(
            "PUT",
            "/copies/m",
            &format!("uploadId={upload}&partNumber={number}"),
            &extra,
            b"",
        )
    };
    let first_range = format!("bytes=0-{}", 5 * 1024 * 1024 - 1);
    // A source long enough for a first part: the plain object, repeated.
    let big: Vec<u8> = (0..5 * 1024 * 1024 + 10).map(|n| (n % 239) as u8).collect();
    assert_eq!(
        call("PUT", "/copies/big", "", &header_refs(&target(b)), &big)
            .1
            .0,
        200
    );
    assert_eq!(
        part(1, "/copies/big", &first_range, &[source(b)]).1.0,
        400,
        "without the upload's key"
    );
    let (head1, (status, reply1)) = part(1, "/copies/big", &first_range, &[source(b), target(a)]);
    assert_eq!(status, 200, "{reply1}");
    let (head2, (status, reply2)) = part(2, "/copies/plain", "bytes=65530-65545", &[target(a)]);
    assert_eq!(status, 200, "{reply2}");
    for head in [&head1, &head2] {
        assert_eq!(
            response_header(head, "x-amz-server-side-encryption-customer-algorithm").as_deref(),
            Some("AES256")
        );
    }
    let part_etag = |reply: &str| xml_value(reply, "ETag").replace("&quot;", "\"");
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        part_etag(&reply1),
        part_etag(&reply2)
    );
    let (_, (status, reply)) = call(
        "POST",
        "/copies/m",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    let expected = [&big[..5 * 1024 * 1024], &data[65530..65546]].concat();
    assert_eq!(read("/copies/m", Some(a)), (200, latin1(&expected)));
}

#[test]
fn a_copy_whose_client_leaves_still_finishes_and_lets_its_source_go() {
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "leaver");
    let call = |method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(&server, method, path, "", extra, body, &key, Some(&secret)).1
    };
    let (a, b) = ([0x5au8; 32], [0x6bu8; 32]);
    // Large enough that the copy (decrypted and encrypted again) is still
    // running when the client goes.
    let data: Vec<u8> = (0..48u32 << 20).map(|n| (n % 253) as u8).collect();
    assert_eq!(call("PUT", "/leave", &[], b"").0, 200);
    let source_key = customer_headers("x-amz-", a);
    assert_eq!(
        call("PUT", "/leave/src", &header_refs(&source_key), &data).0,
        200
    );
    // The copy is sent and the connection dropped before the answer.
    let mut headers = vec![("x-amz-copy-source".to_string(), "/leave/src".to_string())];
    headers.extend(customer_headers("x-amz-copy-source-", a));
    headers.extend(customer_headers("x-amz-", b));
    let request = signed_request(
        server.s3_port,
        "PUT",
        "/leave/dst",
        "",
        &header_refs(&headers),
        b"",
        &key,
        Some(&secret),
    );
    let mut stream = tls_stream(&server);
    stream.write_all(request.as_bytes()).expect("send");
    stream.flush().expect("flush");
    std::thread::sleep(Duration::from_millis(50));
    drop(stream);
    // The copy runs to its end (releasing what it holds on the source), so
    // the object appears.
    let target_key = customer_headers("x-amz-", b);
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let (status, _) = call("HEAD", "/leave/dst", &header_refs(&target_key), b"");
        if status == 200 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the copy never finished after its client left: {status}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let (status, body) = send_tls(
        &server,
        "GET",
        "/leave/dst",
        "",
        &header_refs(&target_key),
        b"",
        &key,
        Some(&secret),
    )
    .1;
    assert_eq!(status, 200);
    assert!(body == latin1(&data), "the copy holds the source's bytes");
}

/// The time `secs` from now as an ISO 8601 date and time (a retain-until
/// date).
fn iso_in(secs: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let basic = sigv4_date((now + secs) as u64);
    format!(
        "{}-{}-{}T{}:{}:{}Z",
        &basic[..4],
        &basic[4..6],
        &basic[6..8],
        &basic[9..11],
        &basic[11..13],
        &basic[13..15]
    )
}

fn content_md5(body: &[u8]) -> String {
    use base64::Engine as _;
    use md5::Digest;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body))
}

#[test]
fn object_lock_keeps_versions_its_retention_and_legal_hold_protect() {
    const MODE: &str = "x-amz-object-lock-mode";
    const UNTIL: &str = "x-amz-object-lock-retain-until-date";
    const HOLD: &str = "x-amz-object-lock-legal-hold";
    const BYPASS: &str = "x-amz-bypass-governance-retention";
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    // A document sent with its Content-MD5, as S3 requires for these.
    let put_doc = |path: &str, query: &str, extra: &[(&str, &str)], body: &str| {
        let md5 = content_md5(body.as_bytes());
        let mut headers = extra.to_vec();
        headers.push(("content-md5", &md5));
        call("PUT", path, query, &headers, body.as_bytes()).1
    };
    let lock_config = |rule: &str| {
        format!(
            "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>{rule}\
             </ObjectLockConfiguration>"
        )
    };
    let retention = |mode: &str, until: &str| {
        format!(
            "<Retention><Mode>{mode}</Mode><RetainUntilDate>{until}</RetainUntilDate></Retention>"
        )
    };
    let version_of = |head: &str| response_header(head, "x-amz-version-id").expect("a version");

    // A bucket without Object Lock has no configuration, takes none while
    // unversioned, and refuses lock headers.
    assert_eq!(call("PUT", "/plain", "", &[], b"").1.0, 200);
    let (status, reply) = call("GET", "/plain", "object-lock", &[], b"").1;
    assert_eq!(status, 404, "{reply}");
    assert!(
        reply.contains("ObjectLockConfigurationNotFoundError"),
        "{reply}"
    );
    let (status, reply) = put_doc("/plain", "object-lock", &[], &lock_config(""));
    assert_eq!(status, 409, "{reply}");
    assert!(reply.contains("InvalidBucketState"), "{reply}");
    let (status, reply) = call("PUT", "/plain/o", "", &[(HOLD, "ON")], b"x").1;
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("InvalidRequest"), "{reply}");
    assert_eq!(call("PUT", "/plain/o", "", &[], b"x").1.0, 200);
    let (status, reply) = put_doc(
        "/plain/o",
        "legal-hold",
        &[],
        "<LegalHold><Status>ON</Status></LegalHold>",
    );
    assert_eq!(status, 400, "{reply}");

    // Created with Object Lock: versioning is enabled, and stays so.
    assert_eq!(
        call(
            "PUT",
            "/locked",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    let (_, reply) = call("GET", "/locked", "versioning", &[], b"").1;
    assert!(reply.contains("<Status>Enabled</Status>"), "{reply}");
    let (status, reply) = call(
        "PUT",
        "/locked",
        "versioning",
        &[],
        b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>",
    )
    .1;
    assert_eq!(status, 409, "{reply}");
    let (status, reply) = call("GET", "/locked", "object-lock", &[], b"").1;
    assert_eq!(status, 200, "{reply}");
    assert!(
        reply.contains("<ObjectLockEnabled>Enabled</ObjectLockEnabled>"),
        "{reply}"
    );
    assert!(!reply.contains("<Rule>"), "{reply}");

    // A default retention: the configuration needs its Content-MD5, and
    // every write after it an integrity check of its body.
    let governance_day = lock_config(
        "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule>",
    );
    assert_eq!(
        call(
            "PUT",
            "/locked",
            "object-lock",
            &[],
            governance_day.as_bytes()
        )
        .1
        .0,
        400
    );
    assert_eq!(
        put_doc("/locked", "object-lock", &[], &governance_day).0,
        200
    );
    let (_, reply) = call("GET", "/locked", "object-lock", &[], b"").1;
    assert!(
        reply.contains("<Mode>GOVERNANCE</Mode><Days>1</Days>"),
        "{reply}"
    );
    let (status, reply) = call("PUT", "/locked/d", "", &[], b"data").1;
    assert_eq!(status, 400, "{reply}");
    let md5 = content_md5(b"data");
    let (head, (status, _)) = call("PUT", "/locked/d", "", &[("content-md5", &md5)], b"data");
    assert_eq!(status, 200);
    let d = version_of(&head);
    let (head, (status, _)) = call("HEAD", "/locked/d", "", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, MODE).as_deref(), Some("GOVERNANCE"));
    assert!(response_header(&head, UNTIL).is_some(), "{head}");
    assert_eq!(response_header(&head, HOLD), None, "no hold was ever set");
    let (_, reply) = call("GET", "/locked/d", "retention", &[], b"").1;
    assert!(reply.contains("<Mode>GOVERNANCE</Mode>"), "{reply}");

    // Protected: the version stays; a delete marker may still hide it, and
    // a GOVERNANCE retention gives way to a bypass.
    let version = format!("versionId={d}");
    let (status, reply) = call("DELETE", "/locked/d", &version, &[], b"").1;
    assert_eq!(status, 403, "{reply}");
    assert!(reply.contains("AccessDenied"), "{reply}");
    let (head, (status, _)) = call("DELETE", "/locked/d", "", &[], b"");
    assert_eq!(status, 204);
    assert_eq!(
        response_header(&head, "x-amz-delete-marker").as_deref(),
        Some("true")
    );
    assert_eq!(
        call("DELETE", "/locked/d", &version, &[(BYPASS, "true")], b"")
            .1
            .0,
        204
    );

    // COMPLIANCE: extended only, never bypassed.
    assert_eq!(
        put_doc("/locked", "object-lock", &[], &lock_config("")).0,
        200
    );
    let until = iso_in(3600);
    let md5 = content_md5(b"kept");
    let (status, reply) = call(
        "PUT",
        "/locked/c",
        "",
        &[(MODE, "COMPLIANCE"), (UNTIL, &until)],
        b"kept",
    )
    .1;
    assert_eq!(status, 400, "a retention needs an integrity check: {reply}");
    let (head, (status, reply)) = call(
        "PUT",
        "/locked/c",
        "",
        &[(MODE, "COMPLIANCE"), (UNTIL, &until), ("content-md5", &md5)],
        b"kept",
    );
    assert_eq!(status, 200, "{reply}");
    let c = format!("versionId={}", version_of(&head));
    assert_eq!(
        call("DELETE", "/locked/c", &c, &[(BYPASS, "true")], b"")
            .1
            .0,
        403
    );
    for (mode, until) in [("COMPLIANCE", iso_in(60)), ("GOVERNANCE", iso_in(7200))] {
        let (status, reply) = put_doc(
            "/locked/c",
            &format!("retention&{c}"),
            &[(BYPASS, "true")],
            &retention(mode, &until),
        );
        assert_eq!(status, 403, "{mode} {until}: {reply}");
    }
    let (status, reply) = put_doc(
        "/locked/c",
        &format!("retention&{c}"),
        &[],
        &retention("COMPLIANCE", &iso_in(7200)),
    );
    assert_eq!(status, 200, "{reply}");
    let (_, reply) = call("GET", "/locked/c", &format!("retention&{c}"), &[], b"").1;
    assert!(reply.contains("<Mode>COMPLIANCE</Mode>"), "{reply}");
    // An empty Retention removes nothing COMPLIANCE keeps.
    let (status, _) = put_doc(
        "/locked/c",
        &format!("retention&{c}"),
        &[(BYPASS, "true")],
        "<Retention/>",
    );
    assert_eq!(status, 403);

    // A legal hold protects whatever the retention, bypass or not.
    let (head, (status, reply)) = call("PUT", "/locked/h", "", &[(HOLD, "ON")], b"held");
    assert_eq!(
        status, 200,
        "a hold alone needs no integrity check: {reply}"
    );
    let h = format!("versionId={}", version_of(&head));
    let (_, reply) = call("GET", "/locked/h", "legal-hold", &[], b"").1;
    assert!(reply.contains("<Status>ON</Status>"), "{reply}");
    let (status, reply) = call("GET", "/locked/h", "retention", &[], b"").1;
    assert_eq!(status, 404, "{reply}");
    assert!(reply.contains("NoSuchObjectLockConfiguration"), "{reply}");
    assert_eq!(
        call("DELETE", "/locked/h", &h, &[(BYPASS, "true")], b"")
            .1
            .0,
        403
    );
    let batch = format!(
        "<Delete><Object><Key>h</Key><VersionId>{}</VersionId></Object></Delete>",
        &h["versionId=".len()..]
    );
    let (status, reply) = call(
        "POST",
        "/locked",
        "delete",
        &[
            ("content-md5", &content_md5(batch.as_bytes())),
            (BYPASS, "true"),
        ],
        batch.as_bytes(),
    )
    .1;
    assert_eq!(status, 200, "{reply}");
    assert!(reply.contains("<Code>AccessDenied</Code>"), "{reply}");
    let (status, reply) = put_doc(
        "/locked/h",
        &format!("legal-hold&{h}"),
        &[],
        "<LegalHold><Status>OFF</Status></LegalHold>",
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(call("DELETE", "/locked/h", &h, &[], b"").1.0, 204);

    // Malformed requests.
    for (query, body) in [
        ("retention", retention("WORM", &iso_in(60))),
        ("retention", retention("GOVERNANCE", &iso_in(-60))),
        (
            "legal-hold",
            "<LegalHold><Status>on</Status></LegalHold>".to_string(),
        ),
    ] {
        let (status, reply) = put_doc("/locked/c", query, &[], &body);
        assert_eq!(status, 400, "{query} {body}: {reply}");
    }
    let (status, reply) = call("PUT", "/locked/x", "", &[(MODE, "GOVERNANCE")], b"x").1;
    assert_eq!(status, 400, "a mode needs its date: {reply}");
    drop(server);
}

#[test]
fn object_lock_follows_policies_and_reaches_uploads_and_forms() {
    use serde_json::json;
    const MODE: &str = "x-amz-object-lock-mode";
    const UNTIL: &str = "x-amz-object-lock-retain-until-date";
    const HOLD: &str = "x-amz-object-lock-legal-hold";
    const BYPASS: &str = "x-amz-bypass-governance-retention";
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "lockers");
    let root = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            path,
            query,
            extra,
            body,
            &root_key,
            Some(&root_secret),
        )
    };
    assert_eq!(
        root(
            "PUT",
            "/vault",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    // A user who may do anything but bypass GOVERNANCE retention, or keep
    // an object more than ten days.
    let policy = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:BypassGovernanceRetention", "Resource": "*"},
        {"Effect": "Deny", "Action": ["s3:PutObject", "s3:PutObjectRetention"], "Resource": "*",
         "Condition": {"NumericGreaterThan": {"s3:object-lock-remaining-retention-days": "10"}}}
    ]})
    .to_string();
    let (user_key, user_secret) = user_with_key(&server, &account, "clerk", Some(&policy));
    let user = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            path,
            query,
            extra,
            body,
            &user_key,
            Some(&user_secret),
        )
    };
    // A reader who may read objects but not their locks.
    let reader_policy = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}
    ]})
    .to_string();
    let (reader_key, reader_secret) =
        user_with_key(&server, &account, "reader", Some(&reader_policy));

    let md5 = content_md5(b"x");
    let in_days = |days: i64| iso_in(days * 86_400);
    let (status, reply) = user(
        "PUT",
        "/vault/long",
        "",
        &[
            (MODE, "GOVERNANCE"),
            (UNTIL, &in_days(30)),
            ("content-md5", &md5),
        ],
        b"x",
    )
    .1;
    assert_eq!(status, 403, "thirty days is past the policy's ten: {reply}");
    let (head, (status, reply)) = user(
        "PUT",
        "/vault/g",
        "",
        &[
            (MODE, "GOVERNANCE"),
            (UNTIL, &in_days(5)),
            ("content-md5", &md5),
        ],
        b"x",
    );
    assert_eq!(status, 200, "{reply}");
    let version = format!(
        "versionId={}",
        response_header(&head, "x-amz-version-id").unwrap()
    );
    let retention = |days: i64| {
        format!(
            "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
            in_days(days)
        )
    };
    let put_retention = |as_user: bool, days: i64| {
        let body = retention(days);
        let md5 = content_md5(body.as_bytes());
        let extra = [("content-md5", md5.as_str())];
        let query = format!("retention&{version}");
        if as_user {
            user("PUT", "/vault/g", &query, &extra, body.as_bytes()).1
        } else {
            root("PUT", "/vault/g", &query, &extra, body.as_bytes()).1
        }
    };
    assert_eq!(
        put_retention(true, 30).0,
        403,
        "the body's retention is checked"
    );
    assert_eq!(put_retention(true, 8).0, 200);
    // The user's bypass is denied: the version stays. The root's is not.
    assert_eq!(
        user("DELETE", "/vault/g", &version, &[(BYPASS, "true")], b"")
            .1
            .0,
        403
    );
    // The reader sees the object, not its lock.
    let (head, (status, _)) = send_with(
        port,
        "HEAD",
        "/vault/g",
        "",
        &[],
        b"",
        &reader_key,
        Some(&reader_secret),
    );
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, MODE), None, "{head}");
    assert_eq!(response_header(&head, UNTIL), None, "{head}");
    let (head, _) = user("HEAD", "/vault/g", "", &[], b"");
    assert_eq!(response_header(&head, MODE).as_deref(), Some("GOVERNANCE"));
    assert_eq!(
        root("DELETE", "/vault/g", &version, &[(BYPASS, "true")], b"")
            .1
            .0,
        204
    );

    // A multipart upload's lock is the object's when it completes.
    let (_, (status, reply)) = root("POST", "/vault/mp", "uploads", &[(HOLD, "ON")], b"");
    assert_eq!(status, 200, "{reply}");
    let upload = reply
        .split("<UploadId>")
        .nth(1)
        .and_then(|rest| rest.split("</UploadId>").next())
        .unwrap()
        .to_string();
    let (head, (status, _)) = root(
        "PUT",
        "/vault/mp",
        &format!("partNumber=1&uploadId={upload}"),
        &[],
        b"part",
    );
    assert_eq!(status, 200);
    let etag = response_header(&head, "etag").unwrap();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let (_, (status, reply)) = root(
        "POST",
        "/vault/mp",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    let (_, reply) = root("GET", "/vault/mp", "legal-hold", &[], b"").1;
    assert!(reply.contains("<Status>ON</Status>"), "{reply}");
    // A bucket without Object Lock refuses an upload asking for one.
    assert_eq!(root("PUT", "/open", "", &[], b"").1.0, 200);
    assert_eq!(
        root("POST", "/open/mp", "uploads", &[(HOLD, "ON")], b"")
            .1
            .0,
        400
    );

    // A form upload asks in its fields.
    let later = "2099-01-01T00:00:00.000Z";
    let form = signed_form(
        &root_key,
        &root_secret,
        later,
        &[
            json!({"bucket": "vault"}),
            json!({"key": "form"}),
            json!({HOLD: "ON"}),
        ],
        &[("key", "form"), (HOLD, "ON")],
    );
    let (_, (status, reply)) = post_form(port, "vault", &form, "f.txt", b"form", 4);
    assert_eq!(status, 204, "{reply}");
    let (_, reply) = root("GET", "/vault/form", "legal-hold", &[], b"").1;
    assert!(reply.contains("<Status>ON</Status>"), "{reply}");
    let form = signed_form(
        &root_key,
        &root_secret,
        later,
        &[
            json!({"bucket": "open"}),
            json!({"key": "form"}),
            json!({HOLD: "ON"}),
        ],
        &[("key", "form"), (HOLD, "ON")],
    );
    let (_, (status, reply)) = post_form(port, "open", &form, "f.txt", b"form", 4);
    assert_eq!(status, 400, "{reply}");
    assert_eq!(root("GET", "/open/form", "", &[], b"").1.0, 404);
}

#[test]
fn a_copy_takes_the_lock_it_asks_for_or_its_buckets_default_not_its_sources() {
    const HOLD: &str = "x-amz-object-lock-legal-hold";
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "copylock");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
        .1
    };
    let lock_enabled = [("x-amz-bucket-object-lock-enabled", "true")];
    assert_eq!(call("PUT", "/clock", "", &lock_enabled, b"").0, 200);
    let customer = customer_headers("x-amz-", [0x77u8; 32]);
    let mut keyed = header_refs(&customer);
    keyed.push((HOLD, "ON"));
    assert_eq!(call("PUT", "/clock/held", "", &keyed, b"secret").0, 200);
    assert_eq!(
        call("PUT", "/clock/plain", "", &[(HOLD, "ON")], b"open").0,
        200
    );
    let hold_of = |path: &str| call("GET", path, "legal-hold", &[], b"");
    // A copy does not inherit its source's lock, on either path: the body
    // shared (plain to plain) or streamed (keyed to plain).
    let source_key = customer_headers("x-amz-copy-source-", [0x77u8; 32]);
    let mut streamed = header_refs(&source_key);
    streamed.push(("x-amz-copy-source", "/clock/held"));
    assert_eq!(call("PUT", "/clock/streamed", "", &streamed, b"").0, 200);
    assert_eq!(
        call(
            "PUT",
            "/clock/shared",
            "",
            &[("x-amz-copy-source", "/clock/plain")],
            b""
        )
        .0,
        200
    );
    for path in ["/clock/streamed", "/clock/shared"] {
        assert_eq!(hold_of(path).0, 404, "{path}");
    }
    // It takes the one its headers ask for, on either path.
    streamed.push((HOLD, "ON"));
    assert_eq!(call("PUT", "/clock/streamed2", "", &streamed, b"").0, 200);
    assert_eq!(
        call(
            "PUT",
            "/clock/shared2",
            "",
            &[("x-amz-copy-source", "/clock/plain"), (HOLD, "ON")],
            b""
        )
        .0,
        200
    );
    for path in ["/clock/streamed2", "/clock/shared2"] {
        let (status, reply) = hold_of(path);
        assert_eq!(status, 200, "{path}: {reply}");
        assert!(reply.contains("<Status>ON</Status>"), "{path}: {reply}");
    }
    // Or the bucket's default retention.
    let config = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                  <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days>\
                  </DefaultRetention></Rule></ObjectLockConfiguration>";
    let md5 = content_md5(config.as_bytes());
    assert_eq!(
        call(
            "PUT",
            "/clock",
            "object-lock",
            &[("content-md5", &md5)],
            config.as_bytes()
        )
        .0,
        200
    );
    streamed.pop();
    assert_eq!(call("PUT", "/clock/streamed3", "", &streamed, b"").0, 200);
    assert_eq!(
        call(
            "PUT",
            "/clock/shared3",
            "",
            &[("x-amz-copy-source", "/clock/plain")],
            b""
        )
        .0,
        200
    );
    for path in ["/clock/streamed3", "/clock/shared3"] {
        let (status, reply) = call("GET", path, "retention", &[], b"");
        assert_eq!(status, 200, "{path}: {reply}");
        assert!(reply.contains("<Mode>GOVERNANCE</Mode>"), "{path}: {reply}");
    }
    // A bucket without Object Lock refuses a copy asking for one.
    assert_eq!(call("PUT", "/cplain", "", &[], b"").0, 200);
    let (status, reply) = call(
        "PUT",
        "/cplain/c",
        "",
        &[("x-amz-copy-source", "/clock/plain"), (HOLD, "ON")],
        b"",
    );
    assert_eq!(status, 400, "{reply}");
    assert_eq!(call("GET", "/cplain/c", "", &[], b"").0, 404);
}

#[test]
fn a_retention_change_is_judged_by_its_body_not_by_lock_headers() {
    use serde_json::json;
    const MODE: &str = "x-amz-object-lock-mode";
    const UNTIL: &str = "x-amz-object-lock-retain-until-date";
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "judged");
    let root = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            path,
            query,
            extra,
            body,
            &root_key,
            Some(&root_secret),
        )
    };
    assert_eq!(
        root(
            "PUT",
            "/judged",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    let md5 = content_md5(b"x");
    let (head, (status, _)) = root(
        "PUT",
        "/judged/k",
        "",
        &[
            (MODE, "GOVERNANCE"),
            (UNTIL, &iso_in(86_400)),
            ("content-md5", &md5),
        ],
        b"x",
    );
    assert_eq!(status, 200);
    let query = format!(
        "retention&versionId={}",
        response_header(&head, "x-amz-version-id").unwrap()
    );
    // May set or extend a retention, and bypass, but never remove one.
    let keeper = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:PutObjectRetention", "Resource": "*",
         "Condition": {"Null": {"s3:object-lock-mode": "true"}}}
    ]})
    .to_string();
    let (key, secret) = user_with_key(&server, &account, "keeper", Some(&keeper));
    let remove = |extra: &[(&str, &str)]| {
        let body = b"<Retention/>";
        let md5 = content_md5(body);
        let mut headers = vec![
            ("content-md5", md5.as_str()),
            ("x-amz-bypass-governance-retention", "true"),
        ];
        headers.extend_from_slice(extra);
        send_with(
            port,
            "PUT",
            "/judged/k",
            &query,
            &headers,
            body,
            &key,
            Some(&secret),
        )
        .1
    };
    assert_eq!(remove(&[]).0, 403);
    let until = iso_in(86_400);
    assert_eq!(
        remove(&[(MODE, "GOVERNANCE"), (UNTIL, &until)]).0,
        403,
        "lock headers do not stand in for the body"
    );
    let (_, reply) = root("GET", "/judged/k", &query, &[], b"").1;
    assert!(reply.contains("<Mode>GOVERNANCE</Mode>"), "{reply}");
    // A policy allowing only GOVERNANCE retentions sees the body's mode.
    let governance_only = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:PutObjectRetention", "Resource": "*",
         "Condition": {"StringEquals": {"s3:object-lock-mode": "GOVERNANCE"}}}
    ]})
    .to_string();
    let (key, secret) = user_with_key(&server, &account, "extender", Some(&governance_only));
    let extend = |mode: &str| {
        let body = format!(
            "<Retention><Mode>{mode}</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
            iso_in(2 * 86_400)
        );
        let md5 = content_md5(body.as_bytes());
        send_with(
            port,
            "PUT",
            "/judged/k",
            &query,
            &[("content-md5", &md5)],
            body.as_bytes(),
            &key,
            Some(&secret),
        )
        .1
    };
    assert_eq!(extend("COMPLIANCE").0, 403);
    let (status, reply) = extend("GOVERNANCE");
    assert_eq!(status, 200, "{reply}");
}

#[test]
fn a_write_decides_its_lock_when_it_is_stored() {
    const HOLD: &str = "x-amz-object-lock-legal-hold";
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "decider");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_tls(
            &server,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
        .1
    };
    let lock_enabled = [("x-amz-bucket-object-lock-enabled", "true")];
    // Plain HTTP: the request is sent in two pieces.
    let plain = start_server();
    let (_, plain_key, plain_secret) = account_with_key(&plain, "decider");
    let plain_call = |method: &str, path: &str, extra: &[(&str, &str)]| {
        send_with(
            plain.s3_port,
            method,
            path,
            "",
            extra,
            b"",
            &plain_key,
            Some(&plain_secret),
        )
        .1
    };
    // The bucket is made again without Object Lock while a PUT asking for a
    // legal hold sends its body: the PUT is refused, not stored unlocked.
    assert_eq!(plain_call("PUT", "/racy", &lock_enabled).0, 200);
    let (_, (status, reply)) = send_paused(
        plain.s3_port,
        "PUT",
        "/racy/k",
        "",
        &[(HOLD, "ON")],
        b"0123456789",
        &plain_key,
        Some(&plain_secret),
        5,
        || {
            assert_eq!(
                send(
                    plain.s3_port,
                    "DELETE",
                    "/racy",
                    "",
                    b"",
                    &plain_key,
                    Some(&plain_secret)
                )
                .0,
                204
            );
            assert_eq!(
                send(
                    plain.s3_port,
                    "PUT",
                    "/racy",
                    "",
                    b"",
                    &plain_key,
                    Some(&plain_secret)
                )
                .0,
                200
            );
        },
    );
    assert_eq!(status, 400, "the bucket has no Object Lock now: {reply}");
    assert_eq!(xml_value(&reply, "Code"), "InvalidRequest");
    assert_eq!(plain_call("GET", "/racy/k", &[]).0, 404);
    // A default retention set while a PUT without an integrity check sends
    // its body applies to it, so the PUT is refused as if sent after it.
    assert_eq!(plain_call("PUT", "/late", &lock_enabled).0, 200);
    let (_, (status, reply)) = send_paused(
        plain.s3_port,
        "PUT",
        "/late/k",
        "",
        &[],
        b"0123456789",
        &plain_key,
        Some(&plain_secret),
        5,
        || {
            let config = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention>\
                </Rule></ObjectLockConfiguration>";
            let md5 = content_md5(config);
            let (_, (status, reply)) = send_with(
                plain.s3_port,
                "PUT",
                "/late",
                "object-lock",
                &[("content-md5", &md5)],
                config,
                &plain_key,
                Some(&plain_secret),
            );
            assert_eq!(status, 200, "{reply}");
        },
    );
    assert_eq!(status, 400, "{reply}");
    assert_eq!(plain_call("GET", "/late/k", &[]).0, 404);

    // A default retention set while a streamed copy runs is the copy's.
    assert_eq!(call("PUT", "/later", "", &lock_enabled, b"").0, 200);
    let customer = customer_headers("x-amz-", [0x3cu8; 32]);
    let data: Vec<u8> = (0..48u32 << 20).map(|n| (n % 251) as u8).collect();
    assert_eq!(
        call("PUT", "/later/src", "", &header_refs(&customer), &data).0,
        200
    );
    let mut copy_headers = vec![("x-amz-copy-source".to_string(), "/later/src".to_string())];
    copy_headers.extend(customer_headers("x-amz-copy-source-", [0x3cu8; 32]));
    let copier = std::thread::scope(|scope| {
        let copier =
            scope.spawn(|| call("PUT", "/later/dst", "", &header_refs(&copy_headers), b""));
        std::thread::sleep(Duration::from_millis(300));
        let config = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                      <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days>\
                      </DefaultRetention></Rule></ObjectLockConfiguration>";
        let md5 = content_md5(config.as_bytes());
        assert_eq!(
            call(
                "PUT",
                "/later",
                "object-lock",
                &[("content-md5", &md5)],
                config.as_bytes()
            )
            .0,
            200
        );
        copier.join().unwrap()
    });
    assert_eq!(copier.0, 200, "{}", copier.1);
    let (status, reply) = call("GET", "/later/dst", "retention", &[], b"");
    assert_eq!(status, 200, "{reply}");
    assert!(reply.contains("<Mode>GOVERNANCE</Mode>"), "{reply}");
}

#[test]
fn an_event_hold_keeps_an_object_until_its_duration_after_release() {
    use serde_json::json;
    const MODE: &str = "x-amz-object-lock-mode";
    const UNTIL: &str = "x-amz-object-lock-retain-until-date";
    const EVENT: &str = "x-amz-object-lock-event-hold";
    const DAYS: &str = "x-amz-object-lock-event-hold-duration-days";
    const YEARS: &str = "x-amz-object-lock-event-hold-duration-years";
    const BYPASS: &str = "x-amz-bypass-governance-retention";
    let server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "events");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    let put_doc = |path: &str, query: &str, body: &str| {
        let md5 = content_md5(body.as_bytes());
        call(
            "PUT",
            path,
            query,
            &[("content-md5", &md5)],
            body.as_bytes(),
        )
        .1
    };
    assert_eq!(
        call(
            "PUT",
            "/held",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    let md5 = content_md5(b"x");
    let (head, (status, reply)) = call(
        "PUT",
        "/held/k",
        "",
        &[
            (MODE, "COMPLIANCE"),
            (EVENT, "ON"),
            (DAYS, "1"),
            ("content-md5", &md5),
        ],
        b"x",
    );
    assert_eq!(status, 200, "{reply}");
    let version = format!(
        "versionId={}",
        response_header(&head, "x-amz-version-id").unwrap()
    );
    let (head, _) = call("HEAD", "/held/k", "", &[], b"");
    assert_eq!(response_header(&head, EVENT).as_deref(), Some("ON"));
    assert_eq!(response_header(&head, DAYS).as_deref(), Some("1"));
    // While held, the date is a day from now.
    let shown = response_header(&head, UNTIL).unwrap();
    assert!(
        shown[..10] >= *iso_in(86_400 - 60).get(..10).unwrap(),
        "{shown}"
    );
    let (_, reply) = call("GET", "/held/k", "retention", &[], b"").1;
    assert!(
        reply.contains(
            "<EventHold>ON</EventHold><EventHoldDuration><Days>1</Days></EventHoldDuration>"
        ),
        "{reply}"
    );
    assert_eq!(
        call("DELETE", "/held/k", &version, &[(BYPASS, "true")], b"")
            .1
            .0,
        403
    );
    // A release fixes the date; there is then nothing left to release.
    let release = "<Retention><Mode>COMPLIANCE</Mode><EventHold>OFF</EventHold></Retention>";
    let query = format!("retention&{version}");
    let (status, reply) = put_doc("/held/k", &query, release);
    assert_eq!(status, 200, "{reply}");
    let (head, _) = call("HEAD", "/held/k", "", &[], b"");
    assert_eq!(response_header(&head, EVENT).as_deref(), Some("OFF"));
    assert_eq!(response_header(&head, DAYS), None, "shown only while held");
    let (status, reply) = put_doc("/held/k", &query, release);
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("InvalidRequest"), "{reply}");
    // Still protected for the duration after release.
    assert_eq!(
        call("DELETE", "/held/k", &version, &[(BYPASS, "true")], b"")
            .1
            .0,
        403
    );
    // Malformed variable retentions.
    for body in [
        "<Retention><Mode>COMPLIANCE</Mode><EventHold>ON</EventHold></Retention>",
        "<Retention><EventHold>ON</EventHold><EventHoldDuration><Days>1</Days></EventHoldDuration></Retention>",
        "<Retention><Mode>COMPLIANCE</Mode><EventHold>OFF</EventHold><EventHoldDuration><Days>1</Days></EventHoldDuration></Retention>",
        "<Retention><Mode>COMPLIANCE</Mode><EventHold>ON</EventHold><EventHoldDuration></EventHoldDuration></Retention>",
    ] {
        let (status, reply) = put_doc("/held/k", &query, body);
        assert_eq!(status, 400, "{body}: {reply}");
    }

    // A bucket's default event hold: a new object starts held.
    let config = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                  <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><DefaultEventHold>\
                  <Years>1</Years></DefaultEventHold></DefaultRetention></Rule>\
                  </ObjectLockConfiguration>";
    assert_eq!(put_doc("/held", "object-lock", config).0, 200);
    let (_, reply) = call("GET", "/held", "object-lock", &[], b"").1;
    assert!(
        reply.contains("<DefaultEventHold><Years>1</Years></DefaultEventHold>"),
        "{reply}"
    );
    assert_eq!(
        call("PUT", "/held/d", "", &[("content-md5", &md5)], b"x")
            .1
            .0,
        200
    );
    let (head, _) = call("HEAD", "/held/d", "", &[], b"");
    assert_eq!(response_header(&head, EVENT).as_deref(), Some("ON"));
    assert_eq!(response_header(&head, YEARS).as_deref(), Some("1"));

    // Policies see the hold and its duration in days (a year is 365).
    let policy = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        {"Effect": "Deny", "Action": ["s3:PutObject", "s3:PutObjectRetention"], "Resource": "*",
         "Condition": {"NumericGreaterThan": {"s3:object-lock-event-hold-duration-days": "30"}}}
    ]})
    .to_string();
    let (user_key, user_secret) = user_with_key(&server, &account, "short", Some(&policy));
    let user_put = |duration: (&str, &str)| {
        send_with(
            port,
            "PUT",
            "/held/u",
            "",
            &[
                (MODE, "GOVERNANCE"),
                (EVENT, "ON"),
                duration,
                ("content-md5", &md5),
            ],
            b"x",
            &user_key,
            Some(&user_secret),
        )
        .1
        .0
    };
    assert_eq!(user_put((DAYS, "31")), 403);
    assert_eq!(user_put((YEARS, "1")), 403);
    assert_eq!(user_put((DAYS, "30")), 200);
}

#[test]
fn a_release_counts_its_duration_from_when_it_is_applied() {
    const MODE: &str = "x-amz-object-lock-mode";
    const EVENT: &str = "x-amz-object-lock-event-hold";
    const DAYS: &str = "x-amz-object-lock-event-hold-duration-days";
    let server = start_server();
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "slow");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(
        call(
            "PUT",
            "/slow",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    let md5 = content_md5(b"x");
    let held = |path: &str, days: &str| {
        let (head, (status, reply)) = call(
            "PUT",
            path,
            "",
            &[
                (MODE, "GOVERNANCE"),
                (EVENT, "ON"),
                (DAYS, days),
                ("content-md5", &md5),
            ],
            b"x",
        );
        assert_eq!(status, 200, "{reply}");
        format!(
            "retention&versionId={}",
            response_header(&head, "x-amz-version-id").unwrap()
        )
    };
    let until_of = |path: &str, query: &str| {
        let (_, reply) = call("GET", path, query, &[], b"").1;
        reply
            .split("<RetainUntilDate>")
            .nth(1)
            .and_then(|rest| rest.split("</RetainUntilDate>").next())
            .unwrap_or_else(|| panic!("{reply}"))
            .to_string()
    };
    // The body arrives three seconds after the request starts: the date is
    // fixed a day after the release is applied, not after it began.
    let query = held("/slow/k", "1");
    let release = b"<Retention><Mode>GOVERNANCE</Mode><EventHold>OFF</EventHold></Retention>";
    let release_md5 = content_md5(release);
    let (_, (status, reply)) = send_paused(
        port,
        "PUT",
        "/slow/k",
        &query,
        &[("content-md5", &release_md5)],
        release,
        &key,
        Some(&secret),
        10,
        || std::thread::sleep(Duration::from_secs(3)),
    );
    assert_eq!(status, 200, "{reply}");
    let floor = iso_in(86_400 - 1);
    let until = until_of("/slow/k", &query);
    assert!(until[..19] >= floor[..19], "{until} is before {floor}");

    // A release with a date keeps the later of the date and its duration.
    let query = held("/slow/m", "30");
    let body = format!(
        "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate>\
         <EventHold>OFF</EventHold></Retention>",
        iso_in(86_400)
    );
    let body_md5 = content_md5(body.as_bytes());
    let (status, reply) = call(
        "PUT",
        "/slow/m",
        &query,
        &[("content-md5", &body_md5)],
        body.as_bytes(),
    )
    .1;
    assert_eq!(status, 200, "{reply}");
    let until = until_of("/slow/m", &query);
    let floor = iso_in(30 * 86_400 - 60);
    assert!(until[..19] >= floor[..19], "{until} is before {floor}");
    let (head, _) = call("HEAD", "/slow/m", "", &[], b"");
    assert_eq!(response_header(&head, EVENT).as_deref(), Some("OFF"));
}

#[test]
fn a_fixed_date_that_ends_an_event_hold_is_authorized_as_a_release() {
    use serde_json::json;
    let server = start_server();
    let port = server.s3_port;
    let (account, root_key, root_secret) = account_with_key(&server, "releaser");
    let root = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            path,
            query,
            extra,
            body,
            &root_key,
            Some(&root_secret),
        )
    };
    assert_eq!(
        root(
            "PUT",
            "/rel",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    let md5 = content_md5(b"x");
    let (head, (status, _)) = root(
        "PUT",
        "/rel/k",
        "",
        &[
            ("x-amz-object-lock-mode", "GOVERNANCE"),
            ("x-amz-object-lock-event-hold", "ON"),
            ("x-amz-object-lock-event-hold-duration-days", "1"),
            ("content-md5", &md5),
        ],
        b"x",
    );
    assert_eq!(status, 200);
    let query = format!(
        "retention&versionId={}",
        response_header(&head, "x-amz-version-id").unwrap()
    );
    // May change retentions, but not end an event hold.
    let policy = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:PutObjectRetention", "Resource": "*",
         "Condition": {"StringEquals": {"s3:object-lock-event-hold": "OFF"}}}
    ]})
    .to_string();
    let (key, secret) = user_with_key(&server, &account, "holder", Some(&policy));
    let put = |as_user: bool, body: &str| {
        let md5 = content_md5(body.as_bytes());
        let extra = [("content-md5", md5.as_str())];
        match as_user {
            true => send_with(
                port,
                "PUT",
                "/rel/k",
                &query,
                &extra,
                body.as_bytes(),
                &key,
                Some(&secret),
            ),
            false => root("PUT", "/rel/k", &query, &extra, body.as_bytes()),
        }
        .1
    };
    let fixed = format!(
        "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
        iso_in(2 * 86_400)
    );
    let release = "<Retention><Mode>GOVERNANCE</Mode><EventHold>OFF</EventHold></Retention>";
    assert_eq!(put(true, &fixed).0, 403, "a fixed date ends the hold");
    assert_eq!(put(true, release).0, 403);
    let (head, _) = root("HEAD", "/rel/k", "", &[], b"");
    assert_eq!(
        response_header(&head, "x-amz-object-lock-event-hold").as_deref(),
        Some("ON")
    );
    let (status, reply) = put(false, &fixed);
    assert_eq!(status, 200, "{reply}");
    let (head, _) = root("HEAD", "/rel/k", "", &[], b"");
    assert_eq!(response_header(&head, "x-amz-object-lock-event-hold"), None);
    // On a version no longer held, a fixed date ends nothing.
    let later = format!(
        "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>",
        iso_in(3 * 86_400)
    );
    let (status, reply) = put(true, &later);
    assert_eq!(status, 200, "{reply}");
}

#[test]
fn every_answer_names_its_request_and_the_journal_names_who_made_each_change() {
    let server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "origins");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(port, method, path, query, &[], body, &key, Some(&secret))
    };
    let ids = |head: &str| {
        (
            response_header(head, "x-amz-request-id").expect("a request id"),
            response_header(head, "x-amz-id-2").expect("a host id"),
        )
    };
    let (head, (status, _)) = call("PUT", "/origins", "", b"");
    assert_eq!(status, 200);
    let (bucket_id, _) = ids(&head);
    assert_eq!(bucket_id.len(), 16);
    assert!(
        bucket_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b)),
        "{bucket_id}"
    );
    let (head, (status, _)) = call("PUT", "/origins/k", "", b"x");
    assert_eq!(status, 200);
    let (put_id, _) = ids(&head);
    assert_ne!(put_id, bucket_id, "each request its own");
    // An error names its request in its document too.
    let (head, (status, body)) = call("GET", "/origins/missing", "", b"");
    assert_eq!(status, 404);
    let (error_id, host_id) = ids(&head);
    assert!(
        body.contains(&format!(
            "<RequestId>{error_id}</RequestId><HostId>{host_id}</HostId>"
        )),
        "{body}"
    );
    let (head, (status, _)) = call("DELETE", "/origins/k", "", b"");
    assert_eq!(status, 204);
    let (delete_id, _) = ids(&head);
    // The journal names the account, the address and the request of each
    // change.
    let reply = native(
        server.native_port,
        "SELECT record_type, requester, source_ip_address, request_id \
         FROM sys.s3_journal('origins') ORDER BY sequence_number",
    );
    let rows = reply["results"][0]["rows"]
        .as_array()
        .expect("rows")
        .clone();
    let row = |i: usize| -> Vec<String> {
        rows[i]
            .as_array()
            .unwrap()
            .iter()
            .map(|cell| cell.as_str().unwrap_or("NULL").to_string())
            .collect()
    };
    assert_eq!(rows.len(), 2, "{reply}");
    assert_eq!(row(0), ["CREATE", &account, "127.0.0.1", &put_id]);
    assert_eq!(row(1), ["DELETE", &account, "127.0.0.1", &delete_id]);
}

#[test]
fn an_object_keeps_the_storage_class_it_was_written_in() {
    use serde_json::json;
    const CLASS: &str = "x-amz-storage-class";
    let server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "classes");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    let class_of = |path: &str| {
        let (head, (status, _)) = call("HEAD", path, "", &[], b"");
        assert_eq!(status, 200, "{path}");
        response_header(&head, CLASS)
    };
    assert_eq!(call("PUT", "/classes", "", &[], b"").1.0, 200);
    assert_eq!(
        call("PUT", "/classes/cold", "", &[(CLASS, "STANDARD_IA")], b"x")
            .1
            .0,
        200
    );
    assert_eq!(call("PUT", "/classes/plain", "", &[], b"x").1.0, 200);
    assert_eq!(class_of("/classes/cold").as_deref(), Some("STANDARD_IA"));
    assert_eq!(class_of("/classes/plain"), None, "STANDARD goes unnamed");
    let (head, _) = call("GET", "/classes/cold", "", &[], b"");
    assert_eq!(
        response_header(&head, CLASS).as_deref(),
        Some("STANDARD_IA")
    );
    for (query, wanted) in [
        ("list-type=2", "<Key>cold</Key>"),
        ("", "<Key>cold</Key>"),
        ("versions", "<Key>cold</Key>"),
    ] {
        let (_, (status, xml)) = call("GET", "/classes", query, &[], b"");
        assert_eq!(status, 200);
        let entry = &xml[xml.find(wanted).expect(wanted)..];
        let end = entry
            .find("</Contents>")
            .or_else(|| entry.find("</Version>"))
            .unwrap();
        let entry = &entry[..end];
        assert!(
            entry.contains("<StorageClass>STANDARD_IA</StorageClass>"),
            "{query}: {xml}"
        );
        assert!(
            xml.contains("<StorageClass>STANDARD</StorageClass>"),
            "{query}: the plain one: {xml}"
        );
    }
    // Classes S3 does not know, or that belong to other kinds of bucket.
    for bad in ["COLD", "standard", "OUTPOSTS", "EXPRESS_ONEZONE", "SNOW"] {
        let (_, (status, reply)) = call("PUT", "/classes/bad", "", &[(CLASS, bad)], b"x");
        assert_eq!(status, 400, "{bad}");
        assert!(reply.contains("InvalidStorageClass"), "{reply}");
    }
    // A copy takes the class its request names, or STANDARD: never the
    // source's. A copy onto itself may change only the class.
    let source = [("x-amz-copy-source", "/classes/cold")];
    assert_eq!(call("PUT", "/classes/copy", "", &source, b"").1.0, 200);
    assert_eq!(class_of("/classes/copy"), None);
    let mut glacier = source.to_vec();
    glacier.push((CLASS, "GLACIER"));
    assert_eq!(call("PUT", "/classes/copy2", "", &glacier, b"").1.0, 200);
    assert_eq!(class_of("/classes/copy2").as_deref(), Some("GLACIER"));
    let itself = [
        ("x-amz-copy-source", "/classes/cold"),
        (CLASS, "STANDARD_IA"),
    ];
    assert_eq!(call("PUT", "/classes/cold", "", &itself, b"").1.0, 400);
    let itself = [
        ("x-amz-copy-source", "/classes/cold"),
        (CLASS, "DEEP_ARCHIVE"),
    ];
    let (_, (status, reply)) = call("PUT", "/classes/cold", "", &itself, b"");
    assert_eq!(status, 200, "{reply}");
    assert_eq!(class_of("/classes/cold").as_deref(), Some("DEEP_ARCHIVE"));
    // A multipart upload's class is listed with it and its parts, and is
    // its object's.
    let (_, (status, reply)) = call(
        "POST",
        "/classes/mp",
        "uploads",
        &[(CLASS, "ONEZONE_IA")],
        b"",
    );
    assert_eq!(status, 200, "{reply}");
    let upload = reply
        .split("<UploadId>")
        .nth(1)
        .and_then(|rest| rest.split("</UploadId>").next())
        .unwrap()
        .to_string();
    let (_, reply) = call("GET", "/classes", "uploads", &[], b"").1;
    assert!(
        reply.contains("<StorageClass>ONEZONE_IA</StorageClass>"),
        "{reply}"
    );
    let (head, (status, _)) = call(
        "PUT",
        "/classes/mp",
        &format!("partNumber=1&uploadId={upload}"),
        &[],
        b"part",
    );
    assert_eq!(status, 200);
    let (_, reply) = call(
        "GET",
        "/classes/mp",
        &format!("uploadId={upload}"),
        &[],
        b"",
    )
    .1;
    assert!(
        reply.contains("<StorageClass>ONEZONE_IA</StorageClass>"),
        "{reply}"
    );
    let etag = response_header(&head, "etag").unwrap();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    assert_eq!(
        call(
            "POST",
            "/classes/mp",
            &format!("uploadId={upload}"),
            &[],
            complete.as_bytes()
        )
        .1
        .0,
        200
    );
    assert_eq!(class_of("/classes/mp").as_deref(), Some("ONEZONE_IA"));
    // A form names its class in a field.
    let form = signed_form(
        &key,
        &secret,
        "2099-01-01T00:00:00.000Z",
        &[
            json!({"bucket": "classes"}),
            json!({"key": "form"}),
            json!({CLASS: "GLACIER_IR"}),
        ],
        &[("key", "form"), (CLASS, "GLACIER_IR")],
    );
    let (_, (status, reply)) = post_form(port, "classes", &form, "f.txt", b"form", 4);
    assert_eq!(status, 204, "{reply}");
    assert_eq!(class_of("/classes/form").as_deref(), Some("GLACIER_IR"));
    // Policies see the class a write asks for.
    let policy = json!({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:PutObject", "Resource": "*",
         "Condition": {"StringEquals": {"s3:x-amz-storage-class": "DEEP_ARCHIVE"}}}
    ]})
    .to_string();
    let (user_key, user_secret) = user_with_key(&server, &account, "warm", Some(&policy));
    let user_put = |class: &str| {
        send_with(
            port,
            "PUT",
            "/classes/u",
            "",
            &[(CLASS, class)],
            b"x",
            &user_key,
            Some(&user_secret),
        )
        .1
        .0
    };
    assert_eq!(user_put("DEEP_ARCHIVE"), 403);
    assert_eq!(user_put("GLACIER"), 200);
}

#[test]
fn a_cold_object_reads_back_alike_compressed() {
    let server = start_server();
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "packed");
    let call = |method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, "", extra, body, &key, Some(&secret)).1
    };
    // Three segments' worth (a segment is 1,040,384 bytes), compressible.
    let data: Vec<u8> = (0..3_200_000u32)
        .map(|n| b"truthdb packs cold bodies "[(n % 26) as usize])
        .collect();
    assert_eq!(call("PUT", "/packed", &[], b"").0, 200);
    assert_eq!(
        call(
            "PUT",
            "/packed/k",
            &[("x-amz-storage-class", "GLACIER")],
            &data
        )
        .0,
        200
    );
    // An archived object is read once restored.
    let restore_request = b"<RestoreRequest><Days>1</Days><Tier>Expedited</Tier></RestoreRequest>";
    assert_eq!(
        send_with(
            port,
            "POST",
            "/packed/k",
            "restore",
            &[],
            restore_request,
            &key,
            Some(&secret)
        )
        .1
        .0,
        202
    );
    // Compressed as it arrived.
    let (status, body) = call("GET", "/packed/k", &[], b"");
    assert_eq!(status, 200);
    assert!(body == latin1(&data), "the whole object");
    for (first, last) in [(1_040_380u64, 1_040_390u64), (0, 9), (2_080_000, 3_199_999)] {
        let range = format!("bytes={first}-{last}");
        let (status, body) = call("GET", "/packed/k", &[("range", &range)], b"");
        assert_eq!(status, 206, "{range}");
        assert!(
            body == latin1(&data[first as usize..=last as usize]),
            "{range}"
        );
    }
    // A cold upload's parts: one sent, one copied by range (both written
    // anew, so compressed as they arrive).
    let send = |method: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            port,
            method,
            "/packed/parts",
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
        .1
    };
    let tag_of = |xml: &str, tag: &str| -> String {
        xml.split(&format!("<{tag}>"))
            .nth(1)
            .and_then(|rest| rest.split(&format!("</{tag}>")).next())
            .unwrap_or_default()
            .to_string()
    };
    let (status, xml) = send(
        "POST",
        "uploads",
        &[("x-amz-storage-class", "GLACIER")],
        b"",
    );
    assert_eq!(status, 200, "{xml}");
    let upload = tag_of(&xml, "UploadId");
    let first: Vec<u8> = data
        .iter()
        .cycle()
        .take(5 * 1024 * 1024 + 17)
        .copied()
        .collect();
    let (status, xml) = send(
        "PUT",
        &format!("partNumber=1&uploadId={upload}"),
        &[],
        &first,
    );
    assert_eq!(status, 200, "{xml}");
    let (status, copied) = send(
        "PUT",
        &format!("partNumber=2&uploadId={upload}"),
        &[
            ("x-amz-copy-source", "/packed/k"),
            ("x-amz-copy-source-range", "bytes=1000-2500999"),
        ],
        b"",
    );
    assert_eq!(status, 200, "{copied}");
    let (status, sent) = send("GET", &format!("uploadId={upload}"), &[], b"");
    assert_eq!(status, 200, "{sent}");
    let etags: Vec<String> = sent
        .split("<ETag>")
        .skip(1)
        .map(|rest| rest.split("</ETag>").next().unwrap().to_string())
        .collect();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let (status, xml) = send(
        "POST",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{xml}");
    assert_eq!(send("POST", "restore", &[], restore_request).0, 202);
    let (status, body) = send("GET", "", &[], b"");
    assert_eq!(status, 200);
    let whole = [&first[..], &data[1000..2_501_000]].concat();
    assert!(body == latin1(&whole), "the whole upload");
}

#[test]
fn an_archived_object_is_read_only_while_restored() {
    const CLASS: &str = "x-amz-storage-class";
    let server = start_server();
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "archive");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    let restore = |path: &str, days: &str, tier: &str| {
        call(
            "POST",
            path,
            "restore",
            &[],
            format!(
                "<RestoreRequest><Days>{days}</Days><GlacierJobParameters><Tier>{tier}</Tier>\
                 </GlacierJobParameters></RestoreRequest>"
            )
            .as_bytes(),
        )
        .1
    };
    let restore_header = |path: &str| {
        let (head, (status, _)) = call("HEAD", path, "", &[], b"");
        assert_eq!(status, 200, "{path}");
        response_header(&head, "x-amz-restore")
    };
    assert_eq!(call("PUT", "/archive", "", &[], b"").1.0, 200);
    assert_eq!(
        call("PUT", "/archive/cold", "", &[(CLASS, "GLACIER")], b"frozen")
            .1
            .0,
        200
    );
    assert_eq!(
        call(
            "PUT",
            "/archive/deep",
            "",
            &[(CLASS, "DEEP_ARCHIVE")],
            b"deeper"
        )
        .1
        .0,
        200
    );
    assert_eq!(
        call(
            "PUT",
            "/archive/ir",
            "",
            &[(CLASS, "GLACIER_IR")],
            b"instant"
        )
        .1
        .0,
        200
    );

    // Not restored: HEAD works, GET and copies are refused.
    let (head, (status, _)) = call("HEAD", "/archive/cold", "", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(response_header(&head, CLASS).as_deref(), Some("GLACIER"));
    assert_eq!(response_header(&head, "x-amz-restore"), None);
    for (method, path, extra) in [
        ("GET", "/archive/cold", &[][..]),
        ("GET", "/archive/cold", &[("range", "bytes=0-1")][..]),
        (
            "PUT",
            "/archive/copy",
            &[("x-amz-copy-source", "/archive/cold")][..],
        ),
    ] {
        let (status, reply) = call(method, path, "", extra, b"").1;
        assert_eq!(status, 403, "{method} {path}: {reply}");
        assert!(reply.contains("<Code>InvalidObjectState</Code>"), "{reply}");
    }
    // GLACIER_IR is read directly and is not restored.
    assert_eq!(
        call("GET", "/archive/ir", "", &[], b"").1,
        (200, "instant".to_string())
    );
    let (status, reply) = restore("/archive/ir", "1", "Standard");
    assert_eq!(status, 403, "{reply}");
    assert!(
        reply.contains("<Code>ObjectAlreadyInActiveTierError</Code>"),
        "{reply}"
    );
    let (status, reply) = restore("/archive/none", "1", "Standard");
    assert_eq!(status, 404, "{reply}");
    assert!(reply.contains("<Code>NoSuchKey</Code>"), "{reply}");
    let (status, reply) = restore("/archive/cold", "0", "Standard");
    assert_eq!(status, 400, "{reply}");
    let (status, reply) = restore("/archive/cold", "1", "Glacial");
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("<Code>MalformedXML</Code>"), "{reply}");

    // DEEP_ARCHIVE has no Expedited tier; a Standard restore here is done
    // at once: readable, then copied.
    let (status, reply) = restore("/archive/deep", "1", "Expedited");
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("<Code>InvalidRequest</Code>"), "{reply}");
    assert_eq!(restore("/archive/deep", "1", "Standard").0, 202);
    let header = restore_header("/archive/deep").expect("x-amz-restore");
    assert!(
        header.starts_with("ongoing-request=\"false\", expiry-date=\""),
        "{header}"
    );
    assert!(header.ends_with(" 00:00:00 GMT\""), "{header}");
    let (head, (status, body)) = call("GET", "/archive/deep", "", &[], b"");
    assert_eq!((status, body.as_str()), (200, "deeper"));
    assert_eq!(
        response_header(&head, "x-amz-restore"),
        Some(header.clone())
    );
    let (status, reply) = call(
        "PUT",
        "/archive/thawed",
        "",
        &[("x-amz-copy-source", "/archive/deep"), (CLASS, "STANDARD")],
        b"",
    )
    .1;
    assert_eq!(status, 200, "{reply}");
    assert_eq!(
        call("GET", "/archive/thawed", "", &[], b"").1,
        (200, "deeper".to_string())
    );
    // Asked again, the restore is extended: 200.
    assert_eq!(restore("/archive/deep", "30", "Standard").0, 200);
    assert_ne!(restore_header("/archive/deep"), Some(header));

    // A Bulk restore stays under way (2 s on these servers).
    assert_eq!(restore("/archive/cold", "1", "Bulk").0, 202);
    assert_eq!(
        restore_header("/archive/cold").as_deref(),
        Some("ongoing-request=\"true\"")
    );
    let (status, reply) = restore("/archive/cold", "1", "Bulk");
    assert_eq!(status, 409, "{reply}");
    assert!(
        reply.contains("<Code>RestoreAlreadyInProgress</Code>"),
        "{reply}"
    );
    assert_eq!(call("GET", "/archive/cold", "", &[], b"").1.0, 403);
    let deadline = Instant::now() + Duration::from_secs(10);
    while restore_header("/archive/cold").as_deref() == Some("ongoing-request=\"true\"") {
        assert!(Instant::now() < deadline, "the restore never completed");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        call("GET", "/archive/cold", "", &[], b"").1,
        (200, "frozen".to_string())
    );
}

#[test]
fn a_lifecycle_configuration_is_kept_shown_and_applied() {
    const MIN: &str = "x-amz-transition-default-minimum-object-size";
    let server = start_server();
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "rules");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/rules", "", &[], b"").1.0, 200);
    // Written before the rules: their first pass expires it.
    assert_eq!(call("PUT", "/rules/logs/old", "", &[], b"x").1.0, 200);
    let (_, (status, reply)) = call("GET", "/rules", "lifecycle", &[], b"");
    assert_eq!(status, 404, "{reply}");
    assert!(
        reply.contains("<Code>NoSuchLifecycleConfiguration</Code>"),
        "{reply}"
    );
    assert_eq!(call("GET", "/none", "lifecycle", &[], b"").1.0, 404);
    // Expired already (a date in the past) under logs/; a year off under
    // data/.
    let config = b"<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
        <Rule><ID>old logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>\
        <Expiration><Date>2020-01-01T00:00:00.000Z</Date></Expiration></Rule>\
        <Rule><ID>data</ID><Filter><Prefix>data/</Prefix></Filter><Status>Enabled</Status>\
        <Transition><Days>30</Days><StorageClass>STANDARD_IA</StorageClass></Transition>\
        <Expiration><Days>365</Days></Expiration></Rule></LifecycleConfiguration>";
    let (head, (status, reply)) = call(
        "PUT",
        "/rules",
        "lifecycle",
        &[(MIN, "varies_by_storage_class")],
        config,
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(
        response_header(&head, MIN).as_deref(),
        Some("varies_by_storage_class")
    );
    let (head, (status, xml)) = call("GET", "/rules", "lifecycle", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, MIN).as_deref(),
        Some("varies_by_storage_class")
    );
    assert!(xml.contains("<ID>old logs</ID>"), "{xml}");
    assert!(
        xml.contains(
            "<Transition><Days>30</Days><StorageClass>STANDARD_IA</StorageClass></Transition>"
        ),
        "{xml}"
    );
    // Refused as S3 refuses it, keeping what was there.
    let (status, reply) = call(
        "PUT",
        "/rules",
        "lifecycle",
        &[],
        b"<LifecycleConfiguration><Rule><Filter/><Status>Enabled</Status>\
          <Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>",
    )
    .1;
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("<Code>InvalidArgument</Code>"), "{reply}");
    assert_eq!(call("GET", "/rules", "lifecycle", &[], b"").1, (200, xml));

    // The data/ object says when it expires, and by which rule.
    let (head, (status, _)) = call("PUT", "/rules/data/a", "", &[], b"a");
    assert_eq!(status, 200);
    let expiration = response_header(&head, "x-amz-expiration").expect("x-amz-expiration");
    assert!(expiration.starts_with("expiry-date=\""), "{expiration}");
    assert!(
        expiration.ends_with(" 00:00:00 GMT\", rule-id=\"data\""),
        "{expiration}"
    );
    for method in ["HEAD", "GET"] {
        let (head, _) = call(method, "/rules/data/a", "", &[], b"");
        assert_eq!(
            response_header(&head, "x-amz-expiration"),
            Some(expiration.clone()),
            "{method}"
        );
    }
    let (head, _) = call("PUT", "/rules/other", "", &[], b"x");
    assert_eq!(response_header(&head, "x-amz-expiration"), None);

    // The server's daily pass expires logs/ (the rule's date is past).
    let deadline = Instant::now() + Duration::from_secs(20);
    while call("HEAD", "/rules/logs/old", "", &[], b"").1.0 != 404 {
        assert!(
            Instant::now() < deadline,
            "the lifecycle pass never expired it"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(call("HEAD", "/rules/data/a", "", &[], b"").1.0, 200);

    let (status, _) = call("DELETE", "/rules", "lifecycle", &[], b"").1;
    assert_eq!(status, 204);
    assert_eq!(call("GET", "/rules", "lifecycle", &[], b"").1.0, 404);
    let (head, _) = call("HEAD", "/rules/data/a", "", &[], b"");
    assert_eq!(response_header(&head, "x-amz-expiration"), None);
    // An object has no lifecycle configuration of its own.
    assert_eq!(call("GET", "/rules/data/a", "lifecycle", &[], b"").1.0, 501);
}

#[test]
fn a_presigned_urls_x_amz_parameters_are_its_headers() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    assert_eq!(
        send(port, "PUT", "/hoist", "", b"", &key, Some(&secret)).0,
        200
    );
    let now = amz_now();
    let url =
        |path: &str, extra: &str| presign(port, "PUT", path, extra, &key, &secret, &now, 3600);
    // As the JS SDK's presigner writes them: headers hoisted into the query
    // (in their own spelling), the payload hash among them.
    let target = url(
        "/hoist/a",
        "x-amz-meta-color=blue&X-Amz-Meta-Size=big&x-amz-storage-class=STANDARD_IA\
         &x-amz-tagging=team%3Dred&x-amz-content-sha256=UNSIGNED-PAYLOAD",
    );
    let (_, (status, reply)) = send_presigned(port, "PUT", &target, b"hoisted");
    assert_eq!(status, 200, "{reply}");
    let (head, (status, _)) =
        send_with(port, "HEAD", "/hoist/a", "", &[], b"", &key, Some(&secret));
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "x-amz-meta-color").as_deref(),
        Some("blue")
    );
    assert_eq!(
        response_header(&head, "x-amz-meta-size").as_deref(),
        Some("big")
    );
    assert_eq!(
        response_header(&head, "x-amz-storage-class").as_deref(),
        Some("STANDARD_IA")
    );
    assert_eq!(
        response_header(&head, "x-amz-tagging-count").as_deref(),
        Some("1")
    );
    // An x-amz header the URL does not sign is refused, as ever; a
    // parameter given twice is refused.
    let target = url("/hoist/b", "x-amz-meta-color=blue");
    let refused = send_presigned_with(port, "PUT", &target, &[("x-amz-meta-color", "blue")], b"x");
    assert_eq!(refused.1.0, 403, "{}", refused.1.1);
    let target = url("/hoist/c", "x-amz-meta-color=blue&X-AMZ-META-COLOR=green");
    let (status, reply) = send_presigned(port, "PUT", &target, b"x").1;
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("given twice"), "{reply}");
    // A header the URL signs, given in the query with another value too, is
    // refused; with the same value it is one header.
    let signed_too = |value: &str| {
        presign_payload(
            port,
            "PUT",
            "/hoist/g",
            &format!("x-amz-meta-color={value}"),
            &key,
            &secret,
            &now,
            3600,
            sigv4::UNSIGNED_PAYLOAD,
            &[("x-amz-meta-color", "blue")],
        )
    };
    let (status, reply) = send_presigned_with(
        port,
        "PUT",
        &signed_too("green"),
        &[("x-amz-meta-color", "blue")],
        b"x",
    )
    .1;
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("different query parameter"), "{reply}");
    let (status, reply) = send_presigned_with(
        port,
        "PUT",
        &signed_too("blue"),
        &[("x-amz-meta-color", "blue")],
        b"x",
    )
    .1;
    assert_eq!(status, 200, "{reply}");
    // Spaces around a value are dropped, as around a header's.
    let target = url("/hoist/h", "x-amz-meta-pad=%20padded%20");
    assert_eq!(send_presigned(port, "PUT", &target, b"x").1.0, 200);
    // (Stored so: HTTP itself trims a header's spaces.)
    let reply = native(
        server.native_port,
        "SELECT value FROM sys.s3_object_metadata('hoist', 'h') WHERE name = 'pad'",
    );
    assert_eq!(reply["results"][0]["rows"][0][0], "padded", "{reply}");
    // A customer-provided key never travels in a URL; nor are temporary
    // credentials taken in any spelling.
    for extra in [
        "x-amz-server-side-encryption-customer-key=a2V5",
        "x-amz-security-token=token",
    ] {
        let target = url("/hoist/i", extra);
        let (status, reply) = send_presigned(port, "PUT", &target, b"x").1;
        assert!(status == 400 || status == 403, "{extra}: {status} {reply}");
        assert!(
            reply.contains("must be sent as a header") || reply.contains("temporary credentials"),
            "{extra}: {reply}"
        );
    }
    // A hoisted header is checked as one: an ACL granting more than the
    // owner is refused, and the object is not stored.
    let target = url("/hoist/d", "x-amz-acl=public-read");
    let (status, reply) = send_presigned(port, "PUT", &target, b"x").1;
    assert_eq!(status, 400, "{reply}");
    assert_eq!(
        send(port, "HEAD", "/hoist/d", "", b"", &key, Some(&secret)).0,
        404
    );
    // A payload hash fixed by the URL in lowercase is checked too.
    let hash = sigv4::sha256_hex(b"exact");
    let target = presign_payload(
        port,
        "PUT",
        "/hoist/e",
        &format!("x-amz-content-sha256={hash}"),
        &key,
        &secret,
        &now,
        3600,
        &hash,
        &[],
    );
    let (status, reply) = send_presigned(port, "PUT", &target, b"other").1;
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("XAmzContentSHA256Mismatch"), "{reply}");
    assert_eq!(send_presigned(port, "PUT", &target, b"exact").1.0, 200);
    let target = url(
        "/hoist/f",
        &format!("x-amz-content-sha256={hash}&X-Amz-Content-Sha256={hash}"),
    );
    assert_eq!(send_presigned(port, "PUT", &target, b"exact").1.0, 400);
}

#[test]
fn a_multipart_object_is_read_by_part_number() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/parts", "", &[], b"").1.0, 200);
    let (_, (_, created)) = call("POST", "/parts/m", "uploads", &[], b"");
    let upload = xml_value(&created, "UploadId");
    let first: Vec<u8> = (0..5 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    let second = b"the second part".to_vec();
    let mut etags = Vec::new();
    for (number, data) in [(1, &first), (2, &second)] {
        let (head, (status, reply)) = call(
            "PUT",
            "/parts/m",
            &format!("uploadId={upload}&partNumber={number}"),
            &[],
            data,
        );
        assert_eq!(status, 200, "{reply}");
        etags.push(response_header(&head, "etag").expect("etag"));
    }
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let (_, (status, reply)) = call(
        "POST",
        "/parts/m",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    let total = first.len() + second.len();
    let (whole_head, _) = call("HEAD", "/parts/m", "", &[], b"");
    let etag = response_header(&whole_head, "etag").expect("etag");
    assert!(etag.ends_with("-2\""), "{etag}");
    assert_eq!(response_header(&whole_head, "x-amz-mp-parts-count"), None);
    for (number, data, start) in [(1, &first, 0), (2, &second, first.len())] {
        let query = format!("partNumber={number}");
        for method in ["GET", "HEAD"] {
            let (head, (status, body)) = call(method, "/parts/m", &query, &[], b"");
            assert_eq!(status, 206, "{method} {query}");
            assert_eq!(
                response_header(&head, "etag"),
                Some(etag.clone()),
                "the whole object's"
            );
            assert_eq!(
                response_header(&head, "x-amz-mp-parts-count").as_deref(),
                Some("2")
            );
            assert_eq!(
                response_header(&head, "content-length"),
                Some(data.len().to_string())
            );
            assert_eq!(
                response_header(&head, "content-range"),
                Some(format!("bytes {start}-{}/{total}", start + data.len() - 1))
            );
            if method == "GET" {
                assert!(body == latin1(data), "{query}");
            }
        }
    }
    // A part that is not there; a number that is not one; a Range as well.
    let refused = |query: &str, extra: &[(&str, &str)], code: &str| {
        let (_, (status, reply)) = call("GET", "/parts/m", query, extra, b"");
        assert_eq!(status, 400, "{query}: {reply}");
        assert!(
            reply.contains(&format!("<Code>{code}</Code>")),
            "{query}: {reply}"
        );
    };
    refused("partNumber=3", &[], "InvalidPart");
    for bad in [
        "partNumber=0",
        "partNumber=10001",
        "partNumber=x",
        "partNumber=+1",
        "partNumber=1&partNumber=2",
    ] {
        refused(bad, &[], "InvalidArgument");
    }
    refused("partNumber=1", &[("range", "bytes=0-1")], "InvalidRequest");
    // An empty last part is read as such (no range to name).
    let (_, (_, created)) = call("POST", "/parts/e", "uploads", &[], b"");
    let upload = xml_value(&created, "UploadId");
    let mut etags = Vec::new();
    for (number, data) in [(1, &first[..]), (2, &b""[..])] {
        let (head, (status, reply)) = call(
            "PUT",
            "/parts/e",
            &format!("uploadId={upload}&partNumber={number}"),
            &[],
            data,
        );
        assert_eq!(status, 200, "{reply}");
        etags.push(response_header(&head, "etag").expect("etag"));
    }
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let (_, (status, reply)) = call(
        "POST",
        "/parts/e",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    let (head, (status, body)) = call("GET", "/parts/e", "partNumber=2", &[], b"");
    assert_eq!((status, body.as_str()), (200, ""));
    assert_eq!(
        response_header(&head, "content-length").as_deref(),
        Some("0")
    );
    assert_eq!(response_header(&head, "content-range"), None);
    assert_eq!(
        response_header(&head, "x-amz-mp-parts-count").as_deref(),
        Some("2")
    );
    // An object put whole is its own part 1, and has no other.
    assert_eq!(call("PUT", "/parts/one", "", &[], b"single").1.0, 200);
    let (head, (status, body)) = call("GET", "/parts/one", "partNumber=1", &[], b"");
    assert_eq!((status, body.as_str()), (200, "single"));
    assert_eq!(response_header(&head, "x-amz-mp-parts-count"), None);
    let (status, reply) = call("GET", "/parts/one", "partNumber=2", &[], b"").1;
    assert_eq!(status, 400, "{reply}");
    assert!(reply.contains("<Code>InvalidPart</Code>"), "{reply}");
}

/// A client tool for the compatibility tests: the path in `env`, or `name`
/// on the PATH; `None` (the test is skipped) when it is not installed.
fn client_tool(env: &str, name: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(env) {
        return Some(PathBuf::from(path));
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(name))
        .find(|path| path.is_file())
}

/// Every file under `root` as (path relative to it, contents).
fn tree(root: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                out.push((relative, std::fs::read(&path).expect("read file")));
            }
        }
    }
    out.sort();
    out
}

/// A local tree to sync: nested folders, spaces, non-ASCII names, an empty
/// file, and one large enough for the tools to upload in parts.
fn sample_tree(root: &std::path::Path) {
    for (path, contents) in [
        ("one.txt", b"one".to_vec()),
        ("a/b/deep.txt", b"deep".to_vec()),
        ("space dir/file with space.txt", b"spaced".to_vec()),
        ("a/ünïcødé.txt", b"unicode".to_vec()),
        ("empty", Vec::new()),
        (
            "big.bin",
            (0..20_000_000u32).map(|n| (n % 251) as u8).collect(),
        ),
    ] {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }
}

/// Runs `tool` with `args` in `env`; panics with its output unless it
/// succeeds.
fn run_tool(tool: &std::path::Path, env: &[(&str, String)], args: &[&str]) -> String {
    let output = Command::new(tool)
        .args(args)
        .envs(env.iter().map(|(name, value)| (*name, value.as_str())))
        .output()
        .expect("run the tool");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{tool:?} {args:?}: {text}");
    text
}

#[test]
fn the_aws_cli_syncs_both_ways() {
    let Some(aws) = client_tool("TRUTHDB_TEST_AWS", "aws") else {
        eprintln!("skipped: the AWS CLI is not installed");
        return;
    };
    let (server, key, secret) = server_with_key();
    let work = server.dir.join("aws");
    let (src, back, copied) = (work.join("src"), work.join("back"), work.join("copied"));
    sample_tree(&src);
    let env = [
        ("AWS_ACCESS_KEY_ID", key.clone()),
        ("AWS_SECRET_ACCESS_KEY", secret.clone()),
        ("AWS_DEFAULT_REGION", "us-east-1".to_string()),
        (
            "AWS_ENDPOINT_URL",
            format!("http://127.0.0.1:{}", server.s3_port),
        ),
        ("AWS_CONFIG_FILE", work.join("none").display().to_string()),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            work.join("none").display().to_string(),
        ),
        ("AWS_PAGER", String::new()),
    ];
    let aws = |args: &[&str]| run_tool(&aws, &env, args);
    let (src_arg, back_arg) = (src.to_str().unwrap(), back.to_str().unwrap());
    aws(&["s3", "mb", "s3://synced"]);
    // Up, with the default (CRC64NVME) checksums and a multipart upload.
    aws(&["s3", "sync", src_arg, "s3://synced/up"]);
    // Nothing changed: nothing to send.
    let again = aws(&["s3", "sync", src_arg, "s3://synced/up"]);
    assert!(!again.contains("upload:"), "{again}");
    aws(&["s3", "sync", "s3://synced/up", back_arg]);
    assert_eq!(tree(&back), tree(&src));
    // A change and a removal, with --delete, both ways.
    std::fs::remove_file(src.join("one.txt")).unwrap();
    std::fs::write(src.join("a/b/deep.txt"), b"deeper").unwrap();
    aws(&["s3", "sync", "--delete", src_arg, "s3://synced/up"]);
    aws(&["s3", "sync", "--delete", "s3://synced/up", back_arg]);
    assert_eq!(tree(&back), tree(&src));
    // Copies within the store, a move, and a recursive copy down.
    aws(&["s3", "sync", "s3://synced/up", "s3://synced/copy"]);
    aws(&[
        "s3",
        "mv",
        "s3://synced/copy/empty",
        "s3://synced/copy/moved",
    ]);
    aws(&[
        "s3",
        "cp",
        "--recursive",
        "s3://synced/copy",
        copied.to_str().unwrap(),
    ]);
    let mut expected = tree(&src);
    for entry in &mut expected {
        if entry.0 == "empty" {
            entry.0 = "moved".to_string();
        }
    }
    expected.sort();
    assert_eq!(tree(&copied), expected);
    aws(&["s3", "rm", "--recursive", "s3://synced/copy"]);
    let left = aws(&["s3", "ls", "--recursive", "s3://synced"]);
    assert!(!left.contains("copy/"), "{left}");
}

#[test]
fn rclone_syncs_both_ways() {
    let Some(rclone) = client_tool("TRUTHDB_TEST_RCLONE", "rclone") else {
        eprintln!("skipped: rclone is not installed");
        return;
    };
    let (server, key, secret) = server_with_key();
    assert_eq!(
        send(
            server.s3_port,
            "PUT",
            "/cloned",
            "",
            b"",
            &key,
            Some(&secret)
        )
        .0,
        200
    );
    let work = server.dir.join("rclone");
    let (src, back) = (work.join("src"), work.join("back"));
    sample_tree(&src);
    let env = [
        ("RCLONE_CONFIG", work.join("none").display().to_string()),
        ("RCLONE_CONFIG_TDB_TYPE", "s3".to_string()),
        ("RCLONE_CONFIG_TDB_PROVIDER", "Other".to_string()),
        ("RCLONE_CONFIG_TDB_ACCESS_KEY_ID", key.clone()),
        ("RCLONE_CONFIG_TDB_SECRET_ACCESS_KEY", secret.clone()),
        (
            "RCLONE_CONFIG_TDB_ENDPOINT",
            format!("http://127.0.0.1:{}", server.s3_port),
        ),
        ("RCLONE_CONFIG_TDB_REGION", "us-east-1".to_string()),
    ];
    let rclone = |args: &[&str]| run_tool(&rclone, &env, args);
    let (src_arg, back_arg) = (src.to_str().unwrap(), back.to_str().unwrap());
    rclone(&["sync", src_arg, "tdb:cloned/up"]);
    // Sizes and digests (multipart objects included) agree.
    let checked = rclone(&["check", src_arg, "tdb:cloned/up"]);
    assert!(checked.contains("0 differences found"), "{checked}");
    rclone(&["sync", "tdb:cloned/up", back_arg]);
    assert_eq!(tree(&back), tree(&src));
    // A change and a removal, both ways.
    std::fs::remove_file(src.join("one.txt")).unwrap();
    std::fs::write(src.join("a/ünïcødé.txt"), b"changed").unwrap();
    rclone(&["sync", src_arg, "tdb:cloned/up"]);
    rclone(&["sync", "tdb:cloned/up", back_arg]);
    assert_eq!(tree(&back), tree(&src));
    // Server-side copies and moves.
    let copied = rclone(&["copy", "-v", "tdb:cloned/up", "tdb:cloned/copy"]);
    assert!(copied.contains("server-side copy"), "{copied}");
    rclone(&[
        "moveto",
        "tdb:cloned/copy/big.bin",
        "tdb:cloned/copy/big2.bin",
    ]);
    let listed = rclone(&["lsf", "-R", "tdb:cloned/copy"]);
    assert!(
        listed.contains("big2.bin") && !listed.contains("big.bin\n"),
        "{listed}"
    );
    rclone(&["purge", "tdb:cloned/copy"]);
    let left = rclone(&["lsf", "-R", "tdb:cloned/copy"]);
    assert!(left.lines().all(|line| line.contains("NOTICE")), "{left}");
}

/// A DER certificate as PEM.
fn pem_of(der: &[u8]) -> Vec<u8> {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(der);
    let lines: Vec<&str> = encoded
        .as_bytes()
        .chunks(64)
        .map(|line| std::str::from_utf8(line).unwrap())
        .collect();
    format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        lines.join("\n")
    )
    .into_bytes()
}

#[test]
fn the_s3_client_works_against_this_server() {
    use truthdb_s3client::{Client, Config, Error};
    let server = start_server_tls(false, true);
    let (_, key, secret) = account_with_key(&server, "client");
    let config = |secret: &str| Config {
        endpoint: format!("https://localhost:{}", server.s3_port),
        region: "us-east-1".to_string(),
        access_key: key.clone(),
        secret: secret.to_string(),
        ca_pem: server.cert.as_deref().map(pem_of),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use http_body_util::BodyExt;
        let client = Client::new(config(&secret)).unwrap();
        client.create_bucket("tools").await.unwrap();
        assert!(
            client
                .list_buckets()
                .await
                .unwrap()
                .contains(&"tools".to_string())
        );
        // One write, read back with its headers and metadata.
        let key_name = "dir/a b/ünï.txt";
        let etag = client
            .put_object(
                "tools",
                key_name,
                &[
                    ("content-type", "text/plain".to_string()),
                    ("x-amz-meta-origin", "client".to_string()),
                ],
                truthdb_s3client::Bytes::from_static(b"hello"),
            )
            .await
            .unwrap();
        let head = client
            .head_object("tools", key_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.size, 5);
        assert_eq!(head.etag, etag);
        assert_eq!(head.content_type.as_deref(), Some("text/plain"));
        assert_eq!(
            head.metadata,
            [("origin".to_string(), "client".to_string())]
        );
        let download = client.get_object("tools", key_name).await.unwrap();
        assert_eq!(download.head.etag, etag);
        let body = download.body.collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"hello");
        // In parts.
        let upload = client
            .create_multipart_upload("tools", "big", &[])
            .await
            .unwrap();
        let first = truthdb_s3client::Bytes::from(vec![7u8; 5 * 1024 * 1024]);
        let mut parts = Vec::new();
        for (number, data) in [
            (1, first.clone()),
            (2, truthdb_s3client::Bytes::from_static(b"tail")),
        ] {
            let etag = client
                .upload_part("tools", "big", &upload, number, data)
                .await
                .unwrap();
            parts.push((number, etag));
        }
        let etag = client
            .complete_multipart_upload("tools", "big", &upload, &parts)
            .await
            .unwrap();
        assert!(etag.ends_with("-2\""), "{etag}");
        let body = client
            .get_object("tools", "big")
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(body.len(), first.len() + 4);
        let abandoned = client
            .create_multipart_upload("tools", "gone", &[])
            .await
            .unwrap();
        client
            .abort_multipart_upload("tools", "gone", &abandoned)
            .await
            .unwrap();
        // An empty object.
        client
            .put_object("tools", "empty", &[], truthdb_s3client::Bytes::new())
            .await
            .unwrap();
        assert_eq!(
            client
                .head_object("tools", "empty")
                .await
                .unwrap()
                .unwrap()
                .size,
            0
        );
        client.delete_object("tools", "empty").await.unwrap();
        // Keys whose bytes a signer could get wrong.
        for odd in ["x+y%z~w", "a//b", "./c/../d", " lead"] {
            client
                .put_object(
                    "tools",
                    odd,
                    &[],
                    truthdb_s3client::Bytes::from_static(b"o"),
                )
                .await
                .unwrap_or_else(|err| panic!("{odd}: {err}"));
            assert!(
                client.head_object("tools", odd).await.unwrap().is_some(),
                "{odd}"
            );
            client.delete_object("tools", odd).await.unwrap();
        }
        // Listings by prefix and delimiter.
        let page = client
            .list_objects("tools", "dir/", Some("/"), None)
            .await
            .unwrap();
        assert!(page.objects.is_empty());
        assert_eq!(page.prefixes, ["dir/a b/"]);
        assert_eq!(page.next, None);
        let page = client.list_objects("tools", "", None, None).await.unwrap();
        let keys: Vec<&str> = page.objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(keys, ["big", key_name]);
        // Deleted, then gone.
        client.delete_object("tools", key_name).await.unwrap();
        assert_eq!(client.head_object("tools", key_name).await.unwrap(), None);
        match client.get_object("tools", key_name).await {
            Err(Error::Service {
                status: 404, code, ..
            }) => assert_eq!(code, "NoSuchKey"),
            Err(other) => panic!("{other}"),
            Ok(_) => panic!("found a deleted object"),
        }
        // A wrong secret is refused by the service, by name.
        let wrong = Client::new(config("not the secret")).unwrap();
        match wrong.list_buckets().await {
            Err(Error::Service {
                status: 403, code, ..
            }) => {
                assert_eq!(code, "SignatureDoesNotMatch")
            }
            other => panic!("{:?}", other.map(|_| ())),
        }
        // A certificate the client does not trust fails the connection.
        let untrusting = Client::new(Config {
            ca_pem: Some(pem_of(
                rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                    .unwrap()
                    .cert
                    .der(),
            )),
            ..config(&secret)
        })
        .unwrap();
        match untrusting.list_buckets().await {
            Err(Error::Transport(why)) => assert!(why.contains("certificate"), "{why}"),
            other => panic!("{:?}", other.map(|_| ())),
        }
    });
}

#[test]
fn the_cli_lists_copies_syncs_and_removes() {
    let (server, key, secret) = server_with_key();
    let work = server.dir.join("cli");
    let (src, back, txt) = (work.join("src"), work.join("back"), work.join("txt"));
    sample_tree(&src);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let attempt = |args: &[&str]| -> (Result<(), String>, String) {
        let mut argv: Vec<String> = vec![
            "--endpoint-url".to_string(),
            format!("http://127.0.0.1:{}", server.s3_port),
            "--region".to_string(),
            "us-east-1".to_string(),
            "--access-key".to_string(),
            key.clone(),
            "--secret-key".to_string(),
            secret.clone(),
        ];
        argv.extend(args.iter().map(|arg| arg.to_string()));
        let mut out = Vec::new();
        let result = runtime
            .block_on(truthdb_cli::s3::run(&argv, &mut out))
            .map_err(|err| format!("{err:#}"));
        (result, String::from_utf8(out).unwrap())
    };
    let cli = |args: &[&str]| -> String {
        let (result, out) = attempt(args);
        result.unwrap_or_else(|err| panic!("{args:?}: {err:#}"));
        out
    };
    let (src_arg, back_arg) = (src.to_str().unwrap(), back.to_str().unwrap());
    assert_eq!(cli(&["mb", "s3://cli"]), "make_bucket: cli\n");
    // Up (the large file in parts), then nothing more to send.
    let sent = cli(&["sync", src_arg, "s3://cli/up"]);
    assert_eq!(sent.lines().count(), 6, "{sent}");
    assert_eq!(cli(&["sync", src_arg, "s3://cli/up"]), "");
    let listed = cli(&["ls", "s3://cli/up/"]);
    assert!(listed.contains("PRE a/"), "{listed}");
    assert!(listed.contains("PRE space dir/"), "{listed}");
    assert!(listed.contains(" 20000000 big.bin"), "{listed}");
    let all = cli(&["ls", "--recursive", "s3://cli/up"]);
    assert!(all.contains("up/a/b/deep.txt"), "{all}");
    assert!(cli(&["ls"]).lines().any(|line| line == "cli"));
    // Down, then nothing more to fetch (the files keep the objects' times).
    cli(&["sync", "s3://cli/up", back_arg]);
    assert_eq!(tree(&back), tree(&src));
    assert_eq!(cli(&["sync", "s3://cli/up", back_arg]), "");
    assert_eq!(cli(&["sync", src_arg, "s3://cli/up"]), "");
    // The downloaded copies are not taken for newer.
    assert_eq!(cli(&["sync", back_arg, "s3://cli/up"]), "");
    // A dry run changes nothing; --delete removes what the source lacks.
    std::fs::remove_file(src.join("one.txt")).unwrap();
    let planned = cli(&["sync", "--delete", "--dryrun", src_arg, "s3://cli/up"]);
    assert_eq!(planned, "(dryrun) delete: s3://cli/up/one.txt\n");
    assert!(cli(&["ls", "s3://cli/up/"]).contains("one.txt"));
    cli(&["sync", "--delete", src_arg, "s3://cli/up"]);
    assert!(!cli(&["ls", "s3://cli/up/"]).contains("one.txt"));
    cli(&["sync", "--delete", "s3://cli/up", back_arg]);
    assert_eq!(tree(&back), tree(&src));
    // Filters, the later winning: only the .txt files, but not the deep one.
    cli(&[
        "cp",
        "--recursive",
        "--exclude",
        "*",
        "--include",
        "*.txt",
        "--exclude",
        "a/b/*",
        src_arg,
        "s3://cli/txt",
    ]);
    cli(&["cp", "--recursive", "s3://cli/txt", txt.to_str().unwrap()]);
    let names: Vec<String> = tree(&txt).into_iter().map(|(path, _)| path).collect();
    assert_eq!(names, ["a/ünïcødé.txt", "space dir/file with space.txt"]);
    // One file each way.
    cli(&[
        "cp",
        src.join("empty").to_str().unwrap(),
        "s3://cli/single/",
    ]);
    cli(&[
        "cp",
        "s3://cli/single/empty",
        work.join("empty.copy").to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(work.join("empty.copy")).unwrap(), b"");
    // Removed, a dry run first.
    assert!(
        cli(&["rm", "--recursive", "--dryrun", "s3://cli/txt"]).starts_with("(dryrun) delete:")
    );
    cli(&["rm", "--recursive", "s3://cli/txt"]);
    assert_eq!(cli(&["ls", "--recursive", "s3://cli/txt"]), "");
    cli(&["rm", "s3://cli/single/empty"]);
    // A source folder that is not there is an error, not an empty folder
    // whose sync would remove everything.
    let missing = work.join("missing");
    let (result, _) = attempt(&["sync", "--delete", missing.to_str().unwrap(), "s3://cli/up"]);
    assert!(result.is_err());
    assert!(
        cli(&["ls", "s3://cli/up/"]).contains("big.bin"),
        "nothing removed"
    );
    // A folder marker is no file; a key that is no path is reported, the
    // rest still downloaded, and the run fails.
    for odd in ["up/marker/", "up/odd//x"] {
        assert_eq!(
            send(
                server.s3_port,
                "PUT",
                &format!("/cli/{odd}"),
                "",
                b"z",
                &key,
                Some(&secret)
            )
            .0,
            200
        );
    }
    let fresh = work.join("fresh");
    let (result, out) = attempt(&["sync", "s3://cli/up", fresh.to_str().unwrap()]);
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("1 object(s) were skipped"),
        "{out}"
    );
    assert!(out.contains("skipped: s3://cli/up/odd//x"), "{out}");
    assert!(!out.contains("marker"), "{out}");
    assert_eq!(tree(&fresh), tree(&src));
    // Help is printed, not an error.
    assert!(cli(&["--help"]).contains("sync"));
}

/// Kills the server outright (SIGKILL, as a crash) and starts it again on
/// the same files and ports, waiting until it listens.
fn crash_and_restart(server: &mut Server) {
    server.child.kill().expect("kill");
    server.child.wait().expect("wait");
    let log = std::fs::OpenOptions::new()
        .append(true)
        .open(server.dir.join("server.log"))
        .expect("log");
    server.child = Command::new(env!("CARGO_BIN_EXE_truthdb"))
        .env("TRUTHDB_CONFIG", server.dir.join("truthdb.toml"))
        .stdout(Stdio::from(log.try_clone().expect("clone")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("restart server");
    let deadline = Instant::now() + DEADLINE;
    let pid = server.child.id();
    while ![server.native_port, server.s3_port]
        .iter()
        .all(|&port| listens(pid, port))
    {
        assert!(
            server.child.try_wait().expect("poll").is_none(),
            "the restarted server exited"
        );
        assert!(Instant::now() < deadline, "the server never came back");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The rows of a native SQL query, each a list of cells as text.
fn sql_rows(port: u16, sql: &str) -> Vec<Vec<String>> {
    let reply = native(port, sql);
    reply["results"][0]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{reply}"))
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap()
                .iter()
                .map(|cell| match cell {
                    serde_json::Value::String(text) => text.clone(),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn an_import_copies_another_servers_objects_and_goes_on_after_a_crash() {
    let source = start_server();
    let (_, source_key, source_secret) = account_with_key(&source, "origin");
    let mut target = start_server();
    let (_, key, secret) = account_with_key(&target, "copies");
    let put = |server: &Server, key: &str, secret: &str, path: &str, body: &[u8]| {
        let (status, reply) = send(server.s3_port, "PUT", path, "", body, key, Some(secret));
        assert_eq!(status, 200, "{path}: {reply}");
    };
    put(&source, &source_key, &source_secret, "/src", b"");
    let count = 60;
    let body =
        |i: usize| -> Vec<u8> { (0..2_000_000).map(|n| ((n + i * 7) % 251) as u8).collect() };
    for i in 0..count {
        put(
            &source,
            &source_key,
            &source_secret,
            &format!("/src/data/obj-{i:03}"),
            &body(i),
        );
    }
    put(
        &source,
        &source_key,
        &source_secret,
        "/src/other/x",
        b"not imported",
    );
    put(&target, &key, &secret, "/dst", b"");
    // Versioned: a version written twice would show.
    assert_eq!(
        send(
            target.s3_port,
            "PUT",
            "/dst",
            "versioning",
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
            &key,
            Some(&secret)
        )
        .0,
        200
    );
    // Already there alike (skipped), and there unlike (replaced).
    put(&target, &key, &secret, "/dst/in/obj-000", &body(0));
    put(&target, &key, &secret, "/dst/in/obj-001", b"stale");

    // A source that refuses the keys fails the import.
    let refused = native(
        target.native_port,
        &format!(
            "s3 create import http://127.0.0.1:{} us-east-1 {source_key} wrong s3://src/data/ s3://dst/x/",
            source.s3_port
        ),
    );
    let refused_id = refused["import_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("{refused}"));
    // Commands that cannot start an import.
    for bad in [
        "s3 create import nonsense us-east-1 k s s3://src/ s3://dst/",
        "s3 create import http://127.0.0.1:1 us-east-1 k s src s3://dst/",
        "s3 create import http://127.0.0.1:1 us-east-1 k s s3://src/ s3://nowhere/",
        "s3 create import http://127.0.0.1:1 us-east-1 k s s3://My_Bucket/ s3://dst/",
    ] {
        let reply = native_text(target.native_port, bad);
        assert!(!reply.contains("import_id"), "{bad}: {reply}");
    }

    // One whose source cannot be reached waits, and holds no other back.
    let unreachable = native(
        target.native_port,
        "s3 create import http://127.0.0.1:1 us-east-1 k s s3://src/ s3://dst/never/",
    );
    let unreachable_id = unreachable["import_id"].as_u64().unwrap();
    let created = native(
        target.native_port,
        &format!(
            "s3 create import http://127.0.0.1:{} us-east-1 {source_key} {source_secret} s3://src/data/ s3://dst/in/",
            source.s3_port
        ),
    );
    let id = created["import_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("{created}"));
    let state = |server: &Server, id: u64| -> Vec<String> {
        sql_rows(
            server.native_port,
            &format!(
                "SELECT state, copied, skipped, failed, last_key, last_error \
                 FROM sys.dm_s3_imports WHERE import_id = {id}"
            ),
        )
        .into_iter()
        .next()
        .unwrap_or_default()
    };
    // Killed once it has copied something, then started again.
    let deadline = Instant::now() + Duration::from_secs(120);
    while state(&target, id).get(1).is_none_or(|copied| copied == "0") {
        assert!(Instant::now() < deadline, "the import never started");
        std::thread::sleep(Duration::from_millis(20));
    }
    // Still running when it is killed: the restart must go on from its
    // place.
    let before = state(&target, id);
    assert_eq!(before[0], "RUNNING", "{before:?}");
    crash_and_restart(&mut target);
    let deadline = Instant::now() + Duration::from_secs(120);
    let done = loop {
        let row = state(&target, id);
        if row.first().map(String::as_str) == Some("DONE") {
            break row;
        }
        assert!(Instant::now() < deadline, "the import never ended: {row:?}");
        std::thread::sleep(Duration::from_millis(100));
    };
    // Every object exactly once: what was copied before the crash is not
    // copied again.
    let number = |at: usize| done[at].parse::<usize>().unwrap();
    assert_eq!(number(1) + number(2), count, "{done:?}");
    assert!(number(2) >= 1, "obj-000 was there alike: {done:?}");
    assert_eq!(number(3), 0, "{done:?}");
    assert_eq!(done[4], format!("data/obj-{:03}", count - 1));
    for i in [0, 1, 37, count - 1] {
        let (status, got) = get(
            target.s3_port,
            &format!("/dst/in/obj-{i:03}"),
            &key,
            Some(&secret),
        );
        assert_eq!(status, 200);
        assert!(got == latin1(&body(i)), "obj-{i:03}");
    }
    let listed = sql_rows(
        target.native_port,
        "SELECT COUNT(*) FROM sys.s3_objects('dst', 'in/')",
    );
    assert_eq!(listed[0][0], count.to_string());
    // Each object written once: its one version, and the stale one's two.
    let versions = sql_rows(
        target.native_port,
        "SELECT COUNT(*) FROM sys.s3_object_versions('dst', 'in/')",
    );
    assert_eq!(versions[0][0], (count + 1).to_string());
    let waiting = state(&target, unreachable_id);
    assert_eq!(waiting[0], "RUNNING", "{waiting:?}");
    assert!(!waiting[5].is_empty(), "the error is kept: {waiting:?}");
    assert_eq!(get(target.s3_port, "/dst/in/x", &key, Some(&secret)).0, 404);

    // The refused import failed, naming why.
    let failed = state(&target, refused_id);
    assert_eq!(failed[0], "FAILED", "{failed:?}");
    assert!(failed[5].contains("SignatureDoesNotMatch"), "{failed:?}");
    // Nothing failed object by object; dropped, an import is gone.
    assert!(
        sql_rows(
            target.native_port,
            "SELECT * FROM sys.dm_s3_import_failures"
        )
        .is_empty()
    );
    let dropped = native(target.native_port, &format!("s3 drop import {refused_id}"));
    assert_eq!(dropped["dropped"], true, "{dropped}");
    assert!(state(&target, refused_id).is_empty());
    // Dropped while it runs, an import stops writing.
    let again = native(
        target.native_port,
        &format!(
            "s3 create import http://127.0.0.1:{} us-east-1 {source_key} {source_secret} s3://src/data/ s3://dst/again/",
            source.s3_port
        ),
    );
    let again_id = again["import_id"].as_u64().unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    while state(&target, again_id)
        .get(1)
        .is_none_or(|copied| copied == "0")
    {
        assert!(Instant::now() < deadline, "the second import never started");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        native(target.native_port, &format!("s3 drop import {again_id}"))["dropped"],
        true
    );
    std::thread::sleep(Duration::from_secs(3));
    let copied_after = sql_rows(
        target.native_port,
        "SELECT COUNT(*) FROM sys.s3_objects('dst', 'again/')",
    )[0][0]
        .parse::<usize>()
        .unwrap();
    assert!(copied_after < 10, "it went on: {copied_after} objects");
    // The secret is not shown.
    let shown = native(target.native_port, "SELECT * FROM sys.dm_s3_imports");
    assert!(!shown.to_string().contains(&source_secret), "{shown}");
}

/// Every event message in stream topic `topic` (partitions 0 and 1), as
/// JSON.
fn stream_events(port: u16, topic: &str) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for partition in 0..2 {
        for row in sql_rows(
            port,
            &format!("SELECT value FROM sys.stream_read('{topic}', {partition})"),
        ) {
            let hex = row[0].trim_start_matches("0x");
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
                .collect();
            out.push(serde_json::from_slice(&bytes).unwrap());
        }
    }
    out
}

#[test]
fn bucket_notifications_reach_a_stream_topic_once_across_a_crash() {
    let mut server = start_server();
    let (account, key, secret) = account_with_key(&server, "notifier");
    let call = |server: &Server, method: &str, path: &str, query: &str, body: &[u8]| {
        send(
            server.s3_port,
            method,
            path,
            query,
            body,
            &key,
            Some(&secret),
        )
    };
    native(
        server.native_port,
        &format!("stream create topic events partitions 2 retention 0 account {account}"),
    );
    native(server.native_port, "stream create topic theirs");
    assert_eq!(call(&server, "PUT", "/photos", "", b"").0, 200);
    let config = |topic: &str| {
        format!(
            "<NotificationConfiguration><TopicConfiguration><Id>images</Id>\
             <Topic>arn:aws:sns:us-east-1:{account}:{topic}</Topic>\
             <Event>s3:ObjectCreated:*</Event><Event>s3:ObjectRemoved:*</Event>\
             <Filter><S3Key><FilterRule><Name>prefix</Name><Value>in/</Value></FilterRule>\
             </S3Key></Filter></TopicConfiguration></NotificationConfiguration>"
        )
    };
    // A topic that does not exist, or is not the bucket owner's, is
    // refused, unless validation is skipped; an ARN of another account
    // always is.
    for topic in ["nope", "theirs"] {
        let (status, reply) = call(
            &server,
            "PUT",
            "/photos",
            "notification",
            config(topic).as_bytes(),
        );
        assert_eq!(status, 400, "{reply}");
        assert!(reply.contains("InvalidArgument"), "{reply}");
    }
    let foreign = config("events").replace(&format!(":{account}:"), ":999999999999:");
    let (_, (status, reply)) = send_with(
        server.s3_port,
        "PUT",
        "/photos",
        "notification",
        &[("x-amz-skip-destination-validation", "true")],
        foreign.as_bytes(),
        &key,
        Some(&secret),
    );
    assert_eq!(status, 400, "{reply}");
    let (_, (status, reply)) = send_with(
        server.s3_port,
        "PUT",
        "/photos",
        "notification",
        &[("x-amz-skip-destination-validation", "true")],
        config("nope").as_bytes(),
        &key,
        Some(&secret),
    );
    assert_eq!(status, 200, "{reply}");
    assert!(stream_events(server.native_port, "events").is_empty());
    let (status, reply) = call(
        &server,
        "PUT",
        "/photos",
        "notification",
        config("events").as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    let (status, reply) = call(&server, "GET", "/photos", "notification", b"");
    assert_eq!(status, 200);
    assert!(
        reply.contains(&format!(
            "<Topic>arn:aws:sns:us-east-1:{account}:events</Topic>"
        )) && reply.contains("<Id>images</Id>")
            && reply.contains("<Name>prefix</Name><Value>in/</Value>"),
        "{reply}"
    );
    // The topic got S3's test event.
    let sent = stream_events(server.native_port, "events");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["Event"], "s3:TestEvent");
    assert_eq!(sent[0]["Bucket"], "photos");

    // Changes, a crash part way through, more changes.
    for at in 0..10 {
        let path = format!("/photos/in/{at}.txt");
        assert_eq!(call(&server, "PUT", &path, "", b"x").0, 200);
    }
    assert_eq!(call(&server, "PUT", "/photos/out/x.txt", "", b"x").0, 200);
    crash_and_restart(&mut server);
    for at in 10..20 {
        let path = format!("/photos/in/{at}.txt");
        assert_eq!(call(&server, "PUT", &path, "", b"x").0, 200);
    }
    assert_eq!(call(&server, "DELETE", "/photos/in/0.txt", "", b"").0, 204);
    let deadline = Instant::now() + DEADLINE;
    let events = loop {
        let events: Vec<serde_json::Value> = stream_events(server.native_port, "events")
            .into_iter()
            .filter(|event| event.get("Records").is_some())
            .collect();
        if events.len() >= 21 || Instant::now() > deadline {
            break events;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let mut seen: Vec<(String, String)> = events
        .iter()
        .map(|event| {
            let record = &event["Records"][0];
            assert_eq!(record["eventSource"], "aws:s3");
            assert_eq!(record["awsRegion"], "us-east-1");
            assert_eq!(record["s3"]["configurationId"], "images");
            assert_eq!(
                record["s3"]["bucket"]["ownerIdentity"]["principalId"],
                account.as_str()
            );
            assert_eq!(record["userIdentity"]["principalId"], account.as_str());
            (
                record["eventName"].as_str().unwrap().to_string(),
                record["s3"]["object"]["key"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    seen.sort();
    let mut expected: Vec<(String, String)> = (0..20)
        .map(|at| ("ObjectCreated:Put".to_string(), format!("in%2F{at}.txt")))
        .chain([("ObjectRemoved:Delete".to_string(), "in%2F0.txt".to_string())])
        .collect();
    expected.sort();
    assert_eq!(seen, expected, "each change once, none lost");
    let sequencers: std::collections::BTreeSet<&str> = events
        .iter()
        .map(|event| {
            event["Records"][0]["s3"]["object"]["sequencer"]
                .as_str()
                .unwrap()
        })
        .collect();
    assert_eq!(sequencers.len(), 21);

    // Another account may not read or change the configuration.
    let (_, other_key, other_secret) = account_with_key(&server, "stranger");
    for (method, body) in [("GET", &b""[..]), ("PUT", config("events").as_bytes())] {
        let (status, reply) = send(
            server.s3_port,
            method,
            "/photos",
            "notification",
            body,
            &other_key,
            Some(&other_secret),
        );
        assert_eq!(status, 403, "{method}: {reply}");
    }
    // An empty configuration turns notifications off.
    let (status, _) = call(
        &server,
        "PUT",
        "/photos",
        "notification",
        b"<NotificationConfiguration/>",
    );
    assert_eq!(status, 200);
    let (_, reply) = call(&server, "GET", "/photos", "notification", b"");
    assert!(!reply.contains("TopicConfiguration"), "{reply}");
}

#[test]
fn restores_and_retention_changes_send_their_events() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "noticer");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
        .1
    };
    native(
        server.native_port,
        &format!("stream create topic events partitions 2 retention 0 account {account}"),
    );
    let lock_enabled = [("x-amz-bucket-object-lock-enabled", "true")];
    assert_eq!(call("PUT", "/vault", "", &lock_enabled, b"").0, 200);
    let config = format!(
        "<NotificationConfiguration><TopicConfiguration><Id>vault</Id>\
         <Topic>arn:aws:sns:us-east-1:{account}:events</Topic>\
         <Event>s3:ObjectRestore:*</Event><Event>s3:ObjectRetention:Put</Event>\
         </TopicConfiguration></NotificationConfiguration>"
    );
    assert_eq!(
        call("PUT", "/vault", "notification", &[], config.as_bytes()).0,
        200
    );
    assert_eq!(
        call(
            "PUT",
            "/vault/cold",
            "",
            &[("x-amz-storage-class", "GLACIER")],
            b"frozen"
        )
        .0,
        200
    );
    // A Standard restore completes at once (the test servers' delay).
    let (status, reply) = call(
        "POST",
        "/vault/cold",
        "restore",
        &[],
        b"<RestoreRequest><Days>1</Days><GlacierJobParameters><Tier>Standard</Tier>\
          </GlacierJobParameters></RestoreRequest>",
    );
    assert_eq!(status, 202, "{reply}");
    let retention = b"<Retention><Mode>GOVERNANCE</Mode>\
        <RetainUntilDate>2099-01-01T00:00:00.000Z</RetainUntilDate></Retention>";
    let md5 = content_md5(retention);
    let (status, reply) = call(
        "PUT",
        "/vault/cold",
        "retention",
        &[("content-md5", md5.as_str())],
        retention,
    );
    assert_eq!(status, 200, "{reply}");

    let deadline = Instant::now() + DEADLINE;
    let events = loop {
        let events: Vec<serde_json::Value> = stream_events(server.native_port, "events")
            .into_iter()
            .filter(|event| event.get("Records").is_some())
            .map(|event| event["Records"][0].clone())
            .collect();
        if events.len() >= 3 || Instant::now() > deadline {
            break events;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let named = |name: &str| {
        events
            .iter()
            .find(|event| event["eventName"] == name)
            .unwrap_or_else(|| panic!("no {name} in {events:?}"))
    };
    let posted = named("ObjectRestore:Post");
    assert_eq!(posted["userIdentity"]["principalId"], account.as_str());
    assert_eq!(posted["s3"]["object"]["key"], "cold");
    assert_eq!(posted["s3"]["object"]["size"], 6);
    let completed = named("ObjectRestore:Completed");
    assert_eq!(completed["userIdentity"]["principalId"], "s3.amazonaws.com");
    let restore = &completed["glacierEventData"]["restoreEventData"];
    assert_eq!(restore["lifecycleRestoreStorageClass"], "GLACIER");
    assert!(
        restore["lifecycleRestorationExpiryTime"]
            .as_str()
            .unwrap()
            .ends_with("T00:00:00.000Z"),
        "{restore}"
    );
    let retained = named("ObjectRetention:Put");
    assert_eq!(retained["eventVersion"], "2.6");
    assert_eq!(retained["objectRetentionEventData"]["mode"], "GOVERNANCE");
    assert_eq!(
        retained["objectRetentionEventData"]["retainUntilDate"],
        "2099-01-01T00:00:00.000Z"
    );
    assert_eq!(events.len(), 3, "{events:?}");
    // The journal table shows S3 Metadata's records only.
    let types: Vec<String> = sql_rows(
        server.native_port,
        "SELECT record_type FROM sys.s3_journal('vault')",
    )
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    assert_eq!(types, ["CREATE"]);
}

#[test]
fn bucket_events_are_received_from_a_sql_queue() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "queuer");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(
            server.s3_port,
            method,
            path,
            query,
            body,
            &key,
            Some(&secret),
        )
    };
    native(
        server.native_port,
        &format!("stream create topic jobs retention 0 account {account}"),
    );
    assert_eq!(call("PUT", "/inbox", "", b"").0, 200);
    let config = format!(
        "<NotificationConfiguration><QueueConfiguration><Id>work</Id>\
         <Queue>arn:aws:sqs:us-east-1:{account}:jobs</Queue>\
         <Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>"
    );
    assert_eq!(
        call("PUT", "/inbox", "notification", config.as_bytes()).0,
        200
    );
    let (_, reply) = call("GET", "/inbox", "notification", b"");
    assert!(
        reply.contains(&format!(
            "<Queue>arn:aws:sqs:us-east-1:{account}:jobs</Queue>"
        )),
        "{reply}"
    );
    assert_eq!(call("PUT", "/inbox/order%201.json", "", b"{}").0, 200);
    let receive = "EXEC sys.sp_s3_queue_receive @queue = N'jobs', @max_messages = 10";
    // The test event first, then the object's.
    let deadline = Instant::now() + DEADLINE;
    let mut bodies = Vec::new();
    let mut receipts = Vec::new();
    while bodies.len() < 2 && Instant::now() < deadline {
        for row in sql_rows(server.native_port, receive) {
            receipts.push(row[1].clone());
            bodies.push(serde_json::from_str::<serde_json::Value>(&row[2]).unwrap());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    assert_eq!(bodies[0]["Event"], "s3:TestEvent");
    let event = &bodies[1]["Records"][0];
    assert_eq!(event["eventName"], "ObjectCreated:Put");
    assert_eq!(event["s3"]["configurationId"], "work");
    assert_eq!(event["s3"]["object"]["key"], "order+1.json");
    for receipt in &receipts {
        native(
            server.native_port,
            &format!("EXEC sys.sp_s3_queue_ack N'jobs', N'{receipt}'"),
        );
    }
    assert!(sql_rows(server.native_port, receive).is_empty());
}

/// The requests a webhook receiver got: (headers, body) each.
type Received = std::sync::Arc<std::sync::Mutex<Vec<(Vec<(String, String)>, Vec<u8>)>>>;

/// A webhook receiver on a free port: answers each POST with the next of
/// `statuses` (the last one again once they run out), and keeps the
/// requests.
fn webhook_receiver(statuses: Vec<u16>) -> (u16, Received) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = std::sync::Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let mut headers = Vec::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let (name, value) = header.split_once(':').unwrap();
                    headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
                }
                let len: usize = headers
                    .iter()
                    .find(|(name, _)| name == "content-length")
                    .map_or(0, |(_, value)| value.parse().unwrap());
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();
                let status = {
                    let mut seen = kept.lock().unwrap();
                    seen.push((headers, body));
                    statuses[(seen.len() - 1).min(statuses.len() - 1)]
                };
                let _ = write!(stream, "HTTP/1.1 {status} X\r\ncontent-length: 0\r\n\r\n");
            }
        }
    });
    (port, seen)
}

#[test]
fn bucket_events_reach_a_webhook_signed_and_retried() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "hooker");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send(
            server.s3_port,
            method,
            path,
            query,
            body,
            &key,
            Some(&secret),
        )
    };
    // Refuses the first POST, then takes them.
    let (port, seen) = webhook_receiver(vec![500, 200]);
    native(
        server.native_port,
        &format!("stream create topic hooked retention 0 account {account}"),
    );
    let created = native(
        server.native_port,
        &format!("stream create webhook hooked url http://127.0.0.1:{port}/hook secret topsecret"),
    );
    assert_eq!(created["dead_letter_topic"], "hooked.dead-letter");
    assert_eq!(call("PUT", "/photos", "", b"").0, 200);
    let config = format!(
        "<NotificationConfiguration><CloudFunctionConfiguration><Id>fn</Id>\
         <CloudFunction>arn:aws:lambda:us-east-1:{account}:function:hooked</CloudFunction>\
         <Event>s3:ObjectCreated:*</Event></CloudFunctionConfiguration></NotificationConfiguration>"
    );
    assert_eq!(
        call("PUT", "/photos", "notification", config.as_bytes()).0,
        200
    );
    assert_eq!(call("PUT", "/photos/a.jpg", "", b"x").0, 200);
    let deadline = Instant::now() + DEADLINE;
    while seen.lock().unwrap().len() < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "{seen:?}");
    let header = |at: usize, name: &str| {
        seen[at]
            .0
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| panic!("no {name}"))
    };
    // The test event refused, then sent again under the same delivery id.
    assert_eq!(header(0, "x-truthdb-delivery"), "hooked/0/0");
    assert_eq!(header(1, "x-truthdb-delivery"), "hooked/0/0");
    let test_event: serde_json::Value = serde_json::from_slice(&seen[1].1).unwrap();
    assert_eq!(test_event["Event"], "s3:TestEvent");
    let event: serde_json::Value = serde_json::from_slice(&seen[2].1).unwrap();
    assert_eq!(event["Records"][0]["eventName"], "ObjectCreated:Put");
    assert_eq!(event["Records"][0]["s3"]["configurationId"], "fn");
    assert_eq!(header(2, "content-type"), "application/json");
    // Signed with the secret over the time, the delivery and the body.
    for (at, (_, body)) in seen.iter().enumerate() {
        let signature = header(at, "x-truthdb-signature");
        let (time, mac) = signature
            .strip_prefix("t=")
            .and_then(|rest| rest.split_once(",v1="))
            .unwrap();
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"topsecret");
        let signed = [
            format!("{time}.{}.", header(at, "x-truthdb-delivery")).as_bytes(),
            body,
        ]
        .concat();
        let expected: String = ring::hmac::sign(&key, &signed)
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(mac, expected, "request {at}");
    }
    let rows = sql_rows(
        server.native_port,
        "SELECT topic_name, next_offset, pending_records, delivered, dead_lettered, last_error \
         FROM sys.dm_s3_webhooks",
    );
    // Delivered since its last failure: no error shown.
    assert_eq!(rows, [["hooked", "2", "0", "2", "0", "null"]]);
}

/// The Kafka client test script: `<mode> <port> <key id> <secret> <CA file>
/// [args]`, printing one JSON line.
const KAFKA_CLIENT: &str = r#"
import json, sys, time
from kafka import KafkaConsumer, TopicPartition

mode, port, key, secret, cafile = sys.argv[1:6]
common = dict(
    bootstrap_servers=f"127.0.0.1:{port}",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=key,
    sasl_plain_password=secret,
    ssl_cafile=cafile,
)

def read(consumer, count, deadline):
    out = []
    while len(out) < count and time.time() < deadline:
        for records in consumer.poll(timeout_ms=200).values():
            out.extend(records)
    # Anything past what was expected.
    end = time.time() + 1
    while time.time() < end:
        for records in consumer.poll(timeout_ms=200).values():
            out.extend(records)
    return out

if mode == "read":
    group, count = sys.argv[6], int(sys.argv[7])
    consumer = KafkaConsumer("events", group_id=group, auto_offset_reset="earliest",
                             enable_auto_commit=False, **common)
    records = read(consumer, count, time.time() + 60)
    consumer.commit()
    consumer.close()
    print(json.dumps([[r.partition, r.offset, r.key and r.key.decode(), json.loads(r.value)]
                      for r in records]))
elif mode == "offsets":
    consumer = KafkaConsumer(**common)
    parts = [TopicPartition("events", p) for p in (0, 1)]
    begin = consumer.beginning_offsets(parts)
    end = consumer.end_offsets(parts)
    at_zero = consumer.offsets_for_times({p: 0 for p in parts})
    future = consumer.offsets_for_times({p: 2 ** 62 for p in parts})
    print(json.dumps({
        "topics": sorted(consumer.topics()),
        "begin": [begin[p] for p in parts],
        "end": [end[p] for p in parts],
        "at_zero": [at_zero[p] and at_zero[p].offset for p in parts],
        "future": [future[p] for p in parts],
    }))
    consumer.close()
elif mode == "group":
    consumer = KafkaConsumer("events", group_id=sys.argv[6], auto_offset_reset="earliest",
                             **common)
    deadline, since, last = time.time() + 90, time.time(), None
    while time.time() < deadline:
        consumer.poll(timeout_ms=200)
        now = sorted(p.partition for p in consumer.assignment())
        if now != last:
            since, last = time.time(), now
        elif len(now) == 1 and time.time() - since > 4:
            break
    # Holds its partition a while, so the other member settles too.
    end = time.time() + 4
    while time.time() < end:
        consumer.poll(timeout_ms=200)
    consumer.close()
    print(json.dumps(last))
elif mode == "connect":
    try:
        consumer = KafkaConsumer(**common)
        print(json.dumps(sorted(consumer.topics())))
        consumer.close()
    except Exception as err:
        print(json.dumps(type(err).__name__))
"#;

/// Runs [`KAFKA_CLIENT`] with the Python that has kafka-python; its JSON
/// line.
fn kafka_client(python: &std::path::Path, server: &Server, args: &[&str]) -> serde_json::Value {
    let script = server.dir.join("kafka_client.py");
    std::fs::write(&script, KAFKA_CLIENT).expect("script");
    let output = Command::new(python)
        .arg("-I")
        .arg(&script)
        .args(args)
        .output()
        .expect("run the Kafka client");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().last().unwrap_or_default();
    serde_json::from_str(line).unwrap_or_else(|_| {
        panic!(
            "the Kafka client failed: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn kafka_consumers_read_bucket_events_in_groups() {
    // A Python with kafka-python installed (pip install kafka-python).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_KAFKA_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_KAFKA_PYTHON names no Python with kafka-python");
        return;
    };
    let server = start_server_full(false, true, true);
    let port = server.kafka_port.unwrap().to_string();
    let ca = server.dir.join("s3.crt");
    let ca = ca.to_str().unwrap();
    let (account, key, secret) = account_with_key(&server, "streamer");
    let (_, other_key, other_secret) = account_with_key(&server, "other");
    native(
        server.native_port,
        &format!("stream create topic events partitions 2 retention 0 account {account}"),
    );
    native(server.native_port, "stream create topic unowned");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_tls(&server, method, path, query, &[], body, &key, Some(&secret)).1
    };
    assert_eq!(call("PUT", "/photos", "", b"").0, 200);
    let config = format!(
        "<NotificationConfiguration><TopicConfiguration>\
         <Topic>arn:aws:sns:us-east-1:{account}:events</Topic>\
         <Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>"
    );
    assert_eq!(
        call("PUT", "/photos", "notification", config.as_bytes()).0,
        200
    );
    for name in ["a", "b", "c"] {
        assert_eq!(call("PUT", &format!("/photos/{name}"), "", b"x").0, 200);
    }
    let created = |records: &serde_json::Value| -> Vec<String> {
        let mut keys: Vec<String> = records
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|record| {
                record[3]["Records"][0]["s3"]["object"]["key"]
                    .as_str()
                    .map(str::to_string)
            })
            .collect();
        keys.sort();
        keys
    };

    // The test event and three objects, then (from the committed offsets)
    // only what came after.
    let first = kafka_client(
        &python,
        &server,
        &["read", &port, &key, &secret, ca, "g", "4"],
    );
    assert_eq!(first.as_array().unwrap().len(), 4, "{first}");
    assert_eq!(created(&first), ["a", "b", "c"]);
    for name in ["d", "e"] {
        assert_eq!(call("PUT", &format!("/photos/{name}"), "", b"x").0, 200);
    }
    let second = kafka_client(
        &python,
        &server,
        &["read", &port, &key, &secret, ca, "g", "2"],
    );
    assert_eq!(created(&second), ["d", "e"], "{second}");
    // Another group starts from the beginning.
    let other = kafka_client(
        &python,
        &server,
        &["read", &port, &key, &secret, ca, "h", "6"],
    );
    assert_eq!(created(&other), ["a", "b", "c", "d", "e"]);

    // Offsets: earliest, latest, by time.
    let offsets = kafka_client(&python, &server, &["offsets", &port, &key, &secret, ca]);
    assert_eq!(
        offsets["topics"],
        serde_json::json!(["events"]),
        "{offsets}"
    );
    assert_eq!(offsets["begin"], serde_json::json!([0, 0]));
    let end: Vec<u64> = serde_json::from_value(offsets["end"].clone()).unwrap();
    assert_eq!(end.iter().sum::<u64>(), 6);
    assert_eq!(
        offsets["at_zero"],
        serde_json::json!(
            end.iter()
                .map(|&n| (n > 0).then_some(0))
                .collect::<Vec<_>>()
        )
    );
    assert_eq!(offsets["future"], serde_json::json!([null, null]));

    // Two members of a group split the partitions.
    let group = |name: &'static str| {
        let (python, port, key, secret, ca, dir) = (
            python.clone(),
            port.clone(),
            key.clone(),
            secret.clone(),
            ca.to_string(),
            server.dir.clone(),
        );
        std::thread::spawn(move || {
            let output = Command::new(&python)
                .arg("-I")
                .arg(dir.join("kafka_client.py"))
                .args(["group", &port, &key, &secret, &ca, name])
                .output()
                .expect("run the Kafka client");
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        })
    };
    let (one, two) = (group("split"), group("split"));
    let mut split = [one.join().unwrap(), two.join().unwrap()];
    split.sort();
    assert_eq!(split, ["[0]", "[1]"]);

    // Another account sees none of these topics; a wrong secret is refused.
    let theirs = kafka_client(
        &python,
        &server,
        &["connect", &port, &other_key, &other_secret, ca],
    );
    assert_eq!(theirs, serde_json::json!([]));
    let refused = kafka_client(&python, &server, &["connect", &port, &key, "wrong", ca]);
    assert!(refused.is_string(), "{refused}");
}

type KafkaStream = rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>;

/// Sends one Kafka request (header v1, client id "raw") and returns the
/// answer's body, or `None` when the server closed the connection instead.
fn kafka_request(stream: &mut KafkaStream, api: i16, version: i16, body: &[u8]) -> Option<Vec<u8>> {
    let mut request = Vec::new();
    request.extend_from_slice(&api.to_be_bytes());
    request.extend_from_slice(&version.to_be_bytes());
    request.extend_from_slice(&7i32.to_be_bytes());
    request.extend_from_slice(&3i16.to_be_bytes());
    request.extend_from_slice(b"raw");
    request.extend_from_slice(body);
    let mut frame = (request.len() as i32).to_be_bytes().to_vec();
    frame.extend_from_slice(&request);
    if stream
        .write_all(&frame)
        .and_then(|()| stream.flush())
        .is_err()
    {
        return None;
    }
    let mut size = [0u8; 4];
    stream.read_exact(&mut size).ok()?;
    let mut answer = vec![0u8; i32::from_be_bytes(size) as usize];
    stream.read_exact(&mut answer).ok()?;
    assert_eq!(answer[..4], 7i32.to_be_bytes(), "the correlation id");
    Some(answer[4..].to_vec())
}

/// A Kafka string (int16 length).
fn kafka_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as i16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

/// A connection authenticated with SASL PLAIN as `key`; `None` when the
/// secret was refused (the answer's error code is checked).
fn kafka_login(server: &Server, key: &str, secret: &str) -> Option<KafkaStream> {
    let mut stream = tls_stream_to(server, server.kafka_port.unwrap());
    let mut body = Vec::new();
    kafka_string(&mut body, "PLAIN");
    let handshake = kafka_request(&mut stream, 17, 1, &body).expect("handshake");
    assert_eq!(handshake[..2], [0, 0], "PLAIN is offered");
    let token = format!("\0{key}\0{secret}");
    let mut body = (token.len() as i32).to_be_bytes().to_vec();
    body.extend_from_slice(token.as_bytes());
    let answer = kafka_request(&mut stream, 36, 1, &body).expect("authenticate");
    let error = i16::from_be_bytes([answer[0], answer[1]]);
    if error == 0 {
        return Some(stream);
    }
    assert_eq!(error, 58, "SASL_AUTHENTICATION_FAILED");
    assert!(
        kafka_request(&mut stream, 18, 0, &[]).is_none(),
        "closed after a refusal"
    );
    None
}

/// A Fetch v4 body: no wait, `max_bytes` overall, the given (topic,
/// partition, offset, partition max bytes) entries.
fn fetch_body(max_bytes: i32, entries: &[(&str, i32, i64, i32)]) -> Vec<u8> {
    let mut body = Vec::new();
    for value in [-1i32, 0, 0, max_bytes] {
        body.extend_from_slice(&value.to_be_bytes());
    }
    body.push(0); // isolation level
    body.extend_from_slice(&(entries.len() as i32).to_be_bytes());
    for &(topic, partition, offset, partition_max) in entries {
        kafka_string(&mut body, topic);
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(&partition.to_be_bytes());
        body.extend_from_slice(&offset.to_be_bytes());
        body.extend_from_slice(&partition_max.to_be_bytes());
    }
    body
}

/// The record sets of a Fetch v4 answer, in order: (error, records' bytes).
fn fetched_records(answer: &[u8]) -> Vec<(i16, usize)> {
    let mut at = 4; // throttle time
    let int = |at: &mut usize, n: usize| {
        let mut value = 0i64;
        for byte in &answer[*at..*at + n] {
            value = value << 8 | i64::from(*byte);
        }
        *at += n;
        // Sign-extend.
        value << (64 - 8 * n) >> (64 - 8 * n)
    };
    let mut out = Vec::new();
    let topics = int(&mut at, 4);
    for _ in 0..topics {
        let name = int(&mut at, 2) as usize;
        at += name;
        let partitions = int(&mut at, 4);
        for _ in 0..partitions {
            at += 4; // partition index
            let error = int(&mut at, 2) as i16;
            at += 8 + 8; // high watermark, last stable offset
            let aborted = int(&mut at, 4);
            at += 16 * aborted.max(0) as usize;
            let records = int(&mut at, 4).max(0) as usize;
            at += records;
            out.push((error, records));
        }
    }
    assert_eq!(at, answer.len(), "the whole answer read");
    out
}

#[test]
fn the_kafka_listener_bounds_what_one_request_takes() {
    let server = start_server_full(false, true, true);
    let (account, key, secret) = account_with_key(&server, "rawkafka");
    native(
        server.native_port,
        &format!("stream create topic events partitions 2 retention 0 account {account}"),
    );
    native(server.native_port, "stream create topic unowned");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_tls(&server, method, path, query, &[], body, &key, Some(&secret)).1
    };
    assert_eq!(call("PUT", "/raw", "", b"").0, 200);
    let config = format!(
        "<NotificationConfiguration><TopicConfiguration>\
         <Topic>arn:aws:sns:us-east-1:{account}:events</Topic>\
         <Event>s3:ObjectCreated:*</Event></TopicConfiguration></NotificationConfiguration>"
    );
    assert_eq!(
        call("PUT", "/raw", "notification", config.as_bytes()).0,
        200
    );
    for name in ["a", "b", "c", "d", "e", "f"] {
        assert_eq!(call("PUT", &format!("/raw/{name}"), "", b"x").0, 200);
    }
    // The events reach the topic (the notifier runs in the background).
    let deadline = Instant::now() + DEADLINE;
    while stream_events(server.native_port, "events").len() < 7 {
        assert!(Instant::now() < deadline, "the events never arrived");
        std::thread::sleep(Duration::from_millis(100));
    }

    // Before authenticating, only ApiVersions and SASL are answered.
    let mut anonymous = tls_stream_to(&server, server.kafka_port.unwrap());
    assert!(
        kafka_request(&mut anonymous, 18, 2, &[]).is_some(),
        "ApiVersions"
    );
    assert!(
        kafka_request(&mut anonymous, 3, 0, &0i32.to_be_bytes()).is_none(),
        "Metadata"
    );
    assert!(kafka_login(&server, &key, "wrong").is_none());

    // A fetch of no bytes, listing both partitions many times over: one
    // record set comes back (the first non-empty one), not one per entry.
    let mut stream = kafka_login(&server, &key, &secret).expect("logged in");
    let entries: Vec<(&str, i32, i64, i32)> = (0..200).map(|n| ("events", n % 2, 0, 0)).collect();
    let answer = kafka_request(&mut stream, 1, 4, &fetch_body(0, &entries)).expect("fetch");
    let sets = fetched_records(&answer);
    assert_eq!(sets.len(), 200);
    assert!(sets.iter().all(|&(error, _)| error == 0));
    assert_eq!(
        sets.iter().filter(|&&(_, bytes)| bytes > 0).count(),
        1,
        "{sets:?}"
    );
    // With room, both partitions answer.
    let both = [("events", 0, 0, 1 << 20), ("events", 1, 0, 1 << 20)];
    let answer = kafka_request(&mut stream, 1, 4, &fetch_body(1 << 20, &both)).unwrap();
    assert!(fetched_records(&answer).iter().all(|&(_, bytes)| bytes > 0));

    // Commits: a partition listed twice keeps its last offset; a partition
    // or topic not the account's is refused without holding back the rest.
    let mut body = Vec::new();
    kafka_string(&mut body, "raw-group");
    body.extend_from_slice(&(-1i32).to_be_bytes());
    kafka_string(&mut body, "");
    body.extend_from_slice(&(-1i64).to_be_bytes()); // retention
    body.extend_from_slice(&2i32.to_be_bytes());
    for (topic, partitions) in [
        ("events", vec![(0i32, 5i64), (0, 1), (9, 1)]),
        ("unowned", vec![(0, 1)]),
    ] {
        kafka_string(&mut body, topic);
        body.extend_from_slice(&(partitions.len() as i32).to_be_bytes());
        for (partition, offset) in partitions {
            body.extend_from_slice(&partition.to_be_bytes());
            body.extend_from_slice(&offset.to_be_bytes());
            body.extend_from_slice(&(-1i16).to_be_bytes()); // metadata: null
        }
    }
    let answer = kafka_request(&mut stream, 8, 2, &body).expect("commit");
    // v2 has no throttle time: topics, name, partitions, then (index, error)s.
    let errors: Vec<i16> = [20, 26, 32, 51]
        .iter()
        .map(|&at| i16::from_be_bytes([answer[at], answer[at + 1]]))
        .collect();
    assert_eq!(errors, [0, 0, 3, 3], "{answer:?}");
    let mut body = Vec::new();
    kafka_string(&mut body, "raw-group");
    body.extend_from_slice(&1i32.to_be_bytes());
    kafka_string(&mut body, "events");
    body.extend_from_slice(&1i32.to_be_bytes());
    body.extend_from_slice(&0i32.to_be_bytes());
    let answer = kafka_request(&mut stream, 9, 1, &body).expect("offset fetch");
    // topics, name, partitions, index, then the committed offset.
    let offset_at = 4 + 2 + 6 + 4 + 4;
    assert_eq!(answer[offset_at..offset_at + 8], 1i64.to_be_bytes());

    // A request listing more partitions than allowed closes the connection.
    let entries: Vec<(&str, i32, i64, i32)> = (0..4097).map(|_| ("events", 0, 0, 0)).collect();
    assert!(kafka_request(&mut stream, 1, 4, &fetch_body(0, &entries)).is_none());
}

/// A CreateJob request body (S3 Control): `operation` on the objects the
/// manifest `src/<manifest>` (with `etag`) lists, reporting to `reports`
/// under `rep`.
fn create_job_body(
    token: &str,
    operation: &str,
    manifest: &str,
    etag: &str,
    extra: &str,
) -> String {
    format!(
        "<CreateJobRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
         <ClientRequestToken>{token}</ClientRequestToken><Operation>{operation}</Operation>\
         <Manifest><Spec><Format>S3BatchOperations_CSV_20180820</Format>\
         <Fields><member>Bucket</member><member>Key</member></Fields></Spec>\
         <Location><ObjectArn>arn:aws:s3:::src/{manifest}</ObjectArn><ETag>{etag}</ETag></Location></Manifest>\
         <Report><Enabled>true</Enabled><Bucket>arn:aws:s3:::reports</Bucket>\
         <Format>Report_CSV_20180820</Format><Prefix>rep</Prefix><ReportScope>AllTasks</ReportScope></Report>\
         <Priority>10</Priority><RoleArn>arn:aws:iam::111122223333:role/batch</RoleArn>{extra}\
         </CreateJobRequest>"
    )
}

#[test]
fn batch_jobs_tag_and_copy_what_a_manifest_lists_once_across_a_crash() {
    let mut server = start_server();
    let (account, key, secret) = account_with_key(&server, "batcher");
    let (_, other_key, other_secret) = account_with_key(&server, "outsider");
    let call = |server: &Server, method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &[("x-amz-account-id", &account)],
            body,
            &key,
            Some(&secret),
        )
    };
    for bucket in ["/src", "/dst", "/reports"] {
        assert_eq!(call(&server, "PUT", bucket, "", b"").1.0, 200);
    }
    // The outsider's bucket.
    assert_eq!(
        send(
            server.s3_port,
            "PUT",
            "/theirs",
            "",
            b"",
            &other_key,
            Some(&other_secret)
        )
        .0,
        200
    );
    for name in ["a", "b", "dir/c%20d"] {
        assert_eq!(
            call(&server, "PUT", &format!("/src/{name}"), "", b"body")
                .1
                .0,
            200
        );
    }
    // A source with a website redirect location.
    assert_eq!(
        send_with(
            server.s3_port,
            "PUT",
            "/src/a",
            "",
            &[("x-amz-website-redirect-location", "/elsewhere")],
            b"body",
            &key,
            Some(&secret),
        )
        .1
        .0,
        200
    );
    let put_manifest = |server: &Server, name: &str, lines: &str| {
        let (head, (status, _)) = call(
            server,
            "PUT",
            &format!("/src/lists/{name}"),
            "",
            lines.as_bytes(),
        );
        assert_eq!(status, 200);
        response_header(&head, "etag").expect("an ETag")
    };
    let etag = put_manifest(
        &server,
        "tags.csv",
        "src,a\nsrc,b\nsrc,dir/c%20d\r\n\nsrc,missing\ntheirs,x\n",
    );
    let tagging = "<S3PutObjectTagging><TagSet><S3Tag><Key>k</Key><Value>v</Value></S3Tag></TagSet></S3PutObjectTagging>";
    let body = create_job_body("tag-1", tagging, "lists/tags.csv", &etag, "");
    let (_, (status, answer)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    assert_eq!(status, 200, "{answer}");
    let job = xml_value(&answer, "JobId");
    // The same request again: the same job; the token with another request:
    // refused.
    let (_, (_, again)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    assert_eq!(xml_value(&again, "JobId"), job);
    let changed = body.replace("<Priority>10</Priority>", "<Priority>11</Priority>");
    let (_, (status, refused)) = call(&server, "POST", "/v20180820/jobs", "", changed.as_bytes());
    assert_eq!(
        (status, xml_value(&refused, "Code").as_str()),
        (400, "IdempotencyException")
    );

    let wait_for = |server: &Server, job: &str, done: &[&str]| -> String {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let (_, (status, described)) =
                call(server, "GET", &format!("/v20180820/jobs/{job}"), "", b"");
            assert_eq!(status, 200, "{described}");
            if done.contains(&xml_value(&described, "Status").as_str()) {
                return described;
            }
            assert!(
                Instant::now() < deadline,
                "the job never ended: {described}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let described = wait_for(&server, &job, &["Complete"]);
    assert_eq!(
        xml_value(&described, "TotalNumberOfTasks"),
        "5",
        "the blank line is no task"
    );
    assert_eq!(xml_value(&described, "NumberOfTasksSucceeded"), "3");
    assert_eq!(xml_value(&described, "NumberOfTasksFailed"), "2");
    assert!(described.contains("<S3PutObjectTagging>"), "{described}");
    assert!(described.contains(&format!(
        "<JobArn>arn:aws:s3:us-east-1:{account}:job/{job}</JobArn>"
    )));
    for name in ["a", "dir/c%20d"] {
        let (_, (_, tags)) = call(&server, "GET", &format!("/src/{name}"), "tagging", b"");
        assert!(
            tags.contains("<Key>k</Key><Value>v</Value>"),
            "{name}: {tags}"
        );
    }
    // The report: a file per outcome and a manifest.json.
    let (_, (_, listed)) = call(
        &server,
        "GET",
        "/reports",
        &format!("list-type=2&prefix=rep/job-{job}/"),
        b"",
    );
    assert_eq!(listed.matches("<Key>").count(), 3, "{listed}");
    let (_, (_, manifest)) = call(
        &server,
        "GET",
        &format!("/reports/rep/job-{job}/manifest.json"),
        "",
        b"",
    );
    let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(manifest["Format"], "Report_CSV_20180820");
    let results = manifest["Results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    let failed = results
        .iter()
        .find(|r| r["TaskExecutionStatus"] == "failed")
        .unwrap();
    let (_, (_, rows)) = call(
        &server,
        "GET",
        &format!("/reports/{}", failed["Key"].as_str().unwrap()),
        "",
        b"",
    );
    assert!(
        rows.contains("src,missing,,failed,404,NoSuchKey,"),
        "{rows}"
    );
    assert!(
        rows.contains("theirs,x,,failed,403,AccessDenied,"),
        "{rows}"
    );

    // A copy that waits for confirmation, then runs.
    let copy = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::dst</TargetResource>\
                <TargetKeyPrefix>copies/</TargetKeyPrefix></S3PutObjectCopy>";
    let etag = put_manifest(&server, "copy.csv", "src,a\nsrc,b\n");
    let body = create_job_body(
        "copy-1",
        copy,
        "lists/copy.csv",
        &etag,
        "<ConfirmationRequired>true</ConfirmationRequired>",
    );
    let (_, (_, answer)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    let copy_job = xml_value(&answer, "JobId");
    wait_for(&server, &copy_job, &["Suspended"]);
    let (_, (status, _)) = call(
        &server,
        "POST",
        &format!("/v20180820/jobs/{copy_job}/priority"),
        "priority=99",
        b"",
    );
    assert_eq!(status, 200);
    let (_, (status, updated)) = call(
        &server,
        "POST",
        &format!("/v20180820/jobs/{copy_job}/status"),
        "requestedJobStatus=Ready",
        b"",
    );
    assert_eq!(
        (status, xml_value(&updated, "Status").as_str()),
        (200, "Ready")
    );
    let (_, (status, refused)) = call(
        &server,
        "POST",
        &format!("/v20180820/jobs/{copy_job}/status"),
        "requestedJobStatus=Ready",
        b"",
    );
    assert_eq!(
        (status, xml_value(&refused, "Code").as_str()),
        (400, "JobStatusException")
    );
    let described = wait_for(&server, &copy_job, &["Complete"]);
    assert_eq!(xml_value(&described, "Priority"), "99");
    for name in ["a", "b"] {
        let (head, (status, copied)) =
            call(&server, "GET", &format!("/dst/copies/{name}"), "", b"");
        assert_eq!((status, copied.as_str()), (200, "body"));
        // The source's redirect location is not copied.
        assert_eq!(
            response_header(&head, "x-amz-website-redirect-location"),
            None
        );
    }

    // SSE-S3 on a database not encrypted at rest: refused, as CopyObject
    // refuses it.
    let sse = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::dst</TargetResource>\
               <NewObjectMetadata><SSEAlgorithm>AES256</SSEAlgorithm></NewObjectMetadata></S3PutObjectCopy>";
    let body = create_job_body("sse-1", sse, "lists/copy.csv", &etag, "");
    let (_, (status, refused)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    assert_eq!(
        (status, xml_value(&refused, "Code").as_str()),
        (400, "InvalidArgument")
    );

    // A manifest changed since its ETag was taken: the job fails.
    let body = create_job_body("stale-1", tagging, "lists/copy.csv", "\"0123\"", "");
    let (_, (_, answer)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    let stale = xml_value(&answer, "JobId");
    let described = wait_for(&server, &stale, &["Failed"]);
    assert_eq!(xml_value(&described, "FailureCode"), "ManifestETagMismatch");

    // A job cancelled while it waits for confirmation.
    let body = create_job_body(
        "cancel-1",
        tagging,
        "lists/copy.csv",
        &etag,
        "<ConfirmationRequired>true</ConfirmationRequired>",
    );
    let (_, (_, answer)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    let cancelled = xml_value(&answer, "JobId");
    wait_for(&server, &cancelled, &["Suspended"]);
    call(
        &server,
        "POST",
        &format!("/v20180820/jobs/{cancelled}/status"),
        "requestedJobStatus=Cancelled&statusUpdateReason=no",
        b"",
    );
    let described = wait_for(&server, &cancelled, &["Cancelled"]);
    assert_eq!(xml_value(&described, "StatusUpdateReason"), "no");

    // ListJobs, by status and a page at a time.
    let (_, (_, listed)) = call(
        &server,
        "GET",
        "/v20180820/jobs",
        "jobStatuses=Complete",
        b"",
    );
    assert_eq!(listed.matches("<JobListDescriptor>").count(), 2, "{listed}");
    let (_, (_, page)) = call(&server, "GET", "/v20180820/jobs", "maxResults=3", b"");
    assert_eq!(page.matches("<JobListDescriptor>").count(), 3);
    let token = xml_value(&page, "NextToken");
    let (_, (_, rest)) = call(
        &server,
        "GET",
        "/v20180820/jobs",
        &format!("maxResults=3&nextToken={token}"),
        b"",
    );
    assert_eq!(rest.matches("<JobListDescriptor>").count(), 1);
    assert!(!rest.contains("<NextToken>"));

    // Another account sees none of these jobs; nor does a request for
    // another account.
    let theirs = send_with(
        server.s3_port,
        "GET",
        &format!("/v20180820/jobs/{job}"),
        "",
        &[("x-amz-account-id", "")],
        b"",
        &other_key,
        Some(&other_secret),
    )
    .1;
    assert_eq!(theirs.0, 403);
    let outsider = account_with_key(&server, "outsider2");
    let theirs = send_with(
        server.s3_port,
        "GET",
        &format!("/v20180820/jobs/{job}"),
        "",
        &[("x-amz-account-id", &outsider.0)],
        b"",
        &outsider.1,
        Some(&outsider.2),
    )
    .1;
    assert_eq!(
        (theirs.0, xml_value(&theirs.1, "Code").as_str()),
        (400, "NotFoundException")
    );

    // Across a crash: a long manifest copying the same objects into a
    // versioned bucket. Each task that succeeded made exactly one version.
    let enabled = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(
        call(&server, "PUT", "/dst", "versioning", enabled.as_bytes())
            .1
            .0,
        200
    );
    // A copy of an older version onto its own key restores it, as
    // CopyObject does.
    let put_version = |body: &[u8]| {
        let (head, (status, _)) = call(&server, "PUT", "/src/v", "", body);
        assert_eq!(status, 200);
        response_header(&head, "x-amz-version-id").expect("a version id")
    };
    assert_eq!(
        call(&server, "PUT", "/src", "versioning", enabled.as_bytes())
            .1
            .0,
        200
    );
    let old_version = put_version(b"one");
    put_version(b"two");
    let etag = put_manifest(&server, "restore.csv", &format!("src,v,{old_version}\n"));
    let restore =
        "<S3PutObjectCopy><TargetResource>arn:aws:s3:::src</TargetResource></S3PutObjectCopy>";
    let body = create_job_body("restore-1", restore, "lists/restore.csv", &etag, "").replace(
        "<member>Key</member></Fields>",
        "<member>Key</member><member>VersionId</member></Fields>",
    );
    let (_, (_, answer)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    let restored = xml_value(&answer, "JobId");
    let described = wait_for(&server, &restored, &["Complete"]);
    assert_eq!(
        xml_value(&described, "NumberOfTasksSucceeded"),
        "1",
        "{described}"
    );
    assert_eq!(call(&server, "GET", "/src/v", "", b"").1.1, "one");
    let lines: String = (0..3000)
        .map(|n| format!("src,{}\n", ["a", "b"][n % 2]))
        .collect();
    let etag = put_manifest(&server, "long.csv", &lines);
    let copy = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::dst</TargetResource>\
                <TargetKeyPrefix>many/</TargetKeyPrefix></S3PutObjectCopy>";
    let body = create_job_body("long-1", copy, "lists/long.csv", &etag, "");
    let (_, (_, answer)) = call(&server, "POST", "/v20180820/jobs", "", body.as_bytes());
    let long = xml_value(&answer, "JobId");
    let deadline = Instant::now() + DEADLINE;
    loop {
        let (_, (_, described)) = call(&server, "GET", &format!("/v20180820/jobs/{long}"), "", b"");
        let done: u64 = xml_value(&described, "NumberOfTasksSucceeded")
            .parse()
            .unwrap();
        if done > 0 {
            assert_ne!(
                xml_value(&described, "Status"),
                "Complete",
                "it ran before it could be stopped"
            );
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    crash_and_restart(&mut server);
    let described = wait_for(&server, &long, &["Complete"]);
    assert_eq!(xml_value(&described, "NumberOfTasksSucceeded"), "3000");
    let (_, (_, versions)) = call(
        &server,
        "GET",
        "/dst",
        "versions&prefix=many/&max-keys=1000",
        b"",
    );
    let mut count = versions.matches("<Version>").count();
    let mut listing = versions;
    while xml_value(&listing, "IsTruncated") == "true" {
        let query = format!(
            "versions&prefix=many/&max-keys=1000&key-marker={}&version-id-marker={}",
            xml_value(&listing, "NextKeyMarker"),
            xml_value(&listing, "NextVersionIdMarker")
        );
        listing = call(&server, "GET", "/dst", &query, b"").1.1;
        count += listing.matches("<Version>").count();
    }
    assert_eq!(count, 3000, "each copy made once");
    // The jobs as an administrator sees them.
    let rows = sql_rows(
        server.native_port,
        "SELECT job_id, status, succeeded_tasks, failed_tasks FROM sys.dm_s3_batch_jobs",
    );
    assert_eq!(rows.len(), 6);
    assert!(
        rows.contains(&vec![
            job.clone(),
            "Complete".into(),
            "3".into(),
            "2".into()
        ]),
        "{rows:?}"
    );
}

#[test]
fn batch_job_tasks_are_authorized_as_the_user_that_created_the_job() {
    // Encrypted at rest, so a copy may ask for SSE-S3.
    let server = start_server_with(true);
    let (account, key, secret) = account_with_key(&server, "batchauth");
    let root = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &[("x-amz-account-id", &account)],
            body,
            &key,
            Some(&secret),
        )
    };
    for bucket in ["/secret", "/mine", "/reports"] {
        assert_eq!(root("PUT", bucket, "", b"").1.0, 200);
    }
    assert_eq!(root("PUT", "/secret/s1", "", b"hidden").1.0, 200);
    assert_eq!(root("PUT", "/mine/m1", "", b"open").1.0, 200);
    let (head, (status, _)) = root("PUT", "/mine/lists/m.csv", "", b"secret,s1\nmine,m1\n");
    assert_eq!(status, 200);
    let etag = response_header(&head, "etag").unwrap();
    let policy = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:CreateJob", "s3:DescribeJob"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:PutObjectTagging"],
         "Resource": ["arn:aws:s3:::mine/*", "arn:aws:s3:::reports/*"]}]}"#
        .replace('\n', " ");
    let (eve_key, eve_secret) = user_with_key(&server, &account, "eve", Some(&policy));
    let eve = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &[("x-amz-account-id", &account)],
            body,
            &eve_key,
            Some(&eve_secret),
        )
        .1
    };
    let job_body = |token: &str, operation: &str| {
        create_job_body(token, operation, "lists/m.csv", &etag, "").replace(
            "arn:aws:s3:::src/lists/m.csv",
            "arn:aws:s3:::mine/lists/m.csv",
        )
    };
    let run = |body: String| -> String {
        let (status, answer) = eve("POST", "/v20180820/jobs", "", body.as_bytes());
        assert_eq!(status, 200, "{answer}");
        let job = xml_value(&answer, "JobId");
        let deadline = Instant::now() + DEADLINE;
        loop {
            let (_, described) = eve("GET", &format!("/v20180820/jobs/{job}"), "", b"");
            if xml_value(&described, "Status") == "Complete" {
                return described;
            }
            assert!(Instant::now() < deadline, "{described}");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    // Tagging: her own bucket's object only.
    let tagging = "<S3PutObjectTagging><TagSet><S3Tag><Key>k</Key><Value>v</Value></S3Tag></TagSet></S3PutObjectTagging>";
    let described = run(job_body("auth-tag", tagging));
    assert_eq!(xml_value(&described, "NumberOfTasksSucceeded"), "1");
    assert_eq!(xml_value(&described, "NumberOfTasksFailed"), "1");
    let (_, (_, tags)) = root("GET", "/secret/s1", "tagging", b"");
    assert!(!tags.contains("<Key>k</Key>"), "{tags}");
    // A copy out of a bucket she may not read: refused, and nothing copied.
    let copy = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::mine</TargetResource>\
                <TargetKeyPrefix>copies/</TargetKeyPrefix></S3PutObjectCopy>";
    let described = run(job_body("auth-copy", copy));
    assert_eq!(xml_value(&described, "NumberOfTasksFailed"), "1");
    assert_eq!(root("GET", "/mine/copies/s1", "", b"").1.0, 404);
    assert_eq!(root("GET", "/mine/copies/m1", "", b"").1.0, 200);
    // Her report says why, as AccessDenied, whether the object exists or not.
    let (_, (_, listed)) = root("GET", "/reports", "list-type=2&prefix=rep/", b"");
    assert!(listed.contains("manifest.json"), "{listed}");

    // New metadata is held to a PUT's rules.
    let replacing = |metadata: &str| {
        format!(
            "<S3PutObjectCopy><TargetResource>arn:aws:s3:::mine</TargetResource>\
             <MetadataDirective>REPLACE</MetadataDirective><NewObjectMetadata>{metadata}</NewObjectMetadata>\
             </S3PutObjectCopy>"
        )
    };
    let code = |body: String| {
        let (status, answer) = root("POST", "/v20180820/jobs", "", body.as_bytes()).1;
        (status, xml_value(&answer, "Code"))
    };
    let line_break = replacing("<ContentType>text/plain&#13;&#10;x-evil: 1</ContentType>");
    assert_eq!(
        code(job_body("meta-1", &line_break)),
        (400, "InvalidRequest".to_string())
    );
    let big = replacing(&format!(
        "<UserMetadata><entry><key>big</key><value>{}</value></entry></UserMetadata>",
        "v".repeat(3000)
    ));
    assert_eq!(
        code(job_body("meta-2", &big)),
        (400, "MetadataTooLarge".to_string())
    );
    let spaced =
        replacing("<UserMetadata><entry><key>a b</key><value>1</value></entry></UserMetadata>");
    assert_eq!(
        code(job_body("meta-3", &spaced)),
        (400, "InvalidRequest".to_string())
    );

    // A bucket policy's Deny on the copy's storage class holds for a batch
    // copy as for a PUT.
    let deny = r#"{"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*",
        "Action": "s3:PutObject", "Resource": "arn:aws:s3:::mine/*",
        "Condition": {"StringEquals": {"s3:x-amz-storage-class": "GLACIER"}}}]}"#;
    assert_eq!(root("PUT", "/mine", "policy", deny.as_bytes()).1.0, 204);
    let glacier = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::mine</TargetResource>\
                   <TargetKeyPrefix>cold/</TargetKeyPrefix><StorageClass>GLACIER</StorageClass></S3PutObjectCopy>";
    let described = run(job_body("auth-glacier", glacier));
    assert_eq!(
        xml_value(&described, "NumberOfTasksSucceeded"),
        "0",
        "{described}"
    );
    assert_eq!(root("GET", "/mine/cold/m1", "", b"").1.0, 404);
    assert_eq!(root("DELETE", "/mine", "policy", b"").1.0, 204);
    // A source bucket refusing reads for copies: the copy's source read
    // carries the copy's keys too.
    let no_copies = r#"{"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*",
        "Action": "s3:GetObject", "Resource": "arn:aws:s3:::mine/*",
        "Condition": {"Null": {"s3:x-amz-copy-source": "false"}}}]}"#;
    assert_eq!(
        root("PUT", "/mine", "policy", no_copies.as_bytes()).1.0,
        204
    );
    let copy_again = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::mine</TargetResource>\
                      <TargetKeyPrefix>again/</TargetKeyPrefix></S3PutObjectCopy>";
    let described = run(job_body("auth-nocopy", copy_again));
    assert_eq!(
        xml_value(&described, "NumberOfTasksSucceeded"),
        "0",
        "{described}"
    );
    assert_eq!(root("GET", "/mine/again/m1", "", b"").1.0, 404);
    assert_eq!(
        root("GET", "/mine/m1", "", b"").1.0,
        200,
        "a plain read still goes"
    );
    assert_eq!(root("DELETE", "/mine", "policy", b"").1.0, 204);

    // The root's jobs, for what follows.
    let run_root = |body: String| -> String {
        let (status, answer) = root("POST", "/v20180820/jobs", "", body.as_bytes()).1;
        assert_eq!(status, 200, "{answer}");
        let job = xml_value(&answer, "JobId");
        let deadline = Instant::now() + DEADLINE;
        loop {
            let described = root("GET", &format!("/v20180820/jobs/{job}"), "", b"").1.1;
            if xml_value(&described, "Status") == "Complete" {
                return described;
            }
            assert!(Instant::now() < deadline, "{described}");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    // A copy asking for SSE-S3 carries its key: a policy refusing
    // unencrypted writes lets it through.
    let encrypted_only = r#"{"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*",
        "Action": "s3:PutObject", "Resource": "arn:aws:s3:::mine/*",
        "Condition": {"Null": {"s3:x-amz-server-side-encryption": "true"}}}]}"#;
    assert_eq!(
        root("PUT", "/mine", "policy", encrypted_only.as_bytes())
            .1
            .0,
        204
    );
    let sse = "<S3PutObjectCopy><TargetResource>arn:aws:s3:::mine</TargetResource>\
               <TargetKeyPrefix>sse/</TargetKeyPrefix>\
               <NewObjectMetadata><SSEAlgorithm>AES256</SSEAlgorithm></NewObjectMetadata></S3PutObjectCopy>";
    let described = run_root(job_body("root-sse", sse));
    assert_eq!(
        xml_value(&described, "NumberOfTasksSucceeded"),
        "2",
        "{described}"
    );
    assert_eq!(root("DELETE", "/mine", "policy", b"").1.0, 204);
    // A bucket's default retention is not what a copy asks for: a policy
    // refusing COMPLIANCE requests lets the copy through, which then gets
    // the default.
    assert_eq!(
        send_with(
            server.s3_port,
            "PUT",
            "/vault",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b"",
            &key,
            Some(&secret)
        )
        .1
        .0,
        200
    );
    let config = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
                  <Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>1</Days></DefaultRetention></Rule>\
                  </ObjectLockConfiguration>";
    let md5 = content_md5(config.as_bytes());
    assert_eq!(
        send_with(
            server.s3_port,
            "PUT",
            "/vault",
            "object-lock",
            &[("content-md5", &md5)],
            config.as_bytes(),
            &key,
            Some(&secret)
        )
        .1
        .0,
        200
    );
    let no_compliance = r#"{"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*",
        "Action": "s3:PutObject", "Resource": "arn:aws:s3:::vault/*",
        "Condition": {"StringEquals": {"s3:object-lock-mode": "COMPLIANCE"}}}]}"#;
    assert_eq!(
        root("PUT", "/vault", "policy", no_compliance.as_bytes())
            .1
            .0,
        204
    );
    let into_vault =
        "<S3PutObjectCopy><TargetResource>arn:aws:s3:::vault</TargetResource></S3PutObjectCopy>";
    let described = run_root(job_body("root-vault", into_vault));
    assert_eq!(
        xml_value(&described, "NumberOfTasksSucceeded"),
        "2",
        "{described}"
    );
    let (_, (_, retention)) = root("GET", "/vault/m1", "retention", b"");
    assert!(retention.contains("<Mode>COMPLIANCE</Mode>"), "{retention}");

    // A job whose creator is gone fails, even when a user of the same name
    // is made again before it runs.
    let waiting = job_body("auth-gone", tagging).replace(
        "</CreateJobRequest>",
        "<ConfirmationRequired>true</ConfirmationRequired></CreateJobRequest>",
    );
    let (status, answer) = eve("POST", "/v20180820/jobs", "", waiting.as_bytes());
    assert_eq!(status, 200, "{answer}");
    let gone = xml_value(&answer, "JobId");
    let deadline = Instant::now() + DEADLINE;
    while xml_value(
        &root("GET", &format!("/v20180820/jobs/{gone}"), "", b"").1.1,
        "Status",
    ) != "Suspended"
    {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(100));
    }
    native(server.native_port, &format!("s3 drop access key {eve_key}"));
    native(
        server.native_port,
        &format!("s3 delete user policy {account} eve p"),
    );
    native(server.native_port, &format!("s3 drop user {account} eve"));
    user_with_key(&server, &account, "eve", Some(&policy));
    root(
        "POST",
        &format!("/v20180820/jobs/{gone}/status"),
        "requestedJobStatus=Ready",
        b"",
    );
    let deadline = Instant::now() + DEADLINE;
    loop {
        let described = root("GET", &format!("/v20180820/jobs/{gone}"), "", b"").1.1;
        if xml_value(&described, "Status") == "Failed" {
            assert_eq!(xml_value(&described, "FailureCode"), "AccessDenied");
            break;
        }
        assert!(Instant::now() < deadline, "{described}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn an_account_keeps_a_bounded_number_of_batch_jobs() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "manyjobs");
    let call = |body: &str| {
        let (status, answer) = send_with(
            server.s3_port,
            "POST",
            "/v20180820/jobs",
            "",
            &[("x-amz-account-id", &account)],
            body.as_bytes(),
            &key,
            Some(&secret),
        )
        .1;
        (status, answer)
    };
    // Jobs whose manifest bucket does not exist: they fail at once, and stay.
    let body = |n: usize| {
        create_job_body(
            &format!("t{n}"),
            "<S3DeleteObjectTagging/>",
            "m.csv",
            "\"e\"",
            "",
        )
    };
    for n in 0..1000 {
        assert_eq!(call(&body(n)).0, 200, "job {n}");
    }
    let (status, answer) = call(&body(1000));
    assert_eq!(
        (status, xml_value(&answer, "Code").as_str()),
        (400, "TooManyRequestsException")
    );
    // A repeat of a kept job's request still finds it.
    let (status, answer) = call(&body(7));
    assert_eq!(status, 200, "{answer}");
}

#[test]
fn batch_jobs_restore_and_lock_what_a_manifest_lists() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "batchlock");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        let mut headers = vec![("x-amz-account-id", account.as_str())];
        headers.extend_from_slice(extra);
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &headers,
            body,
            &key,
            Some(&secret),
        )
    };
    for bucket in ["/src", "/reports", "/cold"] {
        assert_eq!(call("PUT", bucket, "", &[], b"").1.0, 200);
    }
    assert_eq!(
        call(
            "PUT",
            "/vault",
            "",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            b""
        )
        .1
        .0,
        200
    );
    assert_eq!(
        call(
            "PUT",
            "/cold/g1",
            "",
            &[("x-amz-storage-class", "GLACIER")],
            b"frozen"
        )
        .1
        .0,
        200
    );
    assert_eq!(call("PUT", "/cold/s1", "", &[], b"warm").1.0, 200);
    for name in ["v1", "v2"] {
        assert_eq!(
            call("PUT", &format!("/vault/{name}"), "", &[], b"kept").1.0,
            200
        );
    }
    let manifest = |name: &str, lines: &str| {
        let (head, (status, _)) = call("PUT", &format!("/src/{name}"), "", &[], lines.as_bytes());
        assert_eq!(status, 200);
        response_header(&head, "etag").unwrap()
    };
    let run_as = |job_key: &str,
                  job_secret: &str,
                  token: &str,
                  operation: &str,
                  list: &str,
                  etag: &str|
     -> String {
        let body = create_job_body(token, operation, list, etag, "");
        let headers = [("x-amz-account-id", account.as_str())];
        let (status, answer) = send_with(
            server.s3_port,
            "POST",
            "/v20180820/jobs",
            "",
            &headers,
            body.as_bytes(),
            job_key,
            Some(job_secret),
        )
        .1;
        assert_eq!(status, 200, "{answer}");
        let job = xml_value(&answer, "JobId");
        let deadline = Instant::now() + DEADLINE;
        loop {
            let described = call("GET", &format!("/v20180820/jobs/{job}"), "", &[], b"")
                .1
                .1;
            // The job's own Status, after its progress (a legal hold has one).
            let status = described.rsplit("</ProgressSummary>").next().unwrap_or("");
            if ["Complete", "Failed"].contains(&xml_value(status, "Status").as_str()) {
                return described;
            }
            assert!(Instant::now() < deadline, "{described}");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let run = |token: &str, operation: &str, list: &str, etag: &str| {
        run_as(&key, &secret, token, operation, list, etag)
    };
    let counts = |described: &str| {
        (
            xml_value(described, "NumberOfTasksSucceeded"),
            xml_value(described, "NumberOfTasksFailed"),
        )
    };

    // Restore: the archived object is restored; the other is in its active
    // tier already.
    let etag = manifest("restore.csv", "cold,g1\ncold,s1\n");
    let restore = "<S3InitiateRestoreObject><ExpirationInDays>2</ExpirationInDays>\
                   <GlacierJobTier>BULK</GlacierJobTier></S3InitiateRestoreObject>";
    let described = run("restore-1", restore, "restore.csv", &etag);
    assert_eq!(counts(&described), ("1".into(), "1".into()), "{described}");
    let (head, _) = call("HEAD", "/cold/g1", "", &[], b"");
    assert!(response_header(&head, "x-amz-restore").is_some(), "{head}");

    // Retention: GOVERNANCE set on both versions; on a bucket without
    // Object Lock, refused.
    let etag = manifest("lock.csv", "vault,v1\nvault,v2\ncold,s1\n");
    let governance = "<S3PutObjectRetention><Retention><Mode>GOVERNANCE</Mode>\
                      <RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention></S3PutObjectRetention>";
    let described = run("retain-1", governance, "lock.csv", &etag);
    assert_eq!(counts(&described), ("2".into(), "1".into()), "{described}");
    let (_, (_, retention)) = call("GET", "/vault/v1", "retention", &[], b"");
    assert!(retention.contains("<Mode>GOVERNANCE</Mode>"), "{retention}");
    // Removing it needs the bypass, and the right to bypass: a user
    // without it is refused.
    let remove = "<S3PutObjectRetention><BypassGovernanceRetention>true</BypassGovernanceRetention>\
                  <Retention/></S3PutObjectRetention>";
    let etag = manifest("vault.csv", "vault,v1\n");
    let policy = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:CreateJob", "s3:GetObject", "s3:PutObject", "s3:PutObjectRetention"],
         "Resource": "*"}]}"#
        .replace('\n', " ");
    let (user_key, user_secret) = user_with_key(&server, &account, "keeper", Some(&policy));
    let described = run_as(
        &user_key,
        &user_secret,
        "remove-user",
        remove,
        "vault.csv",
        &etag,
    );
    assert_eq!(counts(&described), ("0".into(), "1".into()), "{described}");
    let described = run("remove-root", remove, "vault.csv", &etag);
    assert_eq!(counts(&described), ("1".into(), "0".into()), "{described}");
    let (status, _) = call("GET", "/vault/v1", "retention", &[], b"").1;
    assert_eq!(status, 404, "the retention is gone");

    // Legal hold: on, and the version is kept.
    let hold =
        "<S3PutObjectLegalHold><LegalHold><Status>ON</Status></LegalHold></S3PutObjectLegalHold>";
    let described = run("hold-1", hold, "vault.csv", &etag);
    assert_eq!(counts(&described), ("1".into(), "0".into()), "{described}");
    let (_, (_, held)) = call("GET", "/vault/v1", "legal-hold", &[], b"");
    assert!(held.contains("<Status>ON</Status>"), "{held}");
    let (_, (_, versions)) = call("GET", "/vault", "versions&prefix=v1", &[], b"");
    let version = xml_value(&versions, "VersionId");
    let (status, _) = call(
        "DELETE",
        "/vault/v1",
        &format!("versionId={version}"),
        &[],
        b"",
    )
    .1;
    assert_eq!(status, 403, "a version under legal hold is not removed");

    // A user allowed none of these operations: each task refused.
    let reader = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:CreateJob", "s3:GetObject", "s3:PutObject"], "Resource": "*"}]}"#
        .replace('\n', " ");
    let (reader_key, reader_secret) = user_with_key(&server, &account, "reader", Some(&reader));
    let etag = manifest("v2.csv", "vault,v2\ncold,g1\n");
    for (token, operation) in [
        ("deny-retain", governance),
        ("deny-hold", hold),
        ("deny-restore", restore),
    ] {
        let described = run_as(
            &reader_key,
            &reader_secret,
            token,
            operation,
            "v2.csv",
            &etag,
        );
        assert_eq!(
            xml_value(&described, "NumberOfTasksSucceeded"),
            "0",
            "{token}: {described}"
        );
    }

    // A date that passes while the job waits: each task fails, and the job
    // is still described and reports.
    let soon = format!(
        "<S3PutObjectRetention><Retention><Mode>GOVERNANCE</Mode>\
         <RetainUntilDate>{}</RetainUntilDate></Retention></S3PutObjectRetention>",
        iso_in(3)
    );
    let etag = manifest("late.csv", "vault,v2\n");
    let body = create_job_body(
        "late-1",
        &soon,
        "late.csv",
        &etag,
        "<ConfirmationRequired>true</ConfirmationRequired>",
    );
    let (status, answer) = call("POST", "/v20180820/jobs", "", &[], body.as_bytes()).1;
    assert_eq!(status, 200, "{answer}");
    let late = xml_value(&answer, "JobId");
    std::thread::sleep(Duration::from_secs(5));
    call(
        "POST",
        &format!("/v20180820/jobs/{late}/status"),
        "requestedJobStatus=Ready",
        &[],
        b"",
    );
    let deadline = Instant::now() + DEADLINE;
    let described = loop {
        let (status, described) = call("GET", &format!("/v20180820/jobs/{late}"), "", &[], b"").1;
        assert_eq!(status, 200, "{described}");
        let job_status = described
            .rsplit("</ProgressSummary>")
            .next()
            .unwrap_or("")
            .to_string();
        if xml_value(&job_status, "Status") == "Complete" {
            break described;
        }
        assert!(Instant::now() < deadline, "{described}");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        xml_value(&described, "NumberOfTasksFailed"),
        "1",
        "{described}"
    );
    let (_, (_, listed)) = call(
        "GET",
        "/reports",
        &format!("list-type=2&prefix=rep/job-{late}/"),
        &[],
        b"",
    );
    assert!(listed.contains("manifest.json"), "{listed}");

    // A retention in the past is refused at CreateJob, as the API refuses it.
    let past = governance.replace("2099", "2001");
    let body = create_job_body("past-1", &past, "lock.csv", "\"x\"", "");
    let (status, answer) = call("POST", "/v20180820/jobs", "", &[], body.as_bytes()).1;
    assert_eq!(status, 400, "{answer}");
}

#[test]
fn batch_jobs_run_on_a_manifest_they_generate() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "batchgen");
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        let mut headers = vec![("x-amz-account-id", account.as_str())];
        headers.extend_from_slice(extra);
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &headers,
            body,
            &key,
            Some(&secret),
        )
    };
    for bucket in ["/data", "/out", "/reports"] {
        assert_eq!(call("PUT", bucket, "", &[], b"").1.0, 200);
    }
    for (name, body, class) in [
        ("logs/a.txt", &b"small"[..], "STANDARD"),
        ("logs/b.txt", &[b'x'; 100][..], "STANDARD"),
        ("logs/c.csv", &b"small"[..], "STANDARD"),
        ("logs/d.txt", &b"tiny"[..], "STANDARD"),
        ("logs/g.txt", &b"cold"[..], "GLACIER"),
        ("img/x.txt", &b"small"[..], "STANDARD"),
    ] {
        let path = format!("/data/{name}");
        assert_eq!(
            call("PUT", &path, "", &[("x-amz-storage-class", class)], body)
                .1
                .0,
            200
        );
    }
    let body = |token: &str, generator: &str| {
        format!(
            "<CreateJobRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
             <ClientRequestToken>{token}</ClientRequestToken>\
             <Operation><S3PutObjectTagging><TagSet><S3Tag><Key>k</Key><Value>v</Value></S3Tag>\
             </TagSet></S3PutObjectTagging></Operation>\
             <ManifestGenerator><S3JobManifestGenerator>{generator}</S3JobManifestGenerator></ManifestGenerator>\
             <Report><Enabled>true</Enabled><Bucket>arn:aws:s3:::reports</Bucket>\
             <Format>Report_CSV_20180820</Format><ReportScope>AllTasks</ReportScope></Report>\
             <Priority>1</Priority><RoleArn>arn:aws:iam::111122223333:role/batch</RoleArn>\
             </CreateJobRequest>"
        )
    };
    let filtered = "<SourceBucket>arn:aws:s3:::data</SourceBucket><EnableManifestOutput>true</EnableManifestOutput>\
        <ManifestOutputLocation><Bucket>arn:aws:s3:::out</Bucket><ManifestPrefix>gen</ManifestPrefix>\
        <ManifestFormat>S3InventoryReport_CSV_20211130</ManifestFormat></ManifestOutputLocation>\
        <Filter><KeyNameConstraint><MatchAnyPrefix><member>logs/</member></MatchAnyPrefix>\
        <MatchAnySuffix><member>.txt</member></MatchAnySuffix></KeyNameConstraint>\
        <ObjectSizeLessThanBytes>50</ObjectSizeLessThanBytes>\
        <MatchAnyStorageClass><member>STANDARD</member></MatchAnyStorageClass></Filter>";
    let run = |job_key: &str, job_secret: &str, request: String| -> String {
        let headers = [("x-amz-account-id", account.as_str())];
        let (status, answer) = send_with(
            server.s3_port,
            "POST",
            "/v20180820/jobs",
            "",
            &headers,
            request.as_bytes(),
            job_key,
            Some(job_secret),
        )
        .1;
        assert_eq!(status, 200, "{answer}");
        let job = xml_value(&answer, "JobId");
        let deadline = Instant::now() + DEADLINE;
        loop {
            let described = call("GET", &format!("/v20180820/jobs/{job}"), "", &[], b"")
                .1
                .1;
            let status = described
                .rsplit("</ProgressSummary>")
                .next()
                .unwrap_or("")
                .to_string();
            if ["Complete", "Failed"].contains(&xml_value(&status, "Status").as_str()) {
                return described;
            }
            assert!(Instant::now() < deadline, "{described}");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let described = run(&key, &secret, body("gen-1", filtered));
    assert_eq!(
        xml_value(&described, "TotalNumberOfTasks"),
        "2",
        "{described}"
    );
    assert_eq!(xml_value(&described, "NumberOfTasksSucceeded"), "2");
    for (name, tagged) in [
        ("logs/a.txt", true),
        ("logs/d.txt", true),
        ("logs/b.txt", false),
        ("logs/g.txt", false),
        ("img/x.txt", false),
    ] {
        let (_, (_, tags)) = call("GET", &format!("/data/{name}"), "tagging", &[], b"");
        assert_eq!(tags.contains("<Key>k</Key>"), tagged, "{name}: {tags}");
    }
    // The generated manifest, written and described.
    let descriptor = xml_value(&described, "GeneratedManifestDescriptor");
    let arn = xml_value(&descriptor, "ObjectArn");
    let path = arn.strip_prefix("arn:aws:s3:::").unwrap();
    assert!(path.starts_with("out/gen/job-"), "{path}");
    let (_, (status, lines)) = call("GET", &format!("/{path}"), "", &[], b"");
    assert_eq!(
        (status, lines.as_str()),
        (200, "data,logs/a.txt,\ndata,logs/d.txt,\n")
    );

    // A generator that keeps nothing, writing nothing out: complete at once.
    let none = "<SourceBucket>arn:aws:s3:::data</SourceBucket><EnableManifestOutput>false</EnableManifestOutput>\
        <Filter><CreatedAfter>2099-01-01T00:00:00Z</CreatedAfter></Filter>";
    let described = run(&key, &secret, body("gen-2", none));
    assert_eq!(
        xml_value(&described, "TotalNumberOfTasks"),
        "0",
        "{described}"
    );
    assert!(!described.contains("GeneratedManifestDescriptor"));

    // Refused: both a manifest and a generator; a filter not offered; an
    // output asked for nowhere.
    let code = |request: String| {
        let (status, answer) = call("POST", "/v20180820/jobs", "", &[], request.as_bytes()).1;
        (status, xml_value(&answer, "Code"))
    };
    let both = body("gen-3", none).replace(
        "<ManifestGenerator>",
        "<Manifest><Spec><Format>S3BatchOperations_CSV_20180820</Format></Spec>\
         <Location><ObjectArn>arn:aws:s3:::data/m</ObjectArn><ETag>e</ETag></Location></Manifest><ManifestGenerator>",
    );
    assert_eq!(code(both), (400, "InvalidRequest".to_string()));
    let encryption = none.replace(
        "<Filter>",
        "<Filter><MatchAnyObjectEncryption><SSE-S3/></MatchAnyObjectEncryption>",
    );
    assert_eq!(
        code(body("gen-4", &encryption)),
        (501, "NotImplemented".to_string())
    );
    let nowhere = "<SourceBucket>arn:aws:s3:::data</SourceBucket><EnableManifestOutput>true</EnableManifestOutput>";
    assert_eq!(
        code(body("gen-5", nowhere)),
        (400, "InvalidRequest".to_string())
    );

    // A listing over more than one page.
    assert_eq!(call("PUT", "/many", "", &[], b"").1.0, 200);
    for n in 0..1005 {
        assert_eq!(
            call("PUT", &format!("/many/k{n:04}"), "", &[], b"x").1.0,
            200
        );
    }
    let everything = "<SourceBucket>arn:aws:s3:::many</SourceBucket><EnableManifestOutput>false</EnableManifestOutput>";
    let described = run(&key, &secret, body("gen-7", everything));
    assert_eq!(
        xml_value(&described, "TotalNumberOfTasks"),
        "1005",
        "{described}"
    );
    assert_eq!(xml_value(&described, "NumberOfTasksSucceeded"), "1005");

    // Generating a manifest takes s3:PutInventoryConfiguration on the
    // bucket, as AWS asks.
    let policy = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:CreateJob", "s3:PutObject", "s3:PutObjectTagging"], "Resource": "*"}]}"#
        .replace('\n', " ");
    let (user_key, user_secret) = user_with_key(&server, &account, "lister", Some(&policy));
    let described = run(&user_key, &user_secret, body("gen-6", none));
    assert_eq!(
        xml_value(&described, "FailureCode"),
        "AccessDenied",
        "{described}"
    );
}

/// A function's webhook on a free port: answers each POSTed invocation
/// with what `answer` makes of it (status, body), and keeps the requests.
fn function_receiver(
    answer: impl Fn(&serde_json::Value) -> (u16, String) + Send + 'static,
) -> (u16, Received) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = std::sync::Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    break;
                }
                let mut headers = Vec::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let (name, value) = header.split_once(':').unwrap();
                    headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
                }
                let len: usize = headers
                    .iter()
                    .find(|(name, _)| name == "content-length")
                    .map_or(0, |(_, value)| value.parse().unwrap());
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();
                let event: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
                let (status, reply) = answer(&event);
                kept.lock().unwrap().push((headers, body));
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\n\r\n{reply}",
                    reply.len()
                );
            }
        }
    });
    (port, seen)
}

#[test]
fn batch_jobs_invoke_a_function_for_each_object() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "batchfn");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &[("x-amz-account-id", account.as_str())],
            body,
            &key,
            Some(&secret),
        )
    };
    for bucket in ["/src", "/data", "/reports"] {
        assert_eq!(call("PUT", bucket, "", b"").1.0, 200);
    }
    for name in ["ok1", "ok2", "perm", "temp", "bad"] {
        assert_eq!(call("PUT", &format!("/data/{name}"), "", b"x").1.0, 200);
    }
    let (port, received) = function_receiver(|event| {
        let task = &event["tasks"][0];
        let id = task["taskId"].clone();
        let key = task["s3Key"].as_str().unwrap_or("").to_string();
        let result = |code: &str, text: &str| {
            serde_json::json!({
                "invocationSchemaVersion": "2.0",
                "invocationId": event["invocationId"],
                "results": [{"taskId": id, "resultCode": code, "resultString": text}],
            })
            .to_string()
        };
        match key.as_str() {
            "perm" => (200, result("PermanentFailure", "no")),
            "temp" => (200, result("TemporaryFailure", "later")),
            "bad" => (200, "not json".to_string()),
            _ => (200, result("Succeeded", &format!("done:{key}"))),
        }
    });
    native(
        server.native_port,
        &format!("stream create topic fn account {account}"),
    );
    native(
        server.native_port,
        &format!("stream create webhook fn url http://127.0.0.1:{port}/ secret s3cret"),
    );
    // Another account's bucket: never sent.
    let (_, other_key, other_secret) = account_with_key(&server, "fnoutsider");
    assert_eq!(
        send(
            server.s3_port,
            "PUT",
            "/theirs",
            "",
            b"",
            &other_key,
            Some(&other_secret)
        )
        .0,
        200
    );
    assert_eq!(
        send(
            server.s3_port,
            "PUT",
            "/theirs/x",
            "",
            b"x",
            &other_key,
            Some(&other_secret)
        )
        .0,
        200
    );
    let (head, (status, _)) = call(
        "PUT",
        "/src/fn.csv",
        "",
        b"data,ok1\ndata,ok2\ndata,perm\ndata,temp\ndata,bad\ntheirs,x\n",
    );
    assert_eq!(status, 200);
    let etag = response_header(&head, "etag").unwrap();
    let invoke = |function: &str| {
        format!(
            "<LambdaInvoke><FunctionArn>arn:aws:lambda:us-east-1:{account}:function:{function}</FunctionArn>\
             <InvocationSchemaVersion>2.0</InvocationSchemaVersion>\
             <UserArguments><entry><key>k</key><value>v</value></entry></UserArguments></LambdaInvoke>"
        )
    };
    let run = |job_key: &str, job_secret: &str, token: &str, operation: &str| -> String {
        let body = create_job_body(token, operation, "fn.csv", &etag, "");
        let headers = [("x-amz-account-id", account.as_str())];
        let (status, answer) = send_with(
            server.s3_port,
            "POST",
            "/v20180820/jobs",
            "",
            &headers,
            body.as_bytes(),
            job_key,
            Some(job_secret),
        )
        .1;
        assert_eq!(status, 200, "{answer}");
        let job = xml_value(&answer, "JobId");
        let deadline = Instant::now() + DEADLINE;
        loop {
            let described = call("GET", &format!("/v20180820/jobs/{job}"), "", b"").1.1;
            let status = described
                .rsplit("</ProgressSummary>")
                .next()
                .unwrap_or("")
                .to_string();
            if ["Complete", "Failed"].contains(&xml_value(&status, "Status").as_str()) {
                return described;
            }
            assert!(Instant::now() < deadline, "{described}");
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let described = run(&key, &secret, "fn-1", &invoke("fn"));
    let job = xml_value(&described, "JobId");
    assert_eq!(
        xml_value(&described, "NumberOfTasksSucceeded"),
        "2",
        "{described}"
    );
    assert_eq!(xml_value(&described, "NumberOfTasksFailed"), "4");
    // The events: schema 2.0 (the bucket's name, the user arguments),
    // signed; a temporary failure tried three times.
    let requests = received.lock().unwrap().clone();
    let events: Vec<serde_json::Value> = requests
        .iter()
        .map(|(_, body)| serde_json::from_slice(body).unwrap())
        .collect();
    assert_eq!(events[0]["invocationSchemaVersion"], "2.0");
    assert_eq!(events[0]["job"]["userArguments"]["k"], "v");
    assert_eq!(events[0]["job"]["id"], job.as_str());
    assert_eq!(events[0]["tasks"][0]["s3Bucket"], "data");
    let temp = events
        .iter()
        .filter(|event| event["tasks"][0]["s3Key"] == "temp")
        .count();
    assert_eq!(temp, 3, "a temporary failure is tried again");
    assert!(
        events.iter().all(|event| event["tasks"][0]["s3Key"] != "x"),
        "another account's object is not sent"
    );
    for (headers, body) in &requests {
        let header = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        let (delivery, signature) = (header("x-truthdb-delivery"), header("x-truthdb-signature"));
        let (time, digest) = signature.split_once(",v1=").unwrap();
        let secs = time.strip_prefix("t=").unwrap();
        let mac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"s3cret");
        let expected = ring::hmac::sign(
            &mac,
            &[format!("{secs}.{delivery}.").as_bytes(), body].concat(),
        );
        let expected: String = expected
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(digest, expected, "signed");
    }
    // The report: results and why.
    let (_, (_, manifest)) = call(
        "GET",
        &format!("/reports/rep/job-{job}/manifest.json"),
        "",
        b"",
    );
    let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    let rows: String = manifest["Results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| {
            call(
                "GET",
                &format!("/reports/{}", result["Key"].as_str().unwrap()),
                "",
                b"",
            )
            .1
            .1
        })
        .collect();
    assert!(rows.contains("data,ok1,,succeeded,200,,done:ok1"), "{rows}");
    assert!(
        rows.contains("data,perm,,failed,200,PermanentFailure,no"),
        "{rows}"
    );
    assert!(
        rows.contains("data,temp,,failed,200,TemporaryFailure,later"),
        "{rows}"
    );
    assert!(
        rows.contains(
            "data,bad,,failed,200,PermanentFailure,Invalid JSON returned in Lambda payload"
        ),
        "{rows}"
    );
    assert!(rows.contains("theirs,,,failed,403,AccessDenied"), "{rows}");

    // A function that names no topic with a webhook: the job fails.
    let described = run(&key, &secret, "fn-2", &invoke("missing"));
    assert_eq!(
        xml_value(&described, "FailureCode"),
        "LambdaFunctionNotFound",
        "{described}"
    );
    // Invoking takes lambda:InvokeFunction.
    let policy = r#"{"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:CreateJob", "s3:GetObject", "s3:PutObject"], "Resource": "*"}]}"#
        .replace('\n', " ");
    let (user_key, user_secret) = user_with_key(&server, &account, "caller", Some(&policy));
    let described = run(&user_key, &user_secret, "fn-3", &invoke("fn"));
    assert_eq!(
        xml_value(&described, "FailureCode"),
        "AccessDenied",
        "{described}"
    );
    // A Deny on the function's ARN holds however the job names it: here by
    // its bare name.
    let deny = format!(
        r#"{{"Version": "2012-10-17", "Statement": [
        {{"Effect": "Allow", "Action": ["s3:CreateJob", "s3:GetObject", "s3:PutObject", "lambda:InvokeFunction"], "Resource": "*"}},
        {{"Effect": "Deny", "Action": "lambda:InvokeFunction", "Resource": "arn:aws:lambda:*:{account}:function:fn"}}]}}"#
    )
    .replace('\n', " ");
    let (denied_key, denied_secret) = user_with_key(&server, &account, "denied", Some(&deny));
    let bare = "<LambdaInvoke><FunctionArn>fn</FunctionArn></LambdaInvoke>";
    let described = run(&denied_key, &denied_secret, "fn-5", bare);
    assert_eq!(
        xml_value(&described, "FailureCode"),
        "AccessDenied",
        "{described}"
    );

    // User arguments need schema 2.0.
    let old = invoke("fn").replace("2.0", "1.0");
    let body = create_job_body("fn-4", &old, "fn.csv", &etag, "");
    assert_eq!(
        call("POST", "/v20180820/jobs", "", body.as_bytes()).1.0,
        400
    );
}

#[test]
fn a_slow_function_holds_up_no_other_batch_job() {
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "batchslow");
    let call = |method: &str, path: &str, query: &str, body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            &[("x-amz-account-id", account.as_str())],
            body,
            &key,
            Some(&secret),
        )
    };
    for bucket in ["/src", "/data", "/reports"] {
        assert_eq!(call("PUT", bucket, "", b"").1.0, 200);
    }
    assert_eq!(call("PUT", "/data/a", "", b"x").1.0, 200);
    // A function taking 2 s an invocation.
    let (port, _) = function_receiver(|event| {
        std::thread::sleep(Duration::from_secs(2));
        let result = serde_json::json!({
            "results": [{"taskId": event["tasks"][0]["taskId"], "resultCode": "Succeeded", "resultString": ""}],
        });
        (200, result.to_string())
    });
    native(
        server.native_port,
        &format!("stream create topic slow account {account}"),
    );
    native(
        server.native_port,
        &format!("stream create webhook slow url http://127.0.0.1:{port}/ secret s3cret"),
    );
    let manifest = |name: &str, lines: &str| {
        let (head, (status, _)) = call("PUT", &format!("/src/{name}"), "", lines.as_bytes());
        assert_eq!(status, 200);
        response_header(&head, "etag").unwrap()
    };
    let slow_lines: String = (0..16).map(|_| "data,a\n").collect();
    let etag = manifest("slow.csv", &slow_lines);
    let invoke = "<LambdaInvoke><FunctionArn>slow</FunctionArn></LambdaInvoke>";
    let body = create_job_body("slow-1", invoke, "slow.csv", &etag, "")
        .replace("<Priority>10</Priority>", "<Priority>99</Priority>");
    assert_eq!(
        call("POST", "/v20180820/jobs", "", body.as_bytes()).1.0,
        200
    );
    // Once the slow job invokes, a tagging job still runs to its end.
    std::thread::sleep(Duration::from_secs(1));
    let etag = manifest("tag.csv", "data,a\n");
    let tagging = "<S3PutObjectTagging><TagSet><S3Tag><Key>k</Key><Value>v</Value></S3Tag></TagSet></S3PutObjectTagging>";
    let body = create_job_body("tag-1", tagging, "tag.csv", &etag, "");
    let (_, (status, answer)) = call("POST", "/v20180820/jobs", "", body.as_bytes());
    assert_eq!(status, 200, "{answer}");
    let job = xml_value(&answer, "JobId");
    let began = Instant::now();
    loop {
        let described = call("GET", &format!("/v20180820/jobs/{job}"), "", b"").1.1;
        if xml_value(&described, "Status") == "Complete" {
            break;
        }
        assert!(
            began.elapsed() < Duration::from_secs(10),
            "held up by the slow job: {described}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn cors_preflights_and_cross_origin_answers_follow_the_bucket_configuration() {
    let server = start_server();
    let created = native(server.native_port, "s3 create access key");
    let key = created["access_key_id"].as_str().unwrap().to_string();
    let secret = created["secret_access_key"].as_str().unwrap().to_string();
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(
            server.s3_port,
            method,
            path,
            query,
            extra,
            body,
            &key,
            Some(&secret),
        )
    };
    // A browser's preflight: unsigned.
    let preflight = |path: &str, extra: &[(&str, &str)]| {
        send_with(server.s3_port, "OPTIONS", path, "", extra, b"", "", None)
    };
    assert_eq!(call("PUT", "/web", "", &[], b"").1.0, 200);
    assert_eq!(
        call("PUT", "/web/page.html", "", &[], b"<p>hi</p>").1.0,
        200
    );
    let ask = [
        ("origin", "https://app.example"),
        ("access-control-request-method", "PUT"),
    ];
    let (_, (status, body)) = preflight("/web/page.html", &ask);
    assert_eq!(status, 403, "no configuration yet: {body}");
    assert!(
        body.contains("CORS is not enabled for this bucket"),
        "{body}"
    );
    let (_, (status, _)) = call("GET", "/web", "cors", &[], b"");
    assert_eq!(status, 404);

    let config = "<CORSConfiguration><CORSRule><ID>app</ID><AllowedOrigin>https://*.example</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod>\
        <AllowedHeader>content-type</AllowedHeader><AllowedHeader>x-amz-*</AllowedHeader>\
        <ExposeHeader>ETag</ExposeHeader><MaxAgeSeconds>600</MaxAgeSeconds></CORSRule>\
        <CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>\
        </CORSConfiguration>";
    // The configuration needs its Content-MD5.
    let (_, (status, _)) = call("PUT", "/web", "cors", &[], config.as_bytes());
    assert_eq!(status, 400);
    let md5 = content_md5(config.as_bytes());
    let (_, (status, body)) = call(
        "PUT",
        "/web",
        "cors",
        &[("content-md5", &md5)],
        config.as_bytes(),
    );
    assert_eq!(status, 200, "{body}");
    // A configuration whose stored form (escaped) would pass the limit.
    let quoted = format!(
        "<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod>\
         <AllowedHeader>{}</AllowedHeader></CORSRule></CORSConfiguration>",
        "\"".repeat(20_000)
    );
    let quoted_md5 = content_md5(quoted.as_bytes());
    let (_, (status, _)) = call(
        "PUT",
        "/web",
        "cors",
        &[("content-md5", &quoted_md5)],
        quoted.as_bytes(),
    );
    assert_eq!(status, 400, "too large once stored");
    let (_, (status, shown)) = call("GET", "/web", "cors", &[], b"");
    assert_eq!(status, 200);
    assert!(
        shown.contains("<ID>app</ID>") && shown.contains("<MaxAgeSeconds>600</MaxAgeSeconds>"),
        "{shown}"
    );

    // A preflight the first rule allows.
    let asking = [
        ("origin", "https://app.example"),
        ("access-control-request-method", "PUT"),
        ("access-control-request-headers", "Content-Type, X-Amz-Date"),
    ];
    let (head, (status, _)) = preflight("/web/page.html", &asking);
    assert_eq!(status, 200, "{head}");
    assert_eq!(
        response_header(&head, "access-control-allow-origin").as_deref(),
        Some("https://app.example")
    );
    assert_eq!(
        response_header(&head, "access-control-allow-methods").as_deref(),
        Some("GET, PUT")
    );
    assert_eq!(
        response_header(&head, "access-control-allow-headers").as_deref(),
        Some("Content-Type, X-Amz-Date")
    );
    assert_eq!(
        response_header(&head, "access-control-max-age").as_deref(),
        Some("600")
    );
    assert_eq!(
        response_header(&head, "access-control-expose-headers").as_deref(),
        Some("ETag")
    );
    assert!(response_header(&head, "vary").unwrap().contains("Origin"));
    // The second rule: any origin, GET only, answered with "*".
    let (head, (status, _)) = preflight(
        "/web",
        &[
            ("origin", "https://other.site"),
            ("access-control-request-method", "GET"),
        ],
    );
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "access-control-allow-origin").as_deref(),
        Some("*")
    );
    // Refused: a method no rule allows that origin, a header not allowed,
    // a preflight missing what it asks for.
    let refused = [
        vec![
            ("origin", "https://other.site"),
            ("access-control-request-method", "PUT"),
        ],
        vec![
            ("origin", "https://app.example"),
            ("access-control-request-method", "PUT"),
            ("access-control-request-headers", "authorization"),
        ],
    ];
    for headers in &refused {
        let (head, (status, body)) = preflight("/web/page.html", headers);
        assert_eq!(status, 403, "{headers:?}: {body}");
        assert!(body.contains("This CORS request is not allowed"), "{body}");
        assert!(response_header(&head, "access-control-allow-origin").is_none());
    }
    let (_, (status, _)) = preflight("/web/page.html", &[("origin", "https://app.example")]);
    assert_eq!(status, 400, "no requested method");
    // A preflight asking for more headers than any answer would list.
    let many: String = (0..65)
        .map(|n| format!("x-amz-h{n}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (_, (status, _)) = preflight(
        "/web/page.html",
        &[
            ("origin", "https://app.example"),
            ("access-control-request-method", "PUT"),
            ("access-control-request-headers", &many),
        ],
    );
    assert_eq!(status, 403, "too many headers asked for");

    // Cross-origin requests: the headers on their answers, errors too.
    let (head, (status, _)) = call(
        "GET",
        "/web/page.html",
        "",
        &[("origin", "https://app.example")],
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "access-control-allow-origin").as_deref(),
        Some("https://app.example")
    );
    assert_eq!(
        response_header(&head, "access-control-expose-headers").as_deref(),
        Some("ETag")
    );
    let (head, (status, _)) = call(
        "GET",
        "/web/missing",
        "",
        &[("origin", "https://app.example")],
        b"",
    );
    assert_eq!(status, 404);
    assert!(
        response_header(&head, "access-control-allow-origin").is_some(),
        "on an error too"
    );
    // An origin no rule allows for DELETE: the request goes on, without them.
    let (head, (status, _)) = call(
        "DELETE",
        "/web/page.html",
        "",
        &[("origin", "https://other.site")],
        b"",
    );
    assert_eq!(status, 204);
    assert!(response_header(&head, "access-control-allow-origin").is_none());
    // Without an Origin: none.
    let (head, _) = call("GET", "/web", "", &[], b"");
    assert!(response_header(&head, "access-control-allow-origin").is_none());

    // Deleted: gone, and preflights refused again.
    assert_eq!(call("DELETE", "/web", "cors", &[], b"").1.0, 204);
    assert_eq!(call("GET", "/web", "cors", &[], b"").1.0, 404);
    assert_eq!(preflight("/web", &asking).1.0, 403);
}

#[test]
fn a_bucket_is_named_by_the_host_under_a_configured_domain() {
    // The test servers take `s3.test` for their domain.
    let server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "hosted");
    let virtual_host = format!("vbucket.s3.test:{port}");
    let call =
        |host: &str, method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
            let mut headers = vec![("host", host)];
            headers.extend_from_slice(extra);
            send_with(
                port,
                method,
                path,
                query,
                &headers,
                body,
                &key,
                Some(&secret),
            )
        };
    // CreateBucket and PutObject addressed by the host: the path is the
    // whole key, slashes and all.
    let (_, (status, _)) = call(&virtual_host, "PUT", "/", "", &[], b"");
    assert_eq!(status, 200);
    let (_, (status, _)) = call(&virtual_host, "PUT", "/a/b.txt", "", &[], b"hosted body");
    assert_eq!(status, 200);
    // Path-style, on the domain itself or another host, reads it back.
    for host in [format!("s3.test:{port}"), format!("127.0.0.1:{port}")] {
        let (_, (status, body)) = call(&host, "GET", "/vbucket/a/b.txt", "", &[], b"");
        assert_eq!((status, body.as_str()), (200, "hosted body"), "{host}");
    }
    // A bucket of the host: listed, and the host's case does not matter.
    let (_, (status, listed)) = call(
        &format!("VBucket.S3.Test:{port}"),
        "GET",
        "/",
        "list-type=2",
        &[],
        b"",
    );
    assert_eq!(status, 200, "{listed}");
    assert_eq!(xml_value(&listed, "Key"), "a/b.txt");
    // A dotted bucket name is the whole leading part of the host.
    let (_, (status, _)) = call(
        &format!("dotted.name.s3.test:{port}"),
        "PUT",
        "/",
        "",
        &[],
        b"",
    );
    assert_eq!(status, 200);
    let (_, (status, _)) = call(
        &format!("127.0.0.1:{port}"),
        "HEAD",
        "/dotted.name",
        "",
        &[],
        b"",
    );
    assert_eq!(status, 200);
    // An object at an S3 Control path in a bucket named by the host is that
    // object, header or not: the host names a bucket, not the account.
    let account_header = [("x-amz-account-id", account.as_str())];
    let (_, (status, _)) = call(
        &virtual_host,
        "PUT",
        "/v20180820/jobs",
        "",
        &account_header,
        b"job",
    );
    assert_eq!(status, 200);
    let (_, (status, body)) = call(
        &format!("127.0.0.1:{port}"),
        "GET",
        "/vbucket/v20180820/jobs",
        "",
        &[],
        b"",
    );
    assert_eq!((status, body.as_str()), (200, "job"));
    // S3 Control with the account as the host's first label (as the SDKs
    // address it), and path-style.
    for host in [
        format!("{account}.s3.test:{port}"),
        format!("127.0.0.1:{port}"),
    ] {
        let (_, (status, jobs)) = call(&host, "GET", "/v20180820/jobs", "", &account_header, b"");
        assert_eq!(status, 200, "{host}: {jobs}");
        assert!(jobs.contains("<ListJobsResult"), "{jobs}");
    }
    // A request target in absolute form must name the Host header's host.
    let absolute = |target_host: &str| {
        let request = signed_request(
            port,
            "GET",
            "/a/b.txt",
            "",
            &[("host", virtual_host.as_str())],
            b"",
            &key,
            Some(&secret),
        );
        let request = request.replacen(
            "GET /a/b.txt ",
            &format!("GET http://{target_host}/a/b.txt "),
            1,
        );
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.write_all(request.as_bytes()).expect("send");
        let mut answer = String::new();
        stream.read_to_string(&mut answer).expect("answer");
        answer
    };
    let answer = absolute(&format!("other.s3.test:{port}"));
    assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
    let answer = absolute(&virtual_host.to_uppercase());
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(answer.ends_with("hosted body"), "{answer}");
    // Errors carry the host's bucket.
    let (_, (status, missing)) = call(&format!("absent.s3.test:{port}"), "GET", "/k", "", &[], b"");
    assert_eq!(status, 404);
    assert_eq!(xml_value(&missing, "Code"), "NoSuchBucket");
    assert_eq!(
        xml_value(&missing, "Message"),
        "The bucket absent does not exist"
    );
}

#[test]
fn boto3_addresses_buckets_by_host() {
    // A Python with boto3 installed (pip install boto3).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_BOTO3_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_BOTO3_PYTHON names no Python with boto3");
        return;
    };
    let server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "boto");
    let script = server.dir.join("virtual.py");
    std::fs::write(
        &script,
        r#"
import socket, sys, urllib.request
import boto3, botocore.config
port, account, key, secret = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
# Every *.s3.test name is the local listener.
resolve = socket.getaddrinfo
socket.getaddrinfo = lambda host, *rest, **kw: resolve("127.0.0.1", *rest, **kw)
sent = []
def client(service, **config):
    made = boto3.client(service, endpoint_url=f"http://s3.test:{port}", region_name="us-east-1",
        aws_access_key_id=key, aws_secret_access_key=secret, config=botocore.config.Config(**config))
    made.meta.events.register("before-send", lambda request, **kw: sent.append(request.url))
    return made
s3 = client("s3", s3={"addressing_style": "virtual"}, signature_version="s3v4")
s3.create_bucket(Bucket="hosted")
s3.put_object(Bucket="hosted", Key="dir/one.txt", Body=b"one")
assert s3.get_object(Bucket="hosted", Key="dir/one.txt")["Body"].read() == b"one"
listed = s3.list_objects_v2(Bucket="hosted", Prefix="dir/")
assert [item["Key"] for item in listed["Contents"]] == ["dir/one.txt"], listed
url = s3.generate_presigned_url("get_object", Params={"Bucket": "hosted", "Key": "dir/one.txt"})
assert url.startswith(f"http://hosted.s3.test:{port}/dir/one.txt?"), url
assert urllib.request.urlopen(url).read() == b"one"
s3.delete_object(Bucket="hosted", Key="dir/one.txt")
s3.delete_bucket(Bucket="hosted")
assert all(url.startswith(f"http://hosted.s3.test:{port}/") for url in sent), sent
control = client("s3control")
jobs = control.list_jobs(AccountId=account)
assert jobs["Jobs"] == [], jobs
assert sent[-1].startswith(f"http://{account}.s3.test:{port}/v20180820/jobs"), sent
print("ok")
"#,
    )
    .expect("script");
    let output = Command::new(python)
        .arg(&script)
        .args([&port.to_string(), &account, &key, &secret])
        .output()
        .expect("run python");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("ok"),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn reads_answer_conditional_and_range_requests_as_caches_expect() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    assert_eq!(call("PUT", "/cached", "", &[], b"").1.0, 200);
    let versioning = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(
        call("PUT", "/cached", "versioning", &[], versioning).1.0,
        200
    );
    let cache_headers = [
        ("cache-control", "public, max-age=60"),
        ("expires", "Thu, 01 Jan 2099 00:00:00 GMT"),
    ];
    let (first, _) = call("PUT", "/cached/k", "", &cache_headers, b"0123456789 first");
    let first_version = response_header(&first, "x-amz-version-id").expect("a version");
    let (second, _) = call("PUT", "/cached/k", "", &cache_headers, b"0123456789 second");
    // Each version is its own representation: the current one, and the
    // first by its version id.
    for (query, body, put) in [
        (String::new(), "0123456789 second", &second),
        (
            format!("versionId={first_version}"),
            "0123456789 first",
            &first,
        ),
    ] {
        let etag = response_header(put, "etag").expect("an etag");
        let (head, (status, read)) = call("GET", "/cached/k", &query, &[], b"");
        assert_eq!((status, read.as_str()), (200, body));
        assert_eq!(
            response_header(&head, "etag").as_deref(),
            Some(etag.as_str())
        );
        let modified = response_header(&head, "last-modified").expect("last-modified");
        let earlier = "Mon, 01 Jan 2001 00:00:00 GMT";
        let later = "Thu, 01 Jan 2099 00:00:00 GMT";
        let other = "\"00000000000000000000000000000000\"";
        // What a cache stores and revalidates with, on every answer.
        let cached = |head: &str| {
            assert_eq!(
                response_header(head, "etag").as_deref(),
                Some(etag.as_str()),
                "{head}"
            );
            assert_eq!(
                response_header(head, "last-modified").as_deref(),
                Some(modified.as_str()),
                "{head}"
            );
            assert_eq!(
                response_header(head, "cache-control").as_deref(),
                Some("public, max-age=60"),
                "{head}"
            );
            assert_eq!(
                response_header(head, "expires").as_deref(),
                Some(later),
                "{head}"
            );
        };
        cached(&head);
        assert_eq!(
            response_header(&head, "accept-ranges").as_deref(),
            Some("bytes")
        );
        // Conditions alone, then with a Range: a failed condition wins over
        // the range, a passed one leaves it to the range.
        let conditions: [(&[(&str, &str)], u16); 9] = [
            (&[("if-none-match", &etag)], 304),
            (&[("if-none-match", other)], 200),
            (&[("if-match", &etag)], 200),
            (&[("if-match", other)], 412),
            (&[("if-modified-since", &modified)], 304),
            (&[("if-modified-since", earlier)], 200),
            (&[("if-unmodified-since", earlier)], 412),
            // S3's documented pairs: If-Match decides over If-Unmodified-Since,
            // If-None-Match over If-Modified-Since.
            (
                &[("if-match", &etag), ("if-unmodified-since", earlier)],
                200,
            ),
            (
                &[("if-none-match", &etag), ("if-modified-since", earlier)],
                304,
            ),
        ];
        for (headers, expected) in conditions {
            for range in [None, Some("bytes=2-5")] {
                let mut extra = headers.to_vec();
                extra.extend(range.map(|range| ("range", range)));
                for method in ["GET", "HEAD"] {
                    let (head, (status, read)) = call(method, "/cached/k", &query, &extra, b"");
                    let expected = match (expected, range) {
                        (200, Some(_)) => 206,
                        (status, _) => status,
                    };
                    assert_eq!(status, expected, "{method} {query} {extra:?}: {head}");
                    match status {
                        304 => {
                            cached(&head);
                            assert!(read.is_empty());
                        }
                        206 => {
                            cached(&head);
                            assert_eq!(
                                response_header(&head, "content-range").as_deref(),
                                Some(format!("bytes 2-5/{}", body.len()).as_str())
                            );
                            if method == "GET" {
                                assert_eq!(read, &body[2..6]);
                            }
                        }
                        200 => {
                            cached(&head);
                            if method == "GET" {
                                assert_eq!(read, body);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        // If-Range: the range of this representation, or the whole of it
        // when the validator names another one (RFC 9110 13.1.5).
        let weak = format!("W/{etag}");
        for (validator, expected) in [
            (etag.as_str(), 206),
            (other, 200),
            (modified.as_str(), 206),
            (earlier, 200),
            (weak.as_str(), 200),
        ] {
            let extra = [("range", "bytes=2-5"), ("if-range", validator)];
            let (head, (status, read)) = call("GET", "/cached/k", &query, &extra, b"");
            assert_eq!(status, expected, "If-Range {validator}: {head}");
            assert_eq!(read, if expected == 206 { &body[2..6] } else { body });
        }
    }
    // A signed GET may override the cache headers it is answered with.
    let (head, (status, _)) = call(
        "GET",
        "/cached/k",
        "response-cache-control=no-store&response-expires=Mon%2C%2001%20Jan%202001%2000%3A00%3A00%20GMT",
        &[],
        b"",
    );
    assert_eq!(status, 200);
    assert_eq!(
        response_header(&head, "cache-control").as_deref(),
        Some("no-store")
    );
    assert_eq!(
        response_header(&head, "expires").as_deref(),
        Some("Mon, 01 Jan 2001 00:00:00 GMT")
    );
}

#[test]
fn a_caching_proxy_serves_revalidates_and_slices_objects() {
    // An nginx binary (with the slice module, as distributions build it).
    let Some(nginx) = std::env::var_os("TRUTHDB_TEST_NGINX").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_NGINX names no nginx");
        return;
    };
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    // A bucket anyone may read: the proxy forwards unsigned requests.
    assert_eq!(call("PUT", "/edge", "", &[], b"").0, 200);
    let off = b"<PublicAccessBlockConfiguration/>";
    assert_eq!(call("PUT", "/edge", "publicAccessBlock", &[], off).0, 200);
    let public = br#"{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::edge/*"}]}"#;
    assert_eq!(call("PUT", "/edge", "policy", &[], public).0, 204);
    let short = [("cache-control", "max-age=1")];
    let long = [("cache-control", "max-age=3600")];
    assert_eq!(
        call("PUT", "/edge/s", "", &long, b"abcdefghijklmnopqrstuvwxyz").0,
        200
    );

    let dir = server.dir.join("nginx");
    std::fs::create_dir_all(dir.join("cache")).expect("dir");
    let (proxy_port, _, _) = free_ports();
    let d = dir.display();
    std::fs::write(
        dir.join("nginx.conf"),
        format!(
            "worker_processes 1;
pid {d}/nginx.pid;
events {{}}
http {{
  access_log off;
  client_body_temp_path {d}/body;
  proxy_temp_path {d}/proxy;
  fastcgi_temp_path {d}/fastcgi;
  uwsgi_temp_path {d}/uwsgi;
  scgi_temp_path {d}/scgi;
  proxy_cache_path {d}/cache levels=1 keys_zone=s3:1m;
  server {{
    listen 127.0.0.1:{proxy_port};
    add_header X-Cache $upstream_cache_status always;
    location / {{
      proxy_pass http://127.0.0.1:{port};
      proxy_cache s3;
      proxy_cache_revalidate on;
    }}
    location /sliced/ {{
      rewrite ^/sliced/(.*)$ /$1 break;
      proxy_pass http://127.0.0.1:{port};
      slice 4;
      proxy_cache s3;
      proxy_cache_key $uri$slice_range;
      proxy_set_header Range $slice_range;
      proxy_cache_valid 200 206 1h;
    }}
  }}
}}
"
        ),
    )
    .expect("config");
    struct Proxy(Child);
    impl Drop for Proxy {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut proxy = Proxy(
        Command::new(nginx)
            .arg("-p")
            .arg(&dir)
            .arg("-c")
            .arg(dir.join("nginx.conf"))
            .arg("-e")
            .arg(dir.join("error.log"))
            .args(["-g", "daemon off;"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn nginx"),
    );
    let deadline = Instant::now() + DEADLINE;
    // nginx listens in its master process, the one spawned.
    while !listens(proxy.0.id(), proxy_port) {
        assert!(
            proxy.0.try_wait().expect("poll nginx").is_none(),
            "nginx exited: {}",
            std::fs::read_to_string(dir.join("error.log")).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "nginx never listened");
        std::thread::sleep(Duration::from_millis(50));
    }
    let via = |path: &str, extra: &str| {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", proxy_port)).expect("connect");
        stream
            .write_all(format!("GET {path} HTTP/1.0\r\nHost: edge\r\n{extra}\r\n").as_bytes())
            .expect("send");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read");
        let (head, (status, body)) = parse_response(&raw);
        let cache = response_header(&head, "x-cache").unwrap_or_default();
        (status, body, cache)
    };
    let expired = || std::thread::sleep(Duration::from_millis(2100));

    // Stored and served from the cache; then overwritten within the same
    // second, so Last-Modified alone does not tell the versions apart: the
    // revalidation's If-None-Match fails, and the new body is sent.
    let modified = || {
        let (head, (status, _)) =
            send_with(port, "HEAD", "/edge/k", "", &[], b"", &key, Some(&secret));
        assert_eq!(status, 200);
        response_header(&head, "last-modified").expect("last-modified")
    };
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_millis();
    std::thread::sleep(Duration::from_millis(u64::from(1000 - millis)));
    assert_eq!(call("PUT", "/edge/k", "", &short, b"first body").0, 200);
    assert_eq!(
        via("/edge/k", ""),
        (200, "first body".into(), "MISS".into())
    );
    assert_eq!(via("/edge/k", ""), (200, "first body".into(), "HIT".into()));
    let first = modified();
    assert_eq!(call("PUT", "/edge/k", "", &short, b"other body").0, 200);
    assert_eq!(modified(), first, "overwritten within the second");
    expired();
    assert_eq!(
        via("/edge/k", ""),
        (200, "other body".into(), "EXPIRED".into())
    );
    assert_eq!(via("/edge/k", ""), (200, "other body".into(), "HIT".into()));
    // Unchanged: revalidated with a conditional request answered 304.
    expired();
    assert_eq!(
        via("/edge/k", ""),
        (200, "other body".into(), "REVALIDATED".into())
    );

    // Fetched in 4-byte slices with Range requests: any range, and the
    // whole object, are put together from them.
    let (status, body, _) = via("/sliced/edge/s", "Range: bytes=5-12\r\n");
    assert_eq!((status, body.as_str()), (206, "fghijklm"));
    let (status, body, _) = via("/sliced/edge/s", "");
    assert_eq!((status, body.as_str()), (200, "abcdefghijklmnopqrstuvwxyz"));
    let (status, body, cache) = via("/sliced/edge/s", "Range: bytes=-3\r\n");
    assert_eq!((status, body.as_str(), cache.as_str()), (206, "xyz", "HIT"));
    drop(proxy);
}

#[test]
fn objects_keep_the_website_redirect_location_their_write_names() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret))
    };
    let redirect = |path: &str| {
        let (head, (status, _)) = call("HEAD", path, "", &[], b"");
        assert_eq!(status, 200, "{path}");
        response_header(&head, "x-amz-website-redirect-location")
    };
    const LOCATION: &str = "x-amz-website-redirect-location";
    assert_eq!(call("PUT", "/hops", "", &[], b"").1.0, 200);
    // A path in the bucket or an http(s) URL; nothing else.
    for location in [
        "/moved.html",
        "http://example.com/a",
        "https://example.com/",
    ] {
        assert_eq!(
            call("PUT", "/hops/k", "", &[(LOCATION, location)], b"x")
                .1
                .0,
            200
        );
        assert_eq!(redirect("/hops/k").as_deref(), Some(location));
        let (head, (status, _)) = call("GET", "/hops/k", "", &[], b"");
        assert_eq!(status, 200);
        assert_eq!(response_header(&head, LOCATION).as_deref(), Some(location));
    }
    for location in ["moved.html", "ftp://example.com/", ""] {
        let (_, (status, reply)) = call("PUT", "/hops/bad", "", &[(LOCATION, location)], b"x");
        assert_eq!(status, 400, "{location:?}");
        assert_eq!(xml_value(&reply, "Code"), "InvalidRedirectLocation");
    }
    assert_eq!(call("HEAD", "/hops/bad", "", &[], b"").1.0, 404);
    // A plain overwrite has none.
    assert_eq!(call("PUT", "/hops/k", "", &[], b"x").1.0, 200);
    assert_eq!(redirect("/hops/k"), None);

    // A copy has the location its request names, never the source's, with
    // either metadata directive; naming one is a change of an object copied
    // to itself.
    assert_eq!(
        call("PUT", "/hops/src", "", &[(LOCATION, "/from-source")], b"s")
            .1
            .0,
        200
    );
    for (extra, expected) in [
        (vec![("x-amz-copy-source", "/hops/src")], None),
        (
            vec![("x-amz-copy-source", "/hops/src"), (LOCATION, "/copied")],
            Some("/copied"),
        ),
        (
            vec![
                ("x-amz-copy-source", "/hops/src"),
                ("x-amz-metadata-directive", "REPLACE"),
                (LOCATION, "/replaced"),
            ],
            Some("/replaced"),
        ),
    ] {
        let (_, (status, reply)) = call("PUT", "/hops/copy", "", &extra, b"");
        assert_eq!(status, 200, "{extra:?}: {reply}");
        assert_eq!(redirect("/hops/copy").as_deref(), expected, "{extra:?}");
    }
    let (_, (status, reply)) = call(
        "PUT",
        "/hops/copy",
        "",
        &[("x-amz-copy-source", "/hops/copy")],
        b"",
    );
    assert_eq!(status, 400, "{reply}");
    let (_, (status, reply)) = call(
        "PUT",
        "/hops/copy",
        "",
        &[("x-amz-copy-source", "/hops/copy"), (LOCATION, "/again")],
        b"",
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(redirect("/hops/copy").as_deref(), Some("/again"));
    let (_, (status, reply)) = call(
        "PUT",
        "/hops/copy",
        "",
        &[("x-amz-copy-source", "/hops/src"), (LOCATION, "nowhere")],
        b"",
    );
    assert_eq!(status, 400);
    assert_eq!(xml_value(&reply, "Code"), "InvalidRedirectLocation");

    // A multipart upload's, from its start.
    let (_, (status, started)) = call(
        "POST",
        "/hops/parts",
        "uploads",
        &[(LOCATION, "/whole")],
        b"",
    );
    assert_eq!(status, 200);
    let upload = xml_value(&started, "UploadId");
    let (head, (status, _)) = call(
        "PUT",
        "/hops/parts",
        &format!("partNumber=1&uploadId={upload}"),
        &[],
        b"part",
    );
    assert_eq!(status, 200);
    let etag = response_header(&head, "etag").expect("etag");
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let (_, (status, reply)) = call(
        "POST",
        "/hops/parts",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
    );
    assert_eq!(status, 200, "{reply}");
    assert_eq!(redirect("/hops/parts").as_deref(), Some("/whole"));

    // A form upload's, from its field.
    let conditions = [
        serde_json::json!({"bucket": "hops"}),
        serde_json::json!({"key": "formed"}),
        serde_json::json!({LOCATION: "https://example.com/form"}),
    ];
    let fields = [("key", "formed"), (LOCATION, "https://example.com/form")];
    let form = signed_form(
        &key,
        &secret,
        "2099-01-01T00:00:00.000Z",
        &conditions,
        &fields,
    );
    let (_, (status, reply)) = post_form(port, "hops", &form, "f.txt", b"form", 1000);
    assert_eq!(status, 204, "{reply}");
    assert_eq!(
        redirect("/hops/formed").as_deref(),
        Some("https://example.com/form")
    );
}

#[test]
fn a_bucket_keeps_its_website_configuration() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    let put = |bucket: &str, config: &str| {
        let md5 = content_md5(config.as_bytes());
        call(
            "PUT",
            bucket,
            "website",
            &[("content-md5", &md5)],
            config.as_bytes(),
        )
    };
    let site = "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument>\
        <ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules><RoutingRule>\
        <Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>\
        <Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect>\
        </RoutingRule></RoutingRules></WebsiteConfiguration>";
    assert_eq!(put("/nosuch", site).0, 404);
    assert_eq!(call("PUT", "/site", "", &[], b"").0, 200);
    let (status, missing) = call("GET", "/site", "website", &[], b"");
    assert_eq!(status, 404);
    assert_eq!(xml_value(&missing, "Code"), "NoSuchWebsiteConfiguration");
    // The configuration needs its Content-MD5 (or a checksum).
    assert_eq!(call("PUT", "/site", "website", &[], site.as_bytes()).0, 400);
    let (status, body) = put("/site", site);
    assert_eq!(status, 200, "{body}");
    let (status, shown) = call("GET", "/site", "website", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(xml_value(&shown, "Suffix"), "index.html");
    assert_eq!(xml_value(&shown, "Key"), "error.html");
    assert_eq!(xml_value(&shown, "KeyPrefixEquals"), "docs/");
    assert_eq!(xml_value(&shown, "ReplaceKeyPrefixWith"), "documents/");
    // A refused configuration leaves the stored one.
    let (status, refused) = put(
        "/site",
        "<WebsiteConfiguration><IndexDocument><Suffix>a/b</Suffix></IndexDocument></WebsiteConfiguration>",
    );
    assert_eq!(status, 400);
    assert_eq!(xml_value(&refused, "Code"), "InvalidArgument");
    // One whose stored form (escaped) would pass the limit.
    let quoted = format!(
        "<WebsiteConfiguration><IndexDocument><Suffix>{}</Suffix></IndexDocument></WebsiteConfiguration>",
        "\"".repeat(30_000)
    );
    assert_eq!(put("/site", &quoted).0, 400);
    assert_eq!(
        xml_value(&call("GET", "/site", "website", &[], b"").1, "Suffix"),
        "index.html"
    );
    // Replaced by a redirect of every request.
    let all = "<WebsiteConfiguration><RedirectAllRequestsTo><HostName>www.example.com</HostName>\
        <Protocol>https</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>";
    assert_eq!(put("/site", all).0, 200);
    let (_, shown) = call("GET", "/site", "website", &[], b"");
    assert_eq!(xml_value(&shown, "HostName"), "www.example.com");
    assert!(!shown.contains("IndexDocument"), "{shown}");
    assert_eq!(call("DELETE", "/site", "website", &[], b"").0, 204);
    assert_eq!(call("GET", "/site", "website", &[], b"").0, 404);
    assert_eq!(call("DELETE", "/site", "website", &[], b"").0, 204);
    // Gone with its bucket.
    assert_eq!(put("/site", site).0, 200);
    assert_eq!(call("DELETE", "/site", "", &[], b"").0, 204);
    assert_eq!(call("PUT", "/site", "", &[], b"").0, 200);
    assert_eq!(call("GET", "/site", "website", &[], b"").0, 404);
    assert_eq!(call("GET", "/site/k", "website", &[], b"").0, 501);
}

#[test]
fn a_website_endpoint_serves_public_objects_anonymously() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    let md5_put = |path: &str, query: &str, body: &str| {
        let md5 = content_md5(body.as_bytes());
        call(
            "PUT",
            path,
            query,
            &[("content-md5", &md5)],
            body.as_bytes(),
        )
    };
    let host = |bucket: &str| format!("{bucket}.s3-website.s3.test:{port}");
    // Unsigned (or signed: the endpoint takes every request as anonymous).
    let visit = |bucket: &str, method: &str, path: &str, extra: &[(&str, &str)], signed: bool| {
        let site = host(bucket);
        let mut headers = vec![("host", site.as_str())];
        headers.extend_from_slice(extra);
        let (head, (status, body)) = if signed {
            send_with(port, method, path, "", &headers, b"", &key, Some(&secret))
        } else {
            send_with(port, method, path, "", &headers, b"", "", None)
        };
        (status, head, body)
    };
    let get = |bucket: &str, path: &str| visit(bucket, "GET", path, &[], false);
    let location = |head: &str| response_header(head, "location").unwrap_or_default();

    // No bucket; a bucket without a website configuration.
    let (status, head, body) = get("nosuch", "/");
    assert_eq!(status, 404);
    assert!(body.contains("<li>Code: NoSuchBucket</li>"), "{body}");
    assert_eq!(
        response_header(&head, "x-amz-error-code").as_deref(),
        Some("NoSuchBucket")
    );
    assert_eq!(call("PUT", "/site", "", &[], b"").0, 200);
    let (status, _, body) = get("site", "/");
    assert_eq!(status, 404);
    assert!(
        body.contains("<li>Code: NoSuchWebsiteConfiguration</li>"),
        "{body}"
    );

    // A public site.
    let html = [("content-type", "text/html")];
    for (name, content) in [
        ("index.html", "home page"),
        ("sub/index.html", "sub page"),
        ("error.html", "custom error page"),
        ("private/secret", "secret"),
        ("private/index.html", "private index"),
        ("docs/a", "old doc"),
    ] {
        assert_eq!(
            call(
                "PUT",
                &format!("/site/{name}"),
                "",
                &html,
                content.as_bytes()
            )
            .0,
            200
        );
    }
    assert_eq!(
        call(
            "PUT",
            "/site/moved",
            "",
            &[("x-amz-website-redirect-location", "/elsewhere")],
            b"x"
        )
        .0,
        200
    );
    assert_eq!(
        call(
            "PUT",
            "/site",
            "publicAccessBlock",
            &[],
            b"<PublicAccessBlockConfiguration/>"
        )
        .0,
        200
    );
    let policy = |extra: &str| {
        format!(
            r#"{{"Version": "2012-10-17", "Statement": [
            {{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/*"}},
            {{"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/private/*"}}{extra}]}}"#
        )
    };
    assert_eq!(
        call("PUT", "/site", "policy", &[], policy("").as_bytes()).0,
        204
    );
    let config = "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument>\
        <ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules>\
        <RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>\
        <Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect></RoutingRule>\
        <RoutingRule><Condition><KeyPrefixEquals>images/</KeyPrefixEquals>\
        <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>\
        <Redirect><HostName>example.com</HostName><HttpRedirectCode>302</HttpRedirectCode>\
        <ReplaceKeyPrefixWith>report-404/</ReplaceKeyPrefixWith></Redirect></RoutingRule>\
        </RoutingRules></WebsiteConfiguration>";
    assert_eq!(md5_put("/site", "website", config).0, 200);

    // Index documents; a folder named without its slash is redirected.
    let (status, head, body) = get("site", "/");
    assert_eq!((status, body.as_str()), (200, "home page"));
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/html")
    );
    assert_eq!(get("site", "/sub/").2, "sub page");
    let (status, head, body) = get("site", "/sub");
    assert_eq!(
        (status, location(&head).as_str(), body.as_str()),
        (302, "/sub/", "")
    );
    let (status, _, body) = visit("site", "GET", "/", &[("range", "bytes=0-3")], false);
    assert_eq!((status, body.as_str()), (206, "home"));
    let (status, _, body) = visit("site", "HEAD", "/", &[], false);
    assert_eq!((status, body.as_str()), (200, ""));

    // A missing key: 403 unless the public may list the bucket, with the
    // error document; a denied object: 403 too, signed or not.
    let (status, head, body) = get("site", "/missing");
    assert_eq!((status, body.as_str()), (403, "custom error page"));
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/html")
    );
    // A folder whose index the public may not read is not redirected to.
    let (status, head, body) = get("site", "/private");
    assert_eq!(
        (status, body.as_str()),
        (403, "custom error page"),
        "{head}"
    );
    for signed in [false, true] {
        let (status, _, body) = visit("site", "GET", "/private/secret", &[], signed);
        assert_eq!(
            (status, body.as_str()),
            (403, "custom error page"),
            "signed: {signed}"
        );
    }
    let listable = r#",
        {"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::site"}"#;
    assert_eq!(
        call("PUT", "/site", "policy", &[], policy(listable).as_bytes()).0,
        204
    );
    let (status, _, body) = get("site", "/missing");
    assert_eq!((status, body.as_str()), (404, "custom error page"));
    // An error document the public may not read is not shown.
    let hidden = r#",
        {"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::site"},
        {"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/error.html"}"#;
    assert_eq!(
        call("PUT", "/site", "policy", &[], policy(hidden).as_bytes()).0,
        204
    );
    let (status, head, body) = get("site", "/missing");
    assert_eq!(status, 404);
    assert!(body.contains("<li>Code: NoSuchKey</li>"), "{body}");
    assert!(body.contains("<li>Key: missing</li>"), "{body}");
    assert!(!body.contains("custom error page"));
    assert_eq!(
        response_header(&head, "content-type").as_deref(),
        Some("text/html; charset=utf-8")
    );
    // What a key holds is escaped in the page.
    let (_, _, body) = get("site", "/%3Cscript%3Ealert(1)%3C%2Fscript%3E");
    assert!(body.contains("&lt;script&gt;"), "{body}");
    assert!(!body.contains("<script>"), "{body}");

    // Redirects: a routing rule on the key, one on the error, an object's.
    let (status, head, body) = get("site", "/docs/a");
    assert_eq!(
        (status, location(&head), body.as_str()),
        (301, format!("http://{}/documents/a", host("site")), "")
    );
    let (status, head, _) = get("site", "/images/x%20y.png");
    assert_eq!(
        (status, location(&head)),
        (302, "http://example.com/report-404/x%20y.png".to_string())
    );
    let (status, head, _) = get("site", "/moved");
    assert_eq!((status, location(&head)), (301, "/elsewhere".to_string()));

    // Only GET and HEAD.
    let (status, _, body) = visit("site", "PUT", "/new", &[], true);
    assert_eq!(status, 405);
    assert!(body.contains("<li>Code: MethodNotAllowed</li>"), "{body}");
    assert_eq!(call("GET", "/site/new", "", &[], b"").0, 404);

    // Every request redirected.
    assert_eq!(call("PUT", "/moving", "", &[], b"").0, 200);
    let all = "<WebsiteConfiguration><RedirectAllRequestsTo><HostName>www.example.com</HostName>\
        <Protocol>https</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>";
    assert_eq!(md5_put("/moving", "website", all).0, 200);
    let (status, head, _) = get("moving", "/a/b.html");
    assert_eq!(
        (status, location(&head)),
        (301, "https://www.example.com/a/b.html".to_string())
    );

    // A website path is a key, not a bucket's: no other bucket's CORS
    // configuration answers for it.
    assert_eq!(call("PUT", "/open", "", &[], b"").0, 200);
    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    assert_eq!(md5_put("/open", "cors", cors).0, 200);
    let (_, head, _) = visit(
        "site",
        "GET",
        "/open/k",
        &[("origin", "https://a.example")],
        false,
    );
    assert_eq!(
        response_header(&head, "access-control-allow-origin"),
        None,
        "{head}"
    );

    // Blocking public access closes the site.
    let block = "<PublicAccessBlockConfiguration><RestrictPublicBuckets>true</RestrictPublicBuckets>\
        </PublicAccessBlockConfiguration>";
    assert_eq!(
        call("PUT", "/site", "publicAccessBlock", &[], block.as_bytes()).0,
        200
    );
    let (status, _, body) = get("site", "/");
    assert_eq!(status, 403);
    assert!(body.contains("<li>Code: AccessDenied</li>"), "{body}");
    // The REST endpoint is unchanged.
    let (status, body) = call("GET", "/site/index.html", "", &[], b"");
    assert_eq!((status, body.as_str()), (200, "home page"));
}

#[test]
fn bucket_usage_counts_versions_markers_and_upload_parts() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    let usage = || {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let rows = sql_rows(
                server.native_port,
                "SELECT bucket_name, version_kind, storage_class, object_count, size_bytes, complete \
                 FROM sys.dm_s3_bucket_usage WHERE bucket_name = 'metered' \
                 ORDER BY version_kind, storage_class",
            );
            if rows.iter().all(|row| row[5] == "1") || Instant::now() > deadline {
                return rows
                    .into_iter()
                    .map(|row| format!("{} {} {} {}", row[1], row[2], row[3], row[4]))
                    .collect::<Vec<_>>();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    assert_eq!(call("PUT", "/metered", "", &[], b"").0, 200);
    let versioning = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
    assert_eq!(
        call("PUT", "/metered", "versioning", &[], versioning).0,
        200
    );
    assert!(usage().is_empty());
    assert_eq!(call("PUT", "/metered/a", "", &[], b"12345").0, 200);
    assert_eq!(call("PUT", "/metered/a", "", &[], b"1234567").0, 200);
    let glacier = [("x-amz-storage-class", "GLACIER")];
    assert_eq!(call("PUT", "/metered/cold", "", &glacier, b"123").0, 200);
    assert_eq!(call("PUT", "/metered/gone", "", &[], b"12").0, 200);
    assert_eq!(call("DELETE", "/metered/gone", "", &[], b"").0, 204);
    let (status, started) = call("POST", "/metered/big", "uploads", &glacier, b"");
    assert_eq!(status, 200);
    let upload = xml_value(&started, "UploadId");
    for (number, part) in [(1, &b"part one"[..]), (2, b"two")] {
        let query = format!("partNumber={number}&uploadId={upload}");
        assert_eq!(call("PUT", "/metered/big", &query, &[], part).0, 200);
    }
    assert_eq!(
        usage(),
        [
            "CURRENT GLACIER 1 3",
            "CURRENT STANDARD 1 7",
            "DELETE_MARKERS STANDARD 1 0",
            "NONCURRENT STANDARD 2 7",
            "UPLOAD_PARTS GLACIER 2 11",
        ]
    );
    // Removing the marker makes "gone" current again; the upload's abort
    // and the versions' removal empty the counts.
    let listed = call("GET", "/metered", "versions", &[], b"").1;
    let marker = listed
        .split("<DeleteMarker>")
        .nth(1)
        .map(|rest| xml_value(rest, "VersionId"))
        .expect("a delete marker");
    assert_eq!(
        call(
            "DELETE",
            "/metered/gone",
            &format!("versionId={marker}"),
            &[],
            b""
        )
        .0,
        204
    );
    assert_eq!(
        call(
            "DELETE",
            "/metered/big",
            &format!("uploadId={upload}"),
            &[],
            b""
        )
        .0,
        204
    );
    assert_eq!(
        usage(),
        [
            "CURRENT GLACIER 1 3",
            "CURRENT STANDARD 2 9",
            "NONCURRENT STANDARD 1 5",
        ]
    );
}

#[test]
fn storage_shows_what_bodies_take_and_packing_saves() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, "", extra, body, &key, Some(&secret)).1
    };
    let storage = || {
        sql_rows(
            server.native_port,
            "SELECT body_state, pack_level, body_count, logical_bytes, stored_bytes, \
             allocated_bytes, saved_bytes FROM sys.dm_s3_storage ORDER BY body_state, pack_level",
        )
        .into_iter()
        .map(|row| {
            let number = |at: usize| row[at].parse::<i64>().unwrap();
            (
                format!("{} {}", row[0], row[1]),
                [number(2), number(3), number(4), number(5), number(6)],
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert_eq!(call("PUT", "/stored", &[], b"").0, 200);
    let plain: Vec<u8> = (0..300_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    assert_eq!(call("PUT", "/stored/plain", &[], &plain).0, 200);
    // Compressible, and packed as it is written for GLACIER.
    let cold = vec![b'z'; 3_000_000];
    assert_eq!(
        call(
            "PUT",
            "/stored/cold",
            &[("x-amz-storage-class", "GLACIER")],
            &cold
        )
        .0,
        200
    );
    let shown = storage();
    let [bodies, logical, stored, allocated, saved] = shown["LIVE 0"];
    assert_eq!(
        (bodies, logical, stored),
        (1, 300_000, 300_000),
        "{shown:?}"
    );
    assert!(allocated >= stored && allocated % 4096 == 0, "{shown:?}");
    assert_eq!(saved, 0);
    let [bodies, logical, stored, allocated, saved] = shown["LIVE 9"];
    assert_eq!((bodies, logical), (1, 3_000_000), "{shown:?}");
    assert!(
        stored < logical / 100 && allocated < logical / 50,
        "{shown:?}"
    );
    assert_eq!(saved, logical - stored);
    // A multipart object's parts are its bodies; the body made of them
    // takes nothing of its own.
    let started = send_with(
        port,
        "POST",
        "/stored/parts",
        "uploads",
        &[],
        b"",
        &key,
        Some(&secret),
    )
    .1
    .1;
    let upload = xml_value(&started, "UploadId");
    let (head, (status, _)) = send_with(
        port,
        "PUT",
        "/stored/parts",
        &format!("partNumber=1&uploadId={upload}"),
        &[],
        b"one part",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 200);
    let etag = response_header(&head, "etag").unwrap();
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let (_, (status, reply)) = send_with(
        port,
        "POST",
        "/stored/parts",
        &format!("uploadId={upload}"),
        &[],
        complete.as_bytes(),
        &key,
        Some(&secret),
    );
    assert_eq!(status, 200, "{reply}");
    let shown = storage();
    assert_eq!(shown["LIVE 0"][..3], [2, 300_008, 300_008], "{shown:?}");
    // A deleted object's body waits for collection.
    assert_eq!(call("DELETE", "/stored/plain", &[], b"").0, 204);
    let shown = storage();
    assert_eq!(
        shown["COLLECTABLE 0"][..3],
        [1, 300_000, 300_000],
        "{shown:?}"
    );
    assert_eq!(shown["LIVE 0"][..3], [1, 8, 8], "{shown:?}");
}

#[test]
fn requests_are_counted_per_bucket_by_kind_bytes_errors_and_latency() {
    let server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "metered");
    // Every request to "meters", and the bytes each way as the client saw
    // them.
    let sent = std::cell::Cell::new((0u64, 0u64, 0u64));
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        let (head, (status, reply)) =
            send_with(port, method, path, query, extra, body, &key, Some(&secret));
        if path.starts_with("/meters") {
            let (count, up, down) = sent.get();
            sent.set((count + 1, up + body.len() as u64, down + reply.len() as u64));
        }
        let _ = head;
        status
    };
    assert_eq!(call("PUT", "/meters", "", &[], b""), 200);
    assert_eq!(call("PUT", "/other", "", &[], b""), 200);
    assert_eq!(call("PUT", "/meters/a", "", &[], b"0123456789"), 200);
    assert_eq!(call("GET", "/meters/a", "", &[], b""), 200);
    assert_eq!(call("HEAD", "/meters/a", "", &[], b""), 200);
    assert_eq!(call("GET", "/meters/missing", "", &[], b""), 404);
    assert_eq!(call("GET", "/meters", "list-type=2", &[], b""), 200);
    assert_eq!(call("GET", "/meters", "versioning", &[], b""), 200);
    // A copy is a PUT where it goes and a GET where it comes from.
    assert_eq!(
        call(
            "PUT",
            "/other/copy",
            "",
            &[("x-amz-copy-source", "/meters/a")],
            b""
        ),
        200
    );
    // Only buckets that exist count, and a copy's source only when the
    // copy is made: names anyone may send take nothing.
    assert_eq!(
        send(
            port,
            "GET",
            &format!("/{}/k", "x".repeat(60_000)),
            "",
            b"",
            "",
            None
        )
        .0,
        404
    );
    assert_eq!(send(port, "GET", "/ghost/k", "", b"", "", None).0, 404);
    assert_eq!(
        call(
            "PUT",
            "/other/failed",
            "",
            &[("x-amz-copy-source", "/meters/missing")],
            b""
        ),
        404
    );
    assert_eq!(
        call(
            "PUT",
            "/meters/from-ghost",
            "",
            &[("x-amz-copy-source", "/ghost/k")],
            b""
        ),
        404
    );
    // A copy source on what is not a copy takes nothing, though the
    // request succeeds.
    let tagging = b"<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>";
    let tagging_md5 = content_md5(tagging);
    assert_eq!(
        call(
            "PUT",
            "/meters/a",
            "tagging",
            &[
                ("content-md5", &tagging_md5),
                ("x-amz-copy-source", "/not-a-copy-source/k")
            ],
            tagging
        ),
        200
    );
    // Nor does it on a write that is no copy, to a bucket that exists: a
    // legal hold set does not read "other".
    let lock = [("x-amz-bucket-object-lock-enabled", "true")];
    assert_eq!(
        send_with(port, "PUT", "/held", "", &lock, b"", &key, Some(&secret))
            .1
            .0,
        200
    );
    assert_eq!(
        send_with(port, "PUT", "/held/k", "", &[], b"x", &key, Some(&secret))
            .1
            .0,
        200
    );
    let hold = b"<LegalHold><Status>ON</Status></LegalHold>";
    let hold_md5 = content_md5(hold);
    let (_, (status, reply)) = send_with(
        port,
        "PUT",
        "/held/k",
        "legal-hold",
        &[
            ("content-md5", &hold_md5),
            ("x-amz-copy-source", "/other/copy"),
        ],
        hold,
        &key,
        Some(&secret),
    );
    assert_eq!(status, 200, "{reply}");
    // A form upload is a POST, and a preflight (to a bucket with a CORS
    // configuration) a request.
    let form = signed_form(
        &key,
        &secret,
        "2099-01-01T00:00:00.000Z",
        &[
            serde_json::json!({"bucket": "meters"}),
            serde_json::json!({"key": "formed"}),
        ],
        &[("key", "formed")],
    );
    let (_, form_bytes) = form_body(&form, "f.txt", b"form");
    let (_, (status, reply)) = post_form(port, "meters", &form, "f.txt", b"form", 1000);
    assert_eq!(status, 204, "{reply}");
    let (count, up, down) = sent.get();
    sent.set((
        count + 1,
        up + form_bytes.len() as u64,
        down + reply.len() as u64,
    ));
    let cors = b"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin>\
        <AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    let cors_md5 = content_md5(cors);
    assert_eq!(
        call(
            "PUT",
            "/meters",
            "cors",
            &[("content-md5", &cors_md5)],
            cors
        ),
        200
    );
    let (_, (status, reply)) = send_with(
        port,
        "OPTIONS",
        "/meters/a",
        "",
        &[
            ("origin", "https://a.example"),
            ("access-control-request-method", "GET"),
        ],
        b"",
        "",
        None,
    );
    assert_eq!(status, 200, "{reply}");
    let (count, up, down) = sent.get();
    sent.set((count + 1, up, down + reply.len() as u64));
    // An aws-chunked upload counts its payload, not its framing.
    let (status, reply) = put_chunked(
        port,
        "/meters/chunked",
        &[7u8; 1000],
        300,
        &key,
        &secret,
        false,
    );
    assert_eq!(status, 200, "{reply}");
    let (count, up, down) = sent.get();
    sent.set((count + 1, up + 1000, down + reply.len() as u64));
    let delete = b"<Delete><Object><Key>gone</Key></Object></Delete>";
    let md5 = content_md5(delete);
    assert_eq!(
        call(
            "POST",
            "/meters",
            "delete",
            &[("content-md5", &md5)],
            delete
        ),
        200
    );
    assert_eq!(call("DELETE", "/meters/a", "", &[], b""), 204);
    // S3 Control is not a bucket's.
    assert_eq!(
        call(
            "GET",
            "/v20180820/jobs",
            "",
            &[("x-amz-account-id", &account)],
            b""
        ),
        200
    );
    let (count, up, down) = sent.get();
    let row = |bucket: &str| -> Vec<i64> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let rows = sql_rows(
                server.native_port,
                &format!(
                    "SELECT all_requests, get_requests, put_requests, delete_requests, head_requests, \
                     post_requests, list_requests, bytes_downloaded, bytes_uploaded, errors_4xx, errors_5xx \
                     FROM sys.dm_s3_requests WHERE bucket_name = '{bucket}'"
                ),
            );
            let row: Vec<i64> = rows
                .first()
                .map(|row| row.iter().map(|cell| cell.parse().unwrap()).collect())
                .unwrap_or_default();
            // The last answer's body is counted once it is sent.
            if bucket != "meters" || row.get(7) == Some(&(down as i64)) || Instant::now() > deadline
            {
                return row;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    assert_eq!(
        row("meters"),
        // Not its creation: the bucket did not exist yet.
        [
            count as i64 - 1,
            3,
            4,
            2,
            1,
            1,
            1,
            down as i64,
            up as i64,
            2,
            0
        ],
        "all, get (the copy's source too), put, delete (a multi-object one too), head, post, list, \
         bytes down, bytes up, 4xx, 5xx"
    );
    let other = row("other");
    assert_eq!(other[..3], [2, 0, 2], "{other:?}");
    let buckets = sql_rows(
        server.native_port,
        "SELECT bucket_name FROM sys.dm_s3_requests ORDER BY bucket_name",
    );
    assert_eq!(buckets, [["held"], ["meters"], ["other"]]);
    // Each request's latencies, until its answer starts and until it ends.
    let latencies = sql_rows(
        server.native_port,
        "SELECT latency, SUM(request_count) FROM sys.dm_s3_request_latency \
         WHERE bucket_name = 'meters' GROUP BY latency ORDER BY latency",
    );
    assert_eq!(
        latencies,
        [
            ["FIRST_BYTE".to_string(), (count - 1).to_string()],
            ["TOTAL".to_string(), (count - 1).to_string()]
        ]
    );
}

/// An admin listener's answer to `method path` (with `extra` header
/// lines): its status, head and body.
fn admin_call(port: u16, method: &str, path: &str, extra: &str) -> (u16, String, String) {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .write_all(
            format!("{method} {path} HTTP/1.1\r\nHost: admin\r\n{extra}Connection: close\r\n\r\n")
                .as_bytes(),
        )
        .expect("send");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let (head, (status, body)) = parse_response(&raw);
    (status, head, body)
}

#[test]
fn the_admin_listener_serves_metrics_health_and_readiness() {
    let (server, key, secret) = server_with_key();
    let admin = server.admin_port.expect("an admin listener");
    let call = |method: &str, path: &str, body: &[u8]| {
        send(server.s3_port, method, path, "", body, &key, Some(&secret)).0
    };
    assert_eq!(admin_call(admin, "GET", "/healthz", "").0, 200);
    let (status, _, body) = admin_call(admin, "GET", "/readyz", "");
    assert_eq!((status, body.as_str()), (200, "ready: primary\n"));
    // /metrics needs the token.
    let bearer = format!("Authorization: Bearer {ADMIN_TOKEN}\r\n");
    assert_eq!(admin_call(admin, "GET", "/metrics", "").0, 401);
    assert_eq!(
        admin_call(admin, "GET", "/metrics", "Authorization: Bearer wrong\r\n").0,
        401
    );
    // The scheme's case does not matter.
    assert_eq!(
        admin_call(
            admin,
            "GET",
            "/metrics",
            &format!("Authorization: bearer {ADMIN_TOKEN}\r\n")
        )
        .0,
        200
    );
    assert_eq!(admin_call(admin, "POST", "/metrics", &bearer).0, 405);
    assert_eq!(admin_call(admin, "GET", "/elsewhere", &bearer).0, 404);

    assert_eq!(call("PUT", "/scraped", b""), 200);
    assert_eq!(call("PUT", "/scraped/kept", b"0123456789"), 200);
    assert_eq!(call("GET", "/scraped/kept", b""), 200);
    assert_eq!(call("PUT", "/scraped/dropped", b"x"), 200);
    assert_eq!(call("DELETE", "/scraped/dropped", b""), 204);
    let deadline = Instant::now() + DEADLINE;
    let page = loop {
        let (status, head, page) = admin_call(admin, "GET", "/metrics", &bearer);
        assert_eq!(status, 200, "{page}");
        assert_eq!(
            response_header(&head, "content-type").as_deref(),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );
        // The last answer's body is counted once it is sent.
        if page.contains("truthdb_s3_bytes_downloaded_total{bucket=\"scraped\"} 10")
            || Instant::now() > deadline
        {
            break page;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    // Every sample is a name, labels and a number, under its family's TYPE.
    let mut typed = std::collections::HashSet::new();
    for line in page.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            typed.insert(rest.split(' ').next().unwrap().to_string());
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let (series, value) = line.rsplit_once(' ').unwrap_or_else(|| panic!("{line}"));
        value
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("not a number: {line}"));
        let name = series.split('{').next().unwrap();
        let family = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| {
                name.strip_suffix(suffix)
                    .filter(|base| typed.contains(*base))
            })
            .unwrap_or(name);
        assert!(typed.contains(family), "no TYPE for {line}");
    }
    let sample = |series: &str| -> f64 {
        page.lines()
            .find_map(|line| line.strip_prefix(&format!("{series} ")))
            .unwrap_or_else(|| panic!("no {series} in\n{page}"))
            .parse()
            .unwrap()
    };
    assert_eq!(sample("truthdb_s3_requests_total{bucket=\"scraped\"}"), 4.0);
    assert_eq!(
        sample("truthdb_s3_requests_by_kind_total{bucket=\"scraped\",kind=\"put\"}"),
        2.0
    );
    assert_eq!(
        sample("truthdb_s3_bytes_uploaded_total{bucket=\"scraped\"}"),
        11.0
    );
    assert_eq!(
        sample("truthdb_s3_bytes_downloaded_total{bucket=\"scraped\"}"),
        10.0
    );
    assert_eq!(
        sample("truthdb_s3_first_byte_latency_seconds_bucket{bucket=\"scraped\",le=\"+Inf\"}"),
        4.0
    );
    assert_eq!(
        sample("truthdb_s3_first_byte_latency_seconds_count{bucket=\"scraped\"}"),
        4.0
    );
    assert!(sample("truthdb_s3_total_latency_seconds_sum{bucket=\"scraped\"}") > 0.0);
    assert_eq!(
        sample(
            "truthdb_s3_bucket_objects{bucket=\"scraped\",kind=\"CURRENT\",storage_class=\"STANDARD\"}"
        ),
        1.0
    );
    assert_eq!(
        sample(
            "truthdb_s3_bucket_bytes{bucket=\"scraped\",kind=\"CURRENT\",storage_class=\"STANDARD\"}"
        ),
        10.0
    );
    assert_eq!(sample("truthdb_s3_gc_backlog_bodies"), 1.0);
    assert_eq!(sample("truthdb_s3_pack_backlog_bodies"), 0.0);
    assert_eq!(sample("truthdb_standby"), 0.0);
    assert!(sample("truthdb_wal_tail_lsn") > 0.0);
    assert!(sample("truthdb_wal_tail_lsn") >= sample("truthdb_wal_head_lsn"));
    let ratio = sample("truthdb_wal_usage_ratio");
    assert!((0.0..=1.0).contains(&ratio), "{ratio}");
}

#[test]
fn a_bucket_keeps_up_to_a_thousand_metrics_configurations() {
    let (server, key, secret) = server_with_key();
    let port = server.s3_port;
    let call = |method: &str, path: &str, query: &str, extra: &[(&str, &str)], body: &[u8]| {
        send_with(port, method, path, query, extra, body, &key, Some(&secret)).1
    };
    let put = |id: &str, filter: &str| {
        let body = format!("<MetricsConfiguration><Id>{id}</Id>{filter}</MetricsConfiguration>");
        let md5 = content_md5(body.as_bytes());
        call(
            "PUT",
            "/measured",
            &format!("metrics&id={id}"),
            &[("content-md5", &md5)],
            body.as_bytes(),
        )
    };
    assert_eq!(put("a", "").0, 404, "no bucket");
    assert_eq!(call("PUT", "/measured", "", &[], b"").0, 200);
    let (status, listed) = call("GET", "/measured", "metrics", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(xml_value(&listed, "IsTruncated"), "false");
    assert!(!listed.contains("<MetricsConfiguration>"), "{listed}");
    let (status, missing) = call("GET", "/measured", "metrics&id=none", &[], b"");
    assert_eq!(status, 404);
    assert_eq!(xml_value(&missing, "Code"), "NoSuchConfiguration");
    // The body needs no Content-MD5 (the SDKs send none); its Id must be
    // the one named.
    let plain = b"<MetricsConfiguration><Id>a</Id></MetricsConfiguration>";
    assert_eq!(call("PUT", "/measured", "metrics&id=a", &[], plain).0, 200);
    assert_eq!(call("DELETE", "/measured", "metrics&id=a", &[], b"").0, 204);
    let md5 = content_md5(plain);
    let (status, reply) = call(
        "PUT",
        "/measured",
        "metrics&id=b",
        &[("content-md5", &md5)],
        plain,
    );
    assert_eq!(status, 400);
    assert_eq!(xml_value(&reply, "Code"), "InvalidArgument");
    // A Content-MD5 sent must match.
    let (status, reply) = call(
        "PUT",
        "/measured",
        "metrics&id=a",
        &[("content-md5", &md5)],
        b"<MetricsConfiguration><Id>a</Id> </MetricsConfiguration>",
    );
    assert_eq!(status, 400);
    assert_eq!(xml_value(&reply, "Code"), "BadDigest");
    let documents = "<Filter><And><Prefix>documents/</Prefix><Tag><Key>class</Key>\
        <Value>blue</Value></Tag></And></Filter>";
    assert_eq!(put("Documents", documents).0, 200);
    let (status, shown) = call("GET", "/measured", "metrics&id=Documents", &[], b"");
    assert_eq!(status, 200);
    assert_eq!(xml_value(&shown, "Prefix"), "documents/");
    assert_eq!(xml_value(&shown, "Value"), "blue");
    // Replaced whole.
    assert_eq!(
        put("Documents", "<Filter><Prefix>docs/</Prefix></Filter>").0,
        200
    );
    let (_, shown) = call("GET", "/measured", "metrics&id=Documents", &[], b"");
    assert_eq!(xml_value(&shown, "Prefix"), "docs/");
    assert!(!shown.contains("<Tag>"), "{shown}");
    // A thousand at most; one of them may still be replaced.
    for index in 0..999 {
        assert_eq!(put(&format!("m{index:04}"), "").0, 200, "{index}");
    }
    let (status, refused) = put("one-more", "");
    assert_eq!(status, 400);
    assert_eq!(xml_value(&refused, "Code"), "TooManyConfigurations");
    assert_eq!(put("m0007", "<Filter><Prefix>x</Prefix></Filter>").0, 200);
    // Listed a hundred at a time, in id order.
    let mut ids = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let query = match &token {
            Some(token) => format!("metrics&continuation-token={token}"),
            None => "metrics".to_string(),
        };
        let (status, page) = call("GET", "/measured", &query, &[], b"");
        assert_eq!(status, 200);
        let found = page.matches("<MetricsConfiguration>").count();
        assert!(found <= 100, "{found}");
        ids.extend(
            page.split("<Id>")
                .skip(1)
                .map(|rest| rest.split("</Id>").next().unwrap().to_string()),
        );
        if xml_value(&page, "IsTruncated") == "true" {
            token = Some(xml_value(&page, "NextContinuationToken"));
        } else {
            break;
        }
    }
    assert_eq!(ids.len(), 1000);
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
    assert_eq!(ids[0], "Documents");
    // Deleted, then gone; deleting it again is refused.
    assert_eq!(
        call("DELETE", "/measured", "metrics&id=Documents", &[], b"").0,
        204
    );
    assert_eq!(
        call("GET", "/measured", "metrics&id=Documents", &[], b"").0,
        404
    );
    assert_eq!(
        call("DELETE", "/measured", "metrics&id=Documents", &[], b"").0,
        404
    );
    // An object has none; a plain write naming an id is not taken for one.
    assert_eq!(call("GET", "/measured/k", "metrics&id=a", &[], b"").0, 501);
    assert_eq!(call("PUT", "/measured/k", "id=a", &[], b"x").0, 501);
}

#[test]
fn bucket_logging_is_set_by_the_owner_to_a_bucket_of_its_own() {
    // A Python with boto3 installed (pip install boto3).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_BOTO3_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_BOTO3_PYTHON names no Python with boto3");
        return;
    };
    let server = start_server();
    let (_, key, secret) = account_with_key(&server, "logger");
    let (other_account, other_key, other_secret) = account_with_key(&server, "outsider");
    let script = server.dir.join("bucket_logging.py");
    std::fs::write(&script, LOGGING_CONFIG_SCRIPT).expect("script");
    let output = Command::new(&python)
        .arg(&script)
        .args([
            &server.s3_port.to_string(),
            &key,
            &secret,
            &other_key,
            &other_secret,
            &other_account,
        ])
        .output()
        .expect("run python");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("ok"),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

const LOGGING_CONFIG_SCRIPT: &str = r#"
import sys, json
import boto3, botocore.config, botocore.exceptions
port, key, secret, other_key, other_secret, other_account = sys.argv[1:7]
def client(access, secret_key):
    return boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1",
        aws_access_key_id=access, aws_secret_access_key=secret_key,
        config=botocore.config.Config(retries={"max_attempts": 1}))
s3 = client(key, secret)
other = client(other_key, other_secret)
def refused(call, code):
    try:
        call()
        raise SystemExit(f"not refused: {code}")
    except botocore.exceptions.ClientError as error:
        assert error.response["Error"]["Code"] == code, error.response
for bucket in ["source", "logs"]:
    s3.create_bucket(Bucket=bucket)
other.create_bucket(Bucket="elsewhere")
s3.create_bucket(Bucket="locked", ObjectLockEnabledForBucket=True)
# Off at first.
assert "LoggingEnabled" not in s3.get_bucket_logging(Bucket="source")
enabled = {"TargetBucket": "logs", "TargetPrefix": "source/"}
s3.put_bucket_logging(Bucket="source", BucketLoggingStatus={"LoggingEnabled": enabled})
assert s3.get_bucket_logging(Bucket="source")["LoggingEnabled"] == enabled
partitioned = {"TargetBucket": "logs", "TargetPrefix": "p/",
    "TargetObjectKeyFormat": {"PartitionedPrefix": {"PartitionDateSource": "EventTime"}}}
s3.put_bucket_logging(Bucket="source", BucketLoggingStatus={"LoggingEnabled": partitioned})
assert s3.get_bucket_logging(Bucket="source")["LoggingEnabled"] == partitioned
# A bucket may log into itself.
s3.put_bucket_logging(Bucket="logs", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "logs", "TargetPrefix": ""}})
# The target exists, is the owner's, and has no Object Lock.
for target in ["missing", "elsewhere", "locked"]:
    refused(lambda: s3.put_bucket_logging(Bucket="source",
        BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": target, "TargetPrefix": ""}}),
        "InvalidTargetBucketForLogging")
assert s3.get_bucket_logging(Bucket="source")["LoggingEnabled"] == partitioned, "unchanged when refused"
refused(lambda: s3.put_bucket_logging(Bucket="source", BucketLoggingStatus={"LoggingEnabled": {
    "TargetBucket": "logs", "TargetPrefix": "",
    "TargetGrants": [{"Grantee": {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"},
        "Permission": "READ"}]}}), "AccessControlListNotSupported")
refused(lambda: s3.put_bucket_logging(Bucket="nowhere",
    BucketLoggingStatus={"LoggingEnabled": enabled}), "NoSuchBucket")
# Only the owner sets it, even where a policy allows another account.
s3.put_bucket_policy(Bucket="source", Policy=json.dumps({"Version": "2012-10-17", "Statement": [{
    "Effect": "Allow", "Principal": {"AWS": f"arn:aws:iam::{other_account}:root"},
    "Action": ["s3:PutBucketLogging", "s3:GetBucketLogging"],
    "Resource": "arn:aws:s3:::source"}]}))
refused(lambda: other.put_bucket_logging(Bucket="source", BucketLoggingStatus={}), "AccessDenied")
assert other.get_bucket_logging(Bucket="source")["LoggingEnabled"] == partitioned
s3.delete_bucket_policy(Bucket="source")
refused(lambda: other.get_bucket_logging(Bucket="source"), "AccessDenied")
# Off again; and gone with its bucket.
s3.put_bucket_logging(Bucket="source", BucketLoggingStatus={})
assert "LoggingEnabled" not in s3.get_bucket_logging(Bucket="source")
s3.put_bucket_logging(Bucket="source", BucketLoggingStatus={"LoggingEnabled": enabled})
s3.delete_bucket(Bucket="source")
s3.create_bucket(Bucket="source")
assert "LoggingEnabled" not in s3.get_bucket_logging(Bucket="source")
print("ok")
"#;

#[test]
fn a_trail_delivers_its_accounts_events_as_log_files() {
    // A Python with boto3 installed (pip install boto3).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_BOTO3_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_BOTO3_PYTHON names no Python with boto3");
        return;
    };
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "trails");
    let (_, other_key, other_secret) = account_with_key(&server, "neighbour");
    let (user_key, user_secret) = user_with_key(&server, &account, "nobody", None);
    let script = server.dir.join("trails.py");
    std::fs::write(&script, TRAILS_SCRIPT).expect("script");
    let output = Command::new(&python)
        .arg(&script)
        .args([
            &server.s3_port.to_string(),
            &account,
            &key,
            &secret,
            &other_key,
            &other_secret,
            &user_key,
            &user_secret,
        ])
        .output()
        .expect("run python");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("ok"),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

const TRAILS_SCRIPT: &str = r#"
import sys, time, json, gzip, re
import boto3, botocore.config, botocore.exceptions
port, account, key, secret, other_key, other_secret, user_key, user_secret = sys.argv[1:9]
def client(service, access, secret_key):
    return boto3.client(service, endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1",
        aws_access_key_id=access, aws_secret_access_key=secret_key,
        config=botocore.config.Config(retries={"max_attempts": 1}))
s3 = client("s3", key, secret)
ct = client("cloudtrail", key, secret)
other_s3 = client("s3", other_key, other_secret)
other_ct = client("cloudtrail", other_key, other_secret)
def refused(call, code):
    try:
        call()
        raise SystemExit(f"not refused: {code}")
    except botocore.exceptions.ClientError as error:
        assert error.response["Error"]["Code"] == code, error.response
def wait_for(check):
    deadline = time.time() + 30
    while not check():
        assert time.time() < deadline, "never"
        time.sleep(0.5)
def files(prefix):
    return [o["Key"] for o in s3.list_objects_v2(Bucket="trail-logs", Prefix=prefix).get("Contents", [])]
def records(prefix):
    out = []
    for name in files(prefix):
        body = s3.get_object(Bucket="trail-logs", Key=name)["Body"].read()
        out += json.loads(gzip.decompress(body))["Records"]
    return out
s3.create_bucket(Bucket="trail-logs")
other_s3.create_bucket(Bucket="foreign")
arn = f"arn:aws:cloudtrail:us-east-1:{account}:trail/main"
made = ct.create_trail(Name="main", S3BucketName="trail-logs", S3KeyPrefix="ct")
assert made["TrailARN"] == arn and made["S3KeyPrefix"] == "ct" and made["Name"] == "main", made
assert made["IncludeGlobalServiceEvents"] is True and made["IsMultiRegionTrail"] is False, made
# Refused as CloudTrail refuses them.
refused(lambda: ct.create_trail(Name="main", S3BucketName="trail-logs"), "TrailAlreadyExistsException")
refused(lambda: ct.create_trail(Name="my--trail", S3BucketName="trail-logs"), "InvalidTrailNameException")
refused(lambda: ct.create_trail(Name="nobucket", S3BucketName="missing"), "S3BucketDoesNotExistException")
refused(lambda: ct.create_trail(Name="theirs", S3BucketName="foreign"), "InsufficientS3BucketPolicyException")
refused(lambda: ct.create_trail(Name="keyed", S3BucketName="trail-logs", KmsKeyId="alias/x"), "UnsupportedOperationException")
refused(lambda: ct.create_trail(Name="long", S3BucketName="trail-logs", S3KeyPrefix="p" * 201), "InvalidS3PrefixException")
for n in range(4):
    ct.create_trail(Name=f"extra{n}", S3BucketName="trail-logs")
refused(lambda: ct.create_trail(Name="sixth", S3BucketName="trail-logs"), "MaximumNumberOfTrailsExceededException")
listed = ct.list_trails()["Trails"]
assert sorted(t["Name"] for t in listed) == ["extra0", "extra1", "extra2", "extra3", "main"], listed
for n in range(4):
    ct.delete_trail(Name=f"extra{n}")
described = ct.describe_trails(trailNameList=["main", arn])["trailList"]
assert len(described) == 1 and described[0]["TrailARN"] == arn and described[0]["HomeRegion"] == "us-east-1", described
assert ct.get_trail(Name=arn)["Trail"]["S3BucketName"] == "trail-logs"
# Not logging until started.
assert ct.get_trail_status(Name="main")["IsLogging"] is False
s3.create_bucket(Bucket="before")
time.sleep(2.5)
assert files("ct/") == []
ct.start_logging(Name="main")
status = ct.get_trail_status(Name="main")
assert status["IsLogging"] is True and "StartLoggingTime" in status, status
s3.create_bucket(Bucket="watched")
s3.put_bucket_versioning(Bucket="watched", VersioningConfiguration={"Status": "Enabled"})
wait_for(lambda: "PutBucketVersioning" in [r["eventName"] for r in records("ct/")])
for name in files("ct/"):
    assert re.fullmatch(rf"ct/AWSLogs/{account}/CloudTrail/us-east-1/\d{{4}}/\d{{2}}/\d{{2}}/{account}_CloudTrail_us-east-1_\d{{8}}T\d{{4}}Z_[A-Za-z0-9]{{16}}\.json\.gz", name), name
got = records("ct/")
assert all(r["recipientAccountId"] == account and r["eventSource"] == "s3.amazonaws.com" for r in got), got
assert "before" not in [r.get("requestParameters", {}).get("bucketName") for r in got if r["eventName"] == "CreateBucket"]
assert "LatestDeliveryTime" in ct.get_trail_status(Name="main")
# Another account's events and trails are its own.
other_s3.get_bucket_versioning(Bucket="foreign")
refused(lambda: other_ct.get_trail(Name="main"), "TrailNotFoundException")
refused(lambda: other_ct.get_trail(Name=arn), "TrailNotFoundException")
refused(lambda: ct.get_trail(Name="arn:aws:s3:::trail-logs"), "CloudTrailARNInvalidException")
assert other_ct.describe_trails()["trailList"] == []
# A new prefix takes effect; a bucket gone is told in the status.
ct.update_trail(Name="main", S3KeyPrefix="ct2")
s3.get_bucket_versioning(Bucket="watched")
wait_for(lambda: files("ct2/"))
s3.create_bucket(Bucket="doomed")
ct.update_trail(Name="main", S3BucketName="doomed")
s3.delete_bucket(Bucket="doomed")
s3.get_bucket_versioning(Bucket="watched")
wait_for(lambda: ct.get_trail_status(Name="main").get("LatestDeliveryError") == "NoSuchBucket")
# Made again by another account: not written into.
other_s3.create_bucket(Bucket="doomed")
s3.get_bucket_versioning(Bucket="watched")
wait_for(lambda: ct.get_trail_status(Name="main").get("LatestDeliveryError") == "AccessDenied")
assert other_s3.list_objects_v2(Bucket="doomed").get("Contents", []) == []
ct.update_trail(Name="main", S3BucketName="trail-logs", S3KeyPrefix="ct3")
# Stopped: nothing more.
ct.stop_logging(Name="main")
assert ct.get_trail_status(Name="main")["IsLogging"] is False
s3.get_bucket_versioning(Bucket="watched")
time.sleep(3)
assert files("ct3/") == []
# A user needs cloudtrail permissions; a deleted trail is gone.
refused(lambda: client("cloudtrail", user_key, user_secret).create_trail(Name="mine", S3BucketName="trail-logs"), "AccessDenied")
ct.delete_trail(Name="main")
refused(lambda: ct.get_trail_status(Name="main"), "TrailNotFoundException")
print("ok")
"#;

#[test]
fn lookup_events_reads_an_accounts_management_events() {
    // A Python with boto3 installed (pip install boto3).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_BOTO3_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_BOTO3_PYTHON names no Python with boto3");
        return;
    };
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "trailed");
    let (_, other_key, other_secret) = account_with_key(&server, "elsewhere");
    let (user_key, user_secret) = user_with_key(
        &server,
        &account,
        "reader",
        Some(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListAllMyBuckets","s3:GetBucketVersioning"],"Resource":"*"}]}"#,
        ),
    );
    let script = server.dir.join("lookup_events.py");
    std::fs::write(&script, LOOKUP_EVENTS_SCRIPT).expect("script");
    let output = Command::new(&python)
        .arg(&script)
        .args([
            &server.s3_port.to_string(),
            &account,
            &key,
            &secret,
            &other_key,
            &other_secret,
            &user_key,
            &user_secret,
        ])
        .output()
        .expect("run python");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("ok"),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

const LOOKUP_EVENTS_SCRIPT: &str = r#"
import sys, time, json, datetime
import boto3, botocore.config, botocore.exceptions
port, account, key, secret, other_key, other_secret, user_key, user_secret = sys.argv[1:9]
def client(service, access, secret_key, validate=True):
    return boto3.client(service, endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1",
        aws_access_key_id=access, aws_secret_access_key=secret_key,
        config=botocore.config.Config(retries={"max_attempts": 1}, parameter_validation=validate))
s3 = client("s3", key, secret)
trail = client("cloudtrail", key, secret)
loose = client("cloudtrail", key, secret, validate=False)
def refused(call, code):
    try:
        call()
        raise SystemExit(f"not refused: {code}")
    except botocore.exceptions.ClientError as error:
        assert error.response["Error"]["Code"] == code, error.response
before = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=2)
s3.create_bucket(Bucket="alpha")
s3.create_bucket(Bucket="beta")
s3.put_bucket_versioning(Bucket="alpha", VersioningConfiguration={"Status": "Enabled"})
s3.get_bucket_versioning(Bucket="beta")
user = client("s3", user_key, user_secret)
user.list_buckets()
try:
    user.get_bucket_policy(Bucket="alpha")
except botocore.exceptions.ClientError:
    pass
def everything(**args):
    events, token = [], None
    while True:
        page = trail.lookup_events(**args, **({"NextToken": token} if token else {}))
        events += page["Events"]
        token = page.get("NextToken")
        if not token:
            return events
deadline = time.time() + 30
while len(everything()) < 6:
    assert time.time() < deadline, everything()
    time.sleep(0.5)
time.sleep(1.5)
events = everything()
names = [event["EventName"] for event in events]
assert names == ["GetBucketPolicy", "ListBuckets", "GetBucketVersioning", "PutBucketVersioning",
    "CreateBucket", "CreateBucket"], names
times = [event["EventTime"] for event in events]
assert times == sorted(times, reverse=True), times
first = events[0]
record = json.loads(first["CloudTrailEvent"])
assert record["eventID"] == first["EventId"] and record["errorCode"] == "AccessDenied", record
assert first["Username"] == "reader" and first["AccessKeyId"] == user_key, first
assert first["ReadOnly"] == "true" and first["EventSource"] == "s3.amazonaws.com", first
assert first["Resources"] == [{"ResourceType": "AWS::S3::Bucket", "ResourceName": "alpha"}], first
def named(**lookup):
    return [event["EventName"] for event in everything(LookupAttributes=[lookup])]
assert named(AttributeKey="EventName", AttributeValue="CreateBucket") == ["CreateBucket"] * 2
assert named(AttributeKey="Username", AttributeValue="root") == names[2:]
assert named(AttributeKey="Username", AttributeValue="reader") == names[:2]
assert named(AttributeKey="ReadOnly", AttributeValue="false") == ["PutBucketVersioning", "CreateBucket", "CreateBucket"]
assert named(AttributeKey="ResourceName", AttributeValue="beta") == ["GetBucketVersioning", "CreateBucket"]
assert named(AttributeKey="ResourceType", AttributeValue="AWS::S3::Bucket") == [n for n in names if n != "ListBuckets"]
assert named(AttributeKey="AccessKeyId", AttributeValue=key) == names[2:]
assert named(AttributeKey="EventId", AttributeValue=first["EventId"]) == ["GetBucketPolicy"]
assert named(AttributeKey="EventSource", AttributeValue="s3.amazonaws.com") == names
# A time window (StartTime and EndTime both inclusive).
middle = events[3]["EventTime"]
assert [e["EventName"] for e in everything(EndTime=middle)][0] == "PutBucketVersioning"
assert everything(StartTime=before + datetime.timedelta(days=1)) == []
assert len(everything(StartTime=before)) == 6
# StartTime is inclusive; an EndTime in whole seconds takes in its second.
assert [e["EventId"] for e in everything(StartTime=events[2]["EventTime"])][-1] == events[2]["EventId"]
second = events[3]["EventTime"].replace(microsecond=0)
assert events[3]["EventId"] in [e["EventId"] for e in loose.lookup_events(EndTime=int(second.timestamp()))["Events"]]
# A page filled by the last event has no token.
full = trail.lookup_events(MaxResults=6)
assert len(full["Events"]) == 6 and "NextToken" not in full, full.get("NextToken")
# Pages of MaxResults, with a token for the same parameters only.
page = trail.lookup_events(MaxResults=4)
assert len(page["Events"]) == 4 and "NextToken" in page
rest = trail.lookup_events(MaxResults=4, NextToken=page["NextToken"])
assert [e["EventName"] for e in page["Events"] + rest["Events"]] == names and "NextToken" not in rest
refused(lambda: trail.lookup_events(MaxResults=3, NextToken=page["NextToken"]), "InvalidNextTokenException")
refused(lambda: trail.lookup_events(NextToken="garbage"), "InvalidNextTokenException")
# Errors as CloudTrail answers them.
refused(lambda: trail.lookup_events(LookupAttributes=[
    {"AttributeKey": "EventName", "AttributeValue": "a"}, {"AttributeKey": "Username", "AttributeValue": "b"}]),
    "InvalidLookupAttributesException")
refused(lambda: loose.lookup_events(LookupAttributes=[{"AttributeKey": "Color", "AttributeValue": "a"}]),
    "InvalidLookupAttributesException")
refused(lambda: loose.lookup_events(MaxResults=51), "InvalidMaxResultsException")
refused(lambda: loose.lookup_events(MaxResults=0), "InvalidMaxResultsException")
refused(lambda: trail.lookup_events(StartTime=before + datetime.timedelta(hours=1), EndTime=before),
    "InvalidTimeRangeException")
refused(lambda: loose.lookup_events(EventCategory="data"), "InvalidEventCategoryException")
assert trail.lookup_events(EventCategory="insight")["Events"] == []
# Another account reads none of them; a user needs cloudtrail:LookupEvents.
other = client("cloudtrail", other_key, other_secret)
assert other.lookup_events()["Events"] == []
refused(lambda: other.lookup_events(MaxResults=4, NextToken=page["NextToken"]), "InvalidNextTokenException")
refused(lambda: client("cloudtrail", user_key, user_secret).lookup_events(), "AccessDenied")
print("ok")
"#;

#[test]
fn management_events_are_kept_as_api_activity_across_a_crash() {
    let mut server = start_server();
    let port = server.s3_port;
    let (account, key, secret) = account_with_key(&server, "audited");
    let (other_account, other_key, other_secret) = account_with_key(&server, "visitor");
    let (user_key, user_secret) = user_with_key(
        &server,
        &account,
        "auditor",
        Some(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#,
        ),
    );
    let call = |method: &str, path: &str, query: &str, key: &str, secret: Option<&str>| {
        send(port, method, path, query, b"", key, secret)
    };
    assert_eq!(call("PUT", "/made", "", &key, Some(&secret)).0, 200);
    assert_eq!(
        call("GET", "/made", "versioning", &key, Some(&secret)).0,
        200
    );
    let (status, reply) = call("GET", "/made", "policy", &key, Some(&secret));
    assert_eq!(status, 404, "{reply}");
    assert_eq!(call("GET", "/", "", &user_key, Some(&user_secret)).0, 200);
    // Data events are not management events.
    assert_eq!(
        send(port, "PUT", "/made/k", "", b"x", &key, Some(&secret)).0,
        200
    );
    assert_eq!(
        call("GET", "/made", "list-type=2", &key, Some(&secret)).0,
        200
    );
    assert_eq!(call("HEAD", "/made", "", &key, Some(&secret)).0, 200);
    // An anonymous call is no account's; another account's is its own.
    assert_eq!(call("GET", "/made", "acl", "", None).0, 403);
    assert_eq!(
        call("GET", "/made", "location", &other_key, Some(&other_secret)).0,
        403
    );
    // A call naming two sub-resources is refused (and is no event).
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{account}:root"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::made/*"}}]}}"#
    );
    for query in ["location&policy", "policy&versioning"] {
        let (status, reply) = send(
            port,
            "PUT",
            "/made",
            query,
            policy.as_bytes(),
            &key,
            Some(&secret),
        );
        assert_eq!(status, 400, "{query}: {reply}");
    }
    // A user agent too long for the record is cut, and the event kept.
    let quotes = "\"".repeat(1000);
    let (_, (status, _)) = send_with(
        port,
        "GET",
        "/made",
        "versioning",
        &[("user-agent", &quotes)],
        b"",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 200);
    let activity = |server: &Server| {
        sql_rows(
            server.native_port,
            "SELECT account_id, event_source, event_name, ISNULL(user_name, '-'), \
             ISNULL(access_key_id, '-'), source_ip, read_only, ISNULL(error_code, '-'), \
             ISNULL(resource_name, '-'), event_id, cloudtrail_event \
             FROM sys.dm_s3_api_activity ORDER BY event_time, event_name",
        )
    };
    let deadline = Instant::now() + DEADLINE;
    while activity(&server).len() < 6 {
        assert!(Instant::now() < deadline, "{:?}", activity(&server));
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_millis(1500));
    let rows = activity(&server);
    let summary: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{} {} {} {} {} {}",
                if row[0] == account {
                    "owner"
                } else if row[0] == other_account {
                    "other"
                } else {
                    "?"
                },
                row[2],
                row[3],
                row[6],
                row[7],
                row[8]
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            "owner CreateBucket - 0 - made",
            "owner GetBucketVersioning - 1 - made",
            "owner GetBucketPolicy - 1 NoSuchBucketPolicy made",
            "owner ListBuckets auditor 1 - -",
            "other GetBucketLocation - 1 AccessDenied made",
            "owner GetBucketVersioning - 1 - made",
        ],
        "{rows:?}"
    );
    let cut: serde_json::Value = serde_json::from_str(&rows[5][10]).unwrap();
    assert!(cut["requestParameters"].is_null(), "{cut}");
    assert_eq!(cut["userAgent"].as_str().unwrap().len(), 256, "{cut}");
    for row in &rows {
        assert_eq!(row[1], "s3.amazonaws.com");
        assert_eq!(row[5], "127.0.0.1");
        assert_eq!(row[9].len(), 36, "{row:?}");
        let record: serde_json::Value = serde_json::from_str(&row[10]).unwrap();
        assert_eq!(record["eventVersion"], "1.11");
        assert_eq!(record["eventCategory"], "Management");
        assert_eq!(record["eventType"], "AwsApiCall");
        assert_eq!(record["managementEvent"], true);
        assert_eq!(record["recipientAccountId"], row[0].as_str());
        assert_eq!(record["eventID"], row[9].as_str());
        assert_eq!(record["awsRegion"], "us-east-1");
    }
    let record = |name: &str| -> serde_json::Value {
        let row = rows.iter().find(|row| row[2] == name).unwrap();
        serde_json::from_str(&row[10]).unwrap()
    };
    let versioning = record("GetBucketVersioning");
    assert_eq!(versioning["userIdentity"]["type"], "Root");
    assert_eq!(versioning["userIdentity"]["accountId"], account.as_str());
    assert_eq!(versioning["userIdentity"]["accessKeyId"], key.as_str());
    assert_eq!(versioning["requestParameters"]["bucketName"], "made");
    assert_eq!(versioning["requestParameters"]["versioning"], "");
    assert_eq!(versioning["resources"][0]["ARN"], "arn:aws:s3:::made");
    assert_eq!(versioning["resources"][0]["type"], "AWS::S3::Bucket");
    assert!(versioning["eventTime"].as_str().unwrap().ends_with('Z'));
    let listing = record("ListBuckets");
    assert_eq!(listing["userIdentity"]["type"], "IAMUser");
    assert_eq!(listing["userIdentity"]["userName"], "auditor");
    assert_eq!(
        listing["userIdentity"]["arn"],
        format!("arn:aws:iam::{account}:user/auditor")
    );
    assert_eq!(listing["userIdentity"]["accessKeyId"], user_key.as_str());
    let refused = record("GetBucketLocation");
    assert_eq!(refused["userIdentity"]["accountId"], other_account.as_str());
    assert_eq!(refused["errorCode"], "AccessDenied");
    assert!(refused["errorMessage"].is_string());
    // Kept across a crash.
    crash_and_restart(&mut server);
    assert_eq!(activity(&server).len(), 6);
    assert_eq!(
        send(
            server.s3_port,
            "DELETE",
            "/made/k",
            "",
            b"",
            &key,
            Some(&secret)
        )
        .0,
        204
    );
    assert_eq!(
        send(
            server.s3_port,
            "DELETE",
            "/made",
            "",
            b"",
            &key,
            Some(&secret)
        )
        .0,
        204
    );
    let deadline = Instant::now() + DEADLINE;
    while activity(&server).len() < 7 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(activity(&server)[6][2], "DeleteBucket");
    // A refused call naming no bucket there can be is kept too (its name
    // cut short).
    let long = format!("/{}", "a".repeat(2000));
    assert_eq!(
        send(
            server.s3_port,
            "GET",
            &long,
            "versioning",
            b"",
            &key,
            Some(&secret)
        )
        .0,
        404
    );
    let deadline = Instant::now() + DEADLINE;
    while activity(&server).len() < 8 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(100));
    }
    let last = activity(&server).pop().unwrap();
    assert_eq!(last[2], "GetBucketVersioning");
    assert_eq!(last[8], "a".repeat(255));
    // Its control characters too.
    let controls = format!("/{}", "%01".repeat(300));
    let (status, _) = send(
        server.s3_port,
        "GET",
        &controls,
        "policy",
        b"",
        &key,
        Some(&secret),
    );
    assert!(status >= 400, "{status}");
    let deadline = Instant::now() + DEADLINE;
    while activity(&server).len() < 9 {
        assert!(Instant::now() < deadline, "{:?}", activity(&server));
        std::thread::sleep(Duration::from_millis(100));
    }
    let last = activity(&server)
        .into_iter()
        .find(|row| row[2] == "GetBucketPolicy" && row[8] != "made")
        .unwrap();
    assert_eq!(last[8], "?".repeat(255));
}

#[test]
fn access_logs_are_written_into_the_target_bucket() {
    // A Python with boto3 installed (pip install boto3).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_BOTO3_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_BOTO3_PYTHON names no Python with boto3");
        return;
    };
    let server = start_server();
    let (account, key, secret) = account_with_key(&server, "logged");
    let (_, other_key, other_secret) = account_with_key(&server, "bystander");
    let script = server.dir.join("access_logs.py");
    std::fs::write(&script, ACCESS_LOG_SCRIPT).expect("script");
    let output = Command::new(&python)
        .arg(&script)
        .args([
            &server.s3_port.to_string(),
            &account,
            &key,
            &secret,
            &other_key,
            &other_secret,
        ])
        .output()
        .expect("run python");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("ok"),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

const ACCESS_LOG_SCRIPT: &str = r#"
import sys, time, re, http.client, urllib.parse, urllib.request, urllib.error
import boto3, botocore.config, botocore.exceptions
port, account, key, secret, other_key, other_secret = sys.argv[1:7]
def client(access, secret_key, **config):
    return boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1",
        aws_access_key_id=access, aws_secret_access_key=secret_key,
        config=botocore.config.Config(retries={"max_attempts": 1}, **config))
s3 = client(key, secret)
other = client(other_key, other_secret)
LINE = re.compile(r'(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) '
    r'(\S+) (\S+) "([^"]*)" "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+)$')
NAMES = ["owner", "bucket", "time", "ip", "requester", "request_id", "operation", "key", "uri", "status",
    "error", "sent", "size", "total", "turnaround", "referer", "agent", "version", "host_id", "signature",
    "cipher", "auth", "host", "tls", "access_point", "acl", "region"]
def objects(client, bucket, prefix=""):
    return [o["Key"] for o in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])]
def lines(bucket, prefix):
    out = []
    for name in objects(s3, bucket, prefix):
        text = s3.get_object(Bucket=bucket, Key=name)["Body"].read().decode()
        for line in text.splitlines():
            match = LINE.match(line)
            assert match, line
            out.append(dict(zip(NAMES, match.groups())))
    return out
def wait_for(check):
    deadline = time.time() + 30
    while not check():
        assert time.time() < deadline, "never"
        time.sleep(0.5)

for bucket in ["src", "logs", "src2", "src3", "tmp", "src4", "lockme"]:
    s3.create_bucket(Bucket=bucket)
s3.put_bucket_logging(Bucket="src", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "logs", "TargetPrefix": "src/"}})
put = s3.put_object(Bucket="src", Key="a b.txt", Body=b"0123456789")
assert s3.get_object(Bucket="src", Key="a b.txt")["Body"].read() == b"0123456789"
s3.head_object(Bucket="src", Key="a b.txt")
try:
    s3.get_object(Bucket="src", Key="missing")
    raise SystemExit("found")
except botocore.exceptions.ClientError:
    pass
try:
    urllib.request.urlopen(f"http://127.0.0.1:{port}/src/a%20b.txt?x-user=anon")
    raise SystemExit("anonymous read")
except urllib.error.HTTPError as error:
    assert error.code == 403
signer = client(key, secret, signature_version="s3v4")
url = signer.generate_presigned_url("get_object", Params={"Bucket": "src", "Key": "a b.txt"})
assert urllib.request.urlopen(url).read() == b"0123456789"
s3.list_objects_v2(Bucket="src")
s3.get_bucket_versioning(Bucket="src")
# What a client sends cannot add lines or fields.
for target in ['/src/a"b', "/src?versionId=v1%0AINJECTED"]:
    hostile = http.client.HTTPConnection("127.0.0.1", int(port))
    hostile.putrequest("GET", target, skip_accept_encoding=True)
    hostile.putheader("User-Agent", 'UA" 999 "x')
    hostile.putheader("Referer", 'r" INJECTED')
    hostile.endheaders()
    status = hostile.getresponse().status
    assert status == 403, (target, status)
wait_for(lambda: len(lines("logs", "src/")) >= 11)
got = lines("logs", "src/")
for name in objects(s3, "logs", "src/"):
    assert re.fullmatch(r"src/\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-[0-9A-F]{16}", name), name
by_op = {}
for line in got:
    assert line["owner"] == account and line["bucket"] == "src", line
    assert re.fullmatch(r"\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2} \+0000", line["time"]), line
    assert line["ip"] == "127.0.0.1" and line["host"] == f"127.0.0.1:{port}", line
    assert line["cipher"] == line["tls"] == line["access_point"] == line["acl"] == line["region"] == "-", line
    by_op.setdefault((line["operation"], line["status"]), []).append(line)
(written,) = by_op[("REST.PUT.OBJECT", "200")]
assert written["request_id"] == put["ResponseMetadata"]["RequestId"], written
assert written["key"] == "a%20b.txt" and written["size"] == "10" and written["sent"] == "-", written
assert written["requester"] == account and written["auth"] == "AuthHeader" and written["signature"] == "SigV4", written
assert written["uri"].startswith("PUT /src/a%20b.txt") and written["uri"].endswith(" HTTP/1.1"), written
assert written["agent"].startswith("Boto3/") and written["referer"] == "-", written
read = [line for line in by_op[("REST.GET.OBJECT", "200")]]
assert {line["auth"] for line in read} == {"AuthHeader", "QueryString"}, read
assert all(line["sent"] == "10" and line["size"] == "10" for line in read), read
assert by_op[("REST.HEAD.OBJECT", "200")][0]["size"] == "10"
(missing,) = by_op[("REST.GET.OBJECT", "404")]
assert missing["error"] == "NoSuchKey" and missing["key"] == "missing", missing
(anonymous, quoted) = sorted(by_op[("REST.GET.OBJECT", "403")], key=lambda line: line["key"])
assert quoted["key"] == "a%22b" and quoted["uri"] == "GET /src/a%22b HTTP/1.1", quoted
(versioned,) = by_op[("REST.GET.BUCKET", "403")]
assert versioned["version"] == "v1%0AINJECTED", versioned
for line in [quoted, versioned]:
    assert line["agent"] == "UA%22 999 %22x" and line["referer"] == "r%22 INJECTED", line
assert anonymous["error"] == "AccessDenied" and anonymous["requester"] == "-" and anonymous["auth"] == "-", anonymous
assert anonymous["signature"] == "-" and anonymous["uri"].endswith("?x-user=anon HTTP/1.1"), anonymous
assert ("REST.GET.BUCKET", "200") in by_op and ("REST.GET.VERSIONING", "200") in by_op, sorted(by_op)
# Partitioned keys, by the first request's day; a bucket requested while
# its logging was off is logged once it is on.
s3.list_objects_v2(Bucket="src2")
s3.put_bucket_logging(Bucket="src2", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "logs", "TargetPrefix": "p/",
    "TargetObjectKeyFormat": {"PartitionedPrefix": {"PartitionDateSource": "EventTime"}}}})
s3.list_objects_v2(Bucket="src2")
wait_for(lambda: objects(s3, "logs", "p/"))
for name in objects(s3, "logs", "p/"):
    assert re.fullmatch(rf"p/{account}/us-east-1/src2/\d{{4}}/\d{{2}}/\d{{2}}/\d{{4}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}-\d{{2}}-[0-9A-F]{{16}}", name), name
# A target deleted and made again by another account, or given Object
# Lock, gets no logs.
s3.put_bucket_logging(Bucket="src3", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "tmp", "TargetPrefix": ""}})
s3.put_bucket_logging(Bucket="src4", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "lockme", "TargetPrefix": ""}})
s3.delete_bucket(Bucket="tmp")
other.create_bucket(Bucket="tmp")
s3.put_bucket_versioning(Bucket="lockme", VersioningConfiguration={"Status": "Enabled"})
s3.put_object_lock_configuration(Bucket="lockme", ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"})
s3.list_objects_v2(Bucket="src3")
s3.list_objects_v2(Bucket="src4")
# A request to one owner's bucket, still running when another account
# makes the name again, is in neither's logs.
s3.create_bucket(Bucket="src5")
other.create_bucket(Bucket="blogs")
s3.put_bucket_logging(Bucket="src5", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "logs", "TargetPrefix": "five/"}})
url = urllib.parse.urlsplit(signer.generate_presigned_url("put_object", Params={"Bucket": "src5", "Key": "slow"}))
slow = http.client.HTTPConnection("127.0.0.1", int(port))
slow.putrequest("PUT", f"{url.path}?{url.query}", skip_accept_encoding=True)
slow.putheader("Content-Length", "10")
slow.endheaders()
slow.send(b"01234")
time.sleep(1)
s3.delete_bucket(Bucket="src5")
other.create_bucket(Bucket="src5")
other.put_bucket_logging(Bucket="src5", BucketLoggingStatus={"LoggingEnabled": {"TargetBucket": "blogs", "TargetPrefix": ""}})
slow.send(b"56789")
assert slow.getresponse().status >= 400
# Logging off: nothing more.
s3.put_bucket_logging(Bucket="src", BucketLoggingStatus={})
s3.put_object(Bucket="src", Key="after-off", Body=b"x")
time.sleep(3)
assert objects(other, "tmp") == [] and objects(s3, "lockme") == []
for name in objects(other, "blogs"):
    text = other.get_object(Bucket="blogs", Key=name)["Body"].read().decode()
    assert " slow " not in text, text
assert not [line for line in lines("logs", "src/") if line["key"] == "after-off"]
print("ok")
"#;

#[test]
fn cloudwatch_reads_the_request_and_storage_metrics_of_a_bucket() {
    // A Python with boto3 installed (pip install boto3).
    let Some(python) = std::env::var_os("TRUTHDB_TEST_BOTO3_PYTHON").map(PathBuf::from) else {
        eprintln!("skipped: TRUTHDB_TEST_BOTO3_PYTHON names no Python with boto3");
        return;
    };
    let mut server = start_server();
    let port = server.s3_port;
    let (_, key, secret) = account_with_key(&server, "watcher");
    let (_, other_key, other_secret) = account_with_key(&server, "stranger");
    let run = |server: &Server, phase: &str| {
        let script = server.dir.join("cloudwatch.py");
        std::fs::write(&script, CLOUDWATCH_SCRIPT).expect("script");
        let output = Command::new(&python)
            .arg(&script)
            .args([
                phase,
                &port.to_string(),
                &key,
                &secret,
                &other_key,
                &other_secret,
            ])
            .output()
            .expect("run python");
        assert!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("ok"),
            "{phase}\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    };
    run(&server, "requests");
    // Flushed points outlive a crash; the storage is sampled on the way up.
    crash_and_restart(&mut server);
    run(&server, "after-restart");
    crash_and_restart(&mut server);
    run(&server, "after-restart");
}

const CLOUDWATCH_SCRIPT: &str = r#"
import sys, time, datetime, http.client, urllib.parse
import boto3, botocore.config, botocore.exceptions
phase, port, key, secret, other_key, other_secret = sys.argv[1:7]
def client(service, access, secret_key):
    return boto3.client(service, endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1",
        aws_access_key_id=access, aws_secret_access_key=secret_key,
        config=botocore.config.Config(retries={"max_attempts": 1}))
s3 = client("s3", key, secret)
cw = client("cloudwatch", key, secret)
now = datetime.datetime.now(datetime.timezone.utc)
start, end = now - datetime.timedelta(minutes=30), now + datetime.timedelta(minutes=5)
def dims(filter_id):
    return [{"Name": "BucketName", "Value": "watched"}, {"Name": "FilterId", "Value": filter_id}]
def total(name, filter_id, stat="Sum"):
    points = cw.get_metric_statistics(Namespace="AWS/S3", MetricName=name, Dimensions=dims(filter_id),
        StartTime=start, EndTime=end, Period=60, Statistics=[stat])["Datapoints"]
    return sum(point[stat] for point in points)
def wait_for(check):
    deadline = time.time() + 30
    while True:
        try:
            if check():
                return
        except AssertionError:
            pass
        if time.time() > deadline:
            assert check(), "never"
        time.sleep(0.5)

if phase == "requests":
    s3.create_bucket(Bucket="watched")
    s3.put_bucket_metrics_configuration(Bucket="watched", Id="EntireBucket",
        MetricsConfiguration={"Id": "EntireBucket"})
    s3.put_bucket_metrics_configuration(Bucket="watched", Id="Docs",
        MetricsConfiguration={"Id": "Docs", "Filter": {"Prefix": "docs/"}})
    s3.put_object(Bucket="watched", Key="docs/a", Body=b"0123456789")
    assert s3.get_object(Bucket="watched", Key="docs/a")["Body"].read() == b"0123456789"
    s3.head_object(Bucket="watched", Key="docs/a")
    try:
        s3.get_object(Bucket="watched", Key="img/missing")
        raise SystemExit("a missing key was found")
    except botocore.exceptions.ClientError:
        pass
    # The prefix filter counts the three requests under docs/.
    wait_for(lambda: total("AllRequests", "Docs") == 3 and total("TotalRequestLatency", "Docs", "SampleCount") == 3)
    for name, expected in [("GetRequests", 1), ("PutRequests", 1), ("HeadRequests", 1),
                           ("BytesUploaded", 10), ("BytesDownloaded", 10), ("4xxErrors", 0)]:
        assert total(name, "Docs") == expected, (name, total(name, "Docs"))
    assert total("AllRequests", "EntireBucket") >= 5
    assert total("4xxErrors", "EntireBucket") >= 1
    metrics = cw.list_metrics(Namespace="AWS/S3")["Metrics"]
    found = {(m["MetricName"], tuple((d["Name"], d["Value"]) for d in m["Dimensions"])) for m in metrics}
    assert ("AllRequests", (("BucketName", "watched"), ("FilterId", "Docs"))) in found, found
    assert ("FirstByteLatency", (("BucketName", "watched"), ("FilterId", "EntireBucket"))) in found, found
    data = cw.get_metric_data(StartTime=start, EndTime=end, MetricDataQueries=[
        {"Id": "all", "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "AllRequests",
            "Dimensions": dims("Docs")}, "Period": 300, "Stat": "Sum"}},
        {"Id": "slow", "Label": "p99 first byte", "MetricStat": {"Metric": {"Namespace": "AWS/S3",
            "MetricName": "FirstByteLatency", "Dimensions": dims("Docs")}, "Period": 300, "Stat": "p99"}},
    ])["MetricDataResults"]
    assert data[0]["Label"] == "AWS/S3 AllRequests", data
    assert sum(data[0]["Values"]) == 3, data
    assert data[1]["Label"] == "p99 first byte" and data[1]["Values"], data
    # Errors as CloudWatch answers them.
    for call, code in [
        (lambda: cw.get_metric_statistics(Namespace="AWS/S3", MetricName="AllRequests", Dimensions=dims("Docs"),
            StartTime=start, EndTime=end, Period=60), "InvalidParameterCombination"),
        (lambda: cw.get_metric_data(StartTime=start, EndTime=end, MetricDataQueries=[
            {"Id": "e1", "Expression": "SUM(METRICS())"}]), "InvalidParameterValue"),
    ]:
        try:
            call()
            raise SystemExit("not refused")
        except botocore.exceptions.ClientError as error:
            assert error.response["Error"]["Code"] == code, error.response
    # Another account sees none of them.
    stranger = client("cloudwatch", other_key, other_secret)
    assert stranger.list_metrics(Namespace="AWS/S3")["Metrics"] == []
    assert stranger.get_metric_statistics(Namespace="AWS/S3", MetricName="AllRequests", Dimensions=dims("Docs"),
        StartTime=start, EndTime=end, Period=60, Statistics=["Sum"])["Datapoints"] == []
    # An empty time range is refused; a token past the end is an empty page.
    for call in [
        lambda: cw.get_metric_statistics(Namespace="AWS/S3", MetricName="AllRequests", Dimensions=dims("Docs"),
            StartTime=end, EndTime=end, Period=60, Statistics=["Sum"]),
        lambda: cw.get_metric_data(StartTime=end, EndTime=start, MetricDataQueries=[
            {"Id": "all", "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "AllRequests",
                "Dimensions": dims("Docs")}, "Period": 60, "Stat": "Sum"}}]),
    ]:
        try:
            call()
            raise SystemExit("an empty range was answered")
        except botocore.exceptions.ClientError as error:
            assert error.response["Error"]["Code"] == "InvalidParameterValue", error.response
    beyond = str(2**64 - 1)
    listed = cw.list_metrics(Namespace="AWS/S3", NextToken=beyond)
    assert listed["Metrics"] == [] and "NextToken" not in listed, listed
    data = cw.get_metric_data(StartTime=start, EndTime=end, NextToken=beyond, MetricDataQueries=[
        {"Id": "all", "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "AllRequests",
            "Dimensions": dims("Docs")}, "Period": 60, "Stat": "Sum"}}])
    assert data["MetricDataResults"][0]["Values"] == [] and "NextToken" not in data, data
    # A bucket deleted and made again by another account: the new owner
    # reads none of the old one's metrics.
    s3.create_bucket(Bucket="handed")
    s3.put_bucket_metrics_configuration(Bucket="handed", Id="All", MetricsConfiguration={"Id": "All"})
    s3.put_object(Bucket="handed", Key="k", Body=b"x")
    for _ in range(3):
        s3.get_object(Bucket="handed", Key="k")["Body"].read()
    handed = [{"Name": "BucketName", "Value": "handed"}, {"Name": "FilterId", "Value": "All"}]
    def gets(reader, period, name="GetRequests"):
        points = reader.get_metric_statistics(Namespace="AWS/S3", MetricName=name, Dimensions=handed,
            StartTime=start, EndTime=end, Period=period, Statistics=["Sum"])["Datapoints"]
        return sum(point["Sum"] for point in points)
    wait_for(lambda: gets(cw, 60) == 3)
    # Its last minute's points go with it; the earlier ones are left out.
    time.sleep(60.5 - time.time() % 60)
    s3.head_object(Bucket="handed", Key="k")
    wait_for(lambda: gets(cw, 60, "HeadRequests") == 1)
    # A download still going when the bucket goes: what it counts at its
    # end is not the next bucket's.
    s3.put_object(Bucket="handed", Key="big", Body=b"z" * (32 << 20))
    late = s3.get_object(Bucket="handed", Key="big")
    s3.delete_object(Bucket="handed", Key="big")
    s3.delete_object(Bucket="handed", Key="k")
    s3.delete_bucket(Bucket="handed")
    time.sleep(1.1)
    other_s3 = client("s3", other_key, other_secret)
    other_s3.create_bucket(Bucket="handed")
    other_s3.put_bucket_metrics_configuration(Bucket="handed", Id="All", MetricsConfiguration={"Id": "All"})
    for period in [60, 3600]:
        assert gets(stranger, period) == 0, period
        assert gets(stranger, period, "HeadRequests") == 0, period
    late["Body"].close()
    time.sleep(2.5)
    assert gets(stranger, 60, "BytesDownloaded") == 0
    # Its own are counted (in the minute the old one's last were).
    other_s3.put_object(Bucket="handed", Key="mine", Body=b"y")
    wait_for(lambda: gets(stranger, 60, "PutRequests") >= 1)
    named = {m["MetricName"] for m in stranger.list_metrics(Namespace="AWS/S3")["Metrics"]
             if {"Name": "BucketName", "Value": "handed"} in m["Dimensions"]}
    assert "PutRequests" in named and not named & {"GetRequests", "HeadRequests", "BytesDownloaded"}, named
    # A request authorized for one owner's bucket, still running when
    # another account makes the name again, counts in neither's series.
    s3.create_bucket(Bucket="swapped")
    signer = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{port}", region_name="us-east-1",
        aws_access_key_id=key, aws_secret_access_key=secret,
        config=botocore.config.Config(signature_version="s3v4"))
    url = urllib.parse.urlsplit(signer.generate_presigned_url("put_object",
        Params={"Bucket": "swapped", "Key": "slow"}, ExpiresIn=300))
    slow = http.client.HTTPConnection("127.0.0.1", int(port))
    slow.putrequest("PUT", f"{url.path}?{url.query}", skip_accept_encoding=True)
    slow.putheader("Content-Length", "10")
    slow.endheaders()
    slow.send(b"01234")
    time.sleep(1)
    s3.delete_bucket(Bucket="swapped")
    other_s3.create_bucket(Bucket="swapped")
    other_s3.put_bucket_metrics_configuration(Bucket="swapped", Id="All", MetricsConfiguration={"Id": "All"})
    slow.send(b"56789")
    status = slow.getresponse().status
    assert status >= 400, status
    time.sleep(2.5)
    swapped = [{"Name": "BucketName", "Value": "swapped"}, {"Name": "FilterId", "Value": "All"}]
    for name in ["4xxErrors", "5xxErrors"]:
        points = stranger.get_metric_statistics(Namespace="AWS/S3", MetricName=name, Dimensions=swapped,
            StartTime=start, EndTime=end, Period=60, Statistics=["Sum"])["Datapoints"]
        assert sum(point["Sum"] for point in points) == 0, (name, points)
    print("ok")
else:
    assert total("AllRequests", "Docs") == 3
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    def stored(storage, name):
        points = cw.get_metric_statistics(Namespace="AWS/S3", MetricName=name,
            Dimensions=[{"Name": "BucketName", "Value": "watched"}, {"Name": "StorageType", "Value": storage}],
            StartTime=day, EndTime=day + datetime.timedelta(days=1), Period=86400, Statistics=["Average"])["Datapoints"]
        return [point["Average"] for point in points]
    wait_for(lambda: stored("StandardStorage", "BucketSizeBytes") == [10.0])
    assert stored("AllStorageTypes", "NumberOfObjects") == [1.0]
    # Sampled once a day, restarts or not (after this start's first flushes).
    time.sleep(2.5)
    samples = cw.get_metric_statistics(Namespace="AWS/S3", MetricName="NumberOfObjects",
        Dimensions=[{"Name": "BucketName", "Value": "watched"}, {"Name": "StorageType", "Value": "AllStorageTypes"}],
        StartTime=day, EndTime=day + datetime.timedelta(days=1), Period=86400, Statistics=["SampleCount"])["Datapoints"]
    assert [point["SampleCount"] for point in samples] == [1.0], samples
    print("ok")
"#;
