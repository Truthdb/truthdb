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
    let amz_date = amz_now();
    let host = format!("127.0.0.1:{port}");
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
            .map(|(name, value)| (name.to_string(), value.to_string())),
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
    let deadline = Instant::now() + DEADLINE;
    let mut stream = loop {
        match std::net::TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => break stream,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(err) => panic!("S3 listener never came up: {err}"),
        }
    };
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

    let (status, body) = get(server.s3_port, "/bucket/key", &key, None);
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
    for (path, query) in [("/lst", "policy"), ("/lst", "acl"), ("/lst/a", "acl")] {
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
    // Copies are not implemented yet: refused, not stored empty.
    let (_, (status, body)) = send_with(
        port,
        "PUT",
        "/mpu/copy",
        "",
        &[("x-amz-copy-source", "/mpu/other")],
        b"",
        &key,
        Some(&secret),
    );
    assert_eq!(status, 501, "{body}");
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
    assert_eq!(call("GET", "/vers/k", "acl", b"").1.0, 501);
}
