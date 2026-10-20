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

/// Two distinct free ports: both are held until both are chosen, so the
/// second cannot be the first one released.
fn free_ports() -> (u16, u16) {
    let bind = || std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let (first, second) = (bind(), bind());
    (
        first.local_addr().expect("addr").port(),
        second.local_addr().expect("addr").port(),
    )
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

/// Starts the server binary on fresh ports and waits until both listeners
/// answer. A port can still be taken by another process between choosing it
/// and the server binding it; the server then exits, and it is started again
/// on new ports.
fn start_server() -> Server {
    for _ in 0..5 {
        let server = spawn_server();
        let deadline = Instant::now() + DEADLINE;
        let mut server = Some(server);
        while Instant::now() < deadline {
            let running = server.as_mut().unwrap();
            if running.child.try_wait().expect("poll the server").is_some() {
                break;
            }
            let up = [running.native_port, running.s3_port]
                .iter()
                .all(|&port| std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
            // Answering on both ports and still running: the listeners are
            // the server's own.
            if up && running.child.try_wait().expect("poll the server").is_none() {
                return server.take().unwrap();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        drop(server);
    }
    panic!("the server never came up");
}

fn spawn_server() -> Server {
    let dir = std::env::temp_dir().join(format!(
        "truthdb-s3-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let (native_port, s3_port) = free_ports();
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
    stream.write_all(&body[..split]).expect("send body");
    stream.flush().expect("flush");
    pause();
    stream.write_all(&body[split..]).expect("send body");
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
        ("GET", "/cov", "acl", b"", &[]),
        ("PUT", "/cov", "acl", b"", &[]),
        ("GET", "/cov", "ownershipControls", b"", &[]),
        ("PUT", "/cov", "ownershipControls", b"", &[]),
        ("GET", "/cov", "uploads", b"", &[]),
        ("PUT", "/cov/o", "", b"x", &[]),
        ("GET", "/cov/o", "", b"", &[]),
        ("HEAD", "/cov/o", "", b"", &[]),
        ("DELETE", "/cov/o", "", b"", &[]),
        ("GET", "/cov/o", "tagging", b"", &[]),
        ("PUT", "/cov/o", "tagging", tagging, &[]),
        ("DELETE", "/cov/o", "tagging", b"", &[]),
        ("GET", "/cov/o", "acl", b"", &[]),
        ("PUT", "/cov/o", "acl", b"", &[]),
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
    let credential = sigv4::Credential {
        access_key: key.to_string(),
        date: amz_date[..8].to_string(),
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
    };
    let encode = |text: &str| {
        percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
    };
    let mut query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={amz_date}&X-Amz-Expires={expires}&X-Amz-SignedHeaders=host",
        encode(&format!("{key}/{}", credential.scope()))
    );
    if !extra.is_empty() {
        query = format!("{extra}&{query}");
    }
    let headers = vec![("host".to_string(), format!("127.0.0.1:{port}"))];
    let canonical = sigv4::canonical_request(
        method,
        path,
        &query,
        &headers,
        &["host".to_string()],
        sigv4::UNSIGNED_PAYLOAD,
    )
    .unwrap();
    let signature = sigv4::sign(
        &sigv4::signing_key(secret, &credential),
        &sigv4::string_to_sign(amz_date, &credential.scope(), &canonical),
    );
    format!("{path}?{query}&X-Amz-Signature={signature}")
}

/// Sends a request to a presigned `target` (path and query), with no
/// Authorization header; returns the response head and the status and body.
fn send_presigned(port: u16, method: &str, target: &str, body: &[u8]) -> (String, (u16, String)) {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let request = format!(
        "{method} {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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
        ("/lst", "website"),
        ("/lst", "cors"),
        ("/lst/a", "retention"),
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
    assert_eq!(call("GET", "/vers/k", "legal-hold", b"").1.0, 501);
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
