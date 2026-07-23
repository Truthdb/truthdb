//! Stage 18.7 exit harness: TWO real server processes wired as a replication
//! pair through their configs, exercising the full lifecycle over real
//! sockets — standby kill/reconnect resume, primary kill → offline promote →
//! verification, epoch-fencing rejoin refusal, sync-commit fault injection
//! (timeout demotion, then re-synchronization off a fresh standby), and the
//! monitoring DMVs' lag/connectedness reporting.
//!
//! In-process tests already pin the fine-grained semantics (entry-aligned
//! chunks, restartpoint floors, slot invalidation under a retention cap,
//! visibility rules); this harness proves the assembled system — binaries,
//! configs, TLS, process death — behaves the same way.

#![cfg(target_os = "linux")]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use truthdb_net::{read_frame, write_frame};
use truthdb_proto::{
    CommandReq, CommandResp, Frame, HelloReq, HelloResp, MsgType, PROTOCOL_VERSION, decode_message,
    encode_message,
};

const SECRET: &str = "harness-cluster-secret";
const UUID: &str = "8f0e7a34-2d51-4c11-9c9e-3f6d2a7b1c05";
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
    stderr_path: PathBuf,
}

impl Server {
    fn spawn(config_path: &Path, stderr_path: PathBuf) -> Server {
        let log = std::fs::File::create(&stderr_path).expect("stderr file");
        let child = Command::new(env!("CARGO_BIN_EXE_truthdb"))
            .env("TRUTHDB_CONFIG", config_path)
            .stdout(Stdio::from(log.try_clone().expect("clone")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn server");
        Server { child, stderr_path }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
    }
}

#[allow(clippy::too_many_arguments)]
fn write_config(
    path: &Path,
    db_path: &Path,
    net_port: u16,
    repl: &str, // the whole [replication] body, or "" for none
) {
    let mut f = std::fs::File::create(path).expect("config file");
    writeln!(
        f,
        "[network]\naddr = \"127.0.0.1\"\nport = {net_port}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n{repl}",
        db_path.display()
    )
    .expect("write config");
}

async fn connect(port: u16) -> TcpStream {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(mut stream) => {
                let req = HelloReq {
                    protocol_version: PROTOCOL_VERSION,
                    client_name: "repl-harness".into(),
                    client_version: "0".into(),
                };
                let frame = Frame {
                    msg_type: MsgType::HelloReq,
                    flags: 0,
                    payload: encode_message(&req).expect("encode"),
                };
                write_frame(&mut stream, &frame).await.expect("hello");
                let resp = read_frame(&mut stream).await.expect("hello resp");
                let _: HelloResp = decode_message(&resp.payload).expect("hello decode");
                return stream;
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => panic!("server on port {port} never came up: {err}"),
        }
    }
}

async fn sql(stream: &mut TcpStream, command: &str) -> serde_json::Value {
    let req = CommandReq {
        id: 1,
        command: command.to_string(),
    };
    let frame = Frame {
        msg_type: MsgType::CommandReq,
        flags: 0,
        payload: encode_message(&req).expect("encode"),
    };
    write_frame(stream, &frame).await.expect("send");
    let resp = read_frame(stream).await.expect("recv");
    let resp: CommandResp = decode_message(&resp.payload).expect("decode");
    serde_json::from_str(&resp.message).unwrap_or(serde_json::Value::String(resp.message))
}

/// First column of the first row of the first result set, as a string.
fn first_cell(v: &serde_json::Value) -> String {
    let cell = &v["results"][0]["rows"][0][0];
    cell.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| cell.to_string())
}

/// First column of the first row, as a string.
async fn scalar(stream: &mut TcpStream, command: &str) -> String {
    let v = sql(stream, command).await;
    first_cell(&v)
}

async fn wait_scalar(port: u16, command: &str, expected: &str, what: &str) {
    let deadline = Instant::now() + DEADLINE;
    loop {
        // Reconnect each poll: the server may still be starting.
        let mut stream = connect(port).await;
        let full = sql(&mut stream, command).await;
        let got = first_cell(&full);
        if got == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: wanted {expected:?}, last response: {full}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_process_replication_exit_harness() {
    let dir = std::env::temp_dir().join(format!(
        "truthdb-harness-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");

    // TLS material shared by every incarnation of the cluster.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let cert_path = dir.join("repl.crt");
    let key_path = dir.join("repl.key");
    std::fs::write(&cert_path, cert.cert.pem()).expect("cert");
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).expect("key");

    let p_net = free_port();
    let p_repl = free_port();
    let s_net = free_port();

    let primary_db = dir.join("primary.db");
    let standby_db = dir.join("standby.db");
    let primary_cfg = dir.join("primary.toml");
    let standby_cfg = dir.join("standby.toml");

    write_config(
        &primary_cfg,
        &primary_db,
        p_net,
        &format!(
            "[replication]\nenabled = true\nrole = \"primary\"\nnode_id = 1\n\
             cluster_uuid = \"{UUID}\"\nshared_secret = \"{SECRET}\"\n\
             addr = \"127.0.0.1\"\nport = {p_repl}\n\
             tls_cert = \"{}\"\ntls_key = \"{}\"\n\
             heartbeat_ms = 200\nstall_timeout_ms = 2000\n",
            cert_path.display(),
            key_path.display()
        ),
    );
    let standby_repl = format!(
        "[replication]\nenabled = true\nrole = \"standby\"\nnode_id = 7\n\
         cluster_uuid = \"{UUID}\"\nshared_secret = \"{SECRET}\"\n\
         primary_addr = \"127.0.0.1:{p_repl}\"\nserver_name = \"localhost\"\n\
         tls_ca = \"{}\"\nheartbeat_ms = 200\nstall_timeout_ms = 2000\n\
         reconnect_delay_ms = 200\n",
        cert_path.display()
    );
    write_config(&standby_cfg, &standby_db, s_net, &standby_repl);

    // ---- The pair comes up; the standby follows live. ----------------------
    let mut primary = Server::spawn(&primary_cfg, dir.join("primary.log"));
    let mut c = connect(p_net).await;
    sql(
        &mut c,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT);",
    )
    .await;
    for i in 1..=20 {
        sql(&mut c, &format!("INSERT INTO t VALUES ({i}, {i});")).await;
    }
    let bak = dir.join("seed.bak");
    let resp = sql(
        &mut c,
        &format!("BACKUP DATABASE truthdb TO DISK = '{}';", bak.display()),
    )
    .await;
    assert!(
        resp["error"].is_null(),
        "online backup over the wire: {resp}"
    );
    truthdb_core::storage::Storage::restore_full_standby(&standby_db, &bak, &[])
        .expect("standby seed");

    let mut standby = Server::spawn(&standby_cfg, dir.join("standby.log"));
    wait_scalar(s_net, "SELECT COUNT(*) FROM t;", "20", "standby catch-up").await;

    // Lag/connectedness reporting on the primary.
    wait_scalar(
        p_net,
        "SELECT is_connected FROM sys.dm_repl_replica_states;",
        "1",
        "the DMV to report the connected standby",
    )
    .await;
    let lag: i64 = scalar(&mut c, "SELECT lag_bytes FROM sys.dm_repl_replica_states;")
        .await
        .parse()
        .expect("lag");
    assert!(lag >= 0, "lag is reported");

    // ---- Standby kill -9, primary keeps committing, restart resumes. -------
    standby.kill();
    for i in 21..=40 {
        sql(&mut c, &format!("INSERT INTO t VALUES ({i}, {i});")).await;
    }
    let mut standby = Server::spawn(&standby_cfg, dir.join("standby2.log"));
    wait_scalar(
        s_net,
        "SELECT COUNT(*) FROM t;",
        "40",
        "resume after kill -9",
    )
    .await;

    // A pre-failover seed, for the epoch-fencing rejoin refusal below.
    let old_bak = dir.join("old-epoch.bak");
    let resp = sql(
        &mut c,
        &format!("BACKUP DATABASE truthdb TO DISK = '{}';", old_bak.display()),
    )
    .await;
    assert!(resp["error"].is_null(), "{resp}");
    let old_epoch_db = dir.join("old-epoch.db");
    truthdb_core::storage::Storage::restore_full_standby(&old_epoch_db, &old_bak, &[])
        .expect("old-epoch seed");

    // ---- Primary dies; the standby is promoted and serves writes. ----------
    primary.kill();
    standby.kill(); // promote is offline
    let epoch = truthdb_core::storage::Storage::promote(&standby_db).expect("promote");
    assert_eq!(epoch, 1, "first failover");

    // The promoted node restarts as the new primary — with synchronous commit
    // armed, for the fault-injection leg.
    let promoted_cfg = dir.join("promoted.toml");
    write_config(
        &promoted_cfg,
        &standby_db,
        s_net,
        &format!(
            "[replication]\nenabled = true\nrole = \"primary\"\nnode_id = 2\n\
             cluster_uuid = \"{UUID}\"\nshared_secret = \"{SECRET}\"\n\
             addr = \"127.0.0.1\"\nport = {p_repl}\n\
             tls_cert = \"{}\"\ntls_key = \"{}\"\n\
             heartbeat_ms = 200\nstall_timeout_ms = 2000\n\
             synchronous_commit = true\nsync_timeout_ms = 1500\n",
            cert_path.display(),
            key_path.display()
        ),
    );
    let _promoted = Server::spawn(&promoted_cfg, dir.join("promoted.log"));
    let mut n = connect(s_net).await;
    // slt-style verification: the promoted node has exactly the committed
    // history, and accepts writes.
    assert_eq!(scalar(&mut n, "SELECT COUNT(*) FROM t;").await, "40");
    assert_eq!(scalar(&mut n, "SELECT v FROM t WHERE id = 40;").await, "40");
    // ---- Sync-commit fault injection: no standby → one timed-out wait, then
    // demotion to NOT_SYNCHRONIZED and fast local commits. ------------------
    let t0 = Instant::now();
    let resp = sql(&mut n, "INSERT INTO t VALUES (41, 41);").await;
    assert!(resp["error"].is_null(), "{resp}");
    assert!(
        t0.elapsed() >= Duration::from_millis(1500),
        "the first sync-commit wait times out (took {:?})",
        t0.elapsed()
    );
    let t1 = Instant::now();
    sql(&mut n, "INSERT INTO t VALUES (42, 42);").await;
    assert!(
        t1.elapsed() < Duration::from_millis(1200),
        "a degraded link commits locally (took {:?})",
        t1.elapsed()
    );

    // ---- Epoch-fencing rejoin refusal: an old-timeline seed is told to
    // reseed and never connects. --------------------------------------------
    let fence_net = free_port();
    let fence_cfg = dir.join("fence.toml");
    write_config(&fence_cfg, &old_epoch_db, fence_net, &standby_repl);
    let fenced = Server::spawn(&fence_cfg, dir.join("fence.log"));
    let deadline = Instant::now() + DEADLINE;
    loop {
        let log = fenced.stderr();
        if log.contains("reseed") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the old-timeline standby was never refused; log:\n{log}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    drop(fenced);

    // ---- A fresh seed of the NEW timeline attaches, acks, and
    // re-synchronizes the degraded link. ------------------------------------
    let new_bak = dir.join("new-epoch.bak");
    let resp = sql(
        &mut n,
        &format!("BACKUP DATABASE truthdb TO DISK = '{}';", new_bak.display()),
    )
    .await;
    assert!(resp["error"].is_null(), "{resp}");
    let new_standby_db = dir.join("new-standby.db");
    truthdb_core::storage::Storage::restore_full_standby(&new_standby_db, &new_bak, &[])
        .expect("new seed");
    let new_standby_cfg = dir.join("new-standby.toml");
    write_config(&new_standby_cfg, &new_standby_db, fence_net, &standby_repl);
    let _new_standby = Server::spawn(&new_standby_cfg, dir.join("new-standby.log"));
    wait_scalar(
        s_net,
        "SELECT sync_state FROM sys.dm_repl_replica_states;",
        "SYNCHRONIZED",
        "the fresh standby to re-synchronize the link",
    )
    .await;
    // And a sync-commit write now completes fast (the standby acks it).
    let t2 = Instant::now();
    let resp = sql(&mut n, "INSERT INTO t VALUES (43, 43);").await;
    assert!(resp["error"].is_null(), "{resp}");
    assert!(
        t2.elapsed() < Duration::from_millis(1200),
        "a synchronized commit is acked promptly (took {:?})",
        t2.elapsed()
    );
    wait_scalar(
        fence_net,
        "SELECT COUNT(*) FROM t;",
        "43",
        "new standby follows",
    )
    .await;

    let _ = std::fs::remove_dir_all(&dir);
}
