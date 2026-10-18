//! A server that cannot start must exit non-zero, so systemd's
//! `Restart=on-failure` and anything else supervising the process see the
//! failure instead of a clean stop.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "truthdb-startup-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    dir
}

/// Runs the server with `config` and waits for it to exit. A server that is
/// still running after the deadline has not failed: it is killed and the test
/// fails.
fn run_server(dir: &Path, config: &str) -> (ExitStatus, String) {
    let config_path = dir.join("truthdb.toml");
    std::fs::write(&config_path, config).expect("config file");
    let log_path = dir.join("server.log");
    let log = std::fs::File::create(&log_path).expect("log file");
    let mut child = Command::new(env!("CARGO_BIN_EXE_truthdb"))
        .env("TRUTHDB_CONFIG", &config_path)
        .stdout(Stdio::from(log.try_clone().expect("clone")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn server");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().expect("wait") {
            let output = std::fs::read_to_string(&log_path).unwrap_or_default();
            return (status, output);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            let output = std::fs::read_to_string(&log_path).unwrap_or_default();
            panic!("server did not exit:\n{output}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn storage_failure_exits_non_zero() {
    let dir = test_dir("storage");
    let config = format!(
        "[network]\naddr = \"127.0.0.1\"\nport = {}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n",
        free_port(),
        dir.join("missing-dir").join("truth.db").display()
    );
    let (status, output) = run_server(&dir, &config);
    assert!(!status.success(), "exit {status}:\n{output}");
    assert!(output.contains("Failed to create storage"), "{output}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tds_bind_failure_exits_non_zero() {
    let dir = test_dir("tds");
    let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let tds_port = taken.local_addr().expect("addr").port();
    let config = format!(
        "[network]\naddr = \"127.0.0.1\"\nport = {}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n\n\
         [tds]\nenabled = true\naddr = \"127.0.0.1\"\nport = {tds_port}\n",
        free_port(),
        dir.join("truth.db").display()
    );
    let (status, output) = run_server(&dir, &config);
    assert!(!status.success(), "exit {status}:\n{output}");
    assert!(output.contains("Failed to start TDS gateway"), "{output}");
    drop(taken);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn invalid_config_exits_non_zero() {
    let dir = test_dir("config");
    let config = format!(
        "[network]\naddr = \"127.0.0.1\"\nport = {}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n\n\
         [tds]\nencrytion = \"required\"\n",
        free_port(),
        dir.join("truth.db").display()
    );
    let (status, output) = run_server(&dir, &config);
    assert!(!status.success(), "exit {status}:\n{output}");
    assert!(output.contains("Invalid config"), "{output}");
    assert!(output.contains("encrytion"), "{output}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tds_encryption_from_config_file_is_applied() {
    // `required` without a certificate is refused at startup, which is only
    // reachable if the file's `encryption` setting reaches the server.
    let dir = test_dir("encryption");
    let config = format!(
        "[network]\naddr = \"127.0.0.1\"\nport = {}\n\n[storage]\npath = \"{}\"\nsize_gib = 1\n\n\
         [tds]\nenabled = true\naddr = \"127.0.0.1\"\nport = {}\nencryption = \"required\"\n",
        free_port(),
        dir.join("truth.db").display(),
        free_port()
    );
    let (status, output) = run_server(&dir, &config);
    assert!(!status.success(), "exit {status}:\n{output}");
    assert!(
        output.contains("encryption = \"required\" needs tls_cert"),
        "{output}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
