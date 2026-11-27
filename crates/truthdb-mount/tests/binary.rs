//! The binary: it ends when its mount is unmounted, and unmounts when
//! stopped. Run where FUSE is (an openable /dev/fuse and fusermount3);
//! skipped elsewhere. No service is needed to mount (none answers here).

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn fuse() -> bool {
    let device = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")
        .is_ok();
    let fusermount = std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|dir| dir.join("fusermount3").exists())
    });
    device && fusermount
}

fn mounted(dir: &Path) -> bool {
    let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap_or_default();
    let dir = dir.to_string_lossy();
    mounts
        .lines()
        .any(|line| line.split_whitespace().nth(1) == Some(&*dir))
}

fn start(dir: &Path) -> Child {
    let child = Command::new(env!("CARGO_BIN_EXE_truthdb-mount"))
        .args([
            "--endpoint-url",
            "http://127.0.0.1:1",
            "--access-key",
            "key",
            "--secret-key",
            "secret",
            "bucket",
        ])
        .arg(dir)
        .spawn()
        .expect("start truthdb-mount");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !mounted(dir) {
        assert!(Instant::now() < deadline, "not mounted");
        std::thread::sleep(Duration::from_millis(50));
    }
    child
}

/// Whether `child` exits within a few seconds.
fn exits(child: &mut Child) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().expect("wait").is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn it_ends_when_unmounted_and_unmounts_when_stopped() {
    if !fuse() {
        eprintln!("skipped: no /dev/fuse or fusermount3");
        return;
    }
    let dir: PathBuf =
        std::env::temp_dir().join(format!("truthdb-mount-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mount point");
    // Unmounted from outside: it ends.
    let mut child = start(&dir);
    let unmounted = Command::new("fusermount3")
        .arg("-u")
        .arg(&dir)
        .status()
        .expect("fusermount3");
    assert!(unmounted.success());
    let ended = exits(&mut child);
    if !ended {
        let _ = child.kill();
    }
    assert!(ended, "still running after its mount was unmounted");
    // Stopped (SIGTERM): it unmounts, and ends.
    let mut child = start(&dir);
    let term = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(term.success());
    let ended = exits(&mut child);
    if !ended {
        let _ = child.kill();
        let _ = Command::new("fusermount3").arg("-u").arg(&dir).status();
    }
    assert!(ended, "still running after SIGTERM");
    assert!(!mounted(&dir), "still mounted after SIGTERM");
    std::fs::remove_dir(&dir).ok();
}
