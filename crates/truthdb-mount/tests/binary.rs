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

/// A binary started: killed, and its mount removed, when dropped, however
/// the test ends.
struct Started {
    child: Child,
    dir: PathBuf,
}

impl Drop for Started {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if mounted(&self.dir) {
            let _ = Command::new("fusermount3")
                .arg("-u")
                .arg("-z")
                .arg(&self.dir)
                .status();
        }
    }
}

/// The mount point, removed when dropped.
struct MountPoint(PathBuf);

impl Drop for MountPoint {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.0);
    }
}

fn start(dir: &Path) -> Started {
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
    let started = Started {
        child,
        dir: dir.to_path_buf(),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while !mounted(dir) {
        assert!(Instant::now() < deadline, "not mounted");
        std::thread::sleep(Duration::from_millis(50));
    }
    started
}

/// Whether `started` exits within a few seconds.
fn exits(started: &mut Started) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if started.child.try_wait().expect("wait").is_some() {
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
    let point =
        MountPoint(std::env::temp_dir().join(format!("truthdb-mount-test-{}", std::process::id())));
    std::fs::create_dir_all(&point.0).expect("mount point");
    // Unmounted from outside: it ends.
    let mut started = start(&point.0);
    let unmounted = Command::new("fusermount3")
        .arg("-u")
        .arg(&point.0)
        .status()
        .expect("fusermount3");
    assert!(unmounted.success());
    assert!(
        exits(&mut started),
        "still running after its mount was unmounted"
    );
    // Stopped (SIGTERM), even at once: it unmounts, and ends.
    for _ in 0..5 {
        let mut started = start(&point.0);
        let term = Command::new("kill")
            .args(["-TERM", &started.child.id().to_string()])
            .status()
            .expect("kill");
        assert!(term.success());
        assert!(exits(&mut started), "still running after SIGTERM");
        let status = started.child.wait().expect("status");
        assert!(status.success(), "{status}");
        assert!(!mounted(&point.0), "still mounted after SIGTERM");
    }
}
