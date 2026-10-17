// Resolve the TRUTHDB_VERSION string baked into the binary at compile time.
//
// Precedence (highest to lowest):
//   1. $TRUTHDB_VERSION env var — set by CI from the git tag.
//   2. `git describe --tags --dirty --always` — useful for local dev builds.
//   3. $CARGO_PKG_VERSION — last-resort fallback; matches the pre-build.rs behaviour.
//
// The Cargo.toml version is intentionally pinned at 0.1.0; real versions live on git tags.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=TRUTHDB_VERSION");

    let version = std::env::var("TRUTHDB_VERSION")
        .ok()
        .or_else(git_describe)
        .unwrap_or_else(|| std::env::var("CARGO_PKG_VERSION").unwrap());

    println!("cargo:rustc-env=TRUTHDB_VERSION={version}");
}

fn git_describe() -> Option<String> {
    let output = Command::new("git")
        .args(["describe", "--tags", "--dirty", "--always"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Strip leading `v` so the format matches the CI env var (e.g. "0.1.40-rc1").
    Some(trimmed.strip_prefix('v').unwrap_or(trimmed).to_string())
}
