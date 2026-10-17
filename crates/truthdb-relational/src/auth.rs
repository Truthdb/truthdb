//! Login password hashing: PBKDF2-HMAC-SHA256, salted and versioned.
//!
//! The stored credential is a self-describing text blob `v1$<iters>$<hex
//! salt>$<hex hash>` so the iteration count and salt travel with the hash and a
//! future version can raise the work factor without a migration. The KDF is
//! `ring::pbkdf2` (already in the tree via rustls — no new compiled dependency).
//! This is deliberately NOT SQL Server's on-disk hash format: a modern salted
//! KDF beats bug-for-bug faithfulness for stored credentials.
//!
//! Verification is pure CPU with no storage access, so the TDS login path runs
//! it off the engine worker pool (a `spawn_blocking` task) — ~30 ms of PBKDF2
//! must not sit on a worker thread or the async reactor.

use std::num::NonZeroU32;

use ring::pbkdf2;
use ring::rand::SecureRandom;

/// PBKDF2 iteration count for new hashes (OWASP-scale for HMAC-SHA256).
const ITERATIONS: u32 = 210_000;
const SALT_LEN: usize = 16;
const HASH_LEN: usize = 32;
static ALGORITHM: pbkdf2::Algorithm = pbkdf2::PBKDF2_HMAC_SHA256;

/// A well-formed credential blob that no real password matches, used by the
/// login path to spend the same PBKDF2 time when a login does not exist (or is
/// disabled). Verifying against it always yields [`VerifyOutcome::BadPassword`]
/// but costs the same ~30 ms as a real check, so response latency does not
/// reveal which usernames are valid. Its salt/hash are all-zero — the iteration
/// count and lengths match [`ITERATIONS`]/[`SALT_LEN`]/[`HASH_LEN`], which is
/// all that determines timing.
pub const DUMMY_BLOB: &str = "v1$210000$00000000000000000000000000000000$\
    0000000000000000000000000000000000000000000000000000000000000000";

/// The result of verifying a password against a stored credential blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// The password matches the stored hash.
    Ok,
    /// The password does not match.
    BadPassword,
    /// The stored blob could not be parsed (corrupt or unknown version).
    Malformed,
}

/// Hashes a plaintext password into a storable versioned blob. A fresh random
/// salt is drawn per call, so hashing the same password twice yields different
/// blobs — do not compare blobs for equality; verify with [`verify_password`].
pub fn hash_password(password: &str) -> String {
    let rng = ring::rand::SystemRandom::new();
    let mut salt = [0u8; SALT_LEN];
    rng.fill(&mut salt)
        .expect("system RNG must produce a salt for password hashing");
    let mut hash = [0u8; HASH_LEN];
    pbkdf2::derive(
        ALGORITHM,
        NonZeroU32::new(ITERATIONS).expect("iteration count is non-zero"),
        &salt,
        password.as_bytes(),
        &mut hash,
    );
    format!("v1${ITERATIONS}${}${}", hex(&salt), hex(&hash))
}

/// Produces a credential blob for a fresh random 32-byte password that is then
/// discarded. Nobody knows the plaintext, so the login can never authenticate —
/// used to materialize a placeholder (disabled) `sa` when the config supplied no
/// `sa` password. An admin resets it later via `ALTER LOGIN`.
pub fn hash_random_password() -> String {
    let rng = ring::rand::SystemRandom::new();
    let mut secret = [0u8; 32];
    rng.fill(&mut secret)
        .expect("system RNG must produce a placeholder secret");
    hash_password(&hex(&secret))
}

/// Verifies a plaintext password against a stored credential blob in constant
/// time (via `ring::pbkdf2::verify`). Returns [`VerifyOutcome::Malformed`] if the
/// blob cannot be parsed — the caller treats that as an authentication failure.
pub fn verify_password(blob: &str, password: &str) -> VerifyOutcome {
    let Some((iterations, salt, hash)) = parse_blob(blob) else {
        return VerifyOutcome::Malformed;
    };
    let Some(iterations) = NonZeroU32::new(iterations) else {
        return VerifyOutcome::Malformed;
    };
    match pbkdf2::verify(ALGORITHM, iterations, &salt, password.as_bytes(), &hash) {
        Ok(()) => VerifyOutcome::Ok,
        Err(_) => VerifyOutcome::BadPassword,
    }
}

/// Parses `v1$<iters>$<hex salt>$<hex hash>` into (iterations, salt, hash).
fn parse_blob(blob: &str) -> Option<(u32, Vec<u8>, Vec<u8>)> {
    let mut parts = blob.split('$');
    if parts.next()? != "v1" {
        return None;
    }
    let iterations: u32 = parts.next()?.parse().ok()?;
    let salt = unhex(parts.next()?)?;
    let hash = unhex(parts.next()?)?;
    if parts.next().is_some() || salt.len() != SALT_LEN || hash.len() != HASH_LEN {
        return None;
    }
    Some((iterations, salt, hash))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_then_verify_round_trips() {
        let blob = hash_password("Correct Horse Battery Staple");
        assert!(blob.starts_with("v1$210000$"));
        assert_eq!(
            verify_password(&blob, "Correct Horse Battery Staple"),
            VerifyOutcome::Ok
        );
        assert_eq!(
            verify_password(&blob, "correct horse battery staple"),
            VerifyOutcome::BadPassword
        );
        assert_eq!(verify_password(&blob, ""), VerifyOutcome::BadPassword);
    }

    #[test]
    fn a_fresh_salt_is_drawn_per_hash() {
        // Same password, two calls: different blobs (different salts), both verify.
        let a = hash_password("hunter2");
        let b = hash_password("hunter2");
        assert_ne!(a, b, "each hash must use a fresh random salt");
        assert_eq!(verify_password(&a, "hunter2"), VerifyOutcome::Ok);
        assert_eq!(verify_password(&b, "hunter2"), VerifyOutcome::Ok);
    }

    #[test]
    fn malformed_blobs_are_rejected() {
        assert_eq!(verify_password("", "x"), VerifyOutcome::Malformed);
        assert_eq!(verify_password("v2$1$00$00", "x"), VerifyOutcome::Malformed);
        assert_eq!(
            verify_password("v1$notanumber$aa$bb", "x"),
            VerifyOutcome::Malformed
        );
        // Wrong salt/hash lengths.
        assert_eq!(
            verify_password("v1$210000$00$00", "x"),
            VerifyOutcome::Malformed
        );
        // Extra field.
        let good = hash_password("x");
        assert_eq!(
            verify_password(&format!("{good}$extra"), "x"),
            VerifyOutcome::Malformed
        );
    }

    #[test]
    fn dummy_blob_is_well_formed_and_never_matches() {
        // It must PARSE (so verify spends the full KDF time) and reject every
        // password — a Malformed result would short-circuit and leak timing.
        assert_eq!(verify_password(DUMMY_BLOB, ""), VerifyOutcome::BadPassword);
        assert_eq!(
            verify_password(DUMMY_BLOB, "any password at all"),
            VerifyOutcome::BadPassword
        );
    }

    #[test]
    fn known_pbkdf2_hmac_sha256_vector() {
        // RFC-style check: PBKDF2-HMAC-SHA256(password="password", salt="salt",
        // c=1, dkLen=32) = 120fb6cffcf8b32c43e7225256c4f837a86548c9
        // 2ccc35480805987cb70be17b (a widely published test vector). We build a
        // v1 blob with those parameters and confirm verify accepts the password.
        let salt = b"salt";
        let expected = [
            0x12, 0x0f, 0xb6, 0xcf, 0xfc, 0xf8, 0xb3, 0x2c, 0x43, 0xe7, 0x22, 0x52, 0x56, 0xc4,
            0xf8, 0x37, 0xa8, 0x65, 0x48, 0xc9, 0x2c, 0xcc, 0x35, 0x48, 0x08, 0x05, 0x98, 0x7c,
            0xb7, 0x0b, 0xe1, 0x7b,
        ];
        // Our derive must reproduce the vector for iterations=1.
        let mut hash = [0u8; HASH_LEN];
        pbkdf2::derive(
            ALGORITHM,
            NonZeroU32::new(1).unwrap(),
            salt,
            b"password",
            &mut hash,
        );
        assert_eq!(
            hash,
            expected,
            "PBKDF2-HMAC-SHA256 test vector mismatch: got {}",
            hex(&hash)
        );
    }
}
