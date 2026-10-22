//! Server-side encryption with customer-provided keys (SSE-C). The client
//! sends a 256-bit key with each request; it is never stored. What is kept
//! is its MD5 (as the client sent it, returned in answers) and a verifier
//! (a salt and a keyed digest of it under the key) that tells the key again.
//!
//! A body encrypted this way describes itself: a 32-byte header (magic,
//! version, salt), then the plaintext in 64 KiB chunks, each sealed with
//! AES-256-GCM under a key derived from the customer's key and the salt (so
//! every body has a key of its own), the chunk's index as nonce, and the
//! index and whether it is the last chunk as associated data (a reordered,
//! dropped or truncated chunk does not open). A range read decrypts the
//! chunks it overlaps. An empty body is one empty last chunk.

use base64::Engine as _;
use hyper::HeaderMap;
use md5::{Digest, Md5};
use ring::rand::SecureRandom;
use ring::{aead, hkdf, hmac};
use truthdb_object::CustomerKey;
use zeroize::Zeroizing;

use crate::error::S3Error;

pub const ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
pub const KEY: &str = "x-amz-server-side-encryption-customer-key";
pub const KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-MD5";
/// A copy source's key: the same names with this in place of `x-amz-`
/// (`x-amz-copy-source-server-side-encryption-customer-algorithm`).
pub const COPY_SOURCE_PREFIX: &str = "x-amz-copy-source-";

/// The headers a presigned URL need not sign (its signer does not hold the
/// key): the key and its MD5, the object's and a copy source's, lowercased.
pub const UNSIGNED_IN_PRESIGNED: [&str; 4] = [
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-copy-source-server-side-encryption-customer-key",
    "x-amz-copy-source-server-side-encryption-customer-key-md5",
];

/// Plaintext bytes per chunk.
pub const CHUNK: usize = 64 * 1024;
const TAG: usize = 16;
/// Stored bytes per full chunk.
const SEALED_CHUNK: u64 = (CHUNK + TAG) as u64;
/// The body header: magic, version, salt.
pub const HEADER_LEN: usize = 32;
const MAGIC: &[u8; 8] = b"TDBSSEC1";
const VERSION: u8 = 1;
const SALT: usize = 16;

fn base64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// A key the client sent with this request (never stored).
pub struct ProvidedKey {
    key: Zeroizing<[u8; 32]>,
    /// Base64 of the key's MD5, as sent.
    pub key_md5: String,
}

/// The customer-provided key in `headers` (the copy source's with
/// `copy_source`), if any: all three headers or none, AES256, a 256-bit key
/// whose MD5 is the one sent.
pub fn provided(headers: &HeaderMap, copy_source: bool) -> Result<Option<ProvidedKey>, S3Error> {
    let get = |name: &str| {
        let name = match copy_source {
            true => format!("{COPY_SOURCE_PREFIX}{}", &name["x-amz-".len()..]),
            false => name.to_string(),
        };
        headers
            .get(name)
            .map(|value| value.to_str().unwrap_or("").to_string())
    };
    from_values(get(ALGORITHM), get(KEY), get(KEY_MD5))
}

/// [`provided`] from a POST form's fields (`field` finds one by name).
pub fn provided_in_fields<'a>(
    field: impl Fn(&str) -> Option<&'a str>,
) -> Result<Option<ProvidedKey>, S3Error> {
    let get = |name: &str| field(name).map(str::to_string);
    from_values(get(ALGORITHM), get(KEY), get(KEY_MD5))
}

fn from_values(
    algorithm: Option<String>,
    key: Option<String>,
    key_md5: Option<String>,
) -> Result<Option<ProvidedKey>, S3Error> {
    let (algorithm, key, key_md5) = match (algorithm, key, key_md5) {
        (None, None, None) => return Ok(None),
        (Some(algorithm), Some(key), Some(key_md5)) => (algorithm, key, key_md5),
        _ => {
            return Err(invalid(
                "Requests specifying Server Side Encryption with Customer provided keys must \
                 provide the algorithm, the key and the key's MD5",
            ));
        }
    };
    if algorithm != "AES256" {
        return Err(S3Error::new(
            400,
            "InvalidEncryptionAlgorithmError",
            "The encryption request that you specified is not valid. The valid value is AES256.",
        ));
    }
    let decoded = Zeroizing::new(
        base64()
            .decode(key.trim())
            .map_err(|_| invalid("The secret key was invalid for the specified algorithm."))?,
    );
    let key: [u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| invalid("The secret key was invalid for the specified algorithm."))?;
    let key = Zeroizing::new(key);
    let md5 = base64().encode(Md5::digest(key.as_slice()));
    if md5 != key_md5.trim() {
        return Err(invalid(
            "The calculated MD5 hash of the key did not match the hash that was provided.",
        ));
    }
    Ok(Some(ProvidedKey { key, key_md5: md5 }))
}

fn invalid_request(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidRequest", message)
}

/// A write's customer-provided key: not together with
/// `x-amz-server-side-encryption`, and not into a bucket that blocks SSE-C
/// (403, as S3 answers).
pub fn check_write(
    key: Option<&ProvidedKey>,
    headers: &HeaderMap,
    blocked: bool,
) -> Result<(), S3Error> {
    if key.is_none() {
        return Ok(());
    }
    if headers.contains_key(crate::sse::SSE_HEADER) {
        return Err(invalid(
            "Server Side Encryption with Customer provided key is incompatible with the \
             encryption method specified",
        ));
    }
    if blocked {
        return Err(S3Error::access_denied(
            "This bucket blocks server-side encryption with customer-provided keys (SSE-C)",
        ));
    }
    Ok(())
}

/// A read of an object (or a part of an upload) kept with `stored`, by a
/// request that sent `key`: the same key, or none for an object without.
pub fn check_read(stored: Option<&CustomerKey>, key: Option<&ProvidedKey>) -> Result<(), S3Error> {
    match (stored, key) {
        (None, None) => Ok(()),
        (Some(_), None) => Err(invalid_request(
            "The object was stored using a form of Server Side Encryption. The correct \
             parameters must be provided to retrieve the object.",
        )),
        (None, Some(_)) => Err(invalid_request(
            "The encryption parameters are not applicable to this object.",
        )),
        (Some(stored), Some(key)) if key.matches(stored) => Ok(()),
        (Some(_), Some(_)) => Err(S3Error::access_denied("Access Denied")),
    }
}

/// A request naming a customer-provided key (`named`) must arrive over TLS:
/// the key is in it.
pub fn check_transport(named: bool, tls: bool) -> Result<(), S3Error> {
    if named && !tls {
        return Err(invalid_request(
            "Requests specifying Server Side Encryption with Customer provided keys must be \
             made over a secure connection.",
        ));
    }
    Ok(())
}

/// The answer's headers for an object (or upload) kept with `stored`.
pub fn with_echo(
    builder: hyper::http::response::Builder,
    stored: Option<&CustomerKey>,
) -> hyper::http::response::Builder {
    match stored {
        Some(stored) => builder
            .header(ALGORITHM, "AES256")
            .header(KEY_MD5, &stored.key_md5),
        None => builder,
    }
}

/// [`with_echo`] on a built response.
pub fn add_echo(response: &mut hyper::Response<crate::server::Body>, stored: Option<&CustomerKey>) {
    if let Some(stored) = stored {
        let headers = response.headers_mut();
        headers.insert(ALGORITHM, hyper::header::HeaderValue::from_static("AES256"));
        if let Ok(md5) = hyper::header::HeaderValue::from_str(&stored.key_md5) {
            headers.insert(KEY_MD5, md5);
        }
    }
}

/// Whether `headers` name any customer-provided key header (the object's or
/// a copy source's).
pub fn any_in(headers: &HeaderMap) -> bool {
    headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-amz-server-side-encryption-customer-")
            || name.starts_with("x-amz-copy-source-server-side-encryption-customer-")
    })
}

fn random<const N: usize>() -> Result<[u8; N], S3Error> {
    let mut out = [0u8; N];
    ring::rand::SystemRandom::new()
        .fill(&mut out)
        .map_err(|_| S3Error::internal("no randomness"))?;
    Ok(out)
}

fn verifier_mac(key: &[u8; 32], salt: &[u8]) -> hmac::Tag {
    hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, key),
        &[b"truthdb sse-c verifier".as_slice(), salt].concat(),
    )
}

impl ProvidedKey {
    /// What an object or upload encrypted with this key keeps of it.
    pub fn stored(&self) -> Result<CustomerKey, S3Error> {
        let salt: [u8; SALT] = random()?;
        let mac = verifier_mac(&self.key, &salt);
        Ok(CustomerKey {
            key_md5: self.key_md5.clone(),
            verifier: base64().encode([salt.as_slice(), mac.as_ref()].concat()),
        })
    }

    /// Whether this is the key `stored` was kept of.
    pub fn matches(&self, stored: &CustomerKey) -> bool {
        let Ok(verifier) = base64().decode(&stored.verifier) else {
            return false;
        };
        if verifier.len() <= SALT {
            return false;
        }
        let (salt, mac) = verifier.split_at(SALT);
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, self.key.as_slice()),
            &[b"truthdb sse-c verifier".as_slice(), salt].concat(),
            mac,
        )
        .is_ok()
    }

    /// The key a body with `salt` is sealed under.
    fn body_key(&self, salt: &[u8]) -> aead::LessSafeKey {
        let key: aead::UnboundKey = hkdf::Salt::new(hkdf::HKDF_SHA256, salt)
            .extract(self.key.as_slice())
            .expand(&[b"truthdb sse-c body"], &aead::AES_256_GCM)
            .expect("an AES-256 key length")
            .into();
        aead::LessSafeKey::new(key)
    }

    /// Starts encrypting a new body under this key (a fresh salt).
    pub fn encryptor(&self) -> Result<Encryptor, S3Error> {
        let salt: [u8; SALT] = random()?;
        let mut header = [0u8; HEADER_LEN];
        header[..8].copy_from_slice(MAGIC);
        header[8] = VERSION;
        header[9..9 + SALT].copy_from_slice(&salt);
        Ok(Encryptor {
            key: self.body_key(&salt),
            header: Some(header),
            index: 0,
            buffer: Vec::with_capacity(CHUNK),
        })
    }

    /// The decryptor of a body whose header is `header`; `None` when it is
    /// not such a body.
    pub fn decryptor(&self, header: &[u8]) -> Option<Decryptor> {
        if header.len() != HEADER_LEN || &header[..8] != MAGIC || header[8] != VERSION {
            return None;
        }
        Some(Decryptor {
            key: self.body_key(&header[9..9 + SALT]),
        })
    }
}

fn nonce(index: u64) -> aead::Nonce {
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(&index.to_le_bytes());
    aead::Nonce::assume_unique_for_key(nonce)
}

fn aad(index: u64, last: bool) -> [u8; 9] {
    let mut aad = [0u8; 9];
    aad[..8].copy_from_slice(&index.to_le_bytes());
    aad[8] = u8::from(last);
    aad
}

/// Encrypts a body as it streams in.
pub struct Encryptor {
    key: aead::LessSafeKey,
    /// Not yet handed out.
    header: Option<[u8; HEADER_LEN]>,
    index: u64,
    /// Plaintext of the chunk being filled: a full chunk is sealed only when
    /// more follows, so the last one is known as such.
    buffer: Vec<u8>,
}

impl Encryptor {
    fn seal(&mut self, last: bool) -> Vec<u8> {
        let mut chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(CHUNK));
        self.key
            .seal_in_place_append_tag(
                nonce(self.index),
                aead::Aad::from(aad(self.index, last)),
                &mut chunk,
            )
            .expect("a chunk within the AES-GCM length limit");
        self.index += 1;
        chunk
    }

    /// The stored bytes `plain` adds (the header first).
    pub fn push(&mut self, mut plain: &[u8]) -> Vec<u8> {
        let mut out = self.header.take().map(Vec::from).unwrap_or_default();
        while !plain.is_empty() {
            if self.buffer.len() == CHUNK {
                out.extend(self.seal(false));
            }
            let take = (CHUNK - self.buffer.len()).min(plain.len());
            self.buffer.extend_from_slice(&plain[..take]);
            plain = &plain[take..];
        }
        out
    }

    /// The last stored bytes: the last chunk (empty for an empty body).
    pub fn finish(mut self) -> Vec<u8> {
        let mut out = self.header.take().map(Vec::from).unwrap_or_default();
        out.extend(self.seal(true));
        out
    }
}

/// The plaintext length of a body of `stored` bytes, `None` when no body of
/// this format is that long.
pub fn plain_len(stored: u64) -> Option<u64> {
    let sealed = stored.checked_sub(HEADER_LEN as u64)?;
    let chunks = sealed.div_ceil(SEALED_CHUNK).max(1);
    let plain = sealed.checked_sub(chunks * TAG as u64)?;
    (plain.div_ceil(CHUNK as u64).max(1) == chunks).then_some(plain)
}

/// Where chunk `index` of a body of `plain` bytes is stored, as (offset,
/// length), and whether it is the last.
pub fn chunk_at(index: u64, plain: u64) -> (u64, u64, bool) {
    let chunks = plain.div_ceil(CHUNK as u64).max(1);
    let last = index + 1 == chunks;
    let plain_len = if last {
        plain - index * CHUNK as u64
    } else {
        CHUNK as u64
    };
    (
        HEADER_LEN as u64 + index * SEALED_CHUNK,
        plain_len + TAG as u64,
        last,
    )
}

/// Decrypts chunks of one body.
pub struct Decryptor {
    key: aead::LessSafeKey,
}

impl Decryptor {
    /// Chunk `index`'s plaintext from its stored bytes; `None` when it does
    /// not open (another key, or a damaged, moved or cut body).
    pub fn open(&self, index: u64, last: bool, stored: &[u8]) -> Option<Vec<u8>> {
        let mut chunk = stored.to_vec();
        let plain = self
            .key
            .open_in_place(nonce(index), aead::Aad::from(aad(index, last)), &mut chunk)
            .ok()?
            .len();
        chunk.truncate(plain);
        Some(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> ProvidedKey {
        let raw = [byte; 32];
        from_values(
            Some("AES256".to_string()),
            Some(base64().encode(raw)),
            Some(base64().encode(Md5::digest(raw))),
        )
        .unwrap()
        .unwrap()
    }

    fn encrypt(key: &ProvidedKey, plain: &[u8], piece: usize) -> Vec<u8> {
        let mut encryptor = key.encryptor().unwrap();
        let mut stored = Vec::new();
        for part in plain.chunks(piece.max(1)) {
            stored.extend(encryptor.push(part));
        }
        stored.extend(encryptor.finish());
        stored
    }

    fn decrypt(key: &ProvidedKey, stored: &[u8]) -> Option<Vec<u8>> {
        let plain = plain_len(stored.len() as u64)?;
        let decryptor = key.decryptor(&stored[..HEADER_LEN])?;
        let mut out = Vec::new();
        let chunks = plain.div_ceil(CHUNK as u64).max(1);
        for index in 0..chunks {
            let (at, len, last) = chunk_at(index, plain);
            out.extend(decryptor.open(index, last, &stored[at as usize..(at + len) as usize])?);
        }
        Some(out)
    }

    #[test]
    fn a_body_round_trips_at_every_chunk_boundary() {
        let key = key(7);
        for len in [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK, 3 * CHUNK + 5] {
            let plain: Vec<u8> = (0..len).map(|n| (n % 251) as u8).collect();
            for piece in [1000, CHUNK, 7 * CHUNK] {
                let stored = encrypt(&key, &plain, piece);
                assert_eq!(plain_len(stored.len() as u64), Some(len as u64), "{len}");
                assert_eq!(decrypt(&key, &stored).as_deref(), Some(plain.as_slice()));
                assert!(
                    len < 16 || !stored.windows(16).any(|w| w == &plain[..16]),
                    "plaintext in the stored body"
                );
            }
        }
    }

    #[test]
    fn a_wrong_key_or_a_cut_reordered_or_changed_body_does_not_open() {
        let (right, wrong) = (key(1), key(2));
        let plain = vec![9u8; 2 * CHUNK + 10];
        let stored = encrypt(&right, &plain, CHUNK);
        assert!(decrypt(&wrong, &stored).is_none());
        // Cut after a full chunk: that chunk was not sealed as the last.
        let cut = &stored[..HEADER_LEN + SEALED_CHUNK as usize];
        assert!(decrypt(&right, cut).is_none());
        // Chunks swapped.
        let mut swapped = stored.clone();
        let (a, b) = (HEADER_LEN, HEADER_LEN + SEALED_CHUNK as usize);
        let first = stored[a..b].to_vec();
        swapped[a..b].copy_from_slice(&stored[b..b + SEALED_CHUNK as usize]);
        swapped[b..b + SEALED_CHUNK as usize].copy_from_slice(&first);
        assert!(decrypt(&right, &swapped).is_none());
        let mut changed = stored.clone();
        changed[HEADER_LEN + 5] ^= 1;
        assert!(decrypt(&right, &changed).is_none());
        // Every body has its own key: the same plaintext seals differently.
        assert_ne!(encrypt(&right, &plain, CHUNK), stored);
    }

    #[test]
    fn the_headers_are_checked_and_the_verifier_tells_the_key() {
        let raw = [3u8; 32];
        let (good_key, good_md5) = (base64().encode(raw), base64().encode(Md5::digest(raw)));
        let values = |algorithm: &str, key: &str, md5: &str| {
            from_values(
                Some(algorithm.to_string()),
                Some(key.to_string()),
                Some(md5.to_string()),
            )
        };
        assert!(values("AES256", &good_key, &good_md5).unwrap().is_some());
        assert_eq!(
            values("aws:kms", &good_key, &good_md5).err().unwrap().code,
            "InvalidEncryptionAlgorithmError"
        );
        assert!(values("AES256", &base64().encode([3u8; 16]), &good_md5).is_err());
        assert!(values("AES256", &good_key, &base64().encode([0u8; 16])).is_err());
        assert!(values("AES256", "not base64!", &good_md5).is_err());
        assert!(from_values(Some("AES256".to_string()), None, None).is_err());
        assert!(from_values(None, None, None).unwrap().is_none());

        let stored = key(3).stored().unwrap();
        assert!(key(3).matches(&stored));
        assert!(!key(4).matches(&stored));
        assert_eq!(stored.key_md5, good_md5);
        // Salted: the same key is kept differently each time.
        assert_ne!(key(3).stored().unwrap().verifier, stored.verifier);
    }

    #[test]
    fn stored_lengths_map_to_plaintext_lengths_and_chunks() {
        assert_eq!(plain_len(HEADER_LEN as u64 + 16), Some(0));
        assert_eq!(plain_len(HEADER_LEN as u64 + 15), None);
        assert_eq!(plain_len(10), None);
        assert_eq!(chunk_at(0, 0), (HEADER_LEN as u64, 16, true));
        assert_eq!(
            chunk_at(1, CHUNK as u64 + 3),
            (HEADER_LEN as u64 + SEALED_CHUNK, 3 + 16, true)
        );
        assert_eq!(
            chunk_at(0, CHUNK as u64),
            (HEADER_LEN as u64, SEALED_CHUNK, true)
        );
    }
}
