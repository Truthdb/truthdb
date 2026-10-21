//! Encryption at rest of the database file's pages.
//!
//! A database created with a key source holds a random 32-byte database key
//! (DEK), wrapped by the master key in a key block in the file header. The DEK
//! never leaves memory; every page key is derived from it with HKDF, salted
//! with the file's uuid:
//!
//! - data pages: AES-256-XTS over bytes 32.., the tweak being the page number
//!   and the page LSN; the 8-byte checksum field holds a truncated
//!   HMAC-SHA256 over the page number and bytes 8.. (the plaintext header and
//!   the ciphertext), so a changed byte or a page written to the wrong place
//!   fails verification;
//! - the search snapshot and spill pages: AES-256-XTS of whole pages (the
//!   snapshot keeps its own checksum; spill pages are query scratch).
//!
//! The master key is a key file (named keys, the first one current) or an
//! external command that wraps and unwraps the DEK (a KMS).

use std::io::Write as _;
use std::path::{Path, PathBuf};

use aes::Aes256;
use aes::cipher::KeyInit;
use ring::rand::{SecureRandom, SystemRandom};
use ring::{aead, hkdf, hmac};
use xts_mode::Xts128;

use crate::relstore::page;
use crate::storage::StorageError;
use crate::storage_layout::PAGE_SIZE;

/// Where the master key comes from.
#[derive(Clone, PartialEq, Eq)]
pub enum KeySource {
    /// A key file: lines `<id> <64 hex digits>`, the first key current. It
    /// must be readable by its owner only, the server's user.
    File(PathBuf),
    /// A program (and its arguments) run with `wrap` or `unwrap` appended:
    /// `wrap` reads the DEK as hex on stdin and prints an opaque wrapped
    /// form; `unwrap` reads that form and prints the DEK as hex.
    Command(Vec<String>),
}

impl std::fmt::Debug for KeySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeySource::File(path) => write!(f, "KeySource::File({})", path.display()),
            KeySource::Command(argv) => write!(f, "KeySource::Command({argv:?})"),
        }
    }
}

fn config_error(message: impl Into<String>) -> StorageError {
    StorageError::InvalidConfig(message.into())
}

/// One named master key of a key file.
struct MasterKey {
    id: String,
    key: [u8; 32],
}

/// Reads a key file: it must be a regular file owned by this process's
/// user with no permissions for anyone else.
fn read_key_file(path: &Path) -> Result<Vec<MasterKey>, StorageError> {
    use std::os::unix::fs::MetadataExt;
    let what = path.display();
    let meta = std::fs::metadata(path)
        .map_err(|err| config_error(format!("encryption key file {what}: {err}")))?;
    if !meta.is_file() {
        return Err(config_error(format!(
            "encryption key file {what} is not a regular file"
        )));
    }
    if meta.mode() & 0o077 != 0 {
        return Err(config_error(format!(
            "encryption key file {what} must be readable by its owner only (mode 0600), \
             it has mode {:o}",
            meta.mode() & 0o777
        )));
    }
    // SAFETY: geteuid has no preconditions and cannot fail.
    let euid = unsafe { libc::geteuid() };
    if meta.uid() != euid {
        return Err(config_error(format!(
            "encryption key file {what} must be owned by the server's user (uid {euid}), \
             it is owned by uid {}",
            meta.uid()
        )));
    }
    let text = std::fs::read_to_string(path)
        .map_err(|err| config_error(format!("encryption key file {what}: {err}")))?;
    let mut keys = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let bad = || {
            config_error(format!(
                "encryption key file {what}, line {}: expected `<id> <64 hex digits>`",
                number + 1
            ))
        };
        let (id, hex) = line.split_once(char::is_whitespace).ok_or_else(bad)?;
        let key = parse_hex32(hex.trim()).ok_or_else(bad)?;
        if !valid_key_id(id) {
            return Err(config_error(format!(
                "encryption key file {what}, line {}: a key id is 1 to 32 letters, digits, \
                 '-', '_' or '.'",
                number + 1
            )));
        }
        if keys.iter().any(|known: &MasterKey| known.id == id) {
            return Err(config_error(format!(
                "encryption key file {what}: key id {id} appears twice"
            )));
        }
        keys.push(MasterKey {
            id: id.to_string(),
            key,
        });
    }
    if keys.is_empty() {
        return Err(config_error(format!(
            "encryption key file {what} holds no key"
        )));
    }
    Ok(keys)
}

fn valid_key_id(id: &str) -> bool {
    (1..=MAX_KEY_ID).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Runs the key command with `operation` appended, `input` on its stdin;
/// returns its stdout, trimmed.
fn run_key_command(argv: &[String], operation: &str, input: &str) -> Result<String, StorageError> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| config_error("encryption.key_command is empty"))?;
    let mut child = std::process::Command::new(program)
        .args(args)
        .arg(operation)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|err| config_error(format!("encryption key command {program}: {err}")))?;
    if let Some(mut stdin) = child.stdin.take() {
        // A command that exits without reading reports through its status.
        let _ = writeln!(stdin, "{input}");
    }
    let output = child
        .wait_with_output()
        .map_err(|err| config_error(format!("encryption key command {program}: {err}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr: String = stderr.trim().chars().take(512).collect();
        return Err(config_error(format!(
            "encryption key command {program} {operation} failed ({}): {stderr}",
            output.status
        )));
    }
    String::from_utf8(output.stdout)
        .map(|text| text.trim().to_string())
        .map_err(|_| {
            config_error(format!(
                "encryption key command {program} {operation} printed something not UTF-8"
            ))
        })
}

// The key block, in the file header's reserved area.
const KEY_BLOCK_MAGIC: [u8; 8] = *b"TDBKEYS1";
const STATE_ENCRYPTED: u8 = 1;
const MAX_SLOTS: usize = 2;
const SLOTS_OFFSET: usize = 16;
const MAX_KEY_ID: usize = 32;
const MAX_BLOB: usize = 1024;
/// kind u8, id length u8, id, blob length u16, blob.
const SLOT_SIZE: usize = 2 + MAX_KEY_ID + 2 + MAX_BLOB;
const MAC_OFFSET: usize = SLOTS_OFFSET + MAX_SLOTS * SLOT_SIZE;
/// The key block's size.
pub const KEY_BLOCK_SIZE: usize = MAC_OFFSET + 32;
const SLOT_FILE: u8 = 1;
const SLOT_COMMAND: u8 = 2;

/// A DEK wrapped by one master key.
struct Slot {
    kind: u8,
    id: String,
    blob: Vec<u8>,
}

/// Wraps `dek` under `source`'s current master key.
fn wrap(source: &KeySource, dek: &[u8; 32], file_uuid: &[u8; 16]) -> Result<Slot, StorageError> {
    match source {
        KeySource::File(path) => {
            let keys = read_key_file(path)?;
            let current = &keys[0];
            let mut nonce = [0u8; 12];
            SystemRandom::new()
                .fill(&mut nonce)
                .map_err(|_| config_error("no randomness for the key wrap"))?;
            let key = aead::LessSafeKey::new(
                aead::UnboundKey::new(&aead::AES_256_GCM, &current.key).expect("a 32-byte key"),
            );
            let mut sealed = dek.to_vec();
            key.seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(wrap_aad(file_uuid, &current.id)),
                &mut sealed,
            )
            .map_err(|_| config_error("the key wrap failed"))?;
            Ok(Slot {
                kind: SLOT_FILE,
                id: current.id.clone(),
                blob: [nonce.as_slice(), &sealed].concat(),
            })
        }
        KeySource::Command(argv) => {
            let blob = run_key_command(argv, "wrap", &hex(dek))?;
            if blob.is_empty()
                || blob.len() > MAX_BLOB
                || !blob.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(config_error(format!(
                    "encryption key command wrap must print 1 to {MAX_BLOB} printable ASCII \
                     characters"
                )));
            }
            Ok(Slot {
                kind: SLOT_COMMAND,
                id: String::new(),
                blob: blob.into_bytes(),
            })
        }
    }
}

fn wrap_aad(file_uuid: &[u8; 16], key_id: &str) -> Vec<u8> {
    [b"truthdb dek".as_slice(), file_uuid, key_id.as_bytes()].concat()
}

/// Unwraps a slot's DEK with `source`, if it is the source's kind (and, for a
/// key file, it holds the slot's key). `None`: not this source's slot.
fn unwrap(
    source: &KeySource,
    slot: &Slot,
    file_uuid: &[u8; 16],
) -> Result<Option<[u8; 32]>, StorageError> {
    match (source, slot.kind) {
        (KeySource::File(path), SLOT_FILE) => {
            let keys = read_key_file(path)?;
            let Some(master) = keys.iter().find(|key| key.id == slot.id) else {
                return Ok(None);
            };
            if slot.blob.len() != 12 + 32 + 16 {
                return Err(StorageError::InvalidFile(
                    "a wrapped database key has the wrong length".to_string(),
                ));
            }
            let key = aead::LessSafeKey::new(
                aead::UnboundKey::new(&aead::AES_256_GCM, &master.key).expect("a 32-byte key"),
            );
            let nonce: [u8; 12] = slot.blob[..12].try_into().expect("12 bytes");
            let mut sealed = slot.blob[12..].to_vec();
            let dek = key
                .open_in_place(
                    aead::Nonce::assume_unique_for_key(nonce),
                    aead::Aad::from(wrap_aad(file_uuid, &slot.id)),
                    &mut sealed,
                )
                .map_err(|_| {
                    config_error(format!(
                        "master key {} does not unwrap this database's key",
                        slot.id
                    ))
                })?;
            Ok(Some(dek.try_into().expect("32 bytes")))
        }
        (KeySource::Command(argv), SLOT_COMMAND) => {
            let blob = String::from_utf8_lossy(&slot.blob);
            let dek = run_key_command(argv, "unwrap", &blob)?;
            parse_hex32(&dek).map(Some).ok_or_else(|| {
                config_error("encryption key command unwrap must print 64 hex digits")
            })
        }
        _ => Ok(None),
    }
}

/// HKDF output length.
struct Len(usize);

impl hkdf::KeyType for Len {
    fn len(&self) -> usize {
        self.0
    }
}

fn derive<const N: usize>(dek: &[u8; 32], file_uuid: &[u8; 16], info: &str) -> [u8; N] {
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, file_uuid).extract(dek);
    let mut out = [0u8; N];
    prk.expand(&[info.as_bytes()], Len(N))
        .expect("a valid HKDF length")
        .fill(&mut out)
        .expect("a valid HKDF length");
    out
}

fn block_mac(dek: &[u8; 32], file_uuid: &[u8; 16], block: &[u8]) -> [u8; 32] {
    let key = hmac::Key::new(
        hmac::HMAC_SHA256,
        &derive::<32>(dek, file_uuid, "truthdb key block mac"),
    );
    let tag = hmac::sign(&key, &[file_uuid.as_slice(), &block[..MAC_OFFSET]].concat());
    tag.as_ref().try_into().expect("32 bytes")
}

/// A random file uuid.
pub fn random_uuid() -> Result<[u8; 16], StorageError> {
    let mut uuid = [0u8; 16];
    SystemRandom::new()
        .fill(&mut uuid)
        .map_err(|_| config_error("no randomness for the file uuid"))?;
    Ok(uuid)
}

/// Creates the key block of a new database: a fresh DEK wrapped by
/// `source`. Returns the block and the DEK.
pub fn new_key_block(
    source: &KeySource,
    file_uuid: &[u8; 16],
) -> Result<([u8; KEY_BLOCK_SIZE], [u8; 32]), StorageError> {
    let mut dek = [0u8; 32];
    SystemRandom::new()
        .fill(&mut dek)
        .map_err(|_| config_error("no randomness for the database key"))?;
    let slot = wrap(source, &dek, file_uuid)?;
    let mut block = [0u8; KEY_BLOCK_SIZE];
    block[0..8].copy_from_slice(&KEY_BLOCK_MAGIC);
    block[8] = STATE_ENCRYPTED;
    block[9] = 1;
    let at = SLOTS_OFFSET;
    block[at] = slot.kind;
    block[at + 1] = slot.id.len() as u8;
    block[at + 2..at + 2 + slot.id.len()].copy_from_slice(slot.id.as_bytes());
    let blob_at = at + 2 + MAX_KEY_ID;
    block[blob_at..blob_at + 2].copy_from_slice(&(slot.blob.len() as u16).to_le_bytes());
    block[blob_at + 2..blob_at + 2 + slot.blob.len()].copy_from_slice(&slot.blob);
    let mac = block_mac(&dek, file_uuid, &block);
    block[MAC_OFFSET..].copy_from_slice(&mac);
    Ok((block, dek))
}

/// Recovers the DEK from a key block with `source`: the first slot it
/// unwraps whose DEK the block's MAC confirms.
pub fn unlock_key_block(
    block: &[u8],
    source: &KeySource,
    file_uuid: &[u8; 16],
) -> Result<[u8; 32], StorageError> {
    let corrupt = |why: &str| StorageError::InvalidFile(format!("the key block {why}"));
    if block.len() != KEY_BLOCK_SIZE || block[0..8] != KEY_BLOCK_MAGIC {
        return Err(corrupt("is missing"));
    }
    if block[8] != STATE_ENCRYPTED {
        return Err(corrupt(&format!("has unknown state {}", block[8])));
    }
    let count = block[9] as usize;
    if count == 0 || count > MAX_SLOTS {
        return Err(corrupt(&format!("has {count} key slots")));
    }
    let mut refused = None;
    for index in 0..count {
        let at = SLOTS_OFFSET + index * SLOT_SIZE;
        let id_len = block[at + 1] as usize;
        let blob_at = at + 2 + MAX_KEY_ID;
        let blob_len = u16::from_le_bytes([block[blob_at], block[blob_at + 1]]) as usize;
        if id_len > MAX_KEY_ID || blob_len > MAX_BLOB {
            return Err(corrupt("has a malformed key slot"));
        }
        let slot = Slot {
            kind: block[at],
            id: String::from_utf8_lossy(&block[at + 2..at + 2 + id_len]).into_owned(),
            blob: block[blob_at + 2..blob_at + 2 + blob_len].to_vec(),
        };
        match unwrap(source, &slot, file_uuid) {
            Ok(Some(dek)) => {
                let mac = block_mac(&dek, file_uuid, block);
                if equal(&mac, &block[MAC_OFFSET..]) {
                    return Ok(dek);
                }
                refused = Some(config_error(
                    "the configured master key does not unwrap this database's key",
                ));
            }
            Ok(None) => {}
            Err(err) => refused = Some(err),
        }
    }
    Err(refused.unwrap_or_else(|| {
        config_error(
            "this database's key is not wrapped by the configured master key (check \
             [encryption]: the key file must hold the key id the database was created with)",
        )
    }))
}

/// Constant-time equality.
fn equal(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Raw (headerless) pages that are encrypted whole.
#[derive(Debug, Clone, Copy)]
pub enum RawPage {
    /// The search snapshot's extent; the tweak's second half is the
    /// snapshot's generation.
    Snapshot,
    /// Query spill pages; the second half is drawn at each open.
    Spill,
}

/// The keys of an encrypted database.
pub struct PageKeys {
    page: Xts128<Aes256>,
    snapshot: Xts128<Aes256>,
    spill: Xts128<Aes256>,
    mac: hmac::Key,
    spill_salt: u64,
}

fn xts(key: &[u8; 64]) -> Xts128<Aes256> {
    Xts128::new(
        Aes256::new_from_slice(&key[..32]).expect("a 32-byte key"),
        Aes256::new_from_slice(&key[32..]).expect("a 32-byte key"),
    )
}

fn tweak(low: u64, high: u64) -> xts_mode::Array<u8, aes::cipher::consts::U16> {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&low.to_le_bytes());
    bytes[8..].copy_from_slice(&high.to_le_bytes());
    bytes.into()
}

impl PageKeys {
    pub fn new(dek: &[u8; 32], file_uuid: &[u8; 16]) -> Result<Self, StorageError> {
        let mut salt = [0u8; 8];
        SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| config_error("no randomness for the spill key"))?;
        Ok(PageKeys {
            page: xts(&derive::<64>(dek, file_uuid, "truthdb page xts")),
            snapshot: xts(&derive::<64>(dek, file_uuid, "truthdb snapshot xts")),
            spill: xts(&derive::<64>(dek, file_uuid, "truthdb spill xts")),
            mac: hmac::Key::new(
                hmac::HMAC_SHA256,
                &derive::<32>(dek, file_uuid, "truthdb page mac"),
            ),
            spill_salt: u64::from_le_bytes(salt),
        })
    }

    fn page_mac(&self, page_no: u64, page: &[u8]) -> [u8; 8] {
        let mut context = hmac::Context::with_key(&self.mac);
        context.update(&page_no.to_le_bytes());
        context.update(&page[8..]);
        context.sign().as_ref()[..8].try_into().expect("8 bytes")
    }
}

/// How pages are protected on disk: a checksum, or encryption plus a MAC.
pub enum PageCodec {
    Plain,
    Encrypted(Box<PageKeys>),
}

impl std::fmt::Debug for PageCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PageCodec::Plain => "PageCodec::Plain",
            PageCodec::Encrypted(_) => "PageCodec::Encrypted",
        })
    }
}

impl PageCodec {
    pub fn is_encrypted(&self) -> bool {
        matches!(self, PageCodec::Encrypted(_))
    }

    /// Writes `plain` (a page with its header) as it goes to disk at
    /// `page_no` into `out`: checksummed, or encrypted and MACed.
    pub fn seal(&self, page_no: u64, plain: &[u8], out: &mut [u8]) {
        assert_eq!(plain.len(), PAGE_SIZE);
        out.copy_from_slice(plain);
        match self {
            PageCodec::Plain => page::stamp_checksum(out),
            PageCodec::Encrypted(keys) => {
                out[0..8].fill(0);
                let lsn = page::page_lsn(out);
                keys.page
                    .encrypt_sector(&mut out[page::PAGE_HEADER_SIZE..], tweak(page_no, lsn));
                let mac = keys.page_mac(page_no, out);
                out[0..8].copy_from_slice(&mac);
            }
        }
    }

    /// Verifies a page read from `page_no` and, when encrypted, decrypts it
    /// in place. False: the page is damaged or belongs elsewhere.
    #[must_use]
    pub fn open(&self, page_no: u64, page: &mut [u8]) -> bool {
        assert_eq!(page.len(), PAGE_SIZE);
        match self {
            PageCodec::Plain => page::verify_checksum(page),
            PageCodec::Encrypted(keys) => {
                if !equal(&keys.page_mac(page_no, page), &page[0..8]) {
                    return false;
                }
                let lsn = page::page_lsn(page);
                keys.page
                    .decrypt_sector(&mut page[page::PAGE_HEADER_SIZE..], tweak(page_no, lsn));
                page[0..8].fill(0);
                true
            }
        }
    }

    /// Encrypts a raw page in place (nothing to do when plain). `index` is
    /// the page's place; `generation` tells the snapshot's writes apart.
    pub fn seal_raw(&self, kind: RawPage, index: u64, generation: u64, page: &mut [u8]) {
        if let PageCodec::Encrypted(keys) = self {
            match kind {
                RawPage::Snapshot => keys.snapshot.encrypt_sector(page, tweak(index, generation)),
                RawPage::Spill => keys
                    .spill
                    .encrypt_sector(page, tweak(index, keys.spill_salt)),
            }
        }
    }

    /// Decrypts a raw page sealed by [`Self::seal_raw`].
    pub fn open_raw(&self, kind: RawPage, index: u64, generation: u64, page: &mut [u8]) {
        if let PageCodec::Encrypted(keys) = self {
            match kind {
                RawPage::Snapshot => keys.snapshot.decrypt_sector(page, tweak(index, generation)),
                RawPage::Spill => keys
                    .spill
                    .decrypt_sector(page, tweak(index, keys.spill_salt)),
            }
        }
    }
}

/// A key file for this process's tests: `TRUTHDB_TEST_ENCRYPTION=1` makes
/// every database a core test creates or opens without keys use it, so the
/// storage suite runs encrypted.
#[cfg(test)]
pub(crate) fn test_key_source() -> Option<KeySource> {
    use std::os::unix::fs::PermissionsExt;
    static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    if std::env::var("TRUTHDB_TEST_ENCRYPTION").as_deref() != Ok("1") {
        return None;
    }
    let path = PATH.get_or_init(|| {
        let path = std::env::temp_dir().join(format!("truthdb-test-{}.keys", std::process::id()));
        std::fs::write(&path, format!("test {}\n", "ab".repeat(32))).expect("a test key file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("a test key file");
        path
    });
    Some(KeySource::File(path.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn key_file(dir: &Path, name: &str, body: &str, mode: u32) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("truthdb-enc-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    const UUID: [u8; 16] = [7; 16];

    #[test]
    fn a_key_block_unlocks_with_its_key_file_only() {
        let dir = temp_dir("file");
        let one = key_file(
            &dir,
            "one",
            &format!("# keys\nk1 {}\n", "11".repeat(32)),
            0o600,
        );
        let (block, dek) = new_key_block(&KeySource::File(one.clone()), &UUID).unwrap();
        assert_eq!(
            unlock_key_block(&block, &KeySource::File(one), &UUID).unwrap(),
            dek
        );

        // A rotated key file still opens it while it holds the old key.
        let rotated = key_file(
            &dir,
            "rotated",
            &format!("k2 {}\nk1 {}\n", "22".repeat(32), "11".repeat(32)),
            0o600,
        );
        assert_eq!(
            unlock_key_block(&block, &KeySource::File(rotated), &UUID).unwrap(),
            dek
        );
        // Another key under the same id, another file uuid, a changed byte.
        let other = key_file(&dir, "other", &format!("k1 {}\n", "33".repeat(32)), 0o600);
        assert!(unlock_key_block(&block, &KeySource::File(other), &UUID).is_err());
        let one = KeySource::File(dir.join("one"));
        assert!(unlock_key_block(&block, &one, &[8; 16]).is_err());
        let mut changed = block;
        changed[MAC_OFFSET - 1] ^= 1;
        assert!(unlock_key_block(&changed, &one, &UUID).is_err());
        let missing = key_file(&dir, "missing", &format!("k9 {}\n", "11".repeat(32)), 0o600);
        let err = unlock_key_block(&block, &KeySource::File(missing), &UUID).unwrap_err();
        assert!(err.to_string().contains("key id"), "{err}");
    }

    #[test]
    fn a_key_file_must_be_private_and_well_formed() {
        let dir = temp_dir("perms");
        let open = key_file(&dir, "open", &format!("k {}\n", "11".repeat(32)), 0o644);
        let err = read_key_file(&open).err().unwrap().to_string();
        assert!(err.contains("mode 0600"), "{err}");
        for body in [
            "",
            "k 1234\n",
            "k\n",
            &format!("bad/id {}\n", "11".repeat(32)),
        ] {
            let path = key_file(&dir, "bad", body, 0o600);
            assert!(read_key_file(&path).is_err(), "{body:?}");
        }
        let twice = format!("k {}\nk {}\n", "11".repeat(32), "22".repeat(32));
        assert!(read_key_file(&key_file(&dir, "twice", &twice, 0o600)).is_err());
    }

    #[test]
    fn a_key_command_wraps_and_unwraps() {
        let dir = temp_dir("command");
        // A toy KMS: the wrapped form is the DEK hex, reversed.
        let script = key_file(
            &dir,
            "kms.sh",
            "#!/bin/sh\nread line\necho \"$line\" | rev\n",
            0o700,
        );
        let source = KeySource::Command(vec![script.display().to_string()]);
        let (block, dek) = new_key_block(&source, &UUID).unwrap();
        assert_eq!(unlock_key_block(&block, &source, &UUID).unwrap(), dek);
        // A command that returns another key is caught by the block's MAC.
        let liar = key_file(
            &dir,
            "liar.sh",
            &format!("#!/bin/sh\nread line\necho {}\n", "44".repeat(32)),
            0o700,
        );
        let liar = KeySource::Command(vec![liar.display().to_string()]);
        assert!(unlock_key_block(&block, &liar, &UUID).is_err());
        let failing = KeySource::Command(vec!["false".to_string()]);
        let err = unlock_key_block(&block, &failing, &UUID).unwrap_err();
        assert!(err.to_string().contains("failed"), "{err}");
    }

    fn codec() -> PageCodec {
        PageCodec::Encrypted(Box::new(PageKeys::new(&[5; 32], &UUID).unwrap()))
    }

    fn sample(page_no: u64) -> Vec<u8> {
        let mut page = vec![0u8; PAGE_SIZE];
        page::write_header(
            &mut page,
            &page::PageHeader {
                page_lsn: 99,
                page_type: page::PAGE_TYPE_HEAP,
                flags: 0,
                object_id: 3,
                page_no,
            },
        );
        page[100..112].copy_from_slice(b"CANARY-VALUE");
        page
    }

    #[test]
    fn an_encrypted_page_hides_its_content_and_detects_every_change() {
        let codec = codec();
        let plain = sample(12);
        let mut sealed = vec![0u8; PAGE_SIZE];
        codec.seal(12, &plain, &mut sealed);
        assert!(!sealed.windows(12).any(|w| w == b"CANARY-VALUE"));
        let mut opened = sealed.clone();
        assert!(codec.open(12, &mut opened));
        let mut expected = plain.clone();
        expected[0..8].fill(0);
        assert_eq!(opened, expected);
        // Any byte changed, the page read as another page, or another key.
        for at in [0, 7, 8, 31, 32, 100, PAGE_SIZE - 1] {
            let mut changed = sealed.clone();
            changed[at] ^= 0x40;
            assert!(!codec.open(12, &mut changed), "byte {at}");
        }
        assert!(!codec.open(13, &mut sealed.clone()));
        let other = PageCodec::Encrypted(Box::new(PageKeys::new(&[6; 32], &UUID).unwrap()));
        assert!(!other.open(12, &mut sealed.clone()));
        // The same content at another LSN encrypts differently.
        let mut later = plain.clone();
        later[8..16].copy_from_slice(&100u64.to_le_bytes());
        let mut resealed = vec![0u8; PAGE_SIZE];
        codec.seal(12, &later, &mut resealed);
        assert_ne!(resealed[32..], sealed[32..]);
    }

    #[test]
    fn raw_pages_round_trip_under_their_tweak() {
        let codec = codec();
        let mut page = vec![0u8; PAGE_SIZE];
        page[..12].copy_from_slice(b"CANARY-VALUE");
        let plain = page.clone();
        codec.seal_raw(RawPage::Snapshot, 4, 9, &mut page);
        assert!(!page.windows(12).any(|w| w == b"CANARY-VALUE"));
        let mut wrong = page.clone();
        codec.open_raw(RawPage::Snapshot, 4, 10, &mut wrong);
        assert_ne!(wrong, plain);
        codec.open_raw(RawPage::Snapshot, 4, 9, &mut page);
        assert_eq!(page, plain);
        // Plain leaves them as they are.
        PageCodec::Plain.seal_raw(RawPage::Spill, 4, 0, &mut page);
        assert_eq!(page, plain);
    }
}
