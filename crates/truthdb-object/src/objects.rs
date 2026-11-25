//! Object versions. Each version is a row in the objects tree under
//! `bucket, 0, key, 0, !version`: byte order is S3's listing order (UTF-8
//! binary), and inverting the version number puts a key's newest version
//! first. A version holds a sealed body (one reference) and its attributes
//! (system headers and user metadata), which live in the attrs tree in
//! chunks small enough for a tree cell.

use truthdb_core::relstore::btree::{BTree, TreeInsert};
use truthdb_core::relstore::ctx::{OpMode, RelCtx};
use truthdb_core::storage::StorageError;

use crate::body::SealedBody;
use crate::buckets::BucketRow;
use crate::journal::{Change, JournalObject, Notice, RecordType};
use crate::{
    ATTRS_TREE, BUCKETS_TREE, NEXT_VERSION_KEY, OBJECTS_TREE, ObjFile, ObjTxn, Versioning,
    next_counter,
};

/// Longest object key accepted (S3's limit), in UTF-8 bytes.
pub const MAX_KEY_BYTES: usize = 1024;

/// Attribute bytes per attrs-tree row.
const ATTRS_CHUNK: usize = 1500;

/// An object's headers: the system ones S3 stores and returns, user metadata
/// (`x-amz-meta-*`, names lowercased without the prefix), and its tags.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectAttrs {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub user: Vec<(String, String)>,
    pub tags: Vec<(String, String)>,
    /// The customer-provided key (SSE-C) the body is encrypted with.
    pub customer_key: Option<CustomerKey>,
    /// Where a website endpoint redirects requests for the object
    /// (`x-amz-website-redirect-location`).
    pub website_redirect: Option<String>,
    /// The body's checksum, as S3 keeps and returns it.
    pub checksum: Option<StoredChecksum>,
}

/// A body's checksum: its algorithm as S3 names it (`CRC64NVME`, ...),
/// whether it is of the whole object (`FULL_OBJECT`) or of its parts'
/// checksums (`COMPOSITE`), and its value, base64 as S3 sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredChecksum {
    pub algorithm: String,
    pub full_object: bool,
    pub value: String,
}

/// What is kept of a customer-provided key (SSE-C): its MD5 as the client
/// sent it (returned in answers) and a verifier that tells the key again;
/// never the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomerKey {
    /// Base64 of the key's MD5.
    pub key_md5: String,
    /// Base64 of a salt and a keyed digest of it under the key.
    pub verifier: String,
}

impl ObjectAttrs {
    pub(crate) fn is_empty(&self) -> bool {
        *self == ObjectAttrs::default()
    }

    /// `[tag u8][len u16][bytes]` per field; user metadata and tags as name
    /// and value.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut field = |tag: u8, value: &str| {
            out.push(tag);
            out.extend_from_slice(&(value.len() as u16).to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        };
        for (tag, value) in [
            (1, &self.content_type),
            (2, &self.cache_control),
            (3, &self.content_disposition),
            (4, &self.content_encoding),
            (5, &self.content_language),
            (6, &self.expires),
        ] {
            if let Some(value) = value {
                field(tag, value);
            }
        }
        for (name, value) in &self.user {
            field(7, name);
            field(8, value);
        }
        for (name, value) in &self.tags {
            field(9, name);
            field(10, value);
        }
        if let Some(key) = &self.customer_key {
            field(11, &key.key_md5);
            field(12, &key.verifier);
        }
        if let Some(location) = &self.website_redirect {
            field(13, location);
        }
        // `<algorithm> <FULL_OBJECT|COMPOSITE> <value>`.
        if let Some(checksum) = &self.checksum {
            let kind = if checksum.full_object {
                "FULL_OBJECT"
            } else {
                "COMPOSITE"
            };
            field(
                14,
                &format!("{} {kind} {}", checksum.algorithm, checksum.value),
            );
        }
        out
    }

    pub(crate) fn decode(mut bytes: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt object attributes".to_string());
        let mut attrs = ObjectAttrs::default();
        let mut pending_name = None;
        while !bytes.is_empty() {
            if bytes.len() < 3 {
                return Err(corrupt());
            }
            let tag = bytes[0];
            let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
            let value = bytes
                .get(3..3 + len)
                .ok_or_else(corrupt)
                .and_then(|value| String::from_utf8(value.to_vec()).map_err(|_| corrupt()))?;
            bytes = &bytes[3 + len..];
            match tag {
                1 => attrs.content_type = Some(value),
                2 => attrs.cache_control = Some(value),
                3 => attrs.content_disposition = Some(value),
                4 => attrs.content_encoding = Some(value),
                5 => attrs.content_language = Some(value),
                6 => attrs.expires = Some(value),
                7 => pending_name = Some(value),
                8 => attrs
                    .user
                    .push((pending_name.take().ok_or_else(corrupt)?, value)),
                9 => pending_name = Some(value),
                10 => attrs
                    .tags
                    .push((pending_name.take().ok_or_else(corrupt)?, value)),
                11 => pending_name = Some(value),
                12 => {
                    attrs.customer_key = Some(CustomerKey {
                        key_md5: pending_name.take().ok_or_else(corrupt)?,
                        verifier: value,
                    })
                }
                13 => attrs.website_redirect = Some(value),
                14 => {
                    let mut parts = value.splitn(3, ' ');
                    let (Some(algorithm), Some(kind), Some(value)) =
                        (parts.next(), parts.next(), parts.next())
                    else {
                        return Err(corrupt());
                    };
                    attrs.checksum = Some(StoredChecksum {
                        algorithm: algorithm.to_string(),
                        full_object: match kind {
                            "FULL_OBJECT" => true,
                            "COMPOSITE" => false,
                            _ => return Err(corrupt()),
                        },
                        value: value.to_string(),
                    });
                }
                _ => return Err(corrupt()),
            }
        }
        Ok(attrs)
    }
}

/// Which version of a key to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSel {
    Latest,
    /// The version an unversioned (or suspended) bucket writes.
    Null,
    Seq(u64),
}

/// A version id as S3 writes it: `null` for the null version, else its number.
pub fn version_id(version: u64, null_version: bool) -> String {
    if null_version {
        "null".to_string()
    } else {
        version.to_string()
    }
}

/// An object version's S3 storage class: those of general-purpose buckets,
/// and EXPRESS_ONEZONE, a directory bucket's. It is kept with the version; a cold class's body is stored compressed
/// (see [`StorageClass::pack_level`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageClass {
    #[default]
    Standard,
    ReducedRedundancy,
    StandardIa,
    OnezoneIa,
    IntelligentTiering,
    Glacier,
    DeepArchive,
    GlacierIr,
    /// A directory bucket's (and only its).
    ExpressOnezone,
}

impl StorageClass {
    const ALL: [StorageClass; 9] = [
        StorageClass::Standard,
        StorageClass::ReducedRedundancy,
        StorageClass::StandardIa,
        StorageClass::OnezoneIa,
        StorageClass::IntelligentTiering,
        StorageClass::Glacier,
        StorageClass::DeepArchive,
        StorageClass::GlacierIr,
        StorageClass::ExpressOnezone,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            StorageClass::Standard => "STANDARD",
            StorageClass::ReducedRedundancy => "REDUCED_REDUNDANCY",
            StorageClass::StandardIa => "STANDARD_IA",
            StorageClass::OnezoneIa => "ONEZONE_IA",
            StorageClass::IntelligentTiering => "INTELLIGENT_TIERING",
            StorageClass::Glacier => "GLACIER",
            StorageClass::DeepArchive => "DEEP_ARCHIVE",
            StorageClass::GlacierIr => "GLACIER_IR",
            StorageClass::ExpressOnezone => "EXPRESS_ONEZONE",
        }
    }

    /// The general purpose bucket class S3 names `name`, if it is one of
    /// these (not EXPRESS_ONEZONE, a directory bucket's).
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .filter(|class| *class != StorageClass::ExpressOnezone)
            .find(|class| class.as_str() == name)
    }

    /// Its four bits in a row's flags (STANDARD is 0, so rows written before
    /// classes read as STANDARD; EXPRESS_ONEZONE, 8, is the only one with
    /// the fourth).
    pub(crate) fn code(self) -> u8 {
        Self::ALL.iter().position(|class| *class == self).unwrap() as u8
    }

    pub(crate) fn from_code(code: u8) -> Self {
        Self::ALL
            .get((code & 15) as usize)
            .copied()
            .unwrap_or_default()
    }
}

/// An object version as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub version: u64,
    pub null_version: bool,
    pub delete_marker: bool,
    pub body: u64,
    pub size: u64,
    pub md5: [u8; 16],
    /// Parts of a multipart upload (0 for a single PUT).
    pub parts: u16,
    pub mtime_ms: u64,
    pub storage_class: StorageClass,
    pub attrs: ObjectAttrs,
}

impl ObjectInfo {
    /// The quoted ETag: the body's MD5, or for a multipart upload the MD5 of
    /// its parts' MD5s with the part count.
    pub fn etag(&self) -> String {
        let hex: String = self.md5.iter().map(|b| format!("{b:02x}")).collect();
        if self.parts == 0 {
            format!("\"{hex}\"")
        } else {
            format!("\"{hex}-{}\"", self.parts)
        }
    }
}

/// The body side of a new version.
pub(crate) struct NewVersion {
    pub body: u64,
    pub size: u64,
    pub md5: [u8; 16],
    pub parts: u16,
    pub storage_class: StorageClass,
}

/// What a DeleteObject did: the version it removed or the delete marker it
/// added (`version` is `None` when there was nothing to remove).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Deleted {
    pub version: Option<u64>,
    pub null_version: bool,
    pub delete_marker: bool,
}

/// Why an object operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectError {
    NoSuchBucket,
    NoSuchKey,
    NoSuchVersion,
    /// The version named is a delete marker, which the operation cannot act on.
    DeleteMarker,
    InvalidKey(&'static str),
    /// Object Lock protects the version (its retention or legal hold).
    Locked,
    /// The operation needs Object Lock, which the bucket does not have.
    ObjectLockNotEnabled,
    /// A retention change that does not apply to the version, and why.
    InvalidRetention(&'static str),
    /// The version is not in an archive class, so it cannot be restored.
    NotArchived,
    /// The version's restore has not completed yet.
    RestoreInProgress,
    /// The restore's retrieval tier is not offered for the version's class.
    TierUnavailable,
}

pub fn validate_key(key: &str) -> Result<(), ObjectError> {
    if key.is_empty() {
        return Err(ObjectError::InvalidKey("an object key must not be empty"));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(ObjectError::InvalidKey(
            "an object key is at most 1024 bytes",
        ));
    }
    if key.contains('\0') {
        return Err(ObjectError::InvalidKey(
            "an object key must not contain a NUL character",
        ));
    }
    Ok(())
}

/// `bucket, 0, key, 0`: every version of the key starts with this.
pub(crate) fn key_prefix(bucket: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len() + key.len() + 2);
    out.extend_from_slice(bucket.as_bytes());
    out.push(0);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out
}

pub(crate) fn version_key(bucket: &str, key: &str, version: u64) -> Vec<u8> {
    let mut out = key_prefix(bucket, key);
    out.extend_from_slice(&(!version).to_be_bytes());
    out
}

pub(crate) fn version_from_key(row_key: &[u8]) -> Result<u64, StorageError> {
    let tail = row_key
        .len()
        .checked_sub(8)
        .ok_or_else(|| StorageError::InvalidFile("corrupt object key".to_string()))?;
    Ok(!u64::from_be_bytes(row_key[tail..].try_into().unwrap()))
}

pub(crate) const FLAG_DELETE_MARKER: u8 = 1;
const FLAG_NULL_VERSION: u8 = 2;
const FLAG_HAS_ATTRS: u8 = 4;
/// Bits 3-6: the version's storage class (see [`StorageClass::code`]).
pub(crate) const CLASS_SHIFT: u8 = 3;

fn class_of(flags: u8) -> StorageClass {
    StorageClass::from_code(flags >> CLASS_SHIFT)
}

/// A version's stored row.
#[derive(Clone)]
pub(crate) struct VersionRow {
    body: u64,
    pub(crate) size: u64,
    md5: [u8; 16],
    parts: u16,
    mtime_ms: u64,
    pub(crate) flags: u8,
}

impl VersionRow {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(43);
        out.extend_from_slice(&self.body.to_le_bytes());
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&self.md5);
        out.extend_from_slice(&self.parts.to_le_bytes());
        out.extend_from_slice(&self.mtime_ms.to_le_bytes());
        out.push(self.flags);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != 43 {
            return Err(StorageError::InvalidFile("corrupt version row".to_string()));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        Ok(VersionRow {
            body: u64_at(0),
            size: u64_at(8),
            md5: bytes[16..32].try_into().unwrap(),
            parts: u16::from_le_bytes([bytes[32], bytes[33]]),
            mtime_ms: u64_at(34),
            flags: bytes[42],
        })
    }
}

/// The key of one chunk of a version's attributes: its state key (see
/// `version_state`), then the chunk's number.
fn attrs_key(state: &[u8], chunk: u16) -> Vec<u8> {
    [state, &chunk.to_be_bytes()].concat()
}

/// The version of `key` that `which` picks (a delete marker included), as
/// its row key, number and row.
pub(crate) fn find_version(
    ctx: &mut RelCtx<'_>,
    objects: &BTree,
    bucket: &str,
    key: &str,
    which: VersionSel,
) -> Result<Option<(Vec<u8>, u64, VersionRow)>, StorageError> {
    let prefix = key_prefix(bucket, key);
    let mut upper = prefix.clone();
    upper.extend_from_slice(&[0xFF; 8]);
    // Only the null version needs a scan: the newest is the first row, and a
    // numbered version has its own key.
    let rows = match which {
        VersionSel::Latest => objects.scan_range_limit(ctx, Some(&prefix), Some(&upper), 1)?,
        VersionSel::Seq(seq) => {
            let row_key = version_key(bucket, key, seq);
            match objects.get(ctx, &row_key)? {
                Some(row) => vec![(row_key, row)],
                None => Vec::new(),
            }
        }
        VersionSel::Null => objects.scan_range(ctx, Some(&prefix), Some(&upper))?,
    };
    for (row_key, row) in rows {
        let version = version_from_key(&row_key)?;
        let row = VersionRow::decode(&row)?;
        let wanted = match which {
            VersionSel::Latest => true,
            VersionSel::Null => row.flags & FLAG_NULL_VERSION != 0,
            VersionSel::Seq(seq) => seq == version,
        };
        if wanted {
            return Ok(Some((row_key, version, row)));
        }
    }
    Ok(None)
}

/// A version's attributes, from its chunks in the attrs tree.
fn read_attrs(
    ctx: &mut RelCtx<'_>,
    attrs_tree: &BTree,
    state: &[u8],
) -> Result<ObjectAttrs, StorageError> {
    let mut bytes = Vec::new();
    for (_, chunk) in attrs_tree.scan_range(
        ctx,
        Some(&attrs_key(state, 0)),
        Some(&attrs_key(state, u16::MAX)),
    )? {
        bytes.extend_from_slice(&chunk);
    }
    ObjectAttrs::decode(&bytes)
}

impl ObjTxn<'_, '_> {
    /// The newest version of `key`, unless it is a delete marker.
    pub(crate) fn latest_live_version(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<u64>, StorageError> {
        Ok(find_version(
            self.ctx,
            &self.trees.objects(),
            bucket,
            key,
            VersionSel::Latest,
        )?
        .filter(|(_, _, row)| row.flags & FLAG_DELETE_MARKER == 0)
        .map(|(_, version, _)| version))
    }

    /// `key`'s current version as a conditional write or delete reads it, in
    /// this transaction: none, a delete marker (`Some(None)`), or a version
    /// with its quoted ETag. A key that is not valid, or a bucket that is
    /// not there, is said first, as every object operation says it.
    #[allow(clippy::type_complexity)]
    pub fn current_etag(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<Result<Option<Option<String>>, ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        if self.bucket_row(bucket)?.is_none() {
            return Ok(Err(ObjectError::NoSuchBucket));
        }
        let found = find_version(
            self.ctx,
            &self.trees.objects(),
            bucket,
            key,
            VersionSel::Latest,
        )?;
        Ok(Ok(found.map(|(_, version, row)| {
            (row.flags & FLAG_DELETE_MARKER == 0).then(|| {
                ObjectInfo {
                    version,
                    null_version: false,
                    delete_marker: false,
                    body: row.body,
                    size: row.size,
                    md5: row.md5,
                    parts: row.parts,
                    mtime_ms: row.mtime_ms,
                    storage_class: class_of(row.flags),
                    attrs: ObjectAttrs::default(),
                }
                .etag()
            })
        })))
    }

    /// The version `which` picks, unless it is missing or a delete marker:
    /// its number, whether it is the null version and its storage class.
    pub(crate) fn find_live(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
    ) -> Result<Result<(u64, bool, StorageClass), ObjectError>, StorageError> {
        let found = find_version(self.ctx, &self.trees.objects(), bucket, key, which)?;
        Ok(match found {
            None => Err(match which {
                VersionSel::Latest => ObjectError::NoSuchKey,
                _ => ObjectError::NoSuchVersion,
            }),
            Some((_, _, row)) if row.flags & FLAG_DELETE_MARKER != 0 => Err(match which {
                VersionSel::Latest => ObjectError::NoSuchKey,
                _ => ObjectError::DeleteMarker,
            }),
            Some((_, version, row)) => Ok((
                version,
                row.flags & FLAG_NULL_VERSION != 0,
                class_of(row.flags),
            )),
        })
    }

    pub(crate) fn bucket_row(&mut self, bucket: &str) -> Result<Option<BucketRow>, StorageError> {
        self.trees
            .buckets()
            .get(self.ctx, bucket.as_bytes())?
            .map(|row| BucketRow::decode(&row))
            .transpose()
    }

    /// Stores `body` as the newest version of `key`, in `storage_class`:
    /// seals it (the version holds its one reference) and, unless versioning
    /// is enabled, replaces the key's null version (dropping its body's
    /// reference, collected after `grace_ms`).
    #[allow(clippy::too_many_arguments)]
    pub fn put_object(
        &mut self,
        bucket: &str,
        key: &str,
        body: &SealedBody,
        attrs: &ObjectAttrs,
        storage_class: StorageClass,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<ObjectInfo, ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        let sealed = body.seal_in(self)?;
        let info = self.insert_version(
            bucket,
            key,
            bucket_row.versioning,
            NewVersion {
                body: sealed.body,
                size: body.size.unwrap_or(sealed.len),
                md5: sealed.md5,
                parts: 0,
                storage_class,
            },
            attrs,
            now_ms,
            grace_ms,
        )?;
        Ok(Ok(info))
    }

    /// Writes `source` as the newest version of `key` (S3 CopyObject), with
    /// `attrs`, in `storage_class`: the new version shares the source's body
    /// (one more reference), so no bytes are copied. `source` must be a
    /// version read in the same lock hold, and not a delete marker.
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object(
        &mut self,
        bucket: &str,
        key: &str,
        source: &ObjectInfo,
        attrs: &ObjectAttrs,
        storage_class: StorageClass,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<ObjectInfo, ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        // The reference comes first: replacing the source's own null version
        // drops one, and the body must not reach zero in between.
        self.add_ref(source.body)?;
        let info = self.insert_version(
            bucket,
            key,
            bucket_row.versioning,
            NewVersion {
                body: source.body,
                size: source.size,
                md5: source.md5,
                parts: source.parts,
                storage_class,
            },
            attrs,
            now_ms,
            grace_ms,
        )?;
        Ok(Ok(info))
    }

    /// Renames `source` (the current version of `bucket`'s `from`, with its
    /// attributes `attrs`) to `to`, in this transaction (RenameObject): a
    /// version there with its body, attributes, part checksums and
    /// Last-Modified, replacing `to`'s null version, and `source` removed.
    #[allow(clippy::too_many_arguments)]
    pub fn rename_object(
        &mut self,
        bucket: &str,
        (from, source): (&str, &ObjectInfo),
        attrs: &ObjectAttrs,
        to: &str,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<ObjectInfo, ObjectError>, StorageError> {
        if let Err(err) = validate_key(to) {
            return Ok(Err(err));
        }
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        let sums = match source.parts > 0 {
            true => self.part_checksums_of((bucket, from, source.version))?,
            false => Vec::new(),
        };
        // Its body is the new version's too, before the source lets it go.
        self.add_ref(source.body)?;
        let version = next_counter(self, NEXT_VERSION_KEY)?;
        let renamed = self.insert_version_as(
            bucket,
            to,
            bucket_row.versioning,
            NewVersion {
                body: source.body,
                size: source.size,
                md5: source.md5,
                parts: source.parts,
                storage_class: source.storage_class,
            },
            attrs,
            Some((version, source.mtime_ms)),
            now_ms,
            grace_ms,
        )?;
        self.put_part_checksums((bucket, to, renamed.version), &sums)?;
        let which = match source.null_version {
            true => VersionSel::Null,
            false => VersionSel::Seq(source.version),
        };
        // The source was read in this lock hold: its removal fails only if
        // the storage does, and then nothing of the rename stays.
        if let Err(err) = self.delete_object(bucket, from, Some(which), false, now_ms, grace_ms)? {
            return Err(StorageError::InvalidFile(format!(
                "a renamed version could not be removed: {err:?}"
            )));
        }
        Ok(Ok(renamed))
    }

    /// Writes a version of `key` holding `version.body`'s one reference and,
    /// unless versioning is enabled, replaces the key's null version. A
    /// rule of the bucket's replication configuration that applies to it
    /// queues its replication (see `replication`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_version(
        &mut self,
        bucket: &str,
        key: &str,
        versioning: Versioning,
        new: NewVersion,
        attrs: &ObjectAttrs,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<ObjectInfo, StorageError> {
        let info =
            self.insert_version_as(bucket, key, versioning, new, attrs, None, now_ms, grace_ms)?;
        self.queue_replication(bucket, key, &info, now_ms)?;
        Ok(info)
    }

    /// [`Self::insert_version`], numbered and dated as `given` says (a
    /// replica: its source's number and time) or anew; queues nothing.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn insert_version_as(
        &mut self,
        bucket: &str,
        key: &str,
        versioning: Versioning,
        mut new: NewVersion,
        attrs: &ObjectAttrs,
        given: Option<(u64, u64)>,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<ObjectInfo, StorageError> {
        // A directory bucket's objects are EXPRESS_ONEZONE, however written.
        if self
            .bucket_row(bucket)?
            .is_some_and(|row| row.zone.is_some())
        {
            new.storage_class = StorageClass::ExpressOnezone;
        }
        let null_version = versioning != Versioning::Enabled;
        if null_version {
            // Replaced in place: the journal records the new version only.
            // A bucket with Object Lock keeps versioning enabled, so no
            // locked version is replaced here.
            if let Err(err) = self.remove_null_version(bucket, key, false, now_ms, grace_ms)? {
                return Err(StorageError::InvalidFile(format!(
                    "a write would replace a protected null version: {err:?}"
                )));
            }
        }
        let (version, mtime_ms) = match given {
            Some(given) => given,
            None => (next_counter(self, NEXT_VERSION_KEY)?, now_ms),
        };
        let has_attrs = !attrs.is_empty();
        let row = VersionRow {
            body: new.body,
            size: new.size,
            md5: new.md5,
            parts: new.parts,
            mtime_ms,
            flags: if null_version { FLAG_NULL_VERSION } else { 0 }
                | if has_attrs { FLAG_HAS_ATTRS } else { 0 }
                | new.storage_class.code() << CLASS_SHIFT,
        };
        let before = self.key_head(bucket, key)?;
        let objects = self.trees.objects();
        let inserted = objects.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &version_key(bucket, key, version),
            &row.encode(),
        )?;
        debug_assert_eq!(inserted, TreeInsert::Inserted);
        self.count_versions(bucket, key, before, &[], &[(version, row)])?;
        if has_attrs {
            let state = self.version_state_key(bucket, key, version)?;
            self.write_attrs(&state, attrs)?;
        }
        // A cold class's body is packed (one under a customer-provided key
        // is ciphertext: it would not shrink).
        if attrs.customer_key.is_none() {
            self.queue_pack(new.body, new.storage_class.pack_level())?;
        }
        self.record_change(
            Change {
                record_type: RecordType::Create,
                bucket,
                key,
                version,
                null_version,
                is_delete_marker: Some(false),
                object: Some(JournalObject {
                    size: new.size,
                    md5: new.md5,
                    parts: new.parts,
                    last_modified_ms: mtime_ms,
                    storage_class: new.storage_class,
                }),
            },
            now_ms,
        )?;
        Ok(ObjectInfo {
            version,
            null_version,
            delete_marker: false,
            body: new.body,
            size: new.size,
            md5: new.md5,
            parts: new.parts,
            mtime_ms,
            storage_class: new.storage_class,
            attrs: attrs.clone(),
        })
    }

    fn write_attrs(&mut self, state: &[u8], attrs: &ObjectAttrs) -> Result<(), StorageError> {
        let attrs_tree = self.trees.attrs();
        for (index, chunk) in attrs.encode().chunks(ATTRS_CHUNK).enumerate() {
            let inserted = attrs_tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &attrs_key(state, index as u16),
                chunk,
            )?;
            if inserted != TreeInsert::Inserted {
                return Err(StorageError::InvalidFile(
                    "a version's attributes are already stored".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn remove_attrs(&mut self, state: &[u8]) -> Result<(), StorageError> {
        let attrs_tree = self.trees.attrs();
        for (row_key, _) in attrs_tree.scan_range(
            self.ctx,
            Some(&attrs_key(state, 0)),
            Some(&attrs_key(state, u16::MAX)),
        )? {
            attrs_tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
        }
        Ok(())
    }

    /// Replaces the tags of the version `which` picks (S3 PutObjectTagging;
    /// an empty set is DeleteObjectTagging) and returns that version's number
    /// and whether it is the null version. A delete marker has no tags to set.
    pub fn set_tags(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
        tags: Vec<(String, String)>,
        now_ms: u64,
    ) -> Result<Result<(u64, bool), ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        if self.bucket_row(bucket)?.is_none() {
            return Ok(Err(ObjectError::NoSuchBucket));
        }
        let objects = self.trees.objects();
        let Some((row_key, version, mut row)) =
            find_version(self.ctx, &objects, bucket, key, which)?
        else {
            return Ok(Err(match which {
                VersionSel::Latest => ObjectError::NoSuchKey,
                _ => ObjectError::NoSuchVersion,
            }));
        };
        if row.flags & FLAG_DELETE_MARKER != 0 {
            return Ok(Err(match which {
                VersionSel::Latest => ObjectError::NoSuchKey,
                _ => ObjectError::DeleteMarker,
            }));
        }
        let state = self.version_state_key(bucket, key, version)?;
        let mut attrs = if row.flags & FLAG_HAS_ATTRS != 0 {
            let attrs = read_attrs(self.ctx, &self.trees.attrs(), &state)?;
            self.remove_attrs(&state)?;
            attrs
        } else {
            ObjectAttrs::default()
        };
        attrs.tags = tags;
        row.flags &= !FLAG_HAS_ATTRS;
        if !attrs.is_empty() {
            self.write_attrs(&state, &attrs)?;
            row.flags |= FLAG_HAS_ATTRS;
        }
        objects.update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &row_key,
            &row.encode(),
        )?;
        let null_version = row.flags & FLAG_NULL_VERSION != 0;
        self.record_change(
            Change {
                record_type: RecordType::UpdateMetadata,
                bucket,
                key,
                version,
                null_version,
                is_delete_marker: Some(false),
                object: Some(JournalObject {
                    size: row.size,
                    md5: row.md5,
                    parts: row.parts,
                    last_modified_ms: row.mtime_ms,
                    storage_class: class_of(row.flags),
                }),
            },
            now_ms,
        )?;
        self.queue_metadata_replication(bucket, key, version, &attrs.tags, now_ms)?;
        Ok(Ok((version, null_version)))
    }

    /// Moves a version to a colder storage class (an S3 Lifecycle
    /// transition): its body is queued for packing at that class's level,
    /// and the journal records the change. A version gone meanwhile, or a
    /// delete marker, is left alone (`NoSuchVersion`).
    pub fn transition_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        class: StorageClass,
        now_ms: u64,
    ) -> Result<Result<(), ObjectError>, StorageError> {
        let objects = self.trees.objects();
        let row_key = version_key(bucket, key, version);
        let Some(row) = objects.get(self.ctx, &row_key)? else {
            return Ok(Err(ObjectError::NoSuchVersion));
        };
        let mut row = VersionRow::decode(&row)?;
        if row.flags & FLAG_DELETE_MARKER != 0 {
            return Ok(Err(ObjectError::NoSuchVersion));
        }
        let from = class_of(row.flags);
        row.flags = row.flags & !(15 << CLASS_SHIFT) | class.code() << CLASS_SHIFT;
        objects.update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &row_key,
            &row.encode(),
        )?;
        self.count_transition(bucket, key, version, &row, from, class)?;
        // A body under a customer-provided key is ciphertext: it would not
        // shrink.
        let state = self.version_state_key(bucket, key, version)?;
        let sealed_by_customer = row.flags & FLAG_HAS_ATTRS != 0
            && read_attrs(self.ctx, &self.trees.attrs(), &state)?
                .customer_key
                .is_some();
        if !sealed_by_customer {
            self.queue_pack(row.body, class.pack_level())?;
        }
        self.record_change(
            Change {
                record_type: RecordType::UpdateMetadata,
                bucket,
                key,
                version,
                null_version: row.flags & FLAG_NULL_VERSION != 0,
                is_delete_marker: Some(false),
                object: Some(JournalObject {
                    size: row.size,
                    md5: row.md5,
                    parts: row.parts,
                    last_modified_ms: row.mtime_ms,
                    storage_class: class,
                }),
            },
            now_ms,
        )?;
        Ok(Ok(()))
    }

    /// The storage class of version `version` of `bucket`'s `key`, if it is
    /// there.
    pub(crate) fn class_of_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Option<StorageClass>, StorageError> {
        self.trees
            .objects()
            .get(self.ctx, &version_key(bucket, key, version))?
            .map(|row| Ok(class_of(VersionRow::decode(&row)?.flags)))
            .transpose()
    }

    /// Records a notice of `event` of version `version` of `bucket`'s `key`
    /// (with `detail`, see [`Notice`]); nothing when the version is gone.
    pub(crate) fn note_event(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        event: &str,
        detail: Option<String>,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        let Some(row) = self
            .trees
            .objects()
            .get(self.ctx, &version_key(bucket, key, version))?
        else {
            return Ok(());
        };
        let row = VersionRow::decode(&row)?;
        if row.flags & FLAG_DELETE_MARKER != 0 {
            return Ok(());
        }
        self.record_notice(
            bucket,
            key,
            version,
            row.flags & FLAG_NULL_VERSION != 0,
            Some(JournalObject {
                size: row.size,
                md5: row.md5,
                parts: row.parts,
                last_modified_ms: row.mtime_ms,
                storage_class: class_of(row.flags),
            }),
            Notice {
                event: event.to_string(),
                detail,
            },
            now_ms,
        )
    }

    /// Removes the key's versions that `wanted` picks: each row, its
    /// attributes, its lock, its restore and its body's reference. Returns the removed
    /// versions' numbers, or `Locked` (removing nothing) when Object Lock
    /// protects one of them; `bypass` lifts GOVERNANCE retention. With
    /// `only`, reads that version's row alone instead of every version of
    /// the key. With `journal`, each removal is recorded as a permanent
    /// delete.
    #[allow(clippy::too_many_arguments)]
    fn remove_versions(
        &mut self,
        bucket: &str,
        key: &str,
        only: Option<u64>,
        journal: bool,
        bypass: bool,
        now_ms: u64,
        grace_ms: u64,
        mut wanted: impl FnMut(u64, &VersionRow) -> bool,
    ) -> Result<Result<Vec<u64>, ObjectError>, StorageError> {
        let objects = self.trees.objects();
        let (lower, upper) = match only {
            Some(version) => {
                let row_key = version_key(bucket, key, version);
                (row_key.clone(), row_key)
            }
            None => {
                let prefix = key_prefix(bucket, key);
                let mut upper = prefix.clone();
                upper.extend_from_slice(&[0xFF; 8]);
                (prefix, upper)
            }
        };
        let mut chosen = Vec::new();
        for (row_key, row) in objects.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            let row = VersionRow::decode(&row)?;
            let version = version_from_key(&row_key)?;
            if !wanted(version, &row) {
                continue;
            }
            // A delete marker has no lock.
            if row.flags & FLAG_DELETE_MARKER == 0
                && self
                    .lock_of(bucket, key, version)?
                    .blocks_delete(now_ms, bypass)
            {
                return Ok(Err(ObjectError::Locked));
            }
            chosen.push((row_key, version, row));
        }
        let before = if chosen.is_empty() {
            None
        } else {
            self.key_head(bucket, key)?
        };
        let mut removed = Vec::new();
        let mut removed_rows = Vec::new();
        for (row_key, version, row) in chosen {
            objects.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
            removed_rows.push((version, row.clone()));
            self.set_lock(bucket, key, version, &crate::ObjectLock::default())?;
            self.forget_restore(bucket, key, version)?;
            self.forget_replication_status(bucket, key, version)?;
            self.forget_part_checksums(bucket, key, version)?;
            if row.flags & FLAG_HAS_ATTRS != 0 {
                let state = self.version_state_key(bucket, key, version)?;
                self.remove_attrs(&state)?;
            }
            if row.flags & FLAG_DELETE_MARKER == 0 {
                self.drop_ref(row.body, now_ms, grace_ms)?;
            }
            if journal {
                self.record_change(
                    Change {
                        record_type: RecordType::Delete,
                        bucket,
                        key,
                        version,
                        null_version: row.flags & FLAG_NULL_VERSION != 0,
                        is_delete_marker: None,
                        object: None,
                    },
                    now_ms,
                )?;
            }
            removed.push(version);
        }
        if !removed_rows.is_empty() {
            self.count_versions(bucket, key, before, &removed_rows, &[])?;
        }
        Ok(Ok(removed))
    }

    /// Removes the key's null version, if any (unless Object Lock protects
    /// it), recording it in the journal when `journal` (not when a new null
    /// version or marker replaces it).
    fn remove_null_version(
        &mut self,
        bucket: &str,
        key: &str,
        journal: bool,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<(), ObjectError>, StorageError> {
        Ok(self
            .remove_versions(
                bucket,
                key,
                None,
                journal,
                false,
                now_ms,
                grace_ms,
                |_, row| row.flags & FLAG_NULL_VERSION != 0,
            )?
            .map(|_| ()))
    }

    /// Adds a delete marker as the key's newest version; or, `given` (a
    /// replica's), of that version number and time.
    pub(crate) fn add_delete_marker(
        &mut self,
        bucket: &str,
        key: &str,
        null_version: bool,
        given: Option<(u64, u64)>,
        now_ms: u64,
    ) -> Result<u64, StorageError> {
        let (version, mtime_ms) = match given {
            Some(given) => given,
            None => (next_counter(self, NEXT_VERSION_KEY)?, now_ms),
        };
        let row = VersionRow {
            body: 0,
            size: 0,
            md5: [0; 16],
            parts: 0,
            mtime_ms,
            flags: FLAG_DELETE_MARKER | if null_version { FLAG_NULL_VERSION } else { 0 },
        };
        let before = self.key_head(bucket, key)?;
        let objects = self.trees.objects();
        objects.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &version_key(bucket, key, version),
            &row.encode(),
        )?;
        self.count_versions(bucket, key, before, &[], &[(version, row)])?;
        self.record_change(
            Change {
                record_type: RecordType::Delete,
                bucket,
                key,
                version,
                null_version,
                is_delete_marker: Some(true),
                object: Some(JournalObject {
                    size: 0,
                    md5: [0; 16],
                    parts: 0,
                    last_modified_ms: mtime_ms,
                    storage_class: StorageClass::Standard,
                }),
            },
            now_ms,
        )?;
        Ok(version)
    }

    /// Deletes `key` (S3 DeleteObject). With a version, removes that version
    /// for good. Without one: an unversioned bucket removes the key; a
    /// versioned bucket adds a delete marker; a suspended bucket replaces the
    /// null version with a null delete marker. Deleting what is not there
    /// succeeds, as in S3. A version Object Lock protects is not removed
    /// (`Locked`); `bypass` lifts GOVERNANCE retention.
    pub fn delete_object(
        &mut self,
        bucket: &str,
        key: &str,
        which: Option<VersionSel>,
        bypass: bool,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<Deleted, ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        Ok(Ok(match which {
            Some(VersionSel::Latest) | None => match bucket_row.versioning {
                Versioning::Unversioned => {
                    if let Err(err) =
                        self.remove_null_version(bucket, key, true, now_ms, grace_ms)?
                    {
                        return Ok(Err(err));
                    }
                    Deleted::default()
                }
                Versioning::Enabled => Deleted {
                    version: Some(self.add_delete_marker(bucket, key, false, None, now_ms)?),
                    null_version: false,
                    delete_marker: true,
                },
                Versioning::Suspended => {
                    // Replaced by a null marker, which the journal records.
                    if let Err(err) =
                        self.remove_null_version(bucket, key, false, now_ms, grace_ms)?
                    {
                        return Ok(Err(err));
                    }
                    Deleted {
                        version: Some(self.add_delete_marker(bucket, key, true, None, now_ms)?),
                        null_version: true,
                        delete_marker: true,
                    }
                }
            },
            Some(sel) => {
                let only = match sel {
                    VersionSel::Seq(seq) => Some(seq),
                    _ => None,
                };
                let mut marker = false;
                let removed = match self.remove_versions(
                    bucket,
                    key,
                    only,
                    true,
                    bypass,
                    now_ms,
                    grace_ms,
                    |version, row| {
                        let hit = match sel {
                            VersionSel::Null => row.flags & FLAG_NULL_VERSION != 0,
                            VersionSel::Seq(seq) => seq == version,
                            VersionSel::Latest => unreachable!("handled above"),
                        };
                        if hit {
                            marker = row.flags & FLAG_DELETE_MARKER != 0;
                        }
                        hit
                    },
                )? {
                    Ok(removed) => removed,
                    Err(err) => return Ok(Err(err)),
                };
                Deleted {
                    version: removed.first().copied(),
                    null_version: sel == VersionSel::Null,
                    delete_marker: marker && !removed.is_empty(),
                }
            }
        }))
    }
}

impl ObjFile<'_> {
    /// The attributes of version `version` of `bucket`'s `key` (none when
    /// it has none, or is gone).
    pub fn version_attrs(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<ObjectAttrs, StorageError> {
        let state = self.version_state_key(bucket, key, version)?;
        match self.existing_tree(ATTRS_TREE)? {
            Some(attrs_tree) => read_attrs(&mut self.core.page_ctx(0), &attrs_tree, &state),
            None => Ok(ObjectAttrs::default()),
        }
    }

    /// A version of `key` and its attributes; `NoSuchKey` when the key's
    /// newest version is a delete marker.
    pub fn head_object(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
    ) -> Result<Result<ObjectInfo, ObjectError>, StorageError> {
        Ok(match self.object_version(bucket, key, which)? {
            Ok(info) if which == VersionSel::Latest && info.delete_marker => {
                Err(ObjectError::NoSuchKey)
            }
            found => found,
        })
    }

    /// [`Self::head_object`], but a newest version that is a delete marker
    /// is returned as one (S3 GET and HEAD report it).
    pub fn object_version(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
    ) -> Result<Result<ObjectInfo, ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        let Some(buckets) = self.existing_tree(BUCKETS_TREE)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        if buckets
            .get(&mut self.core.page_ctx(0), bucket.as_bytes())?
            .is_none()
        {
            return Ok(Err(ObjectError::NoSuchBucket));
        }
        let (Some(objects), attrs_tree) = (
            self.existing_tree(OBJECTS_TREE)?,
            self.existing_tree(ATTRS_TREE)?,
        ) else {
            return Ok(Err(ObjectError::NoSuchKey));
        };
        let keyed = self.keyed(bucket, key)?;
        let mut ctx = self.core.page_ctx(0);
        let Some((_, version, row)) = find_version(&mut ctx, &objects, bucket, key, which)? else {
            return Ok(Err(match which {
                VersionSel::Latest => ObjectError::NoSuchKey,
                _ => ObjectError::NoSuchVersion,
            }));
        };
        let attrs = match (row.flags & FLAG_HAS_ATTRS != 0, attrs_tree) {
            (true, Some(attrs_tree)) => read_attrs(
                &mut ctx,
                &attrs_tree,
                &crate::version_state::state_key(keyed, bucket, key, version),
            )?,
            _ => ObjectAttrs::default(),
        };
        Ok(Ok(ObjectInfo {
            version,
            null_version: row.flags & FLAG_NULL_VERSION != 0,
            delete_marker: row.flags & FLAG_DELETE_MARKER != 0,
            body: row.body,
            size: row.size,
            md5: row.md5,
            parts: row.parts,
            mtime_ms: row.mtime_ms,
            storage_class: class_of(row.flags),
            attrs,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_round_trip() {
        let attrs = ObjectAttrs {
            content_type: Some("text/plain".to_string()),
            cache_control: None,
            content_disposition: Some("attachment".to_string()),
            content_encoding: None,
            content_language: Some("sv".to_string()),
            expires: None,
            user: vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "two".to_string()),
            ],
            tags: vec![("project".to_string(), "blue sky".to_string())],
            website_redirect: Some("/moved.html".to_string()),
            customer_key: Some(CustomerKey {
                key_md5: "kMd5==".to_string(),
                verifier: "salt+mac".to_string(),
            }),
            checksum: Some(StoredChecksum {
                algorithm: "CRC64NVME".to_string(),
                full_object: true,
                value: "AAAAAAAAAAA=".to_string(),
            }),
        };
        assert_eq!(ObjectAttrs::decode(&attrs.encode()).unwrap(), attrs);
        assert!(ObjectAttrs::default().is_empty());
        // A composite checksum's kind; a checksum field of a bad shape.
        let composite = ObjectAttrs {
            checksum: Some(StoredChecksum {
                algorithm: "SHA256".to_string(),
                full_object: false,
                value: "x/y+=".to_string(),
            }),
            ..ObjectAttrs::default()
        };
        assert_eq!(ObjectAttrs::decode(&composite.encode()).unwrap(), composite);
        for bad in ["CRC32", "CRC32 PARTLY x"] {
            let mut bytes = vec![14];
            bytes.extend_from_slice(&(bad.len() as u16).to_le_bytes());
            bytes.extend_from_slice(bad.as_bytes());
            assert!(ObjectAttrs::decode(&bytes).is_err(), "{bad}");
        }
    }

    #[test]
    fn version_keys_sort_newest_first_and_keys_in_utf8_order() {
        assert!(version_key("b", "k", 9) < version_key("b", "k", 2));
        assert!(version_key("b", "a", 1) < version_key("b", "a/b", 1));
        assert!(version_key("b", "a/b", 1) < version_key("b", "ab", 1));
        assert_eq!(version_from_key(&version_key("b", "k", 77)).unwrap(), 77);
    }
}

/// A version row as listed (no attributes: a listing does not return them).
pub(crate) fn decode_version(version: u64, row: &[u8]) -> Result<ObjectInfo, StorageError> {
    let row = VersionRow::decode(row)?;
    Ok(ObjectInfo {
        version,
        null_version: row.flags & FLAG_NULL_VERSION != 0,
        delete_marker: row.flags & FLAG_DELETE_MARKER != 0,
        body: row.body,
        size: row.size,
        md5: row.md5,
        parts: row.parts,
        mtime_ms: row.mtime_ms,
        storage_class: class_of(row.flags),
        attrs: ObjectAttrs::default(),
    })
}
