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
fn key_prefix(bucket: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len() + key.len() + 2);
    out.extend_from_slice(bucket.as_bytes());
    out.push(0);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out
}

fn version_key(bucket: &str, key: &str, version: u64) -> Vec<u8> {
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

const FLAG_DELETE_MARKER: u8 = 1;
const FLAG_NULL_VERSION: u8 = 2;
const FLAG_HAS_ATTRS: u8 = 4;

/// A version's stored row.
struct VersionRow {
    body: u64,
    size: u64,
    md5: [u8; 16],
    parts: u16,
    mtime_ms: u64,
    flags: u8,
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

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
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

fn attrs_key(version: u64, chunk: u16) -> [u8; 10] {
    let mut key = [0u8; 10];
    key[..8].copy_from_slice(&version.to_be_bytes());
    key[8..].copy_from_slice(&chunk.to_be_bytes());
    key
}

/// The version of `key` that `which` picks (a delete marker included), as
/// its row key, number and row.
fn find_version(
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
    version: u64,
) -> Result<ObjectAttrs, StorageError> {
    let mut bytes = Vec::new();
    for (_, chunk) in attrs_tree.scan_range(
        ctx,
        Some(&attrs_key(version, 0)),
        Some(&attrs_key(version, u16::MAX)),
    )? {
        bytes.extend_from_slice(&chunk);
    }
    ObjectAttrs::decode(&bytes)
}

impl ObjTxn<'_, '_> {
    pub(crate) fn bucket_row(&mut self, bucket: &str) -> Result<Option<BucketRow>, StorageError> {
        self.trees
            .buckets()
            .get(self.ctx, bucket.as_bytes())?
            .map(|row| BucketRow::decode(&row))
            .transpose()
    }

    /// Stores `body` as the newest version of `key`: seals it (the version
    /// holds its one reference) and, unless versioning is enabled, replaces
    /// the key's null version (dropping its body's reference, collected after
    /// `grace_ms`).
    pub fn put_object(
        &mut self,
        bucket: &str,
        key: &str,
        body: &SealedBody,
        attrs: &ObjectAttrs,
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
                size: sealed.len,
                md5: sealed.md5,
                parts: 0,
            },
            attrs,
            now_ms,
            grace_ms,
        )?;
        Ok(Ok(info))
    }

    /// Writes `source` as the newest version of `key` (S3 CopyObject), with
    /// `attrs`: the new version shares the source's body (one more
    /// reference), so no bytes are copied. `source` must be a version read in
    /// the same lock hold, and not a delete marker.
    pub fn copy_object(
        &mut self,
        bucket: &str,
        key: &str,
        source: &ObjectInfo,
        attrs: &ObjectAttrs,
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
            },
            attrs,
            now_ms,
            grace_ms,
        )?;
        Ok(Ok(info))
    }

    /// Writes a version of `key` holding `version.body`'s one reference and,
    /// unless versioning is enabled, replaces the key's null version.
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
        let null_version = versioning != Versioning::Enabled;
        if null_version {
            self.remove_null_version(bucket, key, now_ms, grace_ms)?;
        }
        let version = next_counter(self, NEXT_VERSION_KEY)?;
        let has_attrs = !attrs.is_empty();
        let row = VersionRow {
            body: new.body,
            size: new.size,
            md5: new.md5,
            parts: new.parts,
            mtime_ms: now_ms,
            flags: if null_version { FLAG_NULL_VERSION } else { 0 }
                | if has_attrs { FLAG_HAS_ATTRS } else { 0 },
        };
        let objects = self.trees.objects();
        let inserted = objects.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &version_key(bucket, key, version),
            &row.encode(),
        )?;
        debug_assert_eq!(inserted, TreeInsert::Inserted);
        if has_attrs {
            self.write_attrs(version, attrs)?;
        }
        Ok(ObjectInfo {
            version,
            null_version,
            delete_marker: false,
            body: new.body,
            size: new.size,
            md5: new.md5,
            parts: new.parts,
            mtime_ms: now_ms,
            attrs: attrs.clone(),
        })
    }

    fn write_attrs(&mut self, version: u64, attrs: &ObjectAttrs) -> Result<(), StorageError> {
        let attrs_tree = self.trees.attrs();
        for (index, chunk) in attrs.encode().chunks(ATTRS_CHUNK).enumerate() {
            attrs_tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &attrs_key(version, index as u16),
                chunk,
            )?;
        }
        Ok(())
    }

    fn remove_attrs(&mut self, version: u64) -> Result<(), StorageError> {
        let attrs_tree = self.trees.attrs();
        for (row_key, _) in attrs_tree.scan_range(
            self.ctx,
            Some(&attrs_key(version, 0)),
            Some(&attrs_key(version, u16::MAX)),
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
        let mut attrs = if row.flags & FLAG_HAS_ATTRS != 0 {
            let attrs = read_attrs(self.ctx, &self.trees.attrs(), version)?;
            self.remove_attrs(version)?;
            attrs
        } else {
            ObjectAttrs::default()
        };
        attrs.tags = tags;
        row.flags &= !FLAG_HAS_ATTRS;
        if !attrs.is_empty() {
            self.write_attrs(version, &attrs)?;
            row.flags |= FLAG_HAS_ATTRS;
        }
        objects.update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &row_key,
            &row.encode(),
        )?;
        Ok(Ok((version, row.flags & FLAG_NULL_VERSION != 0)))
    }

    /// Removes the key's versions that `wanted` picks: each row, its
    /// attributes and its body's reference. Returns the removed versions'
    /// numbers. With `only`, reads that version's row alone instead of every
    /// version of the key.
    fn remove_versions(
        &mut self,
        bucket: &str,
        key: &str,
        only: Option<u64>,
        now_ms: u64,
        grace_ms: u64,
        mut wanted: impl FnMut(u64, &VersionRow) -> bool,
    ) -> Result<Vec<u64>, StorageError> {
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
        let mut removed = Vec::new();
        for (row_key, row) in objects.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            let row = VersionRow::decode(&row)?;
            let version = version_from_key(&row_key)?;
            if !wanted(version, &row) {
                continue;
            }
            objects.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
            if row.flags & FLAG_HAS_ATTRS != 0 {
                self.remove_attrs(version)?;
            }
            if row.flags & FLAG_DELETE_MARKER == 0 {
                self.drop_ref(row.body, now_ms, grace_ms)?;
            }
            removed.push(version);
        }
        Ok(removed)
    }

    /// Removes the key's null version, if any.
    fn remove_null_version(
        &mut self,
        bucket: &str,
        key: &str,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<(), StorageError> {
        self.remove_versions(bucket, key, None, now_ms, grace_ms, |_, row| {
            row.flags & FLAG_NULL_VERSION != 0
        })?;
        Ok(())
    }

    /// Adds a delete marker as the key's newest version.
    fn add_delete_marker(
        &mut self,
        bucket: &str,
        key: &str,
        null_version: bool,
        now_ms: u64,
    ) -> Result<u64, StorageError> {
        let version = next_counter(self, NEXT_VERSION_KEY)?;
        let row = VersionRow {
            body: 0,
            size: 0,
            md5: [0; 16],
            parts: 0,
            mtime_ms: now_ms,
            flags: FLAG_DELETE_MARKER | if null_version { FLAG_NULL_VERSION } else { 0 },
        };
        let objects = self.trees.objects();
        objects.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &version_key(bucket, key, version),
            &row.encode(),
        )?;
        Ok(version)
    }

    /// Deletes `key` (S3 DeleteObject). With a version, removes that version
    /// for good. Without one: an unversioned bucket removes the key; a
    /// versioned bucket adds a delete marker; a suspended bucket replaces the
    /// null version with a null delete marker. Deleting what is not there
    /// succeeds, as in S3.
    pub fn delete_object(
        &mut self,
        bucket: &str,
        key: &str,
        which: Option<VersionSel>,
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
                    self.remove_null_version(bucket, key, now_ms, grace_ms)?;
                    Deleted::default()
                }
                Versioning::Enabled => Deleted {
                    version: Some(self.add_delete_marker(bucket, key, false, now_ms)?),
                    null_version: false,
                    delete_marker: true,
                },
                Versioning::Suspended => {
                    self.remove_null_version(bucket, key, now_ms, grace_ms)?;
                    Deleted {
                        version: Some(self.add_delete_marker(bucket, key, true, now_ms)?),
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
                let removed =
                    self.remove_versions(bucket, key, only, now_ms, grace_ms, |version, row| {
                        let hit = match sel {
                            VersionSel::Null => row.flags & FLAG_NULL_VERSION != 0,
                            VersionSel::Seq(seq) => seq == version,
                            VersionSel::Latest => unreachable!("handled above"),
                        };
                        if hit {
                            marker = row.flags & FLAG_DELETE_MARKER != 0;
                        }
                        hit
                    })?;
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
    /// The attributes of version `version` (none when it has none, or is gone).
    pub fn version_attrs(&mut self, version: u64) -> Result<ObjectAttrs, StorageError> {
        match self.existing_tree(ATTRS_TREE)? {
            Some(attrs_tree) => read_attrs(&mut self.core.page_ctx(0), &attrs_tree, version),
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
        let mut ctx = self.core.page_ctx(0);
        let Some((_, version, row)) = find_version(&mut ctx, &objects, bucket, key, which)? else {
            return Ok(Err(match which {
                VersionSel::Latest => ObjectError::NoSuchKey,
                _ => ObjectError::NoSuchVersion,
            }));
        };
        let attrs = match (row.flags & FLAG_HAS_ATTRS != 0, attrs_tree) {
            (true, Some(attrs_tree)) => read_attrs(&mut ctx, &attrs_tree, version)?,
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
        };
        assert_eq!(ObjectAttrs::decode(&attrs.encode()).unwrap(), attrs);
        assert!(ObjectAttrs::default().is_empty());
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
        attrs: ObjectAttrs::default(),
    })
}
