//! Multipart uploads: an upload is created for a bucket and key, takes parts
//! (each a sealed body, numbered 1..=10000, replaceable), and is completed
//! into one object whose body is a composite of the listed parts in order
//! (no bytes are copied) or aborted (its parts released). An unfinished
//! upload stays until it is completed or aborted, or until the engine's
//! garbage collection aborts it after 7 days (as an S3 lifecycle rule
//! would).

use md5::{Digest, Md5};
use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::body::{BodyInfo, SealedBody};
use crate::objects::{NewVersion, validate_key};
use crate::{
    NEXT_UPLOAD_KEY, ObjFile, ObjTxn, ObjectAttrs, ObjectError, ObjectInfo, PARTS_TREE,
    UPLOAD_INDEX_TREE, UPLOADS_TREE, next_counter,
};

/// Largest object a multipart upload completes into (S3's limit).
pub const MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// Highest part number (S3's limit).
pub const MAX_PART_NUMBER: u32 = 10_000;
/// Smallest part but the last (S3's limit).
pub const MIN_PART_BYTES: u64 = 5 * 1024 * 1024;
/// Attribute bytes per upload-attrs row.
const ATTRS_CHUNK: usize = 1500;

/// Why a multipart operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadError {
    Object(ObjectError),
    NoSuchUpload,
    /// A part number outside 1..=10000.
    InvalidPartNumber,
    /// A listed part was not uploaded, or its ETag does not match.
    InvalidPart(u32),
    /// The listed parts are not in ascending order.
    InvalidPartOrder,
    /// A part other than the last is under 5 MiB.
    EntityTooSmall(u32),
    /// The parts add up to more than 5 TiB.
    EntityTooLarge,
}

/// An upload as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadInfo {
    pub upload: u64,
    pub bucket: String,
    pub key: String,
    pub initiated_ms: u64,
}

/// A part as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartInfo {
    pub number: u32,
    pub body: u64,
    pub size: u64,
    pub md5: [u8; 16],
    pub mtime_ms: u64,
}

impl PartInfo {
    pub fn etag(&self) -> String {
        let hex: String = self.md5.iter().map(|b| format!("{b:02x}")).collect();
        format!("\"{hex}\"")
    }
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt {what}"))
}

/// An upload's row: its bucket and key and when it started.
fn encode_upload(bucket: &str, key: &str, initiated_ms: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(11 + bucket.len() + key.len());
    out.push(bucket.len() as u8);
    out.extend_from_slice(bucket.as_bytes());
    out.extend_from_slice(&(key.len() as u16).to_le_bytes());
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(&initiated_ms.to_le_bytes());
    out
}

fn decode_upload(upload: u64, row: &[u8]) -> Result<UploadInfo, StorageError> {
    let bucket_len = *row.first().ok_or_else(|| corrupt("upload row"))? as usize;
    let bucket = row
        .get(1..1 + bucket_len)
        .ok_or_else(|| corrupt("upload row"))?;
    let at = 1 + bucket_len;
    let key_len = u16::from_le_bytes(
        row.get(at..at + 2)
            .ok_or_else(|| corrupt("upload row"))?
            .try_into()
            .unwrap(),
    ) as usize;
    let key = row
        .get(at + 2..at + 2 + key_len)
        .ok_or_else(|| corrupt("upload row"))?;
    let at = at + 2 + key_len;
    let initiated_ms = u64::from_le_bytes(
        row.get(at..at + 8)
            .ok_or_else(|| corrupt("upload row"))?
            .try_into()
            .unwrap(),
    );
    Ok(UploadInfo {
        upload,
        bucket: String::from_utf8(bucket.to_vec()).map_err(|_| corrupt("upload bucket"))?,
        key: String::from_utf8(key.to_vec()).map_err(|_| corrupt("upload key"))?,
        initiated_ms,
    })
}

fn encode_part(part: &PartInfo) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    out.extend_from_slice(&part.body.to_le_bytes());
    out.extend_from_slice(&part.size.to_le_bytes());
    out.extend_from_slice(&part.md5);
    out.extend_from_slice(&part.mtime_ms.to_le_bytes());
    out
}

fn decode_part(number: u32, row: &[u8]) -> Result<PartInfo, StorageError> {
    if row.len() != 40 {
        return Err(corrupt("part row"));
    }
    let u64_at = |at: usize| u64::from_le_bytes(row[at..at + 8].try_into().unwrap());
    Ok(PartInfo {
        number,
        body: u64_at(0),
        size: u64_at(8),
        md5: row[16..32].try_into().unwrap(),
        mtime_ms: u64_at(32),
    })
}

fn part_key(upload: u64, number: u32) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..8].copy_from_slice(&upload.to_be_bytes());
    key[8..].copy_from_slice(&number.to_be_bytes());
    key
}

/// `bucket, 0, key, 0, upload`: the upload index's order is S3's
/// ListMultipartUploads order (key, then initiation).
fn index_key(bucket: &str, key: &str, upload: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len() + key.len() + 10);
    out.extend_from_slice(bucket.as_bytes());
    out.push(0);
    out.extend_from_slice(key.as_bytes());
    out.push(0);
    out.extend_from_slice(&upload.to_be_bytes());
    out
}

fn attrs_key(upload: u64, chunk: u16) -> [u8; 11] {
    // A leading 1 keeps upload attributes apart from versions' (whose keys
    // are 10 bytes and start with a version number's high byte, 0).
    let mut key = [0u8; 11];
    key[0] = 1;
    key[1..9].copy_from_slice(&upload.to_be_bytes());
    key[9..].copy_from_slice(&chunk.to_be_bytes());
    key
}

impl ObjTxn<'_, '_> {
    fn upload_row(&mut self, upload: u64) -> Result<Option<UploadInfo>, StorageError> {
        let uploads = self.trees.uploads();
        uploads
            .get(self.ctx, &upload.to_be_bytes())?
            .map(|row| decode_upload(upload, &row))
            .transpose()
    }

    fn upload_attrs(&mut self, upload: u64) -> Result<ObjectAttrs, StorageError> {
        let attrs_tree = self.trees.attrs();
        let mut bytes = Vec::new();
        for (_, chunk) in attrs_tree.scan_range(
            self.ctx,
            Some(&attrs_key(upload, 0)),
            Some(&attrs_key(upload, u16::MAX)),
        )? {
            bytes.extend_from_slice(&chunk);
        }
        ObjectAttrs::decode(&bytes)
    }

    /// Starts an upload of `key`; `attrs` become the object's on completion.
    pub fn create_upload(
        &mut self,
        bucket: &str,
        key: &str,
        attrs: &ObjectAttrs,
        now_ms: u64,
    ) -> Result<Result<u64, ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        if self.bucket_row(bucket)?.is_none() {
            return Ok(Err(ObjectError::NoSuchBucket));
        }
        let upload = next_counter(self, NEXT_UPLOAD_KEY)?;
        let (uploads, index, attrs_tree) = (
            self.trees.uploads(),
            self.trees.upload_index(),
            self.trees.attrs(),
        );
        uploads.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &upload.to_be_bytes(),
            &encode_upload(bucket, key, now_ms),
        )?;
        index.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &index_key(bucket, key, upload),
            &[],
        )?;
        for (chunk, bytes) in attrs.encode().chunks(ATTRS_CHUNK).enumerate() {
            attrs_tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &attrs_key(upload, chunk as u16),
                bytes,
            )?;
        }
        Ok(Ok(upload))
    }

    /// Stores `body` as part `number` of `upload` (sealing it: the part holds
    /// its one reference), replacing an earlier part with that number.
    pub fn upload_part(
        &mut self,
        upload: u64,
        number: u32,
        body: &SealedBody,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<BodyInfo, UploadError>, StorageError> {
        if !(1..=MAX_PART_NUMBER).contains(&number) {
            return Ok(Err(UploadError::InvalidPartNumber));
        }
        if self.upload_row(upload)?.is_none() {
            return Ok(Err(UploadError::NoSuchUpload));
        }
        let sealed = body.seal_in(self)?;
        let part = PartInfo {
            number,
            body: sealed.body,
            size: sealed.len,
            md5: sealed.md5,
            mtime_ms: now_ms,
        };
        let parts = self.trees.parts();
        let key = part_key(upload, number);
        match parts.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key,
            &encode_part(&part),
        )? {
            TreeInsert::Inserted => {}
            TreeInsert::DuplicateKey => {
                let old = parts
                    .update(
                        self.ctx,
                        &mut OpMode::Txn(self.txn),
                        &key,
                        &encode_part(&part),
                    )?
                    .ok_or_else(|| corrupt("part row"))?;
                let old = decode_part(number, &old)?;
                self.drop_ref(old.body, now_ms, grace_ms)?;
            }
        }
        Ok(Ok(sealed))
    }

    /// Removes the upload, its index entry, its attributes and its part rows,
    /// returning the parts (their references not yet released).
    fn remove_upload(&mut self, info: &UploadInfo) -> Result<Vec<PartInfo>, StorageError> {
        let upload = info.upload;
        let (uploads, index, parts, attrs_tree) = (
            self.trees.uploads(),
            self.trees.upload_index(),
            self.trees.parts(),
            self.trees.attrs(),
        );
        let rows = parts.scan_range(
            self.ctx,
            Some(&part_key(upload, 0)),
            Some(&part_key(upload, u32::MAX)),
        )?;
        let mut removed = Vec::with_capacity(rows.len());
        for (key, row) in rows {
            let number = u32::from_be_bytes(key[8..].try_into().unwrap());
            removed.push(decode_part(number, &row)?);
            parts.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        for (key, _) in attrs_tree.scan_range(
            self.ctx,
            Some(&attrs_key(upload, 0)),
            Some(&attrs_key(upload, u16::MAX)),
        )? {
            attrs_tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        index.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &index_key(&info.bucket, &info.key, upload),
        )?;
        uploads.delete(self.ctx, &mut OpMode::Txn(self.txn), &upload.to_be_bytes())?;
        Ok(removed)
    }

    /// Aborts the upload, releasing every part.
    pub fn abort_upload(
        &mut self,
        upload: u64,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<(), UploadError>, StorageError> {
        let Some(info) = self.upload_row(upload)? else {
            return Ok(Err(UploadError::NoSuchUpload));
        };
        for part in self.remove_upload(&info)? {
            self.drop_ref(part.body, now_ms, grace_ms)?;
        }
        Ok(Ok(()))
    }

    /// Completes the upload from `listed` parts (`(number, MD5)`, ascending)
    /// into the newest version of its key, whose body is a composite of the
    /// parts and whose ETag is the MD5 of their MD5s with the part count.
    /// Uploaded parts that are not listed are released.
    pub fn complete_upload(
        &mut self,
        upload: u64,
        listed: &[(u32, [u8; 16])],
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<ObjectInfo, UploadError>, StorageError> {
        let Some(info) = self.upload_row(upload)? else {
            return Ok(Err(UploadError::NoSuchUpload));
        };
        let Some(bucket_row) = self.bucket_row(&info.bucket)? else {
            return Ok(Err(UploadError::Object(ObjectError::NoSuchBucket)));
        };
        if listed.is_empty() {
            return Ok(Err(UploadError::InvalidPart(0)));
        }
        if listed.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Ok(Err(UploadError::InvalidPartOrder));
        }
        let parts = self.trees.parts();
        let mut chosen = Vec::with_capacity(listed.len());
        for (index, (number, md5)) in listed.iter().enumerate() {
            let Some(row) = parts.get(self.ctx, &part_key(upload, *number))? else {
                return Ok(Err(UploadError::InvalidPart(*number)));
            };
            let part = decode_part(*number, &row)?;
            if part.md5 != *md5 {
                return Ok(Err(UploadError::InvalidPart(*number)));
            }
            if index + 1 < listed.len() && part.size < MIN_PART_BYTES {
                return Ok(Err(UploadError::EntityTooSmall(*number)));
            }
            chosen.push(part);
        }
        if chosen.iter().map(|part| part.size).sum::<u64>() > MAX_OBJECT_BYTES {
            return Ok(Err(UploadError::EntityTooLarge));
        }
        let attrs = self.upload_attrs(upload)?;
        let all = self.remove_upload(&info)?;
        for part in &all {
            // `chosen` is in ascending part order (checked above).
            if chosen
                .binary_search_by_key(&part.number, |chosen| chosen.number)
                .is_err()
            {
                self.drop_ref(part.body, now_ms, grace_ms)?;
            }
        }
        let mut digest = Md5::new();
        for part in &chosen {
            digest.update(part.md5);
        }
        let md5: [u8; 16] = digest.finalize().into();
        let children: Vec<(u64, u64)> = chosen.iter().map(|p| (p.body, p.size)).collect();
        let body = self.compose_body(&children, md5, now_ms)?;
        let object = self.insert_version(
            &info.bucket,
            &info.key,
            bucket_row.versioning,
            NewVersion {
                body,
                size: children.iter().map(|(_, len)| len).sum(),
                md5,
                parts: chosen.len() as u16,
            },
            &attrs,
            now_ms,
            grace_ms,
        )?;
        Ok(Ok(object))
    }
}

impl ObjFile<'_> {
    /// The upload, if it exists.
    pub fn upload(&mut self, upload: u64) -> Result<Option<UploadInfo>, StorageError> {
        let Some(uploads) = self.existing_tree(UPLOADS_TREE)? else {
            return Ok(None);
        };
        uploads
            .get(&mut self.core.page_ctx(0), &upload.to_be_bytes())?
            .map(|row| decode_upload(upload, &row))
            .transpose()
    }

    /// Up to `max` uploads started before `before_ms`, oldest first. Upload
    /// ids grow with time, so the scan stops at the first younger one.
    pub fn expired_uploads(
        &mut self,
        before_ms: u64,
        max: usize,
    ) -> Result<Vec<u64>, StorageError> {
        let Some(uploads) = self.existing_tree(UPLOADS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut expired = Vec::new();
        for (key, row) in uploads.scan_range_limit(&mut self.core.page_ctx(0), None, None, max)? {
            let upload = u64::from_be_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| corrupt("upload key"))?,
            );
            if decode_upload(upload, &row)?.initiated_ms >= before_ms {
                break;
            }
            expired.push(upload);
        }
        Ok(expired)
    }

    /// Up to `max` parts of `upload` numbered after `after`, and whether more
    /// follow.
    pub fn list_parts(
        &mut self,
        upload: u64,
        after: u32,
        max: usize,
    ) -> Result<(Vec<PartInfo>, bool), StorageError> {
        let Some(parts) = self.existing_tree(PARTS_TREE)? else {
            return Ok((Vec::new(), false));
        };
        let rows = parts.scan_range_limit(
            &mut self.core.page_ctx(0),
            Some(&part_key(upload, after.saturating_add(1))),
            Some(&part_key(upload, u32::MAX)),
            max + 1,
        )?;
        let truncated = rows.len() > max;
        let parts = rows
            .into_iter()
            .take(max)
            .map(|(key, row)| decode_part(u32::from_be_bytes(key[8..].try_into().unwrap()), &row))
            .collect::<Result<_, _>>()?;
        Ok((parts, truncated))
    }

    /// Up to `max` uploads in `bucket` with keys starting with `prefix`, in
    /// key then upload order, after `(key, upload)`, and whether more follow.
    pub fn list_uploads(
        &mut self,
        bucket: &str,
        prefix: &str,
        after: Option<(&str, u64)>,
        max: usize,
    ) -> Result<(Vec<UploadInfo>, bool), StorageError> {
        let (Some(index), Some(uploads)) = (
            self.existing_tree(UPLOAD_INDEX_TREE)?,
            self.existing_tree(UPLOADS_TREE)?,
        ) else {
            return Ok((Vec::new(), false));
        };
        let mut lower = bucket.as_bytes().to_vec();
        lower.push(0);
        lower.extend_from_slice(prefix.as_bytes());
        if let Some((key, upload)) = after {
            let mut past = index_key(bucket, key, upload);
            past.push(0);
            lower = lower.max(past);
        }
        let mut upper = bucket.as_bytes().to_vec();
        upper.push(1);
        let mut ctx = self.core.page_ctx(0);
        let rows = index.scan_range_limit(&mut ctx, Some(&lower), Some(&upper), max + 1)?;
        let mut out = Vec::new();
        let mut truncated = false;
        for (key, _) in rows {
            let upload = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
            let row = uploads
                .get(&mut ctx, &upload.to_be_bytes())?
                .ok_or_else(|| corrupt("upload index"))?;
            let info = decode_upload(upload, &row)?;
            if !info.key.starts_with(prefix) {
                break;
            }
            if out.len() == max {
                truncated = true;
                break;
            }
            out.push(info);
        }
        Ok((out, truncated))
    }
}
