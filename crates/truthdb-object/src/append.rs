//! Appends (S3 Express One Zone's `x-amz-write-offset-bytes`): a directory
//! bucket's object grown by another body, as one more part of it.

use md5::{Digest, Md5};
use truthdb_core::storage::StorageError;

use crate::body::{SealedBody, composite_children};
use crate::objects::{NewVersion, ObjectAttrs, ObjectInfo, StoredChecksum};
use crate::{ObjTxn, ObjectError};

/// Parts an object has at most (S3's: "each object can have up to 10,000
/// parts"): an append past them is refused.
pub const MAX_APPEND_PARTS: u16 = 10_000;

/// Why an append was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendError {
    Object(ObjectError),
    /// It would make more than [`MAX_APPEND_PARTS`].
    TooManyParts,
}

impl ObjTxn<'_, '_> {
    /// Appends `body` to `current` (the current version of `bucket`'s
    /// `key`, read in this lock hold): a version of the object's parts then
    /// `body`'s, with `attrs` and the part checksum `part_checksum` of the
    /// appended one. Its ETag is a multipart object's: the MD5 of its parts'
    /// MD5s, with their count.
    #[allow(clippy::too_many_arguments)]
    pub fn append_object(
        &mut self,
        (bucket, key): (&str, &str),
        current: &ObjectInfo,
        body: &SealedBody,
        attrs: &ObjectAttrs,
        part_checksum: Option<&StoredChecksum>,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<ObjectInfo, AppendError>, StorageError> {
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(AppendError::Object(ObjectError::NoSuchBucket)));
        };
        let had = current.parts.max(1);
        if had >= MAX_APPEND_PARTS {
            return Ok(Err(AppendError::TooManyParts));
        }
        // The object's parts as they are (one, when it has no parts), each
        // with its MD5: they are the new body's too.
        let mut children = Vec::new();
        if current.parts == 0 {
            let row = self.load_body(current.body)?;
            children.push((current.body, row.len, row.md5));
        } else {
            for child in composite_children(self.ctx, &self.trees.segments(), current.body)? {
                let md5 = self.load_body(child.child)?.md5;
                children.push((child.child, child.len, md5));
            }
        }
        for (child, _, _) in &children {
            self.add_ref(*child)?;
        }
        let sealed = body.seal_in(self)?;
        children.push((sealed.body, sealed.len, sealed.md5));
        let mut digest = Md5::new();
        for (_, _, md5) in &children {
            digest.update(md5);
        }
        let md5: [u8; 16] = digest.finalize().into();
        let spans: Vec<(u64, u64)> = children
            .iter()
            .map(|(child, len, _)| (*child, *len))
            .collect();
        let composed = self.compose_body(&spans, md5, now_ms)?;
        // The parts' checksums: the object's own for a single body (a whole
        // checksum of it), its kept ones for parts, then the appended one's.
        let mut sums = match current.parts {
            0 => current
                .attrs
                .checksum
                .iter()
                .filter(|checksum| checksum.full_object)
                .map(|checksum| {
                    (
                        1,
                        format!("{} {}", checksum.algorithm, checksum.value).into_bytes(),
                    )
                })
                .collect(),
            _ => self.part_checksums_of((bucket, key, current.version))?,
        };
        if let Some(checksum) = part_checksum {
            sums.push((
                had + 1,
                format!("{} {}", checksum.algorithm, checksum.value).into_bytes(),
            ));
        }
        let appended = self.insert_version(
            bucket,
            key,
            bucket_row.versioning,
            NewVersion {
                body: composed,
                size: current.size + body.size.unwrap_or(sealed.len),
                md5,
                parts: had + 1,
                storage_class: current.storage_class,
            },
            attrs,
            now_ms,
            grace_ms,
        )?;
        self.put_part_checksums((bucket, key, appended.version), &sums)?;
        Ok(Ok(appended))
    }
}
