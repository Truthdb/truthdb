//! Storage usage per bucket: how many versions, delete markers and parts of
//! multipart uploads a bucket holds, and their bytes (logical sizes), per
//! storage class. A version is current while it is its key's newest row,
//! noncurrent after; a delete marker counts as one whatever its place.
//!
//! The counts change in the same transaction as the rows they count, so they
//! are as durable as the rows. A file written before them is counted by a
//! backfill that walks the keys in order (`backfill_step`): until it ends, a
//! change is counted only for a key the walk has passed, the walk counting
//! the others as it finds them.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec;
use crate::objects::{
    CLASS_SHIFT, FLAG_DELETE_MARKER, VersionRow, find_version, key_prefix, version_from_key,
};
use crate::uploads::{decode_part, decode_upload, part_key};
use crate::{ObjFile, ObjTxn, StorageClass, USAGE_TREE, VersionSel};

/// What a usage row counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UsageKind {
    Current = 0,
    Noncurrent = 1,
    DeleteMarkers = 2,
    UploadParts = 3,
}

impl UsageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            UsageKind::Current => "CURRENT",
            UsageKind::Noncurrent => "NONCURRENT",
            UsageKind::DeleteMarkers => "DELETE_MARKERS",
            UsageKind::UploadParts => "UPLOAD_PARTS",
        }
    }

    fn from_code(code: u8) -> Option<UsageKind> {
        Some(match code {
            0 => UsageKind::Current,
            1 => UsageKind::Noncurrent,
            2 => UsageKind::DeleteMarkers,
            3 => UsageKind::UploadParts,
            _ => return None,
        })
    }
}

/// One bucket's count and bytes of one kind in one storage class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketUsage {
    pub bucket: String,
    pub kind: UsageKind,
    pub storage_class: StorageClass,
    pub objects: u64,
    pub bytes: u64,
}

/// META key of the backfill's position: the keys before it are counted.
const CURSOR_KEY: &[u8] = b"usage_cursor";
/// META key set once the backfill has counted every key.
const DONE_KEY: &[u8] = b"usage_done";

fn usage_key(bucket: &str, kind: UsageKind, class: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(bucket.len() + 3);
    out.extend_from_slice(bucket.as_bytes());
    out.push(0);
    out.push(kind as u8);
    out.push(class);
    out
}

/// What a version row counts as, at the key's head or not.
fn kind_of(row: &VersionRow, head: bool) -> (UsageKind, u8) {
    if row.flags & FLAG_DELETE_MARKER != 0 {
        (UsageKind::DeleteMarkers, 0)
    } else if head {
        (UsageKind::Current, row.flags >> CLASS_SHIFT & 7)
    } else {
        (UsageKind::Noncurrent, row.flags >> CLASS_SHIFT & 7)
    }
}

/// The position right after a key (its `prefix`) in the walk: past its
/// version rows and its uploads' index entries (the prefix and eight
/// bytes), before every later key's prefix (keys hold no NUL, so a later
/// key differs from it before the prefix's last byte).
fn after_key(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    out.extend_from_slice(&[0xFF; 9]);
    out
}

impl ObjTxn<'_, '_> {
    /// Adds `objects` and `bytes` (negative: removes) to a usage row.
    fn add_usage(
        &mut self,
        bucket: &str,
        (kind, class): (UsageKind, u8),
        objects: i64,
        bytes: i64,
    ) -> Result<(), StorageError> {
        let usage = self.trees.usage();
        let key = usage_key(bucket, kind, class);
        let (old_objects, old_bytes) = match usage.get(self.ctx, &key)? {
            Some(row) => (
                codec::u64_le(row.get(..8).ok_or_else(|| codec::corrupt("usage row"))?)?,
                codec::u64_le(row.get(8..16).ok_or_else(|| codec::corrupt("usage row"))?)?,
            ),
            None => (0, 0),
        };
        let objects = old_objects.wrapping_add_signed(objects);
        let bytes = old_bytes.wrapping_add_signed(bytes);
        let mut row = objects.to_le_bytes().to_vec();
        row.extend_from_slice(&bytes.to_le_bytes());
        let mode = &mut OpMode::Txn(self.txn);
        match (old_objects, objects) {
            (0, 0) if old_bytes == 0 && bytes == 0 => {}
            (_, 0) if bytes == 0 => {
                usage.delete(self.ctx, mode, &key)?;
            }
            _ if old_objects == 0 && old_bytes == 0 => {
                usage.insert_unique(self.ctx, mode, &key, &row)?;
            }
            _ => {
                usage.update(self.ctx, mode, &key, &row)?;
            }
        }
        Ok(())
    }

    /// Whether changes to `bucket`'s `key` are counted now: always, once the
    /// backfill is done; before, for the keys it has passed.
    fn usage_counted(&mut self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let meta = self.trees.meta();
        if meta.get(self.ctx, DONE_KEY)?.is_some() {
            return Ok(true);
        }
        Ok(meta
            .get(self.ctx, CURSOR_KEY)?
            .is_some_and(|cursor| key_prefix(bucket, key) < cursor))
    }

    /// The key's newest row (its head), if any.
    pub(crate) fn key_head(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(u64, VersionRow)>, StorageError> {
        let objects = self.trees.objects();
        Ok(
            find_version(self.ctx, &objects, bucket, key, VersionSel::Latest)?
                .map(|(_, version, row)| (version, row)),
        )
    }

    /// Counts a change to `bucket`'s `key`: `before` its head before the
    /// change, `removed` and `inserted` the rows it removed and added. Rows
    /// it moved off or onto the head count anew.
    pub(crate) fn count_versions(
        &mut self,
        bucket: &str,
        key: &str,
        before: Option<(u64, VersionRow)>,
        removed: &[(u64, VersionRow)],
        inserted: &[(u64, VersionRow)],
    ) -> Result<(), StorageError> {
        if !self.usage_counted(bucket, key)? {
            return Ok(());
        }
        let after = self.key_head(bucket, key)?;
        let (was, is) = (
            before.as_ref().map(|(version, _)| *version),
            after.as_ref().map(|(version, _)| *version),
        );
        let touched = |version: u64| removed.iter().chain(inserted).any(|(at, _)| *at == version);
        for (version, row) in removed {
            let size = row.size as i64;
            self.add_usage(bucket, kind_of(row, was == Some(*version)), -1, -size)?;
        }
        for (version, row) in inserted {
            let size = row.size as i64;
            self.add_usage(bucket, kind_of(row, is == Some(*version)), 1, size)?;
        }
        if was != is {
            // The old head, still there, is no longer one; the new head, if
            // it was there before, now is.
            for (head, moved_on) in [(before, false), (after, true)] {
                if let Some((version, row)) = head
                    && !touched(version)
                {
                    let size = row.size as i64;
                    self.add_usage(bucket, kind_of(&row, !moved_on), -1, -size)?;
                    self.add_usage(bucket, kind_of(&row, moved_on), 1, size)?;
                }
            }
        }
        Ok(())
    }

    /// Counts version `version` of `bucket`'s `key` moved from storage class
    /// `from` to `to`.
    pub(crate) fn count_transition(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        row: &VersionRow,
        from: StorageClass,
        to: StorageClass,
    ) -> Result<(), StorageError> {
        if !self.usage_counted(bucket, key)? {
            return Ok(());
        }
        let head = self.key_head(bucket, key)?.map(|(at, _)| at) == Some(version);
        let (kind, _) = kind_of(row, head);
        let size = row.size as i64;
        self.add_usage(bucket, (kind, from.code()), -1, -size)?;
        self.add_usage(bucket, (kind, to.code()), 1, size)
    }

    /// Counts parts of an upload of `bucket`'s `key` in `class`: `added`
    /// parts and `bytes` (negative: removed).
    pub(crate) fn count_parts(
        &mut self,
        bucket: &str,
        key: &str,
        class: StorageClass,
        added: i64,
        bytes: i64,
    ) -> Result<(), StorageError> {
        if !self.usage_counted(bucket, key)? {
            return Ok(());
        }
        self.add_usage(bucket, (UsageKind::UploadParts, class.code()), added, bytes)
    }

    /// Forgets a deleted bucket's usage rows.
    pub(crate) fn forget_usage(&mut self, bucket: &str) -> Result<(), StorageError> {
        let usage = self.trees.usage();
        let (lower, upper) = crate::buckets::bucket_object_range(bucket);
        for (key, _) in usage.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            usage.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(())
    }

    /// One step of the backfill: counts the keys (their versions and their
    /// uploads' parts) from its position on, about `rows` version rows of
    /// them, and moves past them. Returns whether keys remain.
    pub fn usage_backfill_step(&mut self, rows: usize) -> Result<bool, StorageError> {
        let meta = self.trees.meta();
        if meta.get(self.ctx, DONE_KEY)?.is_some() {
            return Ok(false);
        }
        let cursor = meta.get(self.ctx, CURSOR_KEY)?;
        let start = cursor.clone().unwrap_or_default();
        let limit = rows.max(1);
        // The keys the next `limit` version rows and the next `limit` upload
        // index entries belong to; the step ends after the last key both
        // scans reached, and counts each key before it whole (its versions
        // and its uploads), however many rows it has.
        let objects = self.trees.objects();
        let found = objects.scan_range_limit(self.ctx, Some(&start), None, limit)?;
        let object_end = (found.len() >= limit)
            .then(|| {
                found
                    .last()
                    .map(|(row_key, _)| after_key(&row_key[..row_key.len() - 8]))
            })
            .flatten();
        let index = self.trees.upload_index();
        let listed =
            index.scan_range_limit(self.ctx, Some(&start), object_end.as_deref(), limit)?;
        let upload_end = (listed.len() >= limit)
            .then(|| {
                listed
                    .last()
                    .map(|(key, _)| after_key(&key[..key.len() - 8]))
            })
            .flatten();
        let end = match (object_end, upload_end) {
            (Some(objects), Some(uploads)) => Some(objects.min(uploads)),
            (objects, uploads) => objects.or(uploads),
        };
        let before_end = |prefix: &[u8]| end.as_deref().is_none_or(|end| prefix < end);
        let mut prefixes: Vec<Vec<u8>> = Vec::new();
        for (row_key, _) in &found {
            let prefix = row_key[..row_key.len() - 8].to_vec();
            if prefixes.last() != Some(&prefix) && before_end(&prefix) {
                prefixes.push(prefix);
            }
        }
        for prefix in &prefixes {
            let mut upper = prefix.clone();
            upper.extend_from_slice(&[0xFF; 8]);
            let bucket = prefix
                .split(|byte| *byte == 0)
                .next()
                .and_then(|name| std::str::from_utf8(name).ok())
                .ok_or_else(|| codec::corrupt("object key"))?
                .to_string();
            for (index, (row_key, row)) in objects
                .scan_range(self.ctx, Some(prefix), Some(&upper))?
                .into_iter()
                .enumerate()
            {
                version_from_key(&row_key)?;
                let row = VersionRow::decode(&row)?;
                let size = row.size as i64;
                self.add_usage(&bucket, kind_of(&row, index == 0), 1, size)?;
            }
        }
        // The uploads of the keys listed (their index keys on the same
        // prefixes; all before the end), each key's whole.
        let mut upload_prefixes: Vec<Vec<u8>> = Vec::new();
        for (key, _) in &listed {
            let prefix = key[..key.len() - 8].to_vec();
            if upload_prefixes.last() != Some(&prefix) {
                upload_prefixes.push(prefix);
            }
        }
        for prefix in &upload_prefixes {
            for (index_key, _) in
                index.scan_range(self.ctx, Some(prefix), Some(&after_key(prefix)))?
            {
                let upload = u64::from_be_bytes(
                    index_key[index_key.len() - 8..]
                        .try_into()
                        .map_err(|_| codec::corrupt("upload index"))?,
                );
                let Some(row) = self.trees.uploads().get(self.ctx, &upload.to_be_bytes())? else {
                    continue;
                };
                let info = decode_upload(upload, &row)?;
                let parts = self.trees.parts().scan_range(
                    self.ctx,
                    Some(&part_key(upload, 0)),
                    Some(&part_key(upload, u32::MAX)),
                )?;
                for (key, row) in parts {
                    let number = u32::from_be_bytes(
                        key[8..]
                            .try_into()
                            .map_err(|_| codec::corrupt("part key"))?,
                    );
                    let part = decode_part(number, &row)?;
                    self.add_usage(
                        &info.bucket,
                        (UsageKind::UploadParts, info.storage_class.code()),
                        1,
                        part.size as i64,
                    )?;
                }
            }
        }
        let mode = &mut OpMode::Txn(self.txn);
        match end {
            None => {
                meta.insert_unique(self.ctx, mode, DONE_KEY, &[1])?;
                Ok(false)
            }
            Some(end) => {
                if cursor.is_some() {
                    meta.update(self.ctx, mode, CURSOR_KEY, &end)?;
                } else {
                    meta.insert_unique(self.ctx, mode, CURSOR_KEY, &end)?;
                }
                Ok(true)
            }
        }
    }
}

impl ObjFile<'_> {
    /// One step of the usage backfill, in a transaction of its own; returns
    /// whether keys remain.
    pub fn usage_backfill(&mut self, rows: usize) -> Result<bool, StorageError> {
        // A file the engine never wrote has nothing to count, and is not
        // made to hold the engine by counting it.
        if self.eng.root.is_none() || self.usage_complete()? {
            return Ok(false);
        }
        self.transaction(|tx| tx.usage_backfill_step(rows))
    }

    /// Whether every key is counted (the backfill is done).
    pub fn usage_complete(&mut self) -> Result<bool, StorageError> {
        self.core.ensure_rel_usable()?;
        let Some(root) = self.eng.root else {
            return Ok(false);
        };
        let meta = crate::meta_tree(root);
        let mut ctx = self.core.page_ctx(0);
        Ok(meta.get(&mut ctx, DONE_KEY)?.is_some())
    }

    /// Every bucket's usage rows, by bucket, kind and class.
    pub fn bucket_usage(&mut self) -> Result<Vec<BucketUsage>, StorageError> {
        let Some(tree) = self.existing_tree(USAGE_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        let mut out = Vec::new();
        for (key, row) in tree.scan(&mut ctx)? {
            let corrupt = || codec::corrupt("usage row");
            let split = key.len().checked_sub(3).ok_or_else(corrupt)?;
            let bucket = String::from_utf8(key[..split].to_vec()).map_err(|_| corrupt())?;
            let kind = UsageKind::from_code(key[split + 1]).ok_or_else(corrupt)?;
            let storage_class = StorageClass::from_code(key[split + 2]);
            out.push(BucketUsage {
                bucket,
                kind,
                storage_class,
                objects: codec::u64_le(row.get(..8).ok_or_else(corrupt)?)?,
                bytes: codec::u64_le(row.get(8..16).ok_or_else(corrupt)?)?,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
impl ObjTxn<'_, '_> {
    /// The usage rows as a full count of the trees finds them (what the
    /// counters must agree with).
    pub(crate) fn recount_usage(&mut self) -> Result<Vec<BucketUsage>, StorageError> {
        use std::collections::BTreeMap;
        let mut counts: BTreeMap<(String, UsageKind, u8), (u64, u64)> = BTreeMap::new();
        let mut add = |bucket: &str, (kind, class): (UsageKind, u8), bytes: u64| {
            let entry = counts.entry((bucket.to_string(), kind, class)).or_default();
            entry.0 += 1;
            entry.1 += bytes;
        };
        let mut last_prefix: Option<Vec<u8>> = None;
        for (row_key, row) in self.trees.objects().scan(self.ctx)? {
            let prefix = row_key[..row_key.len() - 8].to_vec();
            let head = last_prefix.as_ref() != Some(&prefix);
            let bucket =
                String::from_utf8(prefix.split(|b| *b == 0).next().unwrap().to_vec()).unwrap();
            let row = VersionRow::decode(&row)?;
            add(&bucket, kind_of(&row, head), row.size);
            last_prefix = Some(prefix);
        }
        for (upload, row) in self.trees.uploads().scan(self.ctx)? {
            let upload = u64::from_be_bytes(upload[..8].try_into().unwrap());
            let info = decode_upload(upload, &row)?;
            for (key, row) in self.trees.parts().scan_range(
                self.ctx,
                Some(&part_key(upload, 0)),
                Some(&part_key(upload, u32::MAX)),
            )? {
                let part = decode_part(u32::from_be_bytes(key[8..].try_into().unwrap()), &row)?;
                add(
                    &info.bucket,
                    (UsageKind::UploadParts, info.storage_class.code()),
                    part.size,
                );
            }
        }
        Ok(counts
            .into_iter()
            .map(|((bucket, kind, class), (objects, bytes))| BucketUsage {
                bucket,
                kind,
                storage_class: StorageClass::from_code(class),
                objects,
                bytes,
            })
            .collect())
    }

    /// Forgets every count, as a file written before usage was counted.
    pub(crate) fn reset_usage(&mut self) -> Result<(), StorageError> {
        let usage = self.trees.usage();
        for (key, _) in usage.scan(self.ctx)? {
            usage.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        let meta = self.trees.meta();
        for key in [CURSOR_KEY, DONE_KEY] {
            if meta.get(self.ctx, key)?.is_some() {
                meta.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
            }
        }
        Ok(())
    }
}
