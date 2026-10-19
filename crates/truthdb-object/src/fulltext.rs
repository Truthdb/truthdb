//! Full-text search over object contents (S3 plan item 15), opt-in per
//! bucket. The index (`truthdb_fulltext`) holds each key's latest version,
//! scoped by bucket; the version number is the document id.
//!
//! The engine keeps the index from the change journal: for each journal row
//! of an indexed bucket it looks at the key's latest version as it is now,
//! reads and tokenizes its body outside the storage lock, and then applies
//! the result in one transaction that first checks the key's latest version
//! is still the one it read ([`ObjTxn::apply_fulltext`]). Applying is
//! idempotent, so a row applied twice (after a crash between applying it and
//! advancing the cursor) changes nothing, and a newer write is left to its
//! own, later, journal row. Enabling a bucket backfills its existing keys the
//! same way; disabling it, or deleting the bucket, purges its index in
//! batches. The journal is not trimmed past the indexer's cursor while any
//! bucket has full-text state.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;
use truthdb_fulltext::DocTerms;

use crate::objects::ObjectInfo;
use crate::{FT_BUCKETS_TREE, FT_CURSOR_KEY, FT_KEYS_TREE, ObjFile, ObjTxn, codec};

/// The largest object `s3 enable fulltext` indexes unless told otherwise.
pub const DEFAULT_FULLTEXT_MAX_BYTES: u64 = 16 << 20;

/// A bucket's full-text state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FtState {
    /// Indexing the keys that existed when it was enabled, in key order:
    /// every key up to and including this one is done (none yet when `None`).
    Backfilling(Option<String>),
    On,
    /// Disabled (or its bucket deleted): its index is being removed.
    Purging,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtBucket {
    pub name: String,
    pub state: FtState,
    /// Larger objects are not indexed.
    pub max_bytes: u64,
}

/// Why a full-text command was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtError {
    NoSuchBucket,
    NotEnabled,
    /// The bucket's previous index is still being removed.
    Purging,
}

/// How the index holds a key's version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtStatus {
    Indexed,
    /// Its content type is not text.
    NotText,
    /// It is stored compressed (a Content-Encoding other than identity).
    Encoded,
    /// It is larger than the bucket's `max_bytes`.
    TooLarge,
    /// Indexing it failed (its terms did not fit one transaction, say).
    Failed,
}

impl FtStatus {
    fn code(self) -> u8 {
        match self {
            FtStatus::Indexed => 0,
            FtStatus::NotText => 1,
            FtStatus::Encoded => 2,
            FtStatus::TooLarge => 3,
            FtStatus::Failed => 4,
        }
    }

    fn from_code(code: u8) -> Result<Self, StorageError> {
        Ok(match code {
            0 => FtStatus::Indexed,
            1 => FtStatus::NotText,
            2 => FtStatus::Encoded,
            3 => FtStatus::TooLarge,
            4 => FtStatus::Failed,
            _ => return Err(corrupt("full-text key row")),
        })
    }
}

/// The version of a key the index holds, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FtKey {
    pub version: u64,
    pub status: FtStatus,
}

/// What [`ObjTxn::apply_fulltext`] records for a version.
pub enum FtDoc<'a> {
    Indexed(&'a DocTerms),
    Skipped(FtStatus),
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt {what}"))
}

/// Why `info` is not indexed in a bucket indexing objects up to `max_bytes`,
/// or `None` when it is: a text content type (`text/*`, JSON, XML, NDJSON
/// and the `+json` and `+xml` types), no content encoding, within the size.
pub fn fulltext_status(info: &ObjectInfo, max_bytes: u64) -> Option<FtStatus> {
    if info
        .attrs
        .content_encoding
        .as_deref()
        .map(str::trim)
        .is_some_and(|encoding| !encoding.is_empty() && !encoding.eq_ignore_ascii_case("identity"))
    {
        return Some(FtStatus::Encoded);
    }
    let media = info
        .attrs
        .content_type
        .as_deref()
        .and_then(|value| value.split(';').next())
        .map(|media| media.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let text = media.starts_with("text/")
        || matches!(
            media.as_str(),
            "application/json" | "application/x-ndjson" | "application/xml"
        )
        || media.ends_with("+json")
        || media.ends_with("+xml");
    if !text {
        return Some(FtStatus::NotText);
    }
    (info.size > max_bytes).then_some(FtStatus::TooLarge)
}

/// `[state][max_bytes u64][backfill key]`: state 0 backfilling (the key
/// done up to, empty for none), 1 on, 2 purging.
fn encode_bucket(state: &FtState, max_bytes: u64) -> Vec<u8> {
    let (code, after) = match state {
        FtState::Backfilling(after) => (0, after.as_deref().unwrap_or("")),
        FtState::On => (1, ""),
        FtState::Purging => (2, ""),
    };
    let mut out = vec![code];
    out.extend_from_slice(&max_bytes.to_le_bytes());
    out.extend_from_slice(after.as_bytes());
    out
}

fn decode_bucket(name: &[u8], row: &[u8]) -> Result<FtBucket, StorageError> {
    let bad = || corrupt("full-text bucket row");
    if row.len() < 9 {
        return Err(bad());
    }
    let after = std::str::from_utf8(&row[9..]).map_err(|_| bad())?;
    let state = match row[0] {
        0 => FtState::Backfilling((!after.is_empty()).then(|| after.to_string())),
        1 => FtState::On,
        2 => FtState::Purging,
        _ => return Err(bad()),
    };
    Ok(FtBucket {
        name: String::from_utf8(name.to_vec()).map_err(|_| bad())?,
        state,
        max_bytes: u64::from_le_bytes(row[1..9].try_into().expect("8 bytes")),
    })
}

/// `bucket, 0, key`.
fn key_row_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut out = bucket.as_bytes().to_vec();
    out.push(0);
    out.extend_from_slice(key.as_bytes());
    out
}

/// `bucket, 0, version`.
fn version_row_key(bucket: &str, version: u64) -> Vec<u8> {
    let mut out = bucket.as_bytes().to_vec();
    out.push(0);
    out.extend_from_slice(&version.to_be_bytes());
    out
}

fn decode_key(row: &[u8]) -> Result<FtKey, StorageError> {
    if row.len() != 9 {
        return Err(corrupt("full-text key row"));
    }
    Ok(FtKey {
        version: u64::from_le_bytes(row[..8].try_into().expect("8 bytes")),
        status: FtStatus::from_code(row[8])?,
    })
}

impl ObjTxn<'_, '_> {
    fn ft_bucket(&mut self, bucket: &str) -> Result<Option<FtBucket>, StorageError> {
        self.trees
            .ft_buckets()
            .get(self.ctx, bucket.as_bytes())?
            .map(|row| decode_bucket(bucket.as_bytes(), &row))
            .transpose()
    }

    fn put_ft_bucket(&mut self, bucket: &FtBucket) -> Result<(), StorageError> {
        let tree = self.trees.ft_buckets();
        let row = encode_bucket(&bucket.state, bucket.max_bytes);
        let mode = &mut OpMode::Txn(self.txn);
        if tree
            .update(self.ctx, mode, bucket.name.as_bytes(), &row)?
            .is_none()
        {
            tree.insert_unique(self.ctx, mode, bucket.name.as_bytes(), &row)?;
        }
        Ok(())
    }

    /// Starts indexing `bucket`'s objects up to `max_bytes`, its existing
    /// ones by backfill. On an indexed bucket it sets the new size and
    /// backfills again, so objects skipped as too large are looked at anew.
    pub fn enable_fulltext(
        &mut self,
        bucket: &str,
        max_bytes: u64,
    ) -> Result<Result<(), FtError>, StorageError> {
        if self.bucket_row(bucket)?.is_none() {
            return Ok(Err(FtError::NoSuchBucket));
        }
        let state = match self.ft_bucket(bucket)? {
            Some(FtBucket {
                state: FtState::Purging,
                ..
            }) => return Ok(Err(FtError::Purging)),
            Some(_) => FtState::Backfilling(None),
            None => {
                // With no bucket indexed, nothing has held the journal back,
                // and the cursor may point at rows long trimmed: the backfill
                // covers everything up to now, so the cursor starts here.
                if self
                    .trees
                    .ft_buckets()
                    .scan_range_limit(self.ctx, None, None, 1)?
                    .is_empty()
                {
                    let last = self.last_change()?;
                    self.set_fulltext_cursor(last)?;
                }
                FtState::Backfilling(None)
            }
        };
        self.put_ft_bucket(&FtBucket {
            name: bucket.to_string(),
            state,
            max_bytes,
        })?;
        Ok(Ok(()))
    }

    /// Stops indexing `bucket`; its index is purged in the background.
    pub fn disable_fulltext(&mut self, bucket: &str) -> Result<Result<(), FtError>, StorageError> {
        match self.ft_bucket(bucket)? {
            None
            | Some(FtBucket {
                state: FtState::Purging,
                ..
            }) => Ok(Err(FtError::NotEnabled)),
            Some(mut existing) => {
                existing.state = FtState::Purging;
                self.put_ft_bucket(&existing)?;
                Ok(Ok(()))
            }
        }
    }

    /// Marks a deleted bucket's index for purging, so a bucket created later
    /// under its name starts with none.
    pub(crate) fn drop_fulltext(&mut self, bucket: &str) -> Result<(), StorageError> {
        if let Some(mut existing) = self.ft_bucket(bucket)?
            && existing.state != FtState::Purging
        {
            existing.state = FtState::Purging;
            self.put_ft_bucket(&existing)?;
        }
        Ok(())
    }

    /// Records the last journal row the indexer has applied.
    pub fn set_fulltext_cursor(&mut self, sequence: u64) -> Result<(), StorageError> {
        let meta = self.trees.meta();
        let row = sequence.to_le_bytes();
        let mode = &mut OpMode::Txn(self.txn);
        if meta.update(self.ctx, mode, FT_CURSOR_KEY, &row)?.is_none() {
            meta.insert_unique(self.ctx, mode, FT_CURSOR_KEY, &row)?;
        }
        Ok(())
    }

    /// Records backfill progress: every key up to `after` is done, or, with
    /// `None`, the whole bucket (it is then on). Does nothing to a bucket no
    /// longer backfilling.
    pub fn set_fulltext_backfill(
        &mut self,
        bucket: &str,
        after: Option<&str>,
    ) -> Result<(), StorageError> {
        let Some(mut existing) = self.ft_bucket(bucket)? else {
            return Ok(());
        };
        if !matches!(existing.state, FtState::Backfilling(_)) {
            return Ok(());
        }
        existing.state = match after {
            Some(after) => FtState::Backfilling(Some(after.to_string())),
            None => FtState::On,
        };
        self.put_ft_bucket(&existing)
    }

    /// Makes the index hold `version` of `key` as `doc` (with `version`
    /// `None`, hold nothing for the key) when `version` is still the key's
    /// latest live version and the bucket is still indexed; returns whether
    /// it did. Whatever the index held for the key before is removed.
    pub fn apply_fulltext(
        &mut self,
        bucket: &str,
        key: &str,
        version: Option<u64>,
        doc: FtDoc<'_>,
    ) -> Result<bool, StorageError> {
        match self.ft_bucket(bucket)? {
            Some(FtBucket {
                state: FtState::Backfilling(_) | FtState::On,
                ..
            }) => {}
            _ => return Ok(false),
        }
        if self.latest_live_version(bucket, key)? != version {
            return Ok(false);
        }
        let row_key = key_row_key(bucket, key);
        let keys = self.trees.ft_keys();
        let held = keys
            .get(self.ctx, &row_key)?
            .map(|row| decode_key(&row))
            .transpose()?;
        if let Some(FtKey {
            version: old,
            status: FtStatus::Indexed,
        }) = held
        {
            self.trees
                .ft_index()
                .unindex_document(self.ctx, self.txn, bucket.as_bytes(), old)?;
            self.trees.ft_versions().delete(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &version_row_key(bucket, old),
            )?;
        }
        let Some(version) = version else {
            if held.is_some() {
                keys.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
            }
            return Ok(true);
        };
        let status = match doc {
            FtDoc::Indexed(terms) => {
                self.trees.ft_index().index_document(
                    self.ctx,
                    self.txn,
                    bucket.as_bytes(),
                    version,
                    terms,
                )?;
                self.trees.ft_versions().insert_unique(
                    self.ctx,
                    &mut OpMode::Txn(self.txn),
                    &version_row_key(bucket, version),
                    key.as_bytes(),
                )?;
                FtStatus::Indexed
            }
            FtDoc::Skipped(status) => status,
        };
        let mut row = version.to_le_bytes().to_vec();
        row.push(status.code());
        let mode = &mut OpMode::Txn(self.txn);
        if held.is_some() {
            keys.update(self.ctx, mode, &row_key, &row)?;
        } else {
            let inserted = keys.insert_unique(self.ctx, mode, &row_key, &row)?;
            debug_assert_eq!(inserted, TreeInsert::Inserted);
        }
        Ok(true)
    }

    /// Removes up to `limit` documents (and then key rows) of a purging
    /// bucket's index, and the bucket's full-text state once nothing is
    /// left. Returns whether the purge is done.
    pub fn purge_fulltext(&mut self, bucket: &str, limit: usize) -> Result<bool, StorageError> {
        match self.ft_bucket(bucket)? {
            Some(FtBucket {
                state: FtState::Purging,
                ..
            }) => {}
            _ => return Ok(true),
        }
        let index = self.trees.ft_index();
        let docs = index.documents(self.ctx, bucket.as_bytes(), limit)?;
        for doc in &docs {
            index.unindex_document(self.ctx, self.txn, bucket.as_bytes(), *doc)?;
        }
        let mut left = limit - docs.len();
        if left == 0 {
            return Ok(false);
        }
        // Key rows, then version rows: both start `bucket, 0`.
        let lower = key_row_key(bucket, "");
        let mut upper = bucket.as_bytes().to_vec();
        upper.push(1);
        for tree in [self.trees.ft_keys(), self.trees.ft_versions()] {
            let rows = tree.scan_range_limit(self.ctx, Some(&lower), Some(&upper), left)?;
            for (row_key, _) in &rows {
                tree.delete(self.ctx, &mut OpMode::Txn(self.txn), row_key)?;
            }
            left -= rows.len();
            if left == 0 {
                return Ok(false);
            }
        }
        self.trees
            .ft_buckets()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), bucket.as_bytes())?;
        Ok(true)
    }

    /// The indexer's cursor, when any bucket has full-text state (the
    /// journal must then not be trimmed past it).
    pub(crate) fn fulltext_hold(&mut self) -> Result<Option<u64>, StorageError> {
        if self
            .trees
            .ft_buckets()
            .scan_range_limit(self.ctx, None, None, 1)?
            .is_empty()
        {
            return Ok(None);
        }
        let cursor = self.trees.meta().get(self.ctx, FT_CURSOR_KEY)?;
        Ok(Some(
            cursor
                .map(|row| codec::u64_le(&row))
                .transpose()?
                .unwrap_or(0),
        ))
    }
}

impl ObjFile<'_> {
    /// Every bucket with full-text state, by name.
    pub fn fulltext_buckets(&mut self) -> Result<Vec<FtBucket>, StorageError> {
        let Some(tree) = self.existing_tree(FT_BUCKETS_TREE)? else {
            return Ok(Vec::new());
        };
        tree.scan(&mut self.core.page_ctx(0))?
            .iter()
            .map(|(name, row)| decode_bucket(name, row))
            .collect()
    }

    /// [`ObjTxn::fulltext_hold`], read outside a transaction.
    pub(crate) fn fulltext_hold(&mut self) -> Result<Option<u64>, StorageError> {
        let Some(tree) = self.existing_tree(FT_BUCKETS_TREE)? else {
            return Ok(None);
        };
        if tree
            .scan_range_limit(&mut self.core.page_ctx(0), None, None, 1)?
            .is_empty()
        {
            return Ok(None);
        }
        self.fulltext_cursor().map(Some)
    }

    /// The last journal row the indexer has applied.
    pub fn fulltext_cursor(&mut self) -> Result<u64, StorageError> {
        let Some(meta) = self.eng.root.map(crate::meta_tree) else {
            return Ok(0);
        };
        self.core.ensure_rel_usable()?;
        Ok(meta
            .get(&mut self.core.page_ctx(0), FT_CURSOR_KEY)?
            .map(|row| codec::u64_le(&row))
            .transpose()?
            .unwrap_or(0))
    }

    /// The full-text index's trees, when they exist.
    fn fulltext_index(&self) -> Result<Option<truthdb_fulltext::Index>, StorageError> {
        Ok(
            match (
                self.existing_tree(crate::FT_POSTINGS_TREE)?,
                self.existing_tree(crate::FT_DOCS_TREE)?,
            ) {
                (Some(postings), Some(docs)) => Some(truthdb_fulltext::Index { postings, docs }),
                _ => None,
            },
        )
    }

    /// Reads up to `budget` more postings of `search` in `bucket`'s index;
    /// returns whether all are read (see `truthdb_fulltext::Search::step`).
    pub fn fulltext_step(
        &mut self,
        search: &mut truthdb_fulltext::Search,
        bucket: &str,
        budget: usize,
    ) -> Result<bool, truthdb_fulltext::SearchError> {
        match self.fulltext_index()? {
            Some(index) => search.step(
                &mut self.core.page_ctx(0),
                &index,
                bucket.as_bytes(),
                budget,
            ),
            None => Ok(true),
        }
    }

    /// `bucket`'s document count and length, for ranking.
    pub fn fulltext_stats(
        &mut self,
        bucket: &str,
    ) -> Result<truthdb_fulltext::Stats, StorageError> {
        match self.fulltext_index()? {
            Some(index) => index.stats(&mut self.core.page_ctx(0), bucket.as_bytes()),
            None => Ok(truthdb_fulltext::Stats::default()),
        }
    }

    /// The key of `version`, when the index holds it.
    pub fn fulltext_version_key(
        &mut self,
        bucket: &str,
        version: u64,
    ) -> Result<Option<String>, StorageError> {
        let Some(tree) = self.existing_tree(crate::FT_VERSIONS_TREE)? else {
            return Ok(None);
        };
        tree.get(
            &mut self.core.page_ctx(0),
            &version_row_key(bucket, version),
        )?
        .map(|key| String::from_utf8(key).map_err(|_| corrupt("full-text version row")))
        .transpose()
    }

    /// The version of `key` the index holds, and how.
    pub fn fulltext_key(&mut self, bucket: &str, key: &str) -> Result<Option<FtKey>, StorageError> {
        let Some(tree) = self.existing_tree(FT_KEYS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &key_row_key(bucket, key))?
            .map(|row| decode_key(&row))
            .transpose()
    }
}
