//! The change journal (S3 Metadata's journal table): one row per change to
//! any bucket's objects, numbered in commit order and written in the same
//! transaction as the change. CREATE records a new version (a PUT, a copy, a
//! completed multipart upload); UPDATE_METADATA a tag change; DELETE a delete
//! marker (`is_delete_marker` TRUE) or a version removed for good (the
//! flag NULL). Replacing a key's null version in place (an overwrite in an
//! unversioned bucket, or a delete in a suspended one) records only the new
//! version or marker. Rows older than a retention period are trimmed.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{CHANGES_TREE, NEXT_CHANGE_KEY, ObjFile, ObjTxn, next_counter};

/// What a journal row records (AWS's `record_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Create,
    UpdateMetadata,
    Delete,
}

impl RecordType {
    pub fn as_str(self) -> &'static str {
        match self {
            RecordType::Create => "CREATE",
            RecordType::UpdateMetadata => "UPDATE_METADATA",
            RecordType::Delete => "DELETE",
        }
    }
}

/// One journal row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRecord {
    pub sequence: u64,
    pub record_type: RecordType,
    pub timestamp_ms: u64,
    pub bucket: String,
    pub key: String,
    pub version: u64,
    pub null_version: bool,
    /// TRUE for a delete marker; FALSE for CREATE and UPDATE_METADATA; `None`
    /// for a version removed for good.
    pub is_delete_marker: Option<bool>,
    /// The version's size, digest, part count and modification time; `None`
    /// for a version removed for good.
    pub object: Option<JournalObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalObject {
    pub size: u64,
    pub md5: [u8; 16],
    pub parts: u16,
    pub last_modified_ms: u64,
}

const FLAG_NULL_VERSION: u8 = 1;
const FLAG_MARKER: u8 = 2;
/// A version removed for good: no marker flag, no object columns.
const FLAG_PERMANENT: u8 = 4;

fn corrupt() -> StorageError {
    StorageError::InvalidFile("corrupt journal row".to_string())
}

impl JournalRecord {
    /// `[type][flags][time][version][size][md5][parts][mtime][bucket len]
    /// [bucket][key]`: at most 1,140 bytes (a 63-byte bucket name and a
    /// 1,024-byte key), within a tree cell.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(60 + self.bucket.len() + self.key.len());
        out.push(match self.record_type {
            RecordType::Create => 0,
            RecordType::UpdateMetadata => 1,
            RecordType::Delete => 2,
        });
        out.push(
            if self.null_version {
                FLAG_NULL_VERSION
            } else {
                0
            } | if self.is_delete_marker == Some(true) {
                FLAG_MARKER
            } else {
                0
            } | if self.is_delete_marker.is_none() {
                FLAG_PERMANENT
            } else {
                0
            },
        );
        out.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        out.extend_from_slice(&self.version.to_le_bytes());
        let object = self.object.unwrap_or(JournalObject {
            size: 0,
            md5: [0; 16],
            parts: 0,
            last_modified_ms: 0,
        });
        out.extend_from_slice(&object.size.to_le_bytes());
        out.extend_from_slice(&object.md5);
        out.extend_from_slice(&object.parts.to_le_bytes());
        out.extend_from_slice(&object.last_modified_ms.to_le_bytes());
        out.push(self.bucket.len() as u8);
        out.extend_from_slice(self.bucket.as_bytes());
        out.extend_from_slice(self.key.as_bytes());
        out
    }

    fn decode(sequence: u64, bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() < 53 {
            return Err(corrupt());
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let record_type = match bytes[0] {
            0 => RecordType::Create,
            1 => RecordType::UpdateMetadata,
            2 => RecordType::Delete,
            _ => return Err(corrupt()),
        };
        let flags = bytes[1];
        let bucket_len = bytes[52] as usize;
        let names = &bytes[53..];
        if names.len() < bucket_len {
            return Err(corrupt());
        }
        let text = |b: &[u8]| String::from_utf8(b.to_vec()).map_err(|_| corrupt());
        let permanent = flags & FLAG_PERMANENT != 0;
        Ok(JournalRecord {
            sequence,
            record_type,
            timestamp_ms: u64_at(2),
            version: u64_at(10),
            null_version: flags & FLAG_NULL_VERSION != 0,
            is_delete_marker: (!permanent).then_some(flags & FLAG_MARKER != 0),
            object: (!permanent).then(|| JournalObject {
                size: u64_at(18),
                md5: bytes[26..42].try_into().unwrap(),
                parts: u16::from_le_bytes([bytes[42], bytes[43]]),
                last_modified_ms: u64_at(44),
            }),
            bucket: text(&names[..bucket_len])?,
            key: text(&names[bucket_len..])?,
        })
    }
}

/// A journal row's fields, numbered when written.
pub(crate) struct Change<'a> {
    pub record_type: RecordType,
    pub bucket: &'a str,
    pub key: &'a str,
    pub version: u64,
    pub null_version: bool,
    pub is_delete_marker: Option<bool>,
    pub object: Option<JournalObject>,
}

impl ObjTxn<'_, '_> {
    /// The journal's last sequence number handed out (0 before any).
    pub(crate) fn last_change(&mut self) -> Result<u64, StorageError> {
        Ok(match self.trees.meta().get(self.ctx, NEXT_CHANGE_KEY)? {
            Some(next) => crate::codec::u64_le(&next)?.saturating_sub(1),
            None => 0,
        })
    }

    /// Records `change` as the journal's next row, in this transaction.
    pub(crate) fn record_change(
        &mut self,
        change: Change<'_>,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        let sequence = next_counter(self, NEXT_CHANGE_KEY)?;
        let record = JournalRecord {
            sequence,
            record_type: change.record_type,
            timestamp_ms: now_ms,
            bucket: change.bucket.to_string(),
            key: change.key.to_string(),
            version: change.version,
            null_version: change.null_version,
            is_delete_marker: change.is_delete_marker,
            object: change.object,
        };
        self.trees.changes().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &sequence.to_be_bytes(),
            &record.encode(),
        )?;
        Ok(())
    }

    /// Deletes up to `limit` of the oldest rows that are past `retention_ms`
    /// at `now_ms` and returns how many it deleted. Rows are in recording
    /// order, so a row recorded before an expired one is at least as old,
    /// whatever its timestamp says (the clock may have stepped back since):
    /// everything up to the last expired row in a window from the head goes.
    /// While any bucket has full-text state, no row the indexer has yet to
    /// apply goes.
    pub fn trim_journal(
        &mut self,
        now_ms: u64,
        retention_ms: u64,
        limit: usize,
    ) -> Result<usize, StorageError> {
        let changes = self.trees.changes();
        let window = changes.scan_range_limit(self.ctx, None, None, trim_window(limit))?;
        let hold = self.fulltext_hold()?;
        let Some(last) = last_expired(&window, now_ms, retention_ms, hold)? else {
            return Ok(0);
        };
        let mut trimmed = 0;
        for (key, _) in window.iter().take((last + 1).min(limit)) {
            changes.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
            trimmed += 1;
        }
        Ok(trimmed)
    }
}

/// Rows a trim looks at from the head to find the last expired one: a few
/// batches, so a short run of rows stamped by a clock that ran ahead does not
/// hold up the rows recorded after them.
fn trim_window(limit: usize) -> usize {
    limit.saturating_mul(4).max(64)
}

/// The index of the last row in `rows` (journal rows from the head) past
/// `retention_ms` at `now_ms`, and numbered at most `hold` when one is given.
fn last_expired(
    rows: &[(Vec<u8>, Vec<u8>)],
    now_ms: u64,
    retention_ms: u64,
    hold: Option<u64>,
) -> Result<Option<usize>, StorageError> {
    let mut last = None;
    for (index, (key, row)) in rows.iter().enumerate() {
        let sequence = u64::from_be_bytes(key.as_slice().try_into().map_err(|_| corrupt())?);
        if hold.is_some_and(|hold| sequence > hold) {
            break;
        }
        let record = JournalRecord::decode(sequence, row)?;
        if record.timestamp_ms.saturating_add(retention_ms) <= now_ms {
            last = Some(index);
        }
    }
    Ok(last)
}

impl ObjFile<'_> {
    /// Whether a trim at `now_ms` would delete anything: a read, so a sweep
    /// with nothing to trim writes nothing (not even the log).
    pub fn journal_due(&mut self, now_ms: u64, retention_ms: u64) -> Result<bool, StorageError> {
        let Some(changes) = self.existing_tree(CHANGES_TREE)? else {
            return Ok(false);
        };
        let window =
            changes.scan_range_limit(&mut self.core.page_ctx(0), None, None, trim_window(1))?;
        let hold = self.fulltext_hold()?;
        Ok(last_expired(&window, now_ms, retention_ms, hold)?.is_some())
    }

    /// Up to `limit` journal rows numbered after `after`, oldest first.
    pub fn journal(
        &mut self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<JournalRecord>, StorageError> {
        let Some(changes) = self.existing_tree(CHANGES_TREE)? else {
            return Ok(Vec::new());
        };
        let Some(from) = after.checked_add(1) else {
            return Ok(Vec::new());
        };
        changes
            .scan_range_limit(
                &mut self.core.page_ctx(0),
                Some(&from.to_be_bytes()),
                None,
                limit,
            )?
            .into_iter()
            .map(|(key, row)| {
                let sequence =
                    u64::from_be_bytes(key.as_slice().try_into().map_err(|_| corrupt())?);
                JournalRecord::decode(sequence, &row)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_in_every_shape() {
        let object = JournalObject {
            size: 12,
            md5: [9; 16],
            parts: 3,
            last_modified_ms: 77,
        };
        for record in [
            JournalRecord {
                sequence: 1,
                record_type: RecordType::Create,
                timestamp_ms: 5,
                bucket: "bucket-a".into(),
                key: "dir/ké y".into(),
                version: 40,
                null_version: true,
                is_delete_marker: Some(false),
                object: Some(object),
            },
            JournalRecord {
                sequence: 2,
                record_type: RecordType::Delete,
                timestamp_ms: 6,
                bucket: "bucket-a".into(),
                key: "k".into(),
                version: 41,
                null_version: false,
                is_delete_marker: Some(true),
                object: Some(JournalObject {
                    size: 0,
                    md5: [0; 16],
                    parts: 0,
                    last_modified_ms: 6,
                }),
            },
            JournalRecord {
                sequence: 3,
                record_type: RecordType::Delete,
                timestamp_ms: 7,
                bucket: "b".repeat(63),
                key: "x".repeat(1024),
                version: 40,
                null_version: false,
                is_delete_marker: None,
                object: None,
            },
        ] {
            let bytes = record.encode();
            assert!(bytes.len() <= 1140);
            assert_eq!(
                JournalRecord::decode(record.sequence, &bytes).unwrap(),
                record
            );
        }
    }
}
