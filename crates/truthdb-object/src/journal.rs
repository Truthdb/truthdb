//! The change journal (S3 Metadata's journal table): one row per change to
//! any bucket's objects, numbered in commit order and written in the same
//! transaction as the change. CREATE records a new version (a PUT, a copy, a
//! completed multipart upload); UPDATE_METADATA a tag change; DELETE a delete
//! marker (`is_delete_marker` TRUE) or a version removed for good (the
//! flag NULL). Replacing a key's null version in place (an overwrite in an
//! unversioned bucket, or a delete in a suspended one) records only the new
//! version or marker. Rows older than a retention period are trimmed. A row
//! made by a request carries its origin: the operation, the request id, the
//! account (and user) that made it, and the address it came from.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::consumers::FULLTEXT_CONSUMER;
use crate::objects::StorageClass;
use crate::{CHANGES_TREE, NEXT_CHANGE_KEY, ObjFile, ObjTxn, next_counter};

/// What a journal row records (AWS's `record_type`), or a notice: an
/// event of an object that is not a change S3 Metadata's journal shows (a
/// restore, a retention change), kept for the event notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Create,
    UpdateMetadata,
    Delete,
    Notice,
}

impl RecordType {
    pub fn as_str(self) -> &'static str {
        match self {
            RecordType::Create => "CREATE",
            RecordType::UpdateMetadata => "UPDATE_METADATA",
            RecordType::Delete => "DELETE",
            RecordType::Notice => "NOTICE",
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
    /// Who made the change; `None` for one made without a request.
    pub origin: Option<Origin>,
    /// A notice's event; `None` for every other row.
    pub notice: Option<Notice>,
}

/// The event a notice row records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// The event type (`s3:ObjectRestore:Post`, ...).
    pub event: String,
    /// The event message's fields particular to it, as a JSON object
    /// (`glacierEventData`, ...).
    pub detail: Option<String>,
}

/// The request a change came from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Origin {
    /// The S3 operation (`PutObject`, `DeleteObject`, ...).
    pub operation: String,
    pub request_id: String,
    /// The account the request acted for, and its user (none for the
    /// account's root); both none for an anonymous request.
    pub account: Option<String>,
    pub user: Option<String>,
    pub source_ip: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalObject {
    pub size: u64,
    pub md5: [u8; 16],
    pub parts: u16,
    pub last_modified_ms: u64,
    pub storage_class: StorageClass,
}

/// The type byte's flag for a row with its key's length and an origin
/// (rows without one keep the original layout).
const TYPE_WITH_ORIGIN: u8 = 0x80;
/// The origin's fields, as `[tag u8][len u16][bytes]` after the key.
const TAG_OPERATION: u8 = 1;
const TAG_REQUEST_ID: u8 = 2;
const TAG_ACCOUNT: u8 = 3;
const TAG_USER: u8 = 4;
const TAG_SOURCE_IP: u8 = 5;
/// A notice's event and detail (a notice row always has the origin layout,
/// its origin fields empty when it was made without a request).
const TAG_EVENT: u8 = 6;
const TAG_DETAIL: u8 = 7;

const FLAG_NULL_VERSION: u8 = 1;
const FLAG_MARKER: u8 = 2;
/// A version removed for good: no marker flag, no object columns.
const FLAG_PERMANENT: u8 = 4;
/// Bits 3-5: the version's storage class (STANDARD, 0, in rows from before).
const CLASS_SHIFT: u8 = 3;

fn corrupt() -> StorageError {
    StorageError::InvalidFile("corrupt journal row".to_string())
}

impl JournalRecord {
    /// `[type][flags][time][version][size][md5][parts][mtime][bucket len]
    /// [bucket][key]`: at most 1,140 bytes (a 63-byte bucket name and a
    /// 1,024-byte key), within a tree cell. With an origin, the type byte
    /// has `TYPE_WITH_ORIGIN` set and the key is `[len u16][key]`, followed by
    /// the origin's fields: about 200 bytes more at most.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(60 + self.bucket.len() + self.key.len());
        out.push(
            match self.record_type {
                RecordType::Create => 0,
                RecordType::UpdateMetadata => 1,
                RecordType::Delete => 2,
                RecordType::Notice => 3,
            } | if self.origin.is_some() || self.notice.is_some() {
                TYPE_WITH_ORIGIN
            } else {
                0
            },
        );
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
            } | self
                .object
                .map_or(0, |object| object.storage_class.code() << CLASS_SHIFT),
        );
        out.extend_from_slice(&self.timestamp_ms.to_le_bytes());
        out.extend_from_slice(&self.version.to_le_bytes());
        let object = self.object.unwrap_or(JournalObject {
            size: 0,
            md5: [0; 16],
            parts: 0,
            last_modified_ms: 0,
            storage_class: StorageClass::Standard,
        });
        out.extend_from_slice(&object.size.to_le_bytes());
        out.extend_from_slice(&object.md5);
        out.extend_from_slice(&object.parts.to_le_bytes());
        out.extend_from_slice(&object.last_modified_ms.to_le_bytes());
        out.push(self.bucket.len() as u8);
        out.extend_from_slice(self.bucket.as_bytes());
        if self.origin.is_none() && self.notice.is_none() {
            out.extend_from_slice(self.key.as_bytes());
            return out;
        }
        let origin = self.origin.clone().unwrap_or_default();
        out.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        out.extend_from_slice(self.key.as_bytes());
        for (tag, value) in [
            (TAG_OPERATION, Some(&origin.operation)),
            (TAG_REQUEST_ID, Some(&origin.request_id)),
            (TAG_ACCOUNT, origin.account.as_ref()),
            (TAG_USER, origin.user.as_ref()),
            (TAG_SOURCE_IP, origin.source_ip.as_ref()),
            (TAG_EVENT, self.notice.as_ref().map(|notice| &notice.event)),
            (
                TAG_DETAIL,
                self.notice
                    .as_ref()
                    .and_then(|notice| notice.detail.as_ref()),
            ),
        ] {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                out.push(tag);
                out.extend_from_slice(&(value.len() as u16).to_le_bytes());
                out.extend_from_slice(value.as_bytes());
            }
        }
        out
    }

    fn decode(sequence: u64, bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() < 53 {
            return Err(corrupt());
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let with_origin = bytes[0] & TYPE_WITH_ORIGIN != 0;
        let record_type = match bytes[0] & !TYPE_WITH_ORIGIN {
            0 => RecordType::Create,
            1 => RecordType::UpdateMetadata,
            2 => RecordType::Delete,
            3 => RecordType::Notice,
            _ => return Err(corrupt()),
        };
        let flags = bytes[1];
        let bucket_len = bytes[52] as usize;
        let names = &bytes[53..];
        if names.len() < bucket_len {
            return Err(corrupt());
        }
        let text = |b: &[u8]| String::from_utf8(b.to_vec()).map_err(|_| corrupt());
        let rest = &names[bucket_len..];
        let mut event = None;
        let mut detail = None;
        let (key, origin) = if with_origin {
            let (key, mut fields) = sized(rest).ok_or_else(corrupt)?;
            let mut origin = Origin::default();
            while !fields.is_empty() {
                let tag = fields[0];
                let (value, after) = sized(&fields[1..]).ok_or_else(corrupt)?;
                let value = text(value)?;
                match tag {
                    TAG_OPERATION => origin.operation = value,
                    TAG_REQUEST_ID => origin.request_id = value,
                    TAG_ACCOUNT => origin.account = Some(value),
                    TAG_USER => origin.user = Some(value),
                    TAG_SOURCE_IP => origin.source_ip = Some(value),
                    TAG_EVENT => event = Some(value),
                    TAG_DETAIL => detail = Some(value),
                    // A field a later release added.
                    _ => {}
                }
                fields = after;
            }
            (text(key)?, Some(origin))
        } else {
            (text(rest)?, None)
        };
        let permanent = flags & FLAG_PERMANENT != 0;
        Ok(JournalRecord {
            sequence,
            record_type,
            timestamp_ms: u64_at(2),
            version: u64_at(10),
            null_version: flags & FLAG_NULL_VERSION != 0,
            is_delete_marker: (!permanent).then_some(flags & FLAG_MARKER != 0),
            object: (!permanent).then(|| JournalObject {
                storage_class: StorageClass::from_code(flags >> CLASS_SHIFT),
                size: u64_at(18),
                md5: bytes[26..42].try_into().unwrap(),
                parts: u16::from_le_bytes([bytes[42], bytes[43]]),
                last_modified_ms: u64_at(44),
            }),
            bucket: text(&names[..bucket_len])?,
            key,
            origin,
            notice: match (record_type, event) {
                (RecordType::Notice, Some(event)) => Some(Notice { event, detail }),
                (RecordType::Notice, None) => return Err(corrupt()),
                _ => None,
            },
        })
    }
}

/// `[len u16][bytes]` at the head of `bytes`: the bytes, and what follows.
fn sized(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = u16::from_le_bytes(bytes.get(..2)?.try_into().unwrap()) as usize;
    let value = bytes.get(2..2 + len)?;
    Some((value, &bytes[2 + len..]))
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
            origin: self.origin.clone(),
            notice: None,
        };
        self.append_record(&record)
    }

    /// Records a notice of an event of `bucket`'s `key` (version `version`)
    /// as the journal's next row, in this transaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_notice(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        null_version: bool,
        object: Option<JournalObject>,
        notice: Notice,
        now_ms: u64,
    ) -> Result<(), StorageError> {
        let sequence = next_counter(self, NEXT_CHANGE_KEY)?;
        let record = JournalRecord {
            sequence,
            record_type: RecordType::Notice,
            timestamp_ms: now_ms,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            null_version,
            is_delete_marker: Some(false),
            object,
            origin: self.origin.clone(),
            notice: Some(notice),
        };
        self.append_record(&record)
    }

    fn append_record(&mut self, record: &JournalRecord) -> Result<(), StorageError> {
        let sequence = record.sequence;
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
    /// No row a consumer has yet to apply goes, unless it is past `cap_ms`
    /// too: a consumer still before it is then moved past it, the rows it
    /// missed counted (and the full-text index rebuilt).
    pub fn trim_journal(
        &mut self,
        now_ms: u64,
        retention_ms: u64,
        cap_ms: u64,
        limit: usize,
    ) -> Result<usize, StorageError> {
        let changes = self.trees.changes();
        let window = changes.scan_range_limit(self.ctx, None, None, trim_window(limit))?;
        let consumers = self.consumers()?;
        let hold = consumers.iter().map(|consumer| consumer.cursor).min();
        let Some(last) = last_expired(&window, now_ms, retention_ms, cap_ms, hold)? else {
            return Ok(0);
        };
        let take = (last + 1).min(limit);
        for (key, _) in window.iter().take(take) {
            changes.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        let through = sequence_of(&window[take - 1].0)?;
        for mut consumer in consumers {
            if consumer.cursor < through {
                consumer.dropped += through - consumer.cursor;
                consumer.cursor = through;
                self.put_consumer(&consumer)?;
                if consumer.name == FULLTEXT_CONSUMER {
                    self.refill_fulltext()?;
                }
            }
        }
        Ok(take)
    }
}

fn sequence_of(key: &[u8]) -> Result<u64, StorageError> {
    Ok(u64::from_be_bytes(key.try_into().map_err(|_| corrupt())?))
}

/// Rows a trim looks at from the head to find the last expired one: a few
/// batches, so a short run of rows stamped by a clock that ran ahead does not
/// hold up the rows recorded after them.
fn trim_window(limit: usize) -> usize {
    limit.saturating_mul(4).max(64)
}

/// The index of the last row in `rows` (journal rows from the head) that
/// may go at `now_ms`: past `retention_ms` and numbered at most `hold` when
/// one is given, or past `cap_ms` whatever the hold.
fn last_expired(
    rows: &[(Vec<u8>, Vec<u8>)],
    now_ms: u64,
    retention_ms: u64,
    cap_ms: u64,
    hold: Option<u64>,
) -> Result<Option<usize>, StorageError> {
    let mut last = None;
    for (index, (key, row)) in rows.iter().enumerate() {
        let sequence = sequence_of(key)?;
        let record = JournalRecord::decode(sequence, row)?;
        let held = hold.is_some_and(|hold| sequence > hold);
        let expired = record.timestamp_ms.saturating_add(retention_ms) <= now_ms;
        let capped = record.timestamp_ms.saturating_add(cap_ms) <= now_ms;
        if (expired && !held) || capped {
            last = Some(index);
        }
    }
    Ok(last)
}

impl ObjFile<'_> {
    /// The journal's last sequence number handed out (0 before any).
    pub fn last_change(&mut self) -> Result<u64, StorageError> {
        let Some(meta) = self.eng.root.map(crate::meta_tree) else {
            return Ok(0);
        };
        self.core.ensure_rel_usable()?;
        Ok(
            match meta.get(&mut self.core.page_ctx(0), NEXT_CHANGE_KEY)? {
                Some(next) => crate::codec::u64_le(&next)?.saturating_sub(1),
                None => 0,
            },
        )
    }

    /// Whether a trim at `now_ms` would delete anything: a read, so a sweep
    /// with nothing to trim writes nothing (not even the log).
    pub fn journal_due(
        &mut self,
        now_ms: u64,
        retention_ms: u64,
        cap_ms: u64,
    ) -> Result<bool, StorageError> {
        let Some(changes) = self.existing_tree(CHANGES_TREE)? else {
            return Ok(false);
        };
        let window =
            changes.scan_range_limit(&mut self.core.page_ctx(0), None, None, trim_window(1))?;
        let hold = self
            .journal_consumers()?
            .iter()
            .map(|consumer| consumer.cursor)
            .min();
        Ok(last_expired(&window, now_ms, retention_ms, cap_ms, hold)?.is_some())
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
            storage_class: StorageClass::GlacierIr,
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
                origin: None,
                notice: None,
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
                    storage_class: StorageClass::Standard,
                }),
                origin: None,
                notice: None,
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
                origin: None,
                notice: None,
            },
        ] {
            let bytes = record.encode();
            assert!(bytes.len() <= 1140);
            assert_eq!(
                bytes[0] & TYPE_WITH_ORIGIN,
                0,
                "no origin: the first layout"
            );
            assert_eq!(
                JournalRecord::decode(record.sequence, &bytes).unwrap(),
                record
            );
        }
    }

    #[test]
    fn a_record_with_its_origin_round_trips_within_a_tree_cell() {
        let full = Origin {
            operation: "CompleteMultipartUpload".into(),
            request_id: "0123456789ABCDEF".into(),
            account: Some("123456789012".into()),
            user: Some("u".repeat(64)),
            source_ip: Some("ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255".into()),
        };
        let anonymous = Origin {
            operation: "PostObject".into(),
            request_id: "FEDCBA9876543210".into(),
            account: None,
            user: None,
            source_ip: Some("192.0.2.1".into()),
        };
        for origin in [full, anonymous] {
            let record = JournalRecord {
                sequence: 9,
                record_type: RecordType::Delete,
                timestamp_ms: 7,
                bucket: "b".repeat(63),
                key: "x".repeat(1024),
                version: 40,
                null_version: false,
                is_delete_marker: None,
                object: None,
                origin: Some(origin),
                notice: None,
            };
            let bytes = record.encode();
            assert!(
                bytes.len() + 8 <= truthdb_core::relstore::btree::TREE_MAX_CELL,
                "{} bytes",
                bytes.len()
            );
            assert_eq!(JournalRecord::decode(9, &bytes).unwrap(), record);
        }
        // A field a later release added is skipped; a field cut short is
        // corrupt.
        let record = JournalRecord {
            sequence: 1,
            record_type: RecordType::Create,
            timestamp_ms: 5,
            bucket: "a".into(),
            key: "k".into(),
            version: 1,
            null_version: false,
            is_delete_marker: Some(false),
            object: None,
            origin: Some(Origin {
                operation: "PutObject".into(),
                ..Origin::default()
            }),
            notice: None,
        };
        let mut bytes = record.encode();
        bytes.extend_from_slice(&[99, 2, 0, b'z', b'z']);
        let decoded = JournalRecord::decode(1, &bytes).unwrap();
        assert_eq!(decoded.origin, record.origin);
        bytes.truncate(bytes.len() - 1);
        assert!(JournalRecord::decode(1, &bytes).is_err());
    }

    #[test]
    fn a_notice_round_trips_with_or_without_a_request() {
        for origin in [
            None,
            Some(Origin {
                operation: "RestoreObject".into(),
                request_id: "0123456789ABCDEF".into(),
                account: Some("123456789012".into()),
                user: None,
                source_ip: Some("10.0.0.1".into()),
            }),
        ] {
            let record = JournalRecord {
                sequence: 3,
                record_type: RecordType::Notice,
                timestamp_ms: 5,
                bucket: "b".into(),
                key: "k".into(),
                version: 2,
                null_version: true,
                is_delete_marker: Some(false),
                object: Some(JournalObject {
                    size: 9,
                    md5: [1; 16],
                    parts: 0,
                    last_modified_ms: 4,
                    storage_class: StorageClass::Glacier,
                }),
                origin: origin.clone(),
                notice: Some(Notice {
                    event: "s3:ObjectRestore:Completed".into(),
                    detail: Some("{\"glacierEventData\":{}}".into()),
                }),
            };
            let decoded = JournalRecord::decode(3, &record.encode()).unwrap();
            // A notice made without a request reads back with empty origin
            // fields.
            assert_eq!(decoded.origin, Some(origin.unwrap_or_default()));
            assert_eq!(decoded.notice, record.notice);
            assert_eq!(decoded.object, record.object);
        }
        // A notice row without its event is corrupt.
        let mut bare = JournalRecord::decode(
            1,
            &JournalRecord {
                sequence: 1,
                record_type: RecordType::Create,
                timestamp_ms: 5,
                bucket: "b".into(),
                key: "k".into(),
                version: 1,
                null_version: false,
                is_delete_marker: Some(false),
                object: None,
                origin: Some(Origin::default()),
                notice: None,
            }
            .encode(),
        )
        .unwrap()
        .encode();
        bare[0] = 3 | TYPE_WITH_ORIGIN;
        assert!(JournalRecord::decode(1, &bare).is_err());
    }
}
