//! S3 Storage Lens's daily metrics: each bucket's request counts by day
//! (the activity metrics of a day's report), each configuration's reports
//! by day, and the day of each configuration's last report.
//!
//! A configuration is named by its key, `<account>/<id>` (see `truthdb-s3`'s
//! `storage_lens`). A report and the day it is recorded as made are written
//! in one transaction: a report is there whole or not at all.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{LENS_ACTIVITY_TREE, LENS_REPORTS_TREE, LENS_STATE_TREE, ObjFile, ObjTxn};

/// The days a report, and a bucket's request counts, are kept: 15 months
/// ("All S3 Storage Lens metrics are retained for a period of 15 months").
pub const LENS_KEEP_DAYS: u32 = 455;

/// A bucket's requests of one day, in this order: all, GET, PUT, DELETE,
/// HEAD, POST, LIST, bytes downloaded, bytes uploaded, 4xx and 5xx
/// answers.
pub type ActivityCounts = [u64; 11];

/// What a report's record is of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LensRecordType {
    Account,
    Bucket,
    Prefix,
}

impl LensRecordType {
    pub fn as_str(self) -> &'static str {
        match self {
            LensRecordType::Account => "ACCOUNT",
            LensRecordType::Bucket => "BUCKET",
            LensRecordType::Prefix => "PREFIX",
        }
    }

    fn code(self) -> u8 {
        self as u8
    }

    fn of(code: u8) -> Option<LensRecordType> {
        [
            LensRecordType::Account,
            LensRecordType::Bucket,
            LensRecordType::Prefix,
        ]
        .get(code as usize)
        .copied()
    }
}

/// One metric of a report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LensRecord {
    pub record_type: LensRecordType,
    /// Its bucket (empty: an account's record).
    pub bucket: String,
    /// The prefix of a prefix's record (empty otherwise).
    pub record_value: String,
    /// The storage class it counts (empty: a metric of no storage class).
    pub storage_class: String,
    pub metric: String,
    pub value: u64,
}

/// A report's record as listed: its configuration's key and its day.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LensRow {
    pub config: String,
    /// Days since 1970-01-01 (UTC).
    pub day: u32,
    pub record: LensRecord,
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt Storage Lens {what}"))
}

fn activity_key(bucket: &str, day: u32) -> Vec<u8> {
    [bucket.as_bytes(), &[0], &day.to_be_bytes()].concat()
}

/// A configuration's reports lie under `config, 0` (a key holds no NUL).
fn report_prefix(config: &str) -> Vec<u8> {
    [config.as_bytes(), &[0]].concat()
}

fn day_prefix(config: &str, day: u32) -> Vec<u8> {
    [&report_prefix(config)[..], &day.to_be_bytes()].concat()
}

/// A field of a record's key: its length (two bytes), then it.
fn push_field(key: &mut Vec<u8>, field: &str) {
    key.extend_from_slice(&(field.len() as u16).to_be_bytes());
    key.extend_from_slice(field.as_bytes());
}

fn record_key(config: &str, day: u32, record: &LensRecord) -> Vec<u8> {
    let mut key = day_prefix(config, day);
    key.push(record.record_type.code());
    for field in [
        &record.bucket,
        &record.record_value,
        &record.storage_class,
        &record.metric,
    ] {
        push_field(&mut key, field);
    }
    key
}

fn decode_row(key: &[u8], value: &[u8]) -> Result<LensRow, StorageError> {
    let split = key
        .iter()
        .position(|&byte| byte == 0)
        .ok_or_else(|| corrupt("report"))?;
    let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("report"));
    let config = text(&key[..split])?;
    let rest = &key[split + 1..];
    let (day, rest) = rest
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("report"))?;
    let (&code, mut rest) = rest.split_first().ok_or_else(|| corrupt("report"))?;
    let mut fields = Vec::with_capacity(4);
    for _ in 0..4 {
        let (length, after) = rest
            .split_first_chunk::<2>()
            .ok_or_else(|| corrupt("report"))?;
        let length = u16::from_be_bytes(*length) as usize;
        if after.len() < length {
            return Err(corrupt("report"));
        }
        fields.push(text(&after[..length])?);
        rest = &after[length..];
    }
    let value: [u8; 8] = value.try_into().map_err(|_| corrupt("report"))?;
    let [bucket, record_value, storage_class, metric]: [String; 4] =
        fields.try_into().map_err(|_| corrupt("report"))?;
    Ok(LensRow {
        config,
        day: u32::from_be_bytes(*day),
        record: LensRecord {
            record_type: LensRecordType::of(code).ok_or_else(|| corrupt("report"))?,
            bucket,
            record_value,
            storage_class,
            metric,
            value: u64::from_be_bytes(value),
        },
    })
}

fn decode_counts(row: &[u8]) -> Result<ActivityCounts, StorageError> {
    if row.len() != 88 {
        return Err(corrupt("request counts"));
    }
    let mut counts = [0u64; 11];
    for (count, bytes) in counts.iter_mut().zip(row.chunks_exact(8)) {
        *count = u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
    }
    Ok(counts)
}

impl ObjTxn<'_, '_> {
    /// Adds `add` to `bucket`'s request counts of `day`.
    pub fn add_lens_activity(
        &mut self,
        bucket: &str,
        day: u32,
        add: &ActivityCounts,
    ) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_ACTIVITY_TREE);
        let key = activity_key(bucket, day);
        let mut counts = match tree.get(self.ctx, &key)? {
            Some(row) => decode_counts(&row)?,
            None => [0; 11],
        };
        for (count, more) in counts.iter_mut().zip(add) {
            *count = count.saturating_add(*more);
        }
        let row: Vec<u8> = counts
            .iter()
            .flat_map(|count| count.to_le_bytes())
            .collect();
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &key)?;
        tree.insert_unique(self.ctx, mode, &key, &row)?;
        Ok(())
    }

    /// Drops every bucket's request counts of days before `day`.
    pub fn expire_lens_activity(&mut self, day: u32) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_ACTIVITY_TREE);
        for (key, _) in tree.scan(self.ctx)? {
            let old = key
                .last_chunk::<4>()
                .is_some_and(|stamp| u32::from_be_bytes(*stamp) < day);
            if old {
                tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
            }
        }
        Ok(())
    }

    /// Drops the request counts of a bucket being deleted.
    pub(crate) fn forget_lens_activity(&mut self, bucket: &str) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_ACTIVITY_TREE);
        let lower = [bucket.as_bytes(), &[0]].concat();
        let upper = [bucket.as_bytes(), &[1]].concat();
        for (key, _) in tree.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(())
    }

    /// Records configuration `config`'s report of `day` (one made again
    /// replaces it), records `day` as its last reported, and drops its
    /// reports of more than [`LENS_KEEP_DAYS`] before it.
    pub fn put_lens_report(
        &mut self,
        config: &str,
        day: u32,
        records: &[LensRecord],
    ) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_REPORTS_TREE);
        let prefix = report_prefix(config);
        let until = day_prefix(config, day.saturating_sub(LENS_KEEP_DAYS - 1));
        let (this, next) = (day_prefix(config, day), day_prefix(config, day + 1));
        for (lower, upper) in [(&prefix, &until), (&this, &next)] {
            for (key, _) in tree.scan_range(self.ctx, Some(lower), Some(upper))? {
                tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
            }
        }
        for record in records {
            let key = record_key(config, day, record);
            let mode = &mut OpMode::Txn(self.txn);
            // A record given twice keeps the last.
            tree.delete(self.ctx, mode, &key)?;
            tree.insert_unique(self.ctx, mode, &key, &record.value.to_be_bytes())?;
        }
        let state = self.trees.tree(LENS_STATE_TREE);
        let mode = &mut OpMode::Txn(self.txn);
        state.delete(self.ctx, mode, config.as_bytes())?;
        state.insert_unique(self.ctx, mode, config.as_bytes(), &day.to_be_bytes())?;
        Ok(())
    }

    /// Drops configuration `config`'s reports and its last reported day
    /// (it is deleted).
    pub fn forget_lens(&mut self, config: &str) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_REPORTS_TREE);
        let lower = report_prefix(config);
        let upper = [config.as_bytes(), &[1]].concat();
        for (key, _) in tree.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        self.trees.tree(LENS_STATE_TREE).delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            config.as_bytes(),
        )?;
        Ok(())
    }
}

impl ObjFile<'_> {
    /// `bucket`'s request counts of `day`, if any were counted.
    pub fn lens_activity(
        &mut self,
        bucket: &str,
        day: u32,
    ) -> Result<Option<ActivityCounts>, StorageError> {
        let Some(tree) = self.existing_tree(LENS_ACTIVITY_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &activity_key(bucket, day))?
            .map(|row| decode_counts(&row))
            .transpose()
    }

    /// The day of configuration `config`'s last report, if one was made.
    pub fn lens_reported(&mut self, config: &str) -> Result<Option<u32>, StorageError> {
        let Some(tree) = self.existing_tree(LENS_STATE_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), config.as_bytes())?
            .map(|row| {
                row.as_slice()
                    .try_into()
                    .map(u32::from_be_bytes)
                    .map_err(|_| corrupt("state"))
            })
            .transpose()
    }

    /// Configuration `config`'s report of `day`, in key order.
    pub fn lens_report(&mut self, config: &str, day: u32) -> Result<Vec<LensRecord>, StorageError> {
        let Some(tree) = self.existing_tree(LENS_REPORTS_TREE)? else {
            return Ok(Vec::new());
        };
        let (lower, upper) = (day_prefix(config, day), day_prefix(config, day + 1));
        tree.scan_range(&mut self.core.page_ctx(0), Some(&lower), Some(&upper))?
            .into_iter()
            .map(|(key, value)| decode_row(&key, &value).map(|row| row.record))
            .collect()
    }

    /// Up to `limit` reports' records of every configuration, in key order,
    /// after `after` (a key this returned as the next to resume from).
    pub fn lens_rows(
        &mut self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<LensRow>, Option<Vec<u8>>), StorageError> {
        let Some(tree) = self.existing_tree(LENS_REPORTS_TREE)? else {
            return Ok((Vec::new(), None));
        };
        let lower = after.map(|key| [key, &[0]].concat());
        let page = tree.scan_range_limit(
            &mut self.core.page_ctx(0),
            lower.as_deref(),
            None,
            limit + 1,
        )?;
        let next = (page.len() > limit).then(|| page[limit - 1].0.clone());
        let rows = page
            .iter()
            .take(limit)
            .map(|(key, value)| decode_row(key, value))
            .collect::<Result<_, _>>()?;
        Ok((rows, next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_records_key_reads_back() {
        let record = LensRecord {
            record_type: LensRecordType::Prefix,
            bucket: "data".into(),
            record_value: "logs/\u{0}x/".into(),
            storage_class: "STANDARD".into(),
            metric: "StorageBytes".into(),
            value: 42,
        };
        let key = record_key("111111111111/lens", 20_000, &record);
        let row = decode_row(&key, &42u64.to_be_bytes()).unwrap();
        assert_eq!(row.config, "111111111111/lens");
        assert_eq!(row.day, 20_000);
        assert_eq!(row.record, record);
        // A configuration's keys sort by day, then by record.
        assert!(day_prefix("a/b", 1) < day_prefix("a/b", 2));
        assert!(key.starts_with(&day_prefix("111111111111/lens", 20_000)));
        assert!(!day_prefix("a/bc", 0).starts_with(&report_prefix("a/b")));
    }
}
