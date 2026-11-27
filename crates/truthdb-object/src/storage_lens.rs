//! S3 Storage Lens's daily metrics: each bucket's request counts by day
//! (the activity metrics of a day's report), each configuration's reports
//! by day, and each configuration's state: its generation and the day of
//! its last report.
//!
//! A configuration is named by its key, `<account>/<id>` (see `truthdb-s3`'s
//! `storage_lens`). Its reports are kept under its generation, given when it
//! is put: one deleted (its state dropped) and put again has a new one, and
//! the old reports are no longer read. No transaction here grows with what
//! is kept: a report is written in batches, its day recorded as reported
//! last, and a day's records are read only once it is; what is no longer
//! read (another generation's, or past keeping) is dropped in batches.

use std::collections::HashMap;

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{LENS_ACTIVITY_TREE, LENS_REPORTS_TREE, LENS_STATE_TREE, ObjFile, ObjTxn};

/// The days a report, and a bucket's request counts, are kept: 15 months
/// ("All S3 Storage Lens metrics are retained for a period of 15 months").
pub const LENS_KEEP_DAYS: u32 = 455;

/// The rows one transaction here writes or drops at most, so that it (and
/// its rollback) fits the log.
pub const LENS_BATCH: usize = 1000;

/// The bytes of keys one transaction here writes or drops at most (a
/// prefix's record holds up to a key's 1024 bytes).
pub const LENS_BATCH_BYTES: usize = 64 * 1024;

/// The ends of `records`' batches (of configuration `config`'s report):
/// each at most [`LENS_BATCH`] records of at most [`LENS_BATCH_BYTES`] of
/// keys (one record at least).
pub fn lens_batches(config: &str, records: &[LensRecord]) -> Vec<usize> {
    let mut ends = Vec::new();
    let (mut count, mut bytes) = (0, 0);
    for (at, record) in records.iter().enumerate() {
        // As `record_key` makes it: the configuration, a NUL, the
        // generation, the day, the type, four lengths and the fields.
        let size = config.len()
            + 1
            + 8
            + 4
            + 1
            + 4 * 2
            + record.bucket.len()
            + record.record_value.len()
            + record.storage_class.len()
            + record.metric.len();
        if count > 0 && (count == LENS_BATCH || bytes + size > LENS_BATCH_BYTES) {
            ends.push(at);
            (count, bytes) = (0, 0);
        }
        count += 1;
        bytes += size;
    }
    if count > 0 {
        ends.push(records.len());
    }
    ends
}

/// The rows of `rows` a batch drops: at most [`LENS_BATCH`] of at most
/// [`LENS_BATCH_BYTES`] of keys (one at least); and whether that is fewer
/// than all.
fn take_batch<T>(rows: &[(Vec<u8>, T)], count: usize) -> (&[(Vec<u8>, T)], bool) {
    let (mut taken, mut bytes) = (0, 0);
    for (key, _) in rows.iter().take(count) {
        if taken > 0 && bytes + key.len() > LENS_BATCH_BYTES {
            break;
        }
        taken += 1;
        bytes += key.len();
    }
    (&rows[..taken], taken < rows.len())
}

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

/// A configuration's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LensState {
    pub generation: u64,
    /// The day of its last report, if one was made.
    pub reported: Option<u32>,
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt Storage Lens {what}"))
}

/// Request counts lie by day, then bucket: what has expired is a range.
fn activity_key(bucket: &str, day: u32) -> Vec<u8> {
    [&day.to_be_bytes()[..], bucket.as_bytes()].concat()
}

/// A generation's reports lie under `config, 0, generation` (a key holds no
/// NUL).
fn generation_prefix(config: &str, generation: u64) -> Vec<u8> {
    [config.as_bytes(), &[0], &generation.to_be_bytes()].concat()
}

fn day_prefix(config: &str, generation: u64, day: u32) -> Vec<u8> {
    [
        &generation_prefix(config, generation)[..],
        &day.to_be_bytes(),
    ]
    .concat()
}

/// A field of a record's key: its length (two bytes), then it.
fn push_field(key: &mut Vec<u8>, field: &str) {
    key.extend_from_slice(&(field.len() as u16).to_be_bytes());
    key.extend_from_slice(field.as_bytes());
}

fn record_key(config: &str, generation: u64, day: u32, record: &LensRecord) -> Vec<u8> {
    let mut key = day_prefix(config, generation, day);
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

/// A report key's configuration, generation and day.
fn key_head(key: &[u8]) -> Result<(String, u64, u32), StorageError> {
    let split = key
        .iter()
        .position(|&byte| byte == 0)
        .ok_or_else(|| corrupt("report"))?;
    let config = String::from_utf8(key[..split].to_vec()).map_err(|_| corrupt("report"))?;
    let rest = &key[split + 1..];
    let (generation, rest) = rest
        .split_first_chunk::<8>()
        .ok_or_else(|| corrupt("report"))?;
    let (day, _) = rest
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("report"))?;
    Ok((
        config,
        u64::from_be_bytes(*generation),
        u32::from_be_bytes(*day),
    ))
}

fn decode_row(key: &[u8], value: &[u8]) -> Result<LensRow, StorageError> {
    let (config, _, day) = key_head(key)?;
    let rest = &key[config.len() + 1 + 8 + 4..];
    let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("report"));
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
        day,
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

/// A request counts row: its bucket's creation time, then the counts.
fn decode_counts(row: &[u8]) -> Result<(u64, ActivityCounts), StorageError> {
    if row.len() != 96 {
        return Err(corrupt("request counts"));
    }
    let created = u64::from_le_bytes(row[..8].try_into().expect("eight bytes"));
    let mut counts = [0u64; 11];
    for (count, bytes) in counts.iter_mut().zip(row[8..].chunks_exact(8)) {
        *count = u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
    }
    Ok((created, counts))
}

fn encode_state(state: &LensState) -> Vec<u8> {
    [
        &state.generation.to_be_bytes()[..],
        &state.reported.unwrap_or(u32::MAX).to_be_bytes(),
    ]
    .concat()
}

fn decode_state(row: &[u8]) -> Result<LensState, StorageError> {
    let (generation, day) = row
        .split_first_chunk::<8>()
        .ok_or_else(|| corrupt("state"))?;
    let day: [u8; 4] = day.try_into().map_err(|_| corrupt("state"))?;
    let day = u32::from_be_bytes(day);
    Ok(LensState {
        generation: u64::from_be_bytes(*generation),
        reported: (day != u32::MAX).then_some(day),
    })
}

/// Whether a record of `day` under `generation` is read: its configuration
/// still has that generation, and has reported that day.
fn visible(state: Option<&LensState>, generation: u64, day: u32) -> bool {
    state.is_some_and(|state| {
        state.generation == generation && state.reported.is_some_and(|last| day <= last)
    })
}

impl ObjTxn<'_, '_> {
    /// Adds `add` to the request counts of `day` of bucket `bucket` (made
    /// at `created_ms`); counts of a bucket of the name made before are
    /// replaced.
    pub fn add_lens_activity(
        &mut self,
        bucket: &str,
        created_ms: u64,
        day: u32,
        add: &ActivityCounts,
    ) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_ACTIVITY_TREE);
        let key = activity_key(bucket, day);
        let mut counts = match tree.get(self.ctx, &key)? {
            Some(row) => match decode_counts(&row)? {
                (created, counts) if created == created_ms => counts,
                _ => [0; 11],
            },
            None => [0; 11],
        };
        for (count, more) in counts.iter_mut().zip(add) {
            *count = count.saturating_add(*more);
        }
        let row: Vec<u8> = created_ms
            .to_le_bytes()
            .into_iter()
            .chain(counts.iter().flat_map(|count| count.to_le_bytes()))
            .collect();
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &key)?;
        tree.insert_unique(self.ctx, mode, &key, &row)?;
        Ok(())
    }

    /// Drops up to [`LENS_BATCH`] request counts of days before `day`;
    /// whether more remain.
    pub fn expire_lens_activity(&mut self, day: u32) -> Result<bool, StorageError> {
        let tree = self.trees.tree(LENS_ACTIVITY_TREE);
        let upper = day.to_be_bytes();
        let old = tree.scan_range_limit(self.ctx, None, Some(&upper), LENS_BATCH + 1)?;
        let old: Vec<_> = old
            .into_iter()
            .filter(|(key, _)| key.as_slice() < &upper[..])
            .collect();
        let (batch, more) = take_batch(&old, LENS_BATCH);
        for (key, _) in batch {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        Ok(more)
    }

    /// Gives configuration `config` (just put) a state, of `generation`
    /// and no report, unless it has one (it is replaced).
    pub fn track_lens(&mut self, config: &str, generation: u64) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_STATE_TREE);
        if tree.get(self.ctx, config.as_bytes())?.is_none() {
            let state = LensState {
                generation,
                reported: None,
            };
            tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                config.as_bytes(),
                &encode_state(&state),
            )?;
        }
        Ok(())
    }

    /// Drops configuration `config`'s state (it is deleted): its reports
    /// are no longer read, and are dropped by [`Self::lens_maintain`].
    pub fn forget_lens(&mut self, config: &str) -> Result<(), StorageError> {
        self.trees.tree(LENS_STATE_TREE).delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            config.as_bytes(),
        )?;
        Ok(())
    }

    /// Drops up to [`LENS_BATCH`] records of `config`'s `generation` of
    /// days from `from_day` on (a report not finished, to be made again);
    /// whether more remain.
    pub fn clear_lens_days(
        &mut self,
        config: &str,
        generation: u64,
        from_day: u32,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.tree(LENS_REPORTS_TREE);
        let lower = day_prefix(config, generation, from_day);
        let upper = generation_prefix(config, generation + 1);
        let rows = tree.scan_range_limit(self.ctx, Some(&lower), Some(&upper), LENS_BATCH + 1)?;
        let rows: Vec<_> = rows.into_iter().filter(|(key, _)| *key < upper).collect();
        let (batch, more) = take_batch(&rows, LENS_BATCH);
        for (key, _) in batch {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        Ok(more)
    }

    /// Writes records of `config`'s report of `day` under `generation`
    /// (a batch of [`lens_batches`] in one transaction); a record written
    /// again keeps the last.
    pub fn put_lens_records(
        &mut self,
        config: &str,
        generation: u64,
        day: u32,
        records: &[LensRecord],
    ) -> Result<(), StorageError> {
        let tree = self.trees.tree(LENS_REPORTS_TREE);
        for record in records {
            let key = record_key(config, generation, day, record);
            let mode = &mut OpMode::Txn(self.txn);
            tree.delete(self.ctx, mode, &key)?;
            tree.insert_unique(self.ctx, mode, &key, &record.value.to_be_bytes())?;
        }
        Ok(())
    }

    /// Records `day` as `config`'s last report, if it still has
    /// `generation`: its records of that day are read from now on.
    pub fn lens_reported(
        &mut self,
        config: &str,
        generation: u64,
        day: u32,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.tree(LENS_STATE_TREE);
        let Some(row) = tree.get(self.ctx, config.as_bytes())? else {
            return Ok(false);
        };
        if decode_state(&row)?.generation != generation {
            return Ok(false);
        }
        let state = LensState {
            generation,
            reported: Some(day),
        };
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, config.as_bytes())?;
        tree.insert_unique(self.ctx, mode, config.as_bytes(), &encode_state(&state))?;
        Ok(true)
    }

    /// Drops up to [`LENS_BATCH`] records no longer read: of a generation
    /// no configuration has, or of a day before `keep_from`; whether more
    /// remain.
    pub fn lens_maintain(&mut self, keep_from: u32) -> Result<bool, StorageError> {
        let (reports, states) = (
            self.trees.tree(LENS_REPORTS_TREE),
            self.trees.tree(LENS_STATE_TREE),
        );
        let (mut budget, mut bytes) = (LENS_BATCH, LENS_BATCH_BYTES);
        let mut lower: Option<Vec<u8>> = None;
        loop {
            // The next generation's first record.
            let Some((key, _)) = reports
                .scan_range_limit(self.ctx, lower.as_deref(), None, 1)?
                .into_iter()
                .next()
            else {
                return Ok(false);
            };
            let (config, generation, _) = key_head(&key)?;
            let live = match states.get(self.ctx, config.as_bytes())? {
                Some(row) => decode_state(&row)?.generation == generation,
                None => false,
            };
            let next = generation_prefix(&config, generation + 1);
            let upper = match live {
                true => day_prefix(&config, generation, keep_from),
                false => next.clone(),
            };
            let rows = reports.scan_range_limit(
                self.ctx,
                Some(&generation_prefix(&config, generation)),
                Some(&upper),
                budget + 1,
            )?;
            let rows: Vec<_> = rows.into_iter().filter(|(key, _)| *key < upper).collect();
            let mut dropped = 0;
            for (key, _) in &rows {
                if dropped == budget || (dropped > 0 && key.len() > bytes) {
                    return Ok(true);
                }
                reports.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
                dropped += 1;
                bytes = bytes.saturating_sub(key.len());
            }
            budget -= dropped;
            if budget == 0 || bytes == 0 {
                return Ok(true);
            }
            lower = Some(next);
        }
    }
}

impl ObjFile<'_> {
    /// The request counts of `day` of bucket `bucket` (made at
    /// `created_ms`), if any were counted.
    pub fn lens_activity(
        &mut self,
        bucket: &str,
        created_ms: u64,
        day: u32,
    ) -> Result<Option<ActivityCounts>, StorageError> {
        let Some(tree) = self.existing_tree(LENS_ACTIVITY_TREE)? else {
            return Ok(None);
        };
        Ok(
            match tree.get(&mut self.core.page_ctx(0), &activity_key(bucket, day))? {
                Some(row) => match decode_counts(&row)? {
                    (created, counts) if created == created_ms => Some(counts),
                    _ => None,
                },
                None => None,
            },
        )
    }

    /// Configuration `config`'s state, if it has one.
    pub fn lens_state(&mut self, config: &str) -> Result<Option<LensState>, StorageError> {
        let Some(tree) = self.existing_tree(LENS_STATE_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), config.as_bytes())?
            .map(|row| decode_state(&row))
            .transpose()
    }

    /// Configuration `config`'s report of `day`, in key order (none until
    /// it is reported).
    pub fn lens_report(&mut self, config: &str, day: u32) -> Result<Vec<LensRecord>, StorageError> {
        let Some(state) = self.lens_state(config)? else {
            return Ok(Vec::new());
        };
        if !visible(Some(&state), state.generation, day) {
            return Ok(Vec::new());
        }
        let Some(tree) = self.existing_tree(LENS_REPORTS_TREE)? else {
            return Ok(Vec::new());
        };
        let lower = day_prefix(config, state.generation, day);
        let upper = day_prefix(config, state.generation, day + 1);
        tree.scan_range(&mut self.core.page_ctx(0), Some(&lower), Some(&upper))?
            .into_iter()
            .filter(|(key, _)| *key < upper)
            .map(|(key, value)| decode_row(&key, &value).map(|row| row.record))
            .collect()
    }

    /// Reports' records of every configuration (those read: see the
    /// module's doc), in key order, from up to `limit` records after
    /// `after` (a key this returned as where to resume; none: the end).
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
        let mut states: HashMap<String, Option<LensState>> = HashMap::new();
        let mut rows = Vec::new();
        for (key, value) in page.iter().take(limit) {
            let (config, generation, day) = key_head(key)?;
            if !states.contains_key(&config) {
                let state = self.lens_state(&config)?;
                states.insert(config.clone(), state);
            }
            if visible(states[&config].as_ref(), generation, day) {
                rows.push(decode_row(key, value)?);
            }
        }
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
        let key = record_key("111111111111/lens", 7, 20_000, &record);
        let row = decode_row(&key, &42u64.to_be_bytes()).unwrap();
        assert_eq!(row.config, "111111111111/lens");
        assert_eq!(row.day, 20_000);
        assert_eq!(row.record, record);
        assert_eq!(
            key_head(&key).unwrap(),
            ("111111111111/lens".to_string(), 7, 20_000)
        );
        // A configuration's keys sort by generation, then day, then record.
        assert!(day_prefix("a/b", 1, 9) < day_prefix("a/b", 2, 0));
        assert!(day_prefix("a/b", 1, 1) < day_prefix("a/b", 1, 2));
        let state = LensState {
            generation: 5,
            reported: None,
        };
        assert_eq!(decode_state(&encode_state(&state)).unwrap(), state);
    }

    #[test]
    fn batches_hold_a_bounded_count_of_bounded_bytes() {
        let record = |value_len: usize| LensRecord {
            record_type: LensRecordType::Prefix,
            bucket: "data".into(),
            record_value: "p".repeat(value_len),
            storage_class: "STANDARD".into(),
            metric: "StorageBytes".into(),
            value: 1,
        };
        let short = vec![record(1); 2 * LENS_BATCH + 1];
        assert_eq!(
            lens_batches("a/b", &short),
            [LENS_BATCH, 2 * LENS_BATCH, 2 * LENS_BATCH + 1]
        );
        let long = vec![record(1000); 200];
        let ends = lens_batches("a/b", &long);
        let mut start = 0;
        for end in &ends {
            let bytes: usize = long[start..*end]
                .iter()
                .map(|r| record_key("a/b", 0, 0, r).len())
                .sum();
            assert!(bytes <= LENS_BATCH_BYTES && *end > start, "{ends:?}");
            start = *end;
        }
        assert_eq!(start, long.len());
        assert!(lens_batches("a/b", &[]).is_empty());
        // Of the longest configuration key: its keys count too.
        let config = format!("111111111111/{}", "c".repeat(64));
        let ends = lens_batches(&config, &short);
        let mut start = 0;
        for end in &ends {
            let bytes: usize = short[start..*end]
                .iter()
                .map(|r| record_key(&config, 0, 0, r).len())
                .sum();
            assert!(bytes <= LENS_BATCH_BYTES, "{bytes} in {ends:?}");
            start = *end;
        }
    }
}
