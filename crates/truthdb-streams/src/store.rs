//! Topics, partitions and records. A partition's records sit at contiguous
//! offsets from where its log starts (records before have been trimmed) to
//! the next offset to append; their timestamps never go back, so the offset
//! of a time is found by searching. A record is stored in chunks small
//! enough for a tree cell. An append may name its source and that source's
//! sequence number: a source appends each sequence number at most once, so
//! a writer that cannot tell whether its last append landed (a crash) can
//! append it again.

use truthdb_core::relstore::btree::BTree;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{
    GROUP_OFFSETS_TREE, NEXT_TOPIC_KEY, PARTITIONS_TREE, PURGE_TREE, RECORDS_TREE, StreamFile,
    StreamTxn, TOPICS_TREE,
};

/// Partitions a topic may have.
pub const MAX_PARTITIONS: u32 = 64;
/// The largest record (its key and value).
pub const MAX_RECORD_BYTES: usize = 256 * 1024;
/// Bytes per stored chunk of a record.
const CHUNK: usize = 1500;
/// The chunks one append stores at most: two of the largest records, or as
/// many records as that of one chunk each. An append is one transaction,
/// and every chunk is a tree insert whose log is several times its size
/// (with the images of the pages it splits), so this keeps the largest
/// append within a transaction's log budget on the smallest log ring.
pub const MAX_APPEND_CHUNKS: usize = 2 * (MAX_RECORD_BYTES + RECORD_HEADER).div_ceil(CHUNK);
/// A stored record's bytes besides its key and value (see `encode_record`).
const RECORD_HEADER: usize = 17;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub id: u32,
    pub name: String,
    pub partitions: u32,
    /// How long records are kept (0: for ever).
    pub retention_ms: u64,
    pub created_ms: u64,
    /// The S3 account whose buckets may send it their event
    /// notifications (none: no bucket may).
    pub account: Option<String>,
}

/// Where a partition's log stands: records at offsets `[start, next)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Partition {
    pub start: u64,
    pub next: u64,
    /// The newest record's timestamp (0: none yet).
    pub last_ms: u64,
}

/// A record to append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRecord {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

impl NewRecord {
    /// The chunks it is stored in (what [`MAX_APPEND_CHUNKS`] counts).
    pub fn chunks(&self) -> usize {
        (self.key.as_ref().map_or(0, Vec::len) + self.value.len() + RECORD_HEADER).div_ceil(CHUNK)
    }
}

/// A record as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

/// What an append did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// The first record's offset: for a repeat of the source's latest
    /// append, that append's; `None` for a repeat of an older one (its
    /// offset is not kept).
    pub offset: Option<u64>,
    /// False when the source had appended this sequence number already
    /// (nothing was written).
    pub written: bool,
}

/// Why a streams operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    NoSuchTopic,
    TopicExists,
    InvalidName(&'static str),
    /// No such partition in the topic.
    InvalidPartition,
    InvalidPartitions,
    /// A record or an append larger than allowed.
    TooLarge,
}

/// Kafka's topic name rule: 1 to 249 of `[A-Za-z0-9._-]`, not `.` or `..`.
pub fn valid_topic_name(name: &str) -> Result<(), StreamError> {
    if name.is_empty() || name.len() > 249 {
        return Err(StreamError::InvalidName(
            "a topic name is 1 to 249 characters",
        ));
    }
    if name == "." || name == ".." {
        return Err(StreamError::InvalidName("a topic is not named . or .."));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(StreamError::InvalidName(
            "a topic name is letters, digits, '.', '_' and '-'",
        ));
    }
    Ok(())
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt streams {what}"))
}

fn u64_at(bytes: &[u8], at: usize) -> Result<u64, StorageError> {
    bytes
        .get(at..at + 8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| corrupt("row"))
}

fn u32_at(bytes: &[u8], at: usize) -> Result<u32, StorageError> {
    bytes
        .get(at..at + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| corrupt("row"))
}

/// `[id u32][partitions u32][retention ms u64][created ms u64][account]`.
fn encode_topic(topic: &Topic) -> Vec<u8> {
    [
        &topic.id.to_le_bytes()[..],
        &topic.partitions.to_le_bytes(),
        &topic.retention_ms.to_le_bytes(),
        &topic.created_ms.to_le_bytes(),
        topic.account.as_deref().unwrap_or_default().as_bytes(),
    ]
    .concat()
}

fn decode_topic(name: &[u8], row: &[u8]) -> Result<Topic, StorageError> {
    let account = row.get(24..).ok_or_else(|| corrupt("topic"))?;
    Ok(Topic {
        id: u32_at(row, 0)?,
        name: String::from_utf8(name.to_vec()).map_err(|_| corrupt("topic name"))?,
        partitions: u32_at(row, 4)?,
        retention_ms: u64_at(row, 8)?,
        created_ms: u64_at(row, 16)?,
        account: (!account.is_empty())
            .then(|| String::from_utf8(account.to_vec()).map_err(|_| corrupt("topic")))
            .transpose()?,
    })
}

/// `[start u64][next u64][last ms u64]`.
fn encode_partition(partition: &Partition) -> Vec<u8> {
    [
        &partition.start.to_le_bytes()[..],
        &partition.next.to_le_bytes(),
        &partition.last_ms.to_le_bytes(),
    ]
    .concat()
}

fn decode_partition(row: &[u8]) -> Result<Partition, StorageError> {
    if row.len() != 24 {
        return Err(corrupt("partition"));
    }
    Ok(Partition {
        start: u64_at(row, 0)?,
        next: u64_at(row, 8)?,
        last_ms: u64_at(row, 16)?,
    })
}

fn partition_key(topic: u32, partition: u32) -> [u8; 8] {
    let mut key = [0u8; 8];
    key[..4].copy_from_slice(&topic.to_be_bytes());
    key[4..].copy_from_slice(&partition.to_be_bytes());
    key
}

fn record_key(topic: u32, partition: u32, offset: u64, chunk: u16) -> [u8; 18] {
    let mut key = [0u8; 18];
    key[..8].copy_from_slice(&partition_key(topic, partition));
    key[8..16].copy_from_slice(&offset.to_be_bytes());
    key[16..].copy_from_slice(&chunk.to_be_bytes());
    key
}

/// `[timestamp ms u64][key: 0, or 1 + len u32 + bytes][value len u32][value]`.
fn encode_record(timestamp_ms: u64, record: &NewRecord) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + record.value.len());
    out.extend_from_slice(&timestamp_ms.to_le_bytes());
    match &record.key {
        None => out.push(0),
        Some(key) => {
            out.push(1);
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
        }
    }
    out.extend_from_slice(&(record.value.len() as u32).to_le_bytes());
    out.extend_from_slice(&record.value);
    out
}

fn decode_record(offset: u64, bytes: &[u8]) -> Result<Record, StorageError> {
    let timestamp_ms = u64_at(bytes, 0)?;
    let (key, at) = match bytes.get(8) {
        Some(0) => (None, 9),
        Some(1) => {
            let len = u32_at(bytes, 9)? as usize;
            let key = bytes.get(13..13 + len).ok_or_else(|| corrupt("record"))?;
            (Some(key.to_vec()), 13 + len)
        }
        _ => return Err(corrupt("record")),
    };
    let len = u32_at(bytes, at)? as usize;
    let value = bytes
        .get(at + 4..at + 4 + len)
        .ok_or_else(|| corrupt("record"))?;
    if at + 4 + len != bytes.len() {
        return Err(corrupt("record"));
    }
    Ok(Record {
        offset,
        timestamp_ms,
        key,
        value: value.to_vec(),
    })
}

/// `(topic, partition, group)`.
fn group_key(group: &str, topic: u32, partition: u32) -> Vec<u8> {
    [&partition_key(topic, partition)[..], group.as_bytes()].concat()
}

/// `(topic, partition, source)`.
fn source_key(topic: u32, partition: u32, source: &str) -> Vec<u8> {
    [&partition_key(topic, partition)[..], source.as_bytes()].concat()
}

/// Reads one record's chunks from `records`.
fn read_record(
    ctx: &mut truthdb_core::relstore::ctx::RelCtx<'_>,
    records: &BTree,
    topic: u32,
    partition: u32,
    offset: u64,
) -> Result<Option<Record>, StorageError> {
    let chunks = records.scan_range(
        ctx,
        Some(&record_key(topic, partition, offset, 0)),
        Some(&record_key(topic, partition, offset, u16::MAX)),
    )?;
    if chunks.is_empty() {
        return Ok(None);
    }
    let bytes: Vec<u8> = chunks.into_iter().flat_map(|(_, chunk)| chunk).collect();
    decode_record(offset, &bytes).map(Some)
}

impl StreamTxn<'_, '_> {
    fn topic_row(&mut self, name: &str) -> Result<Option<Topic>, StorageError> {
        self.trees
            .topics()
            .get(self.ctx, name.as_bytes())?
            .map(|row| decode_topic(name.as_bytes(), &row))
            .transpose()
    }

    fn partition_row(&mut self, topic: u32, partition: u32) -> Result<Partition, StorageError> {
        self.trees
            .partitions()
            .get(self.ctx, &partition_key(topic, partition))?
            .map(|row| decode_partition(&row))
            .transpose()?
            .ok_or_else(|| corrupt("topic without its partition"))
    }

    /// Creates a topic of `partitions` partitions, keeping records for
    /// `retention_ms` (0: for ever), whose events `account`'s buckets may
    /// send it.
    pub fn create_topic(
        &mut self,
        name: &str,
        partitions: u32,
        retention_ms: u64,
        account: Option<&str>,
        now_ms: u64,
    ) -> Result<Result<Topic, StreamError>, StorageError> {
        if let Err(err) = valid_topic_name(name) {
            return Ok(Err(err));
        }
        if !(1..=MAX_PARTITIONS).contains(&partitions) {
            return Ok(Err(StreamError::InvalidPartitions));
        }
        if self.topic_row(name)?.is_some() {
            return Ok(Err(StreamError::TopicExists));
        }
        let id = u32::try_from(self.next_counter(NEXT_TOPIC_KEY)?)
            .map_err(|_| corrupt("topic counter"))?;
        let topic = Topic {
            id,
            name: name.to_string(),
            partitions,
            retention_ms,
            created_ms: now_ms,
            account: account.map(str::to_string),
        };
        let mode = &mut OpMode::Txn(self.txn);
        self.trees.topics().insert_unique(
            self.ctx,
            mode,
            name.as_bytes(),
            &encode_topic(&topic),
        )?;
        for partition in 0..partitions {
            self.trees.partitions().insert_unique(
                self.ctx,
                mode,
                &partition_key(id, partition),
                &encode_partition(&Partition::default()),
            )?;
        }
        Ok(Ok(topic))
    }

    /// Drops a topic: gone at once, its records removed in the background
    /// (see [`Self::purge_step`]).
    pub fn drop_topic(&mut self, name: &str) -> Result<Result<(), StreamError>, StorageError> {
        let Some(topic) = self.topic_row(name)? else {
            return Ok(Err(StreamError::NoSuchTopic));
        };
        let mode = &mut OpMode::Txn(self.txn);
        self.trees
            .topics()
            .delete(self.ctx, mode, name.as_bytes())?;
        for partition in 0..topic.partitions {
            self.trees
                .partitions()
                .delete(self.ctx, mode, &partition_key(topic.id, partition))?;
        }
        let sources = self.trees.sources();
        let lower = partition_key(topic.id, 0);
        let upper = partition_key(topic.id, u32::MAX);
        for (key, _) in sources.scan_range(
            self.ctx,
            Some(&lower),
            Some(&[&upper[..], &[0xFF]].concat()),
        )? {
            sources.delete(self.ctx, mode, &key)?;
        }
        self.trees.purge().insert_unique(
            self.ctx,
            mode,
            &topic.id.to_be_bytes(),
            &topic.partitions.to_le_bytes(),
        )?;
        Ok(Ok(()))
    }

    /// Appends `records` to a partition at `now_ms` (or, for a source,
    /// unless it appended `sequence` already). Their timestamps never go
    /// back: a clock that steps back stamps them with the newest one.
    pub fn append(
        &mut self,
        topic: &str,
        partition: u32,
        records: &[NewRecord],
        source: Option<(&str, u64)>,
        now_ms: u64,
    ) -> Result<Result<Appended, StreamError>, StorageError> {
        let Some(topic) = self.topic_row(topic)? else {
            return Ok(Err(StreamError::NoSuchTopic));
        };
        if partition >= topic.partitions {
            return Ok(Err(StreamError::InvalidPartition));
        }
        if records.iter().any(|record| {
            record.key.as_ref().map_or(0, Vec::len) + record.value.len() > MAX_RECORD_BYTES
        }) || records.iter().map(NewRecord::chunks).sum::<usize>() > MAX_APPEND_CHUNKS
        {
            return Ok(Err(StreamError::TooLarge));
        }
        let mut state = self.partition_row(topic.id, partition)?;
        if let Some((name, sequence)) = source {
            let key = source_key(topic.id, partition, name);
            let sources = self.trees.sources();
            // `[sequence u64][first offset u64]` of the source's latest
            // append.
            let last = sources
                .get(self.ctx, &key)?
                .map(|row| {
                    if row.len() != 16 {
                        return Err(corrupt("source"));
                    }
                    Ok((u64_at(&row, 0)?, u64_at(&row, 8)?))
                })
                .transpose()?;
            if let Some((last, offset)) = last.filter(|(last, _)| sequence <= *last) {
                return Ok(Ok(Appended {
                    offset: (sequence == last).then_some(offset),
                    written: false,
                }));
            }
            let row = [sequence.to_le_bytes(), state.next.to_le_bytes()].concat();
            let mode = &mut OpMode::Txn(self.txn);
            sources.delete(self.ctx, mode, &key)?;
            sources.insert_unique(self.ctx, mode, &key, &row)?;
        }
        let first = state.next;
        let timestamp_ms = now_ms.max(state.last_ms);
        let records_tree = self.trees.records();
        for record in records {
            let bytes = encode_record(timestamp_ms, record);
            for (index, chunk) in bytes.chunks(CHUNK).enumerate() {
                records_tree.insert_unique(
                    self.ctx,
                    &mut OpMode::Txn(self.txn),
                    &record_key(topic.id, partition, state.next, index as u16),
                    chunk,
                )?;
            }
            state.next += 1;
        }
        if !records.is_empty() {
            state.last_ms = timestamp_ms;
            self.trees.partitions().update(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &partition_key(topic.id, partition),
                &encode_partition(&state),
            )?;
        }
        Ok(Ok(Appended {
            offset: Some(first),
            written: true,
        }))
    }

    /// Records the offset `group` has read up to in a partition.
    pub fn commit_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<Result<(), StreamError>, StorageError> {
        let Some(topic) = self.topic_row(topic)? else {
            return Ok(Err(StreamError::NoSuchTopic));
        };
        if partition >= topic.partitions {
            return Ok(Err(StreamError::InvalidPartition));
        }
        let key = group_key(group, topic.id, partition);
        let tree = self.trees.group_offsets();
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &key)?;
        tree.insert_unique(self.ctx, mode, &key, &offset.to_le_bytes())?;
        Ok(Ok(()))
    }

    /// Removes up to `limit` records past their topic's retention at
    /// `now_ms`, oldest first, and stops at the log budget. Returns how
    /// many it removed, and whether it stopped at the limit or the budget
    /// (so more may be due).
    pub fn trim_step(&mut self, now_ms: u64, limit: usize) -> Result<(usize, bool), StorageError> {
        let start_tail = self.log_tail();
        let budget = self.log_budget();
        let topics: Vec<Topic> = self
            .trees
            .topics()
            .scan(self.ctx)?
            .into_iter()
            .map(|(name, row)| decode_topic(&name, &row))
            .collect::<Result<_, _>>()?;
        let mut removed = 0;
        for topic in topics.into_iter().filter(|topic| topic.retention_ms > 0) {
            let cutoff = now_ms.saturating_sub(topic.retention_ms);
            for partition in 0..topic.partitions {
                let mut state = self.partition_row(topic.id, partition)?;
                let before = state.start;
                while state.start < state.next
                    && removed < limit
                    && self.log_since(start_tail) < budget
                {
                    let records = self.trees.records();
                    let Some(record) =
                        read_record(self.ctx, &records, topic.id, partition, state.start)?
                    else {
                        return Err(corrupt("partition missing a record"));
                    };
                    if record.timestamp_ms >= cutoff {
                        break;
                    }
                    for (key, _) in records.scan_range(
                        self.ctx,
                        Some(&record_key(topic.id, partition, state.start, 0)),
                        Some(&record_key(topic.id, partition, state.start, u16::MAX)),
                    )? {
                        records.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
                    }
                    state.start += 1;
                    removed += 1;
                }
                if state.start != before {
                    self.trees.partitions().update(
                        self.ctx,
                        &mut OpMode::Txn(self.txn),
                        &partition_key(topic.id, partition),
                        &encode_partition(&state),
                    )?;
                }
            }
        }
        let more = removed >= limit || self.log_since(start_tail) >= budget;
        Ok((removed, more))
    }

    /// Removes up to `limit` rows of the first dropped topic (stopping at
    /// the log budget); returns whether no dropped topic is left.
    pub fn purge_step(&mut self, limit: usize) -> Result<bool, StorageError> {
        let purge = self.trees.purge();
        let Some((id_key, _)) = purge.scan_range_limit(self.ctx, None, None, 1)?.pop() else {
            return Ok(true);
        };
        let id: [u8; 4] = id_key
            .as_slice()
            .try_into()
            .map_err(|_| corrupt("purge row"))?;
        let topic = u32::from_be_bytes(id);
        let start_tail = self.log_tail();
        let budget = self.log_budget();
        let records = self.trees.records();
        let rows = records.scan_range_limit(
            self.ctx,
            Some(&record_key(topic, 0, 0, 0)),
            Some(&record_key(topic, u32::MAX, u64::MAX, u16::MAX)),
            limit,
        )?;
        let mut finished = rows.len() < limit;
        for (key, _) in rows {
            if self.log_since(start_tail) >= budget {
                return Ok(false);
            }
            records.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        // Then the offsets consumer groups committed in it.
        if finished {
            let offsets = self.trees.group_offsets();
            let rows = offsets.scan_range_limit(
                self.ctx,
                Some(&partition_key(topic, 0)),
                Some(&[&partition_key(topic, u32::MAX)[..], &[0xFF]].concat()),
                limit,
            )?;
            finished = rows.len() < limit;
            for (key, _) in rows {
                if self.log_since(start_tail) >= budget {
                    return Ok(false);
                }
                offsets.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
            }
        }
        if finished {
            purge.delete(self.ctx, &mut OpMode::Txn(self.txn), &id_key)?;
            return Ok(purge.scan_range_limit(self.ctx, None, None, 1)?.is_empty());
        }
        Ok(false)
    }
}

impl StreamFile<'_> {
    /// Every topic and its partitions, by name.
    pub fn topics(&mut self) -> Result<Vec<(Topic, Vec<Partition>)>, StorageError> {
        let (Some(topics), Some(partitions)) = (
            self.existing_tree(TOPICS_TREE)?,
            self.existing_tree(PARTITIONS_TREE)?,
        ) else {
            return Ok(Vec::new());
        };
        let mut ctx = self.page_ctx();
        let mut out = Vec::new();
        for (name, row) in topics.scan(&mut ctx)? {
            let topic = decode_topic(&name, &row)?;
            let mut states = Vec::new();
            for partition in 0..topic.partitions {
                let row = partitions
                    .get(&mut ctx, &partition_key(topic.id, partition))?
                    .ok_or_else(|| corrupt("topic without its partition"))?;
                states.push(decode_partition(&row)?);
            }
            out.push((topic, states));
        }
        Ok(out)
    }

    /// A topic and its partitions.
    pub fn topic(&mut self, name: &str) -> Result<Option<(Topic, Vec<Partition>)>, StorageError> {
        Ok(self
            .topics()?
            .into_iter()
            .find(|(topic, _)| topic.name == name))
    }

    /// Up to `max_records` records of a partition from `offset` (records
    /// trimmed meanwhile are passed over), stopping before `max_bytes` of
    /// keys and values once at least one is read; with the partition as it
    /// stands.
    pub fn read(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Result<(Vec<Record>, Partition), StreamError>, StorageError> {
        let Some((topic, partitions)) = self.topic(topic)? else {
            return Ok(Err(StreamError::NoSuchTopic));
        };
        let Some(&state) = partitions.get(partition as usize) else {
            return Ok(Err(StreamError::InvalidPartition));
        };
        let Some(records) = self.existing_tree(RECORDS_TREE)? else {
            return Ok(Ok((Vec::new(), state)));
        };
        let mut ctx = self.page_ctx();
        let mut out = Vec::new();
        let mut bytes = 0;
        let mut at = offset.max(state.start);
        while at < state.next && out.len() < max_records {
            let Some(record) = read_record(&mut ctx, &records, topic.id, partition, at)? else {
                return Err(corrupt("partition missing a record"));
            };
            let size = record.key.as_ref().map_or(0, Vec::len) + record.value.len();
            if !out.is_empty() && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            out.push(record);
            at += 1;
        }
        Ok(Ok((out, state)))
    }

    /// The first offset of a partition whose record is at or after
    /// `timestamp_ms` (the next offset when none is).
    pub fn offset_for_time(
        &mut self,
        topic: &str,
        partition: u32,
        timestamp_ms: u64,
    ) -> Result<Result<u64, StreamError>, StorageError> {
        let Some((topic, partitions)) = self.topic(topic)? else {
            return Ok(Err(StreamError::NoSuchTopic));
        };
        let Some(&state) = partitions.get(partition as usize) else {
            return Ok(Err(StreamError::InvalidPartition));
        };
        let Some(records) = self.existing_tree(RECORDS_TREE)? else {
            return Ok(Ok(state.next));
        };
        let mut ctx = self.page_ctx();
        // Timestamps never go back: the first one at or after is found by
        // halving.
        let (mut low, mut high) = (state.start, state.next);
        while low < high {
            let middle = low + (high - low) / 2;
            let record = read_record(&mut ctx, &records, topic.id, partition, middle)?
                .ok_or_else(|| corrupt("partition missing a record"))?;
            if record.timestamp_ms < timestamp_ms {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(Ok(low))
    }

    /// The offset `group` committed in a partition, if any.
    pub fn committed_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>, StorageError> {
        let (Some((topic, _)), Some(tree)) =
            (self.topic(topic)?, self.existing_tree(GROUP_OFFSETS_TREE)?)
        else {
            return Ok(None);
        };
        let mut ctx = self.page_ctx();
        tree.get(&mut ctx, &group_key(group, topic.id, partition))?
            .map(|row| u64_at(&row, 0))
            .transpose()
    }

    /// Whether a dropped topic still has records to remove.
    pub fn purge_pending(&mut self) -> Result<bool, StorageError> {
        let Some(purge) = self.existing_tree(PURGE_TREE)? else {
            return Ok(false);
        };
        let mut ctx = self.page_ctx();
        Ok(!purge.scan_range_limit(&mut ctx, None, None, 1)?.is_empty())
    }

    /// Whether a partition's oldest record is past its topic's retention at
    /// `now_ms` (so [`StreamTxn::trim_step`] has work).
    pub fn trim_due(&mut self, now_ms: u64) -> Result<bool, StorageError> {
        let topics = self.topics()?;
        let Some(records) = self.existing_tree(RECORDS_TREE)? else {
            return Ok(false);
        };
        let mut ctx = self.page_ctx();
        for (topic, partitions) in topics.iter().filter(|(topic, _)| topic.retention_ms > 0) {
            let cutoff = now_ms.saturating_sub(topic.retention_ms);
            for (partition, state) in (0..).zip(partitions) {
                if state.start == state.next {
                    continue;
                }
                let oldest = read_record(&mut ctx, &records, topic.id, partition, state.start)?
                    .ok_or_else(|| corrupt("partition missing a record"))?;
                if oldest.timestamp_ms < cutoff {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_corruption_is_refused() {
        for record in [
            NewRecord {
                key: None,
                value: b"value".to_vec(),
            },
            NewRecord {
                key: Some(Vec::new()),
                value: Vec::new(),
            },
            NewRecord {
                key: Some(b"k".to_vec()),
                value: vec![7; 5000],
            },
        ] {
            let bytes = encode_record(42, &record);
            let read = decode_record(9, &bytes).unwrap();
            assert_eq!(
                (read.offset, read.timestamp_ms, read.key, read.value),
                (9, 42, record.key, record.value)
            );
            for cut in 0..bytes.len() {
                assert!(decode_record(9, &bytes[..cut]).is_err(), "cut at {cut}");
            }
            assert!(decode_record(9, &[bytes.clone(), vec![0]].concat()).is_err());
        }
    }

    #[test]
    fn topic_names_follow_kafkas_rule() {
        for good in ["a", "s3-events.v1", "A_b-9", &"x".repeat(249)] {
            assert!(valid_topic_name(good).is_ok(), "{good}");
        }
        for bad in ["", ".", "..", "a b", "a/b", "ä", &"x".repeat(250)] {
            assert!(valid_topic_name(bad).is_err(), "{bad}");
        }
    }
}
