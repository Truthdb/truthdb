//! The streams engine's rowsets, served by the engine's one provider (see
//! `s3_sql::S3Rowsets`), which hands these names here:
//!
//! - `sys.stream_topics`: one row per partition of every topic, with the
//!   offsets it holds.
//! - `sys.stream_read(@topic, @partition [, @from_offset [,
//!   @max_records]])`: a partition's records from `@from_offset` (default
//!   and NULL: its oldest), up to `@max_records` (default and NULL: all),
//!   oldest first. Records trimmed meanwhile are passed over.
//!
//! Records stream a slice at a time, each slice under its own storage-lock
//! hold. Topics are server-wide: only a server administrator may read them.
//! A standby refuses both: it applies the primary's log as it arrives,
//! transactions still open included, and the streams engine keeps no row
//! versions to hide those from a reader.

use std::sync::Arc;

use truthdb_core::storage::{Storage, StorageError};
use truthdb_relational::engine::{ExternalCaller, ExternalRowset, ExternalStream};
use truthdb_relational::relstore::types::{ColumnType, Datum};
use truthdb_sql::error::SqlError;
use truthdb_sql::value::SqlValue;
use truthdb_streams::{StreamError, StreamFile};

use super::s3_sql::{
    bigint, check_admin, check_arg_count, column, datetime, datetime2, nvarchar, string_arg, text,
};

/// Records one slice reads at most, and the key and value bytes it stops
/// after.
const SLICE_RECORDS: usize = 1024;
const SLICE_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn open(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    if name == "sys.stream_topics" {
        check_arg_count(name, args, 0, 0)?;
        return Ok(ExternalRowset {
            columns: vec![
                nvarchar("topic_name", 249),
                bigint("partition_id"),
                bigint("start_offset"),
                bigint("next_offset"),
                datetime2("last_record_time"),
                bigint("retention_ms"),
                datetime2("created"),
                nvarchar("account_id", 12),
            ],
            rows: Box::new(TopicStream {
                core: Arc::clone(core),
                done: false,
            }),
        });
    }
    check_arg_count(name, args, 2, 4)?;
    let topic = string_arg(name, &args[0])?;
    let number = |at: usize, what: &str, default: u64| match args.get(at) {
        None | Some(SqlValue::Null) => Ok(default),
        Some(SqlValue::Int(value)) if *value >= 0 => Ok(*value as u64),
        Some(_) => Err(SqlError::new(
            206,
            16,
            2,
            format!(
                "Operand type clash: the {what} argument of {name} must be a non-negative integer."
            ),
        )),
    };
    if matches!(args[1], SqlValue::Null) {
        return Err(stream_error(StreamError::InvalidPartition, &topic));
    }
    let partition = u32::try_from(number(1, "partition", 0)?)
        .map_err(|_| stream_error(StreamError::InvalidPartition, &topic))?;
    let from = number(2, "from_offset", 0)?;
    let left = number(3, "max_records", u64::MAX)?;
    // The topic and partition must exist now, even with no records to read.
    with_streams(core, |file| file.read(&topic, partition, from, 0, 0))?
        .map_err(|err| stream_error(err, &topic))?;
    Ok(ExternalRowset {
        columns: vec![
            bigint("offset"),
            datetime2("timestamp"),
            column("key", ColumnType::VarBinaryMax),
            column("value", ColumnType::VarBinaryMax),
        ],
        rows: Box::new(RecordStream {
            core: Arc::clone(core),
            topic,
            partition,
            at: from,
            left,
        }),
    })
}

fn with_streams<T>(
    core: &Arc<Storage>,
    op: impl FnOnce(&mut StreamFile<'_>) -> Result<T, StorageError>,
) -> Result<T, SqlError> {
    if core.is_standby() {
        return Err(SqlError::new(
            50000,
            16,
            1,
            "Streams are read on the primary: a standby holds its open transactions' changes.",
        ));
    }
    let mut guard = core.lock();
    let mut file = StreamFile::try_new(&mut guard).ok_or_else(|| {
        SqlError::new(
            50000,
            16,
            1,
            "this database does not run the streams engine",
        )
    })?;
    op(&mut file).map_err(|err| SqlError::new(50000, 16, 1, format!("streams: {err}")))
}

fn stream_error(err: StreamError, topic: &str) -> SqlError {
    match err {
        StreamError::NoSuchTopic => {
            SqlError::new(208, 16, 1, format!("The topic '{topic}' does not exist."))
        }
        StreamError::InvalidPartition => SqlError::new(
            50000,
            16,
            1,
            format!("The topic '{topic}' has no such partition."),
        ),
        other => SqlError::new(50000, 16, 1, format!("streams: {other:?}")),
    }
}

struct TopicStream {
    core: Arc<Storage>,
    done: bool,
}

impl ExternalStream for TopicStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let topics = with_streams(&self.core, |file| file.topics())?;
        let rows: Vec<Vec<Datum>> = topics
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions.iter().enumerate().map(|(id, state)| {
                    vec![
                        text(&topic.name),
                        Datum::BigInt(id as i64),
                        Datum::BigInt(state.start as i64),
                        Datum::BigInt(state.next as i64),
                        if state.last_ms == 0 {
                            Datum::Null
                        } else {
                            datetime(state.last_ms)
                        },
                        Datum::BigInt(topic.retention_ms as i64),
                        datetime(topic.created_ms),
                        topic.account.as_deref().map_or(Datum::Null, text),
                    ]
                })
            })
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

struct RecordStream {
    core: Arc<Storage>,
    topic: String,
    partition: u32,
    at: u64,
    left: u64,
}

impl ExternalStream for RecordStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.left == 0 {
            return Ok(None);
        }
        let max = SLICE_RECORDS.min(usize::try_from(self.left).unwrap_or(usize::MAX));
        let (records, _) = with_streams(&self.core, |file| {
            file.read(&self.topic, self.partition, self.at, max, SLICE_BYTES)
        })?
        .map_err(|err| stream_error(err, &self.topic))?;
        let Some(last) = records.last() else {
            return Ok(None);
        };
        self.at = last.offset + 1;
        self.left -= records.len() as u64;
        Ok(Some(
            records
                .into_iter()
                .map(|record| {
                    vec![
                        Datum::BigInt(record.offset as i64),
                        datetime(record.timestamp_ms),
                        record.key.map_or(Datum::Null, Datum::VarBinary),
                        Datum::VarBinary(record.value),
                    ]
                })
                .collect(),
        ))
    }
}
