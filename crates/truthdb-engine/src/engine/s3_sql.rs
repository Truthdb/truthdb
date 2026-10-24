//! The object store's inventory as SQL (S3 plan items 13 and 14): server-wide
//! rowsets over the object engine's own trees, so a query sees every write
//! as soon as it commits.
//!
//! - `sys.s3_buckets`: every bucket.
//! - `sys.s3_objects(@bucket [, @prefix])`: each key's newest version, keys
//!   whose newest version is a delete marker left out.
//! - `sys.s3_object_versions(@bucket [, @prefix])`: every version and delete
//!   marker, newest first within a key.
//! - `sys.s3_object_tags(@bucket [, @prefix])` and
//!   `sys.s3_object_metadata(@bucket [, @prefix])`: the tags and the
//!   `x-amz-meta-*` metadata of each key's newest version, one row per pair.
//! - `sys.s3_journal(@bucket [, @after_sequence])`: the change journal's rows
//!   for `bucket` numbered after `@after_sequence`, oldest first (S3
//!   Metadata's journal table): only the current bucket's, not those of a
//!   deleted bucket of the same name. The numbers are not consecutive: the
//!   journal also keeps notices of events that are not S3 Metadata records
//!   (restores, retention changes, for event notifications), which are
//!   left out. A restore to an earlier point rewinds
//!   the numbering with everything else, so a reader resuming from a number
//!   seen before the restore reads the numbers handed out again after it.
//! - `sys.s3_search(@bucket, @condition [, @top])`: full-text search over
//!   the contents of a bucket with full-text search enabled (S3 plan item
//!   15; `s3 enable fulltext`), the shape of `CONTAINSTABLE`: up to `@top`
//!   (default 100, also when NULL; at most 10,000) keys whose latest
//!   version matches the `CONTAINS`-style condition, best first by BM25
//!   score. The index follows
//!   the change journal a moment behind the writes: a key is returned only
//!   while the version that matched is still its latest.
//!
//! Rows stream a slice at a time, each slice under its own storage-lock hold,
//! so a scan reads what is committed when each slice is read (read
//! committed), not one snapshot. Buckets are server-wide: only a server
//! administrator may read them.

use std::sync::Arc;

use std::collections::VecDeque;
use truthdb_core::storage::{Storage, StorageError};

use truthdb_fulltext::{Hit, Search, SearchError};
use truthdb_object::{
    BodyUse, FtState, JournalRecord, ListAfter, Listing, ObjFile, ObjectAttrs, ObjectError,
    ObjectInfo, VersionSel, Versioning, version_id,
};
use truthdb_relational::engine::{
    ExternalCaller, ExternalRowset, ExternalRowsets, ExternalStream, ResultColumn, check_cancelled,
};
use truthdb_relational::relstore::types::{ColumnType, Datum};
use truthdb_sql::error::SqlError;
use truthdb_sql::value::SqlValue;

/// Keys one slice lists (one storage-lock hold), as a table scan's slice.
const SLICE_KEYS: usize = 1024;

/// Postings a search reads per storage-lock hold.
const SEARCH_STEP_POSTINGS: usize = 16 * 1024;
/// The keys `sys.s3_search` returns unless told otherwise, and at most.
const DEFAULT_SEARCH_TOP: usize = 100;
const MAX_SEARCH_TOP: usize = 10_000;

/// Days from 0001-01-01 to 1970-01-01.
const DAYS_0001_TO_1970: u64 = 719_162;
const MS_PER_DAY: u64 = 86_400_000;

/// The object store's rowsets, over the storage the engine runs on.
pub(crate) struct S3Rowsets {
    pub(crate) core: Arc<Storage>,
    pub(crate) request_metrics: Arc<crate::metrics::RequestMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Objects,
    Versions,
    Tags,
    Metadata,
}

impl ExternalRowsets for S3Rowsets {
    fn open(
        &self,
        name: &str,
        args: &[SqlValue],
        caller: ExternalCaller,
    ) -> Option<Result<ExternalRowset, SqlError>> {
        let kind = match name {
            "sys.s3_buckets" => None,
            "sys.s3_objects" => Some(Kind::Objects),
            "sys.s3_object_versions" => Some(Kind::Versions),
            "sys.s3_object_tags" => Some(Kind::Tags),
            "sys.s3_object_metadata" => Some(Kind::Metadata),
            "sys.s3_journal" => return Some(self.open_journal(name, args, caller)),
            "sys.dm_s3_journal_consumers" => {
                return Some(open_consumers(&self.core, name, args, caller));
            }
            "sys.dm_s3_imports" | "sys.dm_s3_import_failures" => {
                return Some(open_imports(&self.core, name, args, caller));
            }
            "sys.dm_s3_batch_jobs" => return Some(open_batch_jobs(&self.core, name, args, caller)),
            "sys.dm_s3_bucket_usage" => {
                return Some(open_bucket_usage(&self.core, name, args, caller));
            }
            "sys.dm_s3_storage" => return Some(open_storage(&self.core, name, args, caller)),
            "sys.dm_s3_api_activity" => {
                return Some(open_api_activity(&self.core, name, args, caller));
            }
            "sys.dm_s3_requests" | "sys.dm_s3_request_latency" => {
                return Some(open_requests(&self.request_metrics, name, args, caller));
            }
            "sys.s3_search" => return Some(self.open_search(name, args, caller)),
            "sys.stream_topics" | "sys.stream_read" | "sys.dm_s3_webhooks" => {
                return Some(super::stream_sql::open(&self.core, name, args, caller));
            }
            "sys.s3_read" => {
                return Some(
                    check_admin(name, caller)
                        .and_then(|()| super::s3_read::open(&self.core, name, args)),
                );
            }
            _ => return None,
        };
        Some(self.open_kind(name, kind, args, caller))
    }
}

impl S3Rowsets {
    fn open_search(
        &self,
        name: &str,
        args: &[SqlValue],
        caller: ExternalCaller,
    ) -> Result<ExternalRowset, SqlError> {
        check_admin(name, caller)?;
        check_arg_count(name, args, 2, 3)?;
        let bucket = string_arg(name, &args[0])?;
        let condition = string_arg(name, &args[1])?;
        let top = match args.get(2) {
            None | Some(SqlValue::Null) => DEFAULT_SEARCH_TOP,
            Some(SqlValue::Int(top)) if (1..=MAX_SEARCH_TOP as i64).contains(top) => *top as usize,
            Some(_) => {
                return Err(SqlError::new(
                    206,
                    16,
                    2,
                    format!(
                        "Operand type clash: the top argument of {name} must be an integer from 1 to {MAX_SEARCH_TOP}."
                    ),
                ));
            }
        };
        {
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            file.bucket(&bucket)
                .map_err(storage_error)?
                .ok_or_else(|| object_error(ObjectError::NoSuchBucket, &bucket))?;
            let indexed = file
                .fulltext_buckets()
                .map_err(storage_error)?
                .iter()
                .any(|ft| ft.name == bucket && ft.state != FtState::Purging);
            if !indexed {
                return Err(SqlError::new(
                    7601,
                    16,
                    2,
                    format!(
                        "Cannot use a CONTAINS predicate on bucket '{bucket}' because it does not have full-text search enabled."
                    ),
                ));
            }
        }
        if condition.trim().is_empty() {
            return Err(SqlError::new(
                7645,
                15,
                1,
                "Null or empty full-text predicate.",
            ));
        }
        let search = Search::new(&condition).map_err(search_error)?;
        Ok(ExternalRowset {
            columns: vec![
                nvarchar("bucket", 63),
                nvarchar("key", 1024),
                nvarchar("version_id", 20),
                column("score", ColumnType::Float),
                bigint("size"),
                datetime2("last_modified_date"),
            ],
            rows: Box::new(SearchStream {
                core: Arc::clone(&self.core),
                bucket,
                search,
                hits: None,
                left: top,
            }),
        })
    }

    fn open_journal(
        &self,
        name: &str,
        args: &[SqlValue],
        caller: ExternalCaller,
    ) -> Result<ExternalRowset, SqlError> {
        check_admin(name, caller)?;
        check_arg_count(name, args, 1, 2)?;
        let bucket = string_arg(name, &args[0])?;
        let after = match args.get(1) {
            None => 0,
            Some(SqlValue::Int(after)) if *after >= 0 => *after as u64,
            Some(_) => {
                return Err(SqlError::new(
                    206,
                    16,
                    2,
                    format!(
                        "Operand type clash: the sequence argument of {name} must be a non-negative integer."
                    ),
                ));
            }
        };
        // The bucket's own rows start after the journal's position at its
        // creation.
        let from = {
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            file.bucket(&bucket)
                .map_err(storage_error)?
                .ok_or_else(|| object_error(ObjectError::NoSuchBucket, &bucket))?
                .journal_from
        };
        Ok(ExternalRowset {
            columns: vec![
                nvarchar("bucket", 63),
                nvarchar("key", 1024),
                bigint("sequence_number"),
                nvarchar("record_type", 16),
                datetime2("record_timestamp"),
                nvarchar("version_id", 20),
                bit("is_delete_marker"),
                bigint("size"),
                datetime2("last_modified_date"),
                nvarchar("e_tag", 64),
                nvarchar("storage_class", 32),
                bit("is_multipart"),
                nvarchar("requester", 12),
                nvarchar("source_ip_address", 45),
                nvarchar("request_id", 16),
            ],
            rows: Box::new(JournalStream {
                core: Arc::clone(&self.core),
                bucket,
                after: after.max(from),
                done: false,
            }),
        })
    }

    fn open_kind(
        &self,
        name: &str,
        kind: Option<Kind>,
        args: &[SqlValue],
        caller: ExternalCaller,
    ) -> Result<ExternalRowset, SqlError> {
        check_admin(name, caller)?;
        let Some(kind) = kind else {
            check_arg_count(name, args, 0, 0)?;
            return Ok(ExternalRowset {
                columns: vec![
                    nvarchar("name", 63),
                    datetime2("creation_date"),
                    nvarchar("versioning", 16),
                ],
                rows: Box::new(BucketStream {
                    core: Arc::clone(&self.core),
                    done: false,
                }),
            });
        };
        check_arg_count(name, args, 1, 2)?;
        let bucket = string_arg(name, &args[0])?;
        let prefix = match args.get(1) {
            Some(value) => string_arg(name, value)?,
            None => String::new(),
        };
        let identity = [
            nvarchar("bucket", 63),
            nvarchar("key", 1024),
            nvarchar("version_id", 20),
        ];
        let mut columns = identity.to_vec();
        match kind {
            Kind::Objects | Kind::Versions => {
                columns.extend([
                    bigint("size"),
                    datetime2("last_modified"),
                    nvarchar("etag", 64),
                    nvarchar_max("content_type"),
                    nvarchar_max("cache_control"),
                    nvarchar_max("content_disposition"),
                    nvarchar_max("content_encoding"),
                    nvarchar_max("content_language"),
                    nvarchar_max("expires"),
                    nvarchar("storage_class", 32),
                ]);
                if kind == Kind::Versions {
                    columns.extend([bit("is_latest"), bit("is_delete_marker")]);
                }
            }
            Kind::Tags => columns.extend([nvarchar("tag_key", 128), nvarchar("tag_value", 256)]),
            Kind::Metadata => columns.extend([nvarchar_max("name"), nvarchar_max("value")]),
        }
        Ok(ExternalRowset {
            columns,
            rows: Box::new(ObjectStream {
                core: Arc::clone(&self.core),
                kind,
                bucket,
                prefix,
                after: None,
                done: false,
            }),
        })
    }
}

/// Buckets are server-wide: only a server administrator reads them.
pub(super) fn check_admin(name: &str, caller: ExternalCaller) -> Result<(), SqlError> {
    if caller.server_admin {
        return Ok(());
    }
    Err(SqlError::new(
        229,
        14,
        5,
        format!(
            "The SELECT permission was denied on the object '{}', database 'mssqlsystemresource', schema 'sys'.",
            name.trim_start_matches("sys.")
        ),
    ))
}

pub(super) fn check_arg_count(
    name: &str,
    args: &[SqlValue],
    min: usize,
    max: usize,
) -> Result<(), SqlError> {
    if args.len() < min {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {name}."
            ),
        ));
    }
    if args.len() > max {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!("Procedure or function {name} has too many arguments specified."),
        ));
    }
    Ok(())
}

pub(super) fn string_arg(name: &str, value: &SqlValue) -> Result<String, SqlError> {
    match value {
        SqlValue::Str(text) => Ok(text.clone()),
        _ => Err(SqlError::new(
            206,
            16,
            2,
            format!("Operand type clash: the arguments of {name} must be character strings."),
        )),
    }
}

pub(super) fn column(name: &str, column_type: ColumnType) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type,
    }
}

pub(super) fn nvarchar(name: &str, max_len: u16) -> ResultColumn {
    column(name, ColumnType::NVarChar { max_len })
}

pub(super) fn nvarchar_max(name: &str) -> ResultColumn {
    column(name, ColumnType::NVarCharMax)
}

pub(super) fn bigint(name: &str) -> ResultColumn {
    column(name, ColumnType::BigInt)
}

fn bit(name: &str) -> ResultColumn {
    column(name, ColumnType::Bit)
}

pub(super) fn datetime2(name: &str) -> ResultColumn {
    column(name, ColumnType::DateTime2)
}

/// Milliseconds since the Unix epoch as a `datetime2`.
pub(super) fn datetime(ms: u64) -> Datum {
    Datum::DateTime2(
        (ms / MS_PER_DAY + DAYS_0001_TO_1970) as u32,
        (ms % MS_PER_DAY) * 10_000,
    )
}

pub(super) fn text(value: &str) -> Datum {
    Datum::NVarChar(value.to_string())
}

fn optional(value: &Option<String>) -> Datum {
    value.as_deref().map_or(Datum::Null, text)
}

pub(super) fn storage_error(err: StorageError) -> SqlError {
    SqlError::new(50000, 16, 1, format!("object store: {err}"))
}

pub(super) fn object_error(err: ObjectError, bucket: &str) -> SqlError {
    match err {
        ObjectError::NoSuchBucket => {
            SqlError::new(208, 16, 1, format!("The bucket '{bucket}' does not exist."))
        }
        other => SqlError::new(50000, 16, 1, format!("object store: {other:?}")),
    }
}

pub(super) fn no_object_engine() -> SqlError {
    SqlError::new(50000, 16, 1, "this database does not run the object store")
}

/// `sys.dm_s3_journal_consumers`: each journal consumer's place, how many
/// rows it has yet to read, and how many were trimmed before it read them.
fn open_consumers(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    Ok(ExternalRowset {
        columns: vec![
            nvarchar("consumer_name", 64),
            bigint("last_sequence_number"),
            bigint("pending_records"),
            bigint("dropped_records"),
        ],
        rows: Box::new(ConsumerStream {
            core: Arc::clone(core),
            done: false,
        }),
    })
}

struct ConsumerStream {
    core: Arc<Storage>,
    done: bool,
}

impl ExternalStream for ConsumerStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let (consumers, last) = {
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            (
                file.journal_consumers().map_err(storage_error)?,
                file.last_change().map_err(storage_error)?,
            )
        };
        let rows: Vec<Vec<Datum>> = consumers
            .into_iter()
            .map(|consumer| {
                vec![
                    text(&consumer.name),
                    Datum::BigInt(consumer.cursor as i64),
                    Datum::BigInt(last.saturating_sub(consumer.cursor) as i64),
                    Datum::BigInt(consumer.dropped as i64),
                ]
            })
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// `sys.dm_s3_imports` (one row per import; its secret is not shown) and
/// `sys.dm_s3_import_failures` (one row per object an import could not
/// copy).
fn open_imports(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    let failures = name == "sys.dm_s3_import_failures";
    let columns = if failures {
        vec![
            bigint("import_id"),
            nvarchar_max("key"),
            nvarchar_max("error"),
        ]
    } else {
        vec![
            bigint("import_id"),
            nvarchar("state", 16),
            nvarchar_max("source_endpoint"),
            nvarchar("source_region", 64),
            nvarchar("source_access_key", 128),
            nvarchar("source_bucket", 63),
            nvarchar_max("source_prefix"),
            nvarchar("bucket", 63),
            nvarchar_max("prefix"),
            nvarchar_max("last_key"),
            bigint("copied"),
            bigint("skipped"),
            bigint("failed"),
            bigint("bytes"),
            datetime2("created"),
            datetime2("updated"),
            nvarchar_max("last_error"),
        ]
    };
    Ok(ExternalRowset {
        columns,
        rows: Box::new(ImportStream {
            core: Arc::clone(core),
            failures,
            done: false,
        }),
    })
}

struct ImportStream {
    core: Arc<Storage>,
    failures: bool,
    done: bool,
}

impl ExternalStream for ImportStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let mut guard = self.core.lock();
        let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
        let imports = file.imports().map_err(storage_error)?;
        let mut rows: Vec<Vec<Datum>> = Vec::new();
        for import in imports {
            if self.failures {
                for failure in file.import_failures(import.id).map_err(storage_error)? {
                    rows.push(vec![
                        Datum::BigInt(import.id as i64),
                        text(&failure.key),
                        text(&failure.error),
                    ]);
                }
                continue;
            }
            rows.push(vec![
                Datum::BigInt(import.id as i64),
                text(import.state.as_str()),
                text(&import.source.endpoint),
                text(&import.source.region),
                text(&import.source.access_key),
                text(&import.source.bucket),
                text(&import.source.prefix),
                text(&import.bucket),
                text(&import.prefix),
                text(&import.after),
                Datum::BigInt(import.copied as i64),
                Datum::BigInt(import.skipped as i64),
                Datum::BigInt(import.failed as i64),
                Datum::BigInt(import.bytes as i64),
                datetime(import.created_ms),
                datetime(import.updated_ms),
                text(&import.error),
            ]);
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// `sys.dm_s3_batch_jobs`: one row per Batch Operations job, of every
/// account.
fn open_batch_jobs(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    Ok(ExternalRowset {
        columns: vec![
            nvarchar("job_id", 33),
            nvarchar("account_id", 12),
            nvarchar("status", 16),
            bigint("priority"),
            bigint("total_tasks"),
            bigint("succeeded_tasks"),
            bigint("failed_tasks"),
            datetime2("created"),
            datetime2("terminated"),
            nvarchar_max("status_reason"),
            nvarchar_max("failure"),
        ],
        rows: Box::new(BatchJobStream {
            core: Arc::clone(core),
            done: false,
        }),
    })
}

struct BatchJobStream {
    core: Arc<Storage>,
    done: bool,
}

impl ExternalStream for BatchJobStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let mut guard = self.core.lock();
        let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
        let rows: Vec<Vec<Datum>> = file
            .jobs()
            .map_err(storage_error)?
            .into_iter()
            .map(|job| {
                let failure = job
                    .failures
                    .iter()
                    .map(|(code, reason)| format!("{code}: {reason}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                vec![
                    text(&job.id()),
                    text(&job.account),
                    text(job.status.as_str()),
                    Datum::BigInt(i64::from(job.priority)),
                    Datum::BigInt(job.total as i64),
                    Datum::BigInt(job.succeeded as i64),
                    Datum::BigInt(job.failed as i64),
                    datetime(job.created_ms),
                    job.terminated_ms.map_or(Datum::Null, datetime),
                    text(&job.status_reason),
                    text(&failure),
                ]
            })
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// `sys.dm_s3_bucket_usage`: each bucket's versions, delete markers and
/// upload parts, current and noncurrent, per storage class: how many and
/// their bytes. `complete` is false while a file written before usage was
/// counted is still being counted.
fn open_bucket_usage(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    Ok(ExternalRowset {
        columns: vec![
            nvarchar("bucket_name", 63),
            nvarchar("version_kind", 16),
            nvarchar("storage_class", 32),
            bigint("object_count"),
            bigint("size_bytes"),
            bit("complete"),
        ],
        rows: Box::new(BucketUsageStream {
            core: Arc::clone(core),
            done: false,
        }),
    })
}

struct BucketUsageStream {
    core: Arc<Storage>,
    done: bool,
}

impl ExternalStream for BucketUsageStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let mut guard = self.core.lock();
        let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
        let complete = file.usage_complete().map_err(storage_error)?;
        let rows: Vec<Vec<Datum>> = file
            .bucket_usage()
            .map_err(storage_error)?
            .into_iter()
            .map(|usage| {
                vec![
                    text(&usage.bucket),
                    text(usage.kind.as_str()),
                    text(usage.storage_class.as_str()),
                    Datum::BigInt(usage.objects as i64),
                    Datum::BigInt(usage.bytes as i64),
                    Datum::Bit(complete),
                ]
            })
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// How many events `sys.dm_s3_api_activity` reads per storage-lock hold.
const ACTIVITY_SLICE: usize = 512;

/// `sys.dm_s3_api_activity`: the S3 management events of the last 90 days
/// (CloudTrail's), by account and newest first within each: when, who,
/// what, from where, with what error, on which bucket, and the whole
/// CloudTrail record.
fn open_api_activity(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    Ok(ExternalRowset {
        columns: vec![
            datetime2("event_time"),
            nvarchar("account_id", 12),
            nvarchar("event_source", 64),
            nvarchar("event_name", 128),
            nvarchar("user_name", 128),
            nvarchar("access_key_id", 128),
            nvarchar("source_ip", 64),
            bit("read_only"),
            nvarchar("error_code", 128),
            nvarchar("resource_name", 1024),
            nvarchar("event_id", 36),
            nvarchar_max("cloudtrail_event"),
        ],
        rows: Box::new(ActivityStream {
            core: Arc::clone(core),
            after: None,
            done: false,
        }),
    })
}

struct ActivityStream {
    core: Arc<Storage>,
    after: Option<Vec<u8>>,
    done: bool,
}

impl ExternalStream for ActivityStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        let events = {
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            file.activity(None, self.after.as_deref(), ACTIVITY_SLICE)
                .map_err(storage_error)?
        };
        self.done = events.len() < ACTIVITY_SLICE;
        self.after = events.last().map(|event| event.resume_key());
        let rows: Vec<Vec<Datum>> = events
            .iter()
            .map(|event| {
                let record: serde_json::Value =
                    serde_json::from_slice(&event.record).unwrap_or_default();
                let field = |path: &[&str]| {
                    path.iter()
                        .try_fold(&record, |value, name| value.get(name))
                        .and_then(serde_json::Value::as_str)
                        .map_or(Datum::Null, text)
                };
                vec![
                    datetime(event.time_ms),
                    text(&event.account),
                    field(&["eventSource"]),
                    field(&["eventName"]),
                    field(&["userIdentity", "userName"]),
                    field(&["userIdentity", "accessKeyId"]),
                    field(&["sourceIPAddress"]),
                    record
                        .get("readOnly")
                        .and_then(serde_json::Value::as_bool)
                        .map_or(Datum::Null, Datum::Bit),
                    field(&["errorCode"]),
                    field(&["requestParameters", "bucketName"]),
                    field(&["eventID"]),
                    text(&String::from_utf8_lossy(&event.record)),
                ]
            })
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// `sys.dm_s3_requests`: each bucket's S3 requests since the server started,
/// by kind, their bytes and errors; `sys.dm_s3_request_latency`: their
/// latencies (until the answer starts, until its last byte), as counts per
/// upper bound in milliseconds (NULL: past the last bound). Requests to
/// buckets past the ones kept apart count under an empty name.
fn open_requests(
    metrics: &Arc<crate::metrics::RequestMetrics>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    let snapshot = metrics.snapshot();
    let (columns, rows): (Vec<ResultColumn>, Vec<Vec<Datum>>) = if name == "sys.dm_s3_requests" {
        (
            vec![
                nvarchar("bucket_name", 63),
                bigint("all_requests"),
                bigint("get_requests"),
                bigint("put_requests"),
                bigint("delete_requests"),
                bigint("head_requests"),
                bigint("post_requests"),
                bigint("list_requests"),
                bigint("bytes_downloaded"),
                bigint("bytes_uploaded"),
                bigint("errors_4xx"),
                bigint("errors_5xx"),
            ],
            snapshot
                .iter()
                .map(|(bucket, counters)| {
                    let counts = counters.counts();
                    let mut row = vec![text(bucket)];
                    row.extend(
                        [
                            counts.all,
                            counts.get,
                            counts.put,
                            counts.delete,
                            counts.head,
                            counts.post,
                            counts.list,
                            counts.bytes_downloaded,
                            counts.bytes_uploaded,
                            counts.errors_4xx,
                            counts.errors_5xx,
                        ]
                        .map(|count| Datum::BigInt(count as i64)),
                    );
                    row
                })
                .collect(),
        )
    } else {
        let mut rows = Vec::new();
        for (bucket, counters) in &snapshot {
            for (latency, histogram) in [
                ("FIRST_BYTE", &counters.first_byte),
                ("TOTAL", &counters.total),
            ] {
                for (bound, count) in histogram.snapshot() {
                    rows.push(vec![
                        text(bucket),
                        text(latency),
                        bound.map_or(Datum::Null, |bound| Datum::BigInt(bound as i64)),
                        Datum::BigInt(count as i64),
                    ]);
                }
            }
        }
        (
            vec![
                nvarchar("bucket_name", 63),
                nvarchar("latency", 16),
                bigint("upper_bound_ms"),
                bigint("request_count"),
            ],
            rows,
        )
    };
    Ok(ExternalRowset {
        columns,
        rows: Box::new(Rows(Some(rows))),
    })
}

/// Rows read up front, in one slice.
struct Rows(Option<Vec<Vec<Datum>>>);

impl ExternalStream for Rows {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        Ok(self.0.take().filter(|rows| !rows.is_empty()))
    }
}

/// How many bodies `sys.dm_s3_storage` reads per storage-lock hold.
const STORAGE_BODIES_PER_HOLD: usize = 1024;

/// `sys.dm_s3_storage`: what object bodies take on disk, by their use (live,
/// collectable, staging) and the zstd level they were packed at: how many,
/// their bytes as written, as stored and as allocated pages, and the bytes
/// packing saved.
fn open_storage(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
    caller: ExternalCaller,
) -> Result<ExternalRowset, SqlError> {
    check_admin(name, caller)?;
    check_arg_count(name, args, 0, 0)?;
    Ok(ExternalRowset {
        columns: vec![
            nvarchar("body_state", 16),
            bigint("pack_level"),
            bigint("body_count"),
            bigint("logical_bytes"),
            bigint("stored_bytes"),
            bigint("allocated_bytes"),
            bigint("saved_bytes"),
        ],
        rows: Box::new(StorageStream {
            core: Arc::clone(core),
            done: false,
        }),
    })
}

struct StorageStream {
    core: Arc<Storage>,
    done: bool,
}

impl ExternalStream for StorageStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        // (use, level) -> bodies, logical, stored, allocated; summed over
        // the bodies a batch at a time, each batch under a hold of its own.
        let mut totals: std::collections::BTreeMap<(BodyUse, u8), [u64; 4]> = Default::default();
        let mut after = None;
        loop {
            let (bodies, last) = {
                let mut guard = self.core.lock();
                let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
                file.stored_bodies(after, STORAGE_BODIES_PER_HOLD)
                    .map_err(storage_error)?
            };
            for body in bodies {
                let total = totals.entry((body.body_use, body.pack_level)).or_default();
                total[0] += 1;
                total[1] += body.logical;
                total[2] += body.stored;
                total[3] += body.allocated;
            }
            match last {
                Some(last) => after = Some(last),
                None => break,
            }
        }
        let rows: Vec<Vec<Datum>> = totals
            .into_iter()
            .map(
                |((body_use, level), [bodies, logical, stored, allocated])| {
                    vec![
                        text(body_use.as_str()),
                        Datum::BigInt(i64::from(level)),
                        Datum::BigInt(bodies as i64),
                        Datum::BigInt(logical as i64),
                        Datum::BigInt(stored as i64),
                        Datum::BigInt(allocated as i64),
                        Datum::BigInt(logical as i64 - stored as i64),
                    ]
                },
            )
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// `sys.s3_buckets`: one slice, the whole (small) bucket list.
struct BucketStream {
    core: Arc<Storage>,
    done: bool,
}

impl ExternalStream for BucketStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let buckets = {
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            file.list_buckets().map_err(storage_error)?
        };
        let rows: Vec<Vec<Datum>> = buckets
            .into_iter()
            .map(|bucket| {
                vec![
                    text(&bucket.name),
                    datetime(bucket.created_ms),
                    match bucket.versioning {
                        Versioning::Unversioned => Datum::Null,
                        Versioning::Enabled => text("Enabled"),
                        Versioning::Suspended => text("Suspended"),
                    },
                ]
            })
            .collect();
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// The per-key rowsets: a listing of `bucket` under `prefix`, resumed after
/// the last key (or version) of the previous slice.
struct ObjectStream {
    core: Arc<Storage>,
    kind: Kind,
    bucket: String,
    prefix: String,
    /// Where the next slice resumes: after this key, or after this version
    /// of it in a version listing.
    after: Option<(String, Option<u64>)>,
    done: bool,
}

impl ObjectStream {
    /// One listing slice and the attributes its rows need, under one lock
    /// hold.
    fn read_slice(&self) -> Result<(Listing, Vec<ObjectAttrs>), SqlError> {
        let mut guard = self.core.lock();
        let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
        let after = self.after.as_ref().map(|(key, version)| match version {
            Some(version) => ListAfter::Version(key, *version),
            None => ListAfter::Key(key),
        });
        let listing = if self.kind == Kind::Versions {
            file.list_versions(&self.bucket, &self.prefix, None, after, SLICE_KEYS)
        } else {
            file.list_objects(&self.bucket, &self.prefix, None, after, SLICE_KEYS)
        }
        .map_err(storage_error)?
        .map_err(|err| object_error(err, &self.bucket))?;
        let attrs = listing
            .objects
            .iter()
            .map(|object| {
                if object.info.delete_marker {
                    Ok(ObjectAttrs::default())
                } else {
                    file.version_attrs(object.info.version)
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage_error)?;
        Ok((listing, attrs))
    }

    fn rows_of(
        &self,
        key: &str,
        info: &ObjectInfo,
        latest: bool,
        attrs: &ObjectAttrs,
        out: &mut Vec<Vec<Datum>>,
    ) {
        let identity = || {
            vec![
                text(&self.bucket),
                text(key),
                text(&version_id(info.version, info.null_version)),
            ]
        };
        match self.kind {
            Kind::Objects | Kind::Versions => {
                let mut row = identity();
                if info.delete_marker {
                    row.extend([Datum::Null, datetime(info.mtime_ms)]);
                    row.extend(std::iter::repeat_n(Datum::Null, 7));
                    row.push(Datum::Null);
                } else {
                    row.extend([
                        Datum::BigInt(info.size as i64),
                        datetime(info.mtime_ms),
                        text(&info.etag()),
                        optional(&attrs.content_type),
                        optional(&attrs.cache_control),
                        optional(&attrs.content_disposition),
                        optional(&attrs.content_encoding),
                        optional(&attrs.content_language),
                        optional(&attrs.expires),
                        text(info.storage_class.as_str()),
                    ]);
                }
                if self.kind == Kind::Versions {
                    row.extend([Datum::Bit(latest), Datum::Bit(info.delete_marker)]);
                }
                out.push(row);
            }
            Kind::Tags | Kind::Metadata => {
                let pairs = if self.kind == Kind::Tags {
                    &attrs.tags
                } else {
                    &attrs.user
                };
                for (name, value) in pairs {
                    let mut row = identity();
                    row.extend([text(name), text(value)]);
                    out.push(row);
                }
            }
        }
    }
}

impl ExternalStream for ObjectStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        let mut rows = Vec::new();
        // A slice can list keys that make no rows (no tags, say), or none at
        // all when its row budget went to deleted keys: read on until some
        // rows come, or the listing ends.
        while rows.is_empty() && !self.done {
            // A run of slices that yield no rows is cancellable too.
            check_cancelled()?;
            let (listing, attrs) = self.read_slice()?;
            for (object, attrs) in listing.objects.iter().zip(&attrs) {
                self.rows_of(&object.key, &object.info, object.latest, attrs, &mut rows);
            }
            match (listing.truncated, listing.last) {
                (true, Some((last, _))) => self.after = Some((last, listing.last_version)),
                _ => self.done = true,
            }
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// `sys.s3_journal`: the journal from `after` on, a bounded slice of rows per
/// lock hold, keeping `bucket`'s.
struct JournalStream {
    core: Arc<Storage>,
    bucket: String,
    after: u64,
    done: bool,
}

/// An S3 ETag: the MD5 in hex, with the part count for a multipart object.
fn etag(md5: &[u8; 16], parts: u16) -> String {
    let hex: String = md5.iter().map(|b| format!("{b:02x}")).collect();
    if parts == 0 {
        format!("\"{hex}\"")
    } else {
        format!("\"{hex}-{parts}\"")
    }
}

fn journal_row(record: &JournalRecord) -> Vec<Datum> {
    let object = record.object.as_ref();
    let origin = record.origin.as_ref();
    vec![
        text(&record.bucket),
        text(&record.key),
        Datum::BigInt(record.sequence as i64),
        text(record.record_type.as_str()),
        datetime(record.timestamp_ms),
        text(&version_id(record.version, record.null_version)),
        record.is_delete_marker.map_or(Datum::Null, Datum::Bit),
        object.map_or(Datum::Null, |o| Datum::BigInt(o.size as i64)),
        object.map_or(Datum::Null, |o| datetime(o.last_modified_ms)),
        match (object, record.is_delete_marker) {
            (Some(o), Some(false)) => text(&etag(&o.md5, o.parts)),
            _ => Datum::Null,
        },
        match (object, record.is_delete_marker) {
            (Some(o), Some(false)) => text(o.storage_class.as_str()),
            _ => Datum::Null,
        },
        match (object, record.is_delete_marker) {
            (Some(o), Some(false)) => Datum::Bit(o.parts > 0),
            _ => Datum::Null,
        },
        // Who made the change: the account (as S3 Metadata's requester), and
        // where from; none for a change made without a request.
        origin
            .and_then(|o| o.account.as_deref())
            .map_or(Datum::Null, text),
        origin
            .and_then(|o| o.source_ip.as_deref())
            .map_or(Datum::Null, text),
        origin.map_or(Datum::Null, |o| text(&o.request_id)),
    ]
}

impl ExternalStream for JournalStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        let mut rows = Vec::new();
        while rows.is_empty() && !self.done {
            // Other buckets' rows can fill many slices: stay cancellable.
            check_cancelled()?;
            let records = {
                let mut guard = self.core.lock();
                let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
                file.journal(self.after, SLICE_KEYS)
                    .map_err(storage_error)?
            };
            if records.len() < SLICE_KEYS {
                self.done = true;
            }
            if let Some(last) = records.last() {
                self.after = last.sequence;
            }
            rows.extend(
                records
                    .iter()
                    // A notice is not one of S3 Metadata's records.
                    .filter(|record| {
                        record.bucket == self.bucket
                            && record.record_type != truthdb_object::RecordType::Notice
                    })
                    .map(journal_row),
            );
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// A search condition SQL Server would refuse as malformed is 7630; one
/// past this engine's limits is 7619 (a full-text query that failed).
fn search_error(err: SearchError) -> SqlError {
    match err {
        // Well formed but past a limit: a query that failed, as below.
        SearchError::Query(err) if err.past_limit => SqlError::new(
            7619,
            16,
            1,
            format!("The execution of a full-text query failed. \"{err}\""),
        ),
        SearchError::Query(err) => SqlError::new(7630, 15, 2, err.to_string()),
        SearchError::Storage(err) => storage_error(err),
        other => SqlError::new(
            7619,
            16,
            1,
            format!("The execution of a full-text query failed. \"{other}\""),
        ),
    }
}

/// `sys.s3_search`: the postings are read a step per storage-lock hold on
/// the first slice, then the ranked hits are checked against each key's
/// latest version a slice at a time.
struct SearchStream {
    core: Arc<Storage>,
    bucket: String,
    search: Search,
    /// The ranked hits not yet looked at, once every posting is read.
    hits: Option<VecDeque<Hit>>,
    /// Rows still to return.
    left: usize,
}

impl ExternalStream for SearchStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        while self.hits.is_none() {
            check_cancelled()?;
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            if file
                .fulltext_step(&mut self.search, &self.bucket, SEARCH_STEP_POSTINGS)
                .map_err(search_error)?
            {
                let stats = file.fulltext_stats(&self.bucket).map_err(storage_error)?;
                self.hits = Some(self.search.rank(stats).into());
            }
        }
        let hits = self.hits.as_mut().expect("ranked");
        let mut rows = Vec::new();
        while rows.is_empty() && self.left > 0 && !hits.is_empty() {
            check_cancelled()?;
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard).ok_or_else(no_object_engine)?;
            for _ in 0..SLICE_KEYS {
                if self.left == 0 {
                    break;
                }
                let Some(hit) = hits.pop_front() else {
                    break;
                };
                // The hit counts while the version that matched is still the
                // key's latest: the index follows the writes a moment behind.
                let Some(key) = file
                    .fulltext_version_key(&self.bucket, hit.doc)
                    .map_err(storage_error)?
                else {
                    continue;
                };
                let Ok(info) = file
                    .head_object(&self.bucket, &key, VersionSel::Latest)
                    .map_err(storage_error)?
                else {
                    continue;
                };
                if info.version != hit.doc {
                    continue;
                }
                rows.push(vec![
                    text(&self.bucket),
                    text(&key),
                    text(&version_id(info.version, info.null_version)),
                    Datum::Float(hit.score),
                    Datum::BigInt(info.size as i64),
                    datetime(info.mtime_ms),
                ]);
                self.left -= 1;
            }
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}
