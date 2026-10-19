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
//!
//! Rows stream a slice at a time, each slice under its own storage-lock hold,
//! so a scan reads what is committed when each slice is read (read
//! committed), not one snapshot. Buckets are server-wide: only a server
//! administrator may read them.

use std::sync::Arc;

use truthdb_core::storage::{Storage, StorageError};
use truthdb_object::{
    ListAfter, Listing, ObjFile, ObjectAttrs, ObjectError, ObjectInfo, Versioning, version_id,
};
use truthdb_relational::engine::{
    ExternalCaller, ExternalRowset, ExternalRowsets, ExternalStream, ResultColumn, check_cancelled,
};
use truthdb_relational::relstore::types::{ColumnType, Datum};
use truthdb_sql::error::SqlError;
use truthdb_sql::value::SqlValue;

/// Keys one slice lists (one storage-lock hold), as a table scan's slice.
const SLICE_KEYS: usize = 1024;

/// Days from 0001-01-01 to 1970-01-01.
const DAYS_0001_TO_1970: u64 = 719_162;
const MS_PER_DAY: u64 = 86_400_000;

/// The object store's rowsets, over the storage the engine runs on.
pub(crate) struct S3Rowsets {
    pub(crate) core: Arc<Storage>,
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
            _ => return None,
        };
        Some(self.open_kind(name, kind, args, caller))
    }
}

impl S3Rowsets {
    fn open_kind(
        &self,
        name: &str,
        kind: Option<Kind>,
        args: &[SqlValue],
        caller: ExternalCaller,
    ) -> Result<ExternalRowset, SqlError> {
        if !caller.server_admin {
            return Err(SqlError::new(
                229,
                14,
                5,
                format!(
                    "The SELECT permission was denied on the object '{}', database 'mssqlsystemresource', schema 'sys'.",
                    name.trim_start_matches("sys.")
                ),
            ));
        }
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
                    nvarchar("storage_class", 16),
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

fn check_arg_count(name: &str, args: &[SqlValue], min: usize, max: usize) -> Result<(), SqlError> {
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

fn string_arg(name: &str, value: &SqlValue) -> Result<String, SqlError> {
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

fn column(name: &str, column_type: ColumnType) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type,
    }
}

fn nvarchar(name: &str, max_len: u16) -> ResultColumn {
    column(name, ColumnType::NVarChar { max_len })
}

fn nvarchar_max(name: &str) -> ResultColumn {
    column(name, ColumnType::NVarCharMax)
}

fn bigint(name: &str) -> ResultColumn {
    column(name, ColumnType::BigInt)
}

fn bit(name: &str) -> ResultColumn {
    column(name, ColumnType::Bit)
}

fn datetime2(name: &str) -> ResultColumn {
    column(name, ColumnType::DateTime2)
}

/// Milliseconds since the Unix epoch as a `datetime2`.
fn datetime(ms: u64) -> Datum {
    Datum::DateTime2(
        (ms / MS_PER_DAY + DAYS_0001_TO_1970) as u32,
        (ms % MS_PER_DAY) * 10_000,
    )
}

fn text(value: &str) -> Datum {
    Datum::NVarChar(value.to_string())
}

fn optional(value: &Option<String>) -> Datum {
    value.as_deref().map_or(Datum::Null, text)
}

fn storage_error(err: StorageError) -> SqlError {
    SqlError::new(50000, 16, 1, format!("object store: {err}"))
}

fn object_error(err: ObjectError, bucket: &str) -> SqlError {
    match err {
        ObjectError::NoSuchBucket => {
            SqlError::new(208, 16, 1, format!("The bucket '{bucket}' does not exist."))
        }
        other => SqlError::new(50000, 16, 1, format!("object store: {other:?}")),
    }
}

fn no_object_engine() -> SqlError {
    SqlError::new(50000, 16, 1, "this database does not run the object store")
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
                        text("STANDARD"),
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
