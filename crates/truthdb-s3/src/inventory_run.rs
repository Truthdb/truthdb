//! S3 Inventory reports: a task beside the S3 listener that makes each
//! enabled configuration's report when it is due (see [`due`]), one at a
//! time. A run lists the source bucket a page at a time, each page under
//! one storage-lock hold, into a body (a gzipped CSV, or a Parquet file
//! compressed with Snappy, a row group a page); then one transaction writes
//! the list, its `manifest.json`, `manifest.checksum` and Hive
//! `symlink.txt` to the destination and records the run, once it has checked
//! that the source bucket and the configuration are still the ones listed
//! and that the destination is still the source owner's. A run cut short (a
//! crash, a shutdown) leaves nothing behind but a staging body, which
//! garbage collection reclaims, and starts again from the beginning. A
//! standby runs none (its log is the primary's).

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use parquet::file::writer::SerializedFileWriter;
use tokio::sync::watch;
use truthdb_core::storage::{Storage, StorageError};
use truthdb_object::{
    BUCKET_INVENTORY, BodyWriter, InventoryConfig, InventoryFormat, InventoryFrequency, ListAfter,
    ListedObject, ObjFile, ObjectAttrs, ObjectLock, Origin, RetentionMode, SealedBody,
    StorageClass, Versioning, time::iso8601,
};

use crate::batch_run::{REPORT_KEY, object};
use crate::objects::{DROP_GRACE_MS, now_ms};
use crate::server::S3State;

/// How long the runner waits when no report is due (unless a configuration
/// is put meanwhile, see [`wake`]), and after a run failed (a storage
/// error).
const IDLE: Duration = Duration::from_secs(60);
const RETRY: Duration = Duration::from_secs(60);

/// Wakes the runner (a configuration was put: its first report is due).
static WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();

/// Tells the runner a configuration was put.
pub(crate) fn wake() {
    WAKE.notify_one();
}
/// Versions listed a page (one lock hold).
const PAGE: usize = 1000;
const DAY_MS: u64 = 86_400_000;
/// How often a list being written is marked touched: one whose pages add
/// little takes long to fill a segment, and a body untouched for an hour is
/// taken for abandoned.
const TOUCH_MS: u64 = 5 * 60 * 1000;

/// How a column's values are typed in a Parquet list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Text,
    Flag,
    Count,
    /// A 32-bit count (the event hold's duration).
    Small,
    /// A time, in milliseconds since the epoch.
    Millis,
}

/// Every column in the order S3 writes them: its name (in a CSV list's
/// schema), its Parquet name and type. Bucket and Key are always there,
/// VersionId, IsLatest and IsDeleteMarker with every version, the others
/// when asked for (the event hold's duration brings its unit).
const COLUMNS: [(&str, &str, Kind); 25] = [
    ("Bucket", "bucket", Kind::Text),
    ("Key", "key", Kind::Text),
    ("VersionId", "version_id", Kind::Text),
    ("IsLatest", "is_latest", Kind::Flag),
    ("IsDeleteMarker", "is_delete_marker", Kind::Flag),
    ("Size", "size", Kind::Count),
    ("LastModifiedDate", "last_modified_date", Kind::Millis),
    ("ETag", "e_tag", Kind::Text),
    ("StorageClass", "storage_class", Kind::Text),
    ("IsMultipartUploaded", "is_multipart_uploaded", Kind::Flag),
    ("ReplicationStatus", "replication_status", Kind::Text),
    ("EncryptionStatus", "encryption_status", Kind::Text),
    (
        "ObjectLockRetainUntilDate",
        "object_lock_retain_until_date",
        Kind::Millis,
    ),
    ("ObjectLockMode", "object_lock_mode", Kind::Text),
    (
        "ObjectLockLegalHoldStatus",
        "object_lock_legal_hold_status",
        Kind::Text,
    ),
    (
        "ObjectLockEventHoldStatus",
        "object_lock_event_hold_status",
        Kind::Text,
    ),
    (
        "ObjectLockEventHoldDuration",
        "object_lock_event_hold_duration",
        Kind::Small,
    ),
    (
        "ObjectLockEventHoldDurationUnit",
        "object_lock_event_hold_duration_unit",
        Kind::Text,
    ),
    (
        "IntelligentTieringAccessTier",
        "intelligent_tiering_access_tier",
        Kind::Text,
    ),
    ("BucketKeyStatus", "bucket_key_status", Kind::Text),
    ("ChecksumAlgorithm", "checksum_algorithm", Kind::Text),
    (
        "ObjectAccessControlList",
        "object_access_control_list",
        Kind::Text,
    ),
    ("ObjectOwner", "object_owner", Kind::Text),
    (
        "LifecycleExpirationDate",
        "lifecycle_expiration_date",
        Kind::Millis,
    ),
    (
        "IntelligentTieringReferenceDate",
        "intelligent_tiering_reference_date",
        Kind::Millis,
    ),
];

/// The columns of a configuration's list.
fn columns(config: &InventoryConfig) -> Vec<(&'static str, &'static str, Kind)> {
    COLUMNS
        .into_iter()
        .filter(|(name, _, _)| match *name {
            "Bucket" | "Key" => true,
            "VersionId" | "IsLatest" | "IsDeleteMarker" => config.all_versions,
            "ObjectLockEventHoldDurationUnit" => config
                .optional_fields
                .iter()
                .any(|field| field == "ObjectLockEventHoldDuration"),
            _ => config.optional_fields.iter().any(|field| field == name),
        })
        .collect()
}

/// Whether a column always has a value (Bucket and Key).
fn required(name: &str) -> bool {
    matches!(name, "Bucket" | "Key")
}

/// Makes reports until `shutdown` turns true.
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let wait = if state.storage.is_standby() {
            IDLE
        } else {
            match step(&state, &shutdown).await {
                Ok(true) => continue,
                Ok(false) => IDLE,
                Err(err) => {
                    eprintln!("inventory reports: {err}");
                    RETRY
                }
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = WAKE.notified() => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// Whether a report last run at `last_ms` (0: never) is due at `now_ms`:
/// the first at once, then a daily one each UTC day, a weekly one each
/// Sunday (as S3 makes them).
fn due(frequency: InventoryFrequency, last_ms: u64, now_ms: u64) -> bool {
    if last_ms == 0 {
        return true;
    }
    let (last_day, today) = (last_ms / DAY_MS, now_ms / DAY_MS);
    match frequency {
        InventoryFrequency::Daily => last_day < today,
        // Day 0 (1970-01-01) was a Thursday: Sundays are the days d with
        // (d + 4) % 7 == 0.
        InventoryFrequency::Weekly => last_day < today - (today + 4) % 7,
    }
}

/// A report to make: its configuration (and the stored document, to tell
/// it unchanged at the end), and the source bucket as it was when the run
/// began.
struct Due {
    bucket: String,
    document: Vec<u8>,
    config: InventoryConfig,
    owner: String,
    created_ms: u64,
    versioning: Versioning,
}

/// Makes the first due report; whether there was one.
async fn step(state: &S3State, shutdown: &watch::Receiver<bool>) -> Result<bool, String> {
    let now = now_ms();
    let next = object(state, move |file| {
        for (bucket, id, last) in file.inventory_runs()? {
            // Run today: due tomorrow at the earliest, whatever its
            // frequency (its document is not read).
            if last != 0 && last / DAY_MS == now / DAY_MS {
                continue;
            }
            let Some(document) = file.bucket_config(&bucket, BUCKET_INVENTORY, &id)? else {
                continue;
            };
            let Some(config) = InventoryConfig::decode(&document) else {
                continue;
            };
            if !config.enabled || !due(config.frequency, last, now) {
                continue;
            }
            let Some(info) = file.bucket(&bucket)? else {
                continue;
            };
            return Ok(Some(Due {
                bucket,
                document,
                config,
                owner: info.owner,
                created_ms: info.created_ms,
                versioning: info.versioning,
            }));
        }
        Ok(None)
    })
    .await?;
    let Some(due) = next else {
        return Ok(false);
    };
    make_report(state, due, now, shutdown).await?;
    Ok(true)
}

/// How a list is encoded.
enum Encoder {
    Csv(Box<flate2::write::GzEncoder<Vec<u8>>>),
    /// With its columns' kinds and whether each always has a value.
    Parquet(Box<SerializedFileWriter<Vec<u8>>>, Vec<(Kind, bool)>),
}

/// Row groups (a page each) a Parquet list file holds at most: a list of
/// more goes on in another file, so neither a file's footer nor what the
/// writer keeps of it grows with the bucket.
const GROUPS_PER_FILE: usize = 100;

/// The list being written: its files, each a body.
struct List {
    format: InventoryFormat,
    columns: Vec<(&'static str, &'static str, Kind)>,
    encoder: Encoder,
    writer: BodyWriter,
    /// Bytes written to the file being written so far.
    size: u64,
    /// Row groups in the file being written.
    groups: usize,
    /// The files written whole, and their sizes.
    done: Vec<(SealedBody, u64)>,
    /// When the bodies were last marked touched.
    touched_ms: u64,
}

fn parquet_error(err: parquet::errors::ParquetError) -> String {
    err.to_string()
}

impl List {
    fn begin(
        storage: &Storage,
        format: InventoryFormat,
        columns: Vec<(&'static str, &'static str, Kind)>,
    ) -> Result<List, String> {
        let encoder = List::encoder(format, &columns)?;
        let now = now_ms();
        let writer = BodyWriter::begin(storage, now).map_err(|err| err.to_string())?;
        Ok(List {
            format,
            columns,
            encoder,
            writer,
            size: 0,
            groups: 0,
            done: Vec::new(),
            touched_ms: now,
        })
    }

    /// An encoder for a list of `columns` in `format`.
    fn encoder(
        format: InventoryFormat,
        columns: &[(&'static str, &'static str, Kind)],
    ) -> Result<Encoder, String> {
        Ok(match format {
            InventoryFormat::Csv => Encoder::Csv(Box::new(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::default(),
            ))),
            InventoryFormat::Parquet => {
                let schema = parquet::schema::parser::parse_message_type(&parquet_schema(columns))
                    .map_err(parquet_error)?;
                let props = parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::SNAPPY)
                    .build();
                let writer =
                    SerializedFileWriter::new(Vec::new(), Arc::new(schema), Arc::new(props))
                        .map_err(parquet_error)?;
                let kinds = columns
                    .iter()
                    .map(|(name, _, kind)| (*kind, required(name)))
                    .collect();
                Encoder::Parquet(Box::new(writer), kinds)
            }
        })
    }

    /// Adds a page's `rows`, writing what is encoded so far.
    fn add(&mut self, storage: &Storage, rows: &[Vec<Value>]) -> Result<(), String> {
        let out = match &mut self.encoder {
            Encoder::Csv(encoder) => {
                for row in rows {
                    encoder
                        .write_all(csv_line(row).as_bytes())
                        .map_err(|err| err.to_string())?;
                }
                std::mem::take(encoder.get_mut())
            }
            Encoder::Parquet(writer, kinds) => {
                write_row_group(writer, kinds, rows).map_err(parquet_error)?;
                self.groups += 1;
                writer.flush().map_err(|err| err.to_string())?;
                // Taken from under the writer, which counts its offsets
                // itself.
                std::mem::take(writer.inner_mut())
            }
        };
        self.size += out.len() as u64;
        let now = now_ms();
        self.writer
            .write(storage, &out, now)
            .map_err(|err| err.to_string())?;
        if self.groups == GROUPS_PER_FILE {
            let encoder = List::encoder(self.format, &self.columns)?;
            let writer = BodyWriter::begin(storage, now).map_err(|err| err.to_string())?;
            let full = (
                std::mem::replace(&mut self.encoder, encoder),
                std::mem::replace(&mut self.writer, writer),
            );
            let file = seal(storage, full.0, full.1, self.size)?;
            self.done.push(file);
            self.size = 0;
            self.groups = 0;
        }
        if now.saturating_sub(self.touched_ms) >= TOUCH_MS {
            self.writer
                .touch(storage, now)
                .map_err(|err| err.to_string())?;
            // The files written whole wait for the delivery as they are.
            ObjFile::new(&mut storage.lock())
                .transaction(|tx| {
                    for (body, _) in &self.done {
                        tx.touch_body(body.body, now)?;
                    }
                    Ok(())
                })
                .map_err(|err| err.to_string())?;
            self.touched_ms = now;
        }
        Ok(())
    }

    /// The list's files, and their sizes.
    fn finish(self, storage: &Storage) -> Result<Vec<(SealedBody, u64)>, String> {
        let mut files = self.done;
        // A Parquet list that ended with a file just made full has no more.
        if files.is_empty() || self.groups > 0 || self.format == InventoryFormat::Csv {
            files.push(seal(storage, self.encoder, self.writer, self.size)?);
        }
        Ok(files)
    }
}

/// Ends a list file (`size` bytes written so far): its body and its size.
fn seal(
    storage: &Storage,
    encoder: Encoder,
    mut writer: BodyWriter,
    size: u64,
) -> Result<(SealedBody, u64), String> {
    let out = match encoder {
        Encoder::Csv(encoder) => encoder.finish().map_err(|err| err.to_string())?,
        Encoder::Parquet(mut encoder, _) => {
            encoder.finish().map_err(parquet_error)?;
            encoder.flush().map_err(|err| err.to_string())?;
            std::mem::take(encoder.inner_mut())
        }
    };
    writer
        .write(storage, &out, now_ms())
        .map_err(|err| err.to_string())?;
    let sealed = writer
        .finish(storage, now_ms())
        .map_err(|err| err.to_string())?;
    Ok((sealed, size + out.len() as u64))
}

/// Writes `rows` as one row group.
fn write_row_group(
    writer: &mut SerializedFileWriter<Vec<u8>>,
    kinds: &[(Kind, bool)],
    rows: &[Vec<Value>],
) -> parquet::errors::Result<()> {
    use parquet::data_type::{BoolType, ByteArray, ByteArrayType, Int32Type, Int64Type};
    let mut group = writer.next_row_group()?;
    for (index, (kind, required)) in kinds.iter().enumerate() {
        let mut column = group
            .next_column()?
            .expect("a column for each of the schema's");
        let cells = rows.iter().map(|row| &row[index]);
        let levels: Vec<i16> = cells
            .clone()
            .map(|cell| i16::from(*cell != Value::Null))
            .collect();
        let levels = (!required).then_some(levels.as_slice());
        match kind {
            Kind::Text => {
                let values: Vec<ByteArray> = cells
                    .filter_map(|cell| match cell {
                        Value::Text(text) => Some(ByteArray::from(text.as_bytes().to_vec())),
                        _ => None,
                    })
                    .collect();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&values, levels, None)?;
            }
            Kind::Flag => {
                let values: Vec<bool> = cells
                    .filter_map(|cell| match cell {
                        Value::Flag(on) => Some(*on),
                        _ => None,
                    })
                    .collect();
                column
                    .typed::<BoolType>()
                    .write_batch(&values, levels, None)?;
            }
            Kind::Count | Kind::Millis => {
                let values: Vec<i64> = cells
                    .filter_map(|cell| match cell {
                        Value::Count(count) | Value::Millis(count) => {
                            Some(i64::try_from(*count).unwrap_or(i64::MAX))
                        }
                        _ => None,
                    })
                    .collect();
                column
                    .typed::<Int64Type>()
                    .write_batch(&values, levels, None)?;
            }
            Kind::Small => {
                let values: Vec<i32> = cells
                    .filter_map(|cell| match cell {
                        Value::Count(count) => Some(i32::try_from(*count).unwrap_or(i32::MAX)),
                        _ => None,
                    })
                    .collect();
                column
                    .typed::<Int32Type>()
                    .write_batch(&values, levels, None)?;
            }
        }
        column.close()?;
    }
    group.close()?;
    Ok(())
}

/// Runs `op` on the list without holding up the runtime.
async fn on_list<T: Send + 'static>(
    state: &S3State,
    list: List,
    op: impl FnOnce(&Storage, List) -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    let storage = Arc::clone(&state.storage);
    tokio::task::spawn_blocking(move || op(&storage, list))
        .await
        .map_err(|err| err.to_string())?
}

/// Lists the source bucket into a report and delivers it. Returns early,
/// recording nothing, when the source bucket goes or the server shuts down.
async fn make_report(
    state: &S3State,
    due: Due,
    started: u64,
    shutdown: &watch::Receiver<bool>,
) -> Result<(), String> {
    let Due {
        bucket,
        document,
        config,
        owner,
        created_ms,
        versioning,
    } = due;
    let (storage, format, list_columns) =
        (Arc::clone(&state.storage), config.format, columns(&config));
    let mut list = tokio::task::spawn_blocking(move || List::begin(&storage, format, list_columns))
        .await
        .map_err(|err| err.to_string())??;
    let encrypted = state.storage.is_encrypted();
    let row = Arc::new(Row::new(&config, &bucket, &owner, versioning, encrypted));
    let mut after: Option<(String, Option<u64>)> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let (name, prefix, all, row) = (
            bucket.clone(),
            config.prefix.clone().unwrap_or_default(),
            config.all_versions,
            Arc::clone(&row),
        );
        let resume = after.clone();
        let page = object(state, move |file| {
            if file
                .bucket(&name)?
                .is_none_or(|info| info.created_ms != created_ms)
            {
                return Ok(None);
            }
            let after = resume.as_ref().map(|(key, version)| match version {
                Some(version) => ListAfter::Version(key, *version),
                None => ListAfter::Key(key),
            });
            let listing = match all {
                true => file.list_versions(&name, &prefix, None, after, PAGE)?,
                false => file.list_objects(&name, &prefix, None, after, PAGE)?,
            };
            let Ok(mut listing) = listing else {
                return Ok(None);
            };
            let mut rows = Vec::new();
            for object in &mut listing.objects {
                if !all && (!object.latest || object.info.delete_marker) {
                    continue;
                }
                // A listing gives no attributes: the checksum is read when
                // asked for.
                if row.checksums && !object.info.delete_marker {
                    object.info.attrs.checksum = file
                        .version_attrs(&name, &object.key, object.info.version)?
                        .checksum;
                }
                let lock = match row.locks {
                    true => file.object_lock(&name, &object.key, object.info.version)?,
                    false => ObjectLock::default(),
                };
                let status = match row.statuses {
                    true => file.replication_status(&name, &object.key, object.info.version)?,
                    false => None,
                };
                rows.push(row.values(object, &lock, status, started));
            }
            let next = listing.last.map(|(key, _)| (key, listing.last_version));
            Ok(Some((rows, next, listing.truncated)))
        })
        .await?;
        let Some((rows, next, truncated)) = page else {
            return Ok(());
        };
        list = on_list(state, list, move |storage, mut list| {
            list.add(storage, &rows)?;
            Ok(list)
        })
        .await?;
        if !truncated || next.is_none() {
            break;
        }
        after = next;
    }
    let data = on_list(state, list, |storage, list| list.finish(storage)).await?;
    let due = Due {
        bucket,
        document,
        config,
        owner,
        created_ms,
        versioning,
    };
    let files = report_files(state, &due, started, data).await?;
    let delivery = object(state, move |file| {
        deliver_in(file, &due, started, &files, now_ms())
    })
    .await?;
    if let Delivery::Refused(problem) = delivery {
        // Made again when next due.
        eprintln!("inventory reports: no report for {problem}");
    }
    Ok(())
}

/// A stored body of `bytes`.
async fn small_body(state: &S3State, bytes: Vec<u8>) -> Result<SealedBody, String> {
    let storage = Arc::clone(&state.storage);
    tokio::task::spawn_blocking(move || {
        let now = now_ms();
        let mut writer = BodyWriter::begin(&storage, now)?;
        writer.write(&storage, &bytes, now)?;
        writer.finish(&storage, now)
    })
    .await
    .map_err(|err| err.to_string())?
    .map_err(|err| err.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A report's files: its key, body and content type each.
type Files = Vec<(String, SealedBody, &'static str)>;

/// The report's files for a run started at `started`: the list's (`data`,
/// each with its size), its manifest, checksum and Hive symlink, keyed as S3
/// keys them under the destination's prefix.
async fn report_files(
    state: &S3State,
    due: &Due,
    started: u64,
    data: Vec<(SealedBody, u64)>,
) -> Result<Files, String> {
    let (bucket, config) = (&due.bucket, &due.config);
    let base = match config
        .destination_prefix
        .as_deref()
        .unwrap_or("")
        .trim_end_matches('/')
    {
        "" => format!("{bucket}/{}", config.id),
        prefix => format!("{prefix}/{bucket}/{}", config.id),
    };
    let name = hex(&ring::digest::digest(
        &ring::digest::SHA256,
        format!("{bucket}/{}/{started}", config.id).as_bytes(),
    )
    .as_ref()[..16]);
    let (extension, format, schema, content_type) = match config.format {
        InventoryFormat::Csv => (
            "csv.gz",
            "CSV",
            csv_schema(&columns(config)),
            "application/x-gzip",
        ),
        InventoryFormat::Parquet => (
            "parquet",
            "Parquet",
            parquet_schema(&columns(config)),
            "application/octet-stream",
        ),
    };
    let data: Vec<(String, SealedBody, u64)> = data
        .into_iter()
        .enumerate()
        .map(|(index, (body, size))| {
            (
                format!("{base}/data/{name}-{index}.{extension}"),
                body,
                size,
            )
        })
        .collect();
    let listed: Vec<serde_json::Value> = data
        .iter()
        .map(|(key, body, size)| {
            serde_json::json!({"key": key, "size": size, "MD5checksum": hex(&body.md5)})
        })
        .collect();
    let stamp = iso8601(started);
    let (date, hour, minute) = (&stamp[..10], &stamp[11..13], &stamp[14..16]);
    let manifest = serde_json::json!({
        "sourceBucket": bucket,
        "destinationBucket": format!("arn:aws:s3:::{}", config.destination),
        "version": "2016-11-30",
        "creationTimestamp": started.to_string(),
        "fileFormat": format,
        "fileSchema": schema,
        "files": listed,
    })
    .to_string();
    let checksum = {
        use md5::Digest as _;
        hex(&md5::Md5::digest(manifest.as_bytes()))
    };
    let symlink: String = data
        .iter()
        .map(|(key, _, _)| format!("s3://{}/{key}\n", config.destination))
        .collect();
    let mut files: Files = data
        .into_iter()
        .map(|(key, body, _)| (key, body, content_type))
        .collect();
    files.extend([
        (
            format!("{base}/{date}T{hour}-{minute}Z/manifest.json"),
            small_body(state, manifest.into_bytes()).await?,
            "application/json",
        ),
        (
            format!("{base}/{date}T{hour}-{minute}Z/manifest.checksum"),
            small_body(state, checksum.into_bytes()).await?,
            "text/plain",
        ),
        (
            format!("{base}/hive/dt={date}-{hour}-{minute}/symlink.txt"),
            small_body(state, symlink.into_bytes()).await?,
            "text/plain",
        ),
    ]);
    Ok(files)
}

/// What became of a run's report.
#[derive(Debug, PartialEq, Eq)]
enum Delivery {
    /// Its files were written and the run recorded.
    Written,
    /// The source bucket or the configuration changed meanwhile: nothing
    /// was written or recorded (a changed configuration runs anew).
    Abandoned,
    /// The destination is gone or no longer the source owner's, or a file
    /// cannot be written there: the run is recorded without its files (made
    /// again when next due), and why.
    Refused(String),
}

/// Writes a report's `files` and records the run started at `started`, in
/// one transaction, once the source bucket and the configuration are
/// checked to be the ones listed and the destination the source owner's.
fn deliver_in(
    file: &mut ObjFile<'_>,
    due: &Due,
    started: u64,
    files: &Files,
    now: u64,
) -> Result<Delivery, StorageError> {
    let (bucket, id) = (&due.bucket, &due.config.id);
    if file
        .bucket(bucket)?
        .is_none_or(|info| info.created_ms != due.created_ms)
        || file.bucket_config(bucket, BUCKET_INVENTORY, id)?.as_ref() != Some(&due.document)
    {
        return Ok(Delivery::Abandoned);
    }
    let destination = &due.config.destination;
    let refused = match file.bucket(destination)? {
        None => Some("its destination bucket does not exist"),
        Some(target) if target.owner != due.owner => {
            Some("its destination bucket is not the source bucket owner's")
        }
        Some(_) => None,
    };
    let (bucket_lock, default) = crate::object_lock::bucket_state(file, destination)?;
    let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
        .ok()
        .flatten();
    file.set_origin(Origin {
        operation: "PutObject".to_string(),
        request_id: format!("inventory-{started}"),
        account: Some(due.owner.clone()),
        ..Origin::default()
    });
    let problem = match refused {
        Some(problem) => problem,
        None => match file.transaction(|tx| {
            for (key, body, content_type) in files {
                let attrs = ObjectAttrs {
                    content_type: Some(content_type.to_string()),
                    ..ObjectAttrs::default()
                };
                match tx.put_object(
                    destination,
                    key,
                    body,
                    &attrs,
                    StorageClass::Standard,
                    now,
                    DROP_GRACE_MS,
                )? {
                    Ok(info) => {
                        if let Some(lock) = &lock {
                            tx.set_lock(destination, key, info.version, lock)?;
                        }
                    }
                    // The files are written all or none: the transaction
                    // rolls back.
                    Err(_) => return Err(StorageError::InvalidConfig(UNWRITTEN.to_string())),
                }
            }
            tx.inventory_ran(bucket, id, started)
        }) {
            Ok(_) => return Ok(Delivery::Written),
            Err(StorageError::InvalidConfig(message)) if message == UNWRITTEN => {
                "its files could not be written"
            }
            Err(err) => return Err(err),
        },
    };
    file.transaction(|tx| tx.inventory_ran(bucket, id, started))?;
    Ok(Delivery::Refused(format!("{bucket}/{id}: {problem}")))
}

/// What a delivery's transaction fails with when a file cannot be written:
/// it rolls back, and the run is recorded without its files.
const UNWRITTEN: &str = "an inventory file cannot be written";

/// A CSV list's schema (its manifest's fileSchema).
fn csv_schema(columns: &[(&'static str, &'static str, Kind)]) -> String {
    columns
        .iter()
        .map(|(name, _, _)| *name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// A Parquet list's schema, as S3 writes it in the manifest (and as the
/// file holds it).
fn parquet_schema(columns: &[(&'static str, &'static str, Kind)]) -> String {
    let fields: Vec<String> = columns
        .iter()
        .map(|(name, field, kind)| {
            let repetition = if required(name) {
                "required"
            } else {
                "optional"
            };
            let ty = match kind {
                Kind::Text => "binary",
                Kind::Flag => "boolean",
                Kind::Count | Kind::Millis => "int64",
                Kind::Small => "int32",
            };
            let annotation = match kind {
                Kind::Text => " (UTF8)",
                Kind::Millis => " (TIMESTAMP_MILLIS)",
                _ => "",
            };
            format!("{repetition} {ty} {field}{annotation}")
        })
        .collect();
    format!("message s3.inventory {{ {};}}", fields.join("; "))
}

/// A column's value in one row.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    Null,
    Text(String),
    Flag(bool),
    Count(u64),
    Millis(u64),
}

/// A CSV list's line: every value quoted, the key (the second) URL-encoded.
fn csv_line(values: &[Value]) -> String {
    let mut line = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let text = match value {
                Value::Null => String::new(),
                Value::Text(text) if index == 1 => {
                    percent_encoding::utf8_percent_encode(text, REPORT_KEY).to_string()
                }
                Value::Text(text) => text.clone(),
                Value::Flag(on) => if *on { "True" } else { "False" }.to_string(),
                Value::Count(count) => count.to_string(),
                Value::Millis(ms) => iso8601(*ms),
            };
            // No value holds a quote (a key is URL-encoded).
            format!("\"{text}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    line.push('\n');
    line
}

/// How a configuration's rows are made.
struct Row {
    bucket: String,
    versioned: bool,
    columns: Vec<&'static str>,
    /// Whether any Object Lock column is asked for (each row then reads its
    /// version's lock).
    locks: bool,
    /// Whether the replication status is asked for (each row then reads it).
    statuses: bool,
    /// Whether the checksum algorithm is asked for (each row then reads its
    /// version's attributes).
    checksums: bool,
    encrypted: bool,
    /// The owner's canonical id, and its full control as an ACL document
    /// (base64), as S3 writes them.
    owner_id: String,
    acl: String,
}

impl Row {
    fn new(
        config: &InventoryConfig,
        bucket: &str,
        owner: &str,
        versioning: Versioning,
        encrypted: bool,
    ) -> Row {
        let columns: Vec<&'static str> = columns(config)
            .into_iter()
            .map(|(name, _, _)| name)
            .collect();
        let owner_id = truthdb_sigv4::sha256_hex(owner.as_bytes());
        // In S3's order of its members.
        let acl = format!(
            "{{\"version\":\"2022-11-10\",\"status\":\"AVAILABLE\",\"grants\":[{{\"canonicalId\":\"{owner_id}\",\"type\":\"CanonicalUser\",\"permission\":\"FULL_CONTROL\"}}]}}"
        );
        Row {
            bucket: bucket.to_string(),
            versioned: versioning != Versioning::Unversioned,
            locks: columns
                .iter()
                .any(|column| column.starts_with("ObjectLock")),
            statuses: columns.contains(&"ReplicationStatus"),
            checksums: columns.contains(&"ChecksumAlgorithm"),
            columns,
            encrypted,
            owner_id,
            acl: base64::engine::general_purpose::STANDARD.encode(acl),
        }
    }

    /// `object`'s values, a column each, at `now_ms` (an event hold's
    /// retain-until date moves with it).
    fn values(
        &self,
        object: &ListedObject,
        lock: &ObjectLock,
        replication: Option<truthdb_object::ReplicationStatus>,
        now_ms: u64,
    ) -> Vec<Value> {
        let info = &object.info;
        let marker = info.delete_marker;
        let text = |text: &str| Value::Text(text.to_string());
        let retention = lock.retention;
        let event = retention.and_then(|retention| retention.event);
        self.columns
            .iter()
            .map(|column| match *column {
                "Bucket" => text(&self.bucket),
                "Key" => text(&object.key),
                // A null version has no id (S3 writes it empty).
                "VersionId" if self.versioned && !info.null_version => {
                    Value::Text(info.version.to_string())
                }
                "IsLatest" => Value::Flag(object.latest),
                "IsDeleteMarker" => Value::Flag(marker),
                "Size" if !marker => Value::Count(info.size),
                "LastModifiedDate" => Value::Millis(info.mtime_ms),
                "ETag" if !marker => text(info.etag().trim_matches('"')),
                "StorageClass" if !marker => text(info.storage_class.as_str()),
                "IsMultipartUploaded" if !marker => Value::Flag(info.parts > 0),
                "EncryptionStatus" if !marker => {
                    text(match (info.attrs.customer_key.is_some(), self.encrypted) {
                        (true, _) => "SSE-C",
                        (false, true) => "SSE-S3",
                        (false, false) => "NOT-SSE",
                    })
                }
                "ObjectLockRetainUntilDate" => retention.map_or(Value::Null, |retention| {
                    Value::Millis(retention.effective_until(now_ms))
                }),
                "ObjectLockMode" => match retention.map(|retention| retention.mode) {
                    Some(RetentionMode::Governance) => text("GOVERNANCE"),
                    Some(RetentionMode::Compliance) => text("COMPLIANCE"),
                    None => Value::Null,
                },
                "ObjectLockLegalHoldStatus" => text(if lock.legal_hold == Some(true) {
                    "ON"
                } else {
                    "OFF"
                }),
                "ObjectLockEventHoldStatus" => event.map_or(Value::Null, |event| {
                    text(if event.on { "ON" } else { "OFF" })
                }),
                "ObjectLockEventHoldDuration" => {
                    event.map_or(Value::Null, |event| Value::Count(u64::from(event.count)))
                }
                "ObjectLockEventHoldDurationUnit" => event.map_or(Value::Null, |event| {
                    text(if event.years { "YEARS" } else { "DAYS" })
                }),
                "ObjectAccessControlList" if !marker => text(&self.acl),
                "ChecksumAlgorithm" => info
                    .attrs
                    .checksum
                    .as_ref()
                    .map_or(Value::Null, |checksum| text(&checksum.algorithm)),
                "ObjectOwner" => text(&self.owner_id),
                "ReplicationStatus" => {
                    replication.map_or(Value::Null, |status| text(status.as_str()))
                }
                // Nothing kept fills the others (and a delete marker has no
                // size, ETag, class or encryption).
                _ => Value::Null,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use truthdb_core::storage::StorageOptions;
    use truthdb_object::VersionSel;

    fn create(path: &std::path::Path) -> Storage {
        const RING: u64 = 8 * 1024 * 1024;
        Storage::create_with_wal_bounds(
            path.to_path_buf(),
            StorageOptions {
                size_bytes: 64 << 20,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
            RING,
            RING,
            truthdb_engine::database_engines,
        )
        .expect("create")
    }

    /// A report's files: `keys`, each with a body of its own.
    fn files(storage: &Storage, keys: &[String]) -> Files {
        keys.iter()
            .map(|key| {
                let mut writer = BodyWriter::begin(storage, 1).unwrap();
                writer.write(storage, key.as_bytes(), 1).unwrap();
                (
                    key.clone(),
                    writer.finish(storage, 1).unwrap(),
                    "text/plain",
                )
            })
            .collect()
    }

    #[test]
    fn a_report_is_delivered_only_while_its_source_and_destination_hold() {
        let path = std::env::temp_dir().join(format!(
            "truthdb-s3-inventory-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let storage = create(&path);
        let (me, them) = ("111111111111", "222222222222");
        let config = InventoryConfig {
            id: "daily".into(),
            enabled: true,
            prefix: None,
            destination: "lists".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Csv,
            sse_s3: false,
            all_versions: false,
            optional_fields: Vec::new(),
            frequency: InventoryFrequency::Daily,
        };
        let document = config.encode();
        {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            file.transaction(|tx| {
                tx.create_bucket("source", me, 5)?.unwrap();
                tx.create_bucket("lists", me, 5)?.unwrap();
                tx.put_bucket_config("source", BUCKET_INVENTORY, "daily", &document)?;
                tx.track_inventory("source", "daily")
            })
            .unwrap();
        }
        let due = Due {
            bucket: "source".into(),
            document: document.clone(),
            config: config.clone(),
            owner: me.into(),
            created_ms: 5,
            versioning: Versioning::Unversioned,
        };
        let deliver = |started: u64, keys: &[String]| {
            let files = files(&storage, keys);
            deliver_in(
                &mut ObjFile::new(&mut storage.lock()),
                &due,
                started,
                &files,
                7,
            )
            .unwrap()
        };
        let last = || ObjFile::new(&mut storage.lock()).inventory_runs().unwrap()[0].2;
        let exists = |key: &str| {
            ObjFile::new(&mut storage.lock())
                .head_object("lists", key, VersionSel::Latest)
                .unwrap()
                .is_ok()
        };
        let key = |name: &str| name.to_string();

        assert_eq!(deliver(100, &[key("a"), key("b")]), Delivery::Written);
        assert!(exists("a") && exists("b"));
        assert_eq!(last(), 100);

        // A file that cannot be written: none is, and the run is recorded.
        let refused = deliver(150, &[key("c"), "k".repeat(2000)]);
        assert!(matches!(&refused, Delivery::Refused(why) if why.contains("could not be written")));
        assert!(!exists("c"));
        assert_eq!(last(), 150);

        // The destination deleted, and taken by another account.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                for key in ["a", "b"] {
                    tx.delete_object("lists", key, None, false, 6, 0)?.unwrap();
                }
                tx.delete_bucket("lists")?.unwrap();
                tx.create_bucket("lists", them, 6)?.unwrap();
                Ok(())
            })
            .unwrap();
        let refused = deliver(200, &[key("d")]);
        assert!(
            matches!(&refused, Delivery::Refused(why) if why.contains("not the source bucket owner"))
        );
        assert!(!exists("d"));
        assert_eq!(last(), 200);

        // The configuration changed: abandoned, nothing recorded.
        let mut changed = config.clone();
        changed.prefix = Some("docs/".into());
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_bucket_config("source", BUCKET_INVENTORY, "daily", &changed.encode())
            })
            .unwrap();
        assert_eq!(deliver(300, &[key("e")]), Delivery::Abandoned);
        assert_eq!(last(), 200);

        // The source bucket deleted and created again with the same
        // configuration: abandoned too.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.delete_bucket("source")?.unwrap();
                tx.create_bucket("source", me, 8)?.unwrap();
                tx.put_bucket_config("source", BUCKET_INVENTORY, "daily", &document)?;
                tx.track_inventory("source", "daily")
            })
            .unwrap();
        assert_eq!(deliver(400, &[key("f")]), Delivery::Abandoned);
        assert_eq!(last(), 0);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    const SUNDAY: u64 = 20_415 * DAY_MS; // 2025-11-23, a Sunday

    #[test]
    fn reports_are_due_daily_or_on_sundays_and_the_first_at_once() {
        use InventoryFrequency::{Daily, Weekly};
        assert!(due(Daily, 0, 5));
        assert!(due(Weekly, 0, 5));
        let saturday = SUNDAY - DAY_MS;
        // Daily: once each UTC day.
        assert!(!due(Daily, saturday + 1, saturday + DAY_MS - 1));
        assert!(due(Daily, saturday + DAY_MS - 1, SUNDAY));
        // Weekly: a first report on Saturday is followed by one on Sunday,
        // then none until the next Sunday.
        assert!(!due(Weekly, saturday + 1, saturday + DAY_MS - 1));
        assert!(due(Weekly, saturday + 1, SUNDAY));
        assert!(!due(Weekly, SUNDAY + 5, SUNDAY + 6 * DAY_MS + DAY_MS - 1));
        assert!(due(Weekly, SUNDAY + 5, SUNDAY + 7 * DAY_MS));
        assert!(due(Weekly, SUNDAY - 8 * DAY_MS, SUNDAY + 3 * DAY_MS));
    }

    #[test]
    fn a_line_holds_each_column_as_s3_writes_it() {
        use truthdb_object::{CustomerKey, EventHold, ObjectInfo, Retention};
        let config = InventoryConfig {
            id: "r".into(),
            enabled: true,
            prefix: None,
            destination: "d".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Csv,
            sse_s3: false,
            all_versions: true,
            optional_fields: COLUMNS[5..]
                .iter()
                .map(|(field, _, _)| field.to_string())
                .collect(),
            frequency: InventoryFrequency::Daily,
        };
        let object = |delete_marker: bool, null_version: bool| ListedObject {
            key: "a b/\"c\".txt".into(),
            info: ObjectInfo {
                version: 7,
                null_version,
                delete_marker,
                body: 1,
                size: 12,
                md5: [0xab; 16],
                parts: 3,
                mtime_ms: 1_000,
                storage_class: StorageClass::StandardIa,
                attrs: ObjectAttrs::default(),
            },
            latest: !null_version,
        };
        let lock = ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 2_000,
                event: Some(EventHold {
                    on: true,
                    years: false,
                    count: 2,
                }),
            }),
            legal_hold: Some(true),
        };
        let row = Row::new(&config, "src", "111111111111", Versioning::Enabled, true);
        let owner = truthdb_sigv4::sha256_hex(b"111111111111");
        let acl = base64::engine::general_purpose::STANDARD.encode(format!(
            "{{\"version\":\"2022-11-10\",\"status\":\"AVAILABLE\",\"grants\":[{{\"canonicalId\":\"{owner}\",\"type\":\"CanonicalUser\",\"permission\":\"FULL_CONTROL\"}}]}}"
        ));
        let fields = |line: String| -> Vec<String> {
            line.trim_end()
                .split(',')
                .map(|field| field.trim_matches('"').to_string())
                .collect()
        };
        // An event hold on: retain-until moves with the report's time.
        let until = iso8601(10 * DAY_MS + 2 * DAY_MS);
        assert_eq!(
            fields(csv_line(&row.values(
                &object(false, false),
                &lock,
                None,
                10 * DAY_MS
            ))),
            [
                "src",
                "a%20b/%22c%22.txt",
                "7",
                "True",
                "False",
                "12",
                "1970-01-01T00:00:01.000Z",
                &format!("{}-3", "ab".repeat(16)),
                "STANDARD_IA",
                "True",
                "",
                "SSE-S3",
                &until,
                "COMPLIANCE",
                "ON",
                "ON",
                "2",
                "DAYS",
                "",
                "",
                "",
                &acl,
                &owner,
                "",
                "",
            ]
            .map(str::to_string)
        );
        // A null version's id is empty; a delete marker has no size, ETag,
        // class, upload flag, encryption or ACL; no lock reads OFF.
        let mut keyed = object(true, true);
        keyed.info.attrs.customer_key = Some(CustomerKey {
            key_md5: String::new(),
            verifier: String::new(),
        });
        let none = ObjectLock::default();
        let line = fields(csv_line(&row.values(&keyed, &none, None, 0)));
        assert_eq!(
            &line[2..12],
            [
                "",
                "False",
                "True",
                "",
                "1970-01-01T00:00:01.000Z",
                "",
                "",
                "",
                "",
                ""
            ]
        );
        assert_eq!(&line[12..18], ["", "", "OFF", "", "", ""]);
        keyed.info.delete_marker = false;
        assert_eq!(
            fields(csv_line(&row.values(&keyed, &none, None, 0)))[11],
            "SSE-C"
        );
        let plain = Row::new(&config, "src", "111111111111", Versioning::Enabled, false);
        assert_eq!(
            fields(csv_line(&plain.values(
                &object(false, false),
                &none,
                None,
                0
            )))[11],
            "NOT-SSE"
        );
    }

    #[test]
    fn a_parquet_list_written_a_row_group_at_a_time_reads_back() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::record::Field;
        let config = InventoryConfig {
            id: "r".into(),
            enabled: true,
            prefix: None,
            destination: "d".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Parquet,
            sse_s3: false,
            all_versions: true,
            optional_fields: ["Size", "LastModifiedDate", "ObjectLockEventHoldDuration"]
                .map(str::to_string)
                .to_vec(),
            frequency: InventoryFrequency::Daily,
        };
        let columns = columns(&config);
        let Encoder::Parquet(mut writer, kinds) = List::encoder(config.format, &columns).unwrap()
        else {
            panic!("a Parquet encoder");
        };
        let row = |key: &str, marker: bool| {
            vec![
                Value::Text("src".into()),
                Value::Text(key.into()),
                Value::Text("7".into()),
                Value::Flag(true),
                Value::Flag(marker),
                if marker {
                    Value::Null
                } else {
                    Value::Count(12)
                },
                Value::Millis(1_000),
                Value::Null,
                Value::Null,
            ]
        };
        // Taken from under the writer after each row group, as a run does.
        let mut file = Vec::new();
        for rows in [
            vec![row("a b", false)],
            vec![row("c", true), row("d", false)],
        ] {
            write_row_group(&mut writer, &kinds, &rows).unwrap();
            writer.flush().unwrap();
            file.append(writer.inner_mut());
        }
        writer.finish().unwrap();
        writer.flush().unwrap();
        file.append(writer.inner_mut());
        let reader = SerializedFileReader::new(bytes::Bytes::from(file)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
        let rows: Vec<Vec<Field>> = reader
            .get_row_iter(None)
            .unwrap()
            .map(|row| {
                row.unwrap()
                    .get_column_iter()
                    .map(|(_, field)| field.clone())
                    .collect()
            })
            .collect();
        assert_eq!(rows.len(), 3);
        // The key is as it is (only a CSV list URL-encodes it).
        assert_eq!(rows[0][1], Field::Str("a b".into()));
        assert_eq!(rows[0][5], Field::Long(12));
        assert_eq!(rows[0][6], Field::TimestampMillis(1_000));
        assert_eq!(rows[1][4], Field::Bool(true));
        assert_eq!(rows[1][5], Field::Null);
        assert_eq!(rows[2][7], Field::Null);
        // The schema the file holds is the manifest's.
        let names: Vec<&str> = reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|column| column.name())
            .collect();
        assert_eq!(
            names,
            [
                "bucket",
                "key",
                "version_id",
                "is_latest",
                "is_delete_marker",
                "size",
                "last_modified_date",
                "object_lock_event_hold_duration",
                "object_lock_event_hold_duration_unit"
            ]
        );
    }

    #[test]
    fn a_long_parquet_list_goes_on_in_another_file_every_hundred_row_groups() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let path = std::env::temp_dir().join(format!(
            "truthdb-s3-inventory-files-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let storage = create(&path);
        let columns = vec![COLUMNS[0], COLUMNS[1]];
        let row = |n: usize| vec![Value::Text("src".into()), Value::Text(format!("k{n}"))];
        let rows_per_file = |pages: usize| -> Vec<i64> {
            let mut list =
                List::begin(&storage, InventoryFormat::Parquet, columns.clone()).unwrap();
            for page in 0..pages {
                list.add(&storage, &[row(page)]).unwrap();
            }
            list.finish(&storage)
                .unwrap()
                .into_iter()
                .map(|(sealed, size)| {
                    let mut lock = storage.lock();
                    let mut file = ObjFile::new(&mut lock);
                    let body = file.transaction(|tx| sealed.seal_in(tx)).unwrap().body;
                    let mut bytes = vec![0; size as usize];
                    file.read_body(body, 0, &mut bytes).unwrap();
                    SerializedFileReader::new(bytes::Bytes::from(bytes))
                        .unwrap()
                        .metadata()
                        .file_metadata()
                        .num_rows()
                })
                .collect()
        };
        assert_eq!(rows_per_file(250), [100, 100, 50]);
        // A list that ends as a file fills has no empty one after it; an
        // empty list is one empty file.
        assert_eq!(rows_per_file(200), [100, 100]);
        assert_eq!(rows_per_file(0), [0]);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_reports_schema_lists_its_columns_in_s3s_order() {
        let config = InventoryConfig {
            id: "r".into(),
            enabled: true,
            prefix: None,
            destination: "d".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Csv,
            sse_s3: false,
            all_versions: true,
            optional_fields: vec![
                "ObjectOwner".into(),
                "ObjectLockEventHoldDuration".into(),
                "Size".into(),
            ],
            frequency: InventoryFrequency::Daily,
        };
        assert_eq!(
            csv_schema(&columns(&config)),
            "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, \
             ObjectLockEventHoldDuration, ObjectLockEventHoldDurationUnit, ObjectOwner"
        );
        assert_eq!(
            parquet_schema(&columns(&config)),
            "message s3.inventory { required binary bucket (UTF8); required binary key (UTF8); \
             optional binary version_id (UTF8); optional boolean is_latest; \
             optional boolean is_delete_marker; optional int64 size; \
             optional int32 object_lock_event_hold_duration; \
             optional binary object_lock_event_hold_duration_unit (UTF8); \
             optional binary object_owner (UTF8);}"
        );
    }
}
