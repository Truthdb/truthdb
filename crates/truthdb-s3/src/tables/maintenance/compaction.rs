//! Table compaction ("combines multiple smaller objects into fewer, larger
//! objects"), the bin-pack strategy: the current snapshot's Parquet data
//! files outside the target size's range are rewritten, a partition (and
//! file schema) at a time, into files of about the target size, committed
//! as a `replace` snapshot ("written as the most recent snapshot of your
//! table").
//!
//! Which files and how many outputs follow Iceberg's bin-pack rewrite
//! (`BinPackRewriteFilePlanner`): a file is a candidate when smaller than
//! 75% of the target or larger than 180% of it; a partition's candidates
//! are rewritten when there are at least 5 of them, or more than one
//! holding more than the target, or more than 180% of it in all.
//!
//! Not built yet: applying row-level deletes (a data file a delete file
//! may apply to is left as it is), the sort and z-order strategies, and
//! Avro or ORC data files (left as they are). A file of an older partition
//! spec is rewritten within its own spec and partition, where Iceberg
//! rewrites such files into the current spec.
//!
//! Data files are written by their writers, so they are read as untrusted
//! (see `truthdb_engine::engine::open_arrow_parquet`); a file that cannot
//! be read within those limits is left as it is. The rewrite commits only
//! when its outputs hold exactly the rows its inputs' manifests count.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use apache_avro::types::Value as Avro;
use serde_json::{Value, json};
use truthdb_core::storage::Storage;
use truthdb_object::{BodyWriter, SealedBody, TableInfo, VersionSel};

use super::{DROP_GRACE_MS, Place, STALE, place, property, stored};
use crate::iceberg_manifests::{
    AvroFile, Set, avro_file, build, field_schema, get, iceberg_map, ids, write_avro,
};
use crate::server::S3State;

const MIB: u64 = 1 << 20;
/// Iceberg's bin-pack defaults: the size range kept, the fewest files a
/// partition is rewritten for, and what a split takes over its share.
const MIN_RATIO: f64 = 0.75;
const MAX_RATIO: f64 = 1.80;
const MIN_INPUT_FILES: usize = 5;
const SPLIT_OVERHEAD: u64 = 5 * 1024;
/// "S3 Tables applies the parquet's row-group-default size of 128 MB."
const ROW_GROUP_BYTES: usize = 128 << 20;
/// What an output file's bytes are buffered in before they are written.
const SINK_BUFFER: usize = 8 << 20;
/// How often a body being written is marked touched (one untouched for an
/// hour is taken for abandoned).
const TOUCH_MS: u64 = 5 * 60 * 1000;

const SORT_NOT_BUILT: &str =
    "Sort compaction is not supported yet: the auto strategy sorts a table that has a sort order.";

/// A manifest of the current snapshot: its manifest list entry, the file,
/// and the list entry's fields read.
struct Manifest {
    entry: Avro,
    file: AvroFile,
    spec: i32,
    sequence: i64,
    added_snapshot: i64,
}

/// A live data file entry of a manifest.
struct DataFile {
    manifest: usize,
    entry: usize,
    key: String,
    partition: Avro,
    records: i64,
    size: i64,
}

/// A rewritten group: its files (indexes into the data files) and its
/// outputs (indexes into the files written).
type Rewrite = (Vec<usize>, Vec<usize>);

/// A rewritten file: its key, version, size and Parquet metadata.
struct Output {
    key: String,
    version: u64,
    size: u64,
    metadata: parquet::file::metadata::ParquetMetaData,
}

/// Compacts `table` with target size `target_mb` and strategy `strategy`,
/// or why not.
pub(super) async fn compact(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    (target_mb, strategy): (i64, &str),
    now: u64,
) -> Result<(), String> {
    let base = crate::tables::iceberg::read_current(state, table)
        .await
        .map_err(|err| err.message)?;
    if base["format-version"].as_i64() != Some(2) {
        return Err("Compaction supports Iceberg format version 2 tables only.".to_string());
    }
    if strategy == "auto" && sorted(&base) {
        return Err(SORT_NOT_BUILT.to_string());
    }
    let Some(current) = base["current-snapshot-id"].as_i64().filter(|id| *id != -1) else {
        return Ok(());
    };
    if base["refs"]["main"]["snapshot-id"].as_i64() != Some(current) {
        return Err(
            "Compaction needs the table's main branch at its current snapshot.".to_string(),
        );
    }
    let snapshot = base["snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(current))
        .cloned()
        .ok_or_else(|| "The table's current snapshot is missing.".to_string())?;
    let warehouse = truthdb_object::warehouse_bucket(&table.id);
    let own = |location: &str, what: &str| match place(location, &warehouse) {
        Place::Own(keys) if keys.len() == 1 => Ok(keys.into_iter().next().expect("one")),
        _ => Err(format!(
            "The {what} {location} is outside the table's storage."
        )),
    };
    let list_key = own(
        snapshot["manifest-list"].as_str().unwrap_or_default(),
        "manifest list",
    )?;
    let list = avro_file(
        &stored(state, &warehouse, &list_key).await?,
        "The manifest list",
    )?;
    let list_ids = ids(&list.schema, &list.schema)?;
    let mut manifests = Vec::new();
    for entry in &list.records {
        let path = text(get(entry, &list_ids, 500)).unwrap_or_default();
        let key = own(path, "manifest")?;
        let file = avro_file(&stored(state, &warehouse, &key).await?, "A manifest")?;
        manifests.push(Manifest {
            entry: entry.clone(),
            file,
            spec: int(get(entry, &list_ids, 502)).unwrap_or_default() as i32,
            sequence: int(get(entry, &list_ids, 515)).unwrap_or_default(),
            added_snapshot: int(get(entry, &list_ids, 503)).unwrap_or_default(),
        });
    }
    let unpartitioned: HashSet<i64> = base["partition-specs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|spec| spec["fields"].as_array().is_none_or(Vec::is_empty))
        .filter_map(|spec| spec["spec-id"].as_i64())
        .collect();
    // The live data files, and the partitions live delete files are in.
    let mut data = Vec::new();
    let mut deleted_in: HashSet<(i32, String)> = HashSet::new();
    let mut global_deletes = false;
    for (at, manifest) in manifests.iter().enumerate() {
        let schema = &manifest.file.schema;
        let entry_ids = ids(schema, schema)?;
        let file_schema = field_schema(schema, schema, 2)
            .ok_or_else(|| "A manifest entry has no data_file.".to_string())?;
        let file_ids = ids(schema, &file_schema)?;
        for (index, record) in manifest.file.records.iter().enumerate() {
            if int(get(record, &entry_ids, 0)) == Some(2) {
                continue;
            }
            let file = get(record, &entry_ids, 2)
                .ok_or_else(|| "A manifest entry has no data_file.".to_string())?;
            let content = int(get(file, &file_ids, 134)).unwrap_or_default();
            let partition = get(file, &file_ids, 102).cloned().unwrap_or(Avro::Null);
            if content != 0 {
                global_deletes |= unpartitioned.contains(&i64::from(manifest.spec));
                deleted_in.insert((manifest.spec, format!("{partition:?}")));
                continue;
            }
            let format = text(get(file, &file_ids, 101)).unwrap_or_default();
            let path = text(get(file, &file_ids, 100)).unwrap_or_default();
            let Place::Own(keys) = place(path, &warehouse) else {
                continue;
            };
            if !format.eq_ignore_ascii_case("parquet") || keys.len() != 1 {
                continue;
            }
            data.push(DataFile {
                manifest: at,
                entry: index,
                key: keys.into_iter().next().expect("one"),
                partition,
                records: int(get(file, &file_ids, 103)).unwrap_or(-1),
                size: int(get(file, &file_ids, 104)).unwrap_or(-1),
            });
        }
    }
    let target = (target_mb.max(1) as u64) * MIB;
    let min = (target as f64 * MIN_RATIO) as u64;
    let max = (target as f64 * MAX_RATIO) as u64;
    let write_max = target + (max - target) / 2;
    // The candidates by partition, then (in a partition that is to be
    // rewritten) by file schema, read from each one's footer: files of one
    // schema are written into one.
    let mut partitions: BTreeMap<(i32, String), Vec<usize>> = BTreeMap::new();
    for (at, file) in data.iter().enumerate() {
        let spec = manifests[file.manifest].spec;
        let partition = format!("{:?}", file.partition);
        let size = file.size.max(0) as u64;
        if global_deletes
            || deleted_in.contains(&(spec, partition.clone()))
            || file.records < 0
            || (min..=max).contains(&size)
        {
            continue;
        }
        partitions.entry((spec, partition)).or_default().push(at);
    }
    let rewritten = |files: &[usize]| {
        let size = sum_sizes(files, &data);
        let n = files.len();
        // (Iceberg's "more than one" is implied by at least five.)
        n >= MIN_INPUT_FILES || (n > 1 && size > target) || size > max
    };
    let mut groups: BTreeMap<(i32, String, String), Vec<usize>> = BTreeMap::new();
    for ((spec, partition), files) in partitions {
        if !rewritten(&files) {
            continue;
        }
        for at in files {
            let Some(schema) = file_schema(state, &warehouse, &data[at].key).await else {
                continue;
            };
            groups
                .entry((spec, partition.clone(), schema))
                .or_default()
                .push(at);
        }
    }
    let codec = codec(&base)?;
    let snapshot_id = new_snapshot_id(&base);
    let mut written: Vec<Output> = Vec::new();
    // Per rewritten group: its files, and its outputs (indexes into
    // `written`).
    let mut rewrites: Vec<Rewrite> = Vec::new();
    // Why the first group that could not be rewritten could not: the job's
    // failure if no group could be (a group that fails is left as it is,
    // and stops no other).
    let mut failure: Option<String> = None;
    for files in groups.into_values() {
        if !rewritten(&files) {
            continue;
        }
        let size = sum_sizes(&files, &data);
        let split = split_size(size, (target, min, write_max));
        let keys: Vec<String> = files.iter().map(|&at| data[at].key.clone()).collect();
        let outputs = match rewrite(state, &warehouse, &keys, (split, write_max), &codec, now).await
        {
            Ok(outputs) => outputs,
            Err(err) => {
                failure.get_or_insert(err);
                continue;
            }
        };
        // Every row the inputs hold, and no other.
        let read: i64 = files.iter().map(|&at| data[at].records).sum();
        let wrote: i64 = outputs
            .iter()
            .map(|output| output.metadata.file_metadata().num_rows())
            .sum();
        let first = written.len();
        written.extend(outputs);
        if read != wrote {
            remove(state, &warehouse, &stored_keys(&written), now).await;
            return Err(format!(
                "Compaction read {wrote} rows from files whose manifests count {read}: nothing was committed."
            ));
        }
        rewrites.push((files, (first..written.len()).collect()));
    }
    if rewrites.is_empty() {
        return failure.map_or(Ok(()), Err);
    }
    let outcome = commit(
        state,
        (account, bucket),
        table,
        (&base, &snapshot, snapshot_id),
        (&list, &manifests, &data),
        (&rewrites, &written),
        now,
    )
    .await;
    if outcome.is_err() {
        remove(state, &warehouse, &stored_keys(&written), now).await;
    }
    outcome
}

/// The sizes of data files `files` together (a size a manifest gives as
/// negative counted as none).
fn sum_sizes(files: &[usize], data: &[DataFile]) -> u64 {
    files
        .iter()
        .map(|&at| data[at].size.max(0) as u64)
        .fold(0, u64::saturating_add)
}

/// The keys and versions of `written`.
fn stored_keys(written: &[Output]) -> Vec<(String, u64)> {
    written
        .iter()
        .map(|output| (output.key.clone(), output.version))
        .collect()
}

/// Whether the table's default sort order sorts by anything.
fn sorted(metadata: &Value) -> bool {
    let id = metadata["default-sort-order-id"].as_i64();
    metadata["sort-orders"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|order| order["order-id"].as_i64() == id)
        .and_then(|order| order["fields"].as_array())
        .is_some_and(|fields| !fields.is_empty())
}

fn text(value: Option<&Avro>) -> Option<&str> {
    match value? {
        Avro::String(text) => Some(text),
        _ => None,
    }
}

fn int(value: Option<&Avro>) -> Option<i64> {
    match value? {
        Avro::Int(value) => Some(i64::from(*value)),
        Avro::Long(value) => Some(*value),
        _ => None,
    }
}

/// A snapshot id not yet the table's: positive, as Iceberg's are.
fn new_snapshot_id(metadata: &Value) -> i64 {
    use ring::rand::SecureRandom as _;
    loop {
        let mut bytes = [0u8; 8];
        ring::rand::SystemRandom::new()
            .fill(&mut bytes)
            .expect("the system's random source");
        let id = (u64::from_le_bytes(bytes) >> 1) as i64;
        let taken = metadata["snapshots"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|snapshot| snapshot["snapshot-id"].as_i64() == Some(id));
        if id != 0 && !taken {
            return id;
        }
    }
}

/// The size of each output's share of a group of `size` bytes, as Iceberg
/// splits one: its expected output count's share, within the target and
/// the largest file written.
fn split_size(size: u64, (target, min, write_max): (u64, u64, u64)) -> u64 {
    let outputs = if size < target {
        1
    } else {
        let with_remainder = size.div_ceil(target);
        let without = size / target;
        if size % target > min {
            with_remainder
        } else if ((size / without) as f64) < (1.1 * target as f64).min(write_max as f64) {
            without
        } else {
            with_remainder
        }
    };
    let estimated = size / outputs + SPLIT_OVERHEAD;
    if estimated < target {
        target
    } else {
        estimated.min(write_max)
    }
}

/// The Parquet codec the table's properties ask its files be written with
/// (Iceberg's default zstd).
fn codec(metadata: &Value) -> Result<parquet::basic::Compression, String> {
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};
    let name = property(metadata, "write.parquet.compression-codec")
        .and_then(Value::as_str)
        .unwrap_or("zstd")
        .to_ascii_lowercase();
    let level = property(metadata, "write.parquet.compression-level")
        .and_then(Value::as_str)
        .map(|level| {
            level
                .parse::<i32>()
                .map_err(|_| "write.parquet.compression-level is not a number.".to_string())
        })
        .transpose()?;
    let invalid = |err: parquet::errors::ParquetError| err.to_string();
    Ok(match name.as_str() {
        "zstd" => Compression::ZSTD(match level {
            Some(level) => ZstdLevel::try_new(level).map_err(invalid)?,
            None => ZstdLevel::default(),
        }),
        "gzip" => Compression::GZIP(match level {
            Some(level) => GzipLevel::try_new(level as u32).map_err(invalid)?,
            None => GzipLevel::default(),
        }),
        "snappy" => Compression::SNAPPY,
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        other => {
            return Err(format!(
                "Compaction does not write Parquet files compressed with {other}."
            ));
        }
    })
}

/// The Parquet schema of data file `key` (as text, to group files of one
/// schema), or none when it cannot be read within the reader's limits or
/// a chunk of it is compressed with a codec compaction does not read.
async fn file_schema(state: &S3State, warehouse: &str, key: &str) -> Option<String> {
    use parquet::basic::Compression;
    let (storage, warehouse, key) = (
        Arc::clone(&state.storage),
        warehouse.to_string(),
        key.to_string(),
    );
    tokio::task::spawn_blocking(move || {
        let file = truthdb_engine::engine::open_arrow_parquet(&storage, &warehouse, &key).ok()?;
        let metadata = file.metadata();
        // "Compression types brotli and lz4 are not supported."
        let unsupported = metadata.row_groups().iter().any(|group| {
            group.columns().iter().any(|column| {
                matches!(
                    column.compression(),
                    Compression::BROTLI(_) | Compression::LZ4 | Compression::LZ4_RAW
                )
            })
        });
        (!unsupported).then(|| format!("{:?}", metadata.file_metadata().schema()))
    })
    .await
    .ok()
    .flatten()
}

/// A body being written, as the Parquet writer's output.
struct BodySink {
    storage: Arc<Storage>,
    writer: Option<BodyWriter>,
    buffer: Vec<u8>,
    size: u64,
    touched_ms: u64,
}

impl BodySink {
    fn begin(storage: &Arc<Storage>) -> Result<BodySink, String> {
        let now = crate::objects::now_ms();
        let writer = BodyWriter::begin(storage, now).map_err(|err| err.to_string())?;
        Ok(BodySink {
            storage: Arc::clone(storage),
            writer: Some(writer),
            buffer: Vec::with_capacity(SINK_BUFFER),
            size: 0,
            touched_ms: now,
        })
    }

    /// Writes what is buffered.
    fn drain(&mut self) -> std::io::Result<()> {
        let now = crate::objects::now_ms();
        let writer = self.writer.as_mut().expect("written until sealed");
        writer
            .write(&self.storage, &self.buffer, now)
            .map_err(std::io::Error::other)?;
        self.buffer.clear();
        if now.saturating_sub(self.touched_ms) >= TOUCH_MS {
            writer
                .touch(&self.storage, now)
                .map_err(std::io::Error::other)?;
            self.touched_ms = now;
        }
        Ok(())
    }

    /// The body being written, taken (this sink left writing nothing).
    fn take(&mut self) -> BodySink {
        BodySink {
            storage: Arc::clone(&self.storage),
            writer: self.writer.take(),
            buffer: std::mem::take(&mut self.buffer),
            size: self.size,
            touched_ms: self.touched_ms,
        }
    }

    fn seal(mut self) -> Result<(SealedBody, u64), String> {
        self.drain().map_err(|err| err.to_string())?;
        let writer = self.writer.take().expect("written until sealed");
        let sealed = writer
            .finish(&self.storage, crate::objects::now_ms())
            .map_err(|err| err.to_string())?;
        Ok((sealed, self.size))
    }
}

impl std::io::Write for BodySink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        self.size += bytes.len() as u64;
        if self.buffer.len() >= SINK_BUFFER {
            self.drain()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// An output file being written.
struct Writing {
    writer: parquet::arrow::ArrowWriter<BodySink>,
    /// The input bytes (row groups' compressed sizes) it holds so far.
    consumed: u64,
}

/// Rewrites the data files `keys` of the table's storage `warehouse` into
/// new files, each holding about `split` bytes of the inputs and at most
/// `write_max` written; each output stored as an object of the storage.
async fn rewrite(
    state: &S3State,
    warehouse: &str,
    keys: &[String],
    (split, write_max): (u64, u64),
    codec: &parquet::basic::Compression,
    now: u64,
) -> Result<Vec<Output>, String> {
    let (storage, from, keys, codec) = (
        Arc::clone(&state.storage),
        warehouse.to_string(),
        keys.to_vec(),
        *codec,
    );
    let files = tokio::task::spawn_blocking(move || {
        rewrite_files(&storage, &from, &keys, (split, write_max), codec)
    })
    .await
    .map_err(|_| "A data file could not be rewritten: its rewrite stopped.".to_string())??;
    let mut outputs = Vec::new();
    for (sealed, size, metadata) in files {
        if metadata.file_metadata().num_rows() == 0 {
            continue;
        }
        let key = format!("data/{}-compacted.parquet", crate::tables::uuid());
        let version = match put(state, warehouse, &key, sealed).await {
            Ok(version) => version,
            Err(err) => {
                remove(state, warehouse, &stored_keys(&outputs), now).await;
                return Err(err);
            }
        };
        outputs.push(Output {
            key,
            version,
            size,
            metadata,
        });
    }
    Ok(outputs)
}

type Written = (SealedBody, u64, parquet::file::metadata::ParquetMetaData);

fn rewrite_files(
    storage: &Arc<Storage>,
    warehouse: &str,
    keys: &[String],
    (split, write_max): (u64, u64),
    codec: parquet::basic::Compression,
) -> Result<Vec<Written>, String> {
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(codec)
        // Iceberg's default metrics mode, truncate(16): a long value's
        // bounds are a prefix and an incremented prefix.
        .set_statistics_truncate_length(Some(16))
        .build();
    let failed =
        |err: parquet::errors::ParquetError| format!("A compacted file cannot be written: {err}");
    let begin = |schema: arrow_schema::SchemaRef,
                 descr: &parquet::schema::types::SchemaDescriptor|
     -> Result<Writing, String> {
        let options = parquet::arrow::arrow_writer::ArrowWriterOptions::new()
            .with_properties(props.clone())
            .with_skip_arrow_metadata(true)
            .with_parquet_schema(descr.clone());
        let writer = parquet::arrow::ArrowWriter::try_new_with_options(
            BodySink::begin(storage)?,
            schema,
            options,
        )
        .map_err(failed)?;
        Ok(Writing {
            writer,
            consumed: 0,
        })
    };
    let finish = |writing: Writing| -> Result<Written, String> {
        let mut writer = writing.writer;
        let metadata = writer.finish().map_err(failed)?;
        // The footer written: the body taken from under the writer.
        let (sealed, size) = writer.inner_mut().take().seal()?;
        Ok((sealed, size, metadata))
    };
    let mut done = Vec::new();
    let mut writing: Option<Writing> = None;
    for key in keys {
        let file = truthdb_engine::engine::open_arrow_parquet(storage, warehouse, key)
            .map_err(|err| format!("The data file {key} cannot be read: {err}"))?;
        let descr = file.metadata().file_metadata().schema_descr_ptr();
        for group in 0..file.metadata().num_row_groups() {
            // The row group's input bytes, spread over its rows.
            let metadata = file.metadata().row_group(group);
            let (group_bytes, group_rows) = (
                metadata.compressed_size().max(0) as u64,
                metadata.num_rows().max(1) as u64,
            );
            for batch in file.batches(group)? {
                let batch =
                    batch.map_err(|err| format!("The data file {key} cannot be read: {err}"))?;
                let out = match writing.as_mut() {
                    Some(out) => out,
                    None => writing.insert(begin(file.schema(), &descr)?),
                };
                out.writer.write(&batch).map_err(failed)?;
                if out.writer.in_progress_size() >= ROW_GROUP_BYTES {
                    out.writer.flush().map_err(failed)?;
                }
                out.consumed = out.consumed.saturating_add(
                    group_bytes.saturating_mul(batch.num_rows() as u64) / group_rows,
                );
                // An output ends at its share of the inputs, or at the
                // largest file written, within a row group too: an output
                // over the range would be rewritten again on every pass.
                let written = (out.writer.bytes_written() + out.writer.in_progress_size()) as u64;
                if out.consumed >= split || written >= write_max {
                    done.push(finish(writing.take().expect("written"))?);
                }
            }
        }
    }
    if let Some(writing) = writing {
        done.push(finish(writing)?);
    }
    Ok(done)
}

/// Stores `body` as object `key` of the table's storage: its version.
async fn put(state: &S3State, warehouse: &str, key: &str, body: SealedBody) -> Result<u64, String> {
    let (warehouse, key) = (warehouse.to_string(), key.to_string());
    let now = crate::objects::now_ms();
    crate::scope::object(state, move |file| {
        file.transaction(|tx| {
            tx.put_object(
                &warehouse,
                &key,
                &body,
                &truthdb_object::ObjectAttrs::default(),
                truthdb_object::StorageClass::Standard,
                now,
                0,
            )
        })
    })
    .await
    .map_err(|err| err.message)?
    .map(|info| info.version)
    .map_err(|err| format!("The table's storage refused a compacted file: {err:?}"))
}

/// Stores `bytes` as object `key` of the table's storage: its version.
async fn put_bytes(
    state: &S3State,
    warehouse: &str,
    key: &str,
    bytes: Vec<u8>,
) -> Result<u64, String> {
    let storage = Arc::clone(&state.storage);
    let body = tokio::task::spawn_blocking(move || {
        let now = crate::objects::now_ms();
        let mut writer = BodyWriter::begin(&storage, now)?;
        writer.write(&storage, &bytes, now)?;
        writer.finish(&storage, now)
    })
    .await
    .map_err(|_| "A manifest was not written.".to_string())?
    .map_err(|err| err.to_string())?;
    put(state, warehouse, key, body).await
}

/// Removes the objects compaction wrote, by key and version (its commit
/// did not happen); one left is unreferenced file removal's to find.
async fn remove(state: &S3State, warehouse: &str, written: &[(String, u64)], now: u64) {
    for (key, version) in written {
        let (warehouse, key, version) = (warehouse.to_string(), key.clone(), *version);
        let _ = crate::scope::object(state, move |file| {
            file.transaction(|tx| {
                tx.delete_object(
                    &warehouse,
                    &key,
                    Some(VersionSel::Seq(version)),
                    true,
                    now,
                    DROP_GRACE_MS,
                )
            })
        })
        .await;
    }
}

/// What a value's bounds are compared and serialized as.
#[derive(Clone, Copy)]
enum Kind {
    /// A 4-byte or 8-byte little-endian integer (int, long, date, time,
    /// timestamp).
    Int32,
    Int64,
    /// A decimal held as an INT32 or INT64: Iceberg's bound is its unscaled
    /// value in the fewest big-endian two's-complement bytes.
    Decimal,
    Bool,
    /// Bytes compared unsigned (string, binary, fixed).
    Bytes,
}

/// How a leaf's bounds are kept, if they are: the types whose Parquet
/// statistics are what Iceberg's bounds hold (no floating point, whose
/// NaN and signed-zero rules differ, and no UUID, which Iceberg's Java
/// orders as signed longs).
fn kind(leaf: &parquet::schema::types::ColumnDescriptor) -> Option<Kind> {
    use parquet::basic::{
        ConvertedType as C, IntType, LogicalType as L, TimeType, TimeUnit, TimestampType, Type as P,
    };
    let logical = leaf.logical_type_ref();
    match leaf.physical_type() {
        P::BOOLEAN if logical.is_none() => Some(Kind::Bool),
        P::INT32 => match (logical, leaf.converted_type()) {
            (Some(L::Decimal { .. }), _) | (None, C::DECIMAL) => Some(Kind::Decimal),
            (
                None
                | Some(L::Integer(IntType {
                    is_signed: true, ..
                }))
                | Some(L::Date),
                C::NONE | C::INT_8 | C::INT_16 | C::INT_32 | C::DATE,
            ) => Some(Kind::Int32),
            _ => None,
        },
        P::INT64 => match (logical, leaf.converted_type()) {
            (Some(L::Decimal { .. }), _) | (None, C::DECIMAL) => Some(Kind::Decimal),
            (Some(L::Time(TimeType { unit, .. })), _)
            | (Some(L::Timestamp(TimestampType { unit, .. })), _)
                if *unit == TimeUnit::MICROS =>
            {
                Some(Kind::Int64)
            }
            (
                None
                | Some(L::Integer(IntType {
                    is_signed: true, ..
                })),
                C::NONE | C::INT_64,
            ) => Some(Kind::Int64),
            _ => None,
        },
        P::BYTE_ARRAY => match (logical, leaf.converted_type()) {
            (None | Some(L::String), C::NONE | C::UTF8) => Some(Kind::Bytes),
            _ => None,
        },
        P::FIXED_LEN_BYTE_ARRAY if logical.is_none() && leaf.converted_type() == C::NONE => {
            Some(Kind::Bytes)
        }
        _ => None,
    }
}

/// A bound as compared.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Bound {
    Int(i64),
    Bytes(Vec<u8>),
}

fn bound(kind: Kind, bytes: &[u8]) -> Option<Bound> {
    Some(match kind {
        Kind::Int32 | Kind::Decimal if bytes.len() == 4 => {
            Bound::Int(i64::from(i32::from_le_bytes(bytes.try_into().ok()?)))
        }
        Kind::Int64 | Kind::Decimal if bytes.len() == 8 => {
            Bound::Int(i64::from_le_bytes(bytes.try_into().ok()?))
        }
        Kind::Bool if bytes.len() == 1 => Bound::Int(i64::from(bytes[0] != 0)),
        Kind::Bytes => Bound::Bytes(bytes.to_vec()),
        _ => return None,
    })
}

/// A bound in Iceberg's single-value serialization.
fn serialized(kind: Kind, bound: &Bound) -> Vec<u8> {
    match (kind, bound) {
        (Kind::Int32, Bound::Int(value)) => (*value as i32).to_le_bytes().to_vec(),
        (Kind::Int64, Bound::Int(value)) => value.to_le_bytes().to_vec(),
        (Kind::Bool, Bound::Int(value)) => vec![u8::from(*value != 0)],
        (Kind::Decimal, Bound::Int(value)) => {
            let bytes = value.to_be_bytes();
            let mut start = 0;
            while start < 7
                && ((bytes[start] == 0 && bytes[start + 1] & 0x80 == 0)
                    || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0))
            {
                start += 1;
            }
            bytes[start..].to_vec()
        }
        (_, Bound::Bytes(bytes)) => bytes.clone(),
        (_, Bound::Int(value)) => value.to_le_bytes().to_vec(),
    }
}

/// An output's metrics, by Iceberg field id: its column sizes (every leaf
/// with a field id), its value and null counts and bounds (leaves outside
/// any list or map, bounds as [`kind`] keeps them).
struct Metrics {
    sizes: Vec<(i32, Avro)>,
    values: Vec<(i32, Avro)>,
    nulls: Vec<(i32, Avro)>,
    lower: Vec<(i32, Avro)>,
    upper: Vec<(i32, Avro)>,
    offsets: Vec<Avro>,
}

fn metrics(metadata: &parquet::file::metadata::ParquetMetaData) -> Metrics {
    let descr = metadata.file_metadata().schema_descr();
    let groups = metadata.row_groups();
    let mut out = Metrics {
        sizes: Vec::new(),
        values: Vec::new(),
        nulls: Vec::new(),
        lower: Vec::new(),
        upper: Vec::new(),
        offsets: groups
            .iter()
            .filter_map(|group| group.columns().first())
            .map(|column| Avro::Long(column.byte_range().0 as i64))
            .collect(),
    };
    for leaf in 0..descr.num_columns() {
        let column = descr.column(leaf);
        let info = column.self_type().get_basic_info();
        if !info.has_id() {
            continue;
        }
        let id = info.id();
        let chunks: Vec<_> = groups.iter().map(|group| group.column(leaf)).collect();
        out.sizes.push((
            id,
            Avro::Long(chunks.iter().map(|chunk| chunk.compressed_size()).sum()),
        ));
        if column.max_rep_level() > 0 {
            continue;
        }
        let values: i64 = chunks.iter().map(|chunk| chunk.num_values()).sum();
        out.values.push((id, Avro::Long(values)));
        let nulls: Option<u64> = chunks
            .iter()
            .map(|chunk| chunk.statistics()?.null_count_opt())
            .sum();
        if let Some(nulls) = nulls {
            out.nulls.push((id, Avro::Long(nulls as i64)));
        }
        let Some(kind) = kind(&column) else {
            continue;
        };
        // Every chunk holding a value gives its bounds, or none are known.
        let mut range: Option<(Bound, Bound)> = None;
        let mut known = true;
        for chunk in &chunks {
            let stats = chunk.statistics();
            let null_count = stats.and_then(|stats| stats.null_count_opt());
            let all_null = null_count.is_some_and(|nulls| nulls as i64 == chunk.num_values());
            let pair = stats.and_then(|stats| {
                Some((
                    bound(kind, stats.min_bytes_opt()?)?,
                    bound(kind, stats.max_bytes_opt()?)?,
                ))
            });
            match pair {
                Some((low, high)) => {
                    range = Some(match range {
                        Some((lowest, highest)) => (lowest.min(low), highest.max(high)),
                        None => (low, high),
                    });
                }
                None if all_null || chunk.num_values() == 0 => {}
                None => known = false,
            }
        }
        if let (true, Some((low, high))) = (known, range) {
            out.lower.push((id, Avro::Bytes(serialized(kind, &low))));
            out.upper.push((id, Avro::Bytes(serialized(kind, &high))));
        }
    }
    out
}

/// Commits the rewrite: the manifests that held rewritten files written
/// again (those entries deleted), a manifest of the outputs per manifest
/// their groups came from, a manifest list, and a `replace` snapshot as
/// the main branch's, from the table as it was read.
#[allow(clippy::too_many_arguments)]
async fn commit(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    (base, snapshot, snapshot_id): (&Value, &Value, i64),
    (list, manifests, data): (&AvroFile, &[Manifest], &[DataFile]),
    (rewrites, written): (&[Rewrite], &[Output]),
    now: u64,
) -> Result<(), String> {
    let warehouse = truthdb_object::warehouse_bucket(&table.id);
    let location = table.warehouse_location.clone();
    let sequence = base["last-sequence-number"].as_i64().unwrap_or_default() + 1;
    let current = snapshot["snapshot-id"].as_i64().unwrap_or_default();
    // The entries rewritten, by manifest.
    let mut replaced: HashMap<usize, HashSet<usize>> = HashMap::new();
    for (files, _) in rewrites {
        for &at in files {
            replaced
                .entry(data[at].manifest)
                .or_default()
                .insert(data[at].entry);
        }
    }
    let mut keys: Vec<(String, u64)> = Vec::new();
    let mut list_records: Vec<Avro> = Vec::new();
    let (mut added_files, mut added_records, mut added_size) = (0i64, 0i64, 0i64);
    let (mut deleted_files, mut deleted_records, mut deleted_size) = (0i64, 0i64, 0i64);
    let outcome: Result<(), String> = async {
        for (at, manifest) in manifests.iter().enumerate() {
            let Some(entries) = replaced.get(&at) else {
                list_records.push(manifest.entry.clone());
                continue;
            };
            let schema = &manifest.file.schema;
            let entry_ids = ids(schema, schema)?;
            let file_schema = field_schema(schema, schema, 2)
                .ok_or_else(|| "A manifest entry has no data_file.".to_string())?;
            let file_ids = ids(schema, &file_schema)?;
            let mut records = Vec::new();
            let (mut existing, mut existing_rows, mut gone, mut gone_rows) = (0, 0i64, 0, 0i64);
            let mut min_sequence: Option<i64> = None;
            for (index, record) in manifest.file.records.iter().enumerate() {
                if int(get(record, &entry_ids, 0)) == Some(2) {
                    continue;
                }
                let data_sequence = int(get(record, &entry_ids, 3)).unwrap_or(manifest.sequence);
                let file_sequence = int(get(record, &entry_ids, 4)).unwrap_or(manifest.sequence);
                let rows = get(record, &entry_ids, 2)
                    .and_then(|file| int(get(file, &file_ids, 103)))
                    .unwrap_or_default();
                let (status, by) = if entries.contains(&index) {
                    gone += 1;
                    gone_rows += rows;
                    deleted_files += 1;
                    deleted_records += rows;
                    deleted_size += get(record, &entry_ids, 2)
                        .and_then(|file| int(get(file, &file_ids, 104)))
                        .unwrap_or_default();
                    (2, snapshot_id)
                } else {
                    existing += 1;
                    existing_rows += rows;
                    min_sequence = Some(min_sequence.map_or(data_sequence, |min| min.min(data_sequence)));
                    (
                        0,
                        int(get(record, &entry_ids, 1)).unwrap_or(manifest.added_snapshot),
                    )
                };
                records.push(build(
                    schema,
                    schema,
                    Some(record),
                    &[
                        (0, Set::To(Avro::Int(status))),
                        (1, Set::To(Avro::Long(by))),
                        (3, Set::To(Avro::Long(data_sequence))),
                        (4, Set::To(Avro::Long(file_sequence))),
                    ],
                )?);
            }
            let bytes = write_avro(schema, &manifest.file.metadata, &records)?;
            let key = format!("metadata/{}-m0.avro", crate::tables::uuid());
            let length = bytes.len() as i64;
            keys.push((key.clone(), put_bytes(state, &warehouse, &key, bytes).await?));
            list_records.push(build(
                &list.schema,
                &list.schema,
                Some(&manifest.entry),
                &[
                    (500, Set::To(Avro::String(format!("{location}/{key}")))),
                    (501, Set::To(Avro::Long(length))),
                    (503, Set::To(Avro::Long(snapshot_id))),
                    (515, Set::To(Avro::Long(sequence))),
                    (516, Set::To(Avro::Long(min_sequence.unwrap_or(sequence)))),
                    (504, Set::To(Avro::Int(0))),
                    (505, Set::To(Avro::Int(existing))),
                    (506, Set::To(Avro::Int(gone))),
                    (512, Set::To(Avro::Long(0))),
                    (513, Set::To(Avro::Long(existing_rows))),
                    (514, Set::To(Avro::Long(gone_rows))),
                ],
            )?);
        }
        // The outputs, in a manifest per manifest their group's first file
        // was in (its schema and metadata: of the group's spec).
        let mut by_manifest: BTreeMap<usize, Vec<(usize, &Avro)>> = BTreeMap::new();
        for (files, outputs) in rewrites {
            let first = &data[files[0]];
            for &output in outputs {
                by_manifest
                    .entry(first.manifest)
                    .or_default()
                    .push((output, &first.partition));
            }
        }
        for (at, outputs) in by_manifest {
            let manifest = &manifests[at];
            let schema = &manifest.file.schema;
            let file_schema = field_schema(schema, schema, 2)
                .ok_or_else(|| "A manifest entry has no data_file.".to_string())?;
            let map_schema = |id| field_schema(schema, &file_schema, id);
            let map = |id, entries: Vec<(i32, Avro)>| -> Result<Set, String> {
                match map_schema(id) {
                    Some(map_schema) => Ok(Set::To(iceberg_map(schema, &map_schema, entries)?)),
                    None => Ok(Set::Null),
                }
            };
            let mut records = Vec::new();
            let mut rows_added = 0i64;
            for (output, partition) in &outputs {
                let output = &written[*output];
                let rows = output.metadata.file_metadata().num_rows();
                rows_added += rows;
                added_files += 1;
                added_records += rows;
                added_size += output.size as i64;
                let metrics = metrics(&output.metadata);
                let file = build(
                    schema,
                    &file_schema,
                    None,
                    &[
                        (134, Set::To(Avro::Int(0))),
                        (100, Set::To(Avro::String(format!("{location}/{}", output.key)))),
                        (101, Set::To(Avro::String("PARQUET".to_string()))),
                        (102, Set::To((*partition).clone())),
                        (103, Set::To(Avro::Long(rows))),
                        (104, Set::To(Avro::Long(output.size as i64))),
                        // Format version 1's (a table upgraded from it):
                        // "deprecated ... write 64MB".
                        (105, Set::To(Avro::Long(64 << 20))),
                        (108, map(108, metrics.sizes)?),
                        (109, map(109, metrics.values)?),
                        (110, map(110, metrics.nulls)?),
                        (125, map(125, metrics.lower)?),
                        (128, map(128, metrics.upper)?),
                        (132, Set::To(Avro::Array(metrics.offsets))),
                    ],
                )?;
                records.push(build(
                    schema,
                    schema,
                    None,
                    &[
                        (0, Set::To(Avro::Int(1))),
                        (1, Set::To(Avro::Long(snapshot_id))),
                        (3, Set::Null),
                        (4, Set::Null),
                        (2, Set::To(file)),
                    ],
                )?);
            }
            let bytes = write_avro(schema, &manifest.file.metadata, &records)?;
            let key = format!("metadata/{}-m1.avro", crate::tables::uuid());
            let length = bytes.len() as i64;
            keys.push((key.clone(), put_bytes(state, &warehouse, &key, bytes).await?));
            list_records.push(build(
                &list.schema,
                &list.schema,
                Some(&manifest.entry),
                &[
                    (500, Set::To(Avro::String(format!("{location}/{key}")))),
                    (501, Set::To(Avro::Long(length))),
                    (503, Set::To(Avro::Long(snapshot_id))),
                    (515, Set::To(Avro::Long(sequence))),
                    (516, Set::To(Avro::Long(sequence))),
                    (504, Set::To(Avro::Int(records.len() as i32))),
                    (505, Set::To(Avro::Int(0))),
                    (506, Set::To(Avro::Int(0))),
                    (512, Set::To(Avro::Long(rows_added))),
                    (513, Set::To(Avro::Long(0))),
                    (514, Set::To(Avro::Long(0))),
                ],
            )?);
        }
        // The manifest list, its metadata naming this snapshot.
        let mut metadata = list.metadata.clone();
        for (key, value) in [
            ("snapshot-id", snapshot_id.to_string()),
            ("parent-snapshot-id", current.to_string()),
            ("sequence-number", sequence.to_string()),
        ] {
            if let Some(entry) = metadata.iter_mut().find(|(name, _)| name == key) {
                entry.1 = value.into_bytes();
            }
        }
        let bytes = write_avro(&list.schema, &metadata, &list_records)?;
        let key = format!(
            "metadata/snap-{snapshot_id}-1-{}.avro",
            crate::tables::uuid()
        );
        keys.push((key.clone(), put_bytes(state, &warehouse, &key, bytes).await?));
        let mut summary = json!({
            "operation": "replace",
            "added-data-files": added_files.to_string(),
            "deleted-data-files": deleted_files.to_string(),
            "added-records": added_records.to_string(),
            "deleted-records": deleted_records.to_string(),
            "added-files-size": added_size.to_string(),
            "removed-files-size": deleted_size.to_string(),
        });
        let before = &snapshot["summary"];
        let total = |field: &str| before[field].as_str().and_then(|value| value.parse::<i64>().ok());
        if let Some(files) = total("total-data-files") {
            summary["total-data-files"] = json!((files + added_files - deleted_files).to_string());
        }
        if let Some(size) = total("total-files-size") {
            summary["total-files-size"] = json!((size + added_size - deleted_size).to_string());
        }
        for field in [
            "total-records",
            "total-delete-files",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            if let Some(value) = before.get(field) {
                summary[field] = value.clone();
            }
        }
        let mut next_snapshot = json!({
            "snapshot-id": snapshot_id,
            "parent-snapshot-id": current,
            "sequence-number": sequence,
            "timestamp-ms": now,
            "manifest-list": format!("{location}/{key}"),
            "summary": summary,
        });
        if let Some(schema) = snapshot.get("schema-id").filter(|id| !id.is_null()) {
            next_snapshot["schema-id"] = schema.clone();
        }
        let requirements = [
            json!({"type": "assert-table-uuid", "uuid": base["table-uuid"]}),
            json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current}),
        ];
        let updates = [
            json!({"action": "add-snapshot", "snapshot": next_snapshot}),
            json!({"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": snapshot_id}),
        ];
        let metadata_location = table
            .metadata_location
            .clone()
            .expect("read: it has a metadata file");
        let next = match crate::iceberg_commit::apply(
            base,
            &metadata_location,
            &requirements,
            &updates,
            now,
        ) {
            Ok(Some(next)) => next,
            Ok(None) => return Ok(()),
            Err(crate::iceberg_commit::CommitError::Failed(_)) => return Err(STALE.to_string()),
            Err(crate::iceberg_commit::CommitError::Invalid(message)) => return Err(message),
        };
        let at = (table.namespace.clone(), table.name.clone());
        match crate::tables::iceberg::write_next(
            state,
            (account, bucket),
            account,
            (table, &at),
            (&metadata_location, &next),
            now,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(err) if err.status == 409 => Err(STALE.to_string()),
            Err(err) => Err(err.message),
        }
    }
    .await;
    if outcome.is_err() {
        remove(state, &warehouse, &keys, now).await;
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_is_split_into_outputs_as_icebergs_bin_pack_splits_it() {
        let target = 100 * MIB;
        let (min, max) = (75 * MIB, 180 * MIB);
        let write_max = target + (max - target) / 2;
        let at = |size| split_size(size, (target, min, write_max));
        // Under the target: one output, the target its share.
        assert_eq!(at(10 * MIB), target);
        // 250 MiB: the 50 MiB left over is not a file of its own (under
        // 75), and two of 125 MiB are over 110: three outputs of 83 MiB,
        // under the target, so each takes the target.
        assert_eq!(at(250 * MIB), target);
        // 205 MiB: two of 102.5 (under 110): two outputs, each its share.
        assert_eq!(at(205 * MIB), 205 * MIB / 2 + SPLIT_OVERHEAD);
        // 280 MiB: the 80 left over is a file of its own: three of 93.
        assert_eq!(at(280 * MIB), target);
        assert_eq!(at(200 * MIB), 100 * MIB + SPLIT_OVERHEAD);
        assert_eq!(at(430 * MIB), 430 * MIB / 4 + SPLIT_OVERHEAD);
    }

    #[test]
    fn bounds_are_icebergs_single_value_serialization() {
        let decimal = |value: i64| serialized(Kind::Decimal, &Bound::Int(value));
        assert_eq!(decimal(0), [0x00]);
        assert_eq!(decimal(127), [0x7f]);
        assert_eq!(decimal(128), [0x00, 0x80]);
        assert_eq!(decimal(-1), [0xff]);
        assert_eq!(decimal(-128), [0x80]);
        assert_eq!(decimal(-129), [0xff, 0x7f]);
        assert_eq!(decimal(i64::MIN), i64::MIN.to_be_bytes());
        assert_eq!(
            serialized(Kind::Int32, &Bound::Int(-2)),
            (-2i32).to_le_bytes()
        );
        assert_eq!(
            serialized(Kind::Int64, &Bound::Int(-2)),
            (-2i64).to_le_bytes()
        );
        assert_eq!(serialized(Kind::Bool, &Bound::Int(1)), [1]);
        // Parquet's statistics read back as bounds, and compared by value.
        assert!(
            bound(Kind::Int32, &(-5i32).to_le_bytes()) < bound(Kind::Int32, &3i32.to_le_bytes())
        );
        assert!(bound(Kind::Bytes, b"b") > bound(Kind::Bytes, b"abc"));
        assert!(bound(Kind::Int64, &[0; 4]).is_none());
    }

    #[test]
    fn an_outputs_metrics_cover_all_its_row_groups() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let id = |n: i32| HashMap::from([("PARQUET:field_id".to_string(), n.to_string())]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(id(1)),
            Field::new("name", DataType::Utf8, true).with_metadata(id(2)),
        ]));
        let batch = |ids: Vec<i64>, names: Vec<Option<&str>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap()
        };
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), None).unwrap();
        writer
            .write(&batch(vec![5, 9], vec![Some("m"), None]))
            .unwrap();
        writer.flush().unwrap();
        writer
            .write(&batch(vec![-3, 7], vec![None, Some("z")]))
            .unwrap();
        writer.flush().unwrap();
        writer.write(&batch(vec![1], vec![None])).unwrap();
        let metadata = writer.finish().unwrap();
        assert_eq!(metadata.num_row_groups(), 3);
        let metrics = metrics(&metadata);
        let of = |entries: &[(i32, Avro)], id: i32| {
            entries
                .iter()
                .find(|(of, _)| *of == id)
                .map(|(_, value)| value.clone())
        };
        assert_eq!(of(&metrics.values, 1), Some(Avro::Long(5)));
        assert_eq!(of(&metrics.nulls, 2), Some(Avro::Long(3)));
        assert_eq!(
            of(&metrics.lower, 1),
            Some(Avro::Bytes((-3i64).to_le_bytes().to_vec()))
        );
        assert_eq!(
            of(&metrics.upper, 1),
            Some(Avro::Bytes(9i64.to_le_bytes().to_vec()))
        );
        // The all-null row group gives no bounds, and takes none away.
        assert_eq!(of(&metrics.lower, 2), Some(Avro::Bytes(b"m".to_vec())));
        assert_eq!(of(&metrics.upper, 2), Some(Avro::Bytes(b"z".to_vec())));
        assert_eq!(metrics.offsets.len(), 3);
    }

    #[test]
    fn an_output_ends_at_the_largest_file_written_inside_a_row_group() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use truthdb_core::storage::StorageOptions;
        let path = std::env::temp_dir().join(format!(
            "truthdb-compaction-{}-{}",
            std::process::id(),
            crate::objects::now_ms()
        ));
        const RING: u64 = 8 * 1024 * 1024;
        let storage = Arc::new(
            Storage::create_with_wal_bounds(
                path.clone(),
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
            .expect("create"),
        );
        // One row group of 30000 rows: four batches.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..30_000))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut body = BodyWriter::begin(&storage, 1).unwrap();
        body.write(&storage, &bytes, 1).unwrap();
        let sealed = body.finish(&storage, 1).unwrap();
        truthdb_object::ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.create_bucket("www", truthdb_object::DEFAULT_ACCOUNT, 1)?
                    .expect("bucket");
                tx.put_object(
                    "www",
                    "data/one.parquet",
                    &sealed,
                    &truthdb_object::ObjectAttrs::default(),
                    truthdb_object::StorageClass::Standard,
                    2,
                    0,
                )
                .map(|put| {
                    put.expect("put");
                })
            })
            .unwrap();
        let outputs = rewrite_files(
            &storage,
            "www",
            &["data/one.parquet".to_string()],
            (u64::MAX, 1),
            parquet::basic::Compression::UNCOMPRESSED,
        )
        .unwrap();
        assert!(outputs.len() > 1, "{} outputs", outputs.len());
        let rows: i64 = outputs
            .iter()
            .map(|(_, _, metadata)| metadata.file_metadata().num_rows())
            .sum();
        assert_eq!(rows, 30_000);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn files_are_written_with_the_codec_the_tables_properties_name() {
        use parquet::basic::Compression;
        let with = |properties: Value| codec(&json!({ "properties": properties }));
        assert!(matches!(with(json!({})), Ok(Compression::ZSTD(_))));
        assert!(matches!(
            with(json!({"write.parquet.compression-codec": "SNAPPY"})),
            Ok(Compression::SNAPPY)
        ));
        assert!(matches!(
            with(
                json!({"write.parquet.compression-codec": "gzip", "write.parquet.compression-level": "9"})
            ),
            Ok(Compression::GZIP(_))
        ));
        assert!(matches!(
            with(json!({"write.parquet.compression-codec": "uncompressed"})),
            Ok(Compression::UNCOMPRESSED)
        ));
        for refused in [
            json!({"write.parquet.compression-codec": "lz4"}),
            json!({"write.parquet.compression-codec": "brotli"}),
            json!({"write.parquet.compression-level": "high"}),
            json!({"write.parquet.compression-level": "99"}),
        ] {
            assert!(with(refused.clone()).is_err(), "{refused}");
        }
    }
}
