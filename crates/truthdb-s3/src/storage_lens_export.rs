//! Storage Lens metrics exports: a configuration's report of a day,
//! written to its destination bucket as CSV or Parquet files with a
//! manifest, once its day is reported.
//!
//! The files are under `<prefix>/StorageLens/<account>/<id>/V_1/reports/
//! dt=<day>/`, the manifest at `.../V_1/manifests/dt=<day>/manifest.json`.
//! Their bodies are written first; they, and the day recorded as exported,
//! are then put in one transaction: a crash leaves them all or none, and an
//! export is made once. A destination that is not there (or not the
//! account's) has none; the day is recorded as exported all the same.

use std::sync::Arc;

use parquet::file::writer::SerializedFileWriter;
use serde_json::json;
use truthdb_core::storage::{Storage, StorageError};
use truthdb_object::{
    BodyWriter, LensRecord, ObjFile, ObjectAttrs, Origin, STORAGE_LENS, SealedBody, StorageClass,
};

use crate::batch_run::{REPORT_KEY, object};
use crate::objects::now_ms;
use crate::server::S3State;
use crate::storage_lens::{Export, bucket_of_arn};

/// Records read in one lock hold (a Parquet file's row group).
const PAGE: usize = 1000;
/// Records a file holds at most: past them, the next file.
const FILE_RECORDS: usize = 100_000;
/// How long a body is kept unreferenced before it is taken for abandoned
/// is an hour: one waiting longer is marked touched.
const TOUCH_MS: u64 = 5 * 60 * 1000;
/// How long a body replaced is kept for readers still on it.
const DROP_GRACE_MS: u64 = 60_000;

/// The export's columns, in order.
const COLUMNS: [&str; 12] = [
    "version_number",
    "configuration_id",
    "report_date",
    "aws_account_number",
    "aws_region",
    "storage_class",
    "record_type",
    "record_value",
    "bucket_name",
    "metric_name",
    "metric_value",
    "int_access_tier",
];

/// The Parquet files' schema, as written.
const PARQUET_SCHEMA: &str = "message s3.storage.lens {
  required binary version_number (UTF8);
  required binary configuration_id (UTF8);
  required binary report_date (UTF8);
  required binary aws_account_number (UTF8);
  required binary aws_region (UTF8);
  required binary storage_class (UTF8);
  required binary record_type (UTF8);
  required binary record_value (UTF8);
  required binary bucket_name (UTF8);
  required binary metric_name (UTF8);
  required int64 metric_value;
  optional binary int_access_tier (UTF8);
}";

/// The manifest's `reportSchema` of a Parquet export, as AWS states it.
const PARQUET_REPORT_SCHEMA: &str = "message s3.storage.lens { required string version_number; required string configuration_id; required string report_date; required string aws_account_number; required string aws_region; required string storage_class; required string record_type; required string record_value; required string bucket_name; required string metric_name; required long metric_value; optional string int_access_tier; }";

/// An export to make: a configuration's report of a day.
pub(crate) struct ExportDue {
    pub(crate) key: String,
    pub(crate) account: String,
    pub(crate) id: String,
    pub(crate) document: Vec<u8>,
    pub(crate) generation: u64,
    pub(crate) day: u32,
    pub(crate) export: Export,
}

/// One record's values, in [`COLUMNS`]' order (the last: none).
fn values(due: &ExportDue, region: &str, date: &str, record: &LensRecord) -> [String; 11] {
    [
        "V_1".to_string(),
        due.id.clone(),
        date.to_string(),
        due.account.clone(),
        region.to_string(),
        record.storage_class.clone(),
        record.record_type.as_str().to_string(),
        // "The record_value is URL-encoded."
        percent_encoding::utf8_percent_encode(&record.record_value, REPORT_KEY).to_string(),
        record.bucket.clone(),
        record.metric.clone(),
        record.value.to_string(),
    ]
}

fn csv_line(fields: &[String]) -> String {
    let quoted: Vec<String> = fields
        .iter()
        .map(|field| format!("\"{}\"", field.replace('"', "\"\"")))
        .collect();
    format!("{}\n", quoted.join(","))
}

enum Encoder {
    Csv(Vec<u8>),
    Parquet(Box<SerializedFileWriter<Vec<u8>>>),
}

fn parquet_error(err: parquet::errors::ParquetError) -> String {
    err.to_string()
}

impl Encoder {
    fn new(csv: bool) -> Result<Encoder, String> {
        if csv {
            return Ok(Encoder::Csv(
                csv_line(&COLUMNS.map(str::to_string)).into_bytes(),
            ));
        }
        let schema =
            parquet::schema::parser::parse_message_type(PARQUET_SCHEMA).map_err(parquet_error)?;
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        Ok(Encoder::Parquet(Box::new(
            SerializedFileWriter::new(Vec::new(), Arc::new(schema), Arc::new(props))
                .map_err(parquet_error)?,
        )))
    }

    /// Encodes `rows`; the bytes encoded so far.
    fn add(&mut self, rows: &[[String; 11]]) -> Result<Vec<u8>, String> {
        match self {
            Encoder::Csv(out) => {
                for row in rows {
                    out.extend_from_slice(
                        csv_line(&[&row[..], &[String::new()]].concat()).as_bytes(),
                    );
                }
                Ok(std::mem::take(out))
            }
            Encoder::Parquet(writer) => {
                write_group(writer, rows).map_err(parquet_error)?;
                writer.flush().map_err(|err| err.to_string())?;
                Ok(std::mem::take(writer.inner_mut()))
            }
        }
    }

    /// The bytes that end the file.
    fn finish(self) -> Result<Vec<u8>, String> {
        match self {
            Encoder::Csv(out) => Ok(out),
            Encoder::Parquet(mut writer) => {
                writer.finish().map_err(parquet_error)?;
                writer.flush().map_err(|err| err.to_string())?;
                Ok(std::mem::take(writer.inner_mut()))
            }
        }
    }
}

fn write_group(
    writer: &mut SerializedFileWriter<Vec<u8>>,
    rows: &[[String; 11]],
) -> parquet::errors::Result<()> {
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    let mut group = writer.next_row_group()?;
    for index in 0..COLUMNS.len() {
        let mut column = group
            .next_column()?
            .expect("a column for each of the schema's");
        match index {
            10 => {
                let values: Vec<i64> = rows
                    .iter()
                    .map(|row| {
                        row[10]
                            .parse::<u64>()
                            .map_or(0, |value| value.min(i64::MAX as u64) as i64)
                    })
                    .collect();
                column
                    .typed::<Int64Type>()
                    .write_batch(&values, None, None)?;
            }
            // int_access_tier: none.
            11 => {
                let levels = vec![0i16; rows.len()];
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&[], Some(&levels), None)?;
            }
            _ => {
                let values: Vec<ByteArray> = rows
                    .iter()
                    .map(|row| ByteArray::from(row[index].as_bytes().to_vec()))
                    .collect();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&values, None, None)?;
            }
        }
        column.close()?;
    }
    group.close()?;
    Ok(())
}

/// The files being written: the one open, and those done (each its body,
/// its size and its MD5 in hex).
struct Files {
    csv: bool,
    open: Option<(Encoder, BodyWriter)>,
    size: u64,
    records: usize,
    done: Vec<(SealedBody, u64, String)>,
    touched_ms: u64,
}

impl Files {
    fn new(csv: bool) -> Files {
        Files {
            csv,
            open: None,
            size: 0,
            records: 0,
            done: Vec::new(),
            touched_ms: now_ms(),
        }
    }

    fn add(&mut self, storage: &Storage, rows: &[[String; 11]]) -> Result<(), String> {
        let now = now_ms();
        if self.open.is_none() {
            let writer = BodyWriter::begin(storage, now).map_err(|err| err.to_string())?;
            self.open = Some((Encoder::new(self.csv)?, writer));
        }
        let (encoder, writer) = self.open.as_mut().expect("just opened");
        let bytes = encoder.add(rows)?;
        self.size += bytes.len() as u64;
        writer
            .write(storage, &bytes, now)
            .map_err(|err| err.to_string())?;
        self.records += rows.len();
        if self.records >= FILE_RECORDS {
            self.seal(storage)?;
        }
        if now.saturating_sub(self.touched_ms) >= TOUCH_MS {
            if let Some((_, writer)) = &mut self.open {
                writer.touch(storage, now).map_err(|err| err.to_string())?;
            }
            // The files written whole wait for the delivery as they are.
            ObjFile::new(&mut storage.lock())
                .transaction(|tx| {
                    for (body, _, _) in &self.done {
                        tx.touch_body(body.body, now)?;
                    }
                    Ok(())
                })
                .map_err(|err| err.to_string())?;
            self.touched_ms = now;
        }
        Ok(())
    }

    /// Ends the open file, if one is.
    fn seal(&mut self, storage: &Storage) -> Result<(), String> {
        let Some((encoder, mut writer)) = self.open.take() else {
            return Ok(());
        };
        let bytes = encoder.finish()?;
        let now = now_ms();
        writer
            .write(storage, &bytes, now)
            .map_err(|err| err.to_string())?;
        let sealed = writer.finish(storage, now).map_err(|err| err.to_string())?;
        let md5: String = sealed
            .md5
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let size = std::mem::take(&mut self.size) + bytes.len() as u64;
        self.done.push((sealed, size, md5));
        self.records = 0;
        Ok(())
    }

    /// The files: at least one (a report of no records has one, empty of
    /// them).
    fn finish(mut self, storage: &Storage) -> Result<Vec<(SealedBody, u64, String)>, String> {
        if self.done.is_empty() && self.open.is_none() {
            self.add(storage, &[])?;
        }
        self.seal(storage)?;
        Ok(self.done)
    }
}

async fn on_files<T: Send + 'static>(
    state: &S3State,
    files: Files,
    op: impl FnOnce(&Storage, Files) -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    let storage = Arc::clone(&state.storage);
    tokio::task::spawn_blocking(move || op(&storage, files))
        .await
        .map_err(|err| err.to_string())?
}

/// The day `day` (days since 1970-01-01) as `YYYY-MM-DD`.
fn date(day: u32) -> String {
    truthdb_object::time::iso8601(u64::from(day) * 86_400_000)[..10].to_string()
}

/// Makes and delivers `due`'s export. Returns early, recording nothing,
/// when its report is no longer read or the server shuts down.
pub(crate) async fn export(
    state: &S3State,
    due: ExportDue,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let csv = due.export.format == "CSV";
    let date = date(due.day);
    let mut files = Files::new(csv);
    let mut after: Option<Vec<u8>> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let (key, generation, day, resume) =
            (due.key.clone(), due.generation, due.day, after.clone());
        let page = object(state, move |file| {
            file.lens_report_page(&key, generation, day, resume.as_deref(), PAGE)
        })
        .await?;
        let Some((records, next)) = page else {
            return Ok(());
        };
        let rows: Vec<[String; 11]> = records
            .iter()
            .map(|record| values(&due, &state.region, &date, record))
            .collect();
        files = on_files(state, files, move |storage, mut files| {
            files.add(storage, &rows)?;
            Ok(files)
        })
        .await?;
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let data = on_files(state, files, |storage, files| files.finish(storage)).await?;
    deliver(state, &due, &date, data).await
}

/// A small body: the manifest.
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

/// What a delivery's transaction fails with when a file cannot be written:
/// it rolls back, and the day is recorded as exported without its files.
const UNWRITTEN: &str = "a Storage Lens export file cannot be written";

async fn deliver(
    state: &S3State,
    due: &ExportDue,
    date: &str,
    data: Vec<(SealedBody, u64, String)>,
) -> Result<(), String> {
    let destination = bucket_of_arn(&due.export.bucket_arn)
        .unwrap_or_default()
        .to_string();
    let base = match due
        .export
        .prefix
        .as_deref()
        .unwrap_or("")
        .trim_end_matches('/')
    {
        "" => format!("StorageLens/{}/{}/V_1", due.account, due.id),
        prefix => format!("{prefix}/StorageLens/{}/{}/V_1", due.account, due.id),
    };
    let (extension, format, schema, content_type) = match due.export.format.as_str() {
        "CSV" => ("csv", "CSV", COLUMNS.join(","), "text/csv"),
        _ => (
            "par",
            "Parquet",
            PARQUET_REPORT_SCHEMA.to_string(),
            "application/octet-stream",
        ),
    };
    // The files' names: of the configuration's generation and the day (one
    // made again names them the same).
    let name = |index: usize| {
        let digest = ring::digest::digest(
            &ring::digest::SHA256,
            format!("{}/{}/{}/{index}", due.key, due.generation, due.day).as_bytes(),
        );
        let hex: String = digest.as_ref()[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!(
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    };
    let data: Vec<(String, SealedBody, u64, String)> = data
        .into_iter()
        .enumerate()
        .map(|(index, (body, size, md5))| {
            (
                format!("{base}/reports/dt={date}/{}.{extension}", name(index)),
                body,
                size,
                md5,
            )
        })
        .collect();
    let manifest = json!({
        "sourceAccountId": due.account,
        "configId": due.id,
        "destinationBucket": format!("arn:aws:s3:::{destination}"),
        "reportVersion": "V_1",
        "reportDate": date,
        "reportFormat": format,
        "reportSchema": schema,
        "reportFiles": data
            .iter()
            .map(|(key, _, size, md5)| json!({"key": key, "size": size, "md5Checksum": md5}))
            .collect::<Vec<_>>(),
    });
    let manifest = serde_json::to_vec(&manifest).map_err(|err| err.to_string())?;
    let manifest = small_body(state, manifest).await?;
    let mut files: Vec<(String, SealedBody, &'static str)> = data
        .into_iter()
        .map(|(key, body, _, _)| (key, body, content_type))
        .collect();
    files.push((
        format!("{base}/manifests/dt={date}/manifest.json"),
        manifest,
        "application/json",
    ));
    let (key, account, document, generation, day) = (
        due.key.clone(),
        due.account.clone(),
        due.document.clone(),
        due.generation,
        due.day,
    );
    let refused = object(state, move |file| {
        deliver_in(
            file,
            &key,
            &account,
            &document,
            generation,
            day,
            &destination,
            &files,
        )
    })
    .await?;
    if let Some(problem) = refused {
        eprintln!(
            "storage lens: the export of {} of {date}: {problem}",
            due.key
        );
    }
    Ok(())
}

/// Puts the files in one transaction with the day recorded as exported; a
/// destination refused records it alone. Nothing when the configuration
/// is no longer as the export began.
#[allow(clippy::too_many_arguments)]
fn deliver_in(
    file: &mut ObjFile<'_>,
    key: &str,
    account: &str,
    document: &[u8],
    generation: u64,
    day: u32,
    destination: &str,
    files: &[(String, SealedBody, &'static str)],
) -> Result<Option<String>, StorageError> {
    if file.bucket_config("", STORAGE_LENS, key)?.as_deref() != Some(document)
        || file.lens_state(key)?.is_none_or(|lens| {
            lens.generation != generation || lens.exported.is_some_and(|last| last >= day)
        })
    {
        return Ok(None);
    }
    let refused = match file.bucket(destination)? {
        None => Some("its destination bucket does not exist"),
        Some(target) if target.owner != account => {
            Some("its destination bucket is not the configuration's account's")
        }
        Some(target) if target.warehouse.is_some() || target.zone.is_some() => {
            Some("its destination is not a general purpose bucket")
        }
        Some(_) => None,
    };
    let now = now_ms();
    let (bucket_lock, default) = crate::object_lock::bucket_state(file, destination)?;
    let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
        .ok()
        .flatten();
    file.set_origin(Origin {
        operation: "PutObject".to_string(),
        request_id: format!("storage-lens-{generation}-{day}"),
        account: Some(account.to_string()),
        ..Origin::default()
    });
    let problem = match refused {
        Some(problem) => problem,
        None => match file.transaction(|tx| {
            for (name, body, content_type) in files {
                let attrs = ObjectAttrs {
                    content_type: Some(content_type.to_string()),
                    ..ObjectAttrs::default()
                };
                match tx.put_object(
                    destination,
                    name,
                    body,
                    &attrs,
                    StorageClass::Standard,
                    now,
                    DROP_GRACE_MS,
                )? {
                    Ok(info) => {
                        if let Some(lock) = &lock {
                            tx.set_lock(destination, name, info.version, lock)?;
                        }
                    }
                    Err(_) => return Err(StorageError::InvalidConfig(UNWRITTEN.to_string())),
                }
            }
            tx.lens_exported(key, generation, day)
        }) {
            Ok(_) => return Ok(None),
            Err(StorageError::InvalidConfig(message)) if message == UNWRITTEN => {
                "its files could not be written"
            }
            Err(err) => return Err(err),
        },
    };
    file.transaction(|tx| tx.lens_exported(key, generation, day))?;
    Ok(Some(problem.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_day_is_its_date_and_values_are_quoted() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(20_000), "2024-10-04");
        assert_eq!(csv_line(&["a\"b".into(), "".into()]), "\"a\"\"b\",\"\"\n");
    }
}
