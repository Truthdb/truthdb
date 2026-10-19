//! SQL over stored files (S3 plan item 16, Athena's role):
//! `sys.s3_read(@bucket, @key, @format [, @options])` reads a CSV or JSON
//! object as rows. With `{"prefix": true}`, `@key` is a prefix and every
//! current object under it is read in key order, each row led by its
//! `_s3_key`.
//!
//! - **CSV**: the header row names the columns (or `c1..cn` with
//!   `{"header": false}`); every value is text. A short row is padded with
//!   NULLs; a row longer than the first is an error.
//! - **JSON**: JSON Lines, concatenated values, or one top-level array (read
//!   whole, up to 64 MiB). Object records give one column per key, in the
//!   order first seen in the first 100 records (later keys are left out);
//!   other records give one `value` column. Strings are text, nested values
//!   their JSON text.
//!
//! Options (a JSON object): `header` (CSV, default true), `delimiter` and
//! `quote` (CSV, one ASCII character each), `max_rows`, `prefix`.
//!
//! A body is leased while it is read and read a chunk per storage-lock hold,
//! so a long query neither holds the lock nor loses its bytes to garbage
//! collection.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use serde_json::Value;
use truthdb_core::storage::Storage;
use truthdb_object::{ListAfter, ObjFile, ObjectError, VersionSel};
use truthdb_relational::engine::{ExternalRowset, ExternalStream, ResultColumn, check_cancelled};
use truthdb_relational::relstore::types::{ColumnType, Datum};
use truthdb_sql::error::SqlError;
use truthdb_sql::value::SqlValue;

/// Bytes read per storage-lock hold.
const READ_CHUNK: usize = 1024 * 1024;
/// Rows handed over per slice.
const SLICE_ROWS: usize = 1024;
/// JSON records the column set is taken from.
const SCHEMA_RECORDS: usize = 100;
/// Largest top-level JSON array read whole.
const MAX_JSON_ARRAY: u64 = 64 * 1024 * 1024;
/// Keys listed per lock hold in prefix mode.
const LIST_KEYS: usize = 1024;

fn read_error(message: impl Into<String>) -> SqlError {
    SqlError::new(4864, 16, 1, message)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Csv,
    Json,
}

#[derive(Debug, Clone)]
struct Options {
    header: bool,
    delimiter: u8,
    quote: u8,
    max_rows: Option<u64>,
    prefix: bool,
}

fn one_ascii(name: &str, value: &Value) -> Result<u8, SqlError> {
    match value.as_str() {
        Some(text) if text.len() == 1 && text.is_ascii() => Ok(text.as_bytes()[0]),
        _ => Err(read_error(format!(
            "The option '{name}' must be one ASCII character."
        ))),
    }
}

fn parse_options(text: Option<&str>) -> Result<Options, SqlError> {
    let mut options = Options {
        header: true,
        delimiter: b',',
        quote: b'"',
        max_rows: None,
        prefix: false,
    };
    let Some(text) = text else {
        return Ok(options);
    };
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(text) else {
        return Err(read_error(
            "The options of sys.s3_read must be a JSON object.",
        ));
    };
    for (name, value) in &map {
        let flag = || {
            value
                .as_bool()
                .ok_or_else(|| read_error(format!("The option '{name}' must be true or false.")))
        };
        match name.as_str() {
            "header" => options.header = flag()?,
            "prefix" => options.prefix = flag()?,
            "delimiter" => options.delimiter = one_ascii(name, value)?,
            "quote" => options.quote = one_ascii(name, value)?,
            "max_rows" => {
                options.max_rows = Some(value.as_u64().ok_or_else(|| {
                    read_error("The option 'max_rows' must be a non-negative integer.")
                })?)
            }
            other => return Err(read_error(format!("Unknown sys.s3_read option '{other}'."))),
        }
    }
    Ok(options)
}

/// An object's body as a byte stream: the version is resolved and its body
/// leased in one lock hold, then read a chunk per lock hold (from the plain
/// part holding each byte, for a multipart body). The lease goes with the
/// reader.
struct BodyReader {
    core: Arc<Storage>,
    body: u64,
    layout: Vec<(u64, u64, u64)>,
    size: u64,
    pos: u64,
}

impl BodyReader {
    fn open(core: &Arc<Storage>, bucket: &str, key: &str) -> Result<BodyReader, SqlError> {
        let mut guard = core.lock();
        let mut file = ObjFile::try_new(&mut guard).ok_or_else(super::s3_sql::no_object_engine)?;
        let info = match file
            .head_object(bucket, key, VersionSel::Latest)
            .map_err(super::s3_sql::storage_error)?
        {
            Ok(info) => info,
            Err(ObjectError::NoSuchKey) => {
                return Err(SqlError::new(
                    208,
                    16,
                    1,
                    format!("The object '{key}' does not exist in bucket '{bucket}'."),
                ));
            }
            Err(err) => return Err(super::s3_sql::object_error(err, bucket)),
        };
        let layout = file
            .body_layout(info.body)
            .map_err(super::s3_sql::storage_error)?;
        file.take_lease(info.body)
            .map_err(super::s3_sql::storage_error)?;
        Ok(BodyReader {
            core: Arc::clone(core),
            body: info.body,
            layout,
            size: info.size,
            pos: 0,
        })
    }
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.size || buf.is_empty() {
            return Ok(0);
        }
        let pos = self.pos;
        let Some(&(part, start, len)) = self
            .layout
            .iter()
            .find(|(_, start, len)| pos >= *start && pos < start + len)
        else {
            return Ok(0);
        };
        let take = ((start + len - pos) as usize)
            .min(buf.len())
            .min(READ_CHUNK);
        {
            let mut guard = self.core.lock();
            let mut file = ObjFile::try_new(&mut guard)
                .ok_or_else(|| std::io::Error::other("the object store is gone"))?;
            file.read_body(part, pos - start, &mut buf[..take])
                .map_err(std::io::Error::other)?;
        }
        self.pos += take as u64;
        Ok(take)
    }
}

impl Drop for BodyReader {
    fn drop(&mut self) {
        let mut guard = self.core.lock();
        if let Some(mut file) = ObjFile::try_new(&mut guard) {
            file.release_lease(self.body);
        }
    }
}

type Rows = Box<dyn Iterator<Item = Result<Vec<Datum>, SqlError>> + Send>;

/// What one object's rows are read as: the first object fixes it.
#[derive(Debug, Clone)]
enum Schema {
    Csv(Vec<String>),
    /// Object records' keys, or `None` for one `value` column.
    Json(Option<Vec<String>>),
}

impl Schema {
    fn columns(&self) -> Vec<String> {
        match self {
            Schema::Csv(names) => names.clone(),
            Schema::Json(Some(keys)) => keys.clone(),
            Schema::Json(None) => vec!["value".to_string()],
        }
    }
}

fn json_datum(value: &Value) -> Datum {
    match value {
        Value::Null => Datum::Null,
        Value::String(text) => Datum::NVarChar(text.clone()),
        other => Datum::NVarChar(other.to_string()),
    }
}

fn json_row(record: &Value, keys: &Option<Vec<String>>) -> Vec<Datum> {
    match (keys, record) {
        (Some(keys), Value::Object(map)) => keys
            .iter()
            .map(|key| map.get(key).map_or(Datum::Null, json_datum))
            .collect(),
        // A record of another shape than the first ones: its fields are absent.
        (Some(keys), _) => vec![Datum::Null; keys.len()],
        (None, record) => vec![json_datum(record)],
    }
}

/// The JSON records of a body: the elements of a top-level array (read whole,
/// bounded), else each value of a stream of values.
fn json_records(
    mut reader: BufReader<BodyReader>,
) -> Result<Box<dyn Iterator<Item = Result<Value, SqlError>> + Send>, SqlError> {
    let first = loop {
        let buf = reader
            .fill_buf()
            .map_err(|err| read_error(err.to_string()))?;
        match buf.iter().position(|b| !b.is_ascii_whitespace()) {
            Some(at) => break Some(buf[at]),
            None if buf.is_empty() => break None,
            None => {
                let len = buf.len();
                reader.consume(len);
            }
        }
    };
    if first == Some(b'[') {
        let mut bytes = Vec::new();
        reader
            .take(MAX_JSON_ARRAY + 1)
            .read_to_end(&mut bytes)
            .map_err(|err| read_error(err.to_string()))?;
        if bytes.len() as u64 > MAX_JSON_ARRAY {
            return Err(read_error(
                "A top-level JSON array larger than 64 MiB cannot be read; use JSON Lines.",
            ));
        }
        let Value::Array(items) = serde_json::from_slice(&bytes)
            .map_err(|err| read_error(format!("Invalid JSON: {err}")))?
        else {
            return Err(read_error("Invalid JSON: expected an array."));
        };
        return Ok(Box::new(items.into_iter().map(Ok)));
    }
    Ok(Box::new(
        serde_json::Deserializer::from_reader(reader)
            .into_iter::<Value>()
            .map(|value| value.map_err(|err| read_error(format!("Invalid JSON: {err}")))),
    ))
}

/// One object's rows, and the schema they were read with: `schema` when one
/// is fixed already (prefix mode after the first object), else learnt here.
fn object_rows(
    reader: BodyReader,
    format: Format,
    options: &Options,
    schema: Option<&Schema>,
) -> Result<(Schema, Rows), SqlError> {
    let reader = BufReader::with_capacity(READ_CHUNK, reader);
    match format {
        Format::Csv => {
            let mut csv = csv::ReaderBuilder::new()
                .has_headers(options.header)
                .delimiter(options.delimiter)
                .quote(options.quote)
                .flexible(true)
                .from_reader(reader);
            let mut first = None;
            let learnt = if options.header {
                let headers = csv
                    .headers()
                    .map_err(|err| read_error(format!("Invalid CSV: {err}")))?;
                headers.iter().map(str::to_string).collect::<Vec<_>>()
            } else {
                let mut record = csv::StringRecord::new();
                let read = csv
                    .read_record(&mut record)
                    .map_err(|err| read_error(format!("Invalid CSV: {err}")))?;
                let width = record.len();
                if read {
                    first = Some(record);
                }
                (1..=width).map(|i| format!("c{i}")).collect()
            };
            let schema = schema.cloned().unwrap_or(Schema::Csv(learnt));
            let width = schema.columns().len();
            let to_row = move |record: csv::StringRecord| -> Result<Vec<Datum>, SqlError> {
                if record.len() > width {
                    return Err(read_error(format!(
                        "A CSV row has {} fields; the file has {width} columns.",
                        record.len()
                    )));
                }
                let mut row: Vec<Datum> = record
                    .iter()
                    .map(|field| Datum::NVarChar(field.to_string()))
                    .collect();
                row.resize(width, Datum::Null);
                Ok(row)
            };
            let rows = first
                .into_iter()
                .map(Ok)
                .chain(csv.into_records())
                .map(move |record| {
                    record
                        .map_err(|err| read_error(format!("Invalid CSV: {err}")))
                        .and_then(&to_row)
                });
            Ok((schema, Box::new(rows)))
        }
        Format::Json => {
            let mut records = json_records(reader)?;
            // The schema's records, kept to be handed over first.
            let mut head = Vec::new();
            if schema.is_none() {
                for record in records.by_ref().take(SCHEMA_RECORDS) {
                    head.push(record?);
                }
            }
            let schema = schema.cloned().unwrap_or_else(|| {
                if head.iter().all(Value::is_object) && !head.is_empty() {
                    let mut keys: Vec<String> = Vec::new();
                    for record in &head {
                        for key in record.as_object().into_iter().flat_map(|m| m.keys()) {
                            if !keys.contains(key) {
                                keys.push(key.clone());
                            }
                        }
                    }
                    Schema::Json(Some(keys))
                } else {
                    Schema::Json(None)
                }
            });
            let Schema::Json(keys) = schema.clone() else {
                unreachable!("a JSON read keeps a JSON schema");
            };
            let rows = head
                .into_iter()
                .map(Ok)
                .chain(records)
                .map(move |record| record.map(|record| json_row(&record, &keys)));
            Ok((schema, Box::new(rows)))
        }
    }
}

/// Opens `sys.s3_read(@bucket, @key, @format [, @options])`.
pub(super) fn open(
    core: &Arc<Storage>,
    name: &str,
    args: &[SqlValue],
) -> Result<ExternalRowset, SqlError> {
    super::s3_sql::check_arg_count(name, args, 3, 4)?;
    let bucket = super::s3_sql::string_arg(name, &args[0])?;
    let key = super::s3_sql::string_arg(name, &args[1])?;
    let format = match super::s3_sql::string_arg(name, &args[2])?
        .to_ascii_lowercase()
        .as_str()
    {
        "csv" => Format::Csv,
        "json" => Format::Json,
        other => {
            return Err(read_error(format!(
                "sys.s3_read reads 'csv' or 'json', not '{other}'."
            )));
        }
    };
    let options = match args.get(3) {
        None | Some(SqlValue::Null) => parse_options(None)?,
        Some(value) => parse_options(Some(&super::s3_sql::string_arg(name, value)?))?,
    };
    let mut stream = ReadStream {
        core: Arc::clone(core),
        bucket,
        format,
        remaining: options.max_rows,
        options,
        pending: VecDeque::new(),
        listed_after: None,
        listing_done: false,
        current: None,
        schema: None,
        prefix: None,
    };
    let first_key = if stream.options.prefix {
        stream.next_key(&key)?
    } else {
        Some(key.clone())
    };
    let mut columns = Vec::new();
    if stream.options.prefix {
        columns.push(text_column(
            "_s3_key",
            ColumnType::NVarChar { max_len: 1024 },
        ));
    }
    match first_key {
        Some(first) => {
            stream.open_object(&first)?;
            let schema = stream.schema.as_ref().expect("set by open_object");
            columns.extend(
                schema
                    .columns()
                    .iter()
                    .map(|name| text_column(name, ColumnType::NVarCharMax)),
            );
        }
        // Nothing under the prefix: no rows, and only the key column.
        None => stream.listing_done = true,
    }
    stream.prefix = stream.options.prefix.then_some(key);
    Ok(ExternalRowset {
        columns,
        rows: Box::new(stream),
    })
}

fn text_column(name: &str, column_type: ColumnType) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type,
    }
}

struct ReadStream {
    core: Arc<Storage>,
    bucket: String,
    format: Format,
    options: Options,
    remaining: Option<u64>,
    /// Prefix mode: keys listed and not read yet, and where listing resumes.
    pending: VecDeque<String>,
    listed_after: Option<String>,
    listing_done: bool,
    /// The object being read: its key and rows.
    current: Option<(String, Rows)>,
    schema: Option<Schema>,
    prefix: Option<String>,
}

impl ReadStream {
    /// Prefix mode: the next current key under `prefix`, listing a slice of
    /// keys per lock hold as needed.
    fn next_key(&mut self, prefix: &str) -> Result<Option<String>, SqlError> {
        while self.pending.is_empty() && !self.listing_done {
            let listing = {
                let mut guard = self.core.lock();
                let mut file =
                    ObjFile::try_new(&mut guard).ok_or_else(super::s3_sql::no_object_engine)?;
                let after = self.listed_after.as_deref().map(ListAfter::Key);
                file.list_objects(&self.bucket, prefix, None, after, LIST_KEYS)
                    .map_err(super::s3_sql::storage_error)?
                    .map_err(|err| super::s3_sql::object_error(err, &self.bucket))?
            };
            self.pending
                .extend(listing.objects.into_iter().map(|object| object.key));
            match (listing.truncated, listing.last) {
                (true, Some((last, _))) => self.listed_after = Some(last),
                _ => self.listing_done = true,
            }
        }
        Ok(self.pending.pop_front())
    }

    fn open_object(&mut self, key: &str) -> Result<(), SqlError> {
        // Drop the previous object's reader (and its lease) first.
        self.current = None;
        let reader = BodyReader::open(&self.core, &self.bucket, key)?;
        let (schema, rows) = object_rows(reader, self.format, &self.options, self.schema.as_ref())?;
        self.schema = Some(schema);
        self.current = Some((key.to_string(), rows));
        Ok(())
    }
}

impl ExternalStream for ReadStream {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        let mut rows = Vec::new();
        while rows.len() < SLICE_ROWS && self.remaining != Some(0) {
            let Some((key, current)) = self.current.as_mut() else {
                break;
            };
            match current.next() {
                Some(row) => {
                    let mut row = row?;
                    if self.prefix.is_some() {
                        row.insert(0, Datum::NVarChar(key.clone()));
                    }
                    rows.push(row);
                    if let Some(remaining) = self.remaining.as_mut() {
                        *remaining -= 1;
                    }
                }
                None => {
                    self.current = None;
                    // Many empty objects in a row stay cancellable.
                    check_cancelled()?;
                    if let Some(prefix) = self.prefix.clone()
                        && let Some(next) = self.next_key(&prefix)?
                    {
                        self.open_object(&next)?;
                    }
                }
            }
        }
        if self.remaining == Some(0) {
            // Done early: release the body now rather than at drop.
            self.current = None;
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}
