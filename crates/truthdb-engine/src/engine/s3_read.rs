//! SQL over stored files (S3 plan item 16, Athena's role):
//! `sys.s3_read(@bucket, @key, @format [, @options])` reads a CSV or JSON
//! object as rows. With `{"prefix": true}`, `@key` is a prefix and every
//! current, non-empty object under it is read in key order, each row led by
//! its `_s3_key`; the first object fixes the columns.
//!
//! - **CSV**: the header row names the columns (or `c1..cn` with
//!   `{"header": false}`); every value is text. A later file in prefix mode
//!   is matched to the columns by its header's names (a column it lacks is
//!   NULL; a column the first file lacks is an error), or by position
//!   without headers. A short row is padded with NULLs; a longer one is an
//!   error.
//! - **Parquet**: typed columns from the file's schema (integers, `real`,
//!   `float`, `decimal`, `bit`, `date`, `time`, `datetime2`, text and
//!   binary); nested groups, lists and maps are their JSON text. A value
//!   outside its SQL type's range (a date past 9999, a decimal with more
//!   digits than declared) is an error. A later file in prefix mode is
//!   matched by column name. Snappy, gzip, LZ4 and Brotli pages are read;
//!   Zstandard is not. A page may decompress to at most 256 MiB over the
//!   number of leaf columns (and at least 1 MiB), and a file may have at
//!   most 1,024 leaf columns. INT96 timestamps keep milliseconds only.
//! - **JSON**: JSON Lines or concatenated values, or one top-level array
//!   whose elements are the records. Object records give one column per key,
//!   in the order first seen in the first 100 records (later keys are left
//!   out); other records give one `value` column. Strings are text; numbers
//!   and nested values are their JSON text as written.
//!
//! Column names that are empty or repeat are made unique (`c3`, `name_2`).
//! One CSV record or JSON value may be at most 64 MiB.
//!
//! Options (a JSON object): `header` (CSV, default true), `delimiter` and
//! `quote` (CSV, one ASCII character each, not a line break, not the same),
//! `max_rows`, `prefix`.
//!
//! A body is leased while it is read and read a chunk per storage-lock hold,
//! so a long query neither holds the lock nor loses its bytes to garbage
//! collection.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::Value;
use serde_json::value::RawValue;
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
/// Largest CSV record or JSON value (smaller under test, to be reached).
const MAX_RECORD_BYTES: u64 = if cfg!(test) {
    1024 * 1024
} else {
    64 * 1024 * 1024
};
/// Keys listed per lock hold in prefix mode.
const LIST_KEYS: usize = 1024;
/// The key column a prefix read adds.
const KEY_COLUMN: &str = "_s3_key";

fn read_error(message: impl Into<String>) -> SqlError {
    SqlError::new(4864, 16, 1, message)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Csv,
    Json,
    Parquet,
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
        Some(text) if text.len() == 1 && text.is_ascii() && !matches!(text, "\n" | "\r") => {
            Ok(text.as_bytes()[0])
        }
        _ => Err(read_error(format!(
            "The option '{name}' must be one ASCII character other than a line break."
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
    if options.delimiter == options.quote {
        return Err(read_error(
            "The options 'delimiter' and 'quote' must differ.",
        ));
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

/// The object is not there (any more): error 208.
fn no_such_object(bucket: &str, key: &str) -> SqlError {
    SqlError::new(
        208,
        16,
        1,
        format!("The object '{key}' does not exist in bucket '{bucket}'."),
    )
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
            Err(ObjectError::NoSuchKey) => return Err(no_such_object(bucket, key)),
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

/// How far a parser has read and where its current record began: the
/// reader refuses to run more than [`MAX_RECORD_BYTES`] past that start, so
/// one record without an end cannot take the whole body into memory.
#[derive(Default)]
struct Progress {
    read: AtomicU64,
    record_start: AtomicU64,
}

struct GuardedReader {
    inner: BodyReader,
    progress: Arc<Progress>,
}

impl Read for GuardedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.progress.read.load(Ordering::Relaxed);
        let start = self.progress.record_start.load(Ordering::Relaxed);
        if read > start + MAX_RECORD_BYTES + READ_CHUNK as u64 {
            return Err(std::io::Error::other(
                "a record is larger than 64 MiB (for JSON, use JSON Lines)",
            ));
        }
        let n = self.inner.read(buf)?;
        self.progress.read.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

type Rows = Box<dyn Iterator<Item = Result<Vec<Datum>, SqlError>> + Send>;

/// What rows are read as: the first object fixes it.
#[derive(Debug, Clone)]
enum Schema {
    /// The (unique) column names.
    Csv(Vec<String>),
    /// Object records' keys, or `None` for one `value` column.
    Json(Option<Vec<String>>),
    /// The top-level fields and their column types.
    Parquet(Vec<(String, ColumnType)>),
}

impl Schema {
    fn columns(&self) -> Vec<String> {
        match self {
            Schema::Csv(names) => names.clone(),
            Schema::Json(Some(keys)) => keys.clone(),
            Schema::Json(None) => vec!["value".to_string()],
            Schema::Parquet(fields) => fields.iter().map(|(name, _)| name.clone()).collect(),
        }
    }

    fn types(&self) -> Vec<ColumnType> {
        match self {
            Schema::Parquet(fields) => fields.iter().map(|(_, column_type)| *column_type).collect(),
            other => vec![ColumnType::NVarCharMax; other.columns().len()],
        }
    }
}

/// `names` made usable as columns: an empty name becomes `c<position>`, and
/// a name already taken (or `reserved`) gets `_2`, `_3`, … appended.
fn unique_names(names: &[String], reserved: Option<&str>) -> Vec<String> {
    let mut taken: Vec<String> = reserved.into_iter().map(str::to_ascii_lowercase).collect();
    names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let base = if name.is_empty() {
                format!("c{}", index + 1)
            } else {
                name.clone()
            };
            let mut candidate = base.clone();
            let mut suffix = 2;
            while taken.contains(&candidate.to_ascii_lowercase()) {
                candidate = format!("{base}_{suffix}");
                suffix += 1;
            }
            taken.push(candidate.to_ascii_lowercase());
            candidate
        })
        .collect()
}

/// A CSV body's records, as rows of `width` columns; `map` places a later
/// file's fields by its header (`None`: by position).
struct CsvRows {
    reader: csv::Reader<BufReader<GuardedReader>>,
    first: Option<csv::StringRecord>,
    map: Option<Vec<usize>>,
    width: usize,
    progress: Arc<Progress>,
}

impl Iterator for CsvRows {
    type Item = Result<Vec<Datum>, SqlError>;

    fn next(&mut self) -> Option<Self::Item> {
        let record = match self.first.take() {
            Some(record) => record,
            None => {
                let mut record = csv::StringRecord::new();
                match self.reader.read_record(&mut record) {
                    Ok(true) => record,
                    Ok(false) => return None,
                    Err(err) => return Some(Err(read_error(format!("Invalid CSV: {err}")))),
                }
            }
        };
        self.progress
            .record_start
            .store(self.reader.position().byte(), Ordering::Relaxed);
        let mut row = vec![Datum::Null; self.width];
        for (index, field) in record.iter().enumerate() {
            let column = match &self.map {
                Some(map) => map.get(index).copied(),
                None => (index < self.width).then_some(index),
            };
            let Some(column) = column else {
                return Some(Err(read_error(format!(
                    "A CSV row has {} fields; the file has {} columns.",
                    record.len(),
                    self.map.as_ref().map_or(self.width, Vec::len)
                ))));
            };
            row[column] = Datum::NVarChar(field.to_string());
        }
        Some(Ok(row))
    }
}

fn csv_rows(
    reader: BufReader<GuardedReader>,
    progress: Arc<Progress>,
    options: &Options,
    schema: Option<&Schema>,
    key: &str,
) -> Result<(Schema, Rows), SqlError> {
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(options.header)
        .delimiter(options.delimiter)
        .quote(options.quote)
        .flexible(true)
        .from_reader(reader);
    let invalid = |err: csv::Error| read_error(format!("Invalid CSV: {err}"));
    let mut first = None;
    let names = if options.header {
        let headers = csv.headers().map_err(invalid)?;
        unique_names(
            &headers.iter().map(str::to_string).collect::<Vec<_>>(),
            None,
        )
    } else {
        let mut record = csv::StringRecord::new();
        let read = csv.read_record(&mut record).map_err(invalid)?;
        let width = record.len();
        if read {
            first = Some(record);
        }
        (1..=width).map(|i| format!("c{i}")).collect()
    };
    let (schema, map) = match schema {
        None => (Schema::Csv(names), None),
        Some(schema) => {
            let columns = schema.columns();
            let map = if options.header {
                let map = names
                    .iter()
                    .map(|name| {
                        columns
                            .iter()
                            .position(|column| column.eq_ignore_ascii_case(name))
                            .ok_or_else(|| {
                                read_error(format!(
                                    "The object '{key}' has a column '{name}' the first object does not."
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Some(map)
            } else {
                None
            };
            (schema.clone(), map)
        }
    };
    let rows = CsvRows {
        reader: csv,
        first,
        map,
        width: schema.columns().len(),
        progress,
    };
    Ok((schema, Box::new(rows)))
}

/// A JSON object's members, in the order written.
struct OrderedObject(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for OrderedObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Members;
        impl<'de> Visitor<'de> for Members {
            type Value = OrderedObject;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<OrderedObject, A::Error> {
                let mut members = Vec::new();
                while let Some(member) = map.next_entry::<String, Box<RawValue>>()? {
                    members.push(member);
                }
                Ok(OrderedObject(members))
            }
        }
        deserializer.deserialize_map(Members)
    }
}

fn invalid_json(err: serde_json::Error) -> SqlError {
    read_error(format!("Invalid JSON: {err}"))
}

fn is_object(raw: &RawValue) -> bool {
    raw.get().trim_start().starts_with('{')
}

/// A JSON value as a cell: a string's text, NULL, or the value's JSON text
/// as written (a number keeps every digit).
fn json_datum(raw: &RawValue) -> Result<Datum, SqlError> {
    let text = raw.get().trim();
    Ok(if text.starts_with('"') {
        Datum::NVarChar(serde_json::from_str::<String>(text).map_err(invalid_json)?)
    } else if text == "null" {
        Datum::Null
    } else {
        Datum::NVarChar(text.to_string())
    })
}

fn json_row(record: &RawValue, keys: &Option<Vec<String>>) -> Result<Vec<Datum>, SqlError> {
    match keys {
        None => Ok(vec![json_datum(record)?]),
        Some(keys) if is_object(record) => {
            let OrderedObject(members) =
                serde_json::from_str(record.get()).map_err(invalid_json)?;
            keys.iter()
                .map(|key| match members.iter().find(|(name, _)| name == key) {
                    Some((_, value)) => json_datum(value),
                    None => Ok(Datum::Null),
                })
                .collect()
        }
        // A record of another shape than the first ones: its fields are absent.
        Some(keys) => Ok(vec![Datum::Null; keys.len()]),
    }
}

fn json_rows(
    mut reader: BufReader<GuardedReader>,
    progress: Arc<Progress>,
    schema: Option<&Schema>,
) -> Result<(Schema, Rows), SqlError> {
    // A UTF-8 byte order mark is not part of the JSON.
    if reader
        .fill_buf()
        .map_err(|err| read_error(err.to_string()))?
        .starts_with(b"\xEF\xBB\xBF")
    {
        reader.consume(3);
    }
    let mut values = serde_json::Deserializer::from_reader(reader).into_iter::<Box<RawValue>>();
    let mut next_value = move || -> Option<Result<Box<RawValue>, SqlError>> {
        let value = values.next()?;
        progress
            .record_start
            .store(values.byte_offset() as u64, Ordering::Relaxed);
        Some(value.map_err(|err| read_error(format!("Invalid JSON: {err}"))))
    };
    // One top-level array (and nothing after it) holds the records;
    // otherwise every value is a record.
    let mut head: VecDeque<Box<RawValue>> = VecDeque::new();
    let mut elements: Option<std::vec::IntoIter<Box<RawValue>>> = None;
    if let Some(first) = next_value() {
        let first = first?;
        let second = next_value().transpose()?;
        match second {
            None if first.get().trim_start().starts_with('[') => {
                let items: Vec<Box<RawValue>> =
                    serde_json::from_str(first.get()).map_err(invalid_json)?;
                elements = Some(items.into_iter());
            }
            second => {
                head.push_back(first);
                head.extend(second);
            }
        }
    }
    let mut records: Box<dyn Iterator<Item = Result<Box<RawValue>, SqlError>> + Send> =
        match elements {
            Some(items) => Box::new(head.into_iter().map(Ok).chain(items.map(Ok))),
            None => Box::new(
                head.into_iter()
                    .map(Ok)
                    .chain(std::iter::from_fn(next_value)),
            ),
        };
    let schema = match schema {
        Some(schema) => schema.clone(),
        None => {
            // The column set comes from the first records, kept to be read
            // again first.
            let mut sample = Vec::new();
            for record in records.by_ref().take(SCHEMA_RECORDS) {
                sample.push(record?);
            }
            let schema = if !sample.is_empty() && sample.iter().all(|r| is_object(r)) {
                let mut keys: Vec<String> = Vec::new();
                for record in &sample {
                    let OrderedObject(members) =
                        serde_json::from_str(record.get()).map_err(invalid_json)?;
                    for (name, _) in members {
                        if !keys.contains(&name) {
                            keys.push(name);
                        }
                    }
                }
                Schema::Json(Some(keys))
            } else {
                Schema::Json(None)
            };
            records = Box::new(sample.into_iter().map(Ok).chain(records));
            schema
        }
    };
    let Schema::Json(keys) = schema.clone() else {
        unreachable!("a JSON read keeps a JSON schema");
    };
    let rows = records.map(move |record| record.and_then(|record| json_row(&record, &keys)));
    Ok((schema, Box::new(rows)))
}

/// One object's rows, and the schema they were read with: `schema` when one
/// is fixed already (prefix mode after the first object), else learnt here.
fn object_rows(
    reader: BodyReader,
    format: Format,
    options: &Options,
    schema: Option<&Schema>,
    key: &str,
) -> Result<(Schema, Rows), SqlError> {
    let progress = Arc::new(Progress::default());
    let reader = BufReader::with_capacity(
        READ_CHUNK,
        GuardedReader {
            inner: reader,
            progress: Arc::clone(&progress),
        },
    );
    match format {
        Format::Csv => csv_rows(reader, progress, options, schema, key),
        Format::Json => json_rows(reader, progress, schema),
        Format::Parquet => parquet_rows(reader.into_inner().inner, schema, key),
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
        "parquet" => Format::Parquet,
        other => {
            return Err(read_error(format!(
                "sys.s3_read reads 'csv', 'json' or 'parquet', not '{other}'."
            )));
        }
    };
    let options = match args.get(3) {
        None | Some(SqlValue::Null) => parse_options(None)?,
        Some(value) => parse_options(Some(&super::s3_sql::string_arg(name, value)?))?,
    };
    let prefix = options.prefix.then(|| key.clone());
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
        prefix,
    };
    if stream.prefix.is_some() {
        stream.advance()?;
    } else {
        stream.open_object(&key)?;
    }
    let mut columns = Vec::new();
    if stream.prefix.is_some() {
        columns.push(text_column(
            KEY_COLUMN,
            ColumnType::NVarChar { max_len: 1024 },
        ));
    }
    if let Some(schema) = &stream.schema {
        let names = schema.columns();
        if names.is_empty() {
            return Err(read_error(format!(
                "The object '{key}' has no columns to read."
            )));
        }
        let reserved = stream.prefix.is_some().then_some(KEY_COLUMN);
        columns.extend(
            unique_names(&names, reserved)
                .iter()
                .zip(schema.types())
                .map(|(name, column_type)| text_column(name, column_type)),
        );
    }
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
    /// Prefix mode: the prefix.
    prefix: Option<String>,
}

impl ReadStream {
    /// Prefix mode: the next current, non-empty key, listing a slice of keys
    /// per lock hold as needed. An empty object (a "folder" marker) has no
    /// rows to give.
    fn next_key(&mut self) -> Result<Option<String>, SqlError> {
        let prefix = self.prefix.clone().expect("prefix mode");
        while self.pending.is_empty() && !self.listing_done {
            let listing = {
                let mut guard = self.core.lock();
                let mut file =
                    ObjFile::try_new(&mut guard).ok_or_else(super::s3_sql::no_object_engine)?;
                let after = self.listed_after.as_deref().map(ListAfter::Key);
                file.list_objects(&self.bucket, &prefix, None, after, LIST_KEYS)
                    .map_err(super::s3_sql::storage_error)?
                    .map_err(|err| super::s3_sql::object_error(err, &self.bucket))?
            };
            self.pending.extend(
                listing
                    .objects
                    .into_iter()
                    .filter(|object| object.info.size > 0)
                    .map(|object| object.key),
            );
            match (listing.truncated, listing.last) {
                (true, Some((last, _))) => self.listed_after = Some(last),
                _ => self.listing_done = true,
            }
        }
        Ok(self.pending.pop_front())
    }

    /// Prefix mode: opens the next object that can be read, skipping any
    /// deleted since it was listed. Leaves nothing open at the end.
    fn advance(&mut self) -> Result<(), SqlError> {
        self.current = None;
        while let Some(key) = self.next_key()? {
            // Many skipped objects in a row stay cancellable.
            check_cancelled()?;
            match self.open_object(&key) {
                Ok(()) => return Ok(()),
                Err(err) if err.number == 208 => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    fn open_object(&mut self, key: &str) -> Result<(), SqlError> {
        // Drop the previous object's reader (and its lease) first.
        self.current = None;
        let reader = BodyReader::open(&self.core, &self.bucket, key)?;
        let (schema, rows) = object_rows(
            reader,
            self.format,
            &self.options,
            self.schema.as_ref(),
            key,
        )?;
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
                None if self.prefix.is_some() => self.advance()?,
                None => self.current = None,
            }
        }
        if self.remaining == Some(0) {
            // Done early: release the body now rather than at drop.
            self.current = None;
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}

/// Decompressed bytes the pages of one Parquet read may hold at once. Every
/// leaf column holds a page (and its dictionary page) while rows are
/// assembled, so one page may take this much over the number of leaves, but
/// never less than [`MIN_PARQUET_PAGE`] (writers cut pages at about 1 MiB).
const PARQUET_PAGE_BUDGET: usize = 256 << 20;
const MIN_PARQUET_PAGE: usize = 1 << 20;
/// The most leaf columns a Parquet file read here may have.
const MAX_PARQUET_LEAVES: usize = 1024;
/// The deepest a Parquet column may nest.
const MAX_PARQUET_DEPTH: usize = 32;
/// The largest footer (file metadata) read.
const MAX_PARQUET_METADATA: usize = 64 << 20;
/// The longest page header read.
const MAX_PAGE_HEADER: u64 = 1 << 20;
/// Bytes a page header read fetches per storage-lock hold.
const PAGE_HEADER_READ: usize = 16 << 10;

/// A leased body as a Parquet source: positioned reads, each a chunk per
/// storage-lock hold. The lease goes when the last reader of it does.
///
/// The Parquet reader reads each page's header through [`ParquetFile::get_read`]
/// and then fetches the page with [`ParquetFile::get_bytes`]. The fetch parses
/// that header itself first and refuses a page whose decompressed size is
/// over the read's page cap, so a small file cannot make the reader allocate
/// gigabytes. Until the file's metadata has been read (`pages` is false),
/// reads are of the footer and the metadata.
struct ParquetSource {
    state: std::sync::Mutex<SourceState>,
    size: u64,
}

struct SourceState {
    reader: BodyReader,
    /// Where the last page header read started, until its page is fetched.
    header_at: Option<u64>,
    /// Whether the metadata has been read: every read after that is a page
    /// header or its page.
    pages: bool,
    /// The largest page this read may decompress.
    page_cap: usize,
}

fn parquet_failure(message: impl Into<String>) -> parquet::errors::ParquetError {
    parquet::errors::ParquetError::General(message.into())
}

impl ParquetSource {
    fn state(&self) -> std::sync::MutexGuard<'_, SourceState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Fills `buf` from `pos`, which with `buf` must lie within the object.
    fn read_exact_at(state: &mut SourceState, pos: u64, buf: &mut [u8]) -> std::io::Result<()> {
        state.reader.pos = pos;
        state.reader.read_exact(buf)
    }
}

#[derive(Clone)]
struct ParquetFile(Arc<ParquetSource>);

/// A page header read: buffered, so the Parquet reader's byte-at-a-time
/// header parsing takes a storage-lock hold per [`PAGE_HEADER_READ`] bytes,
/// not per byte.
struct ParquetRead {
    source: Arc<ParquetSource>,
    pos: u64,
    buf: Vec<u8>,
    at: usize,
}

impl Read for ParquetRead {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.at == self.buf.len() {
            let len = (self.source.size.saturating_sub(self.pos)).min(PAGE_HEADER_READ as u64);
            self.buf = vec![0; len as usize];
            self.at = 0;
            ParquetSource::read_exact_at(&mut self.source.state(), self.pos, &mut self.buf)?;
            self.pos += len;
        }
        let n = out.len().min(self.buf.len() - self.at);
        out[..n].copy_from_slice(&self.buf[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

impl parquet::file::reader::Length for ParquetFile {
    fn len(&self) -> u64 {
        self.0.size
    }
}

impl parquet::file::reader::ChunkReader for ParquetFile {
    type T = ParquetRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<ParquetRead> {
        let mut state = self.0.state();
        if state.pages {
            state.header_at = Some(start);
        }
        Ok(ParquetRead {
            source: Arc::clone(&self.0),
            pos: start,
            buf: Vec::new(),
            at: 0,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.0.size)
        {
            return Err(parquet::errors::ParquetError::EOF(
                "a read past the end of the object".to_string(),
            ));
        }
        let mut state = self.0.state();
        match state.header_at.take() {
            Some(header) if header < start && start - header <= MAX_PAGE_HEADER => {
                let mut bytes = vec![0; (start - header) as usize];
                ParquetSource::read_exact_at(&mut state, header, &mut bytes)?;
                let (uncompressed, compressed) = page_sizes(&bytes)
                    .ok_or_else(|| parquet_failure("a page header that cannot be read"))?;
                if compressed != length as u64 {
                    return Err(parquet_failure("a page read that is not its header's page"));
                }
                if uncompressed.max(compressed) > state.page_cap as u64 {
                    return Err(parquet_failure(format!(
                        "a page of {uncompressed} bytes is larger than this file's limit of {} bytes",
                        state.page_cap
                    )));
                }
            }
            Some(_) => return Err(parquet_failure("a page read that is not its header's page")),
            None if state.pages => {
                return Err(parquet_failure("a read that is not a page"));
            }
            None if length > MAX_PARQUET_METADATA => {
                return Err(parquet_failure(format!(
                    "file metadata of {length} bytes is larger than 64 MiB"
                )));
            }
            None => {}
        }
        let mut out = vec![0u8; length];
        ParquetSource::read_exact_at(&mut state, start, &mut out)?;
        Ok(out.into())
    }
}

/// A Thrift compact-protocol reader over a page header, enough to find its
/// sizes and step over everything else.
struct Thrift<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Thrift<'_> {
    fn byte(&mut self) -> Option<u8> {
        let byte = *self.bytes.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    fn zigzag(&mut self) -> Option<i64> {
        let value = self.varint()?;
        Some((value >> 1) as i64 ^ -((value & 1) as i64))
    }

    fn advance(&mut self, len: u64) -> Option<()> {
        let end = self.at.checked_add(usize::try_from(len).ok()?)?;
        (end <= self.bytes.len()).then(|| self.at = end)
    }

    /// Steps over a value of compact type `kind`; a boolean is a byte only as
    /// a list or map element (a field carries it in its type).
    fn skip(&mut self, kind: u8, element: bool, depth: usize) -> Option<()> {
        match kind {
            1 | 2 if element => self.byte().map(drop),
            1 | 2 => Some(()),
            3 => self.byte().map(drop),
            4..=6 => self.varint().map(drop),
            7 => self.advance(8),
            8 => {
                let len = self.varint()?;
                self.advance(len)
            }
            9 | 10 => {
                let head = self.byte()?;
                let count = match head >> 4 {
                    15 => self.varint()?,
                    count => u64::from(count),
                };
                // Every element takes at least a byte, so a lying count runs
                // out of input rather than looping on.
                for _ in 0..count {
                    self.skip(head & 0x0F, true, depth)?;
                }
                Some(())
            }
            11 => {
                let count = self.varint()?;
                if count > 0 {
                    let kinds = self.byte()?;
                    for _ in 0..count {
                        self.skip(kinds >> 4, true, depth)?;
                        self.skip(kinds & 0x0F, true, depth)?;
                    }
                }
                Some(())
            }
            12 if depth < MAX_PARQUET_DEPTH => self.fields(depth + 1, |_, _, _| None),
            _ => None,
        }
    }

    /// Reads a struct's fields to its end: `field` sees each (id, type) first
    /// and returns `Some(())` when it consumed the value itself.
    fn fields(
        &mut self,
        depth: usize,
        mut field: impl FnMut(&mut Self, i64, u8) -> Option<()>,
    ) -> Option<()> {
        let mut id = 0i64;
        loop {
            let head = self.byte()?;
            if head == 0 {
                return Some(());
            }
            let kind = head & 0x0F;
            id = match head >> 4 {
                0 => self.zigzag()?,
                delta => id + i64::from(delta),
            };
            if field(self, id, kind).is_none() {
                self.skip(kind, false, depth)?;
            }
        }
    }
}

/// A page header's uncompressed and compressed sizes, when `bytes` holds
/// exactly one well-formed header carrying both. A size given twice counts
/// as its last value, as the Parquet reader takes it.
fn page_sizes(bytes: &[u8]) -> Option<(u64, u64)> {
    let mut thrift = Thrift { bytes, at: 0 };
    let (mut uncompressed, mut compressed) = (None, None);
    let mut bad = false;
    thrift.fields(0, |thrift, id, kind| {
        if !matches!(id, 2 | 3) || kind != 5 {
            return None;
        }
        let value = thrift.zigzag().and_then(|v| u64::try_from(v).ok());
        bad |= value.is_none();
        if id == 2 {
            uncompressed = value;
        } else {
            compressed = value;
        }
        Some(())
    })?;
    (!bad && thrift.at == bytes.len()).then_some(())?;
    Some((uncompressed?, compressed?))
}

fn parquet_error(err: parquet::errors::ParquetError) -> SqlError {
    read_error(format!("Invalid Parquet: {err}"))
}

/// Runs a step of the Parquet reader, turning a panic in it into an error:
/// the reader has panicking paths for inputs it does not expect, and a
/// file's bytes are whatever its uploader chose.
fn parquet_guarded<T>(step: impl FnOnce() -> Result<T, SqlError>) -> Result<T, SqlError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(step))
        .unwrap_or_else(|_| Err(read_error("Invalid Parquet: the file cannot be read.")))
}

/// Refuses a leaf column the record reader cannot convert (it would panic),
/// or one nested deeper than [`MAX_PARQUET_DEPTH`].
fn check_parquet_leaf(leaf: &parquet::schema::types::ColumnDescriptor) -> Result<(), SqlError> {
    use parquet::basic::{ConvertedType as C, Type as P};
    let readable = match leaf.physical_type() {
        P::BOOLEAN | P::INT96 | P::FLOAT | P::DOUBLE => true,
        P::INT32 => matches!(
            leaf.converted_type(),
            C::NONE
                | C::INT_8
                | C::INT_16
                | C::INT_32
                | C::UINT_8
                | C::UINT_16
                | C::UINT_32
                | C::DATE
                | C::TIME_MILLIS
                | C::DECIMAL
        ),
        P::INT64 => matches!(
            leaf.converted_type(),
            C::NONE
                | C::INT_64
                | C::UINT_64
                | C::TIME_MICROS
                | C::TIMESTAMP_MILLIS
                | C::TIMESTAMP_MICROS
                | C::DECIMAL
        ),
        P::BYTE_ARRAY => matches!(
            leaf.converted_type(),
            C::NONE | C::UTF8 | C::ENUM | C::JSON | C::BSON | C::DECIMAL
        ),
        P::FIXED_LEN_BYTE_ARRAY => matches!(leaf.converted_type(), C::NONE | C::DECIMAL),
    };
    if !readable {
        return Err(read_error(format!(
            "The Parquet column '{}' is {} {}, which sys.s3_read does not read.",
            leaf.path(),
            leaf.physical_type(),
            leaf.converted_type()
        )));
    }
    if leaf.path().parts().len() > MAX_PARQUET_DEPTH {
        return Err(read_error(format!(
            "The Parquet column '{}' nests deeper than {MAX_PARQUET_DEPTH} levels.",
            leaf.path()
        )));
    }
    Ok(())
}

/// Whether a leaf holds nanoseconds as a plain INT64 (a TIME or TIMESTAMP
/// logical type with no converted type).
fn nanos_unit(leaf: &parquet::schema::types::ColumnDescriptor) -> Option<ColumnType> {
    use parquet::basic::{LogicalType as L, TimeUnit};
    match leaf.logical_type_ref()? {
        L::Timestamp(t) if t.unit == TimeUnit::NANOS => Some(ColumnType::DateTime2),
        L::Time(t) if t.unit == TimeUnit::NANOS => Some(ColumnType::Time),
        _ => None,
    }
}

/// A top-level Parquet field's column type: from its leaf's physical and
/// converted (or logical) type; a group or a repeated field is its JSON text.
fn parquet_column_type(
    field: &parquet::schema::types::Type,
    leaf: Option<&parquet::schema::types::ColumnDescriptor>,
) -> ColumnType {
    use parquet::basic::{ConvertedType as C, LogicalType, Repetition, Type as P};
    let (Some(leaf), true) = (
        leaf,
        field.is_primitive() && field.get_basic_info().repetition() != Repetition::REPEATED,
    ) else {
        return ColumnType::NVarCharMax;
    };
    let decimal = || {
        let (precision, scale) = (leaf.type_precision(), leaf.type_scale());
        if (1..=38).contains(&precision) && (0..=precision).contains(&scale) {
            ColumnType::Decimal {
                precision: precision as u8,
                scale: scale as u8,
            }
        } else {
            ColumnType::NVarCharMax
        }
    };
    match (leaf.physical_type(), leaf.converted_type()) {
        (_, C::DECIMAL) => decimal(),
        (P::BOOLEAN, _) => ColumnType::Bit,
        (P::INT32, C::INT_8 | C::INT_16) => ColumnType::SmallInt,
        (P::INT32, C::UINT_8) => ColumnType::TinyInt,
        (P::INT32, C::UINT_16) => ColumnType::Int,
        (P::INT32, C::UINT_32) => ColumnType::BigInt,
        (P::INT32, C::DATE) => ColumnType::Date,
        (P::INT32, C::TIME_MILLIS) => ColumnType::Time,
        (P::INT32, _) => ColumnType::Int,
        (P::INT64, C::UINT_64) => ColumnType::Decimal {
            precision: 20,
            scale: 0,
        },
        (P::INT64, C::TIMESTAMP_MILLIS | C::TIMESTAMP_MICROS) => ColumnType::DateTime2,
        (P::INT64, C::TIME_MICROS) => ColumnType::Time,
        (P::INT64, _) => nanos_unit(leaf).unwrap_or(ColumnType::BigInt),
        (P::INT96, _) => ColumnType::DateTime2,
        (P::FLOAT, _) => ColumnType::Real,
        (P::DOUBLE, _) => ColumnType::Float,
        (P::BYTE_ARRAY, C::UTF8 | C::ENUM | C::JSON) => ColumnType::NVarCharMax,
        (P::FIXED_LEN_BYTE_ARRAY, _) if leaf.logical_type_ref() == Some(&LogicalType::Float16) => {
            ColumnType::Real
        }
        (P::BYTE_ARRAY | P::FIXED_LEN_BYTE_ARRAY, _) => ColumnType::VarBinaryMax,
    }
}

const TICKS_PER_DAY: i64 = 864_000_000_000;
/// 0001-01-01 to 1970-01-01.
const DAYS_TO_1970: i64 = 719_162;
/// 0001-01-01 to 9999-12-31.
const LAST_DAY: i64 = 3_652_058;

/// Days since 1970 as days since 0001-01-01, within 0001-01-01..9999-12-31.
fn parquet_day(days: i64) -> Result<u32, SqlError> {
    days.checked_add(DAYS_TO_1970)
        .filter(|day| (0..=LAST_DAY).contains(day))
        .map(|day| day as u32)
        .ok_or_else(|| read_error(format!("A date {days} days from 1970 is out of range.")))
}

/// `count` units of `ticks_per_unit` 100 ns ticks since 1970 (or of
/// 1 / `units_per_tick` tick) as a day and a time of day.
fn parquet_instant(
    count: i64,
    ticks_per_unit: i64,
    units_per_tick: i64,
) -> Result<(u32, u64), SqlError> {
    let ticks = count
        .checked_mul(ticks_per_unit)
        .map(|ticks| ticks.div_euclid(units_per_tick))
        .ok_or_else(|| read_error(format!("A timestamp {count} is out of range.")))?;
    let day = parquet_day(ticks.div_euclid(TICKS_PER_DAY))?;
    Ok((day, ticks.rem_euclid(TICKS_PER_DAY) as u64))
}

/// `count` units (as for [`parquet_instant`]) since midnight, within a day.
fn parquet_time(count: i64, ticks_per_unit: i64, units_per_tick: i64) -> Result<u64, SqlError> {
    count
        .checked_mul(ticks_per_unit)
        .map(|ticks| ticks.div_euclid(units_per_tick))
        .filter(|ticks| (0..TICKS_PER_DAY).contains(ticks))
        .map(|ticks| ticks as u64)
        .ok_or_else(|| read_error(format!("A time of day {count} is out of range.")))
}

/// A Parquet decimal's unscaled value.
fn parquet_decimal(decimal: &parquet::data_type::Decimal) -> Result<i128, SqlError> {
    let bytes = decimal.data();
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(read_error(format!(
            "A Parquet decimal of {} bytes is out of range.",
            bytes.len()
        )));
    }
    // Big-endian two's complement, sign-extended to 128 bits.
    let fill = if bytes[0] & 0x80 != 0 { 0xFF } else { 0 };
    let mut wide = [fill; 16];
    wide[16 - bytes.len()..].copy_from_slice(bytes);
    Ok(i128::from_be_bytes(wide))
}

/// `value` as a `decimal(precision, _)`: it may have at most `precision`
/// digits.
fn decimal_datum(value: i128, precision: u8) -> Result<Datum, SqlError> {
    if value.unsigned_abs() >= 10u128.pow(u32::from(precision)) {
        return Err(read_error(format!(
            "A Parquet decimal {value} has more than {precision} digits."
        )));
    }
    Ok(Datum::Decimal(value))
}

/// A Parquet value as a cell of `column_type`.
fn parquet_datum(
    field: &parquet::record::Field,
    column_type: ColumnType,
) -> Result<Datum, SqlError> {
    use parquet::record::Field as F;
    let mismatch = || {
        read_error(format!(
            "A Parquet value {field} does not fit its column's type."
        ))
    };
    Ok(match (column_type, field) {
        (_, F::Null) => Datum::Null,
        (ColumnType::Bit, F::Bool(v)) => Datum::Bit(*v),
        (ColumnType::TinyInt, F::UByte(v)) => Datum::TinyInt(*v),
        (ColumnType::SmallInt, F::Byte(v)) => Datum::SmallInt(*v as i16),
        (ColumnType::SmallInt, F::Short(v)) => Datum::SmallInt(*v),
        (ColumnType::Int, F::Int(v)) => Datum::Int(*v),
        (ColumnType::Int, F::UShort(v)) => Datum::Int(*v as i32),
        (ColumnType::BigInt, F::Long(v)) => Datum::BigInt(*v),
        (ColumnType::BigInt, F::UInt(v)) => Datum::BigInt(*v as i64),
        (ColumnType::Decimal { .. }, F::ULong(v)) => Datum::Decimal(*v as i128),
        (ColumnType::Decimal { precision, .. }, F::Decimal(d)) => {
            decimal_datum(parquet_decimal(d)?, precision)?
        }
        (ColumnType::Real, F::Float(v)) => Datum::Real(*v),
        (ColumnType::Real, F::Float16(v)) => Datum::Real(f32::from(*v)),
        (ColumnType::Float, F::Double(v)) => Datum::Float(*v),
        (ColumnType::Date, F::Date(days)) => Datum::Date(parquet_day(i64::from(*days))?),
        (ColumnType::Time, F::TimeMillis(ms)) => {
            Datum::Time(parquet_time(i64::from(*ms), 10_000, 1)?)
        }
        (ColumnType::Time, F::TimeMicros(us)) => Datum::Time(parquet_time(*us, 10, 1)?),
        // A plain INT64 in a time or timestamp column holds nanoseconds.
        (ColumnType::Time, F::Long(ns)) => Datum::Time(parquet_time(*ns, 1, 100)?),
        (ColumnType::DateTime2, F::TimestampMillis(ms)) => {
            let (day, ticks) = parquet_instant(*ms, 10_000, 1)?;
            Datum::DateTime2(day, ticks)
        }
        (ColumnType::DateTime2, F::TimestampMicros(us)) => {
            let (day, ticks) = parquet_instant(*us, 10, 1)?;
            Datum::DateTime2(day, ticks)
        }
        (ColumnType::DateTime2, F::Long(ns)) => {
            let (day, ticks) = parquet_instant(*ns, 1, 100)?;
            Datum::DateTime2(day, ticks)
        }
        (ColumnType::VarBinaryMax, F::Bytes(bytes)) => Datum::VarBinary(bytes.data().to_vec()),
        (ColumnType::NVarCharMax, F::Str(text)) => Datum::NVarChar(text.clone()),
        (ColumnType::NVarCharMax, other) => {
            let mut text = String::new();
            parquet_json(other, &mut text)?;
            Datum::NVarChar(text)
        }
        _ => return Err(mismatch()),
    })
}

/// Appends a Parquet value's JSON text: groups are objects, lists arrays,
/// maps objects keyed by their keys' text; binary is base64, dates and times
/// are strings, and a non-finite float is null.
fn parquet_json(field: &parquet::record::Field, out: &mut String) -> Result<(), SqlError> {
    use base64::Engine as _;
    use parquet::record::Field as F;
    use std::fmt::Write as _;
    let string = |text: &str, out: &mut String| {
        out.push_str(&serde_json::to_string(text).expect("a string is JSON"));
    };
    let float = |v: f64, out: &mut String| {
        if v.is_finite() {
            let _ = write!(out, "{v}");
        } else {
            out.push_str("null");
        }
    };
    match field {
        F::Null => out.push_str("null"),
        F::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        F::Byte(v) => _ = write!(out, "{v}"),
        F::Short(v) => _ = write!(out, "{v}"),
        F::Int(v) => _ = write!(out, "{v}"),
        F::Long(v) => _ = write!(out, "{v}"),
        F::UByte(v) => _ = write!(out, "{v}"),
        F::UShort(v) => _ = write!(out, "{v}"),
        F::UInt(v) => _ = write!(out, "{v}"),
        F::ULong(v) => _ = write!(out, "{v}"),
        F::Float16(v) => float(f64::from(*v), out),
        F::Float(v) => float(f64::from(*v), out),
        F::Double(v) => float(*v, out),
        F::Decimal(d) => {
            let value = parquet_decimal(d)?;
            let scale = usize::try_from(d.scale())
                .ok()
                .filter(|&scale| scale <= 38)
                .ok_or_else(|| {
                    read_error(format!(
                        "A Parquet decimal of scale {} is out of range.",
                        d.scale()
                    ))
                })?;
            let digits = format!("{:0>width$}", value.unsigned_abs(), width = scale + 1);
            let (whole, fraction) = digits.split_at(digits.len() - scale);
            if value < 0 {
                out.push('-');
            }
            out.push_str(whole);
            if !fraction.is_empty() {
                out.push('.');
                out.push_str(fraction);
            }
        }
        F::Str(text) => string(text, out),
        F::Bytes(bytes) => string(
            &base64::engine::general_purpose::STANDARD.encode(bytes.data()),
            out,
        ),
        F::Date(days) => string(
            &truthdb_sql::temporal::render_date(parquet_day(i64::from(*days))?),
            out,
        ),
        F::TimeMillis(ms) => string(
            &truthdb_sql::temporal::render_time(parquet_time(i64::from(*ms), 10_000, 1)?),
            out,
        ),
        F::TimeMicros(us) => string(
            &truthdb_sql::temporal::render_time(parquet_time(*us, 10, 1)?),
            out,
        ),
        F::TimestampMillis(ms) => {
            let (day, ticks) = parquet_instant(*ms, 10_000, 1)?;
            string(&truthdb_sql::temporal::render_datetime2(day, ticks), out);
        }
        F::TimestampMicros(us) => {
            let (day, ticks) = parquet_instant(*us, 10, 1)?;
            string(&truthdb_sql::temporal::render_datetime2(day, ticks), out);
        }
        F::Group(row) => {
            out.push('{');
            for (index, (name, value)) in row.get_column_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                string(name, out);
                out.push(':');
                parquet_json(value, out)?;
            }
            out.push('}');
        }
        F::ListInternal(list) => {
            out.push('[');
            for (index, value) in list.elements().iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                parquet_json(value, out)?;
            }
            out.push(']');
        }
        F::MapInternal(map) => {
            out.push('{');
            for (index, (key, value)) in map.entries().iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match key {
                    F::Str(text) => string(text, out),
                    other => {
                        let mut text = String::new();
                        parquet_json(other, &mut text)?;
                        string(&text, out);
                    }
                }
                out.push(':');
                parquet_json(value, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// A Parquet body's rows: typed columns from its schema (`schema`, when one
/// is fixed already, by column name).
fn parquet_rows(
    reader: BodyReader,
    schema: Option<&Schema>,
    key: &str,
) -> Result<(Schema, Rows), SqlError> {
    use parquet::file::reader::FileReader;
    let size = reader.size;
    let source = Arc::new(ParquetSource {
        state: std::sync::Mutex::new(SourceState {
            reader,
            header_at: None,
            pages: false,
            page_cap: 0,
        }),
        size,
    });
    let parquet = parquet_guarded(|| {
        parquet::file::serialized_reader::SerializedFileReader::new(ParquetFile(Arc::clone(
            &source,
        )))
        .map_err(parquet_error)
    })?;
    let descr = parquet.metadata().file_metadata().schema_descr_ptr();
    let leaves = descr.columns();
    if leaves.len() > MAX_PARQUET_LEAVES {
        return Err(read_error(format!(
            "The Parquet file has {} leaf columns, more than {MAX_PARQUET_LEAVES}.",
            leaves.len()
        )));
    }
    let mut paths = std::collections::HashSet::new();
    for leaf in leaves {
        check_parquet_leaf(leaf)?;
        // The record reader finds a leaf's values by its path, so two leaves
        // of one path would both read the same values.
        if !paths.insert(leaf.path()) {
            return Err(read_error(format!(
                "The Parquet file has two columns named '{}'.",
                leaf.path()
            )));
        }
    }
    {
        let mut state = source.state();
        state.page_cap = (PARQUET_PAGE_BUDGET / leaves.len().max(1)).max(MIN_PARQUET_PAGE);
        state.pages = true;
    }
    let fields: Vec<(String, ColumnType)> = descr
        .root_schema()
        .get_fields()
        .iter()
        .enumerate()
        .map(|(root, field)| {
            // A primitive top-level field is the one leaf under it.
            let leaf = (0..leaves.len())
                .find(|&leaf| descr.get_column_root_idx(leaf) == root)
                .map(|leaf| leaves[leaf].as_ref());
            (field.name().to_string(), parquet_column_type(field, leaf))
        })
        .collect();
    let (schema, map) = match schema {
        None => (Schema::Parquet(fields.clone()), None),
        Some(schema) => {
            let columns = schema.columns();
            let types = schema.types();
            let mut taken = vec![false; columns.len()];
            let map = fields
                .iter()
                .map(|(name, column_type)| {
                    // The same name first, else one that differs only in case.
                    let index = columns
                        .iter()
                        .position(|column| column == name)
                        .or_else(|| {
                            columns.iter().enumerate().position(|(index, column)| {
                                !taken[index] && column.eq_ignore_ascii_case(name)
                            })
                        })
                        .filter(|&index| !taken[index])
                        .ok_or_else(|| {
                            read_error(format!(
                                "The object '{key}' has a column '{name}' the first object does not."
                            ))
                        })?;
                    taken[index] = true;
                    if types[index] != *column_type {
                        return Err(read_error(format!(
                            "The object '{key}' has column '{name}' of another type than the first object's."
                        )));
                    }
                    Ok(index)
                })
                .collect::<Result<Vec<_>, _>>()?;
            (schema.clone(), Some(map))
        }
    };
    let types = schema.types();
    let mut iter = parquet::record::reader::RowIter::from_file_into(Box::new(parquet));
    let mut failed = false;
    let rows = std::iter::from_fn(move || {
        if failed {
            return None;
        }
        let row = parquet_guarded(|| {
            let Some(row) = iter.next() else {
                return Ok(None);
            };
            let row = row.map_err(parquet_error)?;
            let mut out = vec![Datum::Null; types.len()];
            for (index, (_, field)) in row.get_column_iter().enumerate() {
                let column = match &map {
                    Some(map) => map[index],
                    None => index,
                };
                out[column] = parquet_datum(field, types[column])?;
            }
            Ok(Some(out))
        });
        failed = row.is_err();
        row.transpose()
    });
    Ok((schema, Box::new(rows)))
}

#[cfg(test)]
mod tests {
    use super::page_sizes;

    #[test]
    fn page_sizes_are_read_from_a_compact_header_and_anything_odd_is_refused() {
        // type = 0 (i32), uncompressed = 100, compressed = 60, then a
        // nested data page header struct with an i32, a bool and a list.
        let header = [
            0x15, 0x00, // field 1, i32 0
            0x15, 0xC8, 0x01, // field 2, i32 100
            0x15, 0x78, // field 3, i32 60
            0x2C, // field 5, struct
            0x15, 0x04, 0x11, 0x19, 0x25, 0x02, 0x04, 0x00, // i32, bool, list<i32>
            0x00,
        ];
        assert_eq!(page_sizes(&header), Some((100, 60)));
        // Trailing bytes, a cut-short header, a missing size, a size given
        // again larger, and a negative size are all refused or read last.
        assert_eq!(page_sizes(&[&header[..], &[0]].concat()), None);
        assert_eq!(page_sizes(&header[..header.len() - 1]), None);
        assert_eq!(page_sizes(&[0x15, 0x00, 0x15, 0xC8, 0x01, 0x00]), None);
        assert_eq!(
            page_sizes(&[
                0x25, 0xC8, 0x01, 0x15, 0x78, 0x05, 0x04, 0xFE, 0xFF, 0x03, 0x00
            ]),
            Some((32767, 60))
        );
        assert_eq!(page_sizes(&[0x25, 0x01, 0x15, 0x78, 0x00]), None);
    }
}
