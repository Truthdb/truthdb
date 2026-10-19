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
//!   binary); nested groups and lists are their JSON text. A later file in
//!   prefix mode is matched by column name. Snappy, gzip, LZ4 and Brotli
//!   pages are read; Zstandard is not.
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

/// Largest single read the Parquet reader may ask for (a column chunk).
const MAX_PARQUET_READ: usize = 256 * 1024 * 1024;

/// A leased body as a Parquet source: positioned reads, each a chunk per
/// storage-lock hold. The lease goes when the last reader of it does.
struct ParquetSource {
    reader: std::sync::Mutex<BodyReader>,
    size: u64,
}

impl ParquetSource {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut reader = self.reader.lock().expect("parquet source poisoned");
        reader.pos = pos;
        reader.read(buf)
    }
}

#[derive(Clone)]
struct ParquetFile(Arc<ParquetSource>);

struct ParquetRead {
    source: Arc<ParquetSource>,
    pos: u64,
}

impl Read for ParquetRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.source.read_at(self.pos, buf)?;
        self.pos += n as u64;
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
        Ok(ParquetRead {
            source: Arc::clone(&self.0),
            pos: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        if length > MAX_PARQUET_READ {
            return Err(parquet::errors::ParquetError::General(format!(
                "a column chunk of {length} bytes is larger than 256 MiB"
            )));
        }
        let mut out = vec![0u8; length];
        let mut done = 0;
        while done < length {
            let n = self.0.read_at(start + done as u64, &mut out[done..])?;
            if n == 0 {
                return Err(parquet::errors::ParquetError::EOF(
                    "the object ended inside the file".to_string(),
                ));
            }
            done += n;
        }
        Ok(out.into())
    }
}

fn parquet_error(err: parquet::errors::ParquetError) -> SqlError {
    read_error(format!("Invalid Parquet: {err}"))
}

/// A top-level Parquet field's column type: from its leaf's physical and
/// converted type; a group or a repeated field is its JSON text.
fn parquet_column_type(
    field: &parquet::schema::types::Type,
    leaf: Option<&parquet::schema::types::ColumnDescriptor>,
) -> ColumnType {
    use parquet::basic::{ConvertedType as C, Repetition, Type as P};
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
        (P::INT64, _) => ColumnType::BigInt,
        (P::INT96, _) => ColumnType::DateTime2,
        (P::FLOAT, _) => ColumnType::Real,
        (P::DOUBLE, _) => ColumnType::Float,
        (P::BYTE_ARRAY, C::UTF8 | C::ENUM | C::JSON) => ColumnType::NVarCharMax,
        (P::BYTE_ARRAY | P::FIXED_LEN_BYTE_ARRAY, _) => ColumnType::VarBinaryMax,
    }
}

const TICKS_PER_DAY: i64 = 864_000_000_000;

/// Days since 1970 as days since 0001-01-01.
fn date_datum(days: i64) -> Result<Datum, SqlError> {
    u32::try_from(days + 719_162)
        .map(Datum::Date)
        .map_err(|_| read_error(format!("A date {days} days from 1970 is out of range.")))
}

/// 100 ns ticks since 1970 as a `datetime2`.
fn datetime_datum(ticks: i64) -> Result<Datum, SqlError> {
    let days = ticks.div_euclid(TICKS_PER_DAY);
    let Datum::Date(day) = date_datum(days)? else {
        unreachable!("date_datum makes a date");
    };
    Ok(Datum::DateTime2(
        day,
        ticks.rem_euclid(TICKS_PER_DAY) as u64,
    ))
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
        (ColumnType::Decimal { .. }, F::Decimal(d)) => {
            let bytes = d.data();
            if bytes.is_empty() || bytes.len() > 16 {
                return Err(mismatch());
            }
            // Big-endian two's complement, sign-extended to 128 bits.
            let fill = if bytes[0] & 0x80 != 0 { 0xFF } else { 0 };
            let mut wide = [fill; 16];
            wide[16 - bytes.len()..].copy_from_slice(bytes);
            Datum::Decimal(i128::from_be_bytes(wide))
        }
        (ColumnType::Real, F::Float(v)) => Datum::Real(*v),
        (ColumnType::Real, F::Float16(v)) => Datum::Real(f32::from(*v)),
        (ColumnType::Float, F::Double(v)) => Datum::Float(*v),
        (ColumnType::Date, F::Date(days)) => date_datum(*days as i64)?,
        (ColumnType::Time, F::TimeMillis(ms)) => Datum::Time(*ms as u64 * 10_000),
        (ColumnType::Time, F::TimeMicros(us)) => Datum::Time(*us as u64 * 10),
        (ColumnType::DateTime2, F::TimestampMillis(ms)) => datetime_datum(ms * 10_000)?,
        (ColumnType::DateTime2, F::TimestampMicros(us)) => datetime_datum(us * 10)?,
        (ColumnType::VarBinaryMax, F::Bytes(bytes)) => Datum::VarBinary(bytes.data().to_vec()),
        (ColumnType::NVarCharMax, F::Str(text)) => Datum::NVarChar(text.clone()),
        (ColumnType::NVarCharMax, F::Group(row)) => {
            Datum::NVarChar(row.to_json_value().to_string())
        }
        (ColumnType::NVarCharMax, other) => Datum::NVarChar(other.to_json_value().to_string()),
        _ => return Err(mismatch()),
    })
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
    let file = ParquetFile(Arc::new(ParquetSource {
        reader: std::sync::Mutex::new(reader),
        size,
    }));
    let parquet =
        parquet::file::serialized_reader::SerializedFileReader::new(file).map_err(parquet_error)?;
    let descr = parquet.metadata().file_metadata().schema_descr_ptr();
    let fields: Vec<(String, ColumnType)> = descr
        .root_schema()
        .get_fields()
        .iter()
        .map(|field| {
            let leaf = descr.columns().iter().find(|column| {
                column.path().parts().len() == 1 && column.path().parts()[0] == field.name()
            });
            (
                field.name().to_string(),
                parquet_column_type(field, leaf.map(|leaf| leaf.as_ref())),
            )
        })
        .collect();
    let (schema, map) = match schema {
        None => (Schema::Parquet(fields.clone()), None),
        Some(schema) => {
            let columns = schema.columns();
            let types = schema.types();
            let map = fields
                .iter()
                .map(|(name, column_type)| {
                    let index = columns
                        .iter()
                        .position(|column| column.eq_ignore_ascii_case(name))
                        .ok_or_else(|| {
                            read_error(format!(
                                "The object '{key}' has a column '{name}' the first object does not."
                            ))
                        })?;
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
    let iter = parquet::record::reader::RowIter::from_file_into(Box::new(parquet));
    let rows = iter.map(move |row| {
        let row = row.map_err(parquet_error)?;
        let mut out = vec![Datum::Null; types.len()];
        for (index, (_, field)) in row.get_column_iter().enumerate() {
            let column = match &map {
                Some(map) => map[index],
                None => index,
            };
            out[column] = parquet_datum(field, types[column])?;
        }
        Ok(out)
    });
    Ok((schema, Box::new(rows)))
}
