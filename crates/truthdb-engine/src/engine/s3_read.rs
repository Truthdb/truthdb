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
                .map(|name| text_column(name, ColumnType::NVarCharMax)),
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
