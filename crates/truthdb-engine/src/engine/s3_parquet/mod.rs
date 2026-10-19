//! Parquet for `sys.s3_read` (see `s3_read`): typed rows read with the
//! `parquet` crate's record API from a leased object body.
//!
//! A file's bytes are whatever its uploader chose, and the crate trusts
//! them: sizes and counts in it decide allocations, it recurses over the
//! schema, and it panics on some inputs. So before the crate sees anything:
//! the footer's schema is read here, without recursion, and handed to it;
//! each page header is read here as the crate reads it and its page refused
//! when it would decompress past a cap (a gzip, Brotli or LZ4 page decoded
//! here into nothing first, as those decoders do not stop at the declared
//! size); a dictionary page may not declare more values than its bytes hold;
//! a repeated column's rows are measured from their repetition levels before
//! any row is built, as the record API builds each row whole; and every call
//! into the crate runs under `catch_unwind`.

pub(super) mod thrift;

use std::io::Read;
use std::sync::Arc;

use truthdb_relational::engine::check_cancelled;
use truthdb_relational::relstore::types::{ColumnType, Datum};
use truthdb_sql::error::SqlError;

use super::s3_read::{BodyReader, Rows, Schema, read_error};

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
/// The most values one row may hold across its repeated columns: the
/// record API builds a row whole, at tens of bytes a value.
const MAX_ROW_VALUES: u64 = 1 << 20;
/// Bytes a page header read fetches per storage-lock hold.
const PAGE_HEADER_READ: usize = 16 << 10;

/// A leased body as a Parquet source: positioned reads, each a chunk per
/// storage-lock hold. The lease goes when the last reader of it does.
///
/// The Parquet reader reads each page's header through [`ParquetFile::get_read`]
/// and then fetches the page with [`ParquetFile::get_bytes`]. The fetch parses
/// that header itself first and refuses a page whose decompressed size is
/// over the read's page cap, so a small file cannot make the reader allocate
/// gigabytes. The gzip, Brotli and LZ4 decoders the reader uses do not stop
/// at the size a header declares, so a page in one of those is also decoded
/// here first, into nothing, up to that cap. Until the file's metadata has
/// been read (`pages` is false), reads are of the footer and the metadata.
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
    /// The column chunks, by byte range: a page's chunk gives its codec and
    /// its values' smallest size.
    chunks: Vec<Chunk>,
}

/// A column chunk's byte range, compression and the fewest bits one of its
/// values takes.
struct Chunk {
    start: u64,
    end: u64,
    codec: parquet::basic::Compression,
    value_bits: u64,
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
    /// No header is longer than [`MAX_PAGE_HEADER`]: a read past it ends, so
    /// a header claiming a huge field cannot have the whole object read.
    end: u64,
    buf: Vec<u8>,
    at: usize,
}

impl Read for ParquetRead {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.at == self.buf.len() {
            let len = (self.end.saturating_sub(self.pos)).min(PAGE_HEADER_READ as u64);
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
            end: start.saturating_add(MAX_PAGE_HEADER).min(self.0.size),
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
                let page = thrift::page_header(&bytes)
                    .ok_or_else(|| parquet_failure("a page header that cannot be read"))?;
                let (Ok(uncompressed), Ok(compressed)) = (
                    u64::try_from(page.uncompressed),
                    u64::try_from(page.compressed),
                ) else {
                    return Err(parquet_failure("a page header with a negative size"));
                };
                if compressed != length as u64 {
                    return Err(parquet_failure("a page read that is not its header's page"));
                }
                let cap = state.page_cap as u64;
                if uncompressed.max(compressed) > cap {
                    return Err(parquet_failure(format!(
                        "a page of {uncompressed} bytes is larger than this file's limit of {cap} bytes"
                    )));
                }
                let end = start + length as u64;
                let chunk = state
                    .chunks
                    .iter()
                    .find(|chunk| chunk.start <= start && end <= chunk.end)
                    .ok_or_else(|| parquet_failure("a page outside every column chunk"))?;
                let codec = chunk.codec;
                // A dictionary holds its values in its bytes: the reader
                // allocates the count it declares.
                if page.page_type == thrift::DICTIONARY_PAGE
                    && page.dictionary_values.is_some_and(|values| {
                        u64::try_from(values).map_or(true, |values| {
                            values.saturating_mul(chunk.value_bits) > uncompressed * 8
                        })
                    })
                {
                    return Err(parquet_failure(
                        "a dictionary page declaring more values than its bytes hold",
                    ));
                }
                let levels = match page.levels {
                    Some((definition, repetition)) => {
                        let (Ok(definition), Ok(repetition)) =
                            (usize::try_from(definition), usize::try_from(repetition))
                        else {
                            return Err(parquet_failure("a page header with a negative size"));
                        };
                        definition.saturating_add(repetition)
                    }
                    None => 0,
                };
                let mut out = vec![0u8; length];
                ParquetSource::read_exact_at(&mut state, start, &mut out)?;
                if page.is_compressed != Some(false)
                    && out
                        .get(levels..)
                        .is_some_and(|data| inflates_past(codec, data, cap))
                {
                    return Err(parquet_failure(format!(
                        "a page that decompresses past this file's limit of {cap} bytes"
                    )));
                }
                return Ok(out.into());
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

/// Whether `data`, compressed with `codec`, decodes to more than `cap`
/// bytes. It is decoded into nothing, so the check takes no memory for the
/// output. A stream that fails to decode within the cap is left to the
/// Parquet reader, which fails at the same place.
fn inflates_past(codec: parquet::basic::Compression, data: &[u8], cap: u64) -> bool {
    use parquet::basic::Compression;
    let limit = cap + 1;
    let decoded = match codec {
        Compression::GZIP(_) => std::io::copy(
            &mut flate2::read::MultiGzDecoder::new(data).take(limit),
            &mut std::io::sink(),
        ),
        Compression::BROTLI(_) => std::io::copy(
            &mut brotli::Decompressor::new(data, 4096).take(limit),
            &mut std::io::sink(),
        ),
        // The reader decodes LZ4 as Hadoop's framing, which stops at the
        // declared size, and failing that as an LZ4 frame, which does not.
        Compression::LZ4 => std::io::copy(
            &mut lz4_flex::frame::FrameDecoder::new(data).take(limit),
            &mut std::io::sink(),
        ),
        _ => return false,
    };
    matches!(decoded, Ok(n) if n > cap)
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
pub(super) fn rows(
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
            chunks: Vec::new(),
        }),
        size,
    });
    // The footer: the metadata, its length and the magic. Its schema is read
    // here and handed to the reader, which then skips the footer's copy.
    let parquet_schema = {
        let mut state = source.state();
        let mut tail = [0u8; 8];
        if size < 12 {
            return Err(read_error("Invalid Parquet: the file is too short."));
        }
        ParquetSource::read_exact_at(&mut state, size - 8, &mut tail)
            .map_err(|err| read_error(err.to_string()))?;
        let len = u64::from(u32::from_le_bytes(tail[..4].try_into().expect("4 bytes")));
        if &tail[4..] != b"PAR1" || len > size - 12 || len > MAX_PARQUET_METADATA as u64 {
            return Err(read_error("Invalid Parquet: the footer cannot be read."));
        }
        let mut metadata = vec![0u8; len as usize];
        ParquetSource::read_exact_at(&mut state, size - 8 - len, &mut metadata)
            .map_err(|err| read_error(err.to_string()))?;
        thrift::footer_schema(&metadata, MAX_PARQUET_DEPTH)
            .map_err(|err| read_error(format!("Invalid Parquet: {err}.")))?
    };
    let parquet = parquet_guarded(|| {
        let options = parquet::file::serialized_reader::ReadOptionsBuilder::new()
            .with_parquet_schema(Arc::new(parquet_schema))
            .build();
        parquet::file::serialized_reader::SerializedFileReader::new_with_options(
            ParquetFile(Arc::clone(&source)),
            options,
        )
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
        let mut chunks: Vec<Chunk> = parquet
            .metadata()
            .row_groups()
            .iter()
            .flat_map(|group| group.columns())
            .map(|column| {
                let (start, len) = column.byte_range();
                Chunk {
                    start,
                    end: start.saturating_add(len),
                    codec: column.compression(),
                    value_bits: value_bits(column.column_descr()),
                }
            })
            .collect();
        // Chunks may not overlap: a page is checked against its own chunk's
        // codec, and an overlap would let it be checked against another's.
        chunks.sort_by_key(|chunk| (chunk.start, chunk.end));
        if chunks.windows(2).any(|pair| pair[1].start < pair[0].end) {
            return Err(read_error("Invalid Parquet: two column chunks overlap."));
        }
        state.chunks = chunks;
        state.pages = true;
    }
    parquet_guarded(|| check_row_lengths(&parquet))?;
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

/// The fewest bits one value of a leaf column takes in a plain dictionary.
fn value_bits(leaf: &parquet::schema::types::ColumnDescriptor) -> u64 {
    use parquet::basic::Type as P;
    match leaf.physical_type() {
        P::BOOLEAN => 1,
        P::INT32 | P::FLOAT => 32,
        P::INT64 | P::DOUBLE => 64,
        P::INT96 => 96,
        // A length prefix.
        P::BYTE_ARRAY => 32,
        P::FIXED_LEN_BYTE_ARRAY => (leaf.type_length().max(1) as u64) * 8,
    }
}

/// Refuses a file one of whose rows holds more than [`MAX_ROW_VALUES`]
/// values across its repeated columns, read from the repetition levels of
/// every page (each page through the checks above), before the record API
/// builds any row.
fn check_row_lengths(
    parquet: &parquet::file::serialized_reader::SerializedFileReader<ParquetFile>,
) -> Result<(), SqlError> {
    use parquet::column::page::Page;
    use parquet::file::reader::FileReader;
    let descr = parquet.metadata().file_metadata().schema_descr_ptr();
    let repeated: Vec<usize> = (0..descr.num_columns())
        .filter(|&leaf| descr.column(leaf).max_rep_level() > 0)
        .collect();
    if repeated.is_empty() {
        return Ok(());
    }
    let cap = MAX_ROW_VALUES / repeated.len() as u64;
    for group in 0..parquet.num_row_groups() {
        let group = parquet.get_row_group(group).map_err(parquet_error)?;
        for &leaf in &repeated {
            let max_level = descr.column(leaf).max_rep_level();
            let width = 16 - (max_level as u16).leading_zeros();
            let mut row = Row { values: 0, cap };
            let mut pages = group.get_column_page_reader(leaf).map_err(parquet_error)?;
            while let Some(page) = pages.get_next_page().map_err(parquet_error)? {
                check_cancelled()?;
                match page {
                    Page::DataPage {
                        buf,
                        num_values,
                        rep_level_encoding,
                        ..
                    } => {
                        if rep_level_encoding == parquet::basic::Encoding::RLE {
                            let len = buf
                                .get(..4)
                                .map(|len| u32::from_le_bytes(len.try_into().expect("4 bytes")))
                                .unwrap_or(0) as usize;
                            let levels = buf.get(4..4 + len).unwrap_or(&[]);
                            row.hybrid(levels, width, num_values)?;
                        } else {
                            row.packed(&buf, width, num_values)?;
                        }
                    }
                    Page::DataPageV2 {
                        buf,
                        num_values,
                        rep_levels_byte_len,
                        ..
                    } => {
                        let levels = buf.get(..rep_levels_byte_len as usize).unwrap_or(&[]);
                        row.hybrid(levels, width, num_values)?;
                    }
                    Page::DictionaryPage { .. } => {}
                }
            }
        }
    }
    Ok(())
}

/// The values of the row being measured: a repetition level of 0 starts a
/// new row.
struct Row {
    values: u64,
    cap: u64,
}

impl Row {
    fn level(&mut self, level: u64, count: u64) -> Result<(), SqlError> {
        if count == 0 {
            return Ok(());
        }
        self.values = if level == 0 { 1 } else { self.values + count };
        if self.values > self.cap {
            return Err(read_error(format!(
                "A Parquet row holds more than {} values.",
                self.cap
            )));
        }
        Ok(())
    }

    /// Levels in the RLE / bit-packed hybrid encoding, `count` of them
    /// (malformed levels are left to the reader, which fails on them).
    fn hybrid(&mut self, mut bytes: &[u8], width: u32, count: u32) -> Result<(), SqlError> {
        let mut left = u64::from(count);
        let value_bytes = width.div_ceil(8) as usize;
        while left > 0 {
            let Some((header, used)) = uleb(bytes) else {
                return Ok(());
            };
            bytes = &bytes[used..];
            if header & 1 == 0 {
                let run = (header >> 1).min(left);
                let Some(value) = bytes.get(..value_bytes) else {
                    return Ok(());
                };
                let level = value
                    .iter()
                    .rev()
                    .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
                bytes = &bytes[value_bytes..];
                self.level(level, run)?;
                left -= run;
            } else {
                let groups = header >> 1;
                let Some(len) = groups
                    .checked_mul(u64::from(width))
                    .and_then(|len| usize::try_from(len).ok())
                    .filter(|&len| len <= bytes.len())
                else {
                    return Ok(());
                };
                let values = (groups * 8).min(left);
                for index in 0..values {
                    let level = bits_lsb(&bytes[..len], index * u64::from(width), width);
                    self.level(level, 1)?;
                }
                bytes = &bytes[len..];
                left -= values;
            }
        }
        Ok(())
    }

    /// Levels in the deprecated BIT_PACKED encoding (most significant bit
    /// first).
    fn packed(&mut self, bytes: &[u8], width: u32, count: u32) -> Result<(), SqlError> {
        for index in 0..u64::from(count) {
            let start = index * u64::from(width);
            if start + u64::from(width) > bytes.len() as u64 * 8 {
                return Ok(());
            }
            let mut level = 0u64;
            for bit in start..start + u64::from(width) {
                let byte = bytes[(bit / 8) as usize];
                level = (level << 1) | u64::from((byte >> (7 - bit % 8)) & 1);
            }
            self.level(level, 1)?;
        }
        Ok(())
    }
}

fn uleb(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (index, byte) in bytes.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7F) << (7 * index);
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
    }
    None
}

/// `width` bits at bit `at` of `bytes`, least significant bit first.
fn bits_lsb(bytes: &[u8], at: u64, width: u32) -> u64 {
    let mut value = 0u64;
    for bit in 0..u64::from(width) {
        let position = at + bit;
        let byte = bytes[(position / 8) as usize];
        value |= u64::from((byte >> (position % 8)) & 1) << bit;
    }
    value
}
