//! The orders compaction's sort and z-order strategies write rows in: a
//! table's sort order (its fields' transforms, directions and null orders,
//! as Iceberg's spec has them), or the z-order of its sort order's columns
//! (Iceberg's `ZOrderByteUtils`). Each row's place is an encoded key
//! (arrow-row's): keys compare, as bytes, as the rows sort.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, SortOptions, TimeUnit};
use serde_json::Value;

use super::{column_of, holds_id, string_at};

/// A partition (and sort) transform, as a sort field names it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Transform {
    Identity,
    Bucket(u32),
    Truncate(u32),
    Year,
    Month,
    Day,
    Hour,
    Void,
}

impl Transform {
    /// `identity`, `bucket[N]`, `truncate[W]`, `year`, `month`, `day`,
    /// `hour` or `void`.
    fn parse(text: &str) -> Option<Transform> {
        let width = |prefix: &str| -> Option<u32> {
            text.strip_prefix(prefix)?
                .strip_suffix(']')?
                .parse()
                .ok()
                .filter(|width| *width > 0)
        };
        Some(match text {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            _ => match (width("bucket["), width("truncate[")) {
                (Some(n), _) => Transform::Bucket(n),
                (_, Some(w)) => Transform::Truncate(w),
                _ => return None,
            },
        })
    }
}

/// A sort field: its source column's field id, transform, direction and
/// null order.
#[derive(Clone, Debug)]
pub(super) struct SortField {
    pub(super) source: i32,
    pub(super) transform: Transform,
    pub(super) descending: bool,
    pub(super) nulls_first: bool,
}

/// The order a rewrite writes rows in.
#[derive(Clone, Debug)]
pub(super) enum Order {
    /// The table's sort order (its id, and fields).
    Sort(i64, Vec<SortField>),
    /// The z-order of these columns (field ids).
    ZOrder(Vec<i32>),
}

/// The table's default sort order: its id and fields (none: unsorted), or
/// why it cannot be followed.
pub(super) fn table_order(metadata: &Value) -> Result<Option<(i64, Vec<SortField>)>, String> {
    let id = metadata["default-sort-order-id"].as_i64().unwrap_or(0);
    let Some(order) = metadata["sort-orders"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|order| order["order-id"].as_i64() == Some(id))
    else {
        return Ok(None);
    };
    let mut fields = Vec::new();
    for field in order["fields"].as_array().into_iter().flatten() {
        let transform = field["transform"]
            .as_str()
            .and_then(Transform::parse)
            .ok_or_else(|| {
                format!(
                    "The sort order's transform {} is not known.",
                    field["transform"]
                )
            })?;
        let source = field["source-id"]
            .as_i64()
            .or_else(|| match field["source-ids"].as_array().map(Vec::as_slice) {
                Some([one]) => one.as_i64(),
                _ => None,
            })
            .ok_or_else(|| {
                "A sort field of more than one source is not followed here.".to_string()
            })?;
        fields.push(SortField {
            source: source as i32,
            transform,
            descending: field["direction"].as_str() == Some("desc"),
            nulls_first: field["null-order"].as_str() == Some("nulls-first"),
        });
    }
    Ok((!fields.is_empty()).then_some((id, fields)))
}

/// Encodes rows' keys; one is made for a rewrite, its fields the same for
/// every batch.
pub(super) struct Keys {
    order: Order,
    converter: Option<arrow_row::RowConverter>,
}

impl Keys {
    pub(super) fn new(order: Order) -> Keys {
        Keys {
            order,
            converter: None,
        }
    }

    /// The keys of `batch`'s rows.
    pub(super) fn of(&mut self, batch: &RecordBatch) -> Result<arrow_row::Rows, String> {
        let (columns, fields) = match &self.order {
            Order::Sort(_, sort) => {
                let mut columns = Vec::new();
                let mut fields = Vec::new();
                for field in sort {
                    if field.transform == Transform::Void {
                        continue;
                    }
                    let column = source(batch, field.source)?;
                    let column = apply(field.transform, &column)?;
                    fields.push(arrow_row::SortField::new_with_options(
                        column.data_type().clone(),
                        SortOptions {
                            descending: field.descending,
                            nulls_first: field.nulls_first,
                        },
                    ));
                    columns.push(column);
                }
                (columns, fields)
            }
            Order::ZOrder(ids) => {
                let column = z_values(batch, ids)?;
                let field = arrow_row::SortField::new(column.data_type().clone());
                (vec![column], vec![field])
            }
        };
        if self.converter.is_none() {
            self.converter = Some(
                arrow_row::RowConverter::new(fields)
                    .map_err(|err| format!("Rows cannot be ordered by these columns: {err}"))?,
            );
        }
        self.converter
            .as_ref()
            .expect("made above")
            .convert_columns(&columns)
            .map_err(|err| format!("Rows cannot be ordered by these columns: {err}"))
    }
}

/// The column of field id `id` of `batch`: a top-level column, or nulls
/// when the file does not hold it (one nested is not ordered by here).
fn source(batch: &RecordBatch, id: i32) -> Result<ArrayRef, String> {
    let schema = batch.schema();
    match column_of(&schema, id) {
        Some(at) => Ok(Arc::clone(batch.column(at))),
        None if holds_id(schema.fields(), id) => Err(format!(
            "The sort order's field {id} is nested: it is not ordered by here."
        )),
        None => Ok(arrow_array::new_null_array(
            &DataType::Int64,
            batch.num_rows(),
        )),
    }
}

/// `column` with its dictionary (the reader's form of byte arrays)
/// expanded.
fn plain(column: &ArrayRef) -> Result<ArrayRef, String> {
    use arrow_array::{DictionaryArray, types::Int32Type};
    match column.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        Some(dictionary) => arrow_select::take::take(dictionary.values(), dictionary.keys(), None)
            .map_err(|err| format!("A column cannot be read: {err}")),
        None => Ok(Arc::clone(column)),
    }
}

/// The days from 1970-01-01 of a date's civil year and month: the year and
/// month (1 to 12).
fn civil(days: i64) -> (i64, i64) {
    // Howard Hinnant's days_from_civil, inverted.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;

/// The values of `column` (a date, or a timestamp in micros) as days or
/// micros from the epoch.
enum Moments {
    Days(Vec<Option<i64>>),
    Micros(Vec<Option<i64>>),
}

fn moments(column: &ArrayRef) -> Result<Moments, String> {
    use arrow_array::{Date32Array, TimestampMicrosecondArray};
    let any = column.as_any();
    if let Some(dates) = any.downcast_ref::<Date32Array>() {
        return Ok(Moments::Days(
            dates.iter().map(|day| day.map(i64::from)).collect(),
        ));
    }
    if let Some(stamps) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Moments::Micros(stamps.iter().collect()));
    }
    Err(format!(
        "A time transform of a {} column is not applied here.",
        column.data_type()
    ))
}

/// `transform` of `column`.
fn apply(transform: Transform, column: &ArrayRef) -> Result<ArrayRef, String> {
    use arrow_array::Int32Array;
    let ints = |values: Vec<Option<i64>>| -> ArrayRef {
        Arc::new(Int32Array::from(
            values
                .into_iter()
                .map(|value| value.map(|value| value as i32))
                .collect::<Vec<_>>(),
        ))
    };
    match transform {
        Transform::Identity | Transform::Void => Ok(Arc::clone(column)),
        Transform::Bucket(n) => bucket(column, n),
        Transform::Truncate(width) => truncate(column, width),
        Transform::Year | Transform::Month | Transform::Day => {
            let days = match moments(column)? {
                Moments::Days(days) => days,
                Moments::Micros(micros) => micros
                    .into_iter()
                    .map(|at| at.map(|at| at.div_euclid(MICROS_PER_DAY)))
                    .collect(),
            };
            Ok(ints(
                days.into_iter()
                    .map(|day| {
                        day.map(|day| match transform {
                            Transform::Year => civil(day).0 - 1970,
                            Transform::Month => {
                                let (year, month) = civil(day);
                                (year - 1970) * 12 + month - 1
                            }
                            _ => day,
                        })
                    })
                    .collect(),
            ))
        }
        Transform::Hour => match moments(column)? {
            Moments::Micros(micros) => Ok(ints(
                micros
                    .into_iter()
                    .map(|at| at.map(|at| at.div_euclid(MICROS_PER_HOUR)))
                    .collect(),
            )),
            Moments::Days(_) => Err("An hour transform of a date is not defined.".to_string()),
        },
    }
}

/// Iceberg's 32-bit hash: Murmur3, x86 variant, seed 0.
fn murmur3(bytes: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut hash: u32 = 0;
    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes(chunk.try_into().expect("4 bytes"));
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= k;
        hash = hash
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        let mut k: u32 = 0;
        for (at, byte) in tail.iter().enumerate() {
            k |= u32::from(*byte) << (8 * at);
        }
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= k;
    }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^= hash >> 16;
    hash as i32
}

/// A decimal's unscaled value in the fewest big-endian two's-complement
/// bytes.
fn decimal_bytes(unscaled: i128) -> Vec<u8> {
    let bytes = unscaled.to_be_bytes();
    let mut start = 0;
    while start + 1 < bytes.len()
        && ((bytes[start] == 0 && bytes[start + 1] & 0x80 == 0)
            || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0))
    {
        start += 1;
    }
    bytes[start..].to_vec()
}

/// The bucket (of `n`) of each value of `column`, as Iceberg's spec hashes
/// each type: ints, longs, dates, times and timestamps as a long's 8
/// little-endian bytes; strings as UTF-8; binary, fixed and uuids as their
/// bytes; decimals as their unscaled value's fewest big-endian bytes.
fn bucket(column: &ArrayRef, n: u32) -> Result<ArrayRef, String> {
    use arrow_array::{
        BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Int32Array, Int64Array,
        LargeBinaryArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    let column = plain(column)?;
    let any = column.as_any();
    let of_long = |value: i64| murmur3(&value.to_le_bytes());
    let hashes: Vec<Option<i32>> = if let Some(values) = any.downcast_ref::<Int32Array>() {
        values
            .iter()
            .map(|v| v.map(|v| of_long(i64::from(v))))
            .collect()
    } else if let Some(values) = any.downcast_ref::<Date32Array>() {
        values
            .iter()
            .map(|v| v.map(|v| of_long(i64::from(v))))
            .collect()
    } else if let Some(values) = any.downcast_ref::<Int64Array>() {
        values.iter().map(|v| v.map(of_long)).collect()
    } else if let Some(values) = any.downcast_ref::<TimestampMicrosecondArray>() {
        values.iter().map(|v| v.map(of_long)).collect()
    } else if let Some(values) = any.downcast_ref::<Time64MicrosecondArray>() {
        values.iter().map(|v| v.map(of_long)).collect()
    } else if let Some(values) = any.downcast_ref::<Decimal128Array>() {
        values
            .iter()
            .map(|v| v.map(|v| murmur3(&decimal_bytes(v))))
            .collect()
    } else if let Some(values) = any.downcast_ref::<BinaryArray>() {
        values.iter().map(|v| v.map(murmur3)).collect()
    } else if let Some(values) = any.downcast_ref::<LargeBinaryArray>() {
        values.iter().map(|v| v.map(murmur3)).collect()
    } else if let Some(values) = any.downcast_ref::<FixedSizeBinaryArray>() {
        values.iter().map(|v| v.map(murmur3)).collect()
    } else if matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        (0..column.len())
            .map(|row| string_at(&column, row).map(|text| murmur3(text.as_bytes())))
            .collect()
    } else {
        return Err(format!(
            "A bucket transform of a {} column is not applied here.",
            column.data_type()
        ));
    };
    Ok(Arc::new(Int32Array::from(
        hashes
            .into_iter()
            .map(|hash| hash.map(|hash| ((hash & i32::MAX) as u32 % n) as i32))
            .collect::<Vec<_>>(),
    )))
}

/// Each value of `column` truncated to `width`: ints and longs to the
/// multiple of it at or below them, decimals likewise in their unscaled
/// values, strings to their first `width` code points, binary to its first
/// `width` bytes.
fn truncate(column: &ArrayRef, width: u32) -> Result<ArrayRef, String> {
    use arrow_array::{BinaryArray, Decimal128Array, Int32Array, Int64Array, StringArray};
    let column = plain(column)?;
    let any = column.as_any();
    let w = i64::from(width);
    let down = |v: i64| v - (((v % w) + w) % w);
    if let Some(values) = any.downcast_ref::<Int32Array>() {
        return Ok(Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| down(i64::from(v)) as i32))
                .collect::<Vec<_>>(),
        )));
    }
    if let Some(values) = any.downcast_ref::<Int64Array>() {
        return Ok(Arc::new(Int64Array::from(
            values.iter().map(|v| v.map(down)).collect::<Vec<_>>(),
        )));
    }
    if let Some(values) = any.downcast_ref::<Decimal128Array>() {
        let w = i128::from(width);
        let truncated: Decimal128Array = values
            .iter()
            .map(|v| v.map(|v| v - (((v % w) + w) % w)))
            .collect();
        let truncated = truncated
            .with_precision_and_scale(values.precision(), values.scale())
            .map_err(|err| format!("A decimal cannot be truncated: {err}"))?;
        return Ok(Arc::new(truncated));
    }
    if let Some(values) = any.downcast_ref::<BinaryArray>() {
        return Ok(Arc::new(BinaryArray::from(
            values
                .iter()
                .map(|v| v.map(|v| &v[..v.len().min(width as usize)]))
                .collect::<Vec<_>>(),
        )));
    }
    if matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return Ok(Arc::new(StringArray::from(
            (0..column.len())
                .map(|row| {
                    string_at(&column, row).map(|text| {
                        match text.char_indices().nth(width as usize) {
                            Some((end, _)) => text[..end].to_string(),
                            None => text,
                        }
                    })
                })
                .collect::<Vec<_>>(),
        )));
    }
    Err(format!(
        "A truncate transform of a {} column is not applied here.",
        column.data_type()
    ))
}

/// Bytes each column gives a z-value (Iceberg's `PRIMITIVE_BUFFER_SIZE`,
/// and its default for strings and binary).
const Z_BYTES: usize = 8;

/// Each row's z-value of columns `ids` of `batch`: each column's value as 8
/// bytes that order as it does (a whole number with its sign bit flipped,
/// a float's bits flipped as IEEE 754's total order needs, a boolean's
/// first byte, a string's or binary's first 8 bytes, zero padded; a null
/// all zeros), their bits interleaved, the first column's first.
fn z_values(batch: &RecordBatch, ids: &[i32]) -> Result<ArrayRef, String> {
    let rows = batch.num_rows();
    let mut columns: Vec<Vec<[u8; Z_BYTES]>> = Vec::new();
    for id in ids {
        let column = plain(&source(batch, *id)?)?;
        columns.push(z_bytes(&column)?);
    }
    let width = Z_BYTES * columns.len();
    let mut out = arrow_array::builder::BinaryBuilder::with_capacity(rows, rows * width);
    let mut key = vec![0u8; width];
    for row in 0..rows {
        key.iter_mut().for_each(|byte| *byte = 0);
        let mut bit = 0usize;
        for source_bit in 0..Z_BYTES * 8 {
            for column in &columns {
                let byte = column[row][source_bit / 8];
                if byte & (0x80 >> (source_bit % 8)) != 0 {
                    key[bit / 8] |= 0x80 >> (bit % 8);
                }
                bit += 1;
            }
        }
        out.append_value(&key);
    }
    Ok(Arc::new(out.finish()))
}

/// A column's values as z-order bytes.
fn z_bytes(column: &ArrayRef) -> Result<Vec<[u8; Z_BYTES]>, String> {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, TimestampMicrosecondArray,
    };
    let any = column.as_any();
    let whole = |value: Option<i64>| match value {
        Some(value) => ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes(),
        None => [0; Z_BYTES],
    };
    let float = |value: Option<f64>| match value {
        Some(value) => {
            let bits = value.to_bits() as i64;
            let flipped = bits ^ ((bits >> 63) | i64::MIN);
            (flipped as u64).to_be_bytes()
        }
        None => [0; Z_BYTES],
    };
    let filled = |value: Option<&[u8]>| {
        let mut out = [0u8; Z_BYTES];
        if let Some(value) = value {
            let take = value.len().min(Z_BYTES);
            out[..take].copy_from_slice(&value[..take]);
        }
        out
    };
    macro_rules! wholes {
        ($array:ty) => {
            if let Some(values) = any.downcast_ref::<$array>() {
                return Ok(values.iter().map(|v| whole(v.map(i64::from))).collect());
            }
        };
    }
    wholes!(Int8Array);
    wholes!(Int16Array);
    wholes!(Int32Array);
    wholes!(Int64Array);
    wholes!(Date32Array);
    if let Some(values) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(values.iter().map(whole).collect());
    }
    if let Some(values) = any.downcast_ref::<Float32Array>() {
        return Ok(values.iter().map(|v| float(v.map(f64::from))).collect());
    }
    if let Some(values) = any.downcast_ref::<Float64Array>() {
        return Ok(values.iter().map(float).collect());
    }
    if let Some(values) = any.downcast_ref::<BooleanArray>() {
        return Ok(values
            .iter()
            .map(|v| {
                let mut out = [0u8; Z_BYTES];
                if v == Some(true) {
                    out[0] = 0x81;
                }
                out
            })
            .collect());
    }
    if let Some(values) = any.downcast_ref::<BinaryArray>() {
        return Ok(values.iter().map(filled).collect());
    }
    if matches!(column.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        return Ok((0..column.len())
            .map(|row| filled(string_at(column, row).as_deref().map(str::as_bytes)))
            .collect());
    }
    // A timestamp of another unit is not a moment's micros; decimals are
    // not z-ordered by Iceberg.
    if let DataType::Timestamp(unit, _) = column.data_type()
        && *unit != TimeUnit::Microsecond
    {
        return Err("A timestamp of other than microseconds is not z-ordered here.".to_string());
    }
    Err(format!(
        "A {} column is not z-ordered (Iceberg's z-order does not order it).",
        column.data_type()
    ))
}

/// The rows a sort holds before it writes them out as a run, and the rows
/// of each batch it gives.
pub(super) const RUN_BYTES: usize = 256 << 20;
const OUT_ROWS: usize = 8192;

/// A group's rows put in order: held, a run's worth at a time, then each
/// run sorted and written out as an object of the table's storage; given
/// back merged from the runs (or, all held, sorted as they are). The run
/// objects are removed when the sort is dropped.
pub(super) struct Sorter<'a> {
    keys: Keys,
    storage: &'a Arc<truthdb_core::storage::Storage>,
    warehouse: &'a str,
    schema: arrow_schema::SchemaRef,
    descr: parquet::schema::types::SchemaDescriptor,
    run_bytes: usize,
    held: Vec<RecordBatch>,
    held_bytes: usize,
    runs: Vec<(String, u64)>,
}

impl<'a> Sorter<'a> {
    pub(super) fn new(
        keys: Keys,
        (storage, warehouse): (&'a Arc<truthdb_core::storage::Storage>, &'a str),
        (schema, descr): (
            arrow_schema::SchemaRef,
            parquet::schema::types::SchemaDescriptor,
        ),
        run_bytes: usize,
    ) -> Sorter<'a> {
        Sorter {
            keys,
            storage,
            warehouse,
            schema,
            descr,
            run_bytes,
            held: Vec::new(),
            held_bytes: 0,
            runs: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, batch: RecordBatch) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.held_bytes += batch.get_array_memory_size();
        self.held.push(batch);
        if self.held_bytes >= self.run_bytes {
            self.spill()?;
        }
        Ok(())
    }

    /// The rows held, in order.
    fn sorted(&mut self) -> Result<Option<RecordBatch>, String> {
        if self.held.is_empty() {
            return Ok(None);
        }
        let all = arrow_select::concat::concat_batches(&self.schema, &self.held)
            .map_err(|err| format!("Rows cannot be put in order: {err}"))?;
        self.held.clear();
        self.held_bytes = 0;
        let keys = self.keys.of(&all)?;
        let mut order: Vec<u32> = (0..all.num_rows() as u32).collect();
        order.sort_by(|a, b| keys.row(*a as usize).cmp(&keys.row(*b as usize)));
        let order = arrow_array::UInt32Array::from(order);
        arrow_select::take::take_record_batch(&all, &order)
            .map(Some)
            .map_err(|err| format!("Rows cannot be put in order: {err}"))
    }

    /// Writes the rows held, in order, as a run.
    fn spill(&mut self) -> Result<(), String> {
        let Some(sorted) = self.sorted()? else {
            return Ok(());
        };
        let failed =
            |err: parquet::errors::ParquetError| format!("A sorted run cannot be written: {err}");
        let options = parquet::arrow::arrow_writer::ArrowWriterOptions::new()
            .with_skip_arrow_metadata(true)
            .with_parquet_schema(self.descr.clone());
        let mut writer = parquet::arrow::ArrowWriter::try_new_with_options(
            super::BodySink::begin(self.storage)?,
            Arc::clone(&self.schema),
            options,
        )
        .map_err(failed)?;
        writer.write(&sorted).map_err(failed)?;
        writer.finish().map_err(failed)?;
        let (sealed, _) = writer.inner_mut().take().seal()?;
        let key = format!("tmp/compaction-{}.parquet", crate::tables::uuid());
        let now = crate::objects::now_ms();
        let warehouse = self.warehouse;
        let version = truthdb_object::ObjFile::new(&mut self.storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    warehouse,
                    &key,
                    &sealed,
                    &truthdb_object::ObjectAttrs::default(),
                    truthdb_object::StorageClass::Standard,
                    now,
                    0,
                )
            })
            .map_err(|err| err.to_string())?
            .map_err(|err| format!("The table's storage refused a sorted run: {err:?}"))?
            .version;
        self.runs.push((key, version));
        Ok(())
    }

    /// Gives every row pushed, in order, a batch at a time, to `emit`.
    pub(super) fn finish(
        mut self,
        mut emit: impl FnMut(RecordBatch) -> Result<(), String>,
    ) -> Result<(), String> {
        if self.runs.is_empty() {
            if let Some(sorted) = self.sorted()? {
                let mut at = 0;
                while at < sorted.num_rows() {
                    let take = OUT_ROWS.min(sorted.num_rows() - at);
                    emit(sorted.slice(at, take))?;
                    at += take;
                }
            }
            return Ok(());
        }
        self.spill()?;
        self.merge(&mut emit)
    }

    /// Merges the runs (each in order) into one order.
    fn merge(
        &mut self,
        emit: &mut impl FnMut(RecordBatch) -> Result<(), String>,
    ) -> Result<(), String> {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let files = self
            .runs
            .iter()
            .map(|(key, _)| {
                truthdb_engine::engine::open_arrow_parquet(self.storage, self.warehouse, key)
                    .map_err(|err| format!("A sorted run cannot be read: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        type Batches<'f> = Box<dyn Iterator<Item = Result<RecordBatch, String>> + 'f>;
        let mut streams: Vec<Batches<'_>> = files
            .iter()
            .map(|file| {
                let groups = file.metadata().num_row_groups();
                Box::new(
                    (0..groups).flat_map(move |group| match file.batches(group) {
                        Ok(batches) => Box::new(batches) as Batches<'_>,
                        Err(err) => Box::new(std::iter::once(Err(err))),
                    }),
                ) as Batches<'_>
            })
            .collect();
        // Each run's batch being merged (an index into `sources`), its keys
        // and next row.
        struct Cursor {
            source: usize,
            keys: arrow_row::Rows,
            row: usize,
        }
        let mut sources: Vec<RecordBatch> = Vec::new();
        let mut cursors: Vec<Option<Cursor>> = Vec::new();
        let mut heap = BinaryHeap::new();
        let mut next = |run: usize,
                        sources: &mut Vec<RecordBatch>,
                        keys: &mut Keys|
         -> Result<Option<Cursor>, String> {
            loop {
                let Some(batch) = streams[run].next().transpose()? else {
                    return Ok(None);
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                let rows = keys.of(&batch)?;
                sources.push(batch);
                return Ok(Some(Cursor {
                    source: sources.len() - 1,
                    keys: rows,
                    row: 0,
                }));
            }
        };
        for run in 0..files.len() {
            let cursor = next(run, &mut sources, &mut self.keys)?;
            if let Some(cursor) = &cursor {
                heap.push(Reverse((cursor.keys.row(0).as_ref().to_vec(), run)));
            }
            cursors.push(cursor);
        }
        let mut picked: Vec<(usize, usize)> = Vec::with_capacity(OUT_ROWS);
        let flush = |picked: &mut Vec<(usize, usize)>,
                     sources: &mut Vec<RecordBatch>,
                     cursors: &mut Vec<Option<Cursor>>,
                     emit: &mut dyn FnMut(RecordBatch) -> Result<(), String>|
         -> Result<(), String> {
            if picked.is_empty() {
                return Ok(());
            }
            let columns = (0..sources[0].num_columns())
                .map(|column| {
                    let arrays: Vec<&dyn Array> = sources
                        .iter()
                        .map(|batch| batch.column(column).as_ref())
                        .collect();
                    arrow_select::interleave::interleave(&arrays, picked)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| format!("Sorted runs cannot be merged: {err}"))?;
            let batch = RecordBatch::try_new(sources[0].schema(), columns)
                .map_err(|err| format!("Sorted runs cannot be merged: {err}"))?;
            picked.clear();
            // Only the batches still being merged are held on.
            let mut kept = Vec::new();
            for cursor in cursors.iter_mut().flatten() {
                kept.push(sources[cursor.source].clone());
                cursor.source = kept.len() - 1;
            }
            *sources = kept;
            emit(batch)
        };
        while let Some(Reverse((_, run))) = heap.pop() {
            let cursor = cursors[run]
                .as_mut()
                .expect("a run in the heap has a batch");
            picked.push((cursor.source, cursor.row));
            cursor.row += 1;
            if cursor.row == cursor.keys.num_rows() {
                cursors[run] = None;
                if picked.len() >= OUT_ROWS {
                    flush(&mut picked, &mut sources, &mut cursors, emit)?;
                }
                cursors[run] = next(run, &mut sources, &mut self.keys)?;
            } else if picked.len() >= OUT_ROWS {
                flush(&mut picked, &mut sources, &mut cursors, emit)?;
            }
            if let Some(cursor) = &cursors[run] {
                heap.push(Reverse((
                    cursor.keys.row(cursor.row).as_ref().to_vec(),
                    run,
                )));
            }
        }
        flush(&mut picked, &mut sources, &mut cursors, emit)
    }
}

impl Drop for Sorter<'_> {
    fn drop(&mut self) {
        let now = crate::objects::now_ms();
        for (key, version) in &self.runs {
            let _ = truthdb_object::ObjFile::new(&mut self.storage.lock()).transaction(|tx| {
                tx.delete_object(
                    self.warehouse,
                    key,
                    Some(truthdb_object::VersionSel::Seq(*version)),
                    true,
                    now,
                    super::DROP_GRACE_MS,
                )
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transforms_give_icebergs_values() {
        use arrow_array::{
            BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Int32Array,
            Int64Array, StringArray, TimestampMicrosecondArray,
        };
        let ints = |array: ArrayRef| -> Vec<Option<i64>> {
            if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
                return values.iter().map(|v| v.map(i64::from)).collect();
            }
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ints")
                .iter()
                .collect()
        };
        let bucket = |array: ArrayRef| ints(apply(Transform::Bucket(1_000_003), &array).unwrap());
        // PyIceberg 0.12's bucket[1000003] of each.
        assert_eq!(
            bucket(Arc::new(Int32Array::from(vec![Some(34), Some(-1), None]))),
            [Some(233_328), Some(855_759), None]
        );
        assert_eq!(
            bucket(Arc::new(Int64Array::from(vec![34, -7]))),
            [Some(233_328), Some(274_753)]
        );
        assert_eq!(
            bucket(Arc::new(StringArray::from(vec![
                "iceberg",
                "héllo wörld, longer than a block"
            ]))),
            [Some(996_462), Some(57_237)]
        );
        assert_eq!(
            bucket(Arc::new(Date32Array::from(vec![17_486]))),
            [Some(148_744)]
        );
        assert_eq!(
            bucket(Arc::new(TimestampMicrosecondArray::from(vec![
                1_510_871_468_000_000
            ]))),
            [Some(538_910)]
        );
        assert_eq!(
            bucket(Arc::new(
                Decimal128Array::from(vec![1420])
                    .with_precision_and_scale(9, 2)
                    .unwrap()
            )),
            [Some(724_121)]
        );
        assert_eq!(
            bucket(Arc::new(BinaryArray::from(vec![&[0u8, 1, 2, 3][..]]))),
            [Some(794_567)]
        );
        let uuid = [
            0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7,
            0x85, 0xe7,
        ];
        assert_eq!(
            bucket(Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![uuid].into_iter()).unwrap()
            )),
            [Some(50_876)]
        );
        // truncate[W].
        let truncated = |array: ArrayRef, width| apply(Transform::Truncate(width), &array).unwrap();
        assert_eq!(
            ints(truncated(Arc::new(Int32Array::from(vec![-1, 17])), 10)),
            [Some(-10), Some(10)]
        );
        let text = truncated(Arc::new(StringArray::from(vec!["héllo", "hé"])), 3);
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((text.value(0), text.value(1)), ("hél", "hé"));
        let decimal = truncated(
            Arc::new(
                Decimal128Array::from(vec![-5])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            10,
        );
        assert_eq!(
            decimal
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -10
        );
        // year, month, day and hour, before the epoch too.
        let of = |transform, array: ArrayRef| ints(apply(transform, &array).unwrap());
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![-366, -1, 0, 17_486]));
        assert_eq!(
            of(Transform::Year, Arc::clone(&dates)),
            [Some(-2), Some(-1), Some(0), Some(47)]
        );
        assert_eq!(
            of(Transform::Month, Arc::clone(&dates)),
            [Some(-13), Some(-1), Some(0), Some(574)]
        );
        assert_eq!(
            of(Transform::Day, dates),
            [Some(-366), Some(-1), Some(0), Some(17_486)]
        );
        let stamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            -3_600_000_001,
            -1,
            1_510_871_468_000_000,
        ]));
        assert_eq!(
            of(Transform::Hour, Arc::clone(&stamps)),
            [Some(-2), Some(-1), Some(419_686)]
        );
        assert_eq!(
            of(Transform::Month, Arc::clone(&stamps)),
            [Some(-1), Some(-1), Some(574)]
        );
        assert_eq!(of(Transform::Year, stamps), [Some(-1), Some(-1), Some(47)]);
        assert!(
            apply(
                Transform::Hour,
                &(Arc::new(Date32Array::from(vec![1])) as ArrayRef)
            )
            .is_err()
        );
    }

    #[test]
    fn transforms_are_parsed_as_iceberg_names_them() {
        assert_eq!(Transform::parse("identity"), Some(Transform::Identity));
        assert_eq!(Transform::parse("bucket[16]"), Some(Transform::Bucket(16)));
        assert_eq!(
            Transform::parse("truncate[3]"),
            Some(Transform::Truncate(3))
        );
        assert_eq!(Transform::parse("hour"), Some(Transform::Hour));
        assert_eq!(Transform::parse("void"), Some(Transform::Void));
        for refused in [
            "bucket[0]",
            "bucket[]",
            "truncate[x]",
            "zorder",
            "bucket[16",
        ] {
            assert_eq!(Transform::parse(refused), None, "{refused}");
        }
    }

    #[test]
    fn z_values_interleave_ordered_bytes() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{Field, Schema};
        let id = |n: i32| {
            std::collections::HashMap::from([("PARQUET:field_id".to_string(), n.to_string())])
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true).with_metadata(id(1)),
            Field::new("s", DataType::Utf8, true).with_metadata(id(2)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(-1), Some(0), Some(1), None])),
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("a"),
                    Some(""),
                    None,
                ])),
            ],
        )
        .unwrap();
        let keys = z_values(&batch, &[1]).unwrap();
        let keys = keys
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        // One column: its ordered bytes as they are (a null all zeros).
        assert_eq!(
            keys.value(0),
            [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
        assert_eq!(keys.value(1), [0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(keys.value(3), [0; 8]);
        // Two: their bits interleaved, the first column's first: 0x80 and
        // 'a' (0x61) give 1,0 / 0,1 / 0,1 / 0,0, then 0,0 / 0,0 / 0,0 / 0,1.
        let keys = z_values(&batch, &[1, 2]).unwrap();
        let keys = keys
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(1).len(), 16);
        assert_eq!(&keys.value(1)[..2], [0b1001_0100, 0b0000_0001]);
        // Decimals are not z-ordered.
        let decimals: ArrayRef = Arc::new(
            arrow_array::Decimal128Array::from(vec![1])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        );
        assert!(z_bytes(&decimals).is_err());
    }

    #[test]
    fn a_tables_sort_order_is_read() {
        let metadata = serde_json::json!({
            "default-sort-order-id": 3,
            "sort-orders": [
                {"order-id": 0, "fields": []},
                {"order-id": 3, "fields": [
                    {"transform": "identity", "source-id": 2, "direction": "desc", "null-order": "nulls-first"},
                    {"transform": "bucket[4]", "source-id": 1, "direction": "asc", "null-order": "nulls-last"}
                ]}
            ]
        });
        let (id, fields) = table_order(&metadata).unwrap().unwrap();
        assert_eq!(id, 3);
        assert_eq!(fields.len(), 2);
        assert!(fields[0].descending && fields[0].nulls_first && fields[0].source == 2);
        assert_eq!(fields[1].transform, Transform::Bucket(4));
        assert!(!fields[1].descending && !fields[1].nulls_first);
        let unsorted = serde_json::json!({"default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}]});
        assert!(table_order(&unsorted).unwrap().is_none());
        let unknown = serde_json::json!({"default-sort-order-id": 1, "sort-orders": [{"order-id": 1,
            "fields": [{"transform": "zorder", "source-id": 1, "direction": "asc", "null-order": "nulls-first"}]}]});
        assert!(table_order(&unknown).is_err());
    }
}
