//! Conversions between the SQL front end's [`SqlValue`] and the storage
//! layer's typed [`Datum`], plus display rendering for result cells. The
//! target-type conversion here is also the implicit-conversion ladder used by
//! INSERT/UPDATE (and, indirectly, CAST).

use truthdb_sql::SqlValue;
use truthdb_sql::decimal::Decimal;
use truthdb_sql::error::SqlError;
use truthdb_sql::temporal;

use crate::relstore::types::{ColumnType, Datum};

/// Infers a concrete result column type for a projected (computed) column from
/// its values. Follows the values' natural types with SQL Server-ish
/// precedence; an all-NULL column defaults to INT.
pub fn infer_type(values: &[SqlValue]) -> ColumnType {
    let mut has_str = false;
    let mut has_float = false;
    let mut has_int = false;
    let mut has_bool = false;
    let mut max_len = 1usize;
    let mut decimal_scale: Option<u8> = None;
    // Widest integer-part digit count across every numeric value; a mixed
    // int/decimal column must reserve precision for the largest whole part.
    let mut max_int_digits = 1u8;
    let mut temporal_type: Option<ColumnType> = None;
    let mut max_bin = 1usize;
    for value in values {
        match value {
            SqlValue::Str(s) => {
                has_str = true;
                max_len = max_len.max(s.encode_utf16().count().max(1));
            }
            SqlValue::Float(_) => has_float = true,
            SqlValue::Int(v) => {
                has_int = true;
                max_int_digits = max_int_digits.max(int_digits(*v));
            }
            SqlValue::Bool(_) => has_bool = true,
            SqlValue::Decimal(d) => {
                decimal_scale = Some(decimal_scale.unwrap_or(0).max(d.scale));
                max_int_digits = max_int_digits.max(d.precision.saturating_sub(d.scale).max(1));
            }
            SqlValue::Date(_) => temporal_type = Some(ColumnType::Date),
            SqlValue::Time(_) => temporal_type = Some(ColumnType::Time),
            SqlValue::DateTime2(..) => temporal_type = Some(ColumnType::DateTime2),
            SqlValue::Guid(_) => temporal_type = Some(ColumnType::UniqueIdentifier),
            SqlValue::Binary(b) => {
                max_bin = max_bin.max(b.len().max(1));
                temporal_type = Some(ColumnType::VarBinary {
                    max_len: max_bin.min(8000) as u16,
                });
            }
            SqlValue::Null => {}
        }
    }
    if has_str {
        ColumnType::NVarChar {
            max_len: max_len.min(4000) as u16,
        }
    } else if has_float {
        ColumnType::Float
    } else if let Some(scale) = decimal_scale {
        // Precision must cover the widest integral part plus the scale.
        let precision = (max_int_digits as u16 + scale as u16).clamp(1, 38) as u8;
        ColumnType::Decimal {
            precision: precision.max(scale),
            scale,
        }
    } else if has_int {
        ColumnType::BigInt
    } else if has_bool {
        ColumnType::Bit
    } else if let Some(t) = temporal_type {
        t
    } else {
        ColumnType::Int
    }
}

/// Renders a datum to its result-cell display string (`None` = NULL).
pub fn display(datum: &Datum, column_type: &ColumnType) -> Option<String> {
    datum_display(datum, column_type)
}

/// Storage value -> SQL value for expression evaluation. Needs the column type
/// so a DECIMAL's scale (which the datum does not carry) is restored.
pub fn datum_to_sql(datum: &Datum, column_type: &ColumnType) -> SqlValue {
    match datum {
        Datum::Null => SqlValue::Null,
        // Resolved by every storage read path before rows escape; reaching
        // the executor means a resolve hook was missed.
        Datum::OverflowRef { .. } => {
            unreachable!("overflow reference escaped the storage layer")
        }
        Datum::TinyInt(v) => SqlValue::Int(*v as i64),
        Datum::SmallInt(v) => SqlValue::Int(*v as i64),
        Datum::Int(v) => SqlValue::Int(*v as i64),
        Datum::BigInt(v) => SqlValue::Int(*v),
        Datum::Bit(v) => SqlValue::Bool(*v),
        Datum::Real(v) => SqlValue::Float(*v as f64),
        Datum::Float(v) => SqlValue::Float(*v),
        Datum::Decimal(unscaled) => {
            let (precision, scale) = match column_type {
                ColumnType::Decimal { precision, scale } => (*precision, *scale),
                _ => (38, 0),
            };
            SqlValue::Decimal(Box::new(Decimal::new(*unscaled, precision, scale)))
        }
        Datum::Date(days) => SqlValue::Date(*days),
        Datum::Time(ticks) => SqlValue::Time(*ticks),
        Datum::DateTime2(days, ticks) => SqlValue::DateTime2(*days, *ticks),
        Datum::UniqueIdentifier(bytes) => SqlValue::Guid(*bytes),
        Datum::VarChar(s) | Datum::NVarChar(s) => SqlValue::Str(s.clone()),
        Datum::VarBinary(b) => SqlValue::Binary(b.clone()),
    }
}

/// SQL value -> storage value for a target column type (INSERT/UPDATE, and the
/// projection of computed columns). Applies the implicit-conversion ladder and
/// enforces integer range / string length with SQL Server error numbers.
pub fn sql_to_datum(
    value: &SqlValue,
    column_type: &ColumnType,
    column_name: &str,
) -> Result<Datum, SqlError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let clash = || {
        SqlError::conversion(format!(
            "Operand type clash: {} is incompatible with {}",
            value.type_name(),
            column_type.name()
        ))
    };
    // Overflow on assignment/implicit conversion is error 220 in SQL Server
    // (explicit CAST/CONVERT uses 8115).
    let overflow = || {
        SqlError::new(
            220,
            16,
            2,
            format!(
                "Arithmetic overflow error converting expression to data type {}.",
                column_type.name()
            ),
        )
    };
    match column_type {
        ColumnType::TinyInt => Ok(Datum::TinyInt(
            int_in_range(value, 0, u8::MAX as i64, overflow)? as u8,
        )),
        ColumnType::SmallInt => {
            Ok(Datum::SmallInt(
                int_in_range(value, i16::MIN as i64, i16::MAX as i64, overflow)? as i16,
            ))
        }
        ColumnType::Int => {
            Ok(Datum::Int(
                int_in_range(value, i32::MIN as i64, i32::MAX as i64, overflow)? as i32,
            ))
        }
        ColumnType::BigInt => Ok(Datum::BigInt(int_in_range(
            value,
            i64::MIN,
            i64::MAX,
            overflow,
        )?)),
        ColumnType::Bit => match value {
            SqlValue::Bool(b) => Ok(Datum::Bit(*b)),
            SqlValue::Int(v) => Ok(Datum::Bit(*v != 0)),
            SqlValue::Decimal(d) => Ok(Datum::Bit(!d.is_zero())),
            _ => Err(clash()),
        },
        ColumnType::Real => Ok(Datum::Real(to_f64(value).ok_or_else(clash)? as f32)),
        ColumnType::Float => Ok(Datum::Float(to_f64(value).ok_or_else(clash)?)),
        ColumnType::Decimal { precision, scale } => {
            let dec = to_decimal(value).ok_or_else(clash)?;
            dec.coerce(*precision, *scale)
                .map(|d| Datum::Decimal(d.value))
                .map_err(|_| overflow())
        }
        ColumnType::Date => match value {
            SqlValue::Date(d) => Ok(Datum::Date(*d)),
            SqlValue::DateTime2(d, _) => Ok(Datum::Date(*d)),
            SqlValue::Str(s) => temporal::parse_date(s)
                .map(Datum::Date)
                .ok_or_else(|| convert_fail(s, "date")),
            _ => Err(clash()),
        },
        ColumnType::Time => match value {
            SqlValue::Time(t) => Ok(Datum::Time(*t)),
            SqlValue::DateTime2(_, t) => Ok(Datum::Time(*t)),
            SqlValue::Str(s) => temporal::parse_time(s)
                .map(Datum::Time)
                .ok_or_else(|| convert_fail(s, "time")),
            _ => Err(clash()),
        },
        ColumnType::DateTime2 => match value {
            SqlValue::DateTime2(d, t) => Ok(Datum::DateTime2(*d, *t)),
            SqlValue::Date(d) => Ok(Datum::DateTime2(*d, 0)),
            SqlValue::Str(s) => temporal::parse_datetime2(s)
                .map(|(d, t)| Datum::DateTime2(d, t))
                .ok_or_else(|| convert_fail(s, "datetime2")),
            _ => Err(clash()),
        },
        ColumnType::UniqueIdentifier => match value {
            SqlValue::Guid(b) => Ok(Datum::UniqueIdentifier(*b)),
            SqlValue::Str(s) => parse_guid(s)
                .map(Datum::UniqueIdentifier)
                .ok_or_else(|| convert_fail(s, "uniqueidentifier")),
            _ => Err(clash()),
        },
        ColumnType::VarChar { max_len } => {
            let s = to_string_value(value);
            if s.len() > *max_len as usize {
                return Err(SqlError::string_truncation(column_name));
            }
            Ok(Datum::VarChar(s))
        }
        ColumnType::NVarChar { max_len } => {
            let s = to_string_value(value);
            if s.encode_utf16().count() > *max_len as usize {
                return Err(SqlError::string_truncation(column_name));
            }
            Ok(Datum::NVarChar(s))
        }
        ColumnType::VarBinary { max_len } => match value {
            SqlValue::Binary(b) => {
                if b.len() > *max_len as usize {
                    return Err(SqlError::string_truncation(column_name));
                }
                Ok(Datum::VarBinary(b.clone()))
            }
            _ => Err(clash()),
        },
        // (MAX) types have no declared-length cap: 8152 never fires; the
        // storage layer decides in-row vs overflow.
        ColumnType::VarCharMax => Ok(Datum::VarChar(to_string_value(value))),
        ColumnType::NVarCharMax => Ok(Datum::NVarChar(to_string_value(value))),
        ColumnType::VarBinaryMax => match value {
            SqlValue::Binary(b) => Ok(Datum::VarBinary(b.clone())),
            _ => Err(clash()),
        },
    }
}

/// Converts a value to i64 for an integer target, truncating toward zero
/// (SQL Server assignment/conversion to an integer type truncates), then
/// range-checks. Out-of-range floats/decimals error rather than saturate.
fn int_in_range(
    value: &SqlValue,
    min: i64,
    max: i64,
    overflow: impl Fn() -> SqlError,
) -> Result<i64, SqlError> {
    let v = match value {
        SqlValue::Int(v) => *v,
        SqlValue::Bool(b) => *b as i64,
        SqlValue::Float(f) => {
            let t = f.trunc();
            if !t.is_finite() || t < i64::MIN as f64 || t > i64::MAX as f64 {
                return Err(overflow());
            }
            t as i64
        }
        SqlValue::Decimal(d) => i64::try_from(d.truncated_to_int()).map_err(|_| overflow())?,
        SqlValue::Str(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| convert_fail(s, "int"))?,
        other => {
            return Err(SqlError::conversion(format!(
                "Operand type clash: {} is incompatible with int",
                other.type_name()
            )));
        }
    };
    if v < min || v > max {
        return Err(overflow());
    }
    Ok(v)
}

fn to_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Int(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v),
        SqlValue::Bool(b) => Some(*b as i64 as f64),
        SqlValue::Decimal(d) => Some(d.to_f64()),
        SqlValue::Str(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn to_decimal(value: &SqlValue) -> Option<Decimal> {
    match value {
        SqlValue::Decimal(d) => Some(**d),
        SqlValue::Int(v) => Some(Decimal::from_i64(*v)),
        SqlValue::Bool(b) => Some(Decimal::from_i64(*b as i64)),
        SqlValue::Str(s) => Decimal::parse(s),
        // Float -> decimal via its shortest round-trip text.
        SqlValue::Float(f) => Decimal::parse(&format!("{f}")),
        _ => None,
    }
}

/// Renders any value to its string form (for a character target).
fn to_string_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Str(s) => s.clone(),
        SqlValue::Int(v) => v.to_string(),
        SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Float(f) => format_float(*f),
        SqlValue::Decimal(d) => d.render(),
        SqlValue::Date(days) => temporal::render_date(*days),
        SqlValue::Time(t) => temporal::render_time(*t),
        SqlValue::DateTime2(d, t) => temporal::render_datetime2(*d, *t),
        SqlValue::Guid(b) => render_guid(b),
        SqlValue::Binary(b) => hex(b),
        SqlValue::Null => String::new(),
    }
}

fn convert_fail(text: &str, target: &str) -> SqlError {
    SqlError::conversion(format!(
        "Conversion failed when converting the varchar value '{text}' to data type {target}."
    ))
}

fn datum_display(datum: &Datum, column_type: &ColumnType) -> Option<String> {
    match datum {
        Datum::Null => None,
        Datum::OverflowRef { .. } => {
            unreachable!("overflow reference escaped the storage layer")
        }
        Datum::Bit(b) => Some(if *b { "1" } else { "0" }.to_string()),
        Datum::Real(v) => Some(format_float(*v as f64)),
        Datum::Float(v) => Some(format_float(*v)),
        Datum::Decimal(unscaled) => {
            let scale = match column_type {
                ColumnType::Decimal { scale, .. } => *scale,
                _ => 0,
            };
            Some(Decimal::new(*unscaled, 38, scale).render())
        }
        Datum::Date(days) => Some(temporal::render_date(*days)),
        Datum::Time(ticks) => Some(temporal::render_time(*ticks)),
        Datum::DateTime2(days, ticks) => Some(temporal::render_datetime2(*days, *ticks)),
        Datum::UniqueIdentifier(bytes) => Some(render_guid(bytes)),
        Datum::VarChar(s) | Datum::NVarChar(s) => Some(s.clone()),
        Datum::VarBinary(b) => Some(hex(b)),
        Datum::TinyInt(v) => Some(v.to_string()),
        Datum::SmallInt(v) => Some(v.to_string()),
        Datum::Int(v) => Some(v.to_string()),
        Datum::BigInt(v) => Some(v.to_string()),
    }
}

fn format_float(v: f64) -> String {
    if v == v.trunc() && v.is_finite() && v.abs() < 1e15 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

/// Decimal digit count of an integer's magnitude (at least 1).
fn int_digits(v: i64) -> u8 {
    v.unsigned_abs()
        .checked_ilog10()
        .map(|l| l as u8 + 1)
        .unwrap_or(1)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        out.push_str(&format!("{b:02X}"));
    }
    out
}

/// Renders a GUID in SQL Server's canonical 8-4-4-4-12 uppercase form. The
/// first three groups are little-endian in the stored byte order.
fn render_guid(b: &[u8; 16]) -> String {
    format!(
        "{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
        b[3],
        b[2],
        b[1],
        b[0],
        b[5],
        b[4],
        b[7],
        b[6],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

/// Parses a GUID string (with or without braces/hyphens) into the stored byte
/// order (first three groups little-endian).
fn parse_guid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .chars()
        .filter(|c| *c != '-')
        .collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let byte = |i: usize| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok();
    let mut out = [0u8; 16];
    // Reverse the first three groups (LE), copy the rest as-is.
    let order = [3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15];
    for (dst, src) in order.iter().enumerate() {
        out[*src] = byte(dst)?;
    }
    Some(out)
}
