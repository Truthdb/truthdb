//! Conversions between the SQL front end's [`SqlValue`] and the storage
//! layer's typed [`Datum`], plus display rendering for result cells.

use truthdb_sql::SqlValue;
use truthdb_sql::error::SqlError;

use crate::relstore::types::{ColumnType, Datum};

/// A projected result cell: the value used for evaluation/ordering and its
/// rendered form (`None` = SQL NULL).
#[derive(Debug, Clone)]
pub struct Cell {
    pub value: SqlValue,
    pub display: Option<String>,
}

impl Cell {
    pub fn from_datum(datum: &Datum, column_type: &ColumnType) -> Cell {
        Cell {
            value: datum_to_sql(datum),
            display: datum_display(datum, column_type),
        }
    }

    pub fn from_sql(value: SqlValue) -> Cell {
        let display = sql_display(&value);
        Cell { value, display }
    }
}

/// Storage value -> SQL value for expression evaluation. Lossy for types the
/// SQL layer collapses (decimal/temporal), which Stage 3 user tables never
/// use in predicates.
pub fn datum_to_sql(datum: &Datum) -> SqlValue {
    match datum {
        Datum::Null => SqlValue::Null,
        Datum::TinyInt(v) => SqlValue::Int(*v as i64),
        Datum::SmallInt(v) => SqlValue::Int(*v as i64),
        Datum::Int(v) => SqlValue::Int(*v as i64),
        Datum::BigInt(v) => SqlValue::Int(*v),
        Datum::Bit(v) => SqlValue::Bool(*v),
        Datum::Real(v) => SqlValue::Float(*v as f64),
        Datum::Float(v) => SqlValue::Float(*v),
        Datum::Decimal(unscaled) => SqlValue::Float(*unscaled as f64),
        Datum::Date(days) => SqlValue::Int(*days as i64),
        Datum::Time(ticks) => SqlValue::Int(*ticks as i64),
        Datum::DateTime2(days, ticks) => SqlValue::Int((*days as i64) << 40 | *ticks as i64),
        Datum::UniqueIdentifier(bytes) => SqlValue::Str(hex(bytes)),
        Datum::VarChar(s) | Datum::NVarChar(s) => SqlValue::Str(s.clone()),
        Datum::VarBinary(b) => SqlValue::Str(hex(b)),
    }
}

/// SQL value -> storage value for a target column type (INSERT). Enforces
/// integer range and string length with SQL Server error numbers.
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
    let as_int = |min: i64, max: i64| -> Result<i64, SqlError> {
        let v = match value {
            SqlValue::Int(v) => *v,
            // A whole-valued float (this is how a numeric literal that
            // overflowed i64 arrives) must be range-checked BEFORE the cast,
            // which would otherwise saturate silently.
            SqlValue::Float(f) if f.fract() == 0.0 => {
                if !f.is_finite() || *f < min as f64 || *f > max as f64 {
                    return Err(overflow());
                }
                *f as i64
            }
            _ => return Err(clash()),
        };
        if v < min || v > max {
            return Err(overflow());
        }
        Ok(v)
    };
    match column_type {
        ColumnType::TinyInt => Ok(Datum::TinyInt(as_int(0, u8::MAX as i64)? as u8)),
        ColumnType::SmallInt => Ok(Datum::SmallInt(
            as_int(i16::MIN as i64, i16::MAX as i64)? as i16
        )),
        ColumnType::Int => Ok(Datum::Int(as_int(i32::MIN as i64, i32::MAX as i64)? as i32)),
        ColumnType::BigInt => Ok(Datum::BigInt(as_int(i64::MIN, i64::MAX)?)),
        ColumnType::Bit => match value {
            SqlValue::Bool(b) => Ok(Datum::Bit(*b)),
            SqlValue::Int(v) if *v == 0 || *v == 1 => Ok(Datum::Bit(*v == 1)),
            _ => Err(clash()),
        },
        ColumnType::Real => match value {
            SqlValue::Int(v) => Ok(Datum::Real(*v as f32)),
            SqlValue::Float(v) => Ok(Datum::Real(*v as f32)),
            _ => Err(clash()),
        },
        ColumnType::Float => match value {
            SqlValue::Int(v) => Ok(Datum::Float(*v as f64)),
            SqlValue::Float(v) => Ok(Datum::Float(*v)),
            _ => Err(clash()),
        },
        ColumnType::VarChar { max_len } => {
            let SqlValue::Str(s) = value else {
                return Err(clash());
            };
            if s.len() > *max_len as usize {
                return Err(SqlError::string_truncation(column_name));
            }
            Ok(Datum::VarChar(s.clone()))
        }
        ColumnType::NVarChar { max_len } => {
            let SqlValue::Str(s) = value else {
                return Err(clash());
            };
            if s.encode_utf16().count() > *max_len as usize {
                return Err(SqlError::string_truncation(column_name));
            }
            Ok(Datum::NVarChar(s.clone()))
        }
        // Types not creatable in Stage 3 DDL.
        _ => Err(clash()),
    }
}

fn datum_display(datum: &Datum, column_type: &ColumnType) -> Option<String> {
    match datum {
        Datum::Null => None,
        Datum::Bit(b) => Some(if *b { "1" } else { "0" }.to_string()),
        // Floats render through the same formatter as computed expressions,
        // so `SELECT f, f + 0` shows the value identically in both columns.
        Datum::Real(v) => Some(format_float(*v as f64)),
        Datum::Float(v) => Some(format_float(*v)),
        _ => match datum.to_json(column_type) {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s),
            other => Some(other.to_string()),
        },
    }
}

fn sql_display(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Null => None,
        SqlValue::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
        SqlValue::Int(v) => Some(v.to_string()),
        SqlValue::Float(v) => Some(format_float(*v)),
        SqlValue::Str(s) => Some(s.clone()),
    }
}

fn format_float(v: f64) -> String {
    if v == v.trunc() && v.is_finite() && v.abs() < 1e15 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
