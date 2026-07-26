use crate::ast::DataType;
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::value::SqlValue;

/// CAST/CONVERT: converts a value to a target [`DataType`], producing a value
/// of that type. Numeric overflow is 8115; a failed parse is 241.
#[inline(never)]
pub(super) fn cast_value(value: SqlValue, target: &DataType) -> SqlResult<SqlValue> {
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    let overflow = || {
        SqlError::new(
            8115,
            16,
            2,
            format!(
                "Arithmetic overflow error converting to data type {}.",
                type_label(target)
            ),
        )
    };
    let cfail = |t: &str| {
        SqlError::message_only(
            241,
            format!("Conversion failed when converting to data type {t}."),
        )
    };
    match target {
        DataType::TinyInt => cast_int(&value, 0, u8::MAX as i64, overflow),
        DataType::SmallInt => cast_int(&value, i16::MIN as i64, i16::MAX as i64, overflow),
        DataType::Int => cast_int(&value, i32::MIN as i64, i32::MAX as i64, overflow),
        DataType::BigInt => cast_int(&value, i64::MIN, i64::MAX, overflow),
        DataType::Bit => Ok(SqlValue::Bool(
            cast_to_i64(&value).ok_or_else(|| cfail("bit"))? != 0,
        )),
        DataType::Real => Ok(SqlValue::Float(
            cast_to_f64(&value).ok_or_else(|| cfail("real"))? as f32 as f64,
        )),
        DataType::Float => Ok(SqlValue::Float(
            cast_to_f64(&value).ok_or_else(|| cfail("float"))?,
        )),
        DataType::Decimal { precision, scale } => {
            let d = cast_to_decimal(&value).ok_or_else(|| cfail("decimal"))?;
            d.coerce(*precision, *scale)
                .map(|d| SqlValue::Decimal(Box::new(d)))
                .map_err(|_| overflow())
        }
        DataType::VarChar(n) | DataType::NVarChar(n) => {
            // CAST to a char type truncates silently.
            let s: String = cast_to_string(&value).chars().take(*n as usize).collect();
            Ok(SqlValue::Str(s))
        }
        // (MAX): no cap to truncate to.
        DataType::VarCharMax | DataType::NVarCharMax => Ok(SqlValue::Str(cast_to_string(&value))),
        DataType::Date => match &value {
            SqlValue::Date(d) => Ok(SqlValue::Date(*d)),
            SqlValue::DateTime2(d, _) => Ok(SqlValue::Date(*d)),
            SqlValue::Str(s) => crate::temporal::parse_date(s)
                .map(SqlValue::Date)
                .ok_or_else(|| cfail("date")),
            _ => Err(cfail("date")),
        },
        DataType::Time => match &value {
            SqlValue::Time(t) => Ok(SqlValue::Time(*t)),
            SqlValue::DateTime2(_, t) => Ok(SqlValue::Time(*t)),
            SqlValue::Str(s) => crate::temporal::parse_time(s)
                .map(SqlValue::Time)
                .ok_or_else(|| cfail("time")),
            _ => Err(cfail("time")),
        },
        DataType::DateTime2 => match &value {
            SqlValue::DateTime2(d, t) => Ok(SqlValue::DateTime2(*d, *t)),
            SqlValue::Date(d) => Ok(SqlValue::DateTime2(*d, 0)),
            SqlValue::Str(s) => crate::temporal::parse_datetime2(s)
                .map(|(d, t)| SqlValue::DateTime2(d, t))
                .ok_or_else(|| cfail("datetime2")),
            _ => Err(cfail("datetime2")),
        },
        DataType::UniqueIdentifier => match &value {
            SqlValue::Guid(b) => Ok(SqlValue::Guid(*b)),
            SqlValue::Str(s) => crate::guid::parse(s)
                .map(SqlValue::Guid)
                .ok_or_else(|| cfail("uniqueidentifier")),
            _ => Err(cfail("uniqueidentifier")),
        },
        DataType::VarBinary(n) => match &value {
            SqlValue::Binary(b) => Ok(SqlValue::Binary(
                b.iter().take(*n as usize).copied().collect(),
            )),
            _ => Err(cfail("varbinary")),
        },
        DataType::VarBinaryMax => match &value {
            SqlValue::Binary(b) => Ok(SqlValue::Binary(b.clone())),
            _ => Err(cfail("varbinary")),
        },
    }
}

fn type_label(target: &DataType) -> &'static str {
    match target {
        DataType::TinyInt => "tinyint",
        DataType::SmallInt => "smallint",
        DataType::Int => "int",
        DataType::BigInt => "bigint",
        DataType::Bit => "bit",
        DataType::Real => "real",
        DataType::Float => "float",
        DataType::Decimal { .. } => "decimal",
        DataType::Date => "date",
        DataType::Time => "time",
        DataType::DateTime2 => "datetime2",
        DataType::UniqueIdentifier => "uniqueidentifier",
        DataType::VarChar(_) | DataType::VarCharMax => "varchar",
        DataType::NVarChar(_) | DataType::NVarCharMax => "nvarchar",
        DataType::VarBinary(_) | DataType::VarBinaryMax => "varbinary",
    }
}

fn cast_int(
    value: &SqlValue,
    min: i64,
    max: i64,
    overflow: impl Fn() -> SqlError,
) -> SqlResult<SqlValue> {
    let v = cast_to_i64(value).ok_or_else(|| {
        SqlError::message_only(
            245,
            format!(
                "Conversion failed converting {} to an integer.",
                value.type_name()
            ),
        )
    })?;
    if v < min || v > max {
        return Err(overflow());
    }
    Ok(SqlValue::Int(v))
}

fn cast_to_i64(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Int(v) => Some(*v),
        SqlValue::Bool(b) => Some(*b as i64),
        // CAST to an integer type truncates toward zero (SQL Server); a float
        // out of i64 range fails rather than saturating.
        SqlValue::Float(f) => {
            let t = f.trunc();
            (t.is_finite() && t >= i64::MIN as f64 && t <= i64::MAX as f64).then_some(t as i64)
        }
        SqlValue::Decimal(d) => i64::try_from(d.truncated_to_int()).ok(),
        SqlValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn cast_to_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Int(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v),
        SqlValue::Bool(b) => Some(*b as i64 as f64),
        SqlValue::Decimal(d) => Some(d.to_f64()),
        SqlValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn cast_to_decimal(value: &SqlValue) -> Option<Decimal> {
    match value {
        SqlValue::Decimal(d) => Some(**d),
        SqlValue::Int(v) => Some(Decimal::from_i64(*v)),
        SqlValue::Bool(b) => Some(Decimal::from_i64(*b as i64)),
        SqlValue::Str(s) => Decimal::parse(s),
        SqlValue::Float(f) => Decimal::parse(&format!("{f}")),
        _ => None,
    }
}

fn cast_to_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Str(s) => s.clone(),
        SqlValue::Int(v) => v.to_string(),
        SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Float(f) => format!("{f}"),
        SqlValue::Decimal(d) => d.render(),
        SqlValue::Date(days) => crate::temporal::render_date(*days),
        SqlValue::Time(t) => crate::temporal::render_time(*t),
        SqlValue::DateTime2(d, t) => crate::temporal::render_datetime2(*d, *t),
        SqlValue::Guid(b) => crate::guid::render(b),
        SqlValue::Binary(_) => String::new(),
        SqlValue::Null => String::new(),
    }
}
