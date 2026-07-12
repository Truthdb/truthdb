//! Storage-level column types and values with T-SQL on-disk encodings.
//!
//! Fixed-width encodings (little-endian unless noted):
//! - TINYINT 1B (u8), SMALLINT 2B (i16), INT 4B (i32), BIGINT 8B (i64)
//! - BIT 1B (0/1)
//! - REAL 4B (f32), FLOAT 8B (f64)
//! - DECIMAL(p<=38, s) 16B: unscaled value as i128
//! - DATE 3B: days since 0001-01-01 (proleptic Gregorian)
//! - TIME 5B: 100ns ticks since midnight (precision 7)
//! - DATETIME2 8B: TIME(7) ticks (5B) then DATE days (3B)
//! - UNIQUEIDENTIFIER 16B: RFC 4122 byte order
//!
//! Variable-width: VARCHAR(n) as UTF-8 bytes (single-byte collations arrive
//! in Stage 5), NVARCHAR(n) as UTF-16LE, VARBINARY(n) raw.

use serde_json::Value as Json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Bit,
    Real,
    Float,
    /// precision <= 38, scale <= precision.
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    Time,
    DateTime2,
    UniqueIdentifier,
    /// max length in bytes.
    VarChar {
        max_len: u16,
    },
    /// max length in characters (stored as UTF-16LE code units).
    NVarChar {
        max_len: u16,
    },
    VarBinary {
        max_len: u16,
    },
}

impl ColumnType {
    /// Size of the fixed-width encoding, or None for variable-width types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::TinyInt | ColumnType::Bit => Some(1),
            ColumnType::SmallInt => Some(2),
            ColumnType::Int | ColumnType::Real => Some(4),
            ColumnType::BigInt | ColumnType::Float => Some(8),
            ColumnType::Decimal { .. } | ColumnType::UniqueIdentifier => Some(16),
            ColumnType::Date => Some(3),
            ColumnType::Time => Some(5),
            ColumnType::DateTime2 => Some(8),
            ColumnType::VarChar { .. }
            | ColumnType::NVarChar { .. }
            | ColumnType::VarBinary { .. } => None,
        }
    }

    /// Parses the debug-command type syntax: `int`, `varchar(30)`,
    /// `decimal(10,2)`, ...
    pub fn parse(spec: &str) -> Result<Self, TypeError> {
        let spec = spec.trim().to_ascii_lowercase();
        let (name, args) = match spec.find('(') {
            Some(open) => {
                let close = spec
                    .rfind(')')
                    .filter(|close| *close > open)
                    .ok_or_else(|| TypeError(format!("unbalanced parens in type '{spec}'")))?;
                let args: Vec<&str> = spec[open + 1..close].split(',').map(str::trim).collect();
                (&spec[..open], args)
            }
            None => (spec.as_str(), Vec::new()),
        };
        let one_arg = |args: &[&str]| -> Result<u16, TypeError> {
            if args.len() != 1 {
                return Err(TypeError(format!("type '{name}' takes one argument")));
            }
            args[0]
                .parse::<u16>()
                .map_err(|_| TypeError(format!("bad length in type '{name}'")))
        };
        match name.trim() {
            "tinyint" => Ok(ColumnType::TinyInt),
            "smallint" => Ok(ColumnType::SmallInt),
            "int" => Ok(ColumnType::Int),
            "bigint" => Ok(ColumnType::BigInt),
            "bit" => Ok(ColumnType::Bit),
            "real" => Ok(ColumnType::Real),
            "float" => Ok(ColumnType::Float),
            "date" => Ok(ColumnType::Date),
            "time" => Ok(ColumnType::Time),
            "datetime2" => Ok(ColumnType::DateTime2),
            "uniqueidentifier" => Ok(ColumnType::UniqueIdentifier),
            "varchar" => Ok(ColumnType::VarChar {
                max_len: one_arg(&args)?,
            }),
            "nvarchar" => Ok(ColumnType::NVarChar {
                max_len: one_arg(&args)?,
            }),
            "varbinary" => Ok(ColumnType::VarBinary {
                max_len: one_arg(&args)?,
            }),
            "decimal" | "numeric" => {
                if args.len() != 2 {
                    return Err(TypeError("decimal takes (precision, scale)".to_string()));
                }
                let precision: u8 = args[0]
                    .parse()
                    .map_err(|_| TypeError("bad decimal precision".to_string()))?;
                let scale: u8 = args[1]
                    .parse()
                    .map_err(|_| TypeError("bad decimal scale".to_string()))?;
                if precision == 0 || precision > 38 || scale > precision {
                    return Err(TypeError(format!(
                        "decimal({precision},{scale}) out of range (p in 1..=38, s <= p)"
                    )));
                }
                Ok(ColumnType::Decimal { precision, scale })
            }
            other => Err(TypeError(format!("unsupported type '{other}'"))),
        }
    }

    pub fn name(&self) -> String {
        match self {
            ColumnType::TinyInt => "tinyint".to_string(),
            ColumnType::SmallInt => "smallint".to_string(),
            ColumnType::Int => "int".to_string(),
            ColumnType::BigInt => "bigint".to_string(),
            ColumnType::Bit => "bit".to_string(),
            ColumnType::Real => "real".to_string(),
            ColumnType::Float => "float".to_string(),
            ColumnType::Date => "date".to_string(),
            ColumnType::Time => "time".to_string(),
            ColumnType::DateTime2 => "datetime2".to_string(),
            ColumnType::UniqueIdentifier => "uniqueidentifier".to_string(),
            ColumnType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
            ColumnType::VarChar { max_len } => format!("varchar({max_len})"),
            ColumnType::NVarChar { max_len } => format!("nvarchar({max_len})"),
            ColumnType::VarBinary { max_len } => format!("varbinary({max_len})"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypeError(pub String);

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A storage-level value. Integer-backed temporal types carry their T-SQL
/// tick encodings directly.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    TinyInt(u8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Bit(bool),
    Real(f32),
    Float(f64),
    /// Unscaled value; scale comes from the column type.
    Decimal(i128),
    /// Days since 0001-01-01.
    Date(u32),
    /// 100ns ticks since midnight.
    Time(u64),
    /// (days since 0001-01-01, 100ns ticks since midnight).
    DateTime2(u32, u64),
    UniqueIdentifier([u8; 16]),
    VarChar(String),
    NVarChar(String),
    VarBinary(Vec<u8>),
}

impl Datum {
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// Fixed-width on-disk encoding (only valid for fixed-width types).
    pub fn encode_fixed(&self, out: &mut Vec<u8>) {
        match self {
            Datum::TinyInt(v) => out.push(*v),
            Datum::SmallInt(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Int(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::BigInt(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Bit(v) => out.push(*v as u8),
            Datum::Real(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Float(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Decimal(v) => out.extend_from_slice(&v.to_le_bytes()),
            Datum::Date(days) => out.extend_from_slice(&days.to_le_bytes()[..3]),
            Datum::Time(ticks) => out.extend_from_slice(&ticks.to_le_bytes()[..5]),
            Datum::DateTime2(days, ticks) => {
                out.extend_from_slice(&ticks.to_le_bytes()[..5]);
                out.extend_from_slice(&days.to_le_bytes()[..3]);
            }
            Datum::UniqueIdentifier(bytes) => out.extend_from_slice(bytes),
            Datum::Null | Datum::VarChar(_) | Datum::NVarChar(_) | Datum::VarBinary(_) => {
                unreachable!("not a fixed-width datum")
            }
        }
    }

    /// Variable-width on-disk encoding (only valid for var-width types).
    pub fn encode_var(&self) -> Vec<u8> {
        match self {
            Datum::VarChar(s) => s.as_bytes().to_vec(),
            Datum::NVarChar(s) => s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect(),
            Datum::VarBinary(b) => b.clone(),
            _ => unreachable!("not a var-width datum"),
        }
    }

    pub fn decode_fixed(column_type: &ColumnType, bytes: &[u8]) -> Result<Datum, TypeError> {
        let want = column_type.fixed_size().expect("fixed-width type");
        if bytes.len() != want {
            return Err(TypeError(format!(
                "fixed datum length mismatch for {}: {} vs {want}",
                column_type.name(),
                bytes.len()
            )));
        }
        Ok(match column_type {
            ColumnType::TinyInt => Datum::TinyInt(bytes[0]),
            ColumnType::SmallInt => Datum::SmallInt(i16::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::Int => Datum::Int(i32::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::BigInt => Datum::BigInt(i64::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::Bit => Datum::Bit(bytes[0] != 0),
            ColumnType::Real => Datum::Real(f32::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::Float => Datum::Float(f64::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::Decimal { .. } => {
                Datum::Decimal(i128::from_le_bytes(bytes.try_into().unwrap()))
            }
            ColumnType::Date => Datum::Date(u24_le(bytes)),
            ColumnType::Time => Datum::Time(u40_le(bytes)),
            ColumnType::DateTime2 => Datum::DateTime2(u24_le(&bytes[5..8]), u40_le(&bytes[0..5])),
            ColumnType::UniqueIdentifier => {
                Datum::UniqueIdentifier(bytes.try_into().expect("16 bytes"))
            }
            _ => unreachable!("fixed_size returned Some for var type"),
        })
    }

    pub fn decode_var(column_type: &ColumnType, bytes: &[u8]) -> Result<Datum, TypeError> {
        Ok(match column_type {
            ColumnType::VarChar { .. } => Datum::VarChar(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| TypeError("invalid utf-8 in varchar".to_string()))?,
            ),
            ColumnType::NVarChar { .. } => {
                if !bytes.len().is_multiple_of(2) {
                    return Err(TypeError("odd nvarchar byte length".to_string()));
                }
                let units: Vec<u16> = bytes
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect();
                Datum::NVarChar(
                    String::from_utf16(&units)
                        .map_err(|_| TypeError("invalid utf-16 in nvarchar".to_string()))?,
                )
            }
            ColumnType::VarBinary { .. } => Datum::VarBinary(bytes.to_vec()),
            _ => unreachable!("not a var-width type"),
        })
    }

    /// Converts a JSON value (debug-command input) into a typed datum,
    /// validating range and length limits.
    pub fn from_json(column_type: &ColumnType, value: &Json) -> Result<Datum, TypeError> {
        if value.is_null() {
            return Ok(Datum::Null);
        }
        let int_in = |min: i64, max: i64| -> Result<i64, TypeError> {
            let v = value
                .as_i64()
                .ok_or_else(|| TypeError(format!("expected integer for {}", column_type.name())))?;
            if v < min || v > max {
                return Err(TypeError(format!(
                    "value {v} out of range for {}",
                    column_type.name()
                )));
            }
            Ok(v)
        };
        let string_in = || -> Result<&str, TypeError> {
            value
                .as_str()
                .ok_or_else(|| TypeError(format!("expected string for {}", column_type.name())))
        };
        match column_type {
            ColumnType::TinyInt => Ok(Datum::TinyInt(int_in(0, u8::MAX as i64)? as u8)),
            ColumnType::SmallInt => Ok(Datum::SmallInt(
                int_in(i16::MIN as i64, i16::MAX as i64)? as i16
            )),
            ColumnType::Int => Ok(Datum::Int(int_in(i32::MIN as i64, i32::MAX as i64)? as i32)),
            ColumnType::BigInt => Ok(Datum::BigInt(int_in(i64::MIN, i64::MAX)?)),
            ColumnType::Bit => match value {
                Json::Bool(b) => Ok(Datum::Bit(*b)),
                Json::Number(_) => Ok(Datum::Bit(int_in(0, 1)? != 0)),
                _ => Err(TypeError("expected bool or 0/1 for bit".to_string())),
            },
            ColumnType::Real => {
                let v = value
                    .as_f64()
                    .ok_or_else(|| TypeError("expected number for real".to_string()))?;
                let narrowed = v as f32;
                if v.is_finite() && !narrowed.is_finite() {
                    return Err(TypeError(format!("value {v} out of range for real")));
                }
                Ok(Datum::Real(narrowed))
            }
            ColumnType::Float => {
                let v = value
                    .as_f64()
                    .ok_or_else(|| TypeError("expected number for float".to_string()))?;
                Ok(Datum::Float(v))
            }
            ColumnType::Decimal { precision, scale } => {
                let text = match value {
                    Json::String(s) => s.clone(),
                    Json::Number(n) => n.to_string(),
                    _ => {
                        return Err(TypeError(
                            "expected number or string for decimal".to_string(),
                        ));
                    }
                };
                parse_decimal(&text, *precision, *scale).map(Datum::Decimal)
            }
            ColumnType::Date => Ok(Datum::Date(parse_date(string_in()?)?)),
            ColumnType::Time => Ok(Datum::Time(parse_time(string_in()?)?)),
            ColumnType::DateTime2 => {
                let raw = string_in()?;
                let split = raw
                    .find([' ', 'T'])
                    .ok_or_else(|| TypeError("expected 'YYYY-MM-DD hh:mm:ss'".to_string()))?;
                let days = parse_date(&raw[..split])?;
                let ticks = parse_time(&raw[split + 1..])?;
                Ok(Datum::DateTime2(days, ticks))
            }
            ColumnType::UniqueIdentifier => Ok(Datum::UniqueIdentifier(parse_guid(string_in()?)?)),
            ColumnType::VarChar { max_len } => {
                let s = string_in()?;
                if s.len() > *max_len as usize {
                    return Err(TypeError(format!(
                        "string of {} bytes exceeds varchar({max_len})",
                        s.len()
                    )));
                }
                Ok(Datum::VarChar(s.to_string()))
            }
            ColumnType::NVarChar { max_len } => {
                let s = string_in()?;
                let units = s.encode_utf16().count();
                if units > *max_len as usize {
                    return Err(TypeError(format!(
                        "string of {units} characters exceeds nvarchar({max_len})"
                    )));
                }
                Ok(Datum::NVarChar(s.to_string()))
            }
            ColumnType::VarBinary { max_len } => {
                let hex = string_in()?;
                // The 0x prefix is debug-command sugar; GUIDs must not have
                // one, so parse_hex itself stays strict.
                let bytes = parse_hex(hex.strip_prefix("0x").unwrap_or(hex))?;
                if bytes.len() > *max_len as usize {
                    return Err(TypeError(format!(
                        "{} bytes exceeds varbinary({max_len})",
                        bytes.len()
                    )));
                }
                Ok(Datum::VarBinary(bytes))
            }
        }
    }

    /// Renders a datum back to JSON for debug-command output.
    pub fn to_json(&self, column_type: &ColumnType) -> Json {
        match self {
            Datum::Null => Json::Null,
            Datum::TinyInt(v) => Json::from(*v),
            Datum::SmallInt(v) => Json::from(*v),
            Datum::Int(v) => Json::from(*v),
            Datum::BigInt(v) => Json::from(*v),
            Datum::Bit(v) => Json::from(*v),
            Datum::Real(v) => Json::from(*v as f64),
            Datum::Float(v) => Json::from(*v),
            Datum::Decimal(unscaled) => {
                let scale = match column_type {
                    ColumnType::Decimal { scale, .. } => *scale,
                    _ => 0,
                };
                Json::String(format_decimal(*unscaled, scale))
            }
            Datum::Date(days) => Json::String(format_date(*days)),
            Datum::Time(ticks) => Json::String(format_time(*ticks)),
            Datum::DateTime2(days, ticks) => {
                Json::String(format!("{} {}", format_date(*days), format_time(*ticks)))
            }
            Datum::UniqueIdentifier(bytes) => Json::String(format_guid(bytes)),
            Datum::VarChar(s) | Datum::NVarChar(s) => Json::String(s.clone()),
            Datum::VarBinary(b) => Json::String(format_hex(b)),
        }
    }
}

fn u24_le(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0])
}

fn u40_le(bytes: &[u8]) -> u64 {
    u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], 0, 0, 0])
}

/// Days since 0001-01-01 for a proleptic-Gregorian civil date
/// (Howard Hinnant's days-from-civil, rebased).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468 // days since 1970-01-01
}

const EPOCH_1970_FROM_0001: i64 = 719_162; // days_from_civil(1970,1,1) - days_from_civil(1,1,1)

fn civil_from_days(days_since_0001: u32) -> (i64, u32, u32) {
    let z = days_since_0001 as i64 - EPOCH_1970_FROM_0001 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn parse_date(text: &str) -> Result<u32, TypeError> {
    let bad = || TypeError(format!("bad date '{text}', expected YYYY-MM-DD"));
    let parts: Vec<&str> = text.trim().split('-').collect();
    if parts.len() != 3 {
        return Err(bad());
    }
    let year: i64 = parts[0].parse().map_err(|_| bad())?;
    let month: u32 = parts[1].parse().map_err(|_| bad())?;
    let day: u32 = parts[2].parse().map_err(|_| bad())?;
    if !(1..=9999).contains(&year) || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(bad());
    }
    let days = days_from_civil(year, month, day) + EPOCH_1970_FROM_0001;
    // Reject non-existent dates like Feb 30 (round-trip check).
    if civil_from_days(days as u32) != (year, month, day) {
        return Err(bad());
    }
    Ok(days as u32)
}

fn format_date(days: u32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

const TICKS_PER_SECOND: u64 = 10_000_000;

fn parse_time(text: &str) -> Result<u64, TypeError> {
    let bad = || TypeError(format!("bad time '{text}', expected hh:mm:ss[.fffffff]"));
    let text = text.trim();
    let (main, frac) = match text.find('.') {
        Some(dot) => (&text[..dot], &text[dot + 1..]),
        None => (text, ""),
    };
    let parts: Vec<&str> = main.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(bad());
    }
    let hours: u64 = parts[0].parse().map_err(|_| bad())?;
    let minutes: u64 = parts[1].parse().map_err(|_| bad())?;
    let seconds: u64 = if parts.len() == 3 {
        parts[2].parse().map_err(|_| bad())?
    } else {
        0
    };
    if hours > 23 || minutes > 59 || seconds > 59 {
        return Err(bad());
    }
    if frac.len() > 7 || !frac.chars().all(|c| c.is_ascii_digit()) {
        return Err(bad());
    }
    let mut frac_ticks = 0u64;
    if !frac.is_empty() {
        frac_ticks = frac.parse::<u64>().map_err(|_| bad())? * 10u64.pow(7 - frac.len() as u32);
    }
    Ok((hours * 3600 + minutes * 60 + seconds) * TICKS_PER_SECOND + frac_ticks)
}

fn format_time(ticks: u64) -> String {
    let seconds_total = ticks / TICKS_PER_SECOND;
    let frac = ticks % TICKS_PER_SECOND;
    let (h, m, s) = (
        seconds_total / 3600,
        (seconds_total / 60) % 60,
        seconds_total % 60,
    );
    if frac == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{h:02}:{m:02}:{s:02}.{frac:07}")
    }
}

fn parse_decimal(text: &str, precision: u8, scale: u8) -> Result<i128, TypeError> {
    let bad = || TypeError(format!("bad decimal '{text}'"));
    let text = text.trim();
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int_part, frac_part) = match rest.find('.') {
        Some(dot) => (&rest[..dot], &rest[dot + 1..]),
        None => (rest, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(bad());
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(bad());
    }
    if frac_part.len() > scale as usize {
        return Err(TypeError(format!(
            "decimal '{text}' has more than {scale} fractional digits"
        )));
    }
    let mut unscaled: i128 = 0;
    for c in int_part.chars().chain(frac_part.chars()) {
        unscaled = unscaled
            .checked_mul(10)
            .and_then(|v| v.checked_add((c as u8 - b'0') as i128))
            .ok_or_else(bad)?;
    }
    for _ in frac_part.len()..scale as usize {
        unscaled = unscaled.checked_mul(10).ok_or_else(bad)?;
    }
    let limit = 10i128.checked_pow(precision as u32).ok_or_else(bad)?;
    if unscaled >= limit {
        return Err(TypeError(format!(
            "decimal '{text}' overflows precision {precision}"
        )));
    }
    Ok(if negative { -unscaled } else { unscaled })
}

fn format_decimal(unscaled: i128, scale: u8) -> String {
    let negative = unscaled < 0;
    let magnitude = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let digits = if magnitude.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - magnitude.len()), magnitude)
    } else {
        magnitude
    };
    let split = digits.len() - scale;
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&digits[..split]);
    if scale > 0 {
        out.push('.');
        out.push_str(&digits[split..]);
    }
    out
}

fn parse_guid(text: &str) -> Result<[u8; 16], TypeError> {
    let bad = || TypeError(format!("bad uniqueidentifier '{text}'"));
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || text.chars().filter(|c| *c == '-').count() != 4 {
        return Err(bad());
    }
    let bytes = parse_hex(&hex).map_err(|_| bad())?;
    bytes.try_into().map_err(|_| bad())
}

fn format_guid(bytes: &[u8; 16]) -> String {
    let hex = format_hex(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn parse_hex(text: &str) -> Result<Vec<u8>, TypeError> {
    // Byte-wise so multi-byte characters cannot cause slicing panics.
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(TypeError("odd hex length".to_string()));
    }
    let nibble = |b: u8| -> Result<u8, TypeError> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err(TypeError(format!("bad hex '{text}'"))),
        }
    };
    bytes
        .chunks_exact(2)
        .map(|pair| Ok(nibble(pair[0])? << 4 | nibble(pair[1])?))
        .collect()
}

fn format_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip_fixed(column_type: ColumnType, datum: Datum) {
        let mut bytes = Vec::new();
        datum.encode_fixed(&mut bytes);
        assert_eq!(bytes.len(), column_type.fixed_size().unwrap());
        assert_eq!(Datum::decode_fixed(&column_type, &bytes).unwrap(), datum);
    }

    #[test]
    fn fixed_encodings_round_trip() {
        round_trip_fixed(ColumnType::TinyInt, Datum::TinyInt(255));
        round_trip_fixed(ColumnType::SmallInt, Datum::SmallInt(-32768));
        round_trip_fixed(ColumnType::Int, Datum::Int(-2_000_000_000));
        round_trip_fixed(ColumnType::BigInt, Datum::BigInt(i64::MIN));
        round_trip_fixed(ColumnType::Bit, Datum::Bit(true));
        round_trip_fixed(ColumnType::Real, Datum::Real(-1.5));
        round_trip_fixed(ColumnType::Float, Datum::Float(std::f64::consts::PI));
        round_trip_fixed(
            ColumnType::Decimal {
                precision: 38,
                scale: 10,
            },
            Datum::Decimal(-1234567890123456789012345678901234567i128),
        );
        round_trip_fixed(ColumnType::Date, Datum::Date(738_000));
        round_trip_fixed(ColumnType::Time, Datum::Time(863_999_999_999));
        round_trip_fixed(
            ColumnType::DateTime2,
            Datum::DateTime2(738_000, 123_456_789),
        );
        round_trip_fixed(
            ColumnType::UniqueIdentifier,
            Datum::UniqueIdentifier([7u8; 16]),
        );
    }

    #[test]
    fn var_encodings_round_trip() {
        let nvar = ColumnType::NVarChar { max_len: 30 };
        let datum = Datum::NVarChar("åäö смеш 😀".to_string());
        let bytes = datum.encode_var();
        assert_eq!(Datum::decode_var(&nvar, &bytes).unwrap(), datum);

        let var = ColumnType::VarChar { max_len: 30 };
        let datum = Datum::VarChar("hello".to_string());
        assert_eq!(Datum::decode_var(&var, &datum.encode_var()).unwrap(), datum);
    }

    #[test]
    fn date_time_parsing_and_formatting() {
        // Known anchors: 0001-01-01 is day 0; 1970-01-01 is day 719162.
        assert_eq!(parse_date("0001-01-01").unwrap(), 0);
        assert_eq!(parse_date("1970-01-01").unwrap(), 719_162);
        assert_eq!(format_date(719_162), "1970-01-01");
        assert_eq!(parse_date("2026-07-12").unwrap(), 739_808);
        assert_eq!(format_date(739_808), "2026-07-12");
        assert!(parse_date("2026-02-30").is_err());
        assert!(parse_date("2026-13-01").is_err());

        assert_eq!(parse_time("00:00:00").unwrap(), 0);
        assert_eq!(
            parse_time("23:59:59.9999999").unwrap(),
            24 * 3600 * TICKS_PER_SECOND - 1
        );
        assert_eq!(
            parse_time("12:30").unwrap(),
            (12 * 3600 + 30 * 60) * TICKS_PER_SECOND
        );
        assert_eq!(
            format_time(parse_time("01:02:03.05").unwrap()),
            "01:02:03.0500000"
        );
        assert!(parse_time("24:00:00").is_err());
    }

    #[test]
    fn decimal_parsing_scale_and_precision() {
        assert_eq!(parse_decimal("123.45", 10, 2).unwrap(), 12345);
        assert_eq!(parse_decimal("-0.5", 10, 2).unwrap(), -50);
        assert_eq!(parse_decimal("7", 10, 2).unwrap(), 700);
        assert!(
            parse_decimal("1.234", 10, 2).is_err(),
            "too many frac digits"
        );
        assert!(
            parse_decimal("123456789", 10, 2).is_err(),
            "overflows p=10 s=2"
        );
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(-50, 2), "-0.50");
        assert_eq!(format_decimal(700, 0), "700");
    }

    #[test]
    fn guid_round_trip() {
        let guid = parse_guid("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        assert_eq!(format_guid(&guid), "01234567-89ab-cdef-0123-456789abcdef");
        assert!(parse_guid("not-a-guid").is_err());
        // Review findings: these inputs used to panic.
        assert!(parse_guid("0x234567-89ab-cdef-0123-456789abcdef").is_err());
        assert!(parse_hex("åä").is_err());
        assert!(ColumnType::parse("int)x(").is_err());
        // REAL overflow must error, not become infinity.
        assert!(Datum::from_json(&ColumnType::Real, &serde_json::json!(1e300)).is_err());
    }

    #[test]
    fn json_conversion_validates_ranges() {
        let int = ColumnType::Int;
        assert_eq!(Datum::from_json(&int, &json!(42)).unwrap(), Datum::Int(42));
        assert!(Datum::from_json(&int, &json!(3_000_000_000i64)).is_err());
        assert_eq!(Datum::from_json(&int, &Json::Null).unwrap(), Datum::Null);

        let varchar = ColumnType::VarChar { max_len: 3 };
        assert!(Datum::from_json(&varchar, &json!("toolong")).is_err());

        let bit = ColumnType::Bit;
        assert_eq!(
            Datum::from_json(&bit, &json!(true)).unwrap(),
            Datum::Bit(true)
        );
        assert_eq!(
            Datum::from_json(&bit, &json!(0)).unwrap(),
            Datum::Bit(false)
        );

        let varbinary = ColumnType::VarBinary { max_len: 8 };
        assert_eq!(
            Datum::from_json(&varbinary, &json!("0xdeadbeef")).unwrap(),
            Datum::VarBinary(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    #[test]
    fn type_spec_parsing() {
        assert_eq!(ColumnType::parse("INT").unwrap(), ColumnType::Int);
        assert_eq!(
            ColumnType::parse("varchar(30)").unwrap(),
            ColumnType::VarChar { max_len: 30 }
        );
        assert_eq!(
            ColumnType::parse("decimal(10, 2)").unwrap(),
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert!(ColumnType::parse("decimal(40,2)").is_err());
        assert!(ColumnType::parse("blob").is_err());
    }
}
