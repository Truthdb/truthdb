//! Built-in scalar functions evaluated over already-evaluated argument values.
//! These are pure (no clock/session state); GETDATE/SYSDATETIME and
//! SCOPE_IDENTITY, which need a session context, arrive with sessions.

use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::temporal;
use crate::value::SqlValue;

pub fn eval_function(name: &str, args: Vec<SqlValue>) -> SqlResult<SqlValue> {
    let upper = name.to_ascii_uppercase();
    match upper.as_str() {
        "ISNULL" => isnull(&args),
        "COALESCE" => coalesce(&args),
        "IIF" => iif(&args),
        "NULLIF" => nullif(&args),
        "LEN" => str1(&args, "LEN", |s| SqlValue::Int(s.chars().count() as i64)),
        "DATALENGTH" => datalength(&args),
        "UPPER" => str1(&args, "UPPER", |s| SqlValue::Str(s.to_uppercase())),
        "LOWER" => str1(&args, "LOWER", |s| SqlValue::Str(s.to_lowercase())),
        "LTRIM" => str1(&args, "LTRIM", |s| {
            SqlValue::Str(s.trim_start().to_string())
        }),
        "RTRIM" => str1(&args, "RTRIM", |s| SqlValue::Str(s.trim_end().to_string())),
        "TRIM" => str1(&args, "TRIM", |s| SqlValue::Str(s.trim().to_string())),
        "REVERSE" => str1(&args, "REVERSE", |s| {
            SqlValue::Str(s.chars().rev().collect())
        }),
        "LEFT" => left_right(&args, true),
        "RIGHT" => left_right(&args, false),
        "SUBSTRING" => substring(&args),
        "CHARINDEX" => charindex(&args),
        "REPLACE" => replace(&args),
        "REPLICATE" => replicate(&args),
        "CONCAT" => concat(&args),
        "ABS" => num1(&args, "ABS", f64::abs, i64::wrapping_abs),
        "CEILING" => num1(&args, "CEILING", f64::ceil, |v| v),
        "FLOOR" => num1(&args, "FLOOR", f64::floor, |v| v),
        "SIGN" => num1(&args, "SIGN", f64::signum, i64::signum),
        "SQRT" => float1(&args, "SQRT", f64::sqrt),
        "ROUND" => round(&args),
        "POWER" => power(&args),
        "YEAR" => date_part_fn(&args, DatePart::Year),
        "MONTH" => date_part_fn(&args, DatePart::Month),
        "DAY" => date_part_fn(&args, DatePart::Day),
        "DATEPART" => datepart(&args),
        "DATEADD" => dateadd(&args),
        "DATEDIFF" => datediff(&args),
        _ => Err(SqlError::message_only(
            195,
            format!("'{name}' is not a recognized built-in function name."),
        )),
    }
}

fn arity(name: &str, args: &[SqlValue], n: usize) -> SqlResult<()> {
    if args.len() != n {
        return Err(SqlError::message_only(
            8144,
            format!("The {name} function requires {n} argument(s)."),
        ));
    }
    Ok(())
}

fn isnull(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("ISNULL", args, 2)?;
    Ok(if args[0].is_null() {
        args[1].clone()
    } else {
        args[0].clone()
    })
}

fn coalesce(args: &[SqlValue]) -> SqlResult<SqlValue> {
    if args.is_empty() {
        return Err(SqlError::message_only(
            174,
            "The COALESCE function requires at least 1 argument.".to_string(),
        ));
    }
    Ok(args
        .iter()
        .find(|v| !v.is_null())
        .cloned()
        .unwrap_or(SqlValue::Null))
}

fn iif(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("IIF", args, 3)?;
    Ok(match args[0] {
        SqlValue::Bool(true) => args[1].clone(),
        _ => args[2].clone(),
    })
}

fn nullif(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("NULLIF", args, 2)?;
    if args[0].is_null() {
        return Ok(SqlValue::Null);
    }
    match args[0].compare(&args[1])? {
        Some(std::cmp::Ordering::Equal) => Ok(SqlValue::Null),
        _ => Ok(args[0].clone()),
    }
}

fn as_string(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Str(s) => Some(s.clone()),
        SqlValue::Int(i) => Some(i.to_string()),
        SqlValue::Decimal(d) => Some(d.render()),
        SqlValue::Float(f) => Some(format!("{f}")),
        SqlValue::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
        _ => None,
    }
}

fn as_i64(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Int(i) => Some(*i),
        SqlValue::Bool(b) => Some(*b as i64),
        SqlValue::Decimal(d) => d.rescaled(0).and_then(|x| i64::try_from(x).ok()),
        SqlValue::Float(f) => Some(f.round() as i64),
        SqlValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn as_f64(v: &SqlValue) -> Option<f64> {
    match v {
        SqlValue::Int(i) => Some(*i as f64),
        SqlValue::Float(f) => Some(*f),
        SqlValue::Decimal(d) => Some(d.to_f64()),
        SqlValue::Bool(b) => Some(*b as i64 as f64),
        SqlValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn convert_err(v: &SqlValue) -> SqlError {
    SqlError::conversion(format!(
        "cannot convert {} for this function",
        v.type_name()
    ))
}

/// A one-string-argument function; NULL propagates.
fn str1(args: &[SqlValue], name: &str, f: impl Fn(&str) -> SqlValue) -> SqlResult<SqlValue> {
    arity(name, args, 1)?;
    if args[0].is_null() {
        return Ok(SqlValue::Null);
    }
    let s = as_string(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    Ok(f(&s))
}

fn datalength(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("DATALENGTH", args, 1)?;
    Ok(match &args[0] {
        SqlValue::Null => SqlValue::Null,
        SqlValue::Str(s) => SqlValue::Int(s.len() as i64),
        SqlValue::Binary(b) => SqlValue::Int(b.len() as i64),
        other => SqlValue::Int(as_string(other).map(|s| s.len()).unwrap_or(0) as i64),
    })
}

fn left_right(args: &[SqlValue], left: bool) -> SqlResult<SqlValue> {
    let name = if left { "LEFT" } else { "RIGHT" };
    arity(name, args, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(SqlValue::Null);
    }
    let s = as_string(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    let n = as_i64(&args[1])
        .ok_or_else(|| convert_err(&args[1]))?
        .max(0) as usize;
    let chars: Vec<char> = s.chars().collect();
    let take = n.min(chars.len());
    let slice: String = if left {
        chars[..take].iter().collect()
    } else {
        chars[chars.len() - take..].iter().collect()
    };
    Ok(SqlValue::Str(slice))
}

fn substring(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("SUBSTRING", args, 3)?;
    if args.iter().any(|a| a.is_null()) {
        return Ok(SqlValue::Null);
    }
    let s = as_string(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    let start = as_i64(&args[1]).ok_or_else(|| convert_err(&args[1]))?;
    let len = as_i64(&args[2]).ok_or_else(|| convert_err(&args[2]))?;
    let chars: Vec<char> = s.chars().collect();
    // SQL Server is 1-based; a start < 1 shrinks the effective length.
    let end = start.saturating_add(len);
    let from = start.max(1) as usize;
    let to = end.max(1) as usize;
    if from >= to {
        return Ok(SqlValue::Str(String::new()));
    }
    let lo = (from - 1).min(chars.len());
    let hi = (to - 1).min(chars.len());
    Ok(SqlValue::Str(chars[lo..hi].iter().collect()))
}

fn charindex(args: &[SqlValue]) -> SqlResult<SqlValue> {
    if args.len() != 2 && args.len() != 3 {
        return Err(SqlError::message_only(
            174,
            "The CHARINDEX function requires 2 or 3 arguments.".to_string(),
        ));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(SqlValue::Null);
    }
    let needle = as_string(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    let haystack = as_string(&args[1]).ok_or_else(|| convert_err(&args[1]))?;
    let start = match args.get(2) {
        Some(v) => as_i64(v).unwrap_or(1).max(1) as usize,
        None => 1,
    };
    let hay_chars: Vec<char> = haystack.chars().collect();
    let from = (start - 1).min(hay_chars.len());
    let tail: String = hay_chars[from..].iter().collect();
    let pos = match tail.find(&needle) {
        Some(byte_off) => {
            // Convert byte offset to a 1-based char position in the whole string.
            let char_off = tail[..byte_off].chars().count();
            (from + char_off + 1) as i64
        }
        None => 0,
    };
    Ok(SqlValue::Int(pos))
}

fn replace(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("REPLACE", args, 3)?;
    if args.iter().any(|a| a.is_null()) {
        return Ok(SqlValue::Null);
    }
    let s = as_string(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    let from = as_string(&args[1]).ok_or_else(|| convert_err(&args[1]))?;
    let to = as_string(&args[2]).ok_or_else(|| convert_err(&args[2]))?;
    if from.is_empty() {
        return Ok(SqlValue::Str(s));
    }
    Ok(SqlValue::Str(s.replace(&from, &to)))
}

fn replicate(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("REPLICATE", args, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(SqlValue::Null);
    }
    let s = as_string(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    let n = as_i64(&args[1]).ok_or_else(|| convert_err(&args[1]))?;
    if n < 0 {
        return Ok(SqlValue::Null);
    }
    Ok(SqlValue::Str(s.repeat(n as usize)))
}

fn concat(args: &[SqlValue]) -> SqlResult<SqlValue> {
    if args.len() < 2 {
        return Err(SqlError::message_only(
            189,
            "The CONCAT function requires 2 to 254 arguments.".to_string(),
        ));
    }
    // CONCAT treats NULL as an empty string.
    let mut out = String::new();
    for a in args {
        if let Some(s) = as_string(a) {
            out.push_str(&s);
        }
    }
    Ok(SqlValue::Str(out))
}

/// One numeric argument, keeping the value's family (int stays int, float stays
/// float, decimal handled via float then back is lossy so decimals go float).
fn num1(
    args: &[SqlValue],
    name: &str,
    ff: impl Fn(f64) -> f64,
    fi: impl Fn(i64) -> i64,
) -> SqlResult<SqlValue> {
    arity(name, args, 1)?;
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Int(i) => Ok(SqlValue::Int(fi(*i))),
        SqlValue::Float(f) => Ok(SqlValue::Float(ff(*f))),
        SqlValue::Decimal(d) => {
            let r = ff(d.to_f64());
            Ok(SqlValue::Decimal(Box::new(
                Decimal::parse(&format!("{r:.*}", d.scale as usize)).unwrap_or(**d),
            )))
        }
        other => Ok(SqlValue::Float(ff(
            as_f64(other).ok_or_else(|| convert_err(other))?
        ))),
    }
}

fn float1(args: &[SqlValue], name: &str, f: impl Fn(f64) -> f64) -> SqlResult<SqlValue> {
    arity(name, args, 1)?;
    if args[0].is_null() {
        return Ok(SqlValue::Null);
    }
    let v = as_f64(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    Ok(SqlValue::Float(f(v)))
}

fn round(args: &[SqlValue]) -> SqlResult<SqlValue> {
    if args.len() != 2 && args.len() != 3 {
        return Err(SqlError::message_only(
            174,
            "The ROUND function requires 2 or 3 arguments.".to_string(),
        ));
    }
    if args[0].is_null() {
        return Ok(SqlValue::Null);
    }
    let places = as_i64(&args[1]).unwrap_or(0);
    let factor = 10f64.powi(places as i32);
    let rounded = |v: f64| (v * factor).round() / factor;
    match &args[0] {
        SqlValue::Float(f) => Ok(SqlValue::Float(rounded(*f))),
        SqlValue::Int(i) => Ok(SqlValue::Int(*i)),
        other => {
            let v = as_f64(other).ok_or_else(|| convert_err(other))?;
            Ok(SqlValue::Float(rounded(v)))
        }
    }
}

fn power(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("POWER", args, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(SqlValue::Null);
    }
    let base = as_f64(&args[0]).ok_or_else(|| convert_err(&args[0]))?;
    let exp = as_f64(&args[1]).ok_or_else(|| convert_err(&args[1]))?;
    Ok(SqlValue::Float(base.powf(exp)))
}

enum DatePart {
    Year,
    Month,
    Day,
}

fn ymd_of(v: &SqlValue) -> SqlResult<Option<(i64, u32, u32)>> {
    let days = match v {
        SqlValue::Null => return Ok(None),
        SqlValue::Date(d) => *d,
        SqlValue::DateTime2(d, _) => *d,
        SqlValue::Str(s) => temporal::parse_date(s)
            .ok_or_else(|| SqlError::conversion(format!("cannot convert '{s}' to date")))?,
        other => return Err(convert_err(other)),
    };
    Ok(Some(temporal::ymd_from_days(days)))
}

fn date_part_fn(args: &[SqlValue], part: DatePart) -> SqlResult<SqlValue> {
    arity(
        match part {
            DatePart::Year => "YEAR",
            DatePart::Month => "MONTH",
            DatePart::Day => "DAY",
        },
        args,
        1,
    )?;
    match ymd_of(&args[0])? {
        None => Ok(SqlValue::Null),
        Some((y, m, d)) => Ok(SqlValue::Int(match part {
            DatePart::Year => y,
            DatePart::Month => m as i64,
            DatePart::Day => d as i64,
        })),
    }
}

fn datepart(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("DATEPART", args, 2)?;
    let part = as_string(&args[0])
        .ok_or_else(|| convert_err(&args[0]))?
        .to_ascii_lowercase();
    match part.as_str() {
        "year" | "yy" | "yyyy" => date_part_fn(&args[1..], DatePart::Year),
        "month" | "mm" | "m" => date_part_fn(&args[1..], DatePart::Month),
        "day" | "dd" | "d" => date_part_fn(&args[1..], DatePart::Day),
        other => Err(SqlError::message_only(
            9810,
            format!("The datepart {other} is not supported."),
        )),
    }
}

fn dateadd(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("DATEADD", args, 3)?;
    let part = as_string(&args[0])
        .ok_or_else(|| convert_err(&args[0]))?
        .to_ascii_lowercase();
    let n = as_i64(&args[1]).ok_or_else(|| convert_err(&args[1]))?;
    let (days, ticks, was_datetime) = match &args[2] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Date(d) => (*d as i64, 0i64, false),
        SqlValue::DateTime2(d, t) => (*d as i64, *t as i64, true),
        SqlValue::Str(s) => {
            let (d, t) = temporal::parse_datetime2(s)
                .ok_or_else(|| SqlError::conversion(format!("cannot convert '{s}' to datetime")))?;
            (d as i64, t as i64, t != 0)
        }
        other => return Err(convert_err(other)),
    };
    let ticks_per_day = temporal::TICKS_PER_DAY as i64;
    let (mut total_days, mut total_ticks) = (days, ticks);
    match part.as_str() {
        "year" | "yy" | "yyyy" => {
            let (y, m, d) = temporal::ymd_from_days(total_days as u32);
            let nd = temporal::days_from_ymd(y + n, m, d.min(28)).ok_or_else(date_overflow)?;
            total_days = nd as i64;
        }
        "month" | "mm" | "m" => {
            let (y, m, d) = temporal::ymd_from_days(total_days as u32);
            let mut total_month = (y * 12 + (m as i64 - 1)) + n;
            let ny = total_month.div_euclid(12);
            total_month = total_month.rem_euclid(12);
            let nd = temporal::days_from_ymd(ny, total_month as u32 + 1, d.min(28))
                .ok_or_else(date_overflow)?;
            total_days = nd as i64;
        }
        "day" | "dd" | "d" => total_days += n,
        "hour" | "hh" => total_ticks += n * 3600 * temporal::TICKS_PER_SEC as i64,
        "minute" | "mi" | "n" => total_ticks += n * 60 * temporal::TICKS_PER_SEC as i64,
        "second" | "ss" | "s" => total_ticks += n * temporal::TICKS_PER_SEC as i64,
        other => {
            return Err(SqlError::message_only(
                9810,
                format!("The datepart {other} is not supported."),
            ));
        }
    }
    // Normalize ticks into [0, ticks_per_day) carrying into days.
    total_days += total_ticks.div_euclid(ticks_per_day);
    total_ticks = total_ticks.rem_euclid(ticks_per_day);
    if !(0..=(u32::MAX as i64)).contains(&total_days) {
        return Err(date_overflow());
    }
    if was_datetime || total_ticks != 0 {
        Ok(SqlValue::DateTime2(total_days as u32, total_ticks as u64))
    } else {
        Ok(SqlValue::Date(total_days as u32))
    }
}

fn date_overflow() -> SqlError {
    SqlError::message_only(
        517,
        "Adding a value to a date produced an out-of-range date.".to_string(),
    )
}

fn datediff(args: &[SqlValue]) -> SqlResult<SqlValue> {
    arity("DATEDIFF", args, 3)?;
    let part = as_string(&args[0])
        .ok_or_else(|| convert_err(&args[0]))?
        .to_ascii_lowercase();
    let start = datetime_of(&args[1])?;
    let end = datetime_of(&args[2])?;
    let (Some((sd, st)), Some((ed, et))) = (start, end) else {
        return Ok(SqlValue::Null);
    };
    let ticks_per_day = temporal::TICKS_PER_DAY as i64;
    let start_ticks = sd as i64 * ticks_per_day + st as i64;
    let end_ticks = ed as i64 * ticks_per_day + et as i64;
    let diff = match part.as_str() {
        "day" | "dd" | "d" => ed as i64 - sd as i64,
        "hour" | "hh" => (end_ticks - start_ticks) / (3600 * temporal::TICKS_PER_SEC as i64),
        "minute" | "mi" | "n" => (end_ticks - start_ticks) / (60 * temporal::TICKS_PER_SEC as i64),
        "second" | "ss" | "s" => (end_ticks - start_ticks) / temporal::TICKS_PER_SEC as i64,
        "year" | "yy" | "yyyy" => {
            let (sy, _, _) = temporal::ymd_from_days(sd);
            let (ey, _, _) = temporal::ymd_from_days(ed);
            ey - sy
        }
        "month" | "mm" | "m" => {
            let (sy, sm, _) = temporal::ymd_from_days(sd);
            let (ey, em, _) = temporal::ymd_from_days(ed);
            (ey * 12 + em as i64) - (sy * 12 + sm as i64)
        }
        other => {
            return Err(SqlError::message_only(
                9810,
                format!("The datepart {other} is not supported."),
            ));
        }
    };
    Ok(SqlValue::Int(diff))
}

fn datetime_of(v: &SqlValue) -> SqlResult<Option<(u32, u64)>> {
    match v {
        SqlValue::Null => Ok(None),
        SqlValue::Date(d) => Ok(Some((*d, 0))),
        SqlValue::DateTime2(d, t) => Ok(Some((*d, *t))),
        SqlValue::Str(s) => temporal::parse_datetime2(s)
            .map(Some)
            .ok_or_else(|| SqlError::conversion(format!("cannot convert '{s}' to datetime"))),
        other => Err(convert_err(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, args: Vec<SqlValue>) -> SqlValue {
        eval_function(name, args).expect("function ok")
    }

    #[test]
    fn string_functions() {
        assert_eq!(
            call("LEN", vec![SqlValue::Str("héllo".into())]),
            SqlValue::Int(5)
        );
        assert_eq!(
            call("UPPER", vec![SqlValue::Str("abc".into())]),
            SqlValue::Str("ABC".into())
        );
        assert_eq!(
            call(
                "LEFT",
                vec![SqlValue::Str("abcdef".into()), SqlValue::Int(3)]
            ),
            SqlValue::Str("abc".into())
        );
        assert_eq!(
            call(
                "SUBSTRING",
                vec![
                    SqlValue::Str("abcdef".into()),
                    SqlValue::Int(2),
                    SqlValue::Int(3)
                ]
            ),
            SqlValue::Str("bcd".into())
        );
        assert_eq!(
            call(
                "REPLACE",
                vec![
                    SqlValue::Str("a-b-c".into()),
                    SqlValue::Str("-".into()),
                    SqlValue::Str("+".into())
                ]
            ),
            SqlValue::Str("a+b+c".into())
        );
        assert_eq!(
            call(
                "CHARINDEX",
                vec![SqlValue::Str("cd".into()), SqlValue::Str("abcdef".into())]
            ),
            SqlValue::Int(3)
        );
    }

    #[test]
    fn null_handling() {
        assert_eq!(
            call("ISNULL", vec![SqlValue::Null, SqlValue::Int(7)]),
            SqlValue::Int(7)
        );
        assert_eq!(
            call(
                "COALESCE",
                vec![SqlValue::Null, SqlValue::Null, SqlValue::Int(3)]
            ),
            SqlValue::Int(3)
        );
        assert_eq!(
            call("NULLIF", vec![SqlValue::Int(5), SqlValue::Int(5)]),
            SqlValue::Null
        );
    }

    #[test]
    fn date_functions() {
        let d = SqlValue::Date(temporal::parse_date("2020-06-15").unwrap());
        assert_eq!(call("YEAR", vec![d.clone()]), SqlValue::Int(2020));
        assert_eq!(call("MONTH", vec![d.clone()]), SqlValue::Int(6));
        assert_eq!(call("DAY", vec![d.clone()]), SqlValue::Int(15));
        let added = call(
            "DATEADD",
            vec![SqlValue::Str("day".into()), SqlValue::Int(10), d.clone()],
        );
        assert_eq!(
            added,
            SqlValue::Date(temporal::parse_date("2020-06-25").unwrap())
        );
        let diff = call(
            "DATEDIFF",
            vec![
                SqlValue::Str("day".into()),
                SqlValue::Date(temporal::parse_date("2020-06-15").unwrap()),
                SqlValue::Date(temporal::parse_date("2020-06-25").unwrap()),
            ],
        );
        assert_eq!(diff, SqlValue::Int(10));
    }
}
