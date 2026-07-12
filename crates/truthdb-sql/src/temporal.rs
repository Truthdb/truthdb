//! Proleptic-Gregorian date/time helpers shared by CAST/CONVERT, comparison,
//! the date functions, and the TDS codecs. Encodings match the storage layer:
//! DATE = days since 0001-01-01 (day 0), DATETIME2 = (days, 100ns ticks since
//! midnight), TIME = 100ns ticks since midnight.

/// 100ns ticks in one day (24 * 3600 * 10^7).
pub const TICKS_PER_DAY: u64 = 864_000_000_000;
pub const TICKS_PER_SEC: u64 = 10_000_000;

/// Days from 0001-01-01 to 1970-01-01, to bridge the civil-days algorithm.
const DAYS_0001_TO_1970: i64 = 719_162;

/// Days since 1970-01-01 for a proleptic-Gregorian y/m/d (Howard Hinnant).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Inverse of [`days_from_civil`].
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Days since 0001-01-01 for a valid y/m/d in the DATE range, else None.
pub fn days_from_ymd(y: i64, m: u32, d: u32) -> Option<u32> {
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    let days = days_from_civil(y, m, d) + DAYS_0001_TO_1970;
    u32::try_from(days).ok()
}

pub fn ymd_from_days(days: u32) -> (i64, u32, u32) {
    civil_from_days(days as i64 - DAYS_0001_TO_1970)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

pub fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Parses `YYYY-MM-DD` into days since 0001-01-01.
pub fn parse_date(s: &str) -> Option<u32> {
    let (y, m, d) = parse_ymd(s.trim())?;
    days_from_ymd(y, m, d)
}

fn parse_ymd(s: &str) -> Option<(i64, u32, u32)> {
    let mut parts = s.splitn(3, '-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let d: u32 = parts.next()?.parse().ok()?;
    Some((y, m, d))
}

/// Parses `YYYY-MM-DD[ HH:MM:SS[.fffffff]]` into (days, ticks).
pub fn parse_datetime2(s: &str) -> Option<(u32, u64)> {
    let s = s.trim();
    let (date_part, time_part) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let days = parse_date(date_part)?;
    let ticks = match time_part {
        Some(t) => parse_time(t)?,
        None => 0,
    };
    Some((days, ticks))
}

/// Parses `HH:MM:SS[.fffffff]` into 100ns ticks since midnight.
pub fn parse_time(s: &str) -> Option<u64> {
    let s = s.trim();
    let mut parts = s.split(':');
    let h: u64 = parts.next()?.parse().ok()?;
    let m: u64 = parts.next()?.parse().ok()?;
    let (sec, frac) = match parts.next() {
        Some(sec_str) => match sec_str.split_once('.') {
            Some((sec, frac)) => (sec.parse::<u64>().ok()?, frac),
            None => (sec_str.parse::<u64>().ok()?, ""),
        },
        None => (0, ""),
    };
    if h > 23 || m > 59 || sec > 59 || parts.next().is_some() {
        return None;
    }
    // Fractional seconds: pad/truncate to 7 digits (100ns resolution).
    let mut frac_digits: String = frac.chars().take(7).collect();
    while frac_digits.len() < 7 {
        frac_digits.push('0');
    }
    let frac_ticks: u64 = if frac_digits.is_empty() {
        0
    } else {
        frac_digits.parse().ok()?
    };
    Some((h * 3600 + m * 60 + sec) * TICKS_PER_SEC + frac_ticks)
}

pub fn render_date(days: u32) -> String {
    let (y, m, d) = ymd_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

pub fn render_time(ticks: u64) -> String {
    let secs = ticks / TICKS_PER_SEC;
    let frac = ticks % TICKS_PER_SEC;
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    format!("{h:02}:{m:02}:{s:02}.{frac:07}")
}

pub fn render_datetime2(days: u32, ticks: u64) -> String {
    format!("{} {}", render_date(days), render_time(ticks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_known_dates() {
        assert_eq!(parse_date("0001-01-01"), Some(0));
        // 1970-01-01 is DAYS_0001_TO_1970 days after 0001-01-01.
        assert_eq!(parse_date("1970-01-01"), Some(DAYS_0001_TO_1970 as u32));
        assert_eq!(render_date(0), "0001-01-01");
        let days = parse_date("2024-02-29").expect("leap day");
        assert_eq!(render_date(days), "2024-02-29");
    }

    #[test]
    fn rejects_invalid_dates() {
        assert_eq!(parse_date("2023-02-29"), None); // not a leap year
        assert_eq!(parse_date("2020-13-01"), None);
        assert_eq!(parse_date("2020-00-10"), None);
        assert_eq!(parse_date("10000-01-01"), None);
    }

    #[test]
    fn datetime_round_trip() {
        let (days, ticks) = parse_datetime2("2020-06-15 13:45:30.500").expect("dt");
        assert_eq!(render_datetime2(days, ticks), "2020-06-15 13:45:30.5000000");
        // Date-only defaults to midnight.
        assert_eq!(
            parse_datetime2("2020-06-15"),
            Some((parse_date("2020-06-15").unwrap(), 0))
        );
    }

    #[test]
    fn time_parsing() {
        assert_eq!(parse_time("00:00:00"), Some(0));
        assert_eq!(parse_time("01:00:00"), Some(3600 * TICKS_PER_SEC));
        assert_eq!(parse_time("24:00:00"), None);
    }
}
