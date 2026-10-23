//! Timestamps as S3 writes them (shared with the S3 layer and the event
//! notifications).

/// Milliseconds since the Unix epoch as ISO 8601 (`2024-05-24T00:00:00.000Z`).
pub fn iso8601(ms: u64) -> String {
    let (year, month, day, secs) = civil(ms / 1000);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60,
        ms % 1000
    )
}

/// Seconds since the Unix epoch as `(year, month, day, seconds into the day)`
/// (Howard Hinnant's civil-from-days).
fn civil(secs: u64) -> (u64, u64, u64, u64) {
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + u64::from(month <= 2), month, day, rem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_render_as_iso8601() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601(1_369_353_600_123), "2013-05-24T00:00:00.123Z");
        assert_eq!(iso8601(951_868_799_000), "2000-02-29T23:59:59.000Z");
    }
}
