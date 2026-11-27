//! The Storage Lens dashboard, on the admin listener: `/storage-lens` lists
//! the configurations (dashboards) and the day of each one's last report;
//! `/storage-lens/<account>/<id>` shows one: its last report's account
//! metrics by storage class, its buckets by storage, and the trend of its
//! storage and objects over the days its reports are kept. Read-only HTML,
//! with no script (its Content-Security-Policy allows none).
//!
//! Each read is a lock hold of at most [`PAGE`] records.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use truthdb_core::storage::{Storage, StorageError};
use truthdb_object::{LENS_KEEP_DAYS, LensRecordType, LensState, ObjFile, STORAGE_LENS};

/// Records read in one lock hold.
const PAGE: usize = 1000;
/// The buckets the page shows (the most storage first).
const TOP_BUCKETS: usize = 25;
/// The account metrics a day's trend point is read from: an account's
/// records come first in a day's, and are this many at most (a metric of
/// each storage class, and those of none).
const ACCOUNT_RECORDS: usize = 600;

/// The Content-Security-Policy of the dashboard's pages: nothing loaded,
/// no script; their own inline style only.
pub const POLICY: &str = "default-src 'none'; style-src 'unsafe-inline'; img-src 'none'; form-action 'none'; frame-ancestors 'none'";

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn date(day: u32) -> String {
    truthdb_object::time::iso8601(u64::from(day) * 86_400_000)[..10].to_string()
}

fn page(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>{}</title>\
         <style>body{{font-family:sans-serif;margin:1.5em;color:#222}}table{{border-collapse:collapse;margin:.5em 0 1.5em}}\
         td,th{{border:1px solid #ccc;padding:.25em .6em;text-align:left}}td.n{{text-align:right}}\
         svg{{border:1px solid #ccc}}</style></head><body>{body}</body></html>",
        escape(title)
    )
}

/// Runs `op` on the object store, if it has one.
fn read<T>(
    storage: &Storage,
    op: impl FnOnce(&mut ObjFile<'_>) -> Result<T, StorageError>,
) -> Result<Option<T>, StorageError> {
    let mut guard = storage.lock();
    let Some(mut file) = ObjFile::try_new(&mut guard) else {
        return Ok(None);
    };
    op(&mut file).map(Some)
}

/// `/storage-lens`: the configurations.
pub fn list(storage: &Storage) -> Result<String, StorageError> {
    let configs = read(storage, |file| {
        let mut out = Vec::new();
        for key in file.bucket_config_ids_with("", STORAGE_LENS, "")? {
            let state = file.lens_state(&key)?;
            out.push((key, state));
        }
        Ok(out)
    })?
    .unwrap_or_default();
    let mut body = String::from("<h1>Storage Lens</h1>");
    if configs.is_empty() {
        body.push_str("<p>No Storage Lens configuration.</p>");
    } else {
        body.push_str("<table><tr><th>Account</th><th>Dashboard</th><th>Last report</th></tr>");
        for (key, state) in &configs {
            let (account, id) = key.split_once('/').unwrap_or(("", key));
            let last = state
                .and_then(|state| state.reported)
                .map_or("none yet".to_string(), date);
            let _ = write!(
                body,
                "<tr><td>{}</td><td><a href=\"/storage-lens/{}/{}\">{}</a></td><td>{}</td></tr>",
                escape(account),
                escape(account),
                escape(id),
                escape(id),
                escape(&last)
            );
        }
        body.push_str("</table>");
    }
    Ok(page("Storage Lens", &body))
}

/// A number with thousands grouped.
fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A trend's points as an inline SVG line, with its first and last days.
fn trend(title: &str, points: &[(u32, u64)]) -> String {
    let (width, height) = (600.0, 120.0);
    if points.is_empty() {
        return String::new();
    }
    let max = points
        .iter()
        .map(|(_, value)| *value)
        .max()
        .unwrap_or(0)
        .max(1) as f64;
    let (first, last) = (points[0].0, points[points.len() - 1].0);
    let span = f64::from((last - first).max(1));
    let line: Vec<String> = points
        .iter()
        .map(|(day, value)| {
            let x = f64::from(day - first) / span * width;
            let y = height - *value as f64 / max * height;
            format!("{x:.1},{y:.1}")
        })
        .collect();
    format!(
        "<h3>{}</h3><svg width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"{}\">\
         <polyline fill=\"none\" stroke=\"#2a6\" stroke-width=\"2\" points=\"{}\"/></svg>\
         <p>{} to {}: from {} to {} (highest {}).</p>",
        escape(title),
        escape(title),
        line.join(" "),
        date(first),
        date(last),
        grouped(points[0].1),
        grouped(points[points.len() - 1].1),
        grouped(max as u64)
    )
}

/// `/storage-lens/<account>/<id>`: one configuration's dashboard, if it
/// exists.
pub fn dashboard(storage: &Storage, key: &str) -> Result<Option<String>, StorageError> {
    let (account, id) = key.split_once('/').unwrap_or(("", key));
    let found = read(storage, |file| {
        Ok(match file.bucket_config("", STORAGE_LENS, key)? {
            Some(_) => Some(file.lens_state(key)?),
            None => None,
        })
    })?
    .flatten();
    let Some(state) = found else {
        return Ok(None);
    };
    let title = format!("Storage Lens: {id}");
    let mut body = format!(
        "<p><a href=\"/storage-lens\">Storage Lens</a></p><h1>{}</h1><p>Account {}.</p>",
        escape(id),
        escape(account)
    );
    let Some(LensState {
        generation,
        reported: Some(last),
        ..
    }) = state
    else {
        body.push_str("<p>No report yet: the first is made within a minute of the configuration, of the day before.</p>");
        return Ok(Some(page(&title, &body)));
    };
    // The last report: the account's metrics, and each bucket's storage.
    let mut account_metrics: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut buckets: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let resume = after.take();
        let Some(Some((records, next))) = read(storage, |file| {
            file.lens_report_page(key, generation, last, resume.as_deref(), PAGE)
        })?
        else {
            break;
        };
        for record in records {
            match record.record_type {
                LensRecordType::Account => {
                    account_metrics.insert(
                        (record.metric.clone(), record.storage_class.clone()),
                        record.value,
                    );
                }
                LensRecordType::Bucket => {
                    let entry = buckets.entry(record.bucket.clone()).or_default();
                    match record.metric.as_str() {
                        "StorageBytes" => entry.0 += record.value,
                        "ObjectCount" => entry.1 += record.value,
                        _ => {}
                    }
                }
                LensRecordType::Prefix => {}
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    let _ = write!(body, "<h2>Report of {}</h2>", date(last));
    let total = |metric: &str| {
        account_metrics
            .iter()
            .filter(|((name, _), _)| name == metric)
            .map(|(_, value)| *value)
            .sum::<u64>()
    };
    let _ = write!(
        body,
        "<p>{} bytes in {} objects, in {} buckets.</p>",
        grouped(total("StorageBytes")),
        grouped(total("ObjectCount")),
        grouped(buckets.len() as u64)
    );
    body.push_str("<table><tr><th>Metric</th><th>Storage class</th><th>Value</th></tr>");
    for ((metric, class), value) in &account_metrics {
        let _ = write!(
            body,
            "<tr><td>{}</td><td>{}</td><td class=\"n\">{}</td></tr>",
            escape(metric),
            escape(if class.is_empty() { "-" } else { class }),
            grouped(*value)
        );
    }
    body.push_str("</table>");
    let mut largest: Vec<(&String, &(u64, u64))> = buckets.iter().collect();
    largest.sort_by(|a, b| b.1.0.cmp(&a.1.0).then(a.0.cmp(b.0)));
    let _ = write!(
        body,
        "<h2>Buckets by storage</h2><table><tr><th>Bucket</th><th>Bytes</th><th>Objects</th></tr>"
    );
    for (bucket, (bytes, objects)) in largest.iter().take(TOP_BUCKETS) {
        let _ = write!(
            body,
            "<tr><td>{}</td><td class=\"n\">{}</td><td class=\"n\">{}</td></tr>",
            escape(bucket),
            grouped(*bytes),
            grouped(*objects)
        );
    }
    body.push_str("</table>");
    if largest.len() > TOP_BUCKETS {
        let _ = write!(body, "<p>And {} more.</p>", largest.len() - TOP_BUCKETS);
    }
    // The trend: each kept day's account storage and objects.
    let (mut bytes, mut objects) = (Vec::new(), Vec::new());
    for day in last.saturating_sub(LENS_KEEP_DAYS - 1)..=last {
        let Some(Some((records, _))) = read(storage, |file| {
            file.lens_report_page(key, generation, day, None, ACCOUNT_RECORDS)
        })?
        else {
            continue;
        };
        let accounts: Vec<_> = records
            .iter()
            .take_while(|record| record.record_type == LensRecordType::Account)
            .collect();
        if accounts.is_empty() {
            continue;
        }
        let sum = |metric: &str| {
            accounts
                .iter()
                .filter(|record| record.metric == metric)
                .map(|record| record.value)
                .sum::<u64>()
        };
        bytes.push((day, sum("StorageBytes")));
        objects.push((day, sum("ObjectCount")));
    }
    body.push_str("<h2>Trend</h2>");
    body.push_str(&trend("Storage bytes", &bytes));
    body.push_str(&trend("Objects", &objects));
    Ok(Some(page(&title, &body)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_escaped_and_numbers_grouped() {
        assert_eq!(
            escape("<a href='x'>&\"</a>"),
            "&lt;a href=&#39;x&#39;&gt;&amp;&quot;&lt;/a&gt;"
        );
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(1234), "1,234");
        assert_eq!(grouped(1_234_567), "1,234,567");
        let svg = trend("Storage bytes", &[(10, 5), (12, 10)]);
        assert!(svg.contains("points=\"0.0,60.0 600.0,0.0\""), "{svg}");
        assert!(trend("x", &[]).is_empty());
    }
}
