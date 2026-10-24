//! The metric history's writer: each request to a bucket counts, by the
//! minute, in every metrics configuration of the bucket whose filter it
//! matches (its `FilterId` series), as S3's request metrics count; once a
//! day each bucket's storage is sampled (its `StorageType` series). What is
//! counted is held in memory and flushed into the object engine every
//! `metrics_flush` (a minute by default; a flush adds to the minute, so it
//! may come more often): a crash loses what was not flushed, as S3's
//! metrics are best effort.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use truthdb_engine::metrics::{LATENCY_BOUNDS_MS, RequestKind};
use truthdb_object::{
    BUCKET_METRICS, MetricPoint, MetricSeries, SeriesBy, Stats, StorageClass, VersionSel,
};

use crate::metrics_config::{MetricsConfig, parse};
use crate::server::S3State;

/// The metrics, by the code the history keeps them under: their names and
/// units.
pub const METRICS: [(u8, &str, &str); 15] = [
    (ALL_REQUESTS, "AllRequests", "Count"),
    (1, "GetRequests", "Count"),
    (2, "PutRequests", "Count"),
    (3, "DeleteRequests", "Count"),
    (4, "HeadRequests", "Count"),
    (5, "PostRequests", "Count"),
    (6, "ListRequests", "Count"),
    (BYTES_DOWNLOADED, "BytesDownloaded", "Bytes"),
    (BYTES_UPLOADED, "BytesUploaded", "Bytes"),
    (ERRORS_4XX, "4xxErrors", "Count"),
    (ERRORS_5XX, "5xxErrors", "Count"),
    (FIRST_BYTE_LATENCY, "FirstByteLatency", "Milliseconds"),
    (TOTAL_REQUEST_LATENCY, "TotalRequestLatency", "Milliseconds"),
    (BUCKET_SIZE_BYTES, "BucketSizeBytes", "Bytes"),
    (NUMBER_OF_OBJECTS, "NumberOfObjects", "Count"),
];
const ALL_REQUESTS: u8 = 0;
const BYTES_DOWNLOADED: u8 = 7;
const BYTES_UPLOADED: u8 = 8;
const ERRORS_4XX: u8 = 9;
const ERRORS_5XX: u8 = 10;
pub const FIRST_BYTE_LATENCY: u8 = 11;
pub const TOTAL_REQUEST_LATENCY: u8 = 12;
pub const BUCKET_SIZE_BYTES: u8 = 13;
pub const NUMBER_OF_OBJECTS: u8 = 14;

/// The storage type a storage class's bytes are reported under.
pub fn storage_type(class: StorageClass) -> &'static str {
    match class {
        StorageClass::Standard => "StandardStorage",
        StorageClass::StandardIa => "StandardIAStorage",
        StorageClass::OnezoneIa => "OneZoneIAStorage",
        StorageClass::ReducedRedundancy => "ReducedRedundancyStorage",
        StorageClass::Glacier => "GlacierStorage",
        StorageClass::GlacierIr => "GlacierInstantRetrievalStorage",
        StorageClass::DeepArchive => "DeepArchiveStorage",
        StorageClass::IntelligentTiering => "IntelligentTieringFAStorage",
    }
}

/// The storage type NumberOfObjects is reported under.
pub const ALL_STORAGE_TYPES: &str = "AllStorageTypes";

const DAY: u64 = 86_400;

fn now_secs() -> u64 {
    crate::objects::now_ms() / 1000
}

/// A latency as one value with the bound it falls under.
fn latency(elapsed: Duration) -> Stats {
    let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    let mut stats = Stats::of(micros as f64 / 1000.0);
    stats.histogram = vec![0; LATENCY_BOUNDS_MS.len() + 1];
    let index = LATENCY_BOUNDS_MS
        .iter()
        .position(|bound| micros <= bound * 1000)
        .unwrap_or(LATENCY_BOUNDS_MS.len());
    stats.histogram[index] = 1;
    stats
}

/// What a request counts when its answer starts.
pub(crate) fn answered(
    kind: RequestKind,
    uploaded: u64,
    status: u16,
    first_byte: Duration,
) -> MetricPoint {
    let mut point = MetricPoint::new();
    point.insert(ALL_REQUESTS, Stats::of(1.0));
    let kind_code = match kind {
        RequestKind::Get => Some(1),
        RequestKind::Put => Some(2),
        RequestKind::Delete => Some(3),
        RequestKind::Head => Some(4),
        RequestKind::Post => Some(5),
        RequestKind::List => Some(6),
        RequestKind::Other => None,
    };
    if let Some(code) = kind_code {
        point.insert(code, Stats::of(1.0));
    }
    if uploaded > 0 {
        point.insert(BYTES_UPLOADED, Stats::of(uploaded as f64));
    }
    point.insert(
        ERRORS_4XX,
        Stats::of(f64::from(u8::from((400..500).contains(&status)))),
    );
    point.insert(
        ERRORS_5XX,
        Stats::of(f64::from(u8::from((500..600).contains(&status)))),
    );
    point.insert(FIRST_BYTE_LATENCY, latency(first_byte));
    point
}

/// What a request counts when its answer's body is sent.
pub(crate) fn finished(downloaded: u64, total: Duration) -> MetricPoint {
    let mut point = MetricPoint::new();
    if downloaded > 0 {
        point.insert(BYTES_DOWNLOADED, Stats::of(downloaded as f64));
    }
    point.insert(TOTAL_REQUEST_LATENCY, latency(total));
    point
}

/// Whether a request for `key` (none: the bucket's own), of an object with
/// `tags`, is one a configuration counts.
fn counts(config: &MetricsConfig, key: Option<&str>, tags: &[(String, String)]) -> bool {
    let Some(filter) = &config.filter else {
        return true;
    };
    // Requests through access points: there are none.
    if filter.access_point.is_some() {
        return false;
    }
    if let Some(prefix) = &filter.prefix
        && !key.is_some_and(|key| key.starts_with(prefix.as_str()))
    {
        return false;
    }
    filter.tags.iter().all(|tag| tags.contains(tag))
}

#[derive(Default)]
pub struct MetricRecorder {
    /// Each bucket's metrics configurations, read once and forgotten when
    /// they change (only this server changes them).
    configs: Mutex<HashMap<String, Arc<Vec<MetricsConfig>>>>,
    /// What was counted and not yet flushed, by series and minute.
    pending: Mutex<HashMap<(MetricSeries, u64), MetricPoint>>,
    /// The day (since the epoch) storage was last sampled.
    sampled_day: Mutex<Option<u64>>,
}

impl MetricRecorder {
    /// Forgets `bucket`'s configurations (they changed, or it is gone).
    pub(crate) fn forget(&self, bucket: &str) {
        self.configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(bucket);
    }

    async fn configs(&self, state: &S3State, bucket: &str) -> Arc<Vec<MetricsConfig>> {
        if let Some(found) = self
            .configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(bucket)
        {
            return Arc::clone(found);
        }
        let name = bucket.to_string();
        let loaded = state
            .engine
            .run_object(move |file| {
                let mut configs = Vec::new();
                for id in file.bucket_config_ids(&name, BUCKET_METRICS)? {
                    if let Some(document) = file.bucket_config(&name, BUCKET_METRICS, &id)?
                        && let Ok(config) = parse(&document, &id)
                    {
                        configs.push(config);
                    }
                }
                Ok(configs)
            })
            .await;
        // Not read (the engine busy or gone): nothing counted, nothing kept.
        let Ok(configs) = loaded else {
            return Arc::new(Vec::new());
        };
        let configs = Arc::new(configs);
        self.configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(bucket.to_string(), Arc::clone(&configs));
        configs
    }

    /// The series a request for `bucket`'s `key` (none: the bucket's own)
    /// counts in.
    pub(crate) async fn matching(
        &self,
        state: &S3State,
        bucket: &str,
        key: Option<&str>,
    ) -> Vec<MetricSeries> {
        let configs = self.configs(state, bucket).await;
        if configs.is_empty() {
            return Vec::new();
        }
        // The object's tags, read only when a filter names some.
        let mut tags = Vec::new();
        if let Some(key) = key
            && configs.iter().any(|config| {
                config
                    .filter
                    .as_ref()
                    .is_some_and(|filter| !filter.tags.is_empty())
            })
        {
            let (name, wanted) = (bucket.to_string(), key.to_string());
            if let Ok(Ok(info)) = state
                .engine
                .run_object(move |file| file.head_object(&name, &wanted, VersionSel::Latest))
                .await
            {
                tags = info.attrs.tags;
            }
        }
        configs
            .iter()
            .filter(|config| counts(config, key, &tags))
            .map(|config| MetricSeries {
                bucket: bucket.to_string(),
                by: SeriesBy::Filter(config.id.clone()),
            })
            .collect()
    }

    /// Adds `point` to each of `series` at the minute `at` (seconds) is in.
    pub(crate) fn record(&self, series: &[MetricSeries], at: u64, point: &MetricPoint) {
        let minute = at - at % 60;
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for one in series {
            let kept = pending.entry((one.clone(), minute)).or_default();
            for (metric, stats) in point {
                kept.entry(*metric)
                    .and_modify(|kept| kept.merge(stats))
                    .or_insert_with(|| stats.clone());
            }
        }
    }

    fn take(&self) -> Vec<((MetricSeries, u64), MetricPoint)> {
        std::mem::take(
            &mut *self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
        .into_iter()
        .collect()
    }

    /// Writes what was counted (put back to try again if it could not be)
    /// and, once a day, each bucket's storage.
    pub(crate) async fn flush(&self, state: &S3State) {
        let taken = self.take();
        if !taken.is_empty() {
            let written = taken.clone();
            let stored = state
                .engine
                .run_object(move |file| {
                    file.transaction(|tx| {
                        for ((series, minute), point) in &written {
                            tx.add_metrics(series, *minute, point)?;
                        }
                        Ok(())
                    })
                })
                .await;
            if stored.is_err() {
                for ((series, minute), point) in taken {
                    self.record(&[series], minute, &point);
                }
            }
        }
        let day = now_secs() / DAY;
        let sampled = *self
            .sampled_day
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sampled != Some(day) && sample_storage(state, day).await {
            *self
                .sampled_day
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(day);
        }
    }
}

/// Writes each bucket's storage for `day` (at its first minute), unless it
/// was already (before a restart); whether it is written.
async fn sample_storage(state: &S3State, day: u64) -> bool {
    let start = day * DAY;
    state
        .engine
        .run_object(move |file| {
            let usage = file.bucket_usage()?;
            let mut points: HashMap<MetricSeries, MetricPoint> = HashMap::new();
            let mut objects: HashMap<String, f64> = HashMap::new();
            let mut bytes: HashMap<(String, &'static str), f64> = HashMap::new();
            for row in &usage {
                *objects.entry(row.bucket.clone()).or_default() += row.objects as f64;
                // Delete markers count, with no bytes.
                *bytes
                    .entry((row.bucket.clone(), storage_type(row.storage_class)))
                    .or_default() += row.bytes as f64;
            }
            for (bucket, count) in objects {
                points.insert(
                    MetricSeries {
                        bucket,
                        by: SeriesBy::StorageType(ALL_STORAGE_TYPES.to_string()),
                    },
                    [(NUMBER_OF_OBJECTS, Stats::of(count))]
                        .into_iter()
                        .collect(),
                );
            }
            for ((bucket, storage), size) in bytes {
                points.insert(
                    MetricSeries {
                        bucket,
                        by: SeriesBy::StorageType(storage.to_string()),
                    },
                    [(BUCKET_SIZE_BYTES, Stats::of(size))].into_iter().collect(),
                );
            }
            // Sampled already today (this server restarted): not again.
            let mut fresh = Vec::new();
            for (series, point) in points {
                if file
                    .metric_points(&series, 60, start, start + 60)?
                    .is_empty()
                {
                    fresh.push((series, point));
                }
            }
            file.transaction(|tx| {
                for (series, point) in &fresh {
                    tx.add_metrics(series, start, point)?;
                }
                Ok(())
            })
        })
        .await
        .is_ok()
}

/// Flushes the metric history every `state.metrics_flush` until
/// `shutdown`, and once more then. A standby writes nothing.
pub async fn run(state: Arc<S3State>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    loop {
        let stopping = tokio::select! {
            _ = tokio::time::sleep(state.metrics_flush) => false,
            changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        };
        if !state.storage.is_standby() {
            state.metric_recorder.flush(&state).await;
        }
        if stopping {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics_config::MetricsFilter;

    #[test]
    fn requests_count_where_their_filter_matches() {
        let config = |filter: Option<MetricsFilter>| MetricsConfig {
            id: "c".to_string(),
            filter,
        };
        let tag = |key: &str, value: &str| (key.to_string(), value.to_string());
        assert!(counts(&config(None), None, &[]));
        let docs = config(Some(MetricsFilter {
            prefix: Some("docs/".to_string()),
            ..MetricsFilter::default()
        }));
        assert!(counts(&docs, Some("docs/a"), &[]));
        assert!(!counts(&docs, Some("img/a"), &[]));
        assert!(!counts(&docs, None, &[]));
        let blue = config(Some(MetricsFilter {
            prefix: Some("docs/".to_string()),
            tags: vec![tag("class", "blue")],
            access_point: None,
        }));
        assert!(counts(
            &blue,
            Some("docs/a"),
            &[tag("class", "blue"), tag("x", "y")]
        ));
        assert!(!counts(&blue, Some("docs/a"), &[tag("class", "red")]));
        let point = config(Some(MetricsFilter {
            access_point: Some("arn:aws:s3:us-east-1:1:accesspoint/a".to_string()),
            ..MetricsFilter::default()
        }));
        assert!(!counts(&point, Some("a"), &[]));
    }

    #[test]
    fn an_answer_counts_its_kind_bytes_error_and_latency() {
        let point = answered(RequestKind::Get, 0, 404, Duration::from_micros(1_500));
        assert_eq!(point[&ALL_REQUESTS].count, 1);
        assert_eq!(point[&1].sum, 1.0);
        assert!(!point.contains_key(&BYTES_UPLOADED));
        assert_eq!((point[&ERRORS_4XX].sum, point[&ERRORS_5XX].sum), (1.0, 0.0));
        let first = &point[&FIRST_BYTE_LATENCY];
        assert_eq!(first.sum, 1.5);
        assert_eq!(first.histogram[1], 1, "under 2 ms, past 1 ms");
        let put = answered(RequestKind::Other, 10, 200, Duration::ZERO);
        assert!(!(1..=6).any(|code| put.contains_key(&code)));
        assert_eq!(put[&BYTES_UPLOADED].sum, 10.0);
        let done = finished(0, Duration::from_millis(3));
        assert!(!done.contains_key(&BYTES_DOWNLOADED));
        assert_eq!(done[&TOTAL_REQUEST_LATENCY].sum, 3.0);
    }
}
