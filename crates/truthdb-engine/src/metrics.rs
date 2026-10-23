//! Request metrics of the S3 API, per bucket, since the server started: how
//! many requests of each kind (as S3's request metrics count them), the
//! bytes up and down, the errors, and two latencies as histograms: until the
//! answer starts (first byte) and until its last byte is sent (total).
//!
//! The S3 listener records into it, `sys.dm_s3_requests` and
//! `sys.dm_s3_request_latency` read it. Counters are atomics: recording
//! takes no lock once a bucket's entry exists.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// The buckets kept apart at most; requests to others (which need not
/// exist: anyone may name one) are counted together under "".
const MAX_BUCKETS: usize = 10_000;

/// A request as S3's request metrics count it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    /// GET of an object (not a listing).
    Get,
    /// PUT of an object, or anything on it (a part, tags, a copy's target).
    Put,
    /// DELETE of an object, or a multi-object delete.
    Delete,
    Head,
    /// POST other than a multi-object delete.
    Post,
    /// A listing of a bucket's objects, versions or uploads.
    List,
    /// Anything else (a bucket's configuration): counted in all requests
    /// only.
    Other,
}

/// The latency histograms' upper bounds, in milliseconds: powers of two up
/// to about 17 minutes; slower requests fall in the last, unbounded one.
const LATENCY_BOUNDS_MS: [u64; 21] = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
    262144, 524288, 1048576,
];

/// Requests counted by latency, one count per bound and one past them.
#[derive(Debug, Default)]
pub struct Histogram {
    counts: [AtomicU64; LATENCY_BOUNDS_MS.len() + 1],
}

impl Histogram {
    pub fn record(&self, latency: Duration) {
        let ms = latency.as_millis().min(u128::from(u64::MAX)) as u64;
        let index = LATENCY_BOUNDS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(LATENCY_BOUNDS_MS.len());
        self.counts[index].fetch_add(1, Ordering::Relaxed);
    }

    /// Each bound (`None`: past the last) and its count, for the bounds
    /// with any.
    pub fn snapshot(&self) -> Vec<(Option<u64>, u64)> {
        self.counts
            .iter()
            .enumerate()
            .map(|(index, count)| {
                (
                    LATENCY_BOUNDS_MS.get(index).copied(),
                    count.load(Ordering::Relaxed),
                )
            })
            .filter(|(_, count)| *count > 0)
            .collect()
    }
}

/// One bucket's counters.
#[derive(Debug, Default)]
pub struct BucketRequests {
    all: AtomicU64,
    get: AtomicU64,
    put: AtomicU64,
    delete: AtomicU64,
    head: AtomicU64,
    post: AtomicU64,
    list: AtomicU64,
    bytes_downloaded: AtomicU64,
    bytes_uploaded: AtomicU64,
    errors_4xx: AtomicU64,
    errors_5xx: AtomicU64,
    pub first_byte: Histogram,
    pub total: Histogram,
}

/// A bucket's counters as read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestCounts {
    pub all: u64,
    pub get: u64,
    pub put: u64,
    pub delete: u64,
    pub head: u64,
    pub post: u64,
    pub list: u64,
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
    pub errors_4xx: u64,
    pub errors_5xx: u64,
}

impl BucketRequests {
    /// Counts a request of `kind` that sent `uploaded` bytes, when its
    /// answer starts (with `status`, after `first_byte`).
    pub fn answered(&self, kind: RequestKind, uploaded: u64, status: u16, first_byte: Duration) {
        self.all.fetch_add(1, Ordering::Relaxed);
        let counter = match kind {
            RequestKind::Get => Some(&self.get),
            RequestKind::Put => Some(&self.put),
            RequestKind::Delete => Some(&self.delete),
            RequestKind::Head => Some(&self.head),
            RequestKind::Post => Some(&self.post),
            RequestKind::List => Some(&self.list),
            RequestKind::Other => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        self.bytes_uploaded.fetch_add(uploaded, Ordering::Relaxed);
        match status {
            400..=499 => self.errors_4xx.fetch_add(1, Ordering::Relaxed),
            500..=599 => self.errors_5xx.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        self.first_byte.record(first_byte);
    }

    /// Counts a GET that a copy made of this bucket's object (S3 counts the
    /// source of a copy as one).
    pub fn copied_from(&self) {
        self.get.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an answer's body sent (`downloaded` bytes) in `total`.
    pub fn finished(&self, downloaded: u64, total: Duration) {
        self.bytes_downloaded
            .fetch_add(downloaded, Ordering::Relaxed);
        self.total.record(total);
    }

    pub fn counts(&self) -> RequestCounts {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        RequestCounts {
            all: load(&self.all),
            get: load(&self.get),
            put: load(&self.put),
            delete: load(&self.delete),
            head: load(&self.head),
            post: load(&self.post),
            list: load(&self.list),
            bytes_downloaded: load(&self.bytes_downloaded),
            bytes_uploaded: load(&self.bytes_uploaded),
            errors_4xx: load(&self.errors_4xx),
            errors_5xx: load(&self.errors_5xx),
        }
    }
}

/// Every bucket's counters.
#[derive(Debug, Default)]
pub struct RequestMetrics {
    buckets: RwLock<HashMap<String, Arc<BucketRequests>>>,
}

impl RequestMetrics {
    /// The counters of `bucket` (those of "" once `MAX_BUCKETS` others have
    /// some).
    pub fn bucket(&self, bucket: &str) -> Arc<BucketRequests> {
        if let Some(found) = self
            .buckets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(bucket)
        {
            return Arc::clone(found);
        }
        let mut buckets = self
            .buckets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let name = if buckets.len() < MAX_BUCKETS || buckets.contains_key(bucket) {
            bucket
        } else {
            ""
        };
        Arc::clone(buckets.entry(name.to_string()).or_default())
    }

    /// Every bucket's counters, by name.
    pub fn snapshot(&self) -> Vec<(String, Arc<BucketRequests>)> {
        let mut all: Vec<(String, Arc<BucketRequests>)> = self
            .buckets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(name, counters)| (name.clone(), Arc::clone(counters)))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_count_by_kind_status_and_latency() {
        let metrics = RequestMetrics::default();
        let bucket = metrics.bucket("photos");
        bucket.answered(RequestKind::Put, 100, 200, Duration::from_millis(3));
        bucket.answered(RequestKind::Get, 0, 404, Duration::from_micros(10));
        bucket.answered(RequestKind::Other, 0, 503, Duration::from_secs(2_000));
        bucket.copied_from();
        bucket.finished(42, Duration::from_millis(9));
        let counts = metrics.bucket("photos").counts();
        assert_eq!(
            counts,
            RequestCounts {
                all: 3,
                get: 2,
                put: 1,
                bytes_downloaded: 42,
                bytes_uploaded: 100,
                errors_4xx: 1,
                errors_5xx: 1,
                ..RequestCounts::default()
            }
        );
        assert_eq!(
            bucket.first_byte.snapshot(),
            [(Some(1), 1), (Some(4), 1), (None, 1)]
        );
        assert_eq!(bucket.total.snapshot(), [(Some(16), 1)]);
    }

    #[test]
    fn buckets_past_the_limit_are_counted_together() {
        let metrics = RequestMetrics::default();
        for index in 0..MAX_BUCKETS {
            metrics.bucket(&format!("b{index}"));
        }
        metrics
            .bucket("one-more")
            .answered(RequestKind::Head, 0, 200, Duration::ZERO);
        metrics
            .bucket("and-another")
            .answered(RequestKind::Head, 0, 200, Duration::ZERO);
        // A bucket kept before the limit stays apart.
        metrics
            .bucket("b7")
            .answered(RequestKind::Head, 0, 200, Duration::ZERO);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.len(), MAX_BUCKETS + 1);
        let together = &snapshot.iter().find(|(name, _)| name.is_empty()).unwrap().1;
        assert_eq!(together.counts().head, 2);
        assert_eq!(metrics.bucket("b7").counts().head, 1);
    }
}
