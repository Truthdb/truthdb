//! The metric history: statistics of the S3 request and storage metrics per
//! series (a bucket and a metrics configuration's id, or a bucket and a
//! storage type) and period, as CloudWatch keeps them: by the minute for 15
//! days, by five minutes for 63 days, by the hour for 455 days.
//!
//! A point holds, per metric, the count, sum, minimum and maximum of the
//! values it saw (and, for latencies, how many fell under each bound), so
//! points merge: what a writer adds to a minute is added, in the same
//! transaction, to its five minutes and its hour. Each point is expired, a
//! bounded batch at a time, through an index by expiry.

use std::collections::BTreeMap;

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::corrupt;
use crate::{METRIC_EXPIRY_TREE, METRIC_POINTS_TREE, METRIC_SERIES_TREE, ObjFile, ObjTxn};

/// The periods kept, in seconds, and for how long (seconds).
pub const RESOLUTIONS: [(u64, u64); 3] = [(60, 15 * DAY), (300, 63 * DAY), (3600, 455 * DAY)];
const DAY: u64 = 86_400;

/// What a series is a bucket's metrics by.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SeriesBy {
    /// A metrics configuration's id (the `FilterId` dimension).
    Filter(String),
    /// A storage type (the `StorageType` dimension).
    StorageType(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricSeries {
    pub bucket: String,
    pub by: SeriesBy,
}

impl MetricSeries {
    /// `bucket, 0, kind, value, 0`.
    fn key(&self) -> Vec<u8> {
        let (kind, value) = match &self.by {
            SeriesBy::Filter(id) => (0u8, id.as_str()),
            SeriesBy::StorageType(storage_type) => (1u8, storage_type.as_str()),
        };
        [self.bucket.as_bytes(), &[0, kind], value.as_bytes(), &[0]].concat()
    }

    fn from_key(key: &[u8]) -> Result<(MetricSeries, usize), StorageError> {
        let split = key
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| corrupt("metric series"))?;
        let bucket = std::str::from_utf8(&key[..split]).map_err(|_| corrupt("metric series"))?;
        let kind = *key.get(split + 1).ok_or_else(|| corrupt("metric series"))?;
        let rest = &key[split + 2..];
        let end = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| corrupt("metric series"))?;
        let value = std::str::from_utf8(&rest[..end])
            .map_err(|_| corrupt("metric series"))?
            .to_string();
        let by = match kind {
            0 => SeriesBy::Filter(value),
            1 => SeriesBy::StorageType(value),
            _ => return Err(corrupt("metric series")),
        };
        Ok((
            MetricSeries {
                bucket: bucket.to_string(),
                by,
            },
            split + 2 + end + 1,
        ))
    }
}

/// The values a metric saw in a period.
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    /// How many fell under each bound (a latency's), empty for others.
    pub histogram: Vec<u64>,
}

impl Stats {
    /// One value.
    pub fn of(value: f64) -> Stats {
        Stats {
            count: 1,
            sum: value,
            min: value,
            max: value,
            histogram: Vec::new(),
        }
    }

    fn merge(&mut self, other: &Stats) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        if self.histogram.len() < other.histogram.len() {
            self.histogram.resize(other.histogram.len(), 0);
        }
        for (mine, theirs) in self.histogram.iter_mut().zip(&other.histogram) {
            *mine += theirs;
        }
    }
}

/// A period's statistics, by metric (a code the writer and the reader
/// agree on).
pub type MetricPoint = BTreeMap<u8, Stats>;

/// The most bounds a histogram keeps (and so the most a row holds).
const MAX_BOUNDS: usize = 32;

fn encode(point: &MetricPoint) -> Vec<u8> {
    let mut out = Vec::new();
    for (metric, stats) in point {
        out.push(*metric);
        out.extend_from_slice(&stats.count.to_le_bytes());
        for value in [stats.sum, stats.min, stats.max] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.push(stats.histogram.len() as u8);
        for count in &stats.histogram {
            out.extend_from_slice(&count.to_le_bytes());
        }
    }
    out
}

fn decode(mut bytes: &[u8]) -> Result<MetricPoint, StorageError> {
    let corrupt = || corrupt("metric point");
    let mut point = MetricPoint::new();
    let take = |count: usize, bytes: &mut &[u8]| -> Result<Vec<u8>, StorageError> {
        let (head, rest) = bytes.split_at_checked(count).ok_or_else(corrupt)?;
        *bytes = rest;
        Ok(head.to_vec())
    };
    while !bytes.is_empty() {
        let metric = take(1, &mut bytes)?[0];
        if point.contains_key(&metric) {
            return Err(corrupt());
        }
        let count = u64::from_le_bytes(take(8, &mut bytes)?.try_into().unwrap());
        let mut values = [0f64; 3];
        for value in &mut values {
            *value = f64::from_le_bytes(take(8, &mut bytes)?.try_into().unwrap());
        }
        let bounds = take(1, &mut bytes)?[0] as usize;
        if bounds > MAX_BOUNDS {
            return Err(corrupt());
        }
        let mut histogram = Vec::with_capacity(bounds);
        for _ in 0..bounds {
            histogram.push(u64::from_le_bytes(take(8, &mut bytes)?.try_into().unwrap()));
        }
        point.insert(
            metric,
            Stats {
                count,
                sum: values[0],
                min: values[1],
                max: values[2],
                histogram,
            },
        );
    }
    Ok(point)
}

/// Whether a series whose last minute is `last` (its index row) has no
/// point left at `now`: its last point is in the hour its last minute is
/// in, gone by the end of that hour's keep.
fn stale(last: &[u8], now: u64) -> Result<bool, StorageError> {
    let last = u64::from_be_bytes(last.try_into().map_err(|_| corrupt("metric series"))?);
    let longest = RESOLUTIONS
        .iter()
        .map(|(period, keep)| period + keep)
        .max()
        .unwrap_or(0);
    Ok(last.saturating_add(longest) < now)
}

/// A point's key: the series', the resolution, the period's start.
fn point_key(series: &[u8], resolution: u64, period: u64) -> Vec<u8> {
    [
        series,
        &(resolution as u32).to_be_bytes(),
        &period.to_be_bytes(),
    ]
    .concat()
}

/// An expiry index entry: when, then the point's key.
fn expiry_key(expires: u64, point: &[u8]) -> Vec<u8> {
    [&expires.to_be_bytes()[..], point].concat()
}

impl ObjTxn<'_, '_> {
    /// Adds `point` (a minute's statistics, or part of them) to `series`'
    /// minute starting at `minute` (seconds), and to the five minutes and
    /// the hour that minute is in.
    pub fn add_metrics(
        &mut self,
        series: &MetricSeries,
        minute: u64,
        point: &MetricPoint,
    ) -> Result<(), StorageError> {
        if point
            .values()
            .any(|stats| stats.histogram.len() > MAX_BOUNDS)
        {
            return Err(StorageError::InvalidConfig(format!(
                "a metric histogram keeps at most {MAX_BOUNDS} bounds"
            )));
        }
        let (points, expiry, index) = (
            self.trees.metric_points(),
            self.trees.metric_expiry(),
            self.trees.metric_series(),
        );
        let series_key = series.key();
        for (resolution, keep) in RESOLUTIONS {
            let period = minute - minute % resolution;
            let key = point_key(&series_key, resolution, period);
            let existing = points.get(self.ctx, &key)?;
            let mut merged = match &existing {
                Some(row) => decode(row)?,
                None => MetricPoint::new(),
            };
            for (metric, stats) in point {
                merged
                    .entry(*metric)
                    .and_modify(|kept| kept.merge(stats))
                    .or_insert_with(|| stats.clone());
            }
            let row = encode(&merged);
            let mode = &mut OpMode::Txn(self.txn);
            match existing {
                Some(_) => {
                    points.update(self.ctx, mode, &key, &row)?;
                }
                None => {
                    points.insert_unique(self.ctx, mode, &key, &row)?;
                    expiry.insert_unique(
                        self.ctx,
                        &mut OpMode::Txn(self.txn),
                        &expiry_key(period.saturating_add(resolution).saturating_add(keep), &key),
                        &[],
                    )?;
                }
            }
        }
        // The series, and the last minute it has (its points outlive it at
        // most by the longest keep).
        let last = minute.to_be_bytes();
        let mode = &mut OpMode::Txn(self.txn);
        match index.get(self.ctx, &series_key)? {
            Some(row) if row.as_slice() >= last.as_slice() => {}
            Some(_) => {
                index.update(self.ctx, mode, &series_key, &last)?;
            }
            None => {
                index.insert_unique(self.ctx, mode, &series_key, &last)?;
            }
        }
        Ok(())
    }

    /// Removes up to `limit` points past their keep at `now` (seconds),
    /// and the series none of whose points can remain; returns how many
    /// points went.
    pub fn expire_metrics(&mut self, now: u64, limit: usize) -> Result<usize, StorageError> {
        let (points, expiry, index) = (
            self.trees.metric_points(),
            self.trees.metric_expiry(),
            self.trees.metric_series(),
        );
        let due = expiry.scan_range_limit(
            self.ctx,
            None,
            Some(&now.saturating_add(1).to_be_bytes()),
            limit,
        )?;
        let removed = due.len();
        for (key, _) in due {
            let point = key.get(8..).ok_or_else(|| corrupt("metric expiry"))?;
            points.delete(self.ctx, &mut OpMode::Txn(self.txn), point)?;
            expiry.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        for (key, last) in index.scan(self.ctx)? {
            if stale(&last, now)? {
                index.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
            }
        }
        Ok(removed)
    }
}

impl ObjFile<'_> {
    /// Every series, and the last minute it has (seconds).
    pub fn metric_series(&mut self) -> Result<Vec<(MetricSeries, u64)>, StorageError> {
        let Some(index) = self.existing_tree(METRIC_SERIES_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        index
            .scan(&mut ctx)?
            .into_iter()
            .map(|(key, last)| {
                let (series, _) = MetricSeries::from_key(&key)?;
                let last = u64::from_be_bytes(
                    last.as_slice()
                        .try_into()
                        .map_err(|_| corrupt("metric series"))?,
                );
                Ok((series, last))
            })
            .collect()
    }

    /// `series`' points at `resolution` whose period starts in `from..to`
    /// (seconds), oldest first.
    pub fn metric_points(
        &mut self,
        series: &MetricSeries,
        resolution: u64,
        from: u64,
        to: u64,
    ) -> Result<Vec<(u64, MetricPoint)>, StorageError> {
        let Some(points) = self.existing_tree(METRIC_POINTS_TREE)? else {
            return Ok(Vec::new());
        };
        // Only the resolutions kept (a key holds the period as 32 bits).
        if from >= to || !RESOLUTIONS.iter().any(|(kept, _)| *kept == resolution) {
            return Ok(Vec::new());
        }
        let key = series.key();
        let mut ctx = self.core.page_ctx(0);
        points
            .scan_range(
                &mut ctx,
                Some(&point_key(&key, resolution, from)),
                Some(&point_key(&key, resolution, to - 1)),
            )?
            .into_iter()
            .map(|(row_key, row)| {
                let period = u64::from_be_bytes(
                    row_key[row_key.len() - 8..]
                        .try_into()
                        .map_err(|_| corrupt("metric point"))?,
                );
                Ok((period, decode(&row)?))
            })
            .collect()
    }

    /// Whether any point is past its keep at `now` (seconds), or any
    /// series has none left: a sweep with nothing to expire writes nothing.
    pub fn metrics_due(&mut self, now: u64) -> Result<bool, StorageError> {
        let (expiry, index) = (
            self.existing_tree(METRIC_EXPIRY_TREE)?,
            self.existing_tree(METRIC_SERIES_TREE)?,
        );
        let mut ctx = self.core.page_ctx(0);
        if let Some(expiry) = expiry
            && !expiry
                .scan_range_limit(
                    &mut ctx,
                    None,
                    Some(&now.saturating_add(1).to_be_bytes()),
                    1,
                )?
                .is_empty()
        {
            return Ok(true);
        }
        let Some(index) = index else {
            return Ok(false);
        };
        for (_, last) in index.scan(&mut ctx)? {
            if stale(&last, now)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_metric_is_corruption() {
        let point: MetricPoint = [(4u8, Stats::of(1.0))].into_iter().collect();
        let row = encode(&point);
        assert_eq!(decode(&row).unwrap(), point);
        assert!(decode(&[row.clone(), row].concat()).is_err());
        assert!(decode(&encode(&point)[..10]).is_err());
    }
}
