//! A CloudWatch API over the S3 metric history (namespace `AWS/S3`):
//! ListMetrics, GetMetricStatistics and GetMetricData, in the AWS JSON 1.0
//! protocol the SDKs speak to it (`X-Amz-Target:
//! GraniteServiceVersion20100801.<operation>`, signed for `monitoring`).
//! A caller sees the metrics of its own account's buckets, as allowed by
//! `cloudwatch:<operation>`.
//!
//! As CloudWatch: a start time is rounded down to the minute, five minutes
//! or hour by its age, which picks the resolution read (by the minute for
//! 15 days, five minutes for 63, the hour after); a period that is not a
//! multiple of it reads nothing. Percentiles (`pNN`) are read off a
//! latency's histogram (the bound a share of requests stays under).
//! Metric math and Metrics Insights are not supported.

use std::collections::{BTreeMap, HashMap, HashSet};

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use truthdb_engine::metrics::LATENCY_BOUNDS_MS;
use truthdb_object::{MetricSeries, RESOLUTIONS, SeriesBy, Stats};

use crate::error::S3Error;
use crate::metric_history::{ALL_STORAGE_TYPES, BUCKET_SIZE_BYTES, METRICS, NUMBER_OF_OBJECTS};
use crate::server::{Body, Connection, S3State, full};

const TARGET_PREFIX: &str = "GraniteServiceVersion20100801.";
const NAMESPACE: &str = "AWS/S3";
const MAX_BODY: usize = 1024 * 1024;
const MAX_STATISTICS_DATAPOINTS: u64 = 1440;
const MAX_QUERIES: usize = 500;
const MAX_DATAPOINTS: u64 = 100_800;
const LIST_PAGE: usize = 500;
const DAY: u64 = 86_400;

struct CwError {
    status: u16,
    code: &'static str,
    message: String,
}

impl CwError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> CwError {
        CwError {
            status,
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> CwError {
        CwError::new(400, "InvalidParameterValue", message)
    }

    fn missing(name: &str) -> CwError {
        CwError::new(
            400,
            "MissingParameter",
            format!("The parameter {name} is required."),
        )
    }
}

impl From<S3Error> for CwError {
    fn from(err: S3Error) -> CwError {
        CwError::new(err.status, err.code, err.message)
    }
}

impl From<truthdb_engine::engine::EngineError> for CwError {
    fn from(err: truthdb_engine::engine::EngineError) -> CwError {
        CwError::new(500, "InternalServiceError", err.to_string())
    }
}

/// Answers a CloudWatch request: its result, or its error as the JSON
/// protocol carries it (`__type`, `message`, and the query-mode code).
pub async fn handle(state: &S3State, request: Request<Incoming>) -> Response<Body> {
    let (status, body, code) = match serve(state, request).await {
        Ok(value) => (200, value, None),
        Err(err) => (
            err.status,
            json!({ "__type": err.code, "message": err.message }),
            Some((
                err.code,
                if err.status >= 500 {
                    "Receiver"
                } else {
                    "Sender"
                },
            )),
        ),
    };
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/x-amz-json-1.0");
    if let Some((code, fault)) = code {
        builder = builder.header("x-amzn-query-error", format!("{code};{fault}"));
    }
    builder
        .body(full(body.to_string()))
        .expect("a valid response")
}

async fn serve(state: &S3State, request: Request<Incoming>) -> Result<Value, CwError> {
    let crate::signed_body::SignedBody {
        parts,
        body,
        principal,
    } = crate::signed_body::read(state, request, "monitoring", MAX_BODY).await?;
    let operation = parts
        .headers
        .get("x-amz-target")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix(TARGET_PREFIX))
        .ok_or_else(|| CwError::new(400, "UnknownOperationException", "No operation is named."))?
        .to_string();
    let input: Map<String, Value> = if body.is_empty() {
        Map::new()
    } else {
        match serde_json::from_slice(&body) {
            Ok(Value::Object(map)) => map,
            _ => {
                return Err(CwError::new(
                    400,
                    "SerializationException",
                    "The body is not a JSON object.",
                ));
            }
        }
    };
    let authz = crate::authz::Authz::load(
        state,
        Some(&principal),
        Some(crate::server::Signed {
            auth_type: "REST-HEADER",
            at_ms: parts
                .headers
                .get("x-amz-date")
                .and_then(|value| value.to_str().ok())
                .and_then(crate::server::parse_amz_date)
                .map(|secs| secs * 1000),
        }),
        &parts.headers,
        "",
        parts.extensions.get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    if !matches!(
        operation.as_str(),
        "ListMetrics" | "GetMetricStatistics" | "GetMetricData"
    ) {
        return Err(CwError::new(
            400,
            "UnknownOperationException",
            format!("{operation} is not supported."),
        ));
    }
    authz.check_arn(&format!("cloudwatch:{operation}"), "*", &principal.account)?;
    let account = principal.account.clone();
    let now = crate::objects::now_ms() / 1000;
    // The caller's buckets, and when each was made: its series' points
    // from then on (a bucket of the same name before it was another's).
    let owned: Owned = state
        .engine
        .run_object(move |file| {
            Ok(file
                .list_buckets()?
                .into_iter()
                .filter(|bucket| bucket.owner == account)
                .map(|bucket| (bucket.name, bucket.created_ms / 1000))
                .collect())
        })
        .await?;
    match operation.as_str() {
        "ListMetrics" => list_metrics(state, &input, &owned, now).await,
        "GetMetricStatistics" => statistics(state, &input, &owned, now).await,
        _ => metric_data(state, &input, &owned, now).await,
    }
}

/// The caller's buckets, and when each was made (seconds).
type Owned = HashMap<String, u64>;

fn string<'a>(input: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>, CwError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(CwError::invalid(format!(
            "The parameter {name} must be a string."
        ))),
    }
}

fn number(input: &Map<String, Value>, name: &str) -> Result<Option<f64>, CwError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => Ok(number.as_f64()),
        Some(_) => Err(CwError::invalid(format!(
            "The parameter {name} must be a number."
        ))),
    }
}

/// A timestamp (epoch seconds, fractions dropped).
fn time(input: &Map<String, Value>, name: &str) -> Result<u64, CwError> {
    let value = number(input, name)?.ok_or_else(|| CwError::missing(name))?;
    if !(0.0..=4e12).contains(&value) {
        return Err(CwError::invalid(format!(
            "The parameter {name} is out of range."
        )));
    }
    Ok(value as u64)
}

/// A query's time range is not empty.
fn check_range(start: u64, end: u64) -> Result<(), CwError> {
    if start >= end {
        return Err(CwError::invalid(
            "The parameter StartTime must be less than the parameter EndTime.",
        ));
    }
    Ok(())
}

fn period(input: &Map<String, Value>) -> Result<u64, CwError> {
    let value = number(input, "Period")?.ok_or_else(|| CwError::missing("Period"))?;
    if value < 1.0 || value.fract() != 0.0 || value > f64::from(i32::MAX) {
        return Err(CwError::invalid(
            "The parameter Period must be a positive whole number of seconds.",
        ));
    }
    Ok(value as u64)
}

/// `Dimensions`' name and value pairs.
fn dimensions(input: &Map<String, Value>) -> Result<Vec<(String, Option<String>)>, CwError> {
    let Some(list) = input.get("Dimensions") else {
        return Ok(Vec::new());
    };
    let Value::Array(list) = list else {
        return Err(CwError::invalid("The parameter Dimensions must be a list."));
    };
    if list.len() > 30 {
        return Err(CwError::invalid(
            "The parameter Dimensions holds at most 30 dimensions.",
        ));
    }
    list.iter()
        .map(|dimension| {
            let Value::Object(dimension) = dimension else {
                return Err(CwError::invalid("A dimension must be an object."));
            };
            let name =
                string(dimension, "Name")?.ok_or_else(|| CwError::missing("Dimensions.Name"))?;
            Ok((
                name.to_string(),
                string(dimension, "Value")?.map(str::to_string),
            ))
        })
        .collect()
}

/// The metric named `name`: its code and unit.
fn metric(name: &str) -> Option<(u8, &'static str)> {
    METRICS
        .iter()
        .find(|(_, metric, _)| *metric == name)
        .map(|(code, _, unit)| (*code, *unit))
}

/// The series a metric with these dimensions is kept under, if it is one
/// of the caller's.
fn series_of(
    code: u8,
    dimensions: &[(String, Option<String>)],
    owned: &Owned,
) -> Option<(MetricSeries, u64)> {
    let value = |name: &str| {
        dimensions
            .iter()
            .find(|(dimension, _)| dimension == name)
            .and_then(|(_, value)| value.clone())
    };
    let bucket = value("BucketName")?;
    let created = *owned.get(&bucket)?;
    if dimensions.len() != 2 {
        return None;
    }
    let by = match code {
        BUCKET_SIZE_BYTES => SeriesBy::StorageType(value("StorageType")?),
        NUMBER_OF_OBJECTS => {
            let storage = value("StorageType")?;
            (storage == ALL_STORAGE_TYPES).then_some(SeriesBy::StorageType(storage))?
        }
        _ => SeriesBy::Filter(value("FilterId")?),
    };
    Some((MetricSeries { bucket, by }, created))
}

/// The resolution read for a start time at `now`, and the start rounded
/// down to it.
fn resolution_for(start: u64, now: u64) -> (u64, u64) {
    let age = now.saturating_sub(start);
    let resolution = if age < 15 * DAY {
        60
    } else if age < 63 * DAY {
        300
    } else {
        3600
    };
    (resolution, start - start % resolution)
}

/// Whether the point of `resolution` seconds at `at` is read: not past its
/// `keep` at `now` (though not yet expired), and not over before `since`,
/// when the bucket was made (those were another's of the same name;
/// deleting it removed its points that could last past then).
fn readable(at: u64, resolution: u64, keep: u64, since: u64, now: u64) -> bool {
    at.saturating_add(resolution).saturating_add(keep) > now
        && at.saturating_add(resolution) > since
}

/// A metric's statistics per period of `period` seconds from `start` to
/// `end`, oldest first (only the periods with any), from the points that
/// end after `since` (the bucket's making), read under a storage lock hold
/// of their own.
#[allow(clippy::too_many_arguments)]
async fn aggregate(
    state: &S3State,
    series: &MetricSeries,
    since: u64,
    code: u8,
    start: u64,
    end: u64,
    period: u64,
    now: u64,
) -> Result<Vec<(u64, Stats)>, CwError> {
    let (resolution, start) = resolution_for(start, now);
    if !period.is_multiple_of(resolution) || end <= start {
        return Ok(Vec::new());
    }
    let keep = RESOLUTIONS
        .iter()
        .find(|(kept, _)| *kept == resolution)
        .map_or(0, |(_, keep)| *keep);
    let mut periods: BTreeMap<u64, Stats> = BTreeMap::new();
    let read = series.clone();
    let points = state
        .engine
        .run_object(move |file| file.metric_points(&read, resolution, start, end))
        .await?;
    for (at, point) in points {
        if !readable(at, resolution, keep, since, now) {
            continue;
        }
        let Some(stats) = point.get(&code) else {
            continue;
        };
        let slot = start + (at - start) / period * period;
        periods
            .entry(slot)
            .and_modify(|kept| kept.merge(stats))
            .or_insert_with(|| stats.clone());
    }
    Ok(periods.into_iter().collect())
}

/// The bound a share `percent` of a latency's values stays under, read off
/// its histogram (none for other metrics).
fn percentile(stats: &Stats, percent: f64) -> Option<f64> {
    if stats.histogram.is_empty() || stats.count == 0 {
        return None;
    }
    if percent <= 0.0 {
        return Some(stats.min);
    }
    if percent >= 100.0 {
        return Some(stats.max);
    }
    let wanted = (percent / 100.0 * stats.count as f64).ceil() as u64;
    let mut seen = 0;
    for (index, count) in stats.histogram.iter().enumerate() {
        seen += count;
        if seen >= wanted.max(1) {
            let bound = LATENCY_BOUNDS_MS
                .get(index)
                .map_or(stats.max, |bound| *bound as f64);
            return Some(bound.clamp(stats.min, stats.max));
        }
    }
    Some(stats.max)
}

/// A percentile statistic's share (`p99` is 99).
fn percent_of(statistic: &str) -> Option<f64> {
    let percent: f64 = statistic.strip_prefix('p')?.parse().ok()?;
    (0.0..=100.0).contains(&percent).then_some(percent)
}

/// A statistic's value.
fn statistic(stats: &Stats, name: &str) -> Option<f64> {
    match name {
        "SampleCount" => Some(stats.count as f64),
        "Sum" => Some(stats.sum),
        "Average" => Some(stats.sum / stats.count.max(1) as f64),
        "Minimum" => Some(stats.min),
        "Maximum" => Some(stats.max),
        other => percent_of(other).and_then(|percent| percentile(stats, percent)),
    }
}

fn strings(input: &Map<String, Value>, name: &str) -> Result<Vec<String>, CwError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(list)) => list
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| CwError::invalid(format!("{name} holds strings.")))
            })
            .collect(),
        Some(_) => Err(CwError::invalid(format!(
            "The parameter {name} must be a list."
        ))),
    }
}

async fn statistics(
    state: &S3State,
    input: &Map<String, Value>,
    owned: &Owned,
    now: u64,
) -> Result<Value, CwError> {
    let namespace = string(input, "Namespace")?.ok_or_else(|| CwError::missing("Namespace"))?;
    let name = string(input, "MetricName")?.ok_or_else(|| CwError::missing("MetricName"))?;
    let (start, end, period) = (
        time(input, "StartTime")?,
        time(input, "EndTime")?,
        period(input)?,
    );
    check_range(start, end)?;
    let (plain, extended) = (
        strings(input, "Statistics")?,
        strings(input, "ExtendedStatistics")?,
    );
    if plain.is_empty() == extended.is_empty() {
        return Err(CwError::new(
            400,
            "InvalidParameterCombination",
            "Must specify either Statistics or ExtendedStatistics, but not both.",
        ));
    }
    if plain.len() > 5
        || plain.iter().any(|statistic| {
            !matches!(
                statistic.as_str(),
                "SampleCount" | "Average" | "Sum" | "Minimum" | "Maximum"
            )
        })
    {
        return Err(CwError::invalid(
            "Statistics holds 1 to 5 of SampleCount, Average, Sum, Minimum and Maximum.",
        ));
    }
    if extended.len() > 10
        || extended
            .iter()
            .any(|statistic| percent_of(statistic).is_none())
    {
        return Err(CwError::invalid(
            "ExtendedStatistics holds 1 to 10 percentiles from p0.0 to p100.",
        ));
    }
    let (_, rounded) = resolution_for(start, now);
    let requested = end.saturating_sub(rounded).div_ceil(period);
    if requested > MAX_STATISTICS_DATAPOINTS {
        return Err(CwError::new(
            400,
            "InvalidParameterCombination",
            format!(
                "You have requested up to {requested} datapoints, which exceeds the limit of {MAX_STATISTICS_DATAPOINTS}."
            ),
        ));
    }
    let unit = string(input, "Unit")?;
    let found = (namespace == NAMESPACE)
        .then(|| metric(name))
        .flatten()
        .filter(|(_, metric_unit)| unit.is_none_or(|unit| unit == *metric_unit));
    let mut datapoints = Vec::new();
    if let Some((code, metric_unit)) = found
        && let Some((series, since)) = series_of(code, &dimensions(input)?, owned)
    {
        for (at, stats) in aggregate(state, &series, since, code, start, end, period, now).await? {
            let mut point = Map::new();
            point.insert("Timestamp".to_string(), json!(at));
            for name in &plain {
                if let Some(value) = statistic(&stats, name) {
                    point.insert(name.clone(), json!(value));
                }
            }
            if !extended.is_empty() {
                let values: Map<String, Value> = extended
                    .iter()
                    .filter_map(|name| {
                        statistic(&stats, name).map(|value| (name.clone(), json!(value)))
                    })
                    .collect();
                point.insert("ExtendedStatistics".to_string(), Value::Object(values));
            }
            point.insert("Unit".to_string(), json!(metric_unit));
            datapoints.push(Value::Object(point));
        }
    }
    Ok(json!({ "Label": name, "Datapoints": datapoints }))
}

/// One GetMetricData query: its id, label and whether its data is
/// returned, and its values newest first or oldest first.
struct QueryResult {
    id: String,
    label: String,
    return_data: bool,
    points: Vec<(u64, f64)>,
}

async fn metric_data(
    state: &S3State,
    input: &Map<String, Value>,
    owned: &Owned,
    now: u64,
) -> Result<Value, CwError> {
    let (start, end) = (time(input, "StartTime")?, time(input, "EndTime")?);
    check_range(start, end)?;
    let Some(Value::Array(queries)) = input.get("MetricDataQueries") else {
        return Err(CwError::missing("MetricDataQueries"));
    };
    if queries.is_empty() || queries.len() > MAX_QUERIES {
        return Err(CwError::invalid(format!(
            "MetricDataQueries holds 1 to {MAX_QUERIES} queries."
        )));
    }
    let descending = match string(input, "ScanBy")? {
        None | Some("TimestampDescending") => true,
        Some("TimestampAscending") => false,
        Some(_) => {
            return Err(CwError::invalid(
                "ScanBy is TimestampDescending or TimestampAscending.",
            ));
        }
    };
    let max = match number(input, "MaxDatapoints")? {
        None => MAX_DATAPOINTS,
        Some(value) if value >= 1.0 && value.fract() == 0.0 => (value as u64).min(MAX_DATAPOINTS),
        Some(_) => {
            return Err(CwError::invalid(
                "MaxDatapoints must be a positive integer.",
            ));
        }
    };
    // A token is how many values earlier pages returned.
    let skip = match string(input, "NextToken")? {
        None => 0,
        Some(token) => token.parse::<u64>().map_err(|_| {
            CwError::new(
                400,
                "InvalidNextToken",
                "The next token specified is invalid.",
            )
        })?,
    };
    let mut ids = HashSet::new();
    let mut results = Vec::with_capacity(queries.len());
    for query in queries {
        let Value::Object(query) = query else {
            return Err(CwError::invalid("A metric data query must be an object."));
        };
        let id = string(query, "Id")?.ok_or_else(|| CwError::missing("MetricDataQueries.Id"))?;
        if !id.starts_with(|first: char| first.is_ascii_lowercase())
            || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || id.len() > 255
        {
            return Err(CwError::invalid(format!(
                "The value {id} for parameter MetricDataQueries.member.Id is invalid: it starts with a lowercase letter and holds letters, digits and underscores."
            )));
        }
        if !ids.insert(id.to_string()) {
            return Err(CwError::invalid(format!(
                "The Id {id} is used more than once."
            )));
        }
        if query.get("Expression").is_some() {
            return Err(CwError::invalid(
                "Metric math expressions and Metrics Insights queries are not supported.",
            ));
        }
        let Some(Value::Object(stat)) = query.get("MetricStat") else {
            return Err(CwError::missing("MetricDataQueries.MetricStat"));
        };
        let Some(Value::Object(metric_map)) = stat.get("Metric") else {
            return Err(CwError::missing("MetricStat.Metric"));
        };
        let namespace =
            string(metric_map, "Namespace")?.ok_or_else(|| CwError::missing("Metric.Namespace"))?;
        let name = string(metric_map, "MetricName")?
            .ok_or_else(|| CwError::missing("Metric.MetricName"))?;
        let stat_name = string(stat, "Stat")?.ok_or_else(|| CwError::missing("MetricStat.Stat"))?;
        if statistic(&Stats::of(0.0), stat_name).is_none() && percent_of(stat_name).is_none() {
            return Err(CwError::invalid(format!(
                "The statistic {stat_name} is not valid."
            )));
        }
        let period = period(stat)?;
        let unit = string(stat, "Unit")?;
        let label =
            string(query, "Label")?.map_or_else(|| format!("{namespace} {name}"), str::to_string);
        let return_data = query
            .get("ReturnData")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let mut points = Vec::new();
        if namespace == NAMESPACE
            && let Some((code, metric_unit)) = metric(name)
            && unit.is_none_or(|unit| unit == metric_unit)
            && let Some((series, since)) = series_of(code, &dimensions(metric_map)?, owned)
        {
            for (at, stats) in
                aggregate(state, &series, since, code, start, end, period, now).await?
            {
                if let Some(value) = statistic(&stats, stat_name) {
                    points.push((at, value));
                }
            }
        }
        if descending {
            points.reverse();
        }
        results.push(QueryResult {
            id: id.to_string(),
            label,
            return_data,
            points,
        });
    }
    // Pages of `max` values across the results, in query order.
    let mut to_skip = skip;
    let mut room = max;
    let mut out = Vec::with_capacity(results.len());
    let mut more = false;
    for result in results {
        if !result.return_data {
            out.push(json!({ "Id": result.id, "StatusCode": "Complete" }));
            continue;
        }
        let total = result.points.len() as u64;
        let skipped = to_skip.min(total);
        to_skip -= skipped;
        let taken = (total - skipped).min(room);
        room -= taken;
        let partial = skipped + taken < total;
        more |= partial;
        let page = &result.points[skipped as usize..(skipped + taken) as usize];
        out.push(json!({
            "Id": result.id,
            "Label": result.label,
            "StatusCode": if partial { "PartialData" } else { "Complete" },
            "Timestamps": page.iter().map(|(at, _)| json!(at)).collect::<Vec<_>>(),
            "Values": page.iter().map(|(_, value)| json!(value)).collect::<Vec<_>>(),
        }));
    }
    let mut answer = json!({ "MetricDataResults": out, "Messages": [] });
    if more {
        answer["NextToken"] = json!((skip + max).to_string());
    }
    Ok(answer)
}

async fn list_metrics(
    state: &S3State,
    input: &Map<String, Value>,
    owned: &Owned,
    now: u64,
) -> Result<Value, CwError> {
    let namespace = string(input, "Namespace")?;
    let name = string(input, "MetricName")?;
    let filters = dimensions(input)?;
    // Metrics with data in the last two weeks, or three hours.
    let since = match string(input, "RecentlyActive")? {
        None => now.saturating_sub(14 * DAY),
        Some("PT3H") => now.saturating_sub(3 * 3600),
        Some(_) => return Err(CwError::invalid("RecentlyActive is PT3H.")),
    };
    let skip = match string(input, "NextToken")? {
        None => 0,
        Some(token) => token.parse::<usize>().map_err(|_| {
            CwError::new(
                400,
                "InvalidNextToken",
                "The next token specified is invalid.",
            )
        })?,
    };
    let mut metrics = Vec::new();
    if namespace.is_none_or(|namespace| namespace == NAMESPACE) {
        let all = state.engine.run_object(|file| file.metric_series()).await?;
        for (series, last) in all {
            // Quiet lately, or not the caller's.
            let Some(&created) = owned.get(&series.bucket) else {
                continue;
            };
            if last < since {
                continue;
            }
            let (dimension, value) = match &series.by {
                SeriesBy::Filter(id) => ("FilterId", id.clone()),
                SeriesBy::StorageType(storage) => ("StorageType", storage.clone()),
            };
            let dims = [("BucketName", series.bucket.clone()), (dimension, value)];
            if !filters.iter().all(|(name, value)| {
                dims.iter().any(|(dim, dim_value)| {
                    dim == name && value.as_ref().is_none_or(|value| value == dim_value)
                })
            }) {
                continue;
            }
            // The metrics the series has data for since then (its hours).
            let mut codes = std::collections::BTreeSet::new();
            let read = series.clone();
            let points = state
                .engine
                .run_object(move |file| {
                    file.metric_points(&read, 3600, since - since % 3600, now + 1)
                })
                .await?;
            for (at, point) in points {
                if at + 3600 > created {
                    codes.extend(point.keys().copied());
                }
            }
            for code in codes {
                let Some((_, metric_name, _)) = METRICS.iter().find(|(known, _, _)| *known == code)
                else {
                    continue;
                };
                if name.is_some_and(|name| name != *metric_name) {
                    continue;
                }
                metrics.push(json!({
                    "Namespace": NAMESPACE,
                    "MetricName": metric_name,
                    "Dimensions": dims
                        .iter()
                        .map(|(name, value)| json!({ "Name": name, "Value": value }))
                        .collect::<Vec<_>>(),
                }));
            }
        }
    }
    let page: Vec<Value> = metrics.iter().skip(skip).take(LIST_PAGE).cloned().collect();
    let mut answer = json!({ "Metrics": page });
    if metrics.len() > skip.saturating_add(LIST_PAGE) {
        answer["NextToken"] = json!((skip + LIST_PAGE).to_string());
    }
    Ok(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn points_over_before_the_bucket_was_made_are_not_read() {
        // A bucket made at 1000 s reads the minute it was made in, not the
        // one before; nor a point past its keep.
        assert!(readable(960, 60, 600, 1000, 1100));
        assert!(!readable(900, 60, 600, 1000, 1100));
        assert!(readable(0, 3600, 600, 1000, 1100));
        assert!(!readable(0, 60, 600, 0, 660));
    }

    #[test]
    fn percentiles_are_read_off_the_histogram() {
        let mut stats = Stats::of(0.5);
        // 10 values: 6 under 1 ms, 3 under 4 ms, 1 under 1024 ms.
        stats.count = 10;
        stats.min = 0.2;
        stats.max = 700.0;
        stats.histogram = vec![6, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(percentile(&stats, 50.0), Some(1.0));
        assert_eq!(percentile(&stats, 90.0), Some(4.0));
        assert_eq!(
            percentile(&stats, 99.0),
            Some(700.0),
            "the bound past max is max"
        );
        assert_eq!(percentile(&stats, 0.0), Some(0.2));
        assert_eq!(percentile(&stats, 100.0), Some(700.0));
        assert_eq!(percentile(&Stats::of(3.0), 50.0), None, "no histogram");
        assert_eq!(percent_of("p99.9"), Some(99.9));
        assert_eq!(percent_of("p101"), None);
        assert_eq!(percent_of("tm99"), None);
    }

    #[test]
    fn a_start_picks_the_resolution_by_its_age() {
        let now = 100 * DAY;
        assert_eq!(resolution_for(now - 3600 - 59, now), (60, now - 3600 - 60));
        assert_eq!(resolution_for(now - 20 * DAY - 7, now).0, 300);
        assert_eq!(resolution_for(now - 70 * DAY, now).0, 3600);
    }
}
