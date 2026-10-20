//! S3 Vectors' JSON API (S3 plan item 17) on the S3 listener: requests are
//! `POST /<Operation>` with a JSON body, SigV4-signed for the `s3vectors`
//! service (told apart from S3 requests by that), over the vector engine.
//!
//! Operations: CreateVectorBucket, GetVectorBucket, ListVectorBuckets,
//! DeleteVectorBucket, CreateIndex, GetIndex, ListIndexes, DeleteIndex,
//! PutVectors, GetVectors, ListVectors, DeleteVectors, QueryVectors. The
//! bucket policy, tagging and index-mode operations answer NotImplemented.
//! Buckets and indexes may be named, or given by ARN
//! (`arn:aws:s3vectors:<region>:<account>:bucket/<name>[/index/<name>]`).
//! Errors carry their type in `x-amzn-ErrorType` and a JSON `message`.
//!
//! As AWS's SDKs send no `x-amz-content-sha256` for this service, the body
//! is read (at most 20 MiB) and hashed before the signature is checked: so
//! the access key must exist first, the read must finish within a minute,
//! and at most 16 requests read bodies at once (the rest are told to retry).
//!
//! A QueryVectors response holds all its results (no `nextToken`): one scan
//! per query, and no pages drawn from a moving index. A PutVectors or
//! DeleteVectors batch is written over as many transactions as it takes, so
//! a failure part way leaves the vectors before it written.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use truthdb_engine::engine::{Engine, EngineError};
use truthdb_vector::{
    DistanceMetric, IndexInfo, NewIndex, Query, QueryError, VectorError, VectorInput,
};

use crate::error::S3Error;
use crate::server::{Body, S3State, check_signed_headers, full, signed_request, verify_signature};

/// The largest request body.
const MAX_BODY: usize = 20 << 20;
/// The account id in this server's ARNs.
const ACCOUNT: &str = "000000000000";
/// How long a request body may take to arrive.
const BODY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Requests that may read their bodies (before their signature can be
/// checked) at once: at most this many times [`MAX_BODY`] of memory.
static READING: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);

/// An S3 Vectors error: HTTP status, error type and message.
#[derive(Debug)]
struct VError {
    status: u16,
    kind: &'static str,
    message: String,
}

impl VError {
    fn new(status: u16, kind: &'static str, message: impl Into<String>) -> Self {
        VError {
            status,
            kind,
            message: message.into(),
        }
    }

    fn validation(message: impl Into<String>) -> Self {
        VError::new(400, "ValidationException", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        VError::new(404, "NotFoundException", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        VError::new(409, "ConflictException", message)
    }

    fn internal(message: impl Into<String>) -> Self {
        VError::new(500, "InternalServerException", message)
    }
}

impl From<S3Error> for VError {
    fn from(err: S3Error) -> Self {
        let kind = match err.status {
            400 => "ValidationException",
            403 => "AccessDeniedException",
            501 => "NotImplemented",
            _ => "InternalServerException",
        };
        VError::new(err.status, kind, err.message)
    }
}

impl From<EngineError> for VError {
    fn from(err: EngineError) -> Self {
        VError::internal(err.to_string())
    }
}

/// Answers one S3 Vectors request.
pub async fn handle(state: &S3State, request: Request<Incoming>) -> Response<Body> {
    let (status, body, kind) = match serve(state, request).await {
        Ok(value) => (200, value, None),
        Err(err) => {
            let mut body = json!({ "message": err.message });
            if err.kind == "ValidationException" {
                body["fieldList"] = json!([]);
            }
            (err.status, body, Some(err.kind))
        }
    };
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/json");
    if let Some(kind) = kind {
        builder = builder.header("x-amzn-ErrorType", kind);
    }
    builder
        .body(full(body.to_string()))
        .expect("a valid response")
}

async fn serve(state: &S3State, request: Request<Incoming>) -> Result<Value, VError> {
    let (auth, amz_date) = signed_request(state, &request, "s3vectors")?;
    check_signed_headers(&request, &auth, &["host", "x-amz-date"])?;
    // The signature covers the body, which must be read first: refuse an
    // unknown key before that, and bound how many bodies are read at once and
    // for how long.
    let access_key = auth.credential.access_key.clone();
    if state
        .engine
        .run_object(move |file: &mut truthdb_object::ObjFile<'_>| {
            file.access_key_secret(&access_key)
        })
        .await?
        .is_none()
    {
        return Err(S3Error::invalid_access_key().into());
    }
    let Ok(_reading) = READING.try_acquire() else {
        return Err(VError::new(
            503,
            "ServiceUnavailableException",
            "The service is busy; retry the request.",
        ));
    };
    let (parts, body) = request.into_parts();
    let body = tokio::time::timeout(BODY_TIMEOUT, read_body(body))
        .await
        .map_err(|_| {
            VError::new(
                408,
                "RequestTimeoutException",
                "The request body took too long to arrive.",
            )
        })??;
    let hash = hex(&ring::digest::digest(&ring::digest::SHA256, &body));
    if let Some(sent) = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        && !sent.eq_ignore_ascii_case(&hash)
    {
        return Err(S3Error::signature_mismatch().into());
    }
    verify_signature(
        state,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &auth,
        &amz_date,
        &hash,
    )
    .await?;
    let operation = parts.uri.path().trim_start_matches('/').to_string();
    if parts.method != Method::POST {
        return Err(not_implemented(&operation));
    }
    let input: Map<String, Value> = if body.is_empty() {
        Map::new()
    } else {
        match serde_json::from_slice(&body) {
            Ok(Value::Object(map)) => map,
            _ => return Err(VError::validation("The request body is not a JSON object.")),
        }
    };
    let region = state.region.clone();
    state
        .engine
        .run_engine(move |engine| Ok(dispatch(engine, &region, &operation, &input)))
        .await?
}

fn not_implemented(operation: &str) -> VError {
    VError::new(
        501,
        "NotImplemented",
        format!("{operation} is not implemented."),
    )
}

async fn read_body(body: Incoming) -> Result<Bytes, VError> {
    let limited = http_body_util::Limited::new(body, MAX_BODY);
    limited
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|_| {
            VError::new(
                413,
                "ValidationException",
                format!("The request body is larger than {MAX_BODY} bytes."),
            )
        })
}

fn hex(digest: &ring::digest::Digest) -> String {
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn bucket_arn(region: &str, bucket: &str) -> String {
    format!("arn:aws:s3vectors:{region}:{ACCOUNT}:bucket/{bucket}")
}

fn index_arn(region: &str, bucket: &str, index: &str) -> String {
    format!("{}/index/{index}", bucket_arn(region, bucket))
}

/// An ARN's bucket and (when it names one) index: an ARN of this server
/// (its region and account).
fn parse_arn(arn: &str, region: &str) -> Option<(String, Option<String>)> {
    let rest = arn.strip_prefix("arn:")?;
    let mut parts = rest.splitn(5, ':');
    let (partition, service, arn_region, account, resource) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    if partition != "aws" || service != "s3vectors" || arn_region != region || account != ACCOUNT {
        return None;
    }
    let resource = resource.strip_prefix("bucket/")?;
    Some(match resource.split_once("/index/") {
        Some((bucket, index)) => (bucket.to_string(), Some(index.to_string())),
        None => (resource.to_string(), None),
    })
}

fn string<'a>(input: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>, VError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(VError::validation(format!("{name} must be a string."))),
    }
}

fn integer(
    input: &Map<String, Value>,
    name: &str,
    range: std::ops::RangeInclusive<u64>,
) -> Result<Option<u64>, VError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|n| range.contains(n))
            .map(Some)
            .ok_or_else(|| {
                VError::validation(format!(
                    "{name} must be an integer from {} to {}.",
                    range.start(),
                    range.end()
                ))
            }),
    }
}

fn boolean(input: &Map<String, Value>, name: &str) -> Result<bool, VError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(VError::validation(format!("{name} must be a boolean."))),
    }
}

/// The vector bucket a request names (by name or ARN, not both).
fn bucket_of(input: &Map<String, Value>, region: &str) -> Result<String, VError> {
    if let Some(arn) = string(input, "vectorBucketArn")? {
        if string(input, "vectorBucketName")?.is_some() {
            return Err(VError::validation(
                "Give vectorBucketName or vectorBucketArn, not both.",
            ));
        }
        return match parse_arn(arn, region) {
            Some((bucket, None)) => Ok(bucket),
            _ => Err(VError::validation(
                "vectorBucketArn is not a vector bucket ARN of this server.",
            )),
        };
    }
    string(input, "vectorBucketName")?
        .map(str::to_string)
        .ok_or_else(|| VError::validation("vectorBucketName or vectorBucketArn is required."))
}

/// The vector index a request names (by bucket and name, or ARN, not
/// both), as it is.
fn index_of(
    engine: &Engine,
    input: &Map<String, Value>,
    region: &str,
) -> Result<IndexInfo, VError> {
    let (bucket, name) = match string(input, "indexArn")? {
        Some(arn) => {
            if ["vectorBucketName", "vectorBucketArn", "indexName"]
                .iter()
                .any(|name| input.get(*name).is_some_and(|value| !value.is_null()))
            {
                return Err(VError::validation(
                    "Give indexArn, or the bucket and indexName, not both.",
                ));
            }
            match parse_arn(arn, region) {
                Some((bucket, Some(index))) => (bucket, index),
                _ => {
                    return Err(VError::validation(
                        "indexArn is not a vector index ARN of this server.",
                    ));
                }
            }
        }
        None => (
            bucket_of(input, region)?,
            string(input, "indexName")?
                .ok_or_else(|| VError::validation("indexName or indexArn is required."))?
                .to_string(),
        ),
    };
    engine
        .vector(|file| file.index(&bucket, &name))?
        .ok_or_else(|| {
            VError::not_found(format!("The vector index {bucket}/{name} does not exist."))
        })
}

fn vector_error(err: VectorError) -> VError {
    match err {
        VectorError::NoSuchBucket => VError::not_found("The vector bucket does not exist."),
        VectorError::NoSuchIndex => VError::not_found("The vector index does not exist."),
        VectorError::BucketExists => VError::conflict("The vector bucket already exists."),
        VectorError::IndexExists => VError::conflict("The vector index already exists."),
        VectorError::BucketNotEmpty => {
            VError::conflict("The vector bucket is not empty: delete its indexes first.")
        }
        VectorError::Invalid(message) => VError::validation(message),
    }
}

/// A `{"float32": [...]}` vector.
fn vector_data(value: Option<&Value>, name: &str) -> Result<Vec<f32>, VError> {
    value
        .and_then(|data| data.get("float32"))
        .and_then(Value::as_array)
        .ok_or_else(|| VError::validation(format!("{name} must be {{\"float32\": [numbers]}}.")))?
        .iter()
        .map(|value| {
            value
                .as_f64()
                .map(|number| number as f32)
                .ok_or_else(|| VError::validation(format!("{name} values must be numbers.")))
        })
        .collect()
}

/// A list of 1 to `max` keys.
fn keys(input: &Map<String, Value>, max: usize) -> Result<Vec<String>, VError> {
    let items = input
        .get("keys")
        .and_then(Value::as_array)
        .filter(|items| (1..=max).contains(&items.len()))
        .ok_or_else(|| VError::validation(format!("keys must list 1 to {max} keys.")))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| VError::validation("keys must be strings."))
        })
        .collect()
}

/// A listing's `prefix` (none is the empty prefix; an empty one is
/// refused, as in AWS).
fn prefix(input: &Map<String, Value>) -> Result<String, VError> {
    match string(input, "prefix")? {
        Some("") => Err(VError::validation("prefix must not be empty.")),
        other => Ok(other.unwrap_or("").to_string()),
    }
}

fn token(after: &str) -> String {
    STANDARD.encode(after)
}

fn untoken(input: &Map<String, Value>) -> Result<Option<String>, VError> {
    string(input, "nextToken")?
        .map(|token| {
            STANDARD
                .decode(token)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .ok_or_else(|| VError::validation("nextToken is not valid."))
        })
        .transpose()
}

fn seconds(ms: u64) -> Value {
    json!(ms as f64 / 1000.0)
}

fn entry_json(entry: truthdb_vector::VectorEntry) -> Value {
    let mut out = json!({ "key": entry.key });
    if let Some(data) = entry.data {
        out["data"] = json!({ "float32": data });
    }
    if let Some(metadata) = entry.metadata {
        out["metadata"] = Value::Object(metadata);
    }
    out
}

fn dispatch(
    engine: &Engine,
    region: &str,
    operation: &str,
    input: &Map<String, Value>,
) -> Result<Value, VError> {
    match operation {
        "CreateVectorBucket" => {
            let bucket = string(input, "vectorBucketName")?
                .ok_or_else(|| VError::validation("vectorBucketName is required."))?;
            if let Some(Value::Object(encryption)) = input.get("encryptionConfiguration")
                && string(encryption, "sseType")?.is_some_and(|sse| sse != "AES256")
            {
                return Err(not_implemented("SSE-KMS"));
            }
            engine
                .vector(|file| file.transaction(|tx| tx.create_bucket(bucket, now_ms())))?
                .map_err(vector_error)?;
            Ok(json!({ "vectorBucketArn": bucket_arn(region, bucket) }))
        }
        "GetVectorBucket" => {
            let bucket = bucket_of(input, region)?;
            let info = engine.vector(|file| file.bucket(&bucket))?.ok_or_else(|| {
                VError::not_found(format!("The vector bucket {bucket} does not exist."))
            })?;
            Ok(json!({ "vectorBucket": {
                "vectorBucketName": info.name,
                "vectorBucketArn": bucket_arn(region, &info.name),
                "creationTime": seconds(info.created_ms),
                "encryptionConfiguration": { "sseType": "AES256" },
            }}))
        }
        "ListVectorBuckets" => {
            let limit = integer(input, "maxResults", 1..=500)?.unwrap_or(500) as usize;
            let prefix = prefix(input)?;
            let mut after = untoken(input)?;
            let mut out = Vec::new();
            let mut more = false;
            'pages: loop {
                let page = engine.vector(|file| file.list_buckets(after.as_deref(), 500))?;
                let last_page = page.len() < 500;
                for bucket in page {
                    after = Some(bucket.name.clone());
                    if !bucket.name.starts_with(&prefix) {
                        if bucket.name.as_str() > prefix.as_str() {
                            break 'pages;
                        }
                        continue;
                    }
                    if out.len() == limit {
                        more = true;
                        break 'pages;
                    }
                    out.push(json!({
                        "vectorBucketName": bucket.name,
                        "vectorBucketArn": bucket_arn(region, &bucket.name),
                        "creationTime": seconds(bucket.created_ms),
                    }));
                }
                if last_page {
                    break;
                }
            }
            let mut response = json!({ "vectorBuckets": out });
            if more && let Some(last) = response["vectorBuckets"].as_array().and_then(|l| l.last())
            {
                response["nextToken"] =
                    json!(token(last["vectorBucketName"].as_str().unwrap_or("")));
            }
            Ok(response)
        }
        "DeleteVectorBucket" => {
            let bucket = bucket_of(input, region)?;
            engine
                .vector(|file| file.transaction(|tx| tx.delete_bucket(&bucket)))?
                .map_err(vector_error)?;
            Ok(json!({}))
        }
        "CreateIndex" => {
            let bucket = bucket_of(input, region)?;
            let name = string(input, "indexName")?
                .ok_or_else(|| VError::validation("indexName is required."))?;
            if string(input, "dataType")? != Some("float32") {
                return Err(VError::validation("dataType must be float32."));
            }
            let dimension = integer(input, "dimension", 1..=4096)?
                .ok_or_else(|| VError::validation("dimension is required."))?;
            let metric = string(input, "distanceMetric")?
                .and_then(DistanceMetric::parse)
                .ok_or_else(|| VError::validation("distanceMetric must be euclidean or cosine."))?;
            let non_filterable = match input
                .get("metadataConfiguration")
                .and_then(|config| config.get("nonFilterableMetadataKeys"))
            {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) if items.is_empty() => {
                    return Err(VError::validation(
                        "nonFilterableMetadataKeys must name at least one key.",
                    ));
                }
                Some(Value::Array(items)) => items
                    .iter()
                    .map(|item| {
                        item.as_str().map(str::to_string).ok_or_else(|| {
                            VError::validation("nonFilterableMetadataKeys must be strings.")
                        })
                    })
                    .collect::<Result<_, _>>()?,
                Some(_) => {
                    return Err(VError::validation(
                        "nonFilterableMetadataKeys must be a list.",
                    ));
                }
            };
            let new = NewIndex {
                name: name.to_string(),
                dimension: dimension as u16,
                metric,
                non_filterable,
            };
            let index = engine
                .vector(|file| file.transaction(|tx| tx.create_index(&bucket, &new, now_ms())))?
                .map_err(vector_error)?;
            Ok(json!({ "indexArn": index_arn(region, &index.bucket, &index.name) }))
        }
        "GetIndex" => {
            let index = index_of(engine, input, region)?;
            let mut out = json!({
                "vectorBucketName": index.bucket,
                "indexName": index.name,
                "indexArn": index_arn(region, &index.bucket, &index.name),
                "creationTime": seconds(index.created_ms),
                "dataType": "float32",
                "dimension": index.dimension,
                "distanceMetric": index.metric.as_str(),
                "indexMode": "ENHANCED",
            });
            if !index.non_filterable.is_empty() {
                out["metadataConfiguration"] =
                    json!({ "nonFilterableMetadataKeys": index.non_filterable });
            }
            Ok(json!({ "index": out }))
        }
        "ListIndexes" => {
            let bucket = bucket_of(input, region)?;
            if engine.vector(|file| file.bucket(&bucket))?.is_none() {
                return Err(VError::not_found(format!(
                    "The vector bucket {bucket} does not exist."
                )));
            }
            let limit = integer(input, "maxResults", 1..=500)?.unwrap_or(500) as usize;
            let prefix = prefix(input)?;
            let mut after = untoken(input)?;
            let mut out = Vec::new();
            let mut more = false;
            'pages: loop {
                let page =
                    engine.vector(|file| file.list_indexes(&bucket, after.as_deref(), 500))?;
                let last_page = page.len() < 500;
                for index in page {
                    after = Some(index.name.clone());
                    if !index.name.starts_with(&prefix) {
                        if index.name.as_str() > prefix.as_str() {
                            break 'pages;
                        }
                        continue;
                    }
                    if out.len() == limit {
                        more = true;
                        break 'pages;
                    }
                    out.push(json!({
                        "vectorBucketName": index.bucket,
                        "indexName": index.name,
                        "indexArn": index_arn(region, &index.bucket, &index.name),
                        "creationTime": seconds(index.created_ms),
                    }));
                }
                if last_page {
                    break;
                }
            }
            let mut response = json!({ "indexes": out });
            if more && let Some(last) = response["indexes"].as_array().and_then(|l| l.last()) {
                response["nextToken"] = json!(token(last["indexName"].as_str().unwrap_or("")));
            }
            Ok(response)
        }
        "DeleteIndex" => {
            let index = index_of(engine, input, region)?;
            engine
                .vector(|file| file.transaction(|tx| tx.delete_index(&index.bucket, &index.name)))?
                .map_err(vector_error)?;
            Ok(json!({}))
        }
        "PutVectors" => {
            let index = index_of(engine, input, region)?;
            let items = input
                .get("vectors")
                .and_then(Value::as_array)
                .filter(|items| (1..=500).contains(&items.len()))
                .ok_or_else(|| VError::validation("vectors must list 1 to 500 vectors."))?;
            let vectors = items
                .iter()
                .map(|item| {
                    Ok(VectorInput {
                        key: item
                            .get("key")
                            .and_then(Value::as_str)
                            .ok_or_else(|| VError::validation("Each vector needs a key."))?
                            .to_string(),
                        data: vector_data(item.get("data"), "data")?,
                        metadata: item.get("metadata").cloned(),
                    })
                })
                .collect::<Result<Vec<_>, VError>>()?;
            // As many transactions as the batch takes.
            let mut from = 0;
            while from < vectors.len() {
                from = engine
                    .vector(|file| file.transaction(|tx| tx.put_vectors(&index, &vectors, from)))?
                    .map_err(vector_error)?;
            }
            Ok(json!({}))
        }
        "GetVectors" => {
            let index = index_of(engine, input, region)?;
            let keys = keys(input, 100)?;
            let (data, metadata) = (
                boolean(input, "returnData")?,
                boolean(input, "returnMetadata")?,
            );
            let entries = engine.vector(|file| file.get_vectors(&index, &keys, data, metadata))?;
            Ok(json!({ "vectors": entries.into_iter().map(entry_json).collect::<Vec<_>>() }))
        }
        "ListVectors" => {
            let index = index_of(engine, input, region)?;
            let limit = integer(input, "maxResults", 1..=1000)?.unwrap_or(500) as usize;
            let (segments, segment) = match (
                integer(input, "segmentCount", 1..=16)?,
                integer(input, "segmentIndex", 0..=15)?,
            ) {
                (None, None) => (1, 0),
                (Some(count), Some(index)) if index < count => (count, index),
                (Some(_), Some(_)) => {
                    return Err(VError::validation(
                        "segmentIndex must be less than segmentCount.",
                    ));
                }
                _ => {
                    return Err(VError::validation(
                        "segmentCount and segmentIndex go together.",
                    ));
                }
            };
            let (data, metadata) = (
                boolean(input, "returnData")?,
                boolean(input, "returnMetadata")?,
            );
            let mut after = untoken(input)?;
            // Keys first; the values and metadata only of the keys listed.
            let mut keys = Vec::new();
            let mut more = false;
            'pages: loop {
                let page = engine.vector(|file| {
                    file.list_vectors(&index, after.as_deref(), 1000, false, false)
                })?;
                let last_page = page.len() < 1000;
                for entry in page {
                    after = Some(entry.key.clone());
                    if segment_of(&entry.key, segments) != segment {
                        continue;
                    }
                    if keys.len() == limit {
                        more = true;
                        break 'pages;
                    }
                    keys.push(entry.key);
                }
                if last_page {
                    break;
                }
            }
            let next = more.then(|| keys.last().map(|key| token(key))).flatten();
            let out = if data || metadata {
                engine.vector(|file| file.get_vectors(&index, &keys, data, metadata))?
            } else {
                keys.into_iter()
                    .map(|key| truthdb_vector::VectorEntry {
                        key,
                        data: None,
                        metadata: None,
                    })
                    .collect()
            };
            let mut response =
                json!({ "vectors": out.into_iter().map(entry_json).collect::<Vec<_>>() });
            if let Some(next) = next {
                response["nextToken"] = json!(next);
            }
            Ok(response)
        }
        "DeleteVectors" => {
            let index = index_of(engine, input, region)?;
            let keys = keys(input, 500)?;
            let mut from = 0;
            while from < keys.len() {
                from = engine
                    .vector(|file| file.transaction(|tx| tx.delete_vectors(&index, &keys, from)))?
                    .map_err(vector_error)?;
            }
            Ok(json!({}))
        }
        "QueryVectors" => {
            let index = index_of(engine, input, region)?;
            let top_k = integer(input, "topK", 1..=10_000)?
                .ok_or_else(|| VError::validation("topK is required."))?
                as usize;
            let vector = vector_data(input.get("queryVector"), "queryVector")?;
            if let Some(mode) = string(input, "queryMode")?
                && mode != "CLASSIC"
                && mode != "ENHANCED"
            {
                return Err(VError::validation("queryMode must be CLASSIC or ENHANCED."));
            }
            let filter = input.get("filter").filter(|filter| !filter.is_null());
            let (with_distance, with_metadata) = (
                boolean(input, "returnDistance")?,
                boolean(input, "returnMetadata")?,
            );
            // Every result is in the one response: there are no later pages.
            if string(input, "nextToken")?.is_some() {
                return Err(VError::validation("nextToken is not valid."));
            }
            let mut query =
                Query::new(index.clone(), vector, top_k, filter).map_err(|err| match err {
                    QueryError::Invalid(message) => VError::validation(message),
                    QueryError::Storage(err) => VError::internal(err.to_string()),
                })?;
            loop {
                let done =
                    engine
                        .vector(|file| Ok(query.step(file, 512)))?
                        .map_err(|err| match err {
                            QueryError::Invalid(message) => VError::validation(message),
                            QueryError::Storage(err) => VError::internal(err.to_string()),
                        })?;
                if done {
                    break;
                }
            }
            let hits = engine.vector(|file| query.finish(file, with_metadata))?;
            let page: Vec<Value> = hits
                .iter()
                .map(|hit| {
                    let mut out = json!({ "key": hit.key });
                    if with_distance {
                        out["distance"] = json!(hit.distance);
                    }
                    if let Some(metadata) = &hit.metadata {
                        out["metadata"] = Value::Object(metadata.clone());
                    }
                    out
                })
                .collect();
            Ok(json!({
                "distanceMetric": index.metric.as_str(),
                "vectors": page,
            }))
        }
        other => Err(not_implemented(other)),
    }
}

/// The ListVectors segment a key falls in, of `segments` (FNV-1a).
fn segment_of(key: &str, segments: u64) -> u64 {
    let hash = key.bytes().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    hash % segments
}
