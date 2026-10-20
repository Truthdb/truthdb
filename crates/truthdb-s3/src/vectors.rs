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
//! and the bytes of all bodies not yet verified are bounded (past that, a
//! request is told to retry). A body is parsed only once it has at most
//! [`MAX_JSON_VALUES`] values, and at most 16 verified requests are parsed
//! and served at once, which bounds the memory parsing takes.
//!
//! A QueryVectors response holds all its results (no `nextToken`): one scan
//! per query, and no pages drawn from a moving index. A PutVectors or
//! DeleteVectors batch is written over as many transactions as it takes, so
//! a failure part way leaves the vectors before it written.

use std::sync::atomic::{AtomicUsize, Ordering};

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
use crate::server::{
    Body, Connection, S3State, check_signed_headers, full, signed_request, verify_signature,
};

/// The largest request body.
const MAX_BODY: usize = 20 << 20;
/// How long a request body may take to arrive.
const BODY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Bytes of request bodies held before their signatures are checked, and
/// their bound: a client that knows only an access key id must send this
/// much within [`BODY_TIMEOUT`] to turn others away.
static UNVERIFIED: AtomicUsize = AtomicUsize::new(0);
const UNVERIFIED_MAX: usize = 16 * MAX_BODY;
/// The most values a request body may hold: more than the largest
/// PutVectors needs (500 vectors of 4096 numbers). Parsed, a value takes
/// about 64 bytes (an object member about 110).
const MAX_JSON_VALUES: usize = 4 << 20;
/// Verified requests parsed and served at once (the rest wait their turn):
/// each holds its body and its parsed form, a few hundred MiB at most.
static SERVING: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(16);

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
    // unknown key before that, and bound the unverified bytes held and for
    // how long.
    let access_key = auth.credential.access_key.clone();
    if state
        .engine
        .run_object(move |file: &mut truthdb_object::ObjFile<'_>| file.access_key(&access_key))
        .await?
        .is_none()
    {
        return Err(S3Error::invalid_access_key().into());
    }
    let mut held = Unverified(0);
    let (parts, body) = request.into_parts();
    let body = tokio::time::timeout(BODY_TIMEOUT, read_body(body, &mut held))
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
    let (_, principal) = verify_signature(
        state,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &auth,
        &amz_date,
        &hash,
    )
    .await?;
    drop(held);
    let _serving = SERVING.acquire().await.expect("never closed");
    let operation = parts.uri.path().trim_start_matches('/').to_string();
    if parts.method != Method::POST {
        return Err(not_implemented(&operation));
    }
    let input: Map<String, Value> = if body.is_empty() {
        Map::new()
    } else if json_values(&body) > MAX_JSON_VALUES {
        return Err(VError::validation(format!(
            "The request body holds more than {MAX_JSON_VALUES} JSON values."
        )));
    } else {
        match serde_json::from_slice(&body) {
            Ok(Value::Object(map)) => map,
            _ => return Err(VError::validation("The request body is not a JSON object.")),
        }
    };
    let authz = crate::authz::Authz::load(
        state,
        &principal,
        &parts.headers,
        "",
        parts.extensions.get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    let region = state.region.clone();
    state
        .engine
        .run_engine(move |engine| {
            Ok(dispatch(
                engine, &region, &principal, &authz, &operation, &input,
            ))
        })
        .await?
}

fn not_implemented(operation: &str) -> VError {
    VError::new(
        501,
        "NotImplemented",
        format!("{operation} is not implemented."),
    )
}

/// Body bytes held before the signature is checked, counted in
/// [`UNVERIFIED`] until dropped.
struct Unverified(usize);

impl Unverified {
    fn take(&mut self, bytes: usize) -> bool {
        if UNVERIFIED.fetch_add(bytes, Ordering::Relaxed) + bytes > UNVERIFIED_MAX {
            UNVERIFIED.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        self.0 += bytes;
        true
    }
}

impl Drop for Unverified {
    fn drop(&mut self) {
        UNVERIFIED.fetch_sub(self.0, Ordering::Relaxed);
    }
}

/// Reads the body, counting its bytes in `held` as they arrive.
async fn read_body(mut body: Incoming, held: &mut Unverified) -> Result<Bytes, VError> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| VError::validation("The request body could not be read."))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if out.len() + data.len() > MAX_BODY {
            return Err(VError::new(
                413,
                "ValidationException",
                format!("The request body is larger than {MAX_BODY} bytes."),
            ));
        }
        if !held.take(data.len()) {
            return Err(VError::new(
                503,
                "ServiceUnavailableException",
                "The service is busy; retry the request.",
            ));
        }
        out.extend_from_slice(&data);
    }
    Ok(Bytes::from(out))
}

/// At least the number of values in a JSON text: one, plus one per comma
/// and per opening bracket outside strings.
fn json_values(text: &[u8]) -> usize {
    let (mut count, mut in_string, mut escaped) = (1, false, false);
    for &byte in text {
        if in_string {
            match byte {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
        } else {
            match byte {
                b'"' => in_string = true,
                b',' | b'[' | b'{' => count += 1,
                _ => {}
            }
        }
    }
    count
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

/// A bucket's ARN: a caller reaches only its own account's buckets, so the
/// account in it is the caller's.
fn bucket_arn(region: &str, account: &str, bucket: &str) -> String {
    format!("arn:aws:s3vectors:{region}:{account}:bucket/{bucket}")
}

fn index_arn(region: &str, account: &str, bucket: &str, index: &str) -> String {
    format!("{}/index/{index}", bucket_arn(region, account, bucket))
}

/// An ARN's bucket and (when it names one) index: an ARN of this server's
/// region and the caller's account.
fn parse_arn(arn: &str, region: &str, caller: &str) -> Option<(String, Option<String>)> {
    let rest = arn.strip_prefix("arn:")?;
    let mut parts = rest.splitn(5, ':');
    let (partition, service, arn_region, account, resource) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    if partition != "aws" || service != "s3vectors" || arn_region != region || account != caller {
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
fn bucket_of(input: &Map<String, Value>, region: &str, account: &str) -> Result<String, VError> {
    if let Some(arn) = string(input, "vectorBucketArn")? {
        if string(input, "vectorBucketName")?.is_some() {
            return Err(VError::validation(
                "Give vectorBucketName or vectorBucketArn, not both.",
            ));
        }
        return match parse_arn(arn, region, account) {
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

/// The vector bucket a request names, which must exist and be the caller's
/// account's: until bucket policies, a bucket is its owner's alone.
fn owned_bucket(
    caller: &Caller<'_>,
    input: &Map<String, Value>,
    region: &str,
    account: &str,
) -> Result<truthdb_vector::BucketInfo, VError> {
    let bucket = bucket_of(input, region, account)?;
    let info = caller
        .vector(|file| file.bucket(&bucket))?
        .ok_or_else(|| VError::not_found(format!("The vector bucket {bucket} does not exist.")))?;
    check_owner(&info, account)?;
    Ok(info)
}

/// Refuses a bucket another account owns (a bucket from before owners is the
/// default account's).
fn check_owner(info: &truthdb_vector::BucketInfo, account: &str) -> Result<(), VError> {
    if info
        .owner
        .as_deref()
        .unwrap_or(truthdb_object::DEFAULT_ACCOUNT)
        != account
    {
        return Err(VError::new(403, "AccessDeniedException", "Access denied."));
    }
    Ok(())
}

/// The vector index a request names (by bucket and name, or ARN, not
/// both), as it is.
fn index_of(
    caller: &Caller<'_>,
    input: &Map<String, Value>,
    region: &str,
    account: &str,
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
            match parse_arn(arn, region, account) {
                Some((bucket, Some(index))) => (bucket, index),
                _ => {
                    return Err(VError::validation(
                        "indexArn is not a vector index ARN of this server.",
                    ));
                }
            }
        }
        None => (
            bucket_of(input, region, account)?,
            string(input, "indexName")?
                .ok_or_else(|| VError::validation("indexName or indexArn is required."))?
                .to_string(),
        ),
    };
    let not_found =
        || VError::not_found(format!("The vector index {bucket}/{name} does not exist."));
    let info = caller
        .vector(|file| file.bucket(&bucket))?
        .ok_or_else(not_found)?;
    check_owner(&info, account)?;
    caller
        .vector(|file| file.index(&bucket, &name))?
        .ok_or_else(not_found)
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

/// The caller of an S3 Vectors request. Each of its vector-engine calls
/// checks, in the same storage-lock hold as its work, that the request's
/// access key still acts for its account and that the bucket the request
/// names is missing or the account's: a key dropped or a bucket deleted and
/// created again by another account meanwhile does not let it through.
struct Caller<'e> {
    engine: &'e Engine,
    access_key: String,
    account: String,
    /// The bucket the request names (none for creating or listing buckets).
    bucket: Option<String>,
}

impl Caller<'_> {
    fn vector<T>(
        &self,
        op: impl FnOnce(
            &mut truthdb_vector::VecFile<'_>,
        ) -> Result<T, truthdb_core::storage::StorageError>,
    ) -> Result<T, VError> {
        let (access_key, account, bucket) =
            (&self.access_key, &self.account, self.bucket.as_deref());
        self.engine
            .vector_gated(
                |objects| {
                    Ok(matches!(
                        objects.access_key(access_key)?,
                        Some((key, _)) if key.active && &key.account == account
                    ))
                },
                |vectors| {
                    if let Some(bucket) = bucket
                        && let Some(info) = vectors.bucket(bucket)?
                        && check_owner(&info, account).is_err()
                    {
                        return Ok(None);
                    }
                    op(vectors).map(Some)
                },
            )?
            .flatten()
            .ok_or_else(|| VError::new(403, "AccessDeniedException", "Access denied."))
    }
}

/// The bucket a request names: its index ARN's, or the one [`bucket_of`]
/// finds.
fn named_bucket(input: &Map<String, Value>, region: &str, account: &str) -> Result<String, VError> {
    match string(input, "indexArn")? {
        Some(arn) => parse_arn(arn, region, account)
            .map(|(bucket, _)| bucket)
            .ok_or_else(|| {
                VError::validation("indexArn is not a vector index ARN of this server.")
            }),
        None => bucket_of(input, region, account),
    }
}

fn dispatch(
    engine: &Engine,
    region: &str,
    principal: &crate::server::Principal,
    authz: &crate::authz::Authz,
    operation: &str,
    input: &Map<String, Value>,
) -> Result<Value, VError> {
    let account = principal.account.as_str();
    // The bucket each operation acts on, found as the operation finds it.
    let bucket = match operation {
        "GetVectorBucket" | "DeleteVectorBucket" | "CreateIndex" | "ListIndexes" => {
            Some(bucket_of(input, region, account)?)
        }
        "GetIndex" | "DeleteIndex" | "PutVectors" | "GetVectors" | "ListVectors"
        | "DeleteVectors" | "QueryVectors" => Some(named_bucket(input, region, account)?),
        _ => None,
    };
    // Authorized as `s3vectors:<Operation>` on the bucket's or the index's
    // ARN (the caller's own account's: vector buckets have no policies).
    let arn = match operation {
        "CreateVectorBucket" => {
            string(input, "vectorBucketName")?.map(|name| bucket_arn(region, account, name))
        }
        "ListVectorBuckets" => Some(format!("arn:aws:s3vectors:{region}:{account}:bucket/*")),
        "GetVectorBucket" | "DeleteVectorBucket" | "CreateIndex" | "ListIndexes" => bucket
            .as_deref()
            .map(|bucket| bucket_arn(region, account, bucket)),
        _ => match (bucket.as_deref(), string(input, "indexArn")?) {
            (Some(_), Some(arn)) => Some(arn.to_string()),
            (Some(bucket), None) => Some(index_arn(
                region,
                account,
                bucket,
                string(input, "indexName")?.unwrap_or(""),
            )),
            (None, _) => None,
        },
    };
    if let Some(arn) = arn {
        authz.check_arn(&format!("s3vectors:{operation}"), &arn, account)?;
    }
    let caller = &Caller {
        engine,
        access_key: principal.access_key.clone(),
        account: account.to_string(),
        bucket,
    };
    match operation {
        "CreateVectorBucket" => {
            let bucket = string(input, "vectorBucketName")?
                .ok_or_else(|| VError::validation("vectorBucketName is required."))?;
            if let Some(Value::Object(encryption)) = input.get("encryptionConfiguration")
                && string(encryption, "sseType")?.is_some_and(|sse| sse != "AES256")
            {
                return Err(not_implemented("SSE-KMS"));
            }
            caller
                .vector(|file| file.transaction(|tx| tx.create_bucket(bucket, account, now_ms())))?
                .map_err(vector_error)?;
            Ok(json!({ "vectorBucketArn": bucket_arn(region, account, bucket) }))
        }
        "GetVectorBucket" => {
            let info = owned_bucket(caller, input, region, account)?;
            Ok(json!({ "vectorBucket": {
                "vectorBucketName": info.name,
                "vectorBucketArn": bucket_arn(region, account, &info.name),
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
                let page = caller.vector(|file| file.list_buckets(after.as_deref(), 500))?;
                let last_page = page.len() < 500;
                for bucket in page {
                    after = Some(bucket.name.clone());
                    if bucket
                        .owner
                        .as_deref()
                        .unwrap_or(truthdb_object::DEFAULT_ACCOUNT)
                        != account
                    {
                        continue;
                    }
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
                        "vectorBucketArn": bucket_arn(region, account, &bucket.name),
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
            let bucket = owned_bucket(caller, input, region, account)?.name;
            caller
                .vector(|file| file.transaction(|tx| tx.delete_bucket(&bucket)))?
                .map_err(vector_error)?;
            Ok(json!({}))
        }
        "CreateIndex" => {
            let bucket = owned_bucket(caller, input, region, account)?.name;
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
            let non_filterable = match input.get("metadataConfiguration") {
                None | Some(Value::Null) => None,
                Some(Value::Object(config)) => config.get("nonFilterableMetadataKeys"),
                Some(_) => {
                    return Err(VError::validation(
                        "metadataConfiguration must be an object.",
                    ));
                }
            };
            let non_filterable = match non_filterable {
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
            let index = caller
                .vector(|file| file.transaction(|tx| tx.create_index(&bucket, &new, now_ms())))?
                .map_err(vector_error)?;
            Ok(json!({ "indexArn": index_arn(region, account, &index.bucket, &index.name) }))
        }
        "GetIndex" => {
            let index = index_of(caller, input, region, account)?;
            let mut out = json!({
                "vectorBucketName": index.bucket,
                "indexName": index.name,
                "indexArn": index_arn(region, account, &index.bucket, &index.name),
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
            let bucket = owned_bucket(caller, input, region, account)?.name;
            let limit = integer(input, "maxResults", 1..=500)?.unwrap_or(500) as usize;
            let prefix = prefix(input)?;
            let mut after = untoken(input)?;
            let mut out = Vec::new();
            let mut more = false;
            'pages: loop {
                let page =
                    caller.vector(|file| file.list_indexes(&bucket, after.as_deref(), 500))?;
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
                        "indexArn": index_arn(region, account, &index.bucket, &index.name),
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
            let index = index_of(caller, input, region, account)?;
            caller
                .vector(|file| file.transaction(|tx| tx.delete_index(&index.bucket, &index.name)))?
                .map_err(vector_error)?;
            Ok(json!({}))
        }
        "PutVectors" => {
            let index = index_of(caller, input, region, account)?;
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
                from = caller
                    .vector(|file| file.transaction(|tx| tx.put_vectors(&index, &vectors, from)))?
                    .map_err(vector_error)?;
            }
            Ok(json!({}))
        }
        "GetVectors" => {
            let index = index_of(caller, input, region, account)?;
            let keys = keys(input, 100)?;
            let (data, metadata) = (
                boolean(input, "returnData")?,
                boolean(input, "returnMetadata")?,
            );
            let entries = caller.vector(|file| file.get_vectors(&index, &keys, data, metadata))?;
            Ok(json!({ "vectors": entries.into_iter().map(entry_json).collect::<Vec<_>>() }))
        }
        "ListVectors" => {
            let index = index_of(caller, input, region, account)?;
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
                let page = caller.vector(|file| {
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
                caller.vector(|file| file.get_vectors(&index, &keys, data, metadata))?
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
            let index = index_of(caller, input, region, account)?;
            let keys = keys(input, 500)?;
            let mut from = 0;
            while from < keys.len() {
                from = caller
                    .vector(|file| file.transaction(|tx| tx.delete_vectors(&index, &keys, from)))?
                    .map_err(vector_error)?;
            }
            Ok(json!({}))
        }
        "QueryVectors" => {
            let index = index_of(caller, input, region, account)?;
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
                    caller
                        .vector(|file| Ok(query.step(file, 512)))?
                        .map_err(|err| match err {
                            QueryError::Invalid(message) => VError::validation(message),
                            QueryError::Storage(err) => VError::internal(err.to_string()),
                        })?;
                if done {
                    break;
                }
            }
            let hits = caller.vector(|file| query.finish(file, with_metadata))?;
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
