//! The S3 operations: a request is routed by method, path (`/`, `/bucket`,
//! `/bucket/key`) and sub-resource query, then run against the object engine.

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::{BucketError, BucketInfo, ObjFile};

use crate::error::{S3Error, escape};
use crate::server::{Authenticated, Body, S3State, full, read_body};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// What a request path names.
enum Target {
    Service,
    Bucket(String),
    Object(String, String),
}

fn target(raw_path: &str) -> Result<Target, S3Error> {
    let path = crate::sigv4::decode_path(raw_path)
        .ok_or_else(|| S3Error::invalid_request("the path is not UTF-8"))?;
    let path = path.strip_prefix('/').unwrap_or(&path);
    if path.is_empty() {
        return Ok(Target::Service);
    }
    Ok(match path.split_once('/') {
        None | Some((_, "")) => Target::Bucket(path.trim_end_matches('/').to_string()),
        Some((bucket, key)) => Target::Object(bucket.to_string(), key.to_string()),
    })
}

/// The query's parameter names (sub-resources such as `location`).
fn has_param(request: &Request<Incoming>, name: &str) -> bool {
    request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .any(|pair| pair.split_once('=').map_or(pair, |(key, _)| key) == name)
}

fn bucket_error(err: BucketError, bucket: &str) -> S3Error {
    match err {
        BucketError::NoSuchBucket => S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {bucket} does not exist"),
        ),
        BucketError::AlreadyOwnedByYou => S3Error::new(
            409,
            "BucketAlreadyOwnedByYou",
            format!("The bucket {bucket} already exists and is yours"),
        ),
        BucketError::NotEmpty => S3Error::new(
            409,
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty",
        ),
        BucketError::InvalidName(why) => {
            S3Error::new(400, "InvalidBucketName", format!("The bucket name {why}"))
        }
    }
}

async fn object<T, F>(state: &S3State, op: F) -> Result<T, S3Error>
where
    T: Send + 'static,
    F: FnOnce(&mut ObjFile<'_>) -> Result<T, truthdb_core::storage::StorageError> + Send + 'static,
{
    state
        .engine
        .run_object(op)
        .await
        .map_err(|err| S3Error::internal(err.to_string()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn xml(status: StatusCode, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(Bytes::new()))
        .expect("a valid response")
}

/// Routes an authenticated request to its operation.
pub async fn dispatch(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
) -> Result<Response<Body>, S3Error> {
    let method = request.method().clone();
    match (target(request.uri().path())?, method) {
        (Target::Service, Method::GET) => list_buckets(state).await,
        (Target::Bucket(bucket), Method::PUT) => create_bucket(state, request, auth, bucket).await,
        (Target::Bucket(bucket), Method::DELETE) => delete_bucket(state, bucket).await,
        (Target::Bucket(bucket), Method::HEAD) => head_bucket(state, bucket).await,
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "location") => {
            bucket_location(state, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) => {
            let query = request.uri().query().unwrap_or("").to_string();
            crate::listing::list_objects(state, bucket, &query).await
        }
        (Target::Object(bucket, key), Method::PUT) => {
            crate::objects::put_object(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::GET) => {
            crate::objects::get_object(state, request, bucket, key, false).await
        }
        (Target::Object(bucket, key), Method::HEAD) => {
            crate::objects::get_object(state, request, bucket, key, true).await
        }
        (Target::Object(bucket, key), method) => Err(S3Error::not_implemented(format!(
            "{method} on object {key} in {bucket} is not implemented yet"
        ))),
        (_, method) => Err(S3Error::not_implemented(format!(
            "{method} {} is not implemented yet",
            request.uri().path()
        ))),
    }
}

async fn list_buckets(state: &S3State) -> Result<Response<Body>, S3Error> {
    let buckets = object(state, |file| file.list_buckets()).await?;
    let entries: String = buckets
        .iter()
        .map(
            |BucketInfo {
                 name, created_ms, ..
             }| {
                format!(
                    "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                    escape(name),
                    crate::time::iso8601(*created_ms)
                )
            },
        )
        .collect();
    Ok(xml(
        StatusCode::OK,
        format!(
            "<ListAllMyBucketsResult xmlns=\"{XMLNS}\"><Owner><ID>truthdb</ID><DisplayName>truthdb</DisplayName></Owner><Buckets>{entries}</Buckets></ListAllMyBucketsResult>"
        ),
    ))
}

async fn create_bucket(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    let body = read_body(request, auth, 64 * 1024).await?;
    let body = String::from_utf8_lossy(&body);
    if let Some(constraint) = element_text(&body, "LocationConstraint")
        && !constraint.is_empty()
        && constraint != state.region
    {
        return Err(S3Error::new(
            400,
            "IllegalLocationConstraintException",
            format!(
                "The {constraint} location constraint is incompatible with the region {}",
                state.region
            ),
        ));
    }
    let name = bucket.clone();
    let now = now_ms();
    object(state, move |file| {
        file.transaction(|tx| tx.create_bucket(&name, now))
    })
    .await?
    .map_err(|err| bucket_error(err, &bucket))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("location", format!("/{bucket}"))
        .body(full(Bytes::new()))
        .expect("a valid response"))
}

async fn delete_bucket(state: &S3State, bucket: String) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    object(state, move |file| {
        file.transaction(|tx| tx.delete_bucket(&name))
    })
    .await?
    .map_err(|err| bucket_error(err, &bucket))?;
    Ok(empty(StatusCode::NO_CONTENT))
}

async fn head_bucket(state: &S3State, bucket: String) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    match object(state, move |file| file.bucket(&name)).await? {
        Some(_) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("x-amz-bucket-region", &state.region)
            .body(full(Bytes::new()))
            .expect("a valid response")),
        // HEAD carries no body: just the status.
        None => Ok(empty(StatusCode::NOT_FOUND)),
    }
}

async fn bucket_location(state: &S3State, bucket: String) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    if object(state, move |file| file.bucket(&name))
        .await?
        .is_none()
    {
        return Err(bucket_error(BucketError::NoSuchBucket, &bucket));
    }
    // S3 answers an empty constraint for us-east-1.
    let region = if state.region == "us-east-1" {
        ""
    } else {
        &state.region
    };
    Ok(xml(
        StatusCode::OK,
        format!(
            "<LocationConstraint xmlns=\"{XMLNS}\">{}</LocationConstraint>",
            escape(region)
        ),
    ))
}

/// The text of the first `<name>` element in `xml`, if any.
fn element_text<'x>(xml: &'x str, name: &str) -> Option<&'x str> {
    let open = format!("<{name}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{name}>"))? + start;
    Some(xml[start..end].trim())
}
