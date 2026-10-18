//! The S3 operations: a request is routed by method, path (`/`, `/bucket`,
//! `/bucket/key`) and sub-resource query, then run against the object engine.

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_object::{BucketError, BucketInfo, ObjFile, Versioning};

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

/// Sub-resources of buckets and objects that are not implemented: refused,
/// rather than taken for the plain operation on the same path.
const UNIMPLEMENTED: &[&str] = &[
    "accelerate",
    "acl",
    "analytics",
    "attributes",
    "cors",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "legal-hold",
    "lifecycle",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "ownershipControls",
    "policy",
    "policyStatus",
    "publicAccessBlock",
    "replication",
    "requestPayment",
    "restore",
    "retention",
    "select",
    "torrent",
    "website",
];

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
        _ if UNIMPLEMENTED.iter().any(|name| has_param(&request, name)) => {
            Err(S3Error::not_implemented(format!(
                "{} {}?{} is not implemented",
                request.method(),
                request.uri().path(),
                request.uri().query().unwrap_or("")
            )))
        }
        (Target::Bucket(_), _) if has_param(&request, "tagging") => Err(S3Error::not_implemented(
            "bucket tagging is not implemented",
        )),
        (Target::Service, Method::GET) => list_buckets(state).await,
        (Target::Bucket(bucket), Method::PUT) if has_param(&request, "versioning") => {
            put_versioning(state, request, auth, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "versioning") => {
            get_versioning(state, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "versions") => {
            let query = request.uri().query().unwrap_or("").to_string();
            crate::listing::list_versions(state, bucket, &query).await
        }
        (Target::Bucket(bucket), Method::PUT) => create_bucket(state, request, auth, bucket).await,
        (Target::Bucket(bucket), Method::DELETE) => delete_bucket(state, bucket).await,
        (Target::Bucket(bucket), Method::HEAD) => head_bucket(state, bucket).await,
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "location") => {
            bucket_location(state, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "uploads") => {
            crate::multipart::list_uploads(state, request, bucket).await
        }
        (Target::Bucket(bucket), Method::POST) if has_param(&request, "delete") => {
            crate::delete::delete_objects(state, request, auth, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) => {
            let query = request.uri().query().unwrap_or("").to_string();
            crate::listing::list_objects(state, bucket, &query).await
        }
        (Target::Object(bucket, key), Method::POST) if has_param(&request, "uploads") => {
            crate::multipart::create_upload(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::POST) if has_param(&request, "uploadId") => {
            crate::multipart::complete_upload(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT) if has_param(&request, "tagging") => {
            crate::tagging::put_tagging(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::GET) if has_param(&request, "tagging") => {
            crate::tagging::get_tagging(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::DELETE) if has_param(&request, "tagging") => {
            crate::tagging::delete_tagging(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT)
            if has_param(&request, "uploadId")
                && request.headers().contains_key("x-amz-copy-source") =>
        {
            crate::copy::upload_part_copy(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT)
            if request.headers().contains_key("x-amz-copy-source") =>
        {
            crate::copy::copy_object(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT) if has_param(&request, "uploadId") => {
            crate::multipart::upload_part(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::GET) if has_param(&request, "uploadId") => {
            crate::multipart::list_parts(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::DELETE) if has_param(&request, "uploadId") => {
            crate::multipart::abort_upload(state, request, bucket, key).await
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
        (Target::Object(bucket, key), Method::DELETE) => {
            crate::delete::delete_object(state, request, bucket, key).await
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
    if let Some(constraint) = location_constraint(&body)?
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
    match object(state, move |file| {
        file.transaction(|tx| tx.create_bucket(&name, now))
    })
    .await?
    {
        Ok(()) => {}
        // us-east-1 answers success when the owner creates the bucket again.
        Err(BucketError::AlreadyOwnedByYou) if state.region == "us-east-1" => {}
        Err(err) => return Err(bucket_error(err, &bucket)),
    }
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

async fn put_versioning(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    let body = read_body(request, auth, 64 * 1024).await?;
    let versioning = parse_versioning(&body)?;
    let name = bucket.clone();
    object(state, move |file| {
        file.transaction(|tx| tx.set_versioning(&name, versioning))
    })
    .await?
    .map_err(|err| bucket_error(err, &bucket))?;
    Ok(empty(StatusCode::OK))
}

async fn get_versioning(state: &S3State, bucket: String) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    let Some(info) = object(state, move |file| file.bucket(&name)).await? else {
        return Err(bucket_error(BucketError::NoSuchBucket, &bucket));
    };
    let status = match info.versioning {
        Versioning::Unversioned => "",
        Versioning::Enabled => "<Status>Enabled</Status>",
        Versioning::Suspended => "<Status>Suspended</Status>",
    };
    Ok(xml(
        StatusCode::OK,
        format!("<VersioningConfiguration xmlns=\"{XMLNS}\">{status}</VersioningConfiguration>"),
    ))
}

/// The state a PutBucketVersioning body sets: one `<VersioningConfiguration>`
/// with a `<Status>` of `Enabled` or `Suspended` (and MFA delete, which is
/// not supported, left disabled).
fn parse_versioning(body: &[u8]) -> Result<Versioning, S3Error> {
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        )
    };
    let mut reader = Reader::from_reader(body);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let mut seen_root = false;
    let (mut status, mut mfa_delete): (Option<String>, Option<String>) = (None, None);
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|_| malformed())?;
        let (start, end) = match &event {
            Event::Start(element) => (Some(element.local_name().as_ref().to_vec()), None),
            Event::Empty(element) => {
                let name = element.local_name().as_ref().to_vec();
                (Some(name.clone()), Some(name))
            }
            Event::End(element) => (None, Some(element.local_name().as_ref().to_vec())),
            _ => (None, None),
        };
        if let Some(name) = start {
            let allowed = match (path.last().map(Vec::as_slice), name.as_slice()) {
                (None, b"VersioningConfiguration") => !std::mem::replace(&mut seen_root, true),
                (Some(b"VersioningConfiguration"), b"Status") => {
                    status.replace(String::new()).is_none()
                }
                (Some(b"VersioningConfiguration"), b"MfaDelete") => {
                    mfa_delete.replace(String::new()).is_none()
                }
                _ => false,
            };
            if !allowed {
                return Err(malformed());
            }
            path.push(name);
        }
        if end.is_some() {
            path.pop();
        }
        if let Event::Text(text) = &event {
            let text = text.unescape().map_err(|_| malformed())?;
            match path.last().map(Vec::as_slice) {
                Some(b"Status") => status.get_or_insert_default().push_str(text.trim()),
                Some(b"MfaDelete") => mfa_delete.get_or_insert_default().push_str(text.trim()),
                _ if text.trim().is_empty() => {}
                _ => return Err(malformed()),
            }
        }
        if matches!(event, Event::Eof) {
            if !path.is_empty() || !seen_root {
                return Err(malformed());
            }
            break;
        }
        buf.clear();
    }
    match mfa_delete.as_deref() {
        None | Some("Disabled") => {}
        Some("Enabled") => {
            return Err(S3Error::not_implemented("MFA delete is not supported"));
        }
        Some(_) => return Err(malformed()),
    }
    match status.as_deref() {
        Some("Enabled") => Ok(Versioning::Enabled),
        Some("Suspended") => Ok(Versioning::Suspended),
        _ => Err(malformed()),
    }
}

/// The `<LocationConstraint>` of a CreateBucket body, which must be
/// well-formed XML when present.
fn location_constraint(body: &[u8]) -> Result<Option<String>, S3Error> {
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        )
    };
    let mut reader = Reader::from_reader(body);
    let mut in_constraint = false;
    let mut constraint = None;
    let mut depth = 0usize;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(|_| malformed())? {
            Event::Start(element) => {
                depth += 1;
                in_constraint = element.local_name().as_ref() == b"LocationConstraint";
                if in_constraint && constraint.is_none() {
                    constraint = Some(String::new());
                }
            }
            Event::End(_) => {
                depth -= 1;
                in_constraint = false;
            }
            Event::Text(text) if in_constraint => {
                let text = text.unescape().map_err(|_| malformed())?;
                if let Some(constraint) = &mut constraint {
                    constraint.push_str(text.trim());
                }
            }
            Event::Eof if depth == 0 => break,
            Event::Eof => return Err(malformed()),
            _ => {}
        }
        buf.clear();
    }
    Ok(constraint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_bucket_bodies_parse_or_are_malformed() {
        assert_eq!(location_constraint(b"").unwrap(), None);
        assert_eq!(
            location_constraint(
                b"<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint> eu-west-1 </LocationConstraint></CreateBucketConfiguration>"
            )
            .unwrap()
            .as_deref(),
            Some("eu-west-1")
        );
        for bad in [
            &b"<CreateBucketConfiguration>"[..],
            b"</LocationConstraint>",
            b"<a><b></a></b>",
        ] {
            let err = location_constraint(bad).unwrap_err();
            assert_eq!(
                err.code,
                "MalformedXML",
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}
