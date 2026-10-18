//! Object operations: PutObject streams the request body into the object
//! engine segment by segment; GetObject streams a leased body back, whole or
//! by range, after the conditional headers; HeadObject answers with the
//! headers alone.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::Frame;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::HeaderMap;
use hyper::{Request, Response, StatusCode};
use md5::{Digest, Md5};
use tokio::sync::mpsc;
use truthdb_core::storage::StorageError;
use truthdb_object::{
    ObjFile, ObjectAttrs, ObjectError, ObjectInfo, SEGMENT_BYTES, SealedBody, VersionSel,
    Versioning,
};

use crate::error::S3Error;
use crate::payload::PayloadDecoder;
use crate::server::{Authenticated, Body, S3State, full};

/// Largest single PUT (S3's limit).
pub(crate) const MAX_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Largest total of a PUT's header names and values (S3's limit).
const MAX_HEADER_BYTES: usize = 8 * 1024;
/// Largest total of user metadata names and values (S3's limit).
const MAX_USER_METADATA: usize = 2 * 1024;
/// Bytes a GET reads per engine call.
const READ_CHUNK: usize = 1024 * 1024;
/// How long a dropped object's body outlives it before collection: readers
/// that resolved the old version just before the overwrite may still read it.
pub const DROP_GRACE_MS: u64 = 15 * 60 * 1000;

pub fn object_error(err: ObjectError, bucket: &str, key: &str) -> S3Error {
    match err {
        ObjectError::NoSuchBucket => S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {bucket} does not exist"),
        ),
        ObjectError::NoSuchKey => S3Error::new(
            404,
            "NoSuchKey",
            format!("The specified key {key} does not exist."),
        ),
        ObjectError::NoSuchVersion => S3Error::new(
            404,
            "NoSuchVersion",
            "The specified version does not exist.",
        ),
        ObjectError::DeleteMarker => S3Error::new(
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        ),
        ObjectError::InvalidKey(why) => S3Error::new(400, "InvalidArgument", why),
    }
}

pub async fn object<T, F>(state: &S3State, op: F) -> Result<T, S3Error>
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

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// The system headers, user metadata and tags a PUT stores.
pub fn attrs_from(headers: &HeaderMap) -> Result<ObjectAttrs, S3Error> {
    Ok(ObjectAttrs {
        tags: crate::tagging::tags_from(headers)?,
        ..metadata_from(headers)?
    })
}

/// The system headers and user metadata a PUT stores (no tags).
pub fn metadata_from(headers: &HeaderMap) -> Result<ObjectAttrs, S3Error> {
    // S3's limit on a PUT's headers; it also keeps every stored attribute
    // within the 64 KiB its encoding allows.
    let header_bytes: usize = headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len())
        .sum();
    if header_bytes > MAX_HEADER_BYTES {
        return Err(S3Error::new(
            400,
            "RequestHeaderSectionTooLarge",
            "Your request header section exceeds the maximum allowed size.",
        ));
    }
    let mut user = Vec::new();
    let mut user_bytes = 0;
    for (name, value) in headers {
        if let Some(meta) = name.as_str().strip_prefix("x-amz-meta-") {
            let value = value
                .to_str()
                .map_err(|_| S3Error::invalid_request("user metadata must be ASCII"))?;
            user_bytes += meta.len() + value.len();
            user.push((meta.to_string(), value.to_string()));
        }
    }
    if user_bytes > MAX_USER_METADATA {
        return Err(S3Error::new(
            400,
            "MetadataTooLarge",
            "Your metadata headers exceed the maximum allowed metadata size.",
        ));
    }
    user.sort();
    Ok(ObjectAttrs {
        content_type: header(headers, "content-type"),
        cache_control: header(headers, "cache-control"),
        content_disposition: header(headers, "content-disposition"),
        content_encoding: header(headers, "content-encoding")
            .map(|value| {
                // aws-chunked framing is transport, not the object's encoding.
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.eq_ignore_ascii_case("aws-chunked"))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .filter(|value| !value.is_empty()),
        content_language: header(headers, "content-language"),
        expires: header(headers, "expires"),
        user,
        tags: Vec::new(),
    })
}

/// Receives a request body into a new staging body, verifying its framing,
/// signatures, checksum and Content-MD5 as it streams, and returns it ready
/// to seal (its last segment written). Bodies over 5 GiB are refused. A body
/// left behind by a failure stays staging and garbage collection reclaims it.
pub async fn receive_body(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
) -> Result<SealedBody, S3Error> {
    let headers = request.headers().clone();
    let declared: Option<u64> =
        header(&headers, "x-amz-decoded-content-length").and_then(|value| value.parse().ok());
    let length: Option<u64> = declared
        .or_else(|| header(&headers, "content-length").and_then(|value| value.parse().ok()));
    let too_large = || {
        S3Error::new(
            400,
            "EntityTooLarge",
            "Your proposed upload exceeds the maximum allowed object size.",
        )
    };
    let Some(length) = length else {
        return Err(S3Error::new(
            411,
            "MissingContentLength",
            "You must provide the Content-Length HTTP header.",
        ));
    };
    if length > MAX_PUT_BYTES {
        return Err(too_large());
    }
    // Content-MD5 must be the base64 of 16 bytes: checked before the body.
    let content_md5: Option<[u8; 16]> = match header(&headers, "content-md5") {
        None => None,
        Some(value) => {
            use base64::Engine as _;
            Some(
                base64::engine::general_purpose::STANDARD
                    .decode(value.trim())
                    .ok()
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| {
                        S3Error::new(
                            400,
                            "InvalidDigest",
                            "The Content-MD5 you specified was invalid.",
                        )
                    })?,
            )
        }
    };
    let now = now_ms();
    let body_id = object(state, move |file| file.begin_body(now)).await?;
    let mut decoder = PayloadDecoder::new(auth.framing, auth.checksum)?;
    let mut md5 = Md5::new();
    let mut segment = Vec::with_capacity(SEGMENT_BYTES);
    let mut decoded = Vec::new();
    let mut incoming = request.into_body();
    while let Some(frame) = incoming.frame().await {
        let frame = frame.map_err(|err| S3Error::invalid_request(err.to_string()))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        decoded.clear();
        decoder.feed(&data, &mut decoded)?;
        if decoder.decoded_len() > MAX_PUT_BYTES {
            return Err(too_large());
        }
        md5.update(&decoded);
        let mut rest = decoded.as_slice();
        while !rest.is_empty() {
            let take = (SEGMENT_BYTES - segment.len()).min(rest.len());
            segment.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if segment.len() == SEGMENT_BYTES {
                let full_segment =
                    std::mem::replace(&mut segment, Vec::with_capacity(SEGMENT_BYTES));
                let now = now_ms();
                object(state, move |file| {
                    file.append_segment(body_id, &full_segment, now)
                })
                .await?;
            }
        }
    }
    decoder.finish(declared)?;
    let digest: [u8; 16] = md5.finalize().into();
    if content_md5.is_some_and(|expected| expected != digest) {
        return Err(S3Error::bad_digest(
            "The Content-MD5 you specified did not match what we received.",
        ));
    }
    if !segment.is_empty() {
        let now = now_ms();
        object(state, move |file| {
            file.append_segment(body_id, &segment, now)
        })
        .await?;
    }
    Ok(SealedBody {
        body: body_id,
        md5: digest,
    })
}

pub async fn put_object(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let attrs = attrs_from(request.headers())?;
    // Fail before writing anything when the bucket or key is wrong.
    {
        let (lookup_bucket, lookup_key) = (bucket.clone(), key.clone());
        let check = object(state, move |file| {
            file.head_object(&lookup_bucket, &lookup_key, VersionSel::Latest)
        })
        .await?;
        if let Err(err @ (ObjectError::NoSuchBucket | ObjectError::InvalidKey(_))) = check {
            return Err(object_error(err, &bucket, &key));
        }
    }
    let sealed = receive_body(state, request, auth).await?;
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    let (stored, versioning) = object(state, move |file| {
        let stored = file.transaction(|tx| {
            tx.put_object(
                &bucket_name,
                &key_name,
                &sealed,
                &attrs,
                now_ms(),
                DROP_GRACE_MS,
            )
        })?;
        Ok((stored, versioning_of(file, &bucket_name)?))
    })
    .await?;
    let info = stored.map_err(|err| object_error(err, &bucket, &key))?;
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("etag", info.etag());
    if let Some(version) = version_header(versioning, info.version, info.null_version) {
        response = response.header("x-amz-version-id", version);
    }
    Ok(response.body(full(Bytes::new())).expect("a valid response"))
}

/// A response body fed by a channel: the reader task pushes chunks as it
/// reads them.
struct ChannelBody(mpsc::Receiver<Bytes>);

impl http_body::Body for ChannelBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|chunk| chunk.map(|bytes| Ok(Frame::data(bytes))))
    }
}

/// `Range: bytes=...` against an object of `size`: the inclusive byte range
/// to send, `Ok(None)` for the whole object, or `Err` when unsatisfiable.
fn parse_range(value: &str, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(spec) = value.trim().strip_prefix("bytes=") else {
        return Ok(None);
    };
    if spec.contains(',') {
        // Multiple ranges: S3 sends the whole object.
        return Ok(None);
    }
    let Some((first, last)) = spec.split_once('-') else {
        return Ok(None);
    };
    let (first, last) = (first.trim(), last.trim());
    let range = if first.is_empty() {
        let suffix: u64 = last.parse().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        (size.saturating_sub(suffix), size.checked_sub(1).ok_or(())?)
    } else {
        let first: u64 = first.parse().map_err(|_| ())?;
        if first >= size {
            return Err(());
        }
        let last = if last.is_empty() {
            size - 1
        } else {
            last.parse::<u64>().map_err(|_| ())?.min(size - 1)
        };
        if last < first {
            return Ok(None);
        }
        (first, last)
    };
    Ok(Some(range))
}

fn http_date(ms: u64) -> String {
    httpdate::fmt_http_date(std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms))
}

fn parse_http_date(value: &str) -> Option<u64> {
    httpdate::parse_http_date(value)
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
}

/// Whether an `If-Match`/`If-None-Match` list names this ETag.
fn etag_matches(list: &str, etag: &str) -> bool {
    list.split(',').map(str::trim).any(|candidate| {
        candidate == "*"
            || candidate == etag
            || candidate.trim_matches('"') == etag.trim_matches('"')
    })
}

/// The conditional headers' verdict: `Some(304 or 412)` stops the request.
/// `prefix` is `""` for a GET's own headers, or `x-amz-copy-source-` for a
/// copy's conditions on its source.
pub fn precondition(headers: &HeaderMap, prefix: &str, info: &ObjectInfo) -> Option<StatusCode> {
    let header = |name: &str| header(headers, &format!("{prefix}{name}"));
    let etag = info.etag();
    let modified = info.mtime_ms / 1000;
    let if_match = header("if-match");
    if let Some(list) = &if_match
        && !etag_matches(list, &etag)
    {
        return Some(StatusCode::PRECONDITION_FAILED);
    }
    if if_match.is_none()
        && let Some(since) = header("if-unmodified-since").and_then(|v| parse_http_date(&v))
        && modified > since
    {
        return Some(StatusCode::PRECONDITION_FAILED);
    }
    let if_none_match = header("if-none-match");
    if let Some(list) = &if_none_match
        && etag_matches(list, &etag)
    {
        return Some(StatusCode::NOT_MODIFIED);
    }
    if if_none_match.is_none()
        && let Some(since) = header("if-modified-since").and_then(|v| parse_http_date(&v))
        && modified <= since
    {
        return Some(StatusCode::NOT_MODIFIED);
    }
    None
}

/// The headers every GET and HEAD answer carries.
fn object_headers(
    builder: hyper::http::response::Builder,
    info: &ObjectInfo,
    versioning: Versioning,
) -> hyper::http::response::Builder {
    let attrs = &info.attrs;
    let mut builder = builder;
    if let Some(version) = version_header(versioning, info.version, info.null_version) {
        builder = builder.header("x-amz-version-id", version);
    }
    let mut builder = builder
        .header("etag", info.etag())
        .header("last-modified", http_date(info.mtime_ms))
        .header("accept-ranges", "bytes")
        .header(
            "content-type",
            attrs
                .content_type
                .clone()
                .unwrap_or_else(|| "binary/octet-stream".to_string()),
        );
    for (name, value) in [
        ("cache-control", &attrs.cache_control),
        ("content-disposition", &attrs.content_disposition),
        ("content-encoding", &attrs.content_encoding),
        ("content-language", &attrs.content_language),
        ("expires", &attrs.expires),
    ] {
        if let Some(value) = value {
            builder = builder.header(name, value);
        }
    }
    for (name, value) in &attrs.user {
        builder = builder.header(format!("x-amz-meta-{name}"), value);
    }
    if !attrs.tags.is_empty() {
        builder = builder.header("x-amz-tagging-count", attrs.tags.len().to_string());
    }
    builder
}

/// The bucket's versioning state (`Unversioned` once the bucket is gone).
pub fn versioning_of(file: &mut ObjFile<'_>, bucket: &str) -> Result<Versioning, StorageError> {
    Ok(file
        .bucket(bucket)?
        .map_or(Versioning::Unversioned, |info| info.versioning))
}

/// The `x-amz-version-id` a response about this version carries: none in a
/// bucket that never had versioning, as in S3.
pub fn version_header(versioning: Versioning, version: u64, null_version: bool) -> Option<String> {
    (versioning != Versioning::Unversioned)
        .then(|| crate::delete::version_id(version, null_version))
}

/// Adds a header to a built response.
pub fn add_header(response: &mut Response<Body>, name: &'static str, value: String) {
    response
        .headers_mut()
        .insert(name, value.parse().expect("a version id is a valid header"));
}

/// The version a request names: `?versionId=` or the latest.
pub fn version_sel(query: &str) -> Result<VersionSel, S3Error> {
    for pair in query.split('&') {
        if let Some(value) = pair.strip_prefix("versionId=") {
            return Ok(match value {
                "null" => VersionSel::Null,
                other => VersionSel::Seq(other.parse().map_err(|_| {
                    S3Error::new(400, "InvalidArgument", "Invalid version id specified")
                })?),
            });
        }
    }
    Ok(VersionSel::Latest)
}

pub async fn get_object(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
    head_only: bool,
) -> Result<Response<Body>, S3Error> {
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let headers = request.headers().clone();
    // The read runs in its own task: a client that disconnects drops this
    // handler, and the task still releases the lease it takes.
    let task_state = S3State {
        engine: state.engine.clone(),
        region: state.region.clone(),
    };
    tokio::spawn(
        async move { read_object(&task_state, &headers, bucket, key, which, head_only).await },
    )
    .await
    .map_err(|err| S3Error::internal(err.to_string()))?
}

async fn read_object(
    state: &S3State,
    headers: &HeaderMap,
    bucket: String,
    key: String,
    which: VersionSel,
    head_only: bool,
) -> Result<Response<Body>, S3Error> {
    // Resolve and lease in one lock hold, so the body cannot be collected
    // between the lookup and the lease.
    let (lookup_bucket, lookup_key) = (bucket.clone(), key.clone());
    let (found, versioning) = object(state, move |file| {
        let found = file.object_version(&lookup_bucket, &lookup_key, which)?;
        if let Ok(info) = &found
            && !head_only
            && !info.delete_marker
        {
            file.take_lease(info.body)?;
        }
        Ok((found, versioning_of(file, &lookup_bucket)?))
    })
    .await?;
    let info = found.map_err(|err| object_error(err, &bucket, &key))?;
    if info.delete_marker {
        // The newest version is a delete marker: the key is gone. A marker
        // named by its version id cannot be read.
        let err = if which == VersionSel::Latest {
            object_error(ObjectError::NoSuchKey, &bucket, &key)
        } else {
            S3Error::new(
                405,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
            )
            .with_header("last-modified", http_date(info.mtime_ms))
        };
        return Err(err.with_header("x-amz-delete-marker", "true").with_header(
            "x-amz-version-id",
            crate::delete::version_id(info.version, info.null_version),
        ));
    }
    let release = |state: &S3State| {
        let engine = state.engine.clone();
        let body = info.body;
        async move {
            let _ = engine
                .run_object(move |file| {
                    file.release_lease(body);
                    Ok(())
                })
                .await;
        }
    };

    if let Some(status) = precondition(headers, "", &info) {
        if !head_only {
            release(state).await;
        }
        if status == StatusCode::PRECONDITION_FAILED {
            return Err(S3Error::new(
                412,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
        return Ok(
            object_headers(Response::builder().status(status), &info, versioning)
                .body(full(Bytes::new()))
                .expect("a valid response"),
        );
    }
    let range = match header(headers, "range") {
        Some(value) => match parse_range(&value, info.size) {
            Ok(range) => range,
            Err(()) => {
                if !head_only {
                    release(state).await;
                }
                return Err(S3Error::new(
                    416,
                    "InvalidRange",
                    "The requested range is not satisfiable",
                ));
            }
        },
        None => None,
    };
    let (status, first, len) = match range {
        Some((first, last)) => (StatusCode::PARTIAL_CONTENT, first, last - first + 1),
        None => (StatusCode::OK, 0, info.size),
    };
    let mut builder = object_headers(Response::builder().status(status), &info, versioning)
        .header("content-length", len.to_string());
    if let Some((first, last)) = range {
        builder = builder.header(
            "content-range",
            format!("bytes {first}-{last}/{}", info.size),
        );
    }
    if head_only {
        return Ok(builder.body(full(Bytes::new())).expect("a valid response"));
    }

    let (tx, rx) = mpsc::channel(4);
    let engine = state.engine.clone();
    let body = info.body;
    // The body as plain parts (one, or a multipart object's parts), resolved
    // once: each read then goes straight to the part holding its bytes.
    let layout = match object(state, move |file| file.body_layout(body)).await {
        Ok(layout) => layout,
        Err(err) => {
            release(state).await;
            return Err(err);
        }
    };
    tokio::spawn(async move {
        let mut offset = first;
        let end = first + len;
        while offset < end {
            let Some(&(part, part_start, part_len)) = layout
                .iter()
                .find(|(_, start, len)| offset >= *start && offset < start + len)
            else {
                break;
            };
            let take = ((end.min(part_start + part_len) - offset) as usize).min(READ_CHUNK);
            let at = offset - part_start;
            let chunk = engine
                .run_object(move |file| {
                    let mut out = vec![0u8; take];
                    file.read_body(part, at, &mut out)?;
                    Ok(out)
                })
                .await;
            // A failed read ends the body early: the client sees fewer bytes
            // than Content-Length promised and fails the download.
            let Ok(chunk) = chunk else { break };
            if tx.send(Bytes::from(chunk)).await.is_err() {
                break;
            }
            offset += take as u64;
        }
        let _ = engine
            .run_object(move |file| {
                file.release_lease(body);
                Ok(())
            })
            .await;
    });
    Ok(builder
        .body(ChannelBody(rx).boxed())
        .expect("a valid response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_parse_like_s3() {
        assert_eq!(parse_range("bytes=0-9", 100), Ok(Some((0, 9))));
        assert_eq!(parse_range("bytes=90-", 100), Ok(Some((90, 99))));
        assert_eq!(parse_range("bytes=-10", 100), Ok(Some((90, 99))));
        assert_eq!(parse_range("bytes=50-500", 100), Ok(Some((50, 99))));
        assert_eq!(parse_range("bytes=-500", 100), Ok(Some((0, 99))));
        assert_eq!(parse_range("bytes=100-", 100), Err(()));
        assert_eq!(parse_range("bytes=0-1,5-6", 100), Ok(None));
        assert_eq!(parse_range("items=0-1", 100), Ok(None));
        assert_eq!(parse_range("bytes=0-0", 0), Err(()));
    }

    #[test]
    fn etag_lists_match_quoted_or_bare() {
        assert!(etag_matches("\"abc\"", "\"abc\""));
        assert!(etag_matches("abc", "\"abc\""));
        assert!(etag_matches("\"x\", \"abc\"", "\"abc\""));
        assert!(etag_matches("*", "\"abc\""));
        assert!(!etag_matches("\"abd\"", "\"abc\""));
    }
}
