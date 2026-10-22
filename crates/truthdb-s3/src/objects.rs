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
    ObjFile, ObjectAttrs, ObjectError, ObjectInfo, RestoreState, SEGMENT_BYTES, SealedBody,
    StorageClass, VersionSel, Versioning,
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
        ObjectError::Locked => {
            S3Error::access_denied("Access Denied because object protected by object lock.")
        }
        ObjectError::ObjectLockNotEnabled => {
            S3Error::invalid_request("Bucket is missing Object Lock Configuration")
        }
        ObjectError::InvalidRetention(why) => S3Error::invalid_request(why),
        ObjectError::NotArchived => S3Error::new(
            403,
            "ObjectAlreadyInActiveTierError",
            "This action is not allowed against this storage tier.",
        ),
        ObjectError::RestoreInProgress => S3Error::new(
            409,
            "RestoreAlreadyInProgress",
            "Object restore is already in progress",
        ),
        ObjectError::TierUnavailable => S3Error::invalid_request(
            "Expedited retrievals are not available for the DEEP_ARCHIVE storage class",
        ),
    }
}

pub async fn object<T, F>(state: &S3State, op: F) -> Result<T, S3Error>
where
    T: Send + 'static,
    F: FnOnce(&mut ObjFile<'_>) -> Result<T, truthdb_core::storage::StorageError> + Send + 'static,
{
    crate::scope::object(state, op).await
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

/// The header naming a write's storage class.
pub const STORAGE_CLASS_HEADER: &str = "x-amz-storage-class";

/// The storage class a write names (STANDARD without one).
pub fn storage_class(value: Option<&str>) -> Result<StorageClass, S3Error> {
    match value {
        None => Ok(StorageClass::Standard),
        Some(name) => StorageClass::parse(name).ok_or_else(|| {
            S3Error::new(
                400,
                "InvalidStorageClass",
                "The storage class you specified is not valid",
            )
        }),
    }
}

/// [`storage_class`] from a request's headers.
pub fn storage_class_from(headers: &HeaderMap) -> Result<StorageClass, S3Error> {
    storage_class(
        headers
            .get(STORAGE_CLASS_HEADER)
            .map(|value| value.to_str().unwrap_or("")),
    )
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
        customer_key: None,
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
    key: Option<&crate::ssec::ProvidedKey>,
    class: StorageClass,
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
    let mut sink = BodySink::begin(state, key, class).await?;
    let mut decoder = PayloadDecoder::new(auth.framing, auth.checksum)?;
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
        sink.push(state, &decoded).await?;
    }
    decoder.finish(declared)?;
    let (sealed, plain_md5) = sink.finish(state).await?;
    if content_md5.is_some_and(|expected| expected != plain_md5) {
        return Err(S3Error::bad_digest(
            "The Content-MD5 you specified did not match what we received.",
        ));
    }
    Ok(sealed)
}

/// A body written into the object engine as it arrives: in segments, with
/// its MD5, encrypted with a customer-provided key when given one, else
/// compressed as its storage class asks (off the async threads). A body
/// never finished stays staged until garbage collection reclaims it.
pub struct BodySink {
    body: u64,
    /// Of the bytes stored (the ETag's).
    md5: Md5,
    /// Of the bytes pushed (Content-MD5's).
    plain_md5: Md5,
    encryptor: Option<crate::ssec::Encryptor>,
    /// The zstd level each segment is offered at (0: stored as it is).
    pack_level: u8,
    segment: Vec<u8>,
    len: u64,
}

impl BodySink {
    pub async fn begin(
        state: &S3State,
        key: Option<&crate::ssec::ProvidedKey>,
        class: StorageClass,
    ) -> Result<BodySink, S3Error> {
        let encryptor = key.map(|key| key.encryptor()).transpose()?;
        // Ciphertext does not shrink.
        let pack_level = if key.is_some() { 0 } else { class.pack_level() };
        let now = now_ms();
        let body = object(state, move |file| file.begin_body(now)).await?;
        Ok(BodySink {
            body,
            md5: Md5::new(),
            plain_md5: Md5::new(),
            encryptor,
            pack_level,
            segment: Vec::with_capacity(SEGMENT_BYTES),
            len: 0,
        })
    }

    /// Bytes pushed so far.
    pub fn pushed(&self) -> u64 {
        self.len
    }

    pub async fn push(&mut self, state: &S3State, data: &[u8]) -> Result<(), S3Error> {
        self.plain_md5.update(data);
        self.len += data.len() as u64;
        match self
            .encryptor
            .as_mut()
            .map(|encryptor| encryptor.push(data))
        {
            Some(sealed) => self.store(state, &sealed).await,
            None => self.store(state, data).await,
        }
    }

    /// Writes `data` as it goes to the object engine.
    async fn store(&mut self, state: &S3State, data: &[u8]) -> Result<(), S3Error> {
        self.md5.update(data);
        let mut rest = data;
        while !rest.is_empty() {
            let take = (SEGMENT_BYTES - self.segment.len()).min(rest.len());
            self.segment.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.segment.len() == SEGMENT_BYTES {
                let full_segment =
                    std::mem::replace(&mut self.segment, Vec::with_capacity(SEGMENT_BYTES));
                append_segment(state, self.body, self.pack_level, full_segment).await?;
            }
        }
        Ok(())
    }

    /// The body ready to seal, and the MD5 of the bytes pushed.
    pub async fn finish(mut self, state: &S3State) -> Result<(SealedBody, [u8; 16]), S3Error> {
        let size = match self.encryptor.take() {
            Some(encryptor) => {
                self.store(state, &encryptor.finish()).await?;
                Some(self.len)
            }
            None => None,
        };
        let BodySink {
            body,
            md5,
            plain_md5,
            pack_level,
            segment,
            ..
        } = self;
        if !segment.is_empty() {
            append_segment(state, body, pack_level, segment).await?;
        }
        Ok((
            SealedBody {
                body,
                md5: md5.finalize().into(),
                size,
                pack_level,
            },
            plain_md5.finalize().into(),
        ))
    }
}

/// Compressions of cold uploads under way at once: one per CPU. A level-19
/// segment costs about 70 ms and 20 MB; uploads past the limit wait (and so
/// are read more slowly) instead of filling the blocking pool.
fn compressing() -> &'static tokio::sync::Semaphore {
    static LIMIT: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
    LIMIT.get_or_init(|| {
        tokio::sync::Semaphore::new(std::thread::available_parallelism().map_or(1, |n| n.get()))
    })
}

/// Appends `segment` to `body`, as a zstd frame at `pack_level` when one is
/// smaller (compressed on a blocking thread, outside the storage lock).
async fn append_segment(
    state: &S3State,
    body: u64,
    pack_level: u8,
    segment: Vec<u8>,
) -> Result<(), S3Error> {
    let (frame, segment) = if pack_level == 0 {
        (None, segment)
    } else {
        let permit = compressing()
            .acquire()
            .await
            .map_err(|err| S3Error::internal(err.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            (truthdb_object::pack_segment(&segment, pack_level), segment)
        })
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
    };
    let now = now_ms();
    object(state, move |file| match frame {
        Some(frame) => file.append_packed_segment(body, &frame, segment.len(), now),
        None => file.append_segment(body, &segment, now),
    })
    .await
}

/// PutObject. `lock` is the Object Lock its headers ask for (authorized by
/// the caller); without one, a bucket's default retention applies.
pub async fn put_object(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: String,
    lock: Option<truthdb_object::ObjectLock>,
) -> Result<Response<Body>, S3Error> {
    let mut attrs = attrs_from(request.headers())?;
    let class = storage_class_from(request.headers())?;
    let encrypted_at_rest = state.storage.is_encrypted();
    crate::sse::check_write(request.headers(), encrypted_at_rest)?;
    let customer = crate::ssec::provided(request.headers(), false)?;
    // Fail before writing anything when the bucket or key is wrong.
    {
        let (lookup_bucket, lookup_key) = (bucket.clone(), key.clone());
        let (check, blocked, (bucket_lock, default)) = object(state, move |file| {
            Ok((
                file.head_object(&lookup_bucket, &lookup_key, VersionSel::Latest)?,
                crate::sse::sse_c_blocked(file, &lookup_bucket)?,
                crate::object_lock::bucket_state(file, &lookup_bucket)?,
            ))
        })
        .await?;
        if let Err(err @ (ObjectError::NoSuchBucket | ObjectError::InvalidKey(_))) = check {
            return Err(object_error(err, &bucket, &key));
        }
        crate::ssec::check_write(customer.as_ref(), request.headers(), blocked)?;
        if let Some(lock) =
            crate::object_lock::new_object_lock(lock, bucket_lock, default, now_ms())?
        {
            crate::object_lock::check_integrity(request.headers(), &lock)?;
        }
    }
    attrs.customer_key = customer.as_ref().map(|key| key.stored()).transpose()?;
    let headers = request.headers().clone();
    let sealed = receive_body(state, request, auth, customer.as_ref(), class).await?;
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    let (stored, versioning, expiration) =
        crate::scope::object_as(state, "PutObject", move |file| {
            let now = now_ms();
            // Decided in the write's own lock hold: the bucket's Object Lock
            // may have changed while the body arrived (a default retention set,
            // or the bucket made again without Object Lock).
            let (bucket_lock, default) = crate::object_lock::bucket_state(file, &bucket_name)?;
            let lock = match crate::object_lock::new_object_lock(lock, bucket_lock, default, now)
                .and_then(|lock| {
                    if let Some(lock) = &lock {
                        crate::object_lock::check_integrity(&headers, lock)?;
                    }
                    Ok(lock)
                }) {
                Ok(lock) => lock,
                Err(err) => return Ok(Err(err)),
            };
            let stored = file.transaction(|tx| {
                let stored = tx.put_object(
                    &bucket_name,
                    &key_name,
                    &sealed,
                    &attrs,
                    class,
                    now,
                    DROP_GRACE_MS,
                )?;
                if let (Ok(info), Some(lock)) = (&stored, &lock) {
                    tx.set_lock(info.version, lock)?;
                }
                Ok(stored)
            })?;
            let expiration = match &stored {
                Ok(info) => crate::lifecycle::expiration_header(
                    file.lifecycle(&bucket_name)?.as_ref(),
                    &key_name,
                    info.size,
                    &info.attrs.tags,
                    info.mtime_ms,
                ),
                Err(_) => None,
            };
            Ok(Ok((stored, versioning_of(file, &bucket_name)?, expiration)))
        })
        .await??;
    let info = stored.map_err(|err| object_error(err, &bucket, &key))?;
    let customer_key = info.attrs.customer_key.as_ref();
    let mut response = crate::ssec::with_echo(
        crate::sse::with_header(
            Response::builder()
                .status(StatusCode::OK)
                .header("etag", info.etag()),
            encrypted_at_rest && customer_key.is_none(),
        ),
        customer_key,
    );
    if let Some(version) = version_header(versioning, info.version, info.null_version) {
        response = response.header("x-amz-version-id", version);
    }
    if let Some(expiration) = expiration {
        response = response.header("x-amz-expiration", expiration);
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

/// The last instant an HTTP date can name (9999-12-31 23:59:59 UTC): later
/// times are written as it.
const LAST_HTTP_DATE_MS: u64 = 253_402_300_799_000;

pub(crate) fn http_date(ms: u64) -> String {
    httpdate::fmt_http_date(
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms.min(LAST_HTTP_DATE_MS)),
    )
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
    encrypted_at_rest: bool,
    restore: Option<RestoreState>,
    expiration: Option<&str>,
) -> hyper::http::response::Builder {
    let attrs = &info.attrs;
    // An object with a customer-provided key says so, not AES256.
    let mut builder = crate::ssec::with_echo(
        crate::sse::with_header(builder, encrypted_at_rest && attrs.customer_key.is_none()),
        attrs.customer_key.as_ref(),
    );
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
    // As S3: the class is named only when it is not STANDARD.
    if info.storage_class != StorageClass::Standard {
        builder = builder.header(STORAGE_CLASS_HEADER, info.storage_class.as_str());
    }
    if let Some(restore) = crate::restore::header(restore) {
        builder = builder.header("x-amz-restore", restore);
    }
    if let Some(expiration) = expiration {
        builder = builder.header("x-amz-expiration", expiration);
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
        .then(|| truthdb_object::version_id(version, null_version))
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

/// The `response-*` query parameters of a GET or HEAD: header values that
/// replace the object's own in the response (a signed request's only).
fn response_overrides(query: &str, signed: bool) -> Result<Vec<(String, String)>, S3Error> {
    let overrides: Vec<(String, String)> = crate::listing::query_params(query)
        .into_iter()
        .filter_map(|(name, value)| {
            let header = match name.as_str() {
                "response-content-type" => "content-type",
                "response-content-language" => "content-language",
                "response-expires" => "expires",
                "response-cache-control" => "cache-control",
                "response-content-disposition" => "content-disposition",
                "response-content-encoding" => "content-encoding",
                _ => return None,
            };
            Some((header.to_string(), value))
        })
        .collect();
    if let Some((header, _)) = overrides
        .iter()
        .find(|(_, value)| hyper::header::HeaderValue::from_str(value).is_err())
    {
        return Err(S3Error::new(
            400,
            "InvalidArgument",
            format!("The {header} override is not a valid header value"),
        ));
    }
    if !overrides.is_empty() && !signed {
        return Err(S3Error::invalid_request(
            "Request specific response headers cannot be used for anonymous GET requests.",
        ));
    }
    Ok(overrides)
}

pub async fn get_object(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
    head_only: bool,
    signed: bool,
    lock_visible: LockVisible,
) -> Result<Response<Body>, S3Error> {
    crate::sse::check_read(request.headers())?;
    let customer = crate::ssec::provided(request.headers(), false)?;
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let overrides = response_overrides(request.uri().query().unwrap_or(""), signed)?;
    let headers = request.headers().clone();
    // The read runs in its own task: a client that disconnects drops this
    // handler, and the task still releases the lease it takes.
    let task_state = S3State {
        engine: state.engine.clone(),
        region: state.region.clone(),
        signing_keys: state.signing_keys.clone(),
        storage: state.storage.clone(),
        restore_delays: state.restore_delays,
    };
    let scope = crate::scope::current().ok_or_else(|| S3Error::internal("no request scope"))?;
    tokio::spawn(crate::scope::within(scope, async move {
        read_object(
            &task_state,
            &headers,
            bucket,
            key,
            which,
            head_only,
            &overrides,
            customer,
            lock_visible,
        )
        .await
    }))
    .await
    .map_err(|err| S3Error::internal(err.to_string()))?
}

/// Which of an object's lock headers a GET or HEAD shows: those the caller
/// may read (`s3:GetObjectRetention`, `s3:GetObjectLegalHold`).
#[derive(Debug, Clone, Copy)]
pub struct LockVisible {
    pub retention: bool,
    pub legal_hold: bool,
}

#[allow(clippy::too_many_arguments)]
async fn read_object(
    state: &S3State,
    headers: &HeaderMap,
    bucket: String,
    key: String,
    which: VersionSel,
    head_only: bool,
    overrides: &[(String, String)],
    customer: Option<crate::ssec::ProvidedKey>,
    lock_visible: LockVisible,
) -> Result<Response<Body>, S3Error> {
    // Resolve and lease in one lock hold, so the body cannot be collected
    // between the lookup and the lease.
    let (lookup_bucket, lookup_key) = (bucket.clone(), key.clone());
    let (found, versioning, lock, restore, expiration) = object(state, move |file| {
        let found = file.object_version(&lookup_bucket, &lookup_key, which)?;
        // The current version's expiration, by the bucket's lifecycle rules.
        let expiration = match &found {
            Ok(info) if which == VersionSel::Latest && !info.delete_marker => {
                crate::lifecycle::expiration_header(
                    file.lifecycle(&lookup_bucket)?.as_ref(),
                    &lookup_key,
                    info.size,
                    &info.attrs.tags,
                    info.mtime_ms,
                )
            }
            _ => None,
        };
        // Everything that can fail comes before the lease: a failure after
        // it would leave the lease held with no one to release it.
        let versioning = versioning_of(file, &lookup_bucket)?;
        let (lock, restore) = match &found {
            Ok(info) => (
                file.object_lock(info.version)?,
                file.restore(info.version)?
                    .and_then(|restore| restore.state(now_ms())),
            ),
            Err(_) => (truthdb_object::ObjectLock::default(), None),
        };
        // An archived version not restored is not read: no lease.
        if let Ok(info) = &found
            && !head_only
            && !info.delete_marker
            && !truthdb_object::unreadable(info.storage_class, restore)
        {
            file.take_lease(info.body)?;
        }
        Ok((found, versioning, lock, restore, expiration))
    })
    .await?;
    let mut info = found.map_err(|err| object_error(err, &bucket, &key))?;
    for (header, value) in overrides {
        let attrs = &mut info.attrs;
        let field = match header.as_str() {
            "content-type" => &mut attrs.content_type,
            "content-language" => &mut attrs.content_language,
            "expires" => &mut attrs.expires,
            "cache-control" => &mut attrs.cache_control,
            "content-disposition" => &mut attrs.content_disposition,
            _ => &mut attrs.content_encoding,
        };
        *field = Some(value.clone());
    }
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
            truthdb_object::version_id(info.version, info.null_version),
        ));
    }
    if !head_only && truthdb_object::unreadable(info.storage_class, restore) {
        return Err(crate::restore::invalid_object_state());
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

    // An object encrypted with a customer-provided key is read with that key
    // only; one without, with none.
    if let Err(err) = crate::ssec::check_read(info.attrs.customer_key.as_ref(), customer.as_ref()) {
        if !head_only {
            release(state).await;
        }
        return Err(err);
    }
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
        return Ok(object_headers(
            Response::builder().status(status),
            &info,
            versioning,
            state.storage.is_encrypted(),
            restore,
            expiration.as_deref(),
        )
        .body(full(Bytes::new()))
        .expect("a valid response"));
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
    let mut builder = crate::object_lock::with_headers(
        object_headers(
            Response::builder().status(status),
            &info,
            versioning,
            state.storage.is_encrypted(),
            restore,
            expiration.as_deref(),
        ),
        &lock,
        lock_visible.retention,
        lock_visible.legal_hold,
    )
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
        // A failed read ends the body early: the client sees fewer bytes than
        // Content-Length promised and fails the download.
        stream_plaintext(&engine, &layout, customer.as_ref(), first, len, &tx).await;
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

/// Sends the plaintext bytes `[first, first + len)` of a body laid out as
/// `layout` (its parts encrypted with `customer`, if given) into `tx`, a
/// read's worth at a time. Whether every byte was sent: a failed read, a
/// part that does not decrypt, or a receiver gone ends it early.
pub(crate) async fn stream_plaintext(
    engine: &truthdb_engine::session::EngineHandle,
    layout: &[(u64, u64, u64)],
    customer: Option<&crate::ssec::ProvidedKey>,
    first: u64,
    len: u64,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    match customer {
        Some(customer) => stream_decrypted(engine, layout, customer, first, len, tx).await,
        None => stream_plain(engine, layout, first, len, tx).await,
    }
}

/// [`stream_plaintext`] of a body stored as it is.
async fn stream_plain(
    engine: &truthdb_engine::session::EngineHandle,
    layout: &[(u64, u64, u64)],
    first: u64,
    len: u64,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    let (mut offset, end) = (first, first + len);
    while offset < end {
        let Some(&(part, part_start, part_len)) = layout
            .iter()
            .find(|(_, start, len)| offset >= *start && offset < start + len)
        else {
            return false;
        };
        let at = offset - part_start;
        // Up to the end of the body's segment: a packed segment is one zstd
        // frame, inflated whole for each read.
        let segment_left = SEGMENT_BYTES - (at % SEGMENT_BYTES as u64) as usize;
        let take = ((end.min(part_start + part_len) - offset) as usize)
            .min(READ_CHUNK)
            .min(segment_left);
        let chunk = engine
            .run_object(move |file| {
                let mut out = vec![0u8; take];
                file.read_body(part, at, &mut out)?;
                Ok(out)
            })
            .await;
        let Ok(chunk) = chunk else {
            return false;
        };
        if tx.send(Bytes::from(chunk)).await.is_err() {
            return false;
        }
        offset += take as u64;
    }
    true
}

/// [`stream_plaintext`] of a body whose parts are encrypted with `customer`:
/// each read takes whole chunks of one part and decrypts them.
async fn stream_decrypted(
    engine: &truthdb_engine::session::EngineHandle,
    layout: &[(u64, u64, u64)],
    customer: &crate::ssec::ProvidedKey,
    first: u64,
    len: u64,
    tx: &mpsc::Sender<Bytes>,
) -> bool {
    use crate::ssec::{CHUNK, HEADER_LEN, chunk_at, plain_len};
    let chunk = CHUNK as u64;
    // The parts as plaintext: (body, start, length).
    let mut parts = Vec::with_capacity(layout.len());
    let mut start = 0;
    for &(part, _, stored) in layout {
        let Some(plain) = plain_len(stored) else {
            return false;
        };
        parts.push((part, start, plain));
        start += plain;
    }
    let mut current: Option<(u64, crate::ssec::Decryptor)> = None;
    let (mut offset, end) = (first, first + len);
    while offset < end {
        let Some(&(part, start, plain)) = parts
            .iter()
            .find(|(_, start, len)| offset >= *start && offset < start + len)
        else {
            return false;
        };
        if current.as_ref().map(|(at, _)| *at) != Some(part) {
            let header = engine
                .run_object(move |file| {
                    let mut header = vec![0u8; HEADER_LEN];
                    file.read_body(part, 0, &mut header)?;
                    Ok(header)
                })
                .await;
            let Some(decryptor) = header.ok().and_then(|header| customer.decryptor(&header)) else {
                return false;
            };
            current = Some((part, decryptor));
        }
        let Some((_, decryptor)) = &current else {
            return false;
        };
        // Whole chunks, up to a read's worth, covering what is wanted next.
        let within = offset - start;
        let wanted_end = end.min(start + plain) - start;
        let first_chunk = within / chunk;
        let last_chunk =
            ((wanted_end - 1) / chunk).min(first_chunk + (READ_CHUNK / CHUNK) as u64 - 1);
        let (at, _, _) = chunk_at(first_chunk, plain);
        let (last_at, last_len, _) = chunk_at(last_chunk, plain);
        let stored_len = (last_at + last_len - at) as usize;
        let stored = engine
            .run_object(move |file| {
                let mut stored = vec![0u8; stored_len];
                file.read_body(part, at, &mut stored)?;
                Ok(stored)
            })
            .await;
        let Ok(stored) = stored else {
            return false;
        };
        let mut plaintext = Vec::with_capacity((last_chunk + 1 - first_chunk) as usize * CHUNK);
        for index in first_chunk..=last_chunk {
            let (chunk_at, chunk_len, last) = chunk_at(index, plain);
            let from = (chunk_at - at) as usize;
            let Some(opened) =
                decryptor.open(index, last, &stored[from..from + chunk_len as usize])
            else {
                return false;
            };
            plaintext.extend(opened);
        }
        let covered_end = (first_chunk * chunk + plaintext.len() as u64).min(wanted_end);
        let piece = plaintext
            [(within - first_chunk * chunk) as usize..(covered_end - first_chunk * chunk) as usize]
            .to_vec();
        if tx.send(Bytes::from(piece)).await.is_err() {
            return false;
        }
        offset = start + covered_end;
    }
    true
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
