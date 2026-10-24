//! Copies. CopyObject shares the source's body, so no bytes are copied, when
//! the copy is encrypted as the source is (no customer-provided key, or the
//! same one); otherwise the source is decrypted and encrypted again as it
//! streams into a new body. UploadPartCopy shares a whole plain source as
//! the part; a range, a multipart source, or a customer-provided key has its
//! bytes copied into a new part body (a multipart object's body cannot be a
//! part: composites do not nest).

use std::sync::Arc;

use hyper::body::Incoming;
use hyper::header::HeaderMap;
use hyper::{Request, Response};
use tokio::sync::mpsc;
use truthdb_core::storage::StorageError;
use truthdb_object::{BodyInfo, ObjFile, ObjectInfo, SealedBody, StorageClass, VersionSel};

use crate::error::{S3Error, escape};
use crate::listing::query_params;
use crate::multipart::{check_upload, part_number, upload_error, upload_id, xml};
use crate::objects::{
    BodySink, DROP_GRACE_MS, MAX_PUT_BYTES, add_header, header, metadata_from, now_ms, object,
    object_error, precondition, version_header, version_sel, versioning_of,
};
use crate::server::Body;
use crate::server::S3State;
use crate::ssec::ProvidedKey;
use crate::time::iso8601;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// `x-amz-copy-source`: `bucket/key`, URL-encoded, with an optional leading
/// `/` and `?versionId=`.
pub(crate) fn copy_source(headers: &HeaderMap) -> Result<(String, String, VersionSel), S3Error> {
    let invalid = || {
        S3Error::new(
            400,
            "InvalidArgument",
            "Copy Source must mention the source bucket and key: sourcebucket/sourcekey",
        )
    };
    let raw = header(headers, "x-amz-copy-source").ok_or_else(invalid)?;
    let (path, query) = raw.split_once('?').unwrap_or((&raw, ""));
    let which = version_sel(query)?;
    let path = crate::sigv4::decode_path(path).ok_or_else(invalid)?;
    let path = path.strip_prefix('/').unwrap_or(&path);
    match path.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok((bucket.to_string(), key.to_string(), which))
        }
        _ => Err(invalid()),
    }
}

/// The source version, which must not be a delete marker and must meet the
/// `x-amz-copy-source-if-*` conditions.
fn resolve_source(
    file: &mut ObjFile<'_>,
    bucket: &str,
    key: &str,
    which: VersionSel,
    headers: &HeaderMap,
    source_key: Option<&ProvidedKey>,
) -> Result<Result<ObjectInfo, S3Error>, StorageError> {
    let source = match file.head_object(bucket, key, which)? {
        Ok(source) => source,
        Err(err) => return Ok(Err(object_error(err, bucket, key))),
    };
    if source.delete_marker {
        return Ok(Err(S3Error::new(
            400,
            "InvalidRequest",
            "The source of a copy request may not specifically refer to a delete marker by version id.",
        )));
    }
    // An archived source is copied only while restored.
    if file.archived_unreadable(
        source.version,
        source.storage_class,
        crate::objects::now_ms(),
    )? {
        return Ok(Err(crate::restore::invalid_object_state()));
    }
    // A source with a customer-provided key is read with that key only.
    if let Err(err) = crate::ssec::check_read(source.attrs.customer_key.as_ref(), source_key) {
        return Ok(Err(err));
    }
    if precondition(headers, "x-amz-copy-source-", &source).is_some() {
        return Ok(Err(S3Error::new(
            412,
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
        )));
    }
    Ok(Ok(source))
}

/// Whether a copy's `COPY`/`REPLACE` directive header says `REPLACE`.
fn replaces(headers: &HeaderMap, name: &str, what: &str) -> Result<bool, S3Error> {
    match header(headers, name).as_deref() {
        None | Some("COPY") => Ok(false),
        Some("REPLACE") => Ok(true),
        Some(_) => Err(S3Error::new(
            400,
            "InvalidArgument",
            format!("Unknown {what} directive."),
        )),
    }
}

/// The customer-provided keys of a copy: the source's
/// (`x-amz-copy-source-…`) and the copy's; a copy with one is refused in a
/// bucket blocking SSE-C.
async fn copy_keys(
    state: &S3State,
    headers: &HeaderMap,
    bucket: &str,
) -> Result<(Option<Arc<ProvidedKey>>, Option<Arc<ProvidedKey>>), S3Error> {
    let source_key = crate::ssec::provided(headers, true)?.map(Arc::new);
    let target_key = crate::ssec::provided(headers, false)?.map(Arc::new);
    if target_key.is_some() {
        let name = bucket.to_string();
        let blocked = object(state, move |file| crate::sse::sse_c_blocked(file, &name)).await?;
        crate::ssec::check_write(target_key.as_deref(), headers, blocked)?;
    }
    Ok((source_key, target_key))
}

/// What a copy found under the lock: done (the body shared), or a source to
/// stream (leased) into a new body.
enum CopyStep {
    Shared(ObjectInfo, Vec<(&'static str, String)>),
    Stream {
        source: ObjectInfo,
        attrs: Box<truthdb_object::ObjectAttrs>,
        source_version: Option<String>,
    },
}

/// CopyObject. `lock` is the Object Lock its headers ask for (authorized by
/// the caller); without one, the target bucket's default retention applies.
/// The source's own lock is not copied.
pub async fn copy_object(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
    lock: Option<truthdb_object::ObjectLock>,
) -> Result<Response<Body>, S3Error> {
    let headers = request.headers().clone();
    // The copy runs in its own task: a client that disconnects drops this
    // handler, and the task still releases the lease it takes on a source
    // it streams.
    let task_state = S3State {
        engine: state.engine.clone(),
        region: state.region.clone(),
        signing_keys: state.signing_keys.clone(),
        storage: state.storage.clone(),
        restore_delays: state.restore_delays,
        domains: state.domains.clone(),
        request_metrics: state.request_metrics.clone(),
        metric_recorder: state.metric_recorder.clone(),
        metrics_flush: state.metrics_flush,
        access_logger: state.access_logger.clone(),
        access_log_flush: state.access_log_flush,
        activity: state.activity.clone(),
        activity_flush: state.activity_flush,
    };
    let scope = crate::scope::current().ok_or_else(|| S3Error::internal("no request scope"))?;
    tokio::spawn(crate::scope::within(scope, async move {
        copy_whole(&task_state, headers, bucket, key, lock).await
    }))
    .await
    .map_err(|err| S3Error::internal(err.to_string()))?
}

async fn copy_whole(
    state: &S3State,
    headers: HeaderMap,
    bucket: String,
    key: String,
    lock: Option<truthdb_object::ObjectLock>,
) -> Result<Response<Body>, S3Error> {
    crate::sse::check_write(&headers, state.storage.is_encrypted())?;
    let (src_bucket, src_key, which) = copy_source(&headers)?;
    let replace = replaces(&headers, "x-amz-metadata-directive", "metadata")?;
    let replace_tags = replaces(&headers, "x-amz-tagging-directive", "tagging")?;
    // As S3: the copy's class is the one its request names, or STANDARD;
    // never the source's.
    let class = crate::objects::storage_class_from(&headers)?;
    let (source_key, target_key) = copy_keys(state, &headers, &bucket).await?;
    let target_record = target_key.as_deref().map(ProvidedKey::stored).transpose()?;
    // The copy is encrypted as the source is when neither has a key, or
    // both the same one (the source's key is checked against it below).
    let same_keys = match (&source_key, &target_key) {
        (Some(source), Some(target)) => source.same_key(target),
        (None, None) => true,
        _ => false,
    };
    // A copy's redirect location is the request's, never the source's.
    let website_redirect = crate::objects::website_redirect_from(&headers)?;
    // Only what a directive replaces is read from the request.
    let new_metadata = if replace {
        metadata_from(&headers)?
    } else {
        Default::default()
    };
    let new_tags = if replace_tags {
        crate::tagging::tags_from(&headers)?
    } else {
        Vec::new()
    };
    let (dst_bucket, dst_key) = (bucket.clone(), key.clone());
    let lookup_source_key = source_key.clone();
    // Resolve and share (or lease) in one lock hold: the source cannot
    // change between.
    let step = crate::scope::object_as(state, "CopyObject", move |file| {
        let source = match resolve_source(
            file,
            &src_bucket,
            &src_key,
            which,
            &headers,
            lookup_source_key.as_deref(),
        )? {
            Ok(source) => source,
            Err(err) => return Ok(Err(err)),
        };
        let shared = source.attrs.customer_key.is_some() == target_record.is_some() && same_keys;
        // A copy to itself must change something: its metadata, its
        // storage class, its redirect location, or its encryption.
        if !replace
            && website_redirect.is_none()
            && shared
            && class == source.storage_class
            && (src_bucket.as_str(), src_key.as_str()) == (&dst_bucket, &dst_key)
        {
            let current = file.head_object(&dst_bucket, &dst_key, VersionSel::Latest)?;
            if current.is_ok_and(|current| current.version == source.version) {
                return Ok(Err(S3Error::new(
                    400,
                    "InvalidRequest",
                    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
                )));
            }
        }
        let mut attrs = if replace {
            new_metadata
        } else {
            source.attrs.clone()
        };
        attrs.website_redirect = website_redirect;
        attrs.tags = if replace_tags {
            new_tags
        } else {
            source.attrs.tags.clone()
        };
        let source_version = version_header(
            versioning_of(file, &src_bucket)?,
            source.version,
            source.null_version,
        );
        // Checked before anything is copied; a streamed copy decides its lock
        // again when it writes.
        let (bucket_lock, default) = crate::object_lock::bucket_state(file, &dst_bucket)?;
        let shared_lock =
            match crate::object_lock::new_object_lock(lock, bucket_lock, default, now_ms()) {
                Ok(lock) => lock,
                Err(err) => return Ok(Err(err)),
            };
        if !shared {
            attrs.customer_key = target_record;
            if source.size > MAX_PUT_BYTES {
                return Ok(Err(too_large_source()));
            }
            // The lease keeps the bytes while they are copied outside the
            // lock.
            file.take_lease(source.body)?;
            return Ok(Ok(CopyStep::Stream {
                source,
                attrs: Box::new(attrs),
                source_version,
            }));
        }
        attrs.customer_key = source.attrs.customer_key.clone();
        let copied = file.transaction(|tx| {
            let copied = tx.copy_object(
                &dst_bucket,
                &dst_key,
                &source,
                &attrs,
                class,
                now_ms(),
                DROP_GRACE_MS,
            )?;
            if let (Ok(copied), Some(lock)) = (&copied, &shared_lock) {
                tx.set_lock(copied.version, lock)?;
            }
            Ok(copied)
        })?;
        let copied = match copied {
            Ok(copied) => copied,
            Err(err) => return Ok(Err(object_error(err, &dst_bucket, &dst_key))),
        };
        let mut versions = Vec::new();
        if let Some(version) = source_version {
            versions.push(("x-amz-copy-source-version-id", version));
        }
        if let Some(version) = version_header(
            versioning_of(file, &dst_bucket)?,
            copied.version,
            copied.null_version,
        ) {
            versions.push(("x-amz-version-id", version));
        }
        Ok(Ok(CopyStep::Shared(copied, versions)))
    })
    .await??;
    let (info, versions) = match step {
        CopyStep::Shared(info, versions) => (info, versions),
        CopyStep::Stream {
            source,
            attrs,
            source_version,
        } => {
            let body = source.body;
            let copied = copy_into(
                state,
                body,
                (0, source.size),
                source_key,
                target_key.as_deref(),
                class,
            )
            .await;
            // Unchecked: the lease goes even when the request's scope no
            // longer holds, or the body stays pinned.
            let _ = state
                .engine
                .run_object(move |file| {
                    file.release_lease(body);
                    Ok(())
                })
                .await;
            let sealed = copied?;
            let (dst_bucket, dst_key) = (bucket.clone(), key.clone());
            let (stored, versioning) = crate::scope::object_as(state, "CopyObject", move |file| {
                let now = now_ms();
                // Decided in the write's own lock hold (see `put_object`).
                let (bucket_lock, default) = crate::object_lock::bucket_state(file, &dst_bucket)?;
                let lock =
                    match crate::object_lock::new_object_lock(lock, bucket_lock, default, now) {
                        Ok(lock) => lock,
                        Err(err) => return Ok(Err(err)),
                    };
                let stored = file.transaction(|tx| {
                    let stored = tx.put_object(
                        &dst_bucket,
                        &dst_key,
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
                Ok(Ok((stored, versioning_of(file, &dst_bucket)?)))
            })
            .await??;
            let info = stored.map_err(|err| object_error(err, &bucket, &key))?;
            let mut versions = Vec::new();
            if let Some(version) = source_version {
                versions.push(("x-amz-copy-source-version-id", version));
            }
            if let Some(version) = version_header(versioning, info.version, info.null_version) {
                versions.push(("x-amz-version-id", version));
            }
            (info, versions)
        }
    };
    let mut response = xml(format!(
        "<CopyObjectResult xmlns=\"{XMLNS}\"><ETag>{}</ETag><LastModified>{}</LastModified></CopyObjectResult>",
        escape(&info.etag()),
        iso8601(info.mtime_ms)
    ));
    for (name, version) in versions {
        add_header(&mut response, name, version);
    }
    let customer_key = info.attrs.customer_key.as_ref();
    crate::sse::add_header(
        &mut response,
        state.storage.is_encrypted() && customer_key.is_none(),
    );
    crate::ssec::add_echo(&mut response, customer_key);
    Ok(response)
}

fn too_large_source() -> S3Error {
    S3Error::new(
        400,
        "InvalidRequest",
        format!(
            "The specified copy source is larger than the maximum allowable size for a copy source: {MAX_PUT_BYTES}"
        ),
    )
}

/// `x-amz-copy-source-range: bytes=first-last`, inside a source of `size`:
/// `(first, length)`.
fn copy_range(value: &str, size: u64) -> Result<(u64, u64), S3Error> {
    let parsed = value
        .trim()
        .strip_prefix("bytes=")
        .and_then(|spec| spec.split_once('-'))
        .and_then(|(first, last)| {
            Some((
                first.trim().parse::<u64>().ok()?,
                last.trim().parse::<u64>().ok()?,
            ))
        });
    match parsed {
        Some((first, last)) if first <= last && last < size => Ok((first, last - first + 1)),
        _ => Err(S3Error::new(
            400,
            "InvalidArgument",
            format!("Range specified is not valid for source object of size: {size}"),
        )),
    }
}

/// What the lookup of an UploadPartCopy decided.
enum PartSource {
    /// The part shares the source's body: done.
    Shared(BodyInfo),
    /// The source body is leased; copy `len` bytes from `first`.
    Leased { body: u64, first: u64, len: u64 },
}

pub async fn upload_part_copy(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let headers = request.headers().clone();
    let params = query_params(request.uri().query().unwrap_or(""));
    let upload = upload_id(&params)?;
    let number = part_number(&params, &bucket, &key)?;
    let class = check_upload(state, upload, &bucket, &key)
        .await?
        .storage_class;
    let (src_bucket, src_key, which) = copy_source(&headers)?;
    let keys = copy_keys(state, &headers, &bucket).await?;
    // The copy runs in its own task: a client that disconnects drops this
    // handler, and the task still releases the lease it takes on the source.
    let task_state = S3State {
        engine: state.engine.clone(),
        region: state.region.clone(),
        signing_keys: state.signing_keys.clone(),
        storage: state.storage.clone(),
        restore_delays: state.restore_delays,
        domains: state.domains.clone(),
        request_metrics: state.request_metrics.clone(),
        metric_recorder: state.metric_recorder.clone(),
        metrics_flush: state.metrics_flush,
        access_logger: state.access_logger.clone(),
        access_log_flush: state.access_log_flush,
        activity: state.activity.clone(),
        activity_flush: state.activity_flush,
    };
    let scope = crate::scope::current().ok_or_else(|| S3Error::internal("no request scope"))?;
    let part = tokio::spawn(crate::scope::within(scope, async move {
        let state = &task_state;
        copy_part(
            state,
            upload,
            number,
            (src_bucket, src_key, which),
            headers,
            keys,
            (bucket, key),
            class,
        )
        .await
    }))
    .await
    .map_err(|err| S3Error::internal(err.to_string()))??;
    let (part, source_version, customer_key) = part;
    let etag: String = part.md5.iter().map(|b| format!("{b:02x}")).collect();
    let mut response = xml(format!(
        "<CopyPartResult xmlns=\"{XMLNS}\"><ETag>&quot;{etag}&quot;</ETag><LastModified>{}</LastModified></CopyPartResult>",
        iso8601(now_ms())
    ));
    if let Some(version) = source_version {
        add_header(&mut response, "x-amz-copy-source-version-id", version);
    }
    crate::sse::add_header(
        &mut response,
        state.storage.is_encrypted() && customer_key.is_none(),
    );
    crate::ssec::add_echo(&mut response, customer_key.as_ref());
    Ok(response)
}

/// Stores part `number` of `upload` from the copy source: shared, or with its
/// bytes copied (decrypted and encrypted again as the keys say) under a lease
/// released before returning. Returns the part, the source's
/// `x-amz-copy-source-version-id` and the upload's customer-provided key.
#[allow(clippy::too_many_arguments)]
async fn copy_part(
    state: &S3State,
    upload: u64,
    number: u32,
    (src_bucket, src_key, which): (String, String, VersionSel),
    headers: HeaderMap,
    (source_key, target_key): (Option<Arc<ProvidedKey>>, Option<Arc<ProvidedKey>>),
    (bucket, key): (String, String),
    class: StorageClass,
) -> Result<
    (
        BodyInfo,
        Option<String>,
        Option<truthdb_object::CustomerKey>,
    ),
    S3Error,
> {
    let (part_bucket, part_key) = (bucket.clone(), key.clone());
    let (lookup_source_key, lookup_target_key) = (source_key.clone(), target_key.clone());
    let (step, source_version, customer_key) = object(state, move |file| {
        // The part is encrypted with the upload's key, which the request
        // names (or none for an upload without).
        let customer_key = file.upload_attrs(upload)?.customer_key;
        if let Err(err) =
            crate::ssec::check_read(customer_key.as_ref(), lookup_target_key.as_deref())
        {
            return Ok(Err(err));
        }
        let source = match resolve_source(
            file,
            &src_bucket,
            &src_key,
            which,
            &headers,
            lookup_source_key.as_deref(),
        )? {
            Ok(source) => source,
            Err(err) => return Ok(Err(err)),
        };
        let source_version = version_header(
            versioning_of(file, &src_bucket)?,
            source.version,
            source.null_version,
        );
        let span = match header(&headers, "x-amz-copy-source-range") {
            Some(range) => match copy_range(&range, source.size) {
                Ok(span) => Some(span),
                Err(err) => return Ok(Err(err)),
            },
            None => None,
        };
        let plain = source.attrs.customer_key.is_none() && customer_key.is_none();
        if span.is_none() && source.parts == 0 && plain {
            return Ok(file
                .transaction(|tx| {
                    tx.upload_part_shared(upload, number, source.body, now_ms(), DROP_GRACE_MS)
                })?
                .map(|part| (PartSource::Shared(part), source_version, customer_key))
                .map_err(|err| upload_error(err, &part_bucket, &part_key)));
        }
        let (first, len) = span.unwrap_or((0, source.size));
        if len > MAX_PUT_BYTES {
            return Ok(Err(too_large_source()));
        }
        // The lease keeps the bytes while they are copied outside the lock.
        file.take_lease(source.body)?;
        Ok(Ok((
            PartSource::Leased {
                body: source.body,
                first,
                len,
            },
            source_version,
            customer_key,
        )))
    })
    .await??;
    let part = match step {
        PartSource::Shared(part) => part,
        PartSource::Leased { body, first, len } => {
            let copied = copy_into(
                state,
                body,
                (first, len),
                source_key,
                target_key.as_deref(),
                class,
            )
            .await;
            // Unchecked: the lease goes even when the request's scope no
            // longer holds (a key dropped mid-copy), or the body stays pinned.
            let _ = state
                .engine
                .run_object(move |file| {
                    file.release_lease(body);
                    Ok(())
                })
                .await;
            let sealed = copied?;
            object(state, move |file| {
                file.transaction(|tx| {
                    tx.upload_part(upload, number, &sealed, now_ms(), DROP_GRACE_MS)
                })
            })
            .await?
            .map_err(|err| upload_error(err, &bucket, &key))?
        }
    };
    Ok((part, source_version, customer_key))
}

/// Copies plaintext bytes `[first, first + len)` of the leased `body`
/// (decrypted with `source_key` when it has one) into a new staging body
/// (encrypted with `target_key` when given) and returns it ready to seal. A
/// body left behind by a failure stays staging and garbage collection
/// reclaims it.
async fn copy_into(
    state: &S3State,
    body: u64,
    (first, len): (u64, u64),
    source_key: Option<Arc<ProvidedKey>>,
    target_key: Option<&ProvidedKey>,
    class: StorageClass,
) -> Result<SealedBody, S3Error> {
    let layout = object(state, move |file| file.body_layout(body)).await?;
    let (tx, mut rx) = mpsc::channel(4);
    let engine = state.engine.clone();
    let reader = tokio::spawn(async move {
        crate::objects::stream_plaintext(&engine, &layout, source_key.as_deref(), first, len, &tx)
            .await
    });
    let mut sink = BodySink::begin(state, target_key, class).await?;
    while let Some(chunk) = rx.recv().await {
        sink.push(state, &chunk).await?;
    }
    let complete = reader
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?;
    if !complete || sink.pushed() != len {
        return Err(S3Error::internal(format!(
            "the copy source (body {body}) could not be read whole"
        )));
    }
    Ok(sink.finish(state).await?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_sources_and_ranges_parse_like_s3() {
        let mut headers = HeaderMap::new();
        let mut source = |value: &'static str| {
            headers.insert("x-amz-copy-source", value.parse().unwrap());
            copy_source(&headers).map_err(|err| err.code)
        };
        assert_eq!(
            source("/bkt/a%20b/c?versionId=7"),
            Ok(("bkt".to_string(), "a b/c".to_string(), VersionSel::Seq(7)))
        );
        assert_eq!(
            source("bkt/k%3Fq"),
            Ok(("bkt".to_string(), "k?q".to_string(), VersionSel::Latest))
        );
        assert_eq!(source("bkt"), Err("InvalidArgument"));
        assert_eq!(source("/bkt/"), Err("InvalidArgument"));

        assert_eq!(copy_range("bytes=0-9", 10).unwrap(), (0, 10));
        assert_eq!(copy_range("bytes=5-5", 10).unwrap(), (5, 1));
        for bad in ["bytes=0-10", "bytes=6-5", "bytes=-5", "bytes=3-", "0-1"] {
            assert!(copy_range(bad, 10).is_err(), "{bad}");
        }
    }
}
