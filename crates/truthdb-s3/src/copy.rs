//! Copies. CopyObject shares the source's body, so no bytes are copied.
//! UploadPartCopy shares a whole plain source as the part; a range, or a
//! multipart source, has its bytes copied into a new part body (a multipart
//! object's body cannot be a part: composites do not nest).

use hyper::body::Incoming;
use hyper::header::HeaderMap;
use hyper::{Request, Response};
use md5::{Digest, Md5};
use truthdb_core::storage::StorageError;
use truthdb_object::{BodyInfo, ObjFile, ObjectInfo, SEGMENT_BYTES, SealedBody, VersionSel};

use crate::error::{S3Error, escape};
use crate::listing::query_params;
use crate::multipart::{check_upload, part_number, upload_error, upload_id, xml};
use crate::objects::{
    DROP_GRACE_MS, MAX_PUT_BYTES, add_header, header, metadata_from, now_ms, object, object_error,
    precondition, version_header, version_sel, versioning_of,
};
use crate::server::Body;
use crate::server::S3State;
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

pub async fn copy_object(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let headers = request.headers().clone();
    crate::sse::check_write(&headers, state.storage.is_encrypted())?;
    let (src_bucket, src_key, which) = copy_source(&headers)?;
    let replace = replaces(&headers, "x-amz-metadata-directive", "metadata")?;
    let replace_tags = replaces(&headers, "x-amz-tagging-directive", "tagging")?;
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
    let (dst_bucket, dst_key) = (bucket, key);
    // Resolve and copy in one lock hold: the source cannot change between.
    let info = object(state, move |file| {
        let source = match resolve_source(file, &src_bucket, &src_key, which, &headers)? {
            Ok(source) => source,
            Err(err) => return Ok(Err(err)),
        };
        if !replace && (src_bucket.as_str(), src_key.as_str()) == (&dst_bucket, &dst_key) {
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
        attrs.tags = if replace_tags {
            new_tags
        } else {
            source.attrs.tags.clone()
        };
        let copied = file.transaction(|tx| {
            tx.copy_object(
                &dst_bucket,
                &dst_key,
                &source,
                &attrs,
                now_ms(),
                DROP_GRACE_MS,
            )
        })?;
        let copied = match copied {
            Ok(copied) => copied,
            Err(err) => return Ok(Err(object_error(err, &dst_bucket, &dst_key))),
        };
        let mut versions = Vec::new();
        if let Some(version) = version_header(
            versioning_of(file, &src_bucket)?,
            source.version,
            source.null_version,
        ) {
            versions.push(("x-amz-copy-source-version-id", version));
        }
        if let Some(version) = version_header(
            versioning_of(file, &dst_bucket)?,
            copied.version,
            copied.null_version,
        ) {
            versions.push(("x-amz-version-id", version));
        }
        Ok(Ok((copied, versions)))
    })
    .await??;
    let (info, versions) = info;
    let mut response = xml(format!(
        "<CopyObjectResult xmlns=\"{XMLNS}\"><ETag>{}</ETag><LastModified>{}</LastModified></CopyObjectResult>",
        escape(&info.etag()),
        iso8601(info.mtime_ms)
    ));
    for (name, version) in versions {
        add_header(&mut response, name, version);
    }
    crate::sse::add_header(&mut response, state.storage.is_encrypted());
    Ok(response)
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
    check_upload(state, upload, &bucket, &key).await?;
    let (src_bucket, src_key, which) = copy_source(&headers)?;
    // The copy runs in its own task: a client that disconnects drops this
    // handler, and the task still releases the lease it takes on the source.
    let task_state = S3State {
        engine: state.engine.clone(),
        region: state.region.clone(),
        signing_keys: state.signing_keys.clone(),
        storage: state.storage.clone(),
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
            bucket,
            key,
        )
        .await
    }))
    .await
    .map_err(|err| S3Error::internal(err.to_string()))??;
    let (part, source_version) = part;
    let etag: String = part.md5.iter().map(|b| format!("{b:02x}")).collect();
    let mut response = xml(format!(
        "<CopyPartResult xmlns=\"{XMLNS}\"><ETag>&quot;{etag}&quot;</ETag><LastModified>{}</LastModified></CopyPartResult>",
        iso8601(now_ms())
    ));
    if let Some(version) = source_version {
        add_header(&mut response, "x-amz-copy-source-version-id", version);
    }
    crate::sse::add_header(&mut response, state.storage.is_encrypted());
    Ok(response)
}

/// Stores part `number` of `upload` from the copy source: shared, or with its
/// bytes copied under a lease released before returning. Returns the part and
/// the source's `x-amz-copy-source-version-id`.
async fn copy_part(
    state: &S3State,
    upload: u64,
    number: u32,
    (src_bucket, src_key, which): (String, String, VersionSel),
    headers: HeaderMap,
    bucket: String,
    key: String,
) -> Result<(BodyInfo, Option<String>), S3Error> {
    let (part_bucket, part_key) = (bucket.clone(), key.clone());
    let (step, source_version) = object(state, move |file| {
        let source = match resolve_source(file, &src_bucket, &src_key, which, &headers)? {
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
        if span.is_none() && source.parts == 0 {
            return Ok(file
                .transaction(|tx| {
                    tx.upload_part_shared(upload, number, source.body, now_ms(), DROP_GRACE_MS)
                })?
                .map(|part| (PartSource::Shared(part), source_version))
                .map_err(|err| upload_error(err, &part_bucket, &part_key)));
        }
        let (first, len) = span.unwrap_or((0, source.size));
        if len > MAX_PUT_BYTES {
            return Ok(Err(S3Error::new(
                400,
                "InvalidRequest",
                format!(
                    "The specified copy source is larger than the maximum allowable size for a copy source: {MAX_PUT_BYTES}"
                ),
            )));
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
        )))
    })
    .await??;
    let part = match step {
        PartSource::Shared(part) => part,
        PartSource::Leased { body, first, len } => {
            let copied = copy_bytes(state, body, first, len).await;
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
    Ok((part, source_version))
}

/// Copies `len` bytes of the leased `body` from `first` into a new staging
/// body, segment by segment, and returns it ready to seal. A body left behind
/// by a failure stays staging and garbage collection reclaims it.
async fn copy_bytes(
    state: &S3State,
    body: u64,
    first: u64,
    len: u64,
) -> Result<SealedBody, S3Error> {
    // The source as plain parts (one, or a multipart object's parts),
    // resolved once: each read then goes straight to the part holding it.
    let layout = object(state, move |file| file.body_layout(body)).await?;
    let now = now_ms();
    let copy = object(state, move |file| file.begin_body(now)).await?;
    let mut md5 = Md5::new();
    let mut segment = Vec::with_capacity(SEGMENT_BYTES);
    let (mut offset, end) = (first, first + len);
    while offset < end {
        let &(part, part_start, part_len) = layout
            .iter()
            .find(|(_, start, len)| offset >= *start && offset < start + len)
            .ok_or_else(|| S3Error::internal(format!("offset {offset} is outside body {body}")))?;
        let take = (end.min(part_start + part_len) - offset)
            .min((SEGMENT_BYTES - segment.len()) as u64) as usize;
        let at = offset - part_start;
        let chunk = object(state, move |file| {
            let mut out = vec![0u8; take];
            file.read_body(part, at, &mut out)?;
            Ok(out)
        })
        .await?;
        md5.update(&chunk);
        segment.extend_from_slice(&chunk);
        offset += take as u64;
        if segment.len() == SEGMENT_BYTES || offset == end {
            let full = std::mem::replace(&mut segment, Vec::with_capacity(SEGMENT_BYTES));
            let now = now_ms();
            object(state, move |file| file.append_segment(copy, &full, now)).await?;
        }
    }
    Ok(SealedBody {
        body: copy,
        md5: md5.finalize().into(),
    })
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
