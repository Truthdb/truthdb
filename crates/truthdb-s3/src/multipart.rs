//! Multipart uploads: CreateMultipartUpload, UploadPart, ListParts,
//! CompleteMultipartUpload, AbortMultipartUpload, ListMultipartUploads.

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_object::{UploadError, UploadInfo};

use crate::error::{S3Error, escape};
use crate::listing::query_params;
use crate::objects::{
    DROP_GRACE_MS, add_header, attrs_from, now_ms, object, object_error, receive_body,
    version_header, versioning_of,
};
use crate::server::{Authenticated, Body, S3State, full, read_body};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const MAX_LISTED: usize = 1000;
/// Largest CompleteMultipartUpload request body read (10,000 parts).
const MAX_COMPLETE_BODY: usize = 2 * 1024 * 1024;

pub(crate) fn xml(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

pub(crate) fn upload_error(err: UploadError, bucket: &str, key: &str) -> S3Error {
    match err {
        UploadError::Object(err) => object_error(err, bucket, key),
        UploadError::NoSuchUpload => S3Error::new(
            404,
            "NoSuchUpload",
            "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
        ),
        UploadError::InvalidPartNumber => S3Error::new(
            400,
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000, inclusive",
        ),
        UploadError::InvalidPart(number) => S3Error::new(
            400,
            "InvalidPart",
            format!("Part {number} could not be found, or its ETag does not match."),
        ),
        UploadError::InvalidPartOrder => S3Error::new(
            400,
            "InvalidPartOrder",
            "The list of parts was not in ascending order.",
        ),
        UploadError::EntityTooSmall(number) => S3Error::new(
            400,
            "EntityTooSmall",
            format!("Part {number} is smaller than the minimum allowed size of 5 MiB."),
        ),
        UploadError::EntityTooLarge => S3Error::new(
            400,
            "EntityTooLarge",
            "Your proposed upload exceeds the maximum allowed object size.",
        ),
    }
}

fn param(params: &[(String, String)], name: &str) -> Option<String> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

pub(crate) fn upload_id(params: &[(String, String)]) -> Result<u64, S3Error> {
    param(params, "uploadId")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| upload_error(UploadError::NoSuchUpload, "", ""))
}

/// `?partNumber=`, within S3's limits.
pub(crate) fn part_number(
    params: &[(String, String)],
    bucket: &str,
    key: &str,
) -> Result<u32, S3Error> {
    param(params, "partNumber")
        .and_then(|n| n.parse().ok())
        .filter(|n| (1..=truthdb_object::MAX_PART_NUMBER).contains(n))
        .ok_or_else(|| upload_error(UploadError::InvalidPartNumber, bucket, key))
}

/// The upload, which must belong to this bucket and key.
pub(crate) async fn check_upload(
    state: &S3State,
    upload: u64,
    bucket: &str,
    key: &str,
) -> Result<UploadInfo, S3Error> {
    let found = object(state, move |file| file.upload(upload)).await?;
    match found {
        Some(info) if info.bucket == bucket && info.key == key => Ok(info),
        _ => Err(upload_error(UploadError::NoSuchUpload, bucket, key)),
    }
}

/// CreateMultipartUpload. `lock` is the Object Lock its headers ask for
/// (authorized by the caller), which the object gets on completion.
pub async fn create_upload(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
    lock: Option<truthdb_object::ObjectLock>,
) -> Result<Response<Body>, S3Error> {
    let mut attrs = attrs_from(request.headers())?;
    let class = crate::objects::storage_class_from(request.headers())?;
    let encrypted_at_rest = state.storage.is_encrypted();
    crate::sse::check_write(request.headers(), encrypted_at_rest)?;
    // Every part is encrypted with the key given here (and only with it).
    let customer = crate::ssec::provided(request.headers(), false)?;
    if customer.is_some() {
        let name = bucket.clone();
        let blocked = object(state, move |file| crate::sse::sse_c_blocked(file, &name)).await?;
        crate::ssec::check_write(customer.as_ref(), request.headers(), blocked)?;
    }
    attrs.customer_key = customer.as_ref().map(|key| key.stored()).transpose()?;
    // The checksum it asks for, kept with it (its value made on completion).
    attrs.checksum = crate::part_checksums::of_upload(request.headers())?;
    let asked = attrs.checksum.clone();
    let customer_key = attrs.customer_key.clone();
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    let upload = object(state, move |file| {
        let (bucket_lock, _) = crate::object_lock::bucket_state(file, &bucket_name)?;
        if lock.is_some() && !bucket_lock && file.bucket(&bucket_name)?.is_some() {
            return Ok(Err(None));
        }
        file.transaction(|tx| {
            let upload = tx.create_upload(&bucket_name, &key_name, &attrs, class, now_ms())?;
            if let (Ok(upload), Some(lock)) = (&upload, &lock) {
                tx.set_upload_lock(*upload, lock)?;
            }
            Ok(upload.map_err(Some))
        })
    })
    .await?
    .map_err(|err| match err {
        Some(err) => object_error(err, &bucket, &key),
        None => S3Error::invalid_request("Bucket is missing Object Lock Configuration"),
    })?;
    let mut response = xml(format!(
        "<InitiateMultipartUploadResult xmlns=\"{XMLNS}\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{upload}</UploadId></InitiateMultipartUploadResult>",
        escape(&bucket),
        escape(&key)
    ));
    crate::sse::add_header(&mut response, encrypted_at_rest && customer_key.is_none());
    crate::ssec::add_echo(&mut response, customer_key.as_ref());
    if let Some(asked) = asked {
        add_header(&mut response, "x-amz-checksum-algorithm", asked.algorithm);
        add_header(
            &mut response,
            "x-amz-checksum-type",
            if asked.full_object {
                "FULL_OBJECT"
            } else {
                "COMPOSITE"
            }
            .to_string(),
        );
    }
    Ok(response)
}

pub async fn upload_part(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let params = query_params(request.uri().query().unwrap_or(""));
    let upload = upload_id(&params)?;
    let number = part_number(&params, &bucket, &key)?;
    let class = check_upload(state, upload, &bucket, &key)
        .await?
        .storage_class;
    // A part of an upload with a customer-provided key comes with that key;
    // one of an upload without, with none.
    let customer = crate::ssec::provided(request.headers(), false)?;
    let upload_attrs = object(state, move |file| file.upload_attrs(upload)).await?;
    let customer_key = upload_attrs.customer_key;
    // The algorithm the upload asked for, if any: then every part's. One
    // that asked for none takes any (CRC64NVME computed when a part has none).
    let algorithm = upload_attrs.checksum.map(|asked| asked.algorithm);
    crate::ssec::check_read(customer_key.as_ref(), customer.as_ref())?;
    // A bucket blocking SSE-C refuses every request that names a key, a
    // part of an upload begun before the block included (as S3).
    if customer.is_some() {
        let name = bucket.clone();
        let blocked = object(state, move |file| crate::sse::sse_c_blocked(file, &name)).await?;
        crate::ssec::check_write(customer.as_ref(), request.headers(), blocked)?;
    }
    // Its checksum, of its upload's algorithm when it asked for one,
    // verified (or computed) and kept.
    let (sealed, checksum) = receive_body(
        state,
        request,
        auth,
        customer.as_ref(),
        class,
        algorithm.as_deref(),
    )
    .await?;
    let kept = checksum.clone();
    let info = object(state, move |file| {
        file.transaction(|tx| {
            tx.upload_part(upload, number, &sealed, Some(kept), now_ms(), DROP_GRACE_MS)
        })
    })
    .await?
    .map_err(|err| upload_error(err, &bucket, &key))?;
    let etag: String = info.md5.iter().map(|b| format!("{b:02x}")).collect();
    Ok(crate::ssec::with_echo(
        crate::sse::with_header(
            crate::objects::with_checksum(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("etag", format!("\"{etag}\"")),
                &checksum,
            ),
            state.storage.is_encrypted() && customer_key.is_none(),
        ),
        customer_key.as_ref(),
    )
    .body(full(Bytes::new()))
    .expect("a valid response"))
}

pub async fn list_parts(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let params = query_params(request.uri().query().unwrap_or(""));
    let upload = upload_id(&params)?;
    let info = check_upload(state, upload, &bucket, &key).await?;
    let after: u32 = param(&params, "part-number-marker")
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let max: usize = param(&params, "max-parts")
        .and_then(|n| n.parse().ok())
        .unwrap_or(MAX_LISTED)
        .min(MAX_LISTED);
    let (parts, truncated, asked) = object(state, move |file| {
        let (parts, truncated) = file.list_parts(upload, after, max)?;
        Ok((parts, truncated, file.upload_attrs(upload)?.checksum))
    })
    .await?;
    let mut body = format!(
        "<ListPartsResult xmlns=\"{XMLNS}\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{upload}</UploadId><PartNumberMarker>{after}</PartNumberMarker><MaxParts>{max}</MaxParts><IsTruncated>{truncated}</IsTruncated>",
        escape(&bucket),
        escape(&key)
    );
    if let Some(last) = parts.last() {
        body.push_str(&format!(
            "<NextPartNumberMarker>{}</NextPartNumberMarker>",
            last.number
        ));
    }
    // The upload's checksum algorithm and type, when it asked for one.
    if let Some(asked) = &asked {
        body.push_str(&format!(
            "<ChecksumAlgorithm>{}</ChecksumAlgorithm><ChecksumType>{}</ChecksumType>",
            asked.algorithm,
            if asked.full_object {
                "FULL_OBJECT"
            } else {
                "COMPOSITE"
            }
        ));
    }
    for part in &parts {
        let checksum = part
            .checksum
            .as_ref()
            .map(|checksum| {
                format!(
                    "<Checksum{0}>{1}</Checksum{0}>",
                    checksum.algorithm,
                    escape(&checksum.value)
                )
            })
            .unwrap_or_default();
        body.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size>{checksum}</Part>",
            part.number,
            crate::time::iso8601(part.mtime_ms),
            escape(&part.etag()),
            part.size
        ));
    }
    body.push_str(&format!(
        "<StorageClass>{}</StorageClass></ListPartsResult>",
        info.storage_class.as_str()
    ));
    Ok(xml(body))
}

/// The `(PartNumber, ETag)` list of a CompleteMultipartUpload body: one
/// `<CompleteMultipartUpload>` of `<Part>`s, each with one `<PartNumber>` and
/// one `<ETag>` (the 32 hex digits of its MD5, quoted or not), and optionally
/// the part's checksum, which the part itself was verified against.
/// The `(PartNumber, ETag)` list, and the checksums the parts are listed
/// with (`(PartNumber, algorithm, value)`).
#[allow(clippy::type_complexity)]
fn parse_complete(
    body: &[u8],
) -> Result<(Vec<(u32, [u8; 16])>, Vec<(u32, String, String)>), S3Error> {
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
    let (mut number, mut etag): (Option<String>, Option<String>) = (None, None);
    let mut checksum: Option<(String, String)> = None;
    let mut parts = Vec::new();
    let mut sums = Vec::new();
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|_| malformed())?;
        // An empty element is its start and its end.
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
                (None, b"CompleteMultipartUpload") => !std::mem::replace(&mut seen_root, true),
                (Some(b"CompleteMultipartUpload"), b"Part") => {
                    (number, etag, checksum) = (None, None, None);
                    true
                }
                (Some(b"Part"), b"PartNumber") => number.replace(String::new()).is_none(),
                (Some(b"Part"), b"ETag") => etag.replace(String::new()).is_none(),
                (
                    Some(b"Part"),
                    b"ChecksumCRC32" | b"ChecksumCRC32C" | b"ChecksumCRC64NVME" | b"ChecksumSHA1"
                    | b"ChecksumSHA256",
                ) => {
                    let algorithm = String::from_utf8_lossy(&name["Checksum".len()..]).into_owned();
                    checksum.replace((algorithm, String::new())).is_none()
                }
                _ => false,
            };
            if !allowed {
                return Err(malformed());
            }
            path.push(name);
        }
        if let Some(name) = end {
            if name == b"Part" {
                let (Some(number), Some(etag)) = (number.take(), etag.take()) else {
                    return Err(malformed());
                };
                let number: u32 = number.trim().parse().map_err(|_| malformed())?;
                let hex = etag.trim().trim_matches('"');
                let md5: Option<[u8; 16]> = (hex.len() == 32)
                    .then(|| {
                        (0..16)
                            .map(|at| u8::from_str_radix(&hex[at * 2..at * 2 + 2], 16).ok())
                            .collect::<Option<Vec<u8>>>()
                    })
                    .flatten()
                    .and_then(|bytes| bytes.try_into().ok());
                let md5 =
                    md5.ok_or_else(|| upload_error(UploadError::InvalidPart(number), "", ""))?;
                parts.push((number, md5));
                if let Some((algorithm, value)) = checksum.take() {
                    sums.push((number, algorithm, value.trim().to_string()));
                }
            }
            path.pop();
        }
        let text = match &event {
            Event::Text(text) => Some(text.unescape().map_err(|_| malformed())?.into_owned()),
            Event::CData(text) => Some(
                std::str::from_utf8(text)
                    .map_err(|_| malformed())?
                    .to_string(),
            ),
            _ => None,
        };
        if let Some(text) = text {
            match path.last().map(Vec::as_slice) {
                Some(b"PartNumber") => number.get_or_insert_default().push_str(&text),
                Some(b"ETag") => etag.get_or_insert_default().push_str(&text),
                Some(
                    b"ChecksumCRC32" | b"ChecksumCRC32C" | b"ChecksumCRC64NVME" | b"ChecksumSHA1"
                    | b"ChecksumSHA256",
                ) => {
                    if let Some((_, value)) = &mut checksum {
                        value.push_str(&text);
                    }
                }
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
    if parts.is_empty() {
        return Err(malformed());
    }
    Ok((parts, sums))
}

pub async fn complete_upload(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let params = query_params(request.uri().query().unwrap_or(""));
    let upload = upload_id(&params)?;
    check_upload(state, upload, &bucket, &key).await?;
    let condition = crate::conditional::Condition::of_write(request.headers())?;
    // A checksum header is of the whole object (or, sent as S3's SDKs send
    // it, of nothing to check here), not of this request's XML; the type, of
    // the upload.
    let whole = auth.checksum.clone().and_then(|(name, value)| {
        value.map(|value| {
            (
                name.trim_start_matches("x-amz-checksum-")
                    .to_ascii_uppercase(),
                value,
            )
        })
    });
    let asked_type = crate::objects::header(request.headers(), "x-amz-checksum-type");
    let auth = Authenticated {
        checksum: None,
        ..auth
    };
    let body = read_body(request, auth, MAX_COMPLETE_BODY).await?;
    let (listed, sums) = parse_complete(&body)?;
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    let (completed, versioning) =
        crate::scope::object_as(state, "CompleteMultipartUpload", move |file| {
            let now = now_ms();
            let versioning = versioning_of(file, &bucket_name)?;
            let (_, default) = crate::object_lock::bucket_state(file, &bucket_name)?;
            // The object's checksum, of the listed parts as they were kept, in
            // the completion's lock hold.
            let asked = file.upload_attrs(upload)?.checksum;
            let mut parts = Vec::with_capacity(listed.len());
            for (number, _) in &listed {
                parts.extend(file.upload_part_info(upload, *number)?);
            }
            for (number, algorithm, value) in &sums {
                let kept = parts
                    .iter()
                    .find(|part| part.number == *number)
                    .and_then(|part| part.checksum.as_ref());
                if kept.is_some_and(|kept| kept.algorithm != *algorithm || kept.value != *value) {
                    return Ok((
                        Err(upload_error(UploadError::InvalidPart(*number), &bucket_name, &key_name)),
                        versioning,
                    ));
                }
            }
            let full_object = asked.as_ref().is_none_or(|asked| asked.full_object);
            if asked_type.as_deref().is_some_and(|asked_type| {
                (asked_type == "FULL_OBJECT") != full_object
            }) {
                return Ok((
                    Err(S3Error::bad_digest(
                        "The checksum type does not match the one the upload was created with",
                    )),
                    versioning,
                ));
            }
            let checksum = (parts.len() == listed.len())
                .then(|| crate::part_checksums::of_object(asked.as_ref(), &parts))
                .flatten();
            // A whole-object checksum sent with it is checked (one for an
            // upload that asked for none is not, as S3 neither checks nor
            // keeps it).
            if let (Some(asked), Some((algorithm, value))) = (&asked, &whole)
                && asked.full_object
                && checksum
                    .as_ref()
                    .is_none_or(|checksum| checksum.algorithm != *algorithm || checksum.value != *value)
            {
                return Ok((
                    Err(S3Error::bad_digest(format!(
                        "The x-amz-checksum-{} you specified did not match the calculated checksum.",
                        algorithm.to_ascii_lowercase()
                    ))),
                    versioning,
                ));
            }
            let completed = file.transaction(|tx| {
                // In the completion's own transaction: an upload in progress
                // is no version, so what was written meanwhile counts. One
                // that fails leaves the upload as it was.
                if let Some(condition) = &condition {
                    let current = match tx.current_etag(&bucket_name, &key_name)? {
                        Ok(current) => current.flatten(),
                        Err(err) => return Ok(Err(object_error(err, &bucket_name, &key_name))),
                    };
                    if let Err(err) = condition.check(current.as_deref()) {
                        return Ok(Err(err));
                    }
                }
                let completed =
                    tx.complete_upload(upload, &listed, checksum.clone(), now, DROP_GRACE_MS)?;
                // The object has the lock its upload asked for; without a
                // retention, the bucket's default one, from now.
                if let (Ok(info), Some(default)) = (&completed, default) {
                    let mut lock = tx.lock_of(&bucket_name, &key_name, info.version)?;
                    if lock.retention.is_none() {
                        lock.retention = Some(default.retention(now));
                        tx.set_lock(&bucket_name, &key_name, info.version, &lock)?;
                    }
                }
                Ok(Ok(completed))
            })?;
            Ok((completed, versioning))
        })
        .await?;
    let info = completed?.map_err(|err| upload_error(err, &bucket, &key))?;
    let checksum = info
        .attrs
        .checksum
        .as_ref()
        .map(|checksum| {
            format!(
                "<Checksum{0}>{1}</Checksum{0}><ChecksumType>{2}</ChecksumType>",
                checksum.algorithm,
                escape(&checksum.value),
                if checksum.full_object {
                    "FULL_OBJECT"
                } else {
                    "COMPOSITE"
                }
            )
        })
        .unwrap_or_default();
    let mut response = xml(format!(
        "<CompleteMultipartUploadResult xmlns=\"{XMLNS}\"><Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag>{checksum}</CompleteMultipartUploadResult>",
        escape(&bucket),
        escape(&key),
        escape(&bucket),
        escape(&key),
        escape(&info.etag())
    ));
    if let Some(version) = version_header(versioning, info.version, info.null_version) {
        add_header(&mut response, "x-amz-version-id", version);
    }
    // An object with a customer-provided key is not AES256 (and the key is
    // not echoed here, as S3 does not).
    crate::sse::add_header(
        &mut response,
        state.storage.is_encrypted() && info.attrs.customer_key.is_none(),
    );
    Ok(response)
}

pub async fn abort_upload(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let params = query_params(request.uri().query().unwrap_or(""));
    let upload = upload_id(&params)?;
    check_upload(state, upload, &bucket, &key).await?;
    object(state, move |file| {
        file.transaction(|tx| tx.abort_upload(upload, now_ms(), DROP_GRACE_MS))
    })
    .await?
    .map_err(|err| upload_error(err, &bucket, &key))?;
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(full(Bytes::new()))
        .expect("a valid response"))
}

pub async fn list_uploads(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    let params = query_params(request.uri().query().unwrap_or(""));
    let prefix = param(&params, "prefix").unwrap_or_default();
    let key_marker = param(&params, "key-marker").unwrap_or_default();
    let upload_marker: u64 = param(&params, "upload-id-marker")
        .and_then(|id| id.parse().ok())
        .unwrap_or(u64::MAX);
    let max: usize = param(&params, "max-uploads")
        .and_then(|n| n.parse().ok())
        .unwrap_or(MAX_LISTED)
        .min(MAX_LISTED);
    {
        let name = bucket.clone();
        if object(state, move |file| file.bucket(&name))
            .await?
            .is_none()
        {
            return Err(object_error(
                truthdb_object::ObjectError::NoSuchBucket,
                &bucket,
                "",
            ));
        }
    }
    let (bucket_name, list_prefix, marker) = (bucket.clone(), prefix.clone(), key_marker.clone());
    let (uploads, truncated) = object(state, move |file| {
        let after = (!marker.is_empty()).then_some((marker.as_str(), upload_marker));
        file.list_uploads(&bucket_name, &list_prefix, after, max)
    })
    .await?;
    let mut body = format!(
        "<ListMultipartUploadsResult xmlns=\"{XMLNS}\"><Bucket>{}</Bucket><KeyMarker>{}</KeyMarker><UploadIdMarker>{}</UploadIdMarker><Prefix>{}</Prefix><MaxUploads>{max}</MaxUploads><IsTruncated>{truncated}</IsTruncated>",
        escape(&bucket),
        escape(&key_marker),
        param(&params, "upload-id-marker")
            .map(|m| escape(&m))
            .unwrap_or_default(),
        escape(&prefix)
    );
    if truncated && let Some(last) = uploads.last() {
        body.push_str(&format!(
            "<NextKeyMarker>{}</NextKeyMarker><NextUploadIdMarker>{}</NextUploadIdMarker>",
            escape(&last.key),
            last.upload
        ));
    }
    for upload in &uploads {
        body.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated><StorageClass>{}</StorageClass></Upload>",
            escape(&upload.key),
            upload.upload,
            crate::time::iso8601(upload.initiated_ms),
            upload.storage_class.as_str()
        ));
    }
    body.push_str("</ListMultipartUploadsResult>");
    Ok(xml(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_requests_parse_part_numbers_and_etags() {
        let (parts, _) = parse_complete(
            br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"0123456789abcdef0123456789abcdef"</ETag></Part><Part><ETag>&quot;fedcba9876543210fedcba9876543210&quot;</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>"#,
        )
        .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].0, 1);
        assert_eq!(parts[0].1[0], 0x01);
        assert_eq!(parts[1].0, 2);
        assert_eq!(parts[1].1[15], 0x10);
        assert!(parse_complete(b"<CompleteMultipartUpload></CompleteMultipartUpload>").is_err());
        assert!(
            parse_complete(b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"xyz\"</ETag></Part></CompleteMultipartUpload>").is_err(),
            "an ETag that is not an MD5"
        );
        // A part's checksum may be listed, and is kept; CDATA is read.
        let (parts, sums) = parse_complete(
            b"<CompleteMultipartUpload><Part><PartNumber>3</PartNumber><ChecksumCRC32>AAAAAA==</ChecksumCRC32><ETag><![CDATA[0123456789abcdef0123456789abcdef]]></ETag></Part></CompleteMultipartUpload>",
        )
        .unwrap();
        assert_eq!(parts[0].0, 3);
        assert_eq!(sums, [(3, "CRC32".to_string(), "AAAAAA==".to_string())]);
        const PART: &str =
            "<Part><PartNumber>1</PartNumber><ETag>0123456789abcdef0123456789abcdef</ETag></Part>";
        for bad in [
            format!("<CompleteMultipartUpload>{PART}"),
            format!("<Garbage>{PART}</Garbage>"),
            format!("<CompleteMultipartUpload>{PART}</CompleteMultipartUpload><CompleteMultipartUpload>{PART}</CompleteMultipartUpload>"),
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><PartNumber>2</PartNumber><ETag>0123456789abcdef0123456789abcdef</ETag></Part></CompleteMultipartUpload>".to_string(),
            format!("<CompleteMultipartUpload><PartNumber>1</PartNumber>{PART}</CompleteMultipartUpload>"),
        ] {
            assert_eq!(
                parse_complete(bad.as_bytes()).err().map(|err| err.code),
                Some("MalformedXML"),
                "{bad}"
            );
        }
        assert!(
            parse_complete(b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>0123456789abcdef0123456789abcdefzz</ETag></Part></CompleteMultipartUpload>").is_err(),
            "trailing junk after the MD5"
        );
    }
}
