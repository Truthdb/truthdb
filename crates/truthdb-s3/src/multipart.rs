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
use crate::objects::{DROP_GRACE_MS, attrs_from, now_ms, object, object_error, receive_body};
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

pub async fn create_upload(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let attrs = attrs_from(request.headers())?;
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    let upload = object(state, move |file| {
        file.transaction(|tx| tx.create_upload(&bucket_name, &key_name, &attrs, now_ms()))
    })
    .await?
    .map_err(|err| object_error(err, &bucket, &key))?;
    Ok(xml(format!(
        "<InitiateMultipartUploadResult xmlns=\"{XMLNS}\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{upload}</UploadId></InitiateMultipartUploadResult>",
        escape(&bucket),
        escape(&key)
    )))
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
    check_upload(state, upload, &bucket, &key).await?;
    let sealed = receive_body(state, request, auth).await?;
    let info = object(state, move |file| {
        file.transaction(|tx| tx.upload_part(upload, number, &sealed, now_ms(), DROP_GRACE_MS))
    })
    .await?
    .map_err(|err| upload_error(err, &bucket, &key))?;
    let etag: String = info.md5.iter().map(|b| format!("{b:02x}")).collect();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("etag", format!("\"{etag}\""))
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
    check_upload(state, upload, &bucket, &key).await?;
    let after: u32 = param(&params, "part-number-marker")
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let max: usize = param(&params, "max-parts")
        .and_then(|n| n.parse().ok())
        .unwrap_or(MAX_LISTED)
        .min(MAX_LISTED);
    let (parts, truncated) = object(state, move |file| file.list_parts(upload, after, max)).await?;
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
    for part in &parts {
        body.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size></Part>",
            part.number,
            crate::time::iso8601(part.mtime_ms),
            escape(&part.etag()),
            part.size
        ));
    }
    body.push_str("</ListPartsResult>");
    Ok(xml(body))
}

/// The `(PartNumber, ETag)` list of a CompleteMultipartUpload body.
fn parse_complete(body: &[u8]) -> Result<Vec<(u32, [u8; 16])>, S3Error> {
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        )
    };
    let mut reader = Reader::from_reader(body);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let (mut number, mut etag): (Option<u32>, Option<String>) = (None, None);
    let mut parts = Vec::new();
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(|_| malformed())? {
            Event::Start(element) => path.push(element.local_name().as_ref().to_vec()),
            Event::End(element) => {
                if element.local_name().as_ref() == b"Part" {
                    let (Some(n), Some(tag)) = (number.take(), etag.take()) else {
                        return Err(malformed());
                    };
                    let hex = tag.trim().trim_matches('"');
                    let md5: Vec<u8> = (0..hex.len())
                        .step_by(2)
                        .filter_map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
                        .collect();
                    let md5: [u8; 16] = md5
                        .try_into()
                        .map_err(|_| upload_error(UploadError::InvalidPart(n), "", ""))?;
                    parts.push((n, md5));
                }
                path.pop();
            }
            Event::Text(text) => {
                let text = text.unescape().map_err(|_| malformed())?;
                match path.last().map(Vec::as_slice) {
                    Some(b"PartNumber") => {
                        number = Some(text.trim().parse().map_err(|_| malformed())?)
                    }
                    Some(b"ETag") => etag = Some(text.into_owned()),
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    if parts.is_empty() {
        return Err(malformed());
    }
    Ok(parts)
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
    let body = read_body(request, auth, MAX_COMPLETE_BODY).await?;
    let listed = parse_complete(&body)?;
    let info = object(state, move |file| {
        file.transaction(|tx| tx.complete_upload(upload, &listed, now_ms(), DROP_GRACE_MS))
    })
    .await?
    .map_err(|err| upload_error(err, &bucket, &key))?;
    Ok(xml(format!(
        "<CompleteMultipartUploadResult xmlns=\"{XMLNS}\"><Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
        escape(&bucket),
        escape(&key),
        escape(&bucket),
        escape(&key),
        escape(&info.etag())
    )))
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
            "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated><StorageClass>STANDARD</StorageClass></Upload>",
            escape(&upload.key),
            upload.upload,
            crate::time::iso8601(upload.initiated_ms)
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
        let parts = parse_complete(
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
    }
}
