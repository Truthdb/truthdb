//! GetObjectAttributes (`GET /<key>?attributes`): the attributes of an
//! object that `x-amz-object-attributes` names (ETag, Checksum,
//! ObjectParts, StorageClass, ObjectSize), without its body.

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use truthdb_object::StorageClass;

use crate::error::{S3Error, escape};
use crate::objects::{
    header, http_date, object, object_error, precondition, version_header, version_sel,
    versioning_of,
};
use crate::server::{Body, S3State, full};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Parts listed at most, and when `x-amz-max-parts` asks for none (as
/// ListParts lists them).
const MAX_PARTS: u32 = 1000;

/// The attributes `x-amz-object-attributes` asks for.
#[derive(Default)]
struct Asked {
    etag: bool,
    checksum: bool,
    parts: bool,
    class: bool,
    size: bool,
}

fn asked(value: Option<String>) -> Result<Asked, S3Error> {
    let invalid = |text: &str| S3Error::new(400, "InvalidArgument", text);
    let value = value.ok_or_else(|| invalid("x-amz-object-attributes is required"))?;
    let mut asked = Asked::default();
    for name in value.split(',').map(str::trim) {
        let field = match name {
            "ETag" => &mut asked.etag,
            "Checksum" => &mut asked.checksum,
            "ObjectParts" => &mut asked.parts,
            "StorageClass" => &mut asked.class,
            "ObjectSize" => &mut asked.size,
            _ => return Err(invalid("Invalid attribute name specified.")),
        };
        *field = true;
    }
    Ok(asked)
}

/// A part listed: its place in the object (from 1), its size, and its
/// checksum, if it kept one.
type Part = (u32, u64, Option<truthdb_object::StoredChecksum>);

pub async fn get_object_attributes(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let headers = request.headers().clone();
    crate::sse::check_read(&headers)?;
    let customer = crate::ssec::provided(&headers, false)?;
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let asked = asked(header(&headers, "x-amz-object-attributes"))?;
    let after: u32 = header(&headers, "x-amz-part-number-marker")
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let max: u32 = header(&headers, "x-amz-max-parts")
        .and_then(|n| n.parse().ok())
        .unwrap_or(MAX_PARTS)
        .min(MAX_PARTS);
    let list_parts = asked.parts;
    let (lookup_bucket, lookup_key) = (bucket.clone(), key.clone());
    let (found, versioning, mut parts) = object(state, move |file| {
        let found = file.object_version(&lookup_bucket, &lookup_key, which)?;
        let versioning = versioning_of(file, &lookup_bucket)?;
        // Its parts after the marker, with their checksums: listed only for
        // a version with a checksum (none were kept for its parts without).
        let mut parts: Vec<Part> = Vec::new();
        if let Ok(info) = &found
            && list_parts
            && !info.delete_marker
            && info.parts > 0
            && info.attrs.checksum.is_some()
        {
            let sizes = crate::objects::part_sizes(file, info)?;
            for (index, size) in sizes.iter().enumerate().skip(after as usize) {
                // One more than asked for tells whether the list is cut.
                if parts.len() > max as usize {
                    break;
                }
                let place = index as u32 + 1;
                let checksum =
                    file.part_checksum(&lookup_bucket, &lookup_key, info.version, place as u16)?;
                parts.push((place, *size, checksum));
            }
        }
        Ok((found, versioning, parts))
    })
    .await?;
    let info = found.map_err(|err| object_error(err, &bucket, &key))?;
    if info.delete_marker {
        return Err(crate::objects::delete_marker_error(
            which, &info, &bucket, &key,
        ));
    }
    crate::ssec::check_read(info.attrs.customer_key.as_ref(), customer.as_ref())?;
    let mut builder = Response::builder().header("last-modified", http_date(info.mtime_ms));
    if let Some(version) = version_header(versioning, info.version, info.null_version) {
        builder = builder.header("x-amz-version-id", version);
    }
    match precondition(&headers, "", &info) {
        Some(StatusCode::PRECONDITION_FAILED) => {
            return Err(S3Error::new(
                412,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold",
            ));
        }
        Some(status) => {
            return Ok(builder
                .status(status)
                .header("etag", info.etag())
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"));
        }
        None => {}
    }
    let mut body = format!("<GetObjectAttributesResponse xmlns=\"{XMLNS}\">");
    if asked.etag {
        // Unquoted, as S3 gives it here (HeadObject's is quoted).
        body.push_str(&format!(
            "<ETag>{}</ETag>",
            escape(info.etag().trim_matches('"'))
        ));
    }
    if asked.checksum
        && let Some(checksum) = &info.attrs.checksum
    {
        body.push_str(&format!(
            "<Checksum><Checksum{0}>{1}</Checksum{0}><ChecksumType>{2}</ChecksumType></Checksum>",
            checksum.algorithm,
            escape(&checksum.value),
            if checksum.full_object {
                "FULL_OBJECT"
            } else {
                "COMPOSITE"
            }
        ));
    }
    // A multipart object's parts: their count, and the parts themselves
    // when it has a checksum.
    if asked.parts && info.parts > 0 {
        let truncated = parts.len() > max as usize;
        parts.truncate(max as usize);
        body.push_str(&format!(
            "<ObjectParts><IsTruncated>{truncated}</IsTruncated><MaxParts>{max}</MaxParts>"
        ));
        if let Some((last, _, _)) = parts.last() {
            body.push_str(&format!(
                "<NextPartNumberMarker>{last}</NextPartNumberMarker>"
            ));
        }
        body.push_str(&format!("<PartNumberMarker>{after}</PartNumberMarker>"));
        for (place, size, checksum) in &parts {
            body.push_str("<Part>");
            if let Some(checksum) = checksum {
                body.push_str(&format!(
                    "<Checksum{0}>{1}</Checksum{0}>",
                    checksum.algorithm,
                    escape(&checksum.value)
                ));
            }
            body.push_str(&format!(
                "<PartNumber>{place}</PartNumber><Size>{size}</Size></Part>"
            ));
        }
        body.push_str(&format!(
            "<PartsCount>{}</PartsCount></ObjectParts>",
            info.parts
        ));
    }
    // STANDARD is not said, as S3 does not.
    if asked.class && info.storage_class != StorageClass::Standard {
        body.push_str(&format!(
            "<StorageClass>{}</StorageClass>",
            info.storage_class.as_str()
        ));
    }
    if asked.size {
        body.push_str(&format!("<ObjectSize>{}</ObjectSize>", info.size));
    }
    body.push_str("</GetObjectAttributesResponse>");
    Ok(builder
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response"))
}
