//! Directory buckets (S3 Express One Zone's): a bucket named
//! `<base>--<zone>--x-s3`, made with a CreateBucketConfiguration of type
//! Directory, listed by ListDirectoryBuckets, and refusing what S3 does not
//! support for one.

use base64::Engine as _;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};

use crate::api::Target;
use crate::error::{S3Error, escape};
use crate::server::{Body, Principal, S3State, full};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// The suffix only a directory bucket's name has.
pub(crate) const SUFFIX: &str = "--x-s3";

/// Directory buckets ListDirectoryBuckets lists at most, and when
/// `max-directory-buckets` asks for none (AWS states nothing for 0).
const MAX_LISTED: usize = 1000;

/// A directory bucket's ARN.
pub(crate) fn arn(region: &str, owner: &str, bucket: &str) -> String {
    format!("arn:aws:s3express:{region}:{owner}:bucket/{bucket}")
}

fn not_supported(what: &str) -> S3Error {
    S3Error::not_implemented(format!("{what} is not supported for directory buckets"))
}

fn invalid(message: &str) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// The zone a CreateBucketConfiguration makes a directory bucket in (its
/// Location's Name); none for a general purpose bucket's.
pub(crate) fn zone_asked(body: &[u8]) -> Result<Option<String>, S3Error> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let root = crate::xml::parse(body, "CreateBucketConfiguration")?;
    let (location, bucket) = (root.one("Location")?, root.one("Bucket")?);
    let (Some(location), Some(bucket)) = (location, bucket) else {
        return match location.or(bucket) {
            Some(_) => Err(S3Error::invalid_request(
                "A directory bucket's configuration has both its Location and its Bucket",
            )),
            None => Ok(None),
        };
    };
    if root.one("LocationConstraint")?.is_some() {
        return Err(not_supported("LocationConstraint"));
    }
    if root.one("Tags")?.is_some() {
        return Err(not_supported("Tags at CreateBucket"));
    }
    location.only(&["Name", "Type"])?;
    bucket.only(&["DataRedundancy", "Type"])?;
    if bucket.text_of("Type")? != Some("Directory") {
        return Err(S3Error::invalid_request("The bucket Type is Directory"));
    }
    match (location.text_of("Type")?, bucket.text_of("DataRedundancy")?) {
        (Some("AvailabilityZone"), Some("SingleAvailabilityZone")) => {}
        (Some("LocalZone"), _) => return Err(not_supported("A Local Zone")),
        _ => {
            return Err(S3Error::invalid_request(
                "A directory bucket's Location Type is AvailabilityZone, with DataRedundancy SingleAvailabilityZone",
            ));
        }
    }
    let name = location
        .text_of("Name")?
        .filter(|name| !name.is_empty())
        .ok_or_else(|| S3Error::invalid_request("The Location Name is the bucket's zone id"))?;
    Ok(Some(name.to_string()))
}

/// Headers S3 does not support for directory buckets, by name or prefix.
const UNSUPPORTED_HEADERS: [&str; 12] = [
    "x-amz-acl",
    "x-amz-grant-",
    "x-amz-tagging",
    "x-amz-website-redirect-location",
    "x-amz-object-lock-",
    "x-amz-bucket-object-lock-enabled",
    "x-amz-object-ownership",
    "x-amz-server-side-encryption-customer-",
    "x-amz-copy-source-server-side-encryption-customer-",
    "x-amz-request-payer",
    "x-amz-optional-object-attributes",
    "content-md5",
];

fn unsupported_header(request: &Request<Incoming>) -> Result<(), S3Error> {
    for name in request.headers().keys() {
        let name = name.as_str();
        if UNSUPPORTED_HEADERS.iter().any(|refused| {
            name == *refused || (refused.ends_with('-') && name.starts_with(refused))
        }) {
            return Err(not_supported(name));
        }
    }
    Ok(())
}

/// What a CreateBucket of a directory bucket may not carry.
pub(crate) fn check_create(request: &Request<Incoming>) -> Result<(), S3Error> {
    unsupported_header(request)
}

/// Refuses a request on a directory bucket that S3 does not support for
/// one: an operation, a header, a version id but `null`, a listing's
/// delimiter but `/`, a prefix not ending in it, a start-after, or an
/// upload-id-marker. Its storage class may only be EXPRESS_ONEZONE, which
/// every version and upload of the bucket is (the header is then dropped:
/// the object layer gives the class).
pub(crate) fn check(request: &mut Request<Incoming>, target: &Target) -> Result<(), S3Error> {
    let method = request.method().clone();
    let params = crate::listing::query_params(request.uri().query().unwrap_or(""));
    let param = |name: &str| {
        params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let sub = crate::api::subresource(request);
    let supported = match (target, &method, sub) {
        (Target::Bucket(_), &Method::PUT | &Method::DELETE | &Method::HEAD, None) => true,
        (Target::Bucket(_), &Method::GET, None) => param("list-type") == Some("2"),
        (Target::Bucket(_), _, Some("policy" | "encryption" | "inventory" | "lifecycle")) => true,
        (Target::Bucket(_), &Method::GET, Some("uploads" | "session")) => true,
        (Target::Bucket(_), &Method::POST, Some("delete")) => true,
        (
            Target::Object(..),
            &Method::PUT | &Method::GET | &Method::HEAD | &Method::DELETE,
            None,
        ) => true,
        (Target::Object(..), &Method::POST, Some("uploads")) => true,
        (Target::Object(..), _, Some("uploadId")) => true,
        (Target::Object(..), &Method::GET, Some("attributes")) => true,
        (Target::Object(..), &Method::PUT, Some("renameObject")) => true,
        _ => false,
    };
    if !supported {
        let what = match (target, sub) {
            (Target::Bucket(_), None) if method == Method::GET => "ListObjects (V1)".to_string(),
            (_, Some(sub)) => format!("{method} ?{sub}"),
            (_, None) => method.to_string(),
        };
        return Err(not_supported(&what));
    }
    unsupported_header(request)?;
    if param("versionId").is_some_and(|version| version != "null") {
        return Err(invalid("A directory bucket's only version id is null"));
    }
    // ListObjectsV2's and ListMultipartUploads'.
    let listing = matches!(sub, None | Some("uploads"));
    if matches!(target, Target::Bucket(_)) && method == Method::GET && listing {
        if param("delimiter").is_some_and(|delimiter| delimiter != "/") {
            return Err(invalid("A directory bucket's only delimiter is /"));
        }
        if param("prefix").is_some_and(|prefix| !prefix.is_empty() && !prefix.ends_with('/')) {
            return Err(invalid("A directory bucket's prefixes end in /"));
        }
        if param("start-after").is_some() {
            return Err(not_supported("start-after"));
        }
        if param("upload-id-marker").is_some() {
            return Err(not_supported("upload-id-marker"));
        }
    }
    let class = crate::objects::header(request.headers(), crate::objects::STORAGE_CLASS_HEADER);
    if let Some(class) = class {
        if class != "EXPRESS_ONEZONE" {
            return Err(S3Error::new(
                400,
                "InvalidStorageClass",
                "The storage class you specified is not valid",
            ));
        }
        request
            .headers_mut()
            .remove(crate::objects::STORAGE_CLASS_HEADER);
    }
    Ok(())
}

/// The headers HeadBucket adds for a directory bucket.
pub(crate) fn head_headers(
    builder: hyper::http::response::Builder,
    region: &str,
    owner: &str,
    bucket: &str,
    zone: &str,
) -> hyper::http::response::Builder {
    builder
        .header("x-amz-bucket-location-type", "AvailabilityZone")
        .header("x-amz-bucket-location-name", zone)
        .header("x-amz-bucket-arn", arn(region, owner, bucket))
        .header("x-amz-access-point-alias", "false")
}

/// ListDirectoryBuckets: the caller's account's directory buckets, by name,
/// a page of `max-directory-buckets` (at most 1,000) after the
/// continuation token (the last name listed, base64).
pub(crate) async fn list_directory_buckets(
    state: &S3State,
    principal: &Principal,
    query: &str,
) -> Result<Response<Body>, S3Error> {
    let params = crate::listing::query_params(query);
    let param = |name: &str| {
        params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let max = match param("max-directory-buckets") {
        None => MAX_LISTED,
        Some(value) => match value.parse::<usize>() {
            Ok(0) => MAX_LISTED,
            Ok(max) if max <= MAX_LISTED => max,
            _ => return Err(invalid("max-directory-buckets is from 0 to 1000")),
        },
    };
    let base64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let after = match param("continuation-token") {
        None | Some("") => None,
        Some(token) => Some(
            base64
                .decode(token)
                .ok()
                .and_then(|name| String::from_utf8(name).ok())
                .ok_or_else(|| invalid("The continuation token is not valid"))?,
        ),
    };
    let account = principal.account.clone();
    let mut buckets: Vec<_> = crate::objects::object(state, move |file| file.list_buckets())
        .await?
        .into_iter()
        .filter(|bucket| bucket.owner == account && bucket.zone.is_some())
        .filter(|bucket| after.as_ref().is_none_or(|after| bucket.name > *after))
        .collect();
    let more = buckets.len() > max;
    buckets.truncate(max);
    let mut body = format!("<ListAllMyDirectoryBucketsResult xmlns=\"{XMLNS}\"><Buckets>");
    for bucket in &buckets {
        body.push_str(&format!(
            "<Bucket><BucketArn>{}</BucketArn><BucketRegion>{}</BucketRegion><CreationDate>{}</CreationDate><Name>{}</Name></Bucket>",
            escape(&arn(&state.region, &bucket.owner, &bucket.name)),
            escape(&state.region),
            crate::time::iso8601(bucket.created_ms),
            escape(&bucket.name)
        ));
    }
    body.push_str("</Buckets>");
    if more && let Some(last) = buckets.last() {
        body.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            base64.encode(&last.name)
        ));
    }
    body.push_str("</ListAllMyDirectoryBucketsResult>");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response"))
}
