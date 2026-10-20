//! Bucket policies (Put/Get/DeleteBucketPolicy, GetBucketPolicyStatus) and
//! access control lists, which are disabled: every bucket's object ownership
//! is BucketOwnerEnforced, so the bucket owner owns every object, an ACL
//! read shows the owner's full control, and an ACL write is refused unless
//! it grants only that.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_iam::{Policy, PolicyKind};
use truthdb_object::BUCKET_POLICY;

use crate::error::S3Error;
use crate::server::{Authenticated, Body, S3State, full, read_body};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// The largest bucket policy.
const MAX_POLICY: usize = 20 * 1024;

fn no_such_bucket(bucket: &str) -> S3Error {
    S3Error::new(
        404,
        "NoSuchBucket",
        format!("The bucket {bucket} does not exist"),
    )
}

async fn bucket_owner(state: &S3State, bucket: &str) -> Result<String, S3Error> {
    let name = bucket.to_string();
    crate::scope::object(state, move |file| file.bucket(&name))
        .await?
        .map(|info| info.owner)
        .ok_or_else(|| no_such_bucket(bucket))
}

/// PUT, GET or DELETE `?policy`.
pub async fn bucket_policy(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    bucket_owner(state, &bucket).await?;
    let name = bucket.clone();
    match method {
        Method::PUT => {
            let body = read_body(request, auth, MAX_POLICY).await?;
            let text = String::from_utf8(body)
                .map_err(|_| S3Error::new(400, "MalformedPolicy", "The policy is not UTF-8"))?;
            Policy::parse(&text, PolicyKind::Bucket)
                .map_err(|err| S3Error::new(400, "MalformedPolicy", err.0))?;
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.put_bucket_config(&name, BUCKET_POLICY, "", text.as_bytes())
                })
            })
            .await?;
            Ok(empty(StatusCode::NO_CONTENT))
        }
        Method::GET => {
            let policy = crate::scope::object(state, move |file| {
                file.bucket_config(&name, BUCKET_POLICY, "")
            })
            .await?
            .ok_or_else(|| no_such_policy(&bucket))?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(full(policy))
                .expect("a valid response"))
        }
        _ => {
            crate::scope::object(state, move |file| {
                file.transaction(|tx| tx.delete_bucket_config(&name, BUCKET_POLICY, ""))
            })
            .await?;
            Ok(empty(StatusCode::NO_CONTENT))
        }
    }
}

fn no_such_policy(bucket: &str) -> S3Error {
    S3Error::new(
        404,
        "NoSuchBucketPolicy",
        format!("The bucket {bucket} has no policy"),
    )
}

/// GET `?policyStatus`: whether the bucket's policy is public (a bucket
/// without a policy is not).
pub async fn policy_status(state: &S3State, bucket: String) -> Result<Response<Body>, S3Error> {
    bucket_owner(state, &bucket).await?;
    let name = bucket.clone();
    let public = match crate::scope::object(state, move |file| {
        file.bucket_config(&name, BUCKET_POLICY, "")
    })
    .await?
    {
        Some(document) => {
            let text = String::from_utf8_lossy(&document).into_owned();
            let policy = Policy::parse(&text, PolicyKind::Bucket)
                .map_err(|err| S3Error::internal(format!("stored bucket policy: {}", err.0)))?;
            truthdb_iam::is_public(&policy)
        }
        None => false,
    };
    Ok(xml(format!(
        "<PolicyStatus xmlns=\"{XMLNS}\"><IsPublic>{}</IsPublic></PolicyStatus>",
        if public { "TRUE" } else { "FALSE" }
    )))
}

/// Refuses an ACL that grants anything but the bucket owner's full control
/// (`x-amz-acl` other than `private` or `bucket-owner-full-control`, or an
/// `x-amz-grant-*` header).
pub fn check_acl_headers(headers: &hyper::HeaderMap) -> Result<(), S3Error> {
    let canned = headers
        .get("x-amz-acl")
        .map(|value| value.to_str().unwrap_or(""));
    let grants = headers
        .keys()
        .any(|name| name.as_str().starts_with("x-amz-grant-"));
    if grants || canned.is_some_and(|acl| acl != "private" && acl != "bucket-owner-full-control") {
        return Err(acls_not_supported());
    }
    Ok(())
}

/// Whether an AccessControlPolicy body grants nothing but `owner_id`'s full
/// control (an unreadable body grants something else).
fn owner_only_acl(body: &str, owner_id: &str) -> bool {
    use quick_xml::Reader;
    use quick_xml::events::Event;
    let mut reader = Reader::from_str(body);
    let mut path: Vec<String> = Vec::new();
    let (mut grants, mut owner_grantees, mut full_controls) = (0, 0, 0);
    loop {
        let event = match reader.read_event() {
            Ok(event) => event,
            Err(_) => return false,
        };
        let name = |e: &quick_xml::events::BytesStart<'_>| {
            String::from_utf8_lossy(e.local_name().as_ref()).into_owned()
        };
        match event {
            Event::Start(e) => {
                let name = name(&e);
                if name == "Grant" {
                    grants += 1;
                }
                if path.iter().any(|p| p == "Grant") && (name == "URI" || name == "EmailAddress") {
                    return false;
                }
                path.push(name);
            }
            Event::Empty(e) => {
                let name = name(&e);
                if name == "Grant" {
                    grants += 1;
                }
                if path.iter().any(|p| p == "Grant")
                    && ["URI", "EmailAddress", "ID", "Permission"].contains(&name.as_str())
                {
                    return false;
                }
            }
            Event::End(_) => {
                path.pop();
            }
            Event::Text(text) => {
                let Ok(text) = text.unescape() else {
                    return false;
                };
                let text = text.trim();
                if path.iter().any(|p| p == "Grant") {
                    match path.last().map(String::as_str) {
                        Some("ID") if text == owner_id => owner_grantees += 1,
                        Some("Permission") if text == "FULL_CONTROL" => full_controls += 1,
                        Some("ID" | "Permission") => return false,
                        _ => {}
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    grants <= 1 && owner_grantees == grants && full_controls == grants
}

fn acls_not_supported() -> S3Error {
    S3Error::new(
        400,
        "AccessControlListNotSupported",
        "The bucket does not allow ACLs",
    )
}

/// GET or PUT `?acl` on a bucket or an object: the owner's full control.
pub async fn acl(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: Option<String>,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let owner = bucket_owner(state, &bucket).await?;
    if let Some(key) = &key {
        let which = crate::objects::version_sel(request.uri().query().unwrap_or(""))?;
        let (name, object_key) = (bucket.clone(), key.clone());
        crate::scope::object(state, move |file| {
            file.head_object(&name, &object_key, which)
        })
        .await?
        .map_err(|err| crate::objects::object_error(err, &bucket, key))?;
    }
    if method == Method::PUT {
        check_acl_headers(request.headers())?;
        let body = read_body(request, auth, 64 * 1024).await?;
        // A body may only restate the owner's full control: at most one
        // grant, to the owner's id, of FULL_CONTROL.
        let text = String::from_utf8_lossy(&body);
        let id = crate::sigv4::sha256_hex(owner.as_bytes());
        if !text.trim().is_empty() && !owner_only_acl(&text, &id) {
            return Err(acls_not_supported());
        }
        return Ok(empty(StatusCode::OK));
    }
    let id = crate::sigv4::sha256_hex(owner.as_bytes());
    Ok(xml(format!(
        "<AccessControlPolicy xmlns=\"{XMLNS}\">{}<AccessControlList><Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{id}</ID><DisplayName>{owner}</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>",
        crate::api::owner_xml(&owner)
    )))
}

/// GET or PUT `?ownershipControls`: BucketOwnerEnforced, the only setting.
pub async fn ownership_controls(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    bucket_owner(state, &bucket).await?;
    if method == Method::PUT {
        let body = read_body(request, auth, 64 * 1024).await?;
        let text = String::from_utf8_lossy(&body);
        if !text.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>")
            || text.matches("<ObjectOwnership>").count() != 1
        {
            return Err(S3Error::new(
                400,
                "InvalidRequest",
                "Only BucketOwnerEnforced object ownership is supported",
            ));
        }
        return Ok(empty(StatusCode::OK));
    }
    Ok(xml(format!(
        "<OwnershipControls xmlns=\"{XMLNS}\"><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>"
    )))
}

fn xml(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response")
}
