//! The S3 operations: a request is routed by method, path (`/`, `/bucket`,
//! `/bucket/key`) and sub-resource query, then run against the object engine.

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_object::{BucketError, BucketInfo, ObjFile, Versioning};

use crate::authz::Authz;
use crate::error::{S3Error, escape};
use crate::server::{Authenticated, Body, Connection, Principal, S3State, full, read_body};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// What a request path names.
pub(crate) enum Target {
    Service,
    Bucket(String),
    Object(String, String),
}

pub(crate) fn target(raw_path: &str) -> Result<Target, S3Error> {
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
/// rather than taken for the plain operation on the same path (a bucket GET
/// would list objects, an object GET would return its body).
const UNIMPLEMENTED: &[&str] = &[
    "accelerate",
    "analytics",
    "attributes",
    "cors",
    "intelligent-tiering",
    "inventory",
    "legal-hold",
    "lifecycle",
    "logging",
    "metrics",
    "notification",
    "object-lock",
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
        BucketError::AlreadyExists => S3Error::new(
            409,
            "BucketAlreadyExists",
            format!("The bucket {bucket} already exists and belongs to another account"),
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
    crate::scope::object(state, op).await
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

/// An account's owner element: its canonical user id (the SHA-256 of the
/// account id, in hex) and the account id as display name.
pub(crate) fn owner_xml(account: &str) -> String {
    format!(
        "<Owner><ID>{}</ID><DisplayName>{account}</DisplayName></Owner>",
        crate::sigv4::sha256_hex(account.as_bytes())
    )
}

/// Routes an authenticated request to its operation, authorized against
/// the policies that apply (see `authz`).
pub async fn dispatch(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
) -> Result<Response<Body>, S3Error> {
    let method = request.method().clone();
    // S3 Control's account-level Block Public Access (path-style, with the
    // account in a header). A request to an object of a bucket named
    // `v20180820` at that path, carrying that header, is taken for it: only
    // S3 Control's own host (virtual-hosted addressing) would tell them apart.
    if request.uri().path() == "/v20180820/configuration/publicAccessBlock"
        && request.headers().contains_key("x-amz-account-id")
    {
        return account_public_access(state, request, auth, method).await;
    }
    let target = target(request.uri().path())?;
    let principal = auth.principal.clone();
    let raw_query = request.uri().query().unwrap_or("").to_string();
    // A bucket being created may be another account's: that is the
    // operation's conflict, not the scope's.
    let creating =
        matches!(target, Target::Bucket(_)) && method == Method::PUT && raw_query.is_empty();
    let mut buckets = Vec::new();
    if let Target::Bucket(bucket) | Target::Object(bucket, _) = &target {
        buckets.push(bucket.clone());
    }
    // Decided once, here: whether the request is a copy (a PutObjectTagging
    // or object ACL request carrying a copy source header is not), so the
    // source authorized against is the one the route copies from.
    let copying = matches!(target, Target::Object(..))
        && method == Method::PUT
        && request.headers().contains_key("x-amz-copy-source")
        && !has_param(&request, "tagging")
        && !has_param(&request, "acl");
    let copy_source = match copying {
        true => crate::copy::copy_source(request.headers()).ok(),
        false => None,
    };
    // The objects whose current tags a policy may read: the target, when
    // the operation acts on a version of it that exists (a write makes a new
    // one), and a copy's source.
    let reads_existing = match method {
        Method::GET | Method::HEAD | Method::DELETE => true,
        Method::PUT => has_param(&request, "tagging") || has_param(&request, "acl"),
        _ => false,
    };
    let mut objects = Vec::new();
    if let Target::Object(bucket, key) = &target
        && reads_existing
    {
        objects.push((
            bucket.clone(),
            key.clone(),
            crate::objects::version_sel(&raw_query)?,
        ));
    }
    if let Some(source) = copy_source {
        buckets.push(source.0.clone());
        objects.push(source);
    }
    let connection = request.extensions().get::<Connection>().copied();
    let authz = Authz::load(
        state,
        principal.as_ref(),
        auth.signed,
        request.headers(),
        &raw_query,
        connection,
        buckets,
        objects,
    )
    .await?;
    let scope = crate::scope::Scope {
        key: principal
            .as_ref()
            .map(|principal| (principal.access_key.clone(), principal.account.clone())),
        buckets: authz
            .owners()
            .into_iter()
            .filter(|(bucket, _)| {
                !(creating && matches!(&target, Target::Bucket(created) if created == bucket))
            })
            .collect(),
    };
    crate::scope::within(scope, async move {
        // Refused before the body is read, too.
        crate::scope::object(state, |_| Ok(())).await?;
        route(
            state,
            request,
            auth,
            &authz,
            principal.as_ref(),
            target,
            method,
            copying,
        )
        .await
    })
    .await
}

/// The tags of the request's `x-amz-tagging` header, as the object will
/// store them; `None` without the header.
fn header_tags(request: &Request<Incoming>) -> Result<Option<Vec<(String, String)>>, S3Error> {
    if !request.headers().contains_key("x-amz-tagging") {
        return Ok(None);
    }
    crate::tagging::tags_from(request.headers()).map(Some)
}

/// S3 Control's Put, Get or DeletePublicAccessBlock, for the caller's own
/// account.
async fn account_public_access(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let Some(principal) = auth.principal.clone() else {
        return Err(S3Error::access_denied("Access Denied"));
    };
    let account = request
        .headers()
        .get("x-amz-account-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if account != principal.account {
        return Err(S3Error::access_denied("Access Denied"));
    }
    let action = match method {
        Method::GET => "s3:GetAccountPublicAccessBlock",
        Method::PUT | Method::DELETE => "s3:PutAccountPublicAccessBlock",
        _ => {
            return Err(S3Error::not_implemented(format!(
                "{method} account publicAccessBlock"
            )));
        }
    };
    let authz = Authz::load(
        state,
        Some(&principal),
        auth.signed,
        request.headers(),
        "",
        request.extensions().get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    authz.check(action, crate::authz::Resource::Service)?;
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
    };
    let account = principal.account.clone();
    crate::scope::within(scope, async move {
        crate::public_access::handle(state, request, auth, String::new(), account, method).await
    })
    .await
}

/// The action for an object operation, on a version when the request names
/// one (`s3:GetObject` or `s3:GetObjectVersion`).
fn versioned(
    request: &Request<Incoming>,
    plain: &'static str,
    version: &'static str,
) -> &'static str {
    if has_param(request, "versionId") {
        version
    } else {
        plain
    }
}

#[allow(clippy::too_many_arguments)]
async fn route(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    authz: &Authz,
    principal: Option<&Principal>,
    target: Target,
    method: Method,
    copying: bool,
) -> Result<Response<Body>, S3Error> {
    use crate::authz::Resource::{Bucket, Object, Service};
    // ACLs are disabled: a write may only grant the owner full control.
    if matches!(method, Method::PUT | Method::POST) {
        crate::policy::check_acl_headers(request.headers())?;
    }
    crate::sse::refuse_customer_keys(request.headers().keys().map(|name| name.as_str()))?;
    match (target, method) {
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
        (Target::Service, Method::GET) => {
            authz.check("s3:ListAllMyBuckets", Service)?;
            // An anonymous caller is denied before this.
            let principal = principal.ok_or_else(|| S3Error::access_denied("Access Denied"))?;
            list_buckets(state, principal).await
        }
        (Target::Bucket(bucket), Method::PUT) if has_param(&request, "versioning") => {
            authz.check("s3:PutBucketVersioning", Bucket(&bucket))?;
            put_versioning(state, request, auth, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "versioning") => {
            authz.check("s3:GetBucketVersioning", Bucket(&bucket))?;
            get_versioning(state, bucket).await
        }
        (Target::Bucket(bucket), method) if has_param(&request, "policy") => {
            let action = match method {
                Method::GET => "s3:GetBucketPolicy",
                Method::PUT => "s3:PutBucketPolicy",
                Method::DELETE => "s3:DeleteBucketPolicy",
                _ => return Err(S3Error::not_implemented(format!("{method} ?policy"))),
            };
            authz.check(action, Bucket(&bucket))?;
            crate::policy::bucket_policy(state, request, auth, bucket, method).await
        }
        (Target::Bucket(bucket), method) if has_param(&request, "publicAccessBlock") => {
            let action = match method {
                Method::GET => "s3:GetBucketPublicAccessBlock",
                Method::PUT | Method::DELETE => "s3:PutBucketPublicAccessBlock",
                _ => {
                    return Err(S3Error::not_implemented(format!(
                        "{method} ?publicAccessBlock"
                    )));
                }
            };
            authz.check(action, Bucket(&bucket))?;
            crate::public_access::handle(state, request, auth, bucket, String::new(), method).await
        }
        // An object has no `?encryption` of its own: refused, rather than
        // taken for the plain object operation.
        (Target::Object(..), _) if has_param(&request, "encryption") => Err(
            S3Error::not_implemented("an object has no ?encryption configuration"),
        ),
        (Target::Bucket(bucket), method) if has_param(&request, "encryption") => {
            let action = match method {
                Method::GET => "s3:GetEncryptionConfiguration",
                Method::PUT | Method::DELETE => "s3:PutEncryptionConfiguration",
                _ => return Err(S3Error::not_implemented(format!("{method} ?encryption"))),
            };
            authz.check(action, Bucket(&bucket))?;
            crate::sse::handle(state, request, auth, bucket, method).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "policyStatus") => {
            authz.check("s3:GetBucketPolicyStatus", Bucket(&bucket))?;
            crate::policy::policy_status(state, bucket).await
        }
        (Target::Bucket(bucket), method) if has_param(&request, "acl") => {
            let action = match method {
                Method::GET => "s3:GetBucketAcl",
                Method::PUT => "s3:PutBucketAcl",
                _ => return Err(S3Error::not_implemented(format!("{method} ?acl"))),
            };
            authz.check(action, Bucket(&bucket))?;
            crate::policy::acl(state, request, auth, bucket, None, method).await
        }
        (Target::Bucket(bucket), method) if has_param(&request, "ownershipControls") => {
            let action = match method {
                Method::GET => "s3:GetBucketOwnershipControls",
                Method::PUT => "s3:PutBucketOwnershipControls",
                _ => {
                    return Err(S3Error::not_implemented(format!(
                        "{method} ?ownershipControls"
                    )));
                }
            };
            authz.check(action, Bucket(&bucket))?;
            crate::policy::ownership_controls(state, request, auth, bucket, method).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "versions") => {
            authz.check("s3:ListBucketVersions", Bucket(&bucket))?;
            let query = request.uri().query().unwrap_or("").to_string();
            // The owner shown is the bucket's (the objects are the owner's).
            let owner = authz
                .buckets
                .get(&bucket)
                .and_then(|grant| grant.owner.clone())
                .unwrap_or_default();
            crate::listing::list_versions(state, bucket, &owner, &query).await
        }
        (Target::Bucket(bucket), Method::PUT) => {
            authz.check("s3:CreateBucket", Bucket(&bucket))?;
            create_bucket(state, request, auth, bucket).await
        }
        (Target::Bucket(bucket), Method::DELETE) => {
            authz.check("s3:DeleteBucket", Bucket(&bucket))?;
            delete_bucket(state, bucket).await
        }
        (Target::Bucket(bucket), Method::HEAD) => {
            authz.check("s3:ListBucket", Bucket(&bucket))?;
            head_bucket(state, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "location") => {
            authz.check("s3:GetBucketLocation", Bucket(&bucket))?;
            bucket_location(state, bucket).await
        }
        // GetObject of one part of a multipart object is not implemented.
        (Target::Object(..), Method::GET | Method::HEAD)
            if has_param(&request, "partNumber") && !has_param(&request, "uploadId") =>
        {
            Err(S3Error::not_implemented(
                "GetObject by partNumber is not implemented",
            ))
        }
        (Target::Bucket(bucket), Method::GET) if has_param(&request, "uploads") => {
            authz.check("s3:ListBucketMultipartUploads", Bucket(&bucket))?;
            crate::multipart::list_uploads(state, request, bucket).await
        }
        (Target::Bucket(bucket), Method::POST) if has_param(&request, "delete") => {
            // Each key is authorized on its own.
            crate::delete::delete_objects(state, request, auth, authz, bucket).await
        }
        (Target::Bucket(bucket), Method::GET) => {
            authz.check("s3:ListBucket", Bucket(&bucket))?;
            let query = request.uri().query().unwrap_or("").to_string();
            crate::listing::list_objects(state, bucket, &query).await
        }
        (Target::Object(bucket, key), method) if has_param(&request, "acl") => {
            let action = match method {
                Method::GET => versioned(&request, "s3:GetObjectAcl", "s3:GetObjectVersionAcl"),
                Method::PUT => versioned(&request, "s3:PutObjectAcl", "s3:PutObjectVersionAcl"),
                _ => return Err(S3Error::not_implemented(format!("{method} ?acl"))),
            };
            authz.check(action, Object(&bucket, &key))?;
            crate::policy::acl(state, request, auth, bucket, Some(key), method).await
        }
        (Target::Object(bucket, key), Method::POST) if has_param(&request, "uploads") => {
            authz.check_storing(
                "s3:PutObject",
                Object(&bucket, &key),
                header_tags(&request)?.as_deref(),
            )?;
            crate::multipart::create_upload(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::POST) if has_param(&request, "uploadId") => {
            authz.check("s3:PutObject", Object(&bucket, &key))?;
            crate::multipart::complete_upload(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT) if has_param(&request, "tagging") => {
            let action = versioned(
                &request,
                "s3:PutObjectTagging",
                "s3:PutObjectVersionTagging",
            );
            authz.check(action, Object(&bucket, &key))?;
            // Checked again with the body's tags.
            crate::tagging::put_tagging(state, request, auth, authz, action, bucket, key).await
        }
        (Target::Object(bucket, key), Method::GET) if has_param(&request, "tagging") => {
            let action = versioned(
                &request,
                "s3:GetObjectTagging",
                "s3:GetObjectVersionTagging",
            );
            authz.check(action, Object(&bucket, &key))?;
            crate::tagging::get_tagging(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::DELETE) if has_param(&request, "tagging") => {
            let action = versioned(
                &request,
                "s3:DeleteObjectTagging",
                "s3:DeleteObjectVersionTagging",
            );
            authz.check(action, Object(&bucket, &key))?;
            crate::tagging::delete_tagging(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT) if copying => {
            // A copy stores the header's tags only when told to replace the
            // source's.
            let replaces_tags = request
                .headers()
                .get("x-amz-tagging-directive")
                .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"REPLACE"));
            let tags = match replaces_tags && !has_param(&request, "uploadId") {
                true => header_tags(&request)?,
                false => None,
            };
            authz.check_storing("s3:PutObject", Object(&bucket, &key), tags.as_deref())?;
            let (source, source_key, which) = crate::copy::copy_source(request.headers())?;
            let action = if matches!(which, truthdb_object::VersionSel::Latest) {
                "s3:GetObject"
            } else {
                "s3:GetObjectVersion"
            };
            // The source's version is the copy source's, not the query's.
            let mut context = authz.context().clone();
            match which {
                truthdb_object::VersionSel::Latest => context.remove("s3:VersionId"),
                truthdb_object::VersionSel::Null => {
                    context.set("s3:VersionId", vec!["null".to_string()])
                }
                truthdb_object::VersionSel::Seq(version) => {
                    context.set("s3:VersionId", vec![version.to_string()])
                }
            }
            authz.check_with(action, Object(&source, &source_key), &context)?;
            if has_param(&request, "uploadId") {
                crate::copy::upload_part_copy(state, request, bucket, key).await
            } else {
                crate::copy::copy_object(state, request, bucket, key).await
            }
        }
        (Target::Object(bucket, key), Method::PUT) if has_param(&request, "uploadId") => {
            authz.check("s3:PutObject", Object(&bucket, &key))?;
            crate::multipart::upload_part(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::GET) if has_param(&request, "uploadId") => {
            authz.check("s3:ListMultipartUploadParts", Object(&bucket, &key))?;
            crate::multipart::list_parts(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::DELETE) if has_param(&request, "uploadId") => {
            authz.check("s3:AbortMultipartUpload", Object(&bucket, &key))?;
            crate::multipart::abort_upload(state, request, bucket, key).await
        }
        (Target::Object(bucket, key), Method::PUT) => {
            authz.check_storing(
                "s3:PutObject",
                Object(&bucket, &key),
                header_tags(&request)?.as_deref(),
            )?;
            crate::objects::put_object(state, request, auth, bucket, key).await
        }
        (Target::Object(bucket, key), Method::GET) => {
            authz.check(
                versioned(&request, "s3:GetObject", "s3:GetObjectVersion"),
                Object(&bucket, &key),
            )?;
            crate::objects::get_object(state, request, bucket, key, false, principal.is_some())
                .await
        }
        (Target::Object(bucket, key), Method::HEAD) => {
            authz.check(
                versioned(&request, "s3:GetObject", "s3:GetObjectVersion"),
                Object(&bucket, &key),
            )?;
            crate::objects::get_object(state, request, bucket, key, true, principal.is_some()).await
        }
        (Target::Object(bucket, key), Method::DELETE) => {
            authz.check(
                versioned(&request, "s3:DeleteObject", "s3:DeleteObjectVersion"),
                Object(&bucket, &key),
            )?;
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

/// The principal's account's buckets.
async fn list_buckets(state: &S3State, principal: &Principal) -> Result<Response<Body>, S3Error> {
    let buckets = object(state, |file| file.list_buckets()).await?;
    let entries: String = buckets
        .iter()
        .filter(|bucket| bucket.owner == principal.account)
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
            "<ListAllMyBucketsResult xmlns=\"{XMLNS}\">{}<Buckets>{entries}</Buckets></ListAllMyBucketsResult>",
            owner_xml(&principal.account)
        ),
    ))
}

async fn create_bucket(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    // An anonymous caller's CreateBucket is denied before this.
    let owner = auth
        .principal
        .as_ref()
        .map(|principal| principal.account.clone())
        .ok_or_else(|| S3Error::access_denied("Access Denied"))?;
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
        // A new bucket blocks public access, until its owner says otherwise.
        file.transaction(|tx| {
            let created = tx.create_bucket(&name, &owner, now)?;
            if created.is_ok() {
                tx.put_bucket_config(
                    &name,
                    truthdb_object::PUBLIC_ACCESS_BLOCK,
                    "",
                    &[crate::public_access::ALL],
                )?;
            }
            Ok(created)
        })
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
