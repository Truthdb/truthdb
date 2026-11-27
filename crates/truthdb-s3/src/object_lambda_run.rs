//! Requests through Object Lambda access points (see `object_lambda` for
//! the access points themselves). A request names one by its host,
//! `<name>-<account>.s3-object-lambda.<region>.<domain>` (signed for
//! `s3-object-lambda`), or by its alias in place of a bucket's name.
//!
//! A GetObject the access point transforms is answered by its function:
//! - the caller is authorized for `s3-object-lambda:GetObject` on the
//!   access point (its own policies and the access point's policy), and for
//!   `lambda:InvokeFunction` on the function, with `aws:CalledVia` set to
//!   `s3-object-lambda.amazonaws.com`;
//! - the function (a topic of the access point's account with a webhook,
//!   as Batch Operations' LambdaInvoke invokes) is POSTed AWS's event: a
//!   `getObjectContext` with `inputS3Url`, a URL presigned with the
//!   caller's own key for the object through the supporting access point,
//!   and the `outputRoute` and `outputToken` it answers with;
//! - the function answers with WriteGetObjectResponse, whose status,
//!   headers and body are the caller's answer, streamed. A function that
//!   returns without answering, or does not answer within 60 seconds, gives
//!   the caller a 500.
//!
//! A request through `inputS3Url` carries this server's mark of having
//! come through Object Lambda (see [`called_via`]): only with it does
//! `aws:CalledVia` hold, so a supporting access point's policy may grant to
//! requests through Object Lambda alone.
//!
//! A HeadObject, ListObjects or ListObjectsV2 the access point transforms
//! is answered by what its function returns. A request no transformation
//! names is passed on to the supporting access point ("All other requests
//! are processed as normal"), as one made through Object Lambda.
//!
//! Each is authorized as AWS's `s3-object-lambda` action for it (see
//! [`lambda_action`]) on the Object Lambda access point.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use ring::hmac;
use tokio::sync::{mpsc, oneshot};
use truthdb_iam::{Policy, PolicyKind};
use truthdb_object::ObjectLambdaPoint;

use crate::api::{Target, VirtualHost};
use crate::authz::Authz;
use crate::error::S3Error;
use crate::object_lambda::{Configuration, arn as lambda_arn, supporting_point};
use crate::server::{Authenticated, Body, Connection, RequestIds, S3State, full};

/// The service AWS names in `aws:CalledVia`.
const CALLED_VIA: &str = "s3-object-lambda.amazonaws.com";
/// The query parameter of a URL presigned for a function, marking it as
/// made for one (see [`called_via`]).
pub(crate) const MARK: &str = "x-truthdb-object-lambda";
/// How long a function has to answer ("up to 60 seconds").
const ANSWER_WITHIN: Duration = Duration::from_secs(60);
/// How long `inputS3Url` is good for, in seconds.
const URL_EXPIRES: u64 = 300;
/// The route a function answers on (the host prefix of its
/// WriteGetObjectResponse).
const ROUTE: &str = "io-truthdb";
/// A function's answer to its invocation, at most (a GetObject's is not
/// read; a listing's may hold a thousand keys of a kilobyte each).
const MAX_INVOCATION_ANSWER: usize = 16 * 1024 * 1024;

/// The key the marks of presigned URLs are made with: this process's own
/// (a URL outlives no restart).
static MARK_KEY: LazyLock<hmac::Key> = LazyLock::new(|| {
    hmac::Key::generate(hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
        .expect("the system's random source")
});

/// The GetObjects waiting for their function's WriteGetObjectResponse, by
/// their output token.
static WAITING: LazyLock<Mutex<HashMap<String, Waiter>>> = LazyLock::new(Default::default);

/// The client functions are invoked with.
static POSTER: OnceLock<Option<truthdb_s3client::Poster>> = OnceLock::new();

/// A function's WriteGetObjectResponse, as the caller's answer: its status,
/// error, headers and body.
struct Answer {
    status: u16,
    error: Option<(String, String)>,
    headers: Vec<(String, String)>,
    body: mpsc::Receiver<Bytes>,
}

/// The Object Lambda access point a request is made through.
pub(crate) struct ViaLambda {
    point: ObjectLambdaPoint,
    configuration: Configuration,
    policy: Option<Policy>,
}

/// The Object Lambda access point `request` names, if it names one: by its
/// host (`<name>-<account>.s3-object-lambda.<region>` before a domain), or
/// by its alias in place of a bucket's name. A bucket named as its host
/// (made before such names were refused) keeps its requests.
pub(crate) async fn resolve<B>(
    state: &S3State,
    request: &Request<B>,
    principal: Option<&crate::server::Principal>,
) -> Result<Option<ViaLambda>, S3Error> {
    let label = request
        .extensions()
        .get::<VirtualHost>()
        .map(|host| host.0.clone());
    let suffix = format!(".s3-object-lambda.{}", state.region);
    enum Named {
        Host(String, Option<(String, String)>),
        Alias(String),
    }
    let named = match label
        .as_deref()
        .and_then(|label| label.strip_suffix(&suffix))
    {
        Some(named) => Named::Host(
            label.clone().unwrap_or_default(),
            named
                .rsplit_once('-')
                .filter(|(_, account)| {
                    account.len() == 12 && account.bytes().all(|byte| byte.is_ascii_digit())
                })
                .map(|(name, account)| (account.to_string(), name.to_string())),
        ),
        None => match crate::api::named_target(request)? {
            Target::Bucket(bucket) | Target::Object(bucket, _) if bucket.ends_with("--ol-s3") => {
                Named::Alias(bucket)
            }
            _ => return Ok(None),
        },
    };
    let alias = matches!(named, Named::Alias(_));
    let scope = crate::scope::Scope {
        key: principal.map(|principal| (principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
        origin: None,
    };
    let found = crate::scope::within(
        scope,
        crate::scope::object(state, move |file| {
            let point = match &named {
                Named::Host(label, _) if file.bucket(label)?.is_some() => return Ok(Some(None)),
                Named::Host(_, Some((account, name))) => file.object_lambda_point(account, name)?,
                Named::Host(_, None) => None,
                Named::Alias(alias) => file.object_lambda_point_by_alias(alias)?,
            };
            let Some(point) = point else {
                return Ok(None);
            };
            let policy = file.object_lambda_policy(&point.account, &point.name)?;
            Ok(Some(Some((point, policy))))
        }),
    )
    .await?;
    let (point, policy) = match found {
        Some(Some(found)) => found,
        Some(None) => return Ok(None),
        None if alias => {
            return Err(S3Error::new(
                404,
                "NoSuchBucket",
                "The specified bucket does not exist",
            ));
        }
        None => {
            return Err(S3Error::new(
                404,
                "NoSuchAccessPoint",
                "The specified accesspoint does not exist",
            ));
        }
    };
    let configuration = Configuration::parse(point.configuration.as_bytes(), "Configuration")
        .map_err(|err| {
            S3Error::internal(format!(
                "stored Object Lambda configuration: {}",
                err.message
            ))
        })?;
    let policy = policy
        .map(|text| Policy::parse(&text, PolicyKind::Bucket))
        .transpose()
        .map_err(|err| {
            S3Error::internal(format!(
                "stored Object Lambda access point policy: {}",
                err.0
            ))
        })?;
    Ok(Some(ViaLambda {
        point,
        configuration,
        policy,
    }))
}

/// A request signed for `s3-object-lambda`: a function's
/// WriteGetObjectResponse, or one through an Object Lambda access point's
/// host.
pub(crate) async fn route(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
) -> Result<Response<Body>, S3Error> {
    // A directory bucket's session acts in its bucket only.
    if auth.session.is_some() {
        return Err(session_refused());
    }
    if request.method() == Method::POST && request.uri().path() == "/WriteGetObjectResponse" {
        return write_get_object_response(state, request, auth).await;
    }
    // Served as any request is (see `api::dispatch`), which takes the
    // Object Lambda access point its host names.
    match resolve(state, &request, auth.principal.as_ref()).await? {
        Some(_) => crate::api::dispatch(state, request, auth).await,
        None => Err(S3Error::invalid_request(
            "A request signed for s3-object-lambda names an Object Lambda access point",
        )),
    }
}

/// The operation a request through an Object Lambda access point is, if a
/// transformation may name it: an object's GetObject or HeadObject, or a
/// listing of the bucket (ListObjectsV2 with `list-type=2`).
fn action_of(request: &Request<Incoming>) -> Result<Option<&'static str>, S3Error> {
    if crate::api::subresource(request).is_some() {
        return Ok(None);
    }
    Ok(
        match (crate::api::named_target(request)?, request.method()) {
            (Target::Object(..), &Method::GET) => Some("GetObject"),
            (Target::Object(..), &Method::HEAD) => Some("HeadObject"),
            (Target::Bucket(_), &Method::GET) => Some(
                match crate::listing::query_params(request.uri().query().unwrap_or(""))
                    .iter()
                    .find(|(name, _)| name == "list-type")
                    .map(|(_, value)| value.as_str())
                {
                    Some("2") => "ListObjectsV2",
                    _ => "ListObjects",
                },
            ),
            _ => None,
        },
    )
}

/// Serves `request` through Object Lambda access point `via`: by its
/// function, if a transformation names the operation (the response); else
/// passed on to its supporting access point ("All other requests are
/// processed as normal"), `request` then being one through it (none).
pub(crate) async fn serve(
    state: &S3State,
    request: &mut Request<Incoming>,
    auth: &Authenticated,
    via: ViaLambda,
) -> Result<Option<Response<Body>>, S3Error> {
    if auth.session.is_some() {
        return Err(session_refused());
    }
    let Some(principal) = auth.principal.clone() else {
        return Err(S3Error::access_denied("Access Denied"));
    };
    let held = &*request;
    let action = action_of(held)?;
    let transformation = action.and_then(|action| {
        via.configuration
            .transformations
            .iter()
            .find(|transformation| transformation.actions.iter().any(|named| named == action))
    });
    let point = &via.point;
    let point_arn = lambda_arn(&state.region, &point.account, &point.name);
    // Counted in its own metrics, if they are on: by where it gets its data
    // (its function, or its supporting access point); as served only once
    // it is (see `counted_as`).
    let source = via
        .configuration
        .metrics
        .then(|| match (action, transformation) {
            (Some(_), Some(transformation)) => {
                canonical_function(&state.region, &point.account, &transformation.function_arn)
            }
            _ => via.configuration.supporting.clone(),
        });
    counted_as(held, point, &source, crate::metering::LambdaRole::Unserved);
    let query = held.uri().query().unwrap_or("").to_string();
    let authz = Authz::load(
        state,
        Some(&principal),
        auth.signed,
        held.headers(),
        &query,
        held.extensions().get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    let policies: Vec<&Policy> = via.policy.iter().collect();
    let (Some(action), Some(transformation)) = (action, transformation) else {
        // Passed on: as its operation, on the Object Lambda access point,
        // then through its supporting access point as through Object Lambda.
        let name = crate::activity::data_event_name(held)
            .and_then(|name| lambda_action(name, crate::api::has_param(held, "versionId")))
            .ok_or_else(|| {
                S3Error::not_implemented("This operation through an Object Lambda access point")
            })?;
        authz.check_arn_with(
            &format!("s3-object-lambda:{name}"),
            &point_arn,
            &point.account,
            &policies,
        )?;
        let (account, supporting) = supporting_point(&via.configuration.supporting, &state.region)
            .ok_or_else(|| S3Error::internal("a stored supporting access point is not one"))?;
        crate::access_points::resolve_point(
            state,
            request,
            Some(&principal),
            &account,
            &supporting,
        )
        .await
        .map_err(|err| match err.code {
            "NoSuchAccessPoint" => no_supporting_point(),
            _ => err,
        })?;
        counted_as(
            request,
            point,
            &source,
            crate::metering::LambdaRole::Proxied,
        );
        // Every check the operation makes is made here too (see
        // `Authz::through_lambda`).
        request.extensions_mut().insert(crate::authz::LambdaGate {
            arn: point_arn,
            account: point.account.clone(),
            policy: via.policy.clone(),
        });
        return Ok(None);
    };
    let configuration = &via.configuration;
    // Its events are the Object Lambda access point's.
    if let Some(seen) = crate::metering::seen_of(held) {
        seen.mark_answered_by(&point_arn, &point.account);
    }
    // Ranges and parts only as its features allow (the function serves them).
    let allows = |feature: &str| configuration.features.iter().any(|known| known == feature);
    if matches!(action, "GetObject" | "HeadObject") {
        if held.headers().contains_key("range") && !allows(&format!("{action}-Range")) {
            return Err(S3Error::not_implemented(
                "A Range through this Object Lambda access point",
            ));
        }
        if crate::api::has_param(held, "partNumber") && !allows(&format!("{action}-PartNumber")) {
            return Err(S3Error::not_implemented(
                "A partNumber through this Object Lambda access point",
            ));
        }
    }
    authz.check_arn_with(
        &format!(
            "s3-object-lambda:{}",
            lambda_action(action, crate::api::has_param(held, "versionId"))
                .expect("a transformed operation's action")
        ),
        &point_arn,
        &point.account,
        &policies,
    )?;
    // The function: the access point's account's, named canonically.
    let (function, qualifier, named_account) =
        crate::batch::function_name(&transformation.function_arn)
            .ok_or_else(|| S3Error::internal("a stored function ARN is not one"))?;
    if named_account
        .as_ref()
        .is_some_and(|named| *named != point.account)
    {
        return Err(function_not_found());
    }
    let mut function_arn = format!(
        "arn:aws:lambda:{}:{}:function:{function}",
        state.region, point.account
    );
    if let Some(qualifier) = &qualifier {
        function_arn.push(':');
        function_arn.push_str(qualifier);
    }
    authz.with_keys(&called_via_keys()).check_arn(
        "lambda:InvokeFunction",
        &function_arn,
        &point.account,
    )?;
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
        origin: None,
    };
    let (supporting_account, supporting_name) =
        supporting_point(&configuration.supporting, &state.region)
            .ok_or_else(|| S3Error::internal("a stored supporting access point is not one"))?;
    let access_key = principal.access_key.clone();
    let found = crate::scope::within(
        scope,
        crate::scope::object(state, move |file| {
            let supporting = file.access_point(&supporting_account, &supporting_name)?;
            let secret = file.access_key(&access_key)?.map(|(_, secret)| secret);
            Ok((supporting, secret))
        }),
    )
    .await?;
    let (Some(supporting), secret) = found else {
        return Err(no_supporting_point());
    };
    // The caller's own key signs the function's read: as AWS's is "signed by
    // using the original caller's identity".
    let Some(secret) = secret else {
        return Err(S3Error::access_denied(
            "These credentials cannot be used through an Object Lambda access point",
        ));
    };
    let (account, owner_name) = (point.account.clone(), function.clone());
    let webhook = state
        .engine
        .run_engine(move |engine| {
            engine.streams(|file| {
                let owned = file
                    .topic(&owner_name)?
                    .is_some_and(|(topic, _)| topic.account.as_deref() == Some(account.as_str()));
                Ok(file
                    .webhooks()?
                    .into_iter()
                    .find(|webhook| owned && webhook.topic == owner_name)
                    .map(|webhook| (webhook.url, webhook.secret)))
            })
        })
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .ok_or_else(function_not_found)?;
    let request_id = held
        .extensions()
        .get::<RequestIds>()
        .map(|ids| ids.request_id.clone())
        .unwrap_or_default();
    let tls = held
        .extensions()
        .get::<Connection>()
        .is_some_and(|connection| connection.tls);
    let scheme = if tls { "https" } else { "http" };
    let host = held
        .headers()
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let port = host
        .rsplit_once(':')
        .filter(|(_, port)| port.bytes().all(|byte| byte.is_ascii_digit()))
        .map(|(_, port)| format!(":{port}"))
        .unwrap_or_default();
    let domain = state
        .domains
        .first()
        .cloned()
        .unwrap_or_else(|| "localhost".to_string());
    // The function's read: the object (its version, its part), or the
    // listing with all the caller's query but its signature.
    let (method, path, passed) = match crate::api::named_target(held)? {
        Target::Object(_, key) => (
            if action == "HeadObject" {
                "HEAD"
            } else {
                "GET"
            },
            format!("/{}/{}", supporting.alias, crate::sigv4::encode_key(&key)),
            query
                .split('&')
                .filter(|pair| {
                    let name = pair.split_once('=').map_or(*pair, |(name, _)| name);
                    matches!(name, "versionId" | "partNumber")
                })
                .collect::<Vec<_>>()
                .join("&"),
        ),
        _ => (
            "GET",
            format!("/{}", supporting.alias),
            without_authorization(&query).join("&"),
        ),
    };
    let input_url = presign(
        method,
        &format!("{domain}{port}"),
        scheme,
        &path,
        &passed,
        &principal.access_key,
        &secret,
        &state.region,
    );
    let token = random_token();
    let context = match action {
        "GetObject" => serde_json::json!({
            "inputS3Url": input_url,
            "outputRoute": ROUTE,
            "outputToken": token,
        }),
        _ => serde_json::json!({ "inputS3Url": input_url }),
    };
    let context_key = match action {
        "GetObject" => "getObjectContext",
        "HeadObject" => "headObjectContext",
        "ListObjects" => "listObjectsContext",
        _ => "listObjectsV2Context",
    };
    let mut event = serde_json::json!({
        "xAmzRequestId": request_id,
        "configuration": {
            "accessPointArn": point_arn,
            "supportingAccessPointArn": configuration.supporting,
            "payload": transformation.payload.clone().unwrap_or_default(),
        },
        "userRequest": {
            "url": user_url(scheme, &host, held),
            "headers": user_headers(held),
        },
        "userIdentity": identity(&principal),
        "protocolVersion": "1.00",
    });
    event[context_key] = context;
    let event = event.to_string();
    counted_as(held, point, &source, crate::metering::LambdaRole::Invoked);
    let delivery = format!("object-lambda/{request_id}");
    if action != "GetObject" {
        // Answered by what the function returns.
        let invocation = invoke(webhook, delivery, event);
        let returned = tokio::select! {
            returned = invocation => returned.ok().flatten(),
            _ = tokio::time::sleep(ANSWER_WITHIN) => None,
        };
        let (status, body) = returned.ok_or_else(|| {
            S3Error::new(
                500,
                "LambdaInvocationFailed",
                "The Object Lambda access point's function could not be invoked",
            )
        })?;
        if !(200..300).contains(&status) {
            return Err(S3Error::new(
                500,
                "LambdaRuntimeError",
                format!("The Object Lambda access point's function failed: HTTP {status}"),
            ));
        }
        return returned_response(action, &body, &request_id, held.uri().path()).map(Some);
    }
    let (answered, answer) = oneshot::channel();
    let counted = via.configuration.metrics.then(|| {
        lambda_seen(
            point,
            function_arn.clone(),
            crate::metering::LambdaRole::Responding,
        )
    });
    lock_waiting().insert(token.clone(), (answered, counted));
    let _waiting = Waiting(token.clone());
    // Its return is not read: the function answers with
    // WriteGetObjectResponse.
    let invocation = invoke(webhook, delivery, event);
    let failed = || {
        S3Error::new(
            500,
            "InternalError",
            "The Object Lambda access point's function did not answer",
        )
    };
    // An answer that came is the caller's, even if the function has ended
    // since.
    let answer = tokio::select! {
        biased;
        answer = answer => answer.ok(),
        _ = invocation => None,
        _ = tokio::time::sleep(ANSWER_WITHIN) => None,
    };
    let Some(answer) = answer else {
        return Err(failed());
    };
    Ok(Some(answer_response(
        answer,
        &request_id,
        held.uri().path(),
    )))
}

/// AWS's `s3-object-lambda` actions (the service authorization reference's).
pub(crate) const LAMBDA_ACTIONS: [&str; 26] = [
    "AbortMultipartUpload",
    "DeleteObject",
    "DeleteObjectTagging",
    "DeleteObjectVersion",
    "DeleteObjectVersionTagging",
    "GetObject",
    "GetObjectAcl",
    "GetObjectLegalHold",
    "GetObjectRetention",
    "GetObjectTagging",
    "GetObjectVersion",
    "GetObjectVersionAcl",
    "GetObjectVersionTagging",
    "ListBucket",
    "ListBucketMultipartUploads",
    "ListBucketVersions",
    "ListMultipartUploadParts",
    "PutObject",
    "PutObjectAcl",
    "PutObjectLegalHold",
    "PutObjectRetention",
    "PutObjectTagging",
    "PutObjectVersionAcl",
    "PutObjectVersionTagging",
    "RestoreObject",
    "WriteGetObjectResponse",
];

/// AWS's `s3-object-lambda` action for the operation of data event `name`
/// (`versioned`: naming an object's version), as `s3:` actions authorize
/// them: the reads as GetObject, the listings as ListBucket, the writes of
/// a body (copies and the multipart upload's) as PutObject. An operation it
/// has none for is not served through an Object Lambda access point.
fn lambda_action(name: &str, versioned: bool) -> Option<&'static str> {
    let of = |plain: &'static str, version: &'static str| match versioned {
        true => version,
        false => plain,
    };
    Some(match name {
        "GetObject" | "HeadObject" => of("GetObject", "GetObjectVersion"),
        "ListObjects" | "ListObjectsV2" => "ListBucket",
        "ListObjectVersions" => "ListBucketVersions",
        "PutObject"
        | "CopyObject"
        | "CreateMultipartUpload"
        | "UploadPart"
        | "UploadPartCopy"
        | "CompleteMultipartUpload" => "PutObject",
        "AbortMultipartUpload" => "AbortMultipartUpload",
        "ListParts" => "ListMultipartUploadParts",
        "DeleteObject" => of("DeleteObject", "DeleteObjectVersion"),
        "DeleteObjects" => "DeleteObject",
        "GetObjectTagging" => of("GetObjectTagging", "GetObjectVersionTagging"),
        "PutObjectTagging" => of("PutObjectTagging", "PutObjectVersionTagging"),
        "DeleteObjectTagging" => of("DeleteObjectTagging", "DeleteObjectVersionTagging"),
        "GetObjectAcl" => of("GetObjectAcl", "GetObjectVersionAcl"),
        "PutObjectAcl" => of("PutObjectAcl", "PutObjectVersionAcl"),
        "GetObjectLegalHold" => "GetObjectLegalHold",
        "PutObjectLegalHold" => "PutObjectLegalHold",
        "GetObjectRetention" => "GetObjectRetention",
        "PutObjectLockRetention" => "PutObjectRetention",
        "RestoreObject" => "RestoreObject",
        _ => return None,
    })
}

/// The ARN of the function `arn` names, as an access point of `account`
/// invokes it (in this Region, of that account); `arn` itself if it names
/// none.
fn canonical_function(region: &str, account: &str, arn: &str) -> String {
    match crate::batch::function_name(arn) {
        Some((function, qualifier, _)) => {
            let mut canonical = format!("arn:aws:lambda:{region}:{account}:function:{function}");
            if let Some(qualifier) = &qualifier {
                canonical.push(':');
                canonical.push_str(qualifier);
            }
            canonical
        }
        None => arn.to_string(),
    }
}

/// Marks `request` (to Object Lambda access point `point`) as its metrics
/// count it, from `source`, if they are on.
fn counted_as<B>(
    request: &Request<B>,
    point: &ObjectLambdaPoint,
    source: &Option<String>,
    role: crate::metering::LambdaRole,
) {
    if let (Some(source), Some(seen)) = (source, crate::metering::seen_of(request)) {
        seen.mark_lambda(lambda_seen(point, source.clone(), role));
    }
}

/// A request to Object Lambda access point `point` as its metrics count it.
fn lambda_seen(
    point: &ObjectLambdaPoint,
    source: String,
    role: crate::metering::LambdaRole,
) -> crate::metering::LambdaSeen {
    crate::metering::LambdaSeen {
        series: truthdb_object::MetricSeries {
            bucket: crate::metric_history::object_lambda_series(&point.account, &point.name),
            by: truthdb_object::SeriesBy::ObjectLambda { source },
        },
        made_ms: point.created_ms,
        role,
    }
}

fn no_supporting_point() -> S3Error {
    S3Error::new(
        404,
        "NoSuchAccessPoint",
        "The Object Lambda access point's supporting access point does not exist",
    )
}

/// POSTs `event` to the function's webhook (`url` and the `secret` its POSTs
/// are signed with): its status and what it returned (none: it could not be
/// reached).
fn invoke(
    (url, secret): (String, String),
    delivery: String,
    event: String,
) -> tokio::task::JoinHandle<Option<(u16, Bytes)>> {
    tokio::spawn(async move {
        let poster = POSTER
            .get_or_init(|| truthdb_s3client::Poster::new(None).ok())
            .as_ref()?;
        let secs = crate::objects::now_ms() / 1000;
        let headers = [
            ("content-type", "application/json".to_string()),
            ("x-truthdb-delivery", delivery.clone()),
            (
                "x-truthdb-signature",
                truthdb_s3client::webhook_signature(&secret, secs, &delivery, event.as_bytes()),
            ),
        ];
        poster
            .post_answer(&url, &headers, Bytes::from(event), MAX_INVOCATION_ANSWER)
            .await
            .ok()
    })
}

/// The pairs of `query` but those authenticating a presigned URL.
fn without_authorization(query: &str) -> Vec<&str> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter(|pair| {
            let name = pair.split_once('=').map_or(*pair, |(name, _)| name);
            !crate::server::QUERY_AUTH_PARAMS
                .iter()
                .any(|auth| name.eq_ignore_ascii_case(auth))
                && !name.eq_ignore_ascii_case(MARK)
        })
        .collect()
}

/// The caller's answer from what a HeadObject's or a listing's function
/// returned: `statusCode`, an error (`errorCode`, `errorMessage`), and
/// HeadObject's `headers` (its `Content-Length` required) or a listing's
/// `listResultXml` (forwarded as it is) or `listBucketResult` (rendered).
fn returned_response(
    action: &str,
    body: &[u8],
    request_id: &str,
    resource: &str,
) -> Result<Response<Body>, S3Error> {
    let invalid = |why: &str| {
        S3Error::new(
            500,
            "LambdaInvalidResponse",
            format!("The Object Lambda access point's function returned {why}"),
        )
    };
    let returned: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| invalid("no JSON"))?;
    let status = returned["statusCode"]
        .as_u64()
        .and_then(|status| u16::try_from(status).ok())
        .filter(|status| (200..600).contains(status))
        .ok_or_else(|| invalid("no statusCode"))?;
    if let Some(code) = returned["errorCode"].as_str() {
        let message = returned["errorMessage"].as_str().unwrap_or("").to_string();
        return Ok(answer_response(
            Answer {
                status,
                error: Some((code.to_string(), message)),
                headers: Vec::new(),
                body: mpsc::channel(1).1,
            },
            request_id,
            resource,
        ));
    }
    let mut builder = Response::builder().status(status);
    if action == "HeadObject" {
        let headers = returned["headers"]
            .as_object()
            .ok_or_else(|| invalid("no headers"))?;
        let text = |value: &serde_json::Value| match value {
            serde_json::Value::String(text) => Some(text.clone()),
            serde_json::Value::Number(number) => Some(number.to_string()),
            serde_json::Value::Bool(flag) => Some(flag.to_string()),
            _ => None,
        };
        let mut sized = false;
        for (name, value) in headers {
            let name = name.to_ascii_lowercase();
            let Some(value) = text(value) else {
                return Err(invalid(&format!("header {name} of no text")));
            };
            if name == "content-length" {
                if sized {
                    return Err(invalid("Content-Length twice"));
                }
                value
                    .parse::<u64>()
                    .map_err(|_| invalid("a Content-Length that is no length"))?;
                sized = true;
            } else if !FORWARDED.contains(&name.as_str()) && !name.starts_with("x-amz-meta-") {
                continue;
            }
            builder = builder.header(name, value);
        }
        if !sized {
            return Err(invalid("no Content-Length"));
        }
        return builder
            .body(full(Vec::new()))
            .map_err(|_| invalid("headers that are not valid"));
    }
    let xml = match (
        returned["listResultXml"].as_str(),
        returned.get("listBucketResult"),
    ) {
        (Some(_), Some(_)) => {
            return Err(invalid("both listResultXml and listBucketResult"));
        }
        (Some(xml), None) => xml.to_string(),
        (None, Some(result)) => list_xml(action == "ListObjectsV2", result).map_err(invalid)?,
        (None, None) => return Err(invalid("no listing")),
    };
    Ok(builder
        .header("content-type", "application/xml")
        .body(full(xml))
        .expect("a valid response"))
}

/// A `listBucketResult` as ListObjects' (or ListObjectsV2's) XML, its
/// required fields there.
fn list_xml(v2: bool, result: &serde_json::Value) -> Result<String, &'static str> {
    let escape = crate::access_points::escape;
    let text = |name: &str| result[name].as_str().map(escape);
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    let name = text("name").ok_or("a listBucketResult without its name")?;
    out.push_str(&format!("<Name>{name}</Name>"));
    let optional: &[(&str, &str)] = if v2 {
        &[
            ("prefix", "Prefix"),
            ("startAfter", "StartAfter"),
            ("continuationToken", "ContinuationToken"),
            ("nextContinuationToken", "NextContinuationToken"),
            ("delimiter", "Delimiter"),
            ("encodingType", "EncodingType"),
        ]
    } else {
        &[
            ("prefix", "Prefix"),
            ("marker", "Marker"),
            ("nextMarker", "NextMarker"),
            ("delimiter", "Delimiter"),
            ("encodingType", "EncodingType"),
        ]
    };
    for (field, element) in optional {
        if let Some(value) = text(field) {
            out.push_str(&format!("<{element}>{value}</{element}>"));
        }
    }
    if v2 {
        let count = result["keyCount"]
            .as_u64()
            .ok_or("a listBucketResult without its keyCount")?;
        out.push_str(&format!("<KeyCount>{count}</KeyCount>"));
    }
    let max = result["maxKeys"]
        .as_u64()
        .ok_or("a listBucketResult without its maxKeys")?;
    let truncated = result["isTruncated"]
        .as_bool()
        .ok_or("a listBucketResult without its isTruncated")?;
    out.push_str(&format!(
        "<MaxKeys>{max}</MaxKeys><IsTruncated>{truncated}</IsTruncated>"
    ));
    for object in result["contents"].as_array().into_iter().flatten() {
        let key = object["key"].as_str().ok_or("an object without its key")?;
        let size = object["size"]
            .as_u64()
            .ok_or("an object without its size")?;
        out.push_str(&format!("<Contents><Key>{}</Key>", escape(key)));
        for (field, element) in [
            ("lastModified", "LastModified"),
            ("eTag", "ETag"),
            ("checksumAlgorithm", "ChecksumAlgorithm"),
        ] {
            if let Some(value) = object[field].as_str() {
                out.push_str(&format!("<{element}>{}</{element}>", escape(value)));
            }
        }
        out.push_str(&format!("<Size>{size}</Size>"));
        if let Some(owner) = object.get("owner").filter(|owner| !owner.is_null()) {
            let (Some(id), display) = (owner["id"].as_str(), owner["displayName"].as_str()) else {
                return Err("an owner without its id");
            };
            if !v2 && display.is_none() {
                return Err("an owner without its displayName");
            }
            out.push_str(&format!("<Owner><ID>{}</ID>", escape(id)));
            if let Some(display) = display {
                out.push_str(&format!("<DisplayName>{}</DisplayName>", escape(display)));
            }
            out.push_str("</Owner>");
        }
        if let Some(class) = object["storageClass"].as_str() {
            out.push_str(&format!("<StorageClass>{}</StorageClass>", escape(class)));
        }
        out.push_str("</Contents>");
    }
    for prefix in result["commonPrefixes"].as_array().into_iter().flatten() {
        let prefix = prefix["prefix"]
            .as_str()
            .ok_or("a common prefix without its prefix")?;
        out.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape(prefix)
        ));
    }
    out.push_str("</ListBucketResult>");
    Ok(out)
}

fn session_refused() -> S3Error {
    S3Error::access_denied("A session's credentials are its directory bucket's only")
}

/// Removes a waiting GetObject's entry when its wait ends however it does
/// (its caller gone too).
struct Waiting(String);

impl Drop for Waiting {
    fn drop(&mut self) {
        lock_waiting().remove(&self.0);
    }
}

/// The headers a WriteGetObjectResponse forwards (`x-amz-fwd-header-<name>`),
/// as botocore's model lists them.
const FORWARDED: &[&str] = &[
    "accept-ranges",
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-range",
    "content-type",
    "etag",
    "expires",
    "last-modified",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-delete-marker",
    "x-amz-expiration",
    "x-amz-missing-meta",
    "x-amz-mp-parts-count",
    "x-amz-object-lock-legal-hold",
    "x-amz-object-lock-mode",
    "x-amz-object-lock-retain-until-date",
    "x-amz-replication-status",
    "x-amz-request-charged",
    "x-amz-restore",
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-storage-class",
    "x-amz-tagging-count",
    "x-amz-version-id",
];

fn function_not_found() -> S3Error {
    S3Error::new(
        500,
        "InternalError",
        "The Object Lambda access point's function names no topic of its account with a webhook",
    )
}

/// The condition keys of a request made through Object Lambda.
pub(crate) fn called_via_keys() -> [(&'static str, String); 3] {
    [
        ("aws:CalledVia", CALLED_VIA.to_string()),
        ("aws:CalledViaFirst", CALLED_VIA.to_string()),
        ("aws:CalledViaLast", CALLED_VIA.to_string()),
    ]
}

/// A GetObject waiting for its answer: where it goes, and how the
/// function's answer is counted (if its access point's metrics are on).
type Waiter = (oneshot::Sender<Answer>, Option<crate::metering::LambdaSeen>);

fn lock_waiting() -> std::sync::MutexGuard<'static, HashMap<String, Waiter>> {
    WAITING
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn random_token() -> String {
    use ring::rand::SecureRandom as _;
    let mut bytes = [0u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .expect("the system's random source");
    crate::sigv4::hex(&bytes)
}

/// The caller's request headers, as the event tells them: not its
/// signature, which the function must not be able to replay.
fn user_headers<B>(request: &Request<B>) -> serde_json::Map<String, serde_json::Value> {
    request
        .headers()
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.as_str(),
                "authorization" | "x-amz-security-token" | "x-amz-s3session-token"
            )
        })
        .filter_map(|(name, value)| {
            Some((
                name.as_str().to_string(),
                serde_json::json!(value.to_str().ok()?),
            ))
        })
        .collect()
}

/// The caller's request URL, as the event tells it: "excluding any
/// authorization-related query parameters" (a presigned URL's signature,
/// which the function must not be able to replay).
fn user_url<B>(scheme: &str, host: &str, request: &Request<B>) -> String {
    let kept: Vec<&str> = request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter(|pair| {
            let name = pair.split_once('=').map_or(*pair, |(name, _)| name);
            !crate::server::QUERY_AUTH_PARAMS
                .iter()
                .any(|auth| name.eq_ignore_ascii_case(auth))
        })
        .collect();
    let query = match kept.is_empty() {
        true => String::new(),
        false => format!("?{}", kept.join("&")),
    };
    format!("{scheme}://{host}{}{query}", request.uri().path())
}

/// The caller, as the event's `userIdentity`.
fn identity(principal: &crate::server::Principal) -> serde_json::Value {
    let account = &principal.account;
    match &principal.user {
        None => serde_json::json!({
            "type": "Root",
            "principalId": account,
            "arn": format!("arn:aws:iam::{account}:root"),
            "accountId": account,
            "accessKeyId": principal.access_key,
        }),
        Some(user) => serde_json::json!({
            "type": "IAMUser",
            "principalId": format!("{account}:{user}"),
            "arn": format!("arn:aws:iam::{account}:user/{user}"),
            "accountId": account,
            "accessKeyId": principal.access_key,
            "userName": user,
        }),
    }
}

/// What the mark of a URL presigned for a function is of: its method, its
/// access key, its host and path, and its query (all of it but the mark and
/// the signature: its signing time and expiry among it).
fn marked(method: &str, access_key: &str, host: &str, path: &str, query: &str) -> String {
    format!(
        "{method}\n{access_key}\n{host}\n{path}\n{}",
        crate::sigv4::canonical_query(query)
    )
}

/// A URL for the function's `method` request of `path` at `host`, with
/// query `passed`, presigned with `access_key` and its `secret` and marked
/// as made for a function.
#[allow(clippy::too_many_arguments)]
fn presign(
    method: &str,
    host: &str,
    scheme: &str,
    path: &str,
    passed: &str,
    access_key: &str,
    secret: &str,
    region: &str,
) -> String {
    let now = crate::time::iso8601(crate::objects::now_ms());
    // `2026-11-26T10:00:00.000Z` as `20261126T100000Z`.
    let amz_date: String = now[..19]
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == 'T')
        .collect::<String>()
        + "Z";
    let date = &amz_date[..8];
    let credential = crate::sigv4::Credential {
        access_key: access_key.to_string(),
        date: date.to_string(),
        region: region.to_string(),
        service: "s3".to_string(),
    };
    let expires = URL_EXPIRES.to_string();
    let encode = |text: &str| percent_encoding::utf8_percent_encode(text, QUERY).to_string();
    let mut query: Vec<String> = passed
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(str::to_string)
        .collect();
    query.extend([
        "X-Amz-Algorithm=AWS4-HMAC-SHA256".to_string(),
        format!(
            "X-Amz-Credential={}",
            encode(&format!("{access_key}/{}", credential.scope()))
        ),
        format!("X-Amz-Date={amz_date}"),
        format!("X-Amz-Expires={expires}"),
        "X-Amz-SignedHeaders=host".to_string(),
    ]);
    let unmarked = query.join("&");
    let query = format!(
        "{unmarked}&{MARK}={}",
        crate::sigv4::sign(
            &MARK_KEY,
            &marked(method, access_key, host, path, &unmarked)
        )
    );
    let canonical = crate::sigv4::canonical_request(
        method,
        path,
        &query,
        &[("host".to_string(), host.to_string())],
        &["host".to_string()],
        "UNSIGNED-PAYLOAD",
    )
    .expect("the host is signed");
    let signature = crate::sigv4::sign(
        &crate::sigv4::signing_key(secret, &credential),
        &crate::sigv4::string_to_sign(&amz_date, &credential.scope(), &canonical),
    );
    format!("{scheme}://{host}{path}?{query}&X-Amz-Signature={signature}")
}

/// What a query value is encoded as: all but SigV4's unreserved characters.
const QUERY: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Whether `request` was made through a URL presigned for a function: the
/// very GET [`presign`] made (its access key, host, path and whole query,
/// the signing time and expiry among it), sent as it was presigned (so its
/// expiry was checked with its signature), by `auth`'s key and not a
/// session's. A request marked otherwise is refused; one not marked is not
/// one.
pub(crate) fn called_via<B>(request: &Request<B>, auth: &Authenticated) -> Result<bool, S3Error> {
    let query = request.uri().query().unwrap_or("");
    let name_of = |pair: &str| {
        pair.split_once('=')
            .map_or(pair, |(name, _)| name)
            .to_string()
    };
    let marks: Vec<&str> = query
        .split('&')
        .filter(|pair| name_of(pair).eq_ignore_ascii_case(MARK))
        .collect();
    if marks.is_empty() {
        return Ok(false);
    }
    let refused = || S3Error::access_denied("Access Denied");
    let (Some(principal), Some(signed)) = (&auth.principal, &auth.signed) else {
        return Err(refused());
    };
    let [mark] = marks.as_slice() else {
        return Err(refused());
    };
    let Some(given) = mark.strip_prefix(&format!("{MARK}=")) else {
        return Err(refused());
    };
    if signed.auth_type != "REST-QUERY-STRING"
        || auth.session.is_some()
        || !matches!(*request.method(), Method::GET | Method::HEAD)
    {
        return Err(refused());
    }
    let unmarked: Vec<&str> = query
        .split('&')
        .filter(|pair| {
            let name = name_of(pair);
            !name.eq_ignore_ascii_case(MARK) && !name.eq_ignore_ascii_case("X-Amz-Signature")
        })
        .collect();
    let host = request
        .headers()
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let wanted = marked(
        request.method().as_str(),
        &principal.access_key,
        host,
        request.uri().path(),
        &unmarked.join("&"),
    );
    match crate::sigv4::verify(&MARK_KEY, &wanted, given) {
        true => Ok(true),
        false => Err(refused()),
    }
}

/// The caller's answer from a function's WriteGetObjectResponse.
fn answer_response(answer: Answer, request_id: &str, resource: &str) -> Response<Body> {
    if let Some((code, message)) = answer.error {
        let escape = crate::access_points::escape;
        return Response::builder()
            .status(answer.status)
            .header("content-type", "application/xml")
            .body(full(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource><RequestId>{}</RequestId></Error>",
                escape(&code),
                escape(&message),
                escape(resource),
                escape(request_id)
            )))
            .expect("a valid response");
    }
    let mut builder = Response::builder().status(answer.status);
    for (name, value) in &answer.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder
        .body(crate::objects::ChannelBody(answer.body).boxed())
        .expect("a valid response")
}

/// A function's WriteGetObjectResponse: the answer of the GetObject its
/// token names, streamed to its caller. Authorized for
/// `s3-object-lambda:WriteGetObjectResponse`.
async fn write_get_object_response(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
) -> Result<Response<Body>, S3Error> {
    let Some(principal) = auth.principal.clone() else {
        return Err(S3Error::access_denied("Access Denied"));
    };
    let query = request.uri().query().unwrap_or("").to_string();
    let authz = Authz::load(
        state,
        Some(&principal),
        auth.signed,
        request.headers(),
        &query,
        request.extensions().get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    authz.check(
        "s3-object-lambda:WriteGetObjectResponse",
        crate::authz::Resource::Service,
    )?;
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let token = header("x-amz-request-token").unwrap_or_default();
    // Counted as its access point's function's answer, whatever it answers.
    let counted = lock_waiting()
        .get(&token)
        .and_then(|(_, counted)| counted.clone());
    if let Some(counted) = counted
        && let Some(seen) = crate::metering::seen_of(&request)
    {
        seen.mark_lambda(counted);
    }
    let status = match header("x-amz-fwd-status") {
        None => 200,
        Some(status) => status
            .parse::<u16>()
            .ok()
            .filter(|status| (200..600).contains(status))
            .ok_or_else(|| S3Error::invalid_request("x-amz-fwd-status is an HTTP status"))?,
    };
    let error = header("x-amz-fwd-error-code")
        .map(|code| (code, header("x-amz-fwd-error-message").unwrap_or_default()));
    let mut headers: Vec<(String, String)> = Vec::new();
    for (name, value) in request.headers() {
        let Ok(value) = value.to_str() else {
            continue;
        };
        let name = name.as_str();
        if let Some(forwarded) = name.strip_prefix("x-amz-fwd-header-") {
            if FORWARDED.contains(&forwarded) {
                headers.push((forwarded.to_string(), value.to_string()));
            }
        } else if name.starts_with("x-amz-meta-") {
            headers.push((name.to_string(), value.to_string()));
        }
    }
    // Its length given, so a body cut short (one that does not verify) is
    // seen as such by its caller.
    let length = header("x-amz-decoded-content-length").or_else(|| header("content-length"));
    if error.is_none() {
        let Some(length) = length else {
            return Err(S3Error::new(
                411,
                "MissingContentLength",
                "You must provide the Content-Length HTTP header.",
            ));
        };
        headers.push(("content-length".to_string(), length));
    }
    let Some((waiting, _)) = lock_waiting().remove(&token) else {
        return Err(S3Error::invalid_request(
            "The request token names no GetObject waiting for its answer",
        ));
    };
    let (sender, body) = mpsc::channel(4);
    let _ = waiting.send(Answer {
        status,
        error,
        headers,
        body,
    });
    // The body, verified as it comes: its last part held back until all of
    // it checks out, so a body that does not is cut short for its caller.
    let mut decoder = crate::payload::PayloadDecoder::new(auth.framing, auth.checksum)?;
    let mut incoming = request.into_body();
    let mut held: Option<Bytes> = None;
    while let Some(frame) = incoming.frame().await {
        let frame = frame.map_err(|err| S3Error::invalid_request(err.to_string()))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        let mut out = Vec::new();
        decoder.feed(&data, &mut out)?;
        if out.is_empty() {
            continue;
        }
        if let Some(previous) = held.replace(Bytes::from(out)) {
            // The caller gone: the rest is not wanted.
            if sender.send(previous).await.is_err() {
                return Ok(empty_ok());
            }
        }
    }
    decoder.finish(None)?;
    if let Some(last) = held {
        let _ = sender.send(last).await;
    }
    Ok(empty_ok())
}

fn empty_ok() -> Response<Body> {
    Response::builder()
        .status(200)
        .body(full(Vec::new()))
        .expect("a valid response")
}
