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
//! What else Object Lambda serves (HeadObject, ListObjects and
//! ListObjectsV2 transformed by their function's return value, and
//! requests no transformation names) is not built yet: 501.

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
/// A function's answer to its invocation, at most (it is not read).
const MAX_INVOCATION_ANSWER: usize = 1024 * 1024;

/// The key the marks of presigned URLs are made with: this process's own
/// (a URL outlives no restart).
static MARK_KEY: LazyLock<hmac::Key> = LazyLock::new(|| {
    hmac::Key::generate(hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
        .expect("the system's random source")
});

/// The GetObjects waiting for their function's WriteGetObjectResponse, by
/// their output token.
static WAITING: LazyLock<Mutex<HashMap<String, oneshot::Sender<Answer>>>> =
    LazyLock::new(Default::default);

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
    if request.method() == Method::POST && request.uri().path() == "/WriteGetObjectResponse" {
        return write_get_object_response(state, request, auth).await;
    }
    match resolve(state, &request, auth.principal.as_ref()).await? {
        Some(via) => serve(state, request, auth, via).await,
        None => Err(S3Error::invalid_request(
            "A request signed for s3-object-lambda names an Object Lambda access point",
        )),
    }
}

/// Serves `request` through Object Lambda access point `via`.
pub(crate) async fn serve(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    via: ViaLambda,
) -> Result<Response<Body>, S3Error> {
    let Some(principal) = auth.principal.clone() else {
        return Err(S3Error::access_denied("Access Denied"));
    };
    let key = match crate::api::named_target(&request)? {
        Target::Object(_, key) => key,
        _ => {
            return Err(S3Error::not_implemented(
                "Listings through an Object Lambda access point",
            ));
        }
    };
    if request.method() != Method::GET || crate::api::subresource(&request).is_some() {
        return Err(S3Error::not_implemented(
            "Operations through an Object Lambda access point other than GetObject",
        ));
    }
    let configuration = &via.configuration;
    let Some(transformation) = configuration.transformations.iter().find(|transformation| {
        transformation
            .actions
            .iter()
            .any(|action| action == "GetObject")
    }) else {
        return Err(S3Error::not_implemented(
            "A GetObject the Object Lambda access point does not transform",
        ));
    };
    // Ranges and parts only as its features allow (the function serves them).
    let allows = |feature: &str| configuration.features.iter().any(|known| known == feature);
    if request.headers().contains_key("range") && !allows("GetObject-Range") {
        return Err(S3Error::not_implemented(
            "A Range through this Object Lambda access point",
        ));
    }
    if crate::api::has_param(&request, "partNumber") && !allows("GetObject-PartNumber") {
        return Err(S3Error::not_implemented(
            "A partNumber through this Object Lambda access point",
        ));
    }
    let point = &via.point;
    let point_arn = lambda_arn(&state.region, &point.account, &point.name);
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
    let policies: Vec<&Policy> = via.policy.iter().collect();
    authz.check_arn_with(
        "s3-object-lambda:GetObject",
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
        return Err(S3Error::new(
            404,
            "NoSuchAccessPoint",
            "The Object Lambda access point's supporting access point does not exist",
        ));
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
    let request_id = request
        .extensions()
        .get::<RequestIds>()
        .map(|ids| ids.request_id.clone())
        .unwrap_or_default();
    let tls = request
        .extensions()
        .get::<Connection>()
        .is_some_and(|connection| connection.tls);
    let scheme = if tls { "https" } else { "http" };
    let host = request
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
    let input_url = presign(
        &format!("{domain}{port}"),
        scheme,
        &supporting.alias,
        &key,
        &query,
        &principal.access_key,
        &secret,
        &state.region,
    );
    let token = random_token();
    let event = serde_json::json!({
        "xAmzRequestId": request_id,
        "getObjectContext": {
            "inputS3Url": input_url,
            "outputRoute": ROUTE,
            "outputToken": token,
        },
        "configuration": {
            "accessPointArn": point_arn,
            "supportingAccessPointArn": configuration.supporting,
            "payload": transformation.payload.clone().unwrap_or_default(),
        },
        "userRequest": {
            "url": format!(
                "{scheme}://{host}{}",
                request.uri().path_and_query().map_or("/", |path| path.as_str())
            ),
            "headers": user_headers(&request),
        },
        "userIdentity": identity(&principal),
        "protocolVersion": "1.00",
    })
    .to_string();
    let (answered, answer) = oneshot::channel();
    lock_waiting().insert(token.clone(), answered);
    let (url, webhook_secret) = webhook;
    let delivery = format!("object-lambda/{request_id}");
    let invocation = tokio::spawn(async move {
        let Some(poster) = POSTER
            .get_or_init(|| truthdb_s3client::Poster::new(None).ok())
            .as_ref()
        else {
            return;
        };
        let secs = crate::objects::now_ms() / 1000;
        let headers = [
            ("content-type", "application/json".to_string()),
            ("x-truthdb-delivery", delivery.clone()),
            (
                "x-truthdb-signature",
                truthdb_s3client::webhook_signature(
                    &webhook_secret,
                    secs,
                    &delivery,
                    event.as_bytes(),
                ),
            ),
        ];
        // Its answer is not read: the function answers with
        // WriteGetObjectResponse.
        let _ = poster
            .post_answer(&url, &headers, Bytes::from(event), MAX_INVOCATION_ANSWER)
            .await;
    });
    let failed = || {
        S3Error::new(
            500,
            "InternalError",
            "The Object Lambda access point's function did not answer",
        )
    };
    let answer = tokio::select! {
        answer = answer => answer.ok(),
        _ = invocation => None,
        _ = tokio::time::sleep(ANSWER_WITHIN) => None,
    };
    let Some(answer) = answer else {
        lock_waiting().remove(&token);
        return Err(failed());
    };
    Ok(answer_response(answer, &request_id, request.uri().path()))
}

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

fn lock_waiting() -> std::sync::MutexGuard<'static, HashMap<String, oneshot::Sender<Answer>>> {
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

/// What the mark of a URL presigned for a function is of: its access key,
/// path, signing time and expiry.
fn marked(access_key: &str, path: &str, amz_date: &str, expires: &str) -> String {
    format!("{access_key}\n{path}\n{amz_date}\n{expires}")
}

/// A GetObject URL for `key` through the access point of alias `alias` at
/// `host`, with the caller's query (its version, its part), presigned with
/// `access_key` and its `secret` and marked as made for a function.
#[allow(clippy::too_many_arguments)]
fn presign(
    host: &str,
    scheme: &str,
    alias: &str,
    key: &str,
    caller_query: &str,
    access_key: &str,
    secret: &str,
    region: &str,
) -> String {
    let path = format!("/{alias}/{}", crate::sigv4::encode_key(key));
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
    let mut query: Vec<String> = caller_query
        .split('&')
        .filter(|pair| {
            let name = pair.split_once('=').map_or(*pair, |(name, _)| name);
            matches!(name, "versionId" | "partNumber")
        })
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
        format!(
            "{MARK}={}",
            crate::sigv4::sign(&MARK_KEY, &marked(access_key, &path, &amz_date, &expires))
        ),
    ]);
    let query = query.join("&");
    let canonical = crate::sigv4::canonical_request(
        "GET",
        &path,
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

/// Whether `request` was made through a URL presigned for a function (one
/// [`presign`] made, by `principal`'s key): its mark checks out. A request
/// marked otherwise is refused; one not marked is not one.
pub(crate) fn called_via<B>(
    request: &Request<B>,
    principal: Option<&crate::server::Principal>,
) -> Result<bool, S3Error> {
    let query = request.uri().query().unwrap_or("");
    let param = |name: &str| {
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            key.eq_ignore_ascii_case(name).then_some(value)
        })
    };
    let Some(given) = param(MARK) else {
        return Ok(false);
    };
    let refused = || S3Error::access_denied("Access Denied");
    let principal = principal.ok_or_else(refused)?;
    let (Some(amz_date), Some(expires)) = (param("X-Amz-Date"), param("X-Amz-Expires")) else {
        return Err(refused());
    };
    let wanted = marked(
        &principal.access_key,
        request.uri().path(),
        amz_date,
        expires,
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
    let Some(waiting) = lock_waiting().remove(&token) else {
        return Err(S3Error::invalid_request(
            "The request token names no GetObject waiting for its answer",
        ));
    };
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
            headers.push((forwarded.to_string(), value.to_string()));
        } else if name.starts_with("x-amz-meta-") {
            headers.push((name.to_string(), value.to_string()));
        }
    }
    let length = header("x-amz-decoded-content-length").or_else(|| header("content-length"));
    if let Some(length) = length.filter(|_| error.is_none()) {
        headers.push(("content-length".to_string(), length));
    }
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
