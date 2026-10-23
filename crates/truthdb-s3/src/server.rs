//! The HTTP listener: every request is authenticated with SigV4 against the
//! object engine's access keys, then routed. Path-style addressing
//! (`/bucket/key`); the region is fixed by configuration.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::watch;
use truthdb_engine::session::EngineHandle;
use truthdb_object::ObjFile;

use crate::error::S3Error;
use crate::payload::{Framing, PayloadDecoder};
use crate::sigv4;

/// How far a request's `x-amz-date` may be from the server's clock.
const MAX_SKEW_SECS: u64 = 15 * 60;
/// The headers that carry a full-body checksum (not x-amz-checksum-type,
/// -mode or -algorithm, which describe one).
pub(crate) const CHECKSUM_HEADERS: [&str; 5] = [
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
];
/// Connections served at once; more wait in the accept queue.
const MAX_CONNECTIONS: usize = 1024;
/// How long a client may take to send its request head, or its TLS
/// handshake.
const HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub type Body = BoxBody<Bytes, Infallible>;

/// What a request handler needs.
pub struct S3State {
    pub engine: EngineHandle,
    pub region: String,
    pub signing_keys: std::sync::Arc<sigv4::SigningKeyCache>,
    /// The database itself, for what is read without the engine (whether it
    /// is encrypted at rest).
    pub storage: std::sync::Arc<truthdb_core::storage::Storage>,
    /// How long a restore of an archived object takes, per retrieval tier.
    pub restore_delays: crate::restore::RestoreDelays,
    /// The domains whose subdomains name buckets (virtual-hosted
    /// addressing), in lowercase.
    pub domains: Vec<String>,
}

/// Who sent a request: an account, acting as its root or as one of its
/// users.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The access key the request was signed with.
    pub access_key: String,
    pub account: String,
    pub user: Option<String>,
}

/// The connection a request arrived on (in the request's extensions): the
/// policy conditions `aws:SourceIp` and `aws:SecureTransport`.
#[derive(Debug, Clone, Copy)]
pub struct Connection {
    pub peer: std::net::SocketAddr,
    pub tls: bool,
}

/// How a request was signed: in its headers (`REST-HEADER`) or its query
/// string (`REST-QUERY-STRING`, a presigned URL), and when (from its
/// `x-amz-date`, in ms), for the policy condition keys `s3:authType` and
/// `s3:signatureAge`.
#[derive(Debug, Clone, Copy)]
pub struct Signed {
    pub auth_type: &'static str,
    pub at_ms: Option<u64>,
}

/// A request that passed authentication: who sent it (`None`: an anonymous
/// request, signed by no one) and how to read its body.
pub struct Authenticated {
    pub principal: Option<Principal>,
    /// How it was signed (`None` when anonymous).
    pub signed: Option<Signed>,
    pub framing: Framing,
    /// The full-body checksum header's name and (for a header checksum) value.
    pub checksum: Option<(String, Option<String>)>,
}

/// Serves S3 requests on `listener` (over TLS when `tls` is set) until
/// `shutdown` turns true.
pub async fn serve(
    listener: TcpListener,
    state: Arc<S3State>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    mut shutdown: watch::Receiver<bool>,
) {
    // Batch Operations jobs run beside the listener.
    tokio::spawn(crate::batch_run::run(Arc::clone(&state), shutdown.clone()));
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    loop {
        let slot = tokio::select! {
            slot = Arc::clone(&slots).acquire_owned() => slot.expect("the semaphore is never closed"),
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
                continue;
            }
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(_) => {
                    // Out of descriptors, say: back off instead of spinning.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
                continue;
            }
        };
        let (stream, peer) = stream;
        let state = Arc::clone(&state);
        let tls = tls.clone();
        tokio::spawn(async move {
            let connection = Connection {
                peer,
                tls: tls.is_some(),
            };
            match tls {
                Some(acceptor) => {
                    if let Ok(Ok(stream)) =
                        tokio::time::timeout(HEAD_TIMEOUT, acceptor.accept(stream)).await
                    {
                        serve_connection(stream, state, connection).await;
                    }
                }
                None => serve_connection(stream, state, connection).await,
            }
            drop(slot);
        });
    }
}

async fn serve_connection<I>(io: I, state: Arc<S3State>, connection: Connection)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |mut request: Request<Incoming>| {
        request.extensions_mut().insert(connection);
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(handle(&state, request).await) }
    });
    let _ = http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HEAD_TIMEOUT)
        .serve_connection(TokioIo::new(io), service)
        .await;
}

/// A request's identifiers, as S3 gives them: `x-amz-request-id` (16
/// uppercase hex digits) and `x-amz-id-2` (the "host id", base64), on every
/// answer and in error documents. Kept in the request's extensions.
#[derive(Debug, Clone)]
pub struct RequestIds {
    pub request_id: String,
    pub host_id: String,
}

impl RequestIds {
    fn new() -> Self {
        use base64::Engine as _;
        use ring::rand::SecureRandom;
        let mut bytes = [0u8; 8 + 24];
        // No randomness: identifiers of zeros (they only label the request).
        let _ = ring::rand::SystemRandom::new().fill(&mut bytes);
        RequestIds {
            request_id: bytes[..8]
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect(),
            host_id: base64::engine::general_purpose::STANDARD.encode(&bytes[8..]),
        }
    }
}

async fn handle(state: &S3State, mut request: Request<Incoming>) -> Response<Body> {
    let resource = request.uri().path().to_string();
    // Virtual-hosted addressing: the bucket named by the Host, decided once
    // for every reader of the request's target.
    let host = request
        .headers()
        .get(hyper::header::HOST)
        .and_then(|host| host.to_str().ok())
        .or_else(|| {
            request
                .uri()
                .authority()
                .map(|authority| authority.as_str())
        });
    if let Some(bucket) = host.and_then(|host| crate::api::host_bucket(host, &state.domains)) {
        request
            .extensions_mut()
            .insert(crate::api::VirtualHost(bucket));
    }
    let ids = RequestIds::new();
    request.extensions_mut().insert(ids.clone());
    // A cross-origin request (not a preflight, which answers for itself):
    // its answer, an error too, carries the CORS headers its bucket's
    // configuration allows.
    let cross_origin = (request.method() != hyper::Method::OPTIONS)
        .then(|| {
            request
                .headers()
                .get("origin")?
                .to_str()
                .ok()
                .map(str::to_string)
        })
        .flatten()
        .and_then(|origin| match crate::api::target(&request).ok()? {
            crate::api::Target::Bucket(bucket) | crate::api::Target::Object(bucket, _) => {
                Some((bucket, origin, request.method().clone()))
            }
            crate::api::Target::Service => None,
        });
    let mut response = match route(state, request).await {
        Ok(response) => response,
        Err(err) => error_response(&err, &resource, &ids),
    };
    if let Some((bucket, origin, method)) = cross_origin {
        let headers = response.headers_mut();
        for (name, value) in crate::cors::answer_headers(state, &bucket, &origin, &method).await {
            if let Ok(value) = hyper::header::HeaderValue::from_str(&value) {
                headers.insert(name, value);
            }
        }
    }
    let headers = response.headers_mut();
    for (name, value) in [
        ("x-amz-request-id", &ids.request_id),
        ("x-amz-id-2", &ids.host_id),
    ] {
        if let Ok(value) = hyper::header::HeaderValue::from_str(value) {
            headers.insert(name, value);
        }
    }
    response
}

pub fn error_response(err: &S3Error, resource: &str, ids: &RequestIds) -> Response<Body> {
    let mut builder = Response::builder()
        .status(StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/xml");
    for (name, value) in &err.headers {
        builder = builder.header(*name, value);
    }
    builder
        .body(full(err.to_xml(resource, ids)))
        .expect("a valid response")
}

pub fn full(text: impl Into<Bytes>) -> Body {
    Full::new(text.into()).boxed()
}

async fn route(state: &S3State, mut request: Request<Incoming>) -> Result<Response<Body>, S3Error> {
    // S3 Vectors' JSON API on the same listener, told apart by the
    // service its requests are signed for.
    match request_service(&request).as_deref() {
        Some("s3vectors") => return Ok(crate::vectors::handle(state, request).await),
        // IAM and STS (the Query protocol), told apart the same way.
        Some(service @ ("iam" | "sts")) => {
            let service = service.to_string();
            return Ok(crate::iam_api::handle(state, request, &service).await);
        }
        _ => {}
    }
    // The signature covers parameter names decoded, while operations are
    // told apart by their names as written: a name must not be encoded, or
    // `%74agging` would be signed as `tagging` and served as something else.
    if request.uri().query().unwrap_or("").split('&').any(|pair| {
        pair.split('=')
            .next()
            .is_some_and(|name| name.contains('%'))
    }) {
        return Err(S3Error::new(
            400,
            "InvalidArgument",
            "A query parameter name must not be percent-encoded",
        ));
    }
    // A CORS preflight: answered from the bucket's configuration, before
    // (and without) any authentication, as a browser sends it.
    if request.method() == hyper::Method::OPTIONS {
        return crate::cors::preflight(state, &request).await;
    }
    // A form upload (POST Object) carries its signature in the form.
    if request.method() == hyper::Method::POST
        && request.uri().query().is_none()
        && !request.headers().contains_key("authorization")
        && crate::post::is_form(&request)
        && let crate::api::Target::Bucket(bucket) = crate::api::target(&request)?
    {
        return crate::post::post_object(state, request, bucket).await;
    }
    let auth = match anonymous(&request)? {
        Some(anonymous) => anonymous,
        None if request.headers().contains_key("authorization") => {
            authenticate(state, &request).await?
        }
        None => authenticate_presigned(state, &mut request).await?,
    };
    crate::api::dispatch(state, request, auth).await
}

/// The query parameters of a presigned URL that authenticate it.
const QUERY_AUTH_PARAMS: [&str; 8] = [
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-date",
    "x-amz-expires",
    "x-amz-signedheaders",
    "x-amz-signature",
    "x-amz-security-token",
    "x-amz-content-sha256",
];

/// Moves a presigned URL's other `x-amz-*` query parameters into the
/// request's headers, as S3 reads them: the SDKs' presigners carry headers
/// such as `x-amz-meta-*`, `x-amz-tagging`, `x-amz-storage-class` and
/// `x-amz-acl` in the query, where the signature covers them; values are
/// trimmed as header values are. A parameter given twice, or given as a
/// header with another value, is refused, and so is a customer-provided key
/// (SSE-C): key material does not belong in a URL.
fn hoist_query_headers(request: &mut Request<Incoming>) -> Result<(), S3Error> {
    let invalid = |why: String| S3Error::new(400, "InvalidArgument", why);
    let query = request.uri().query().unwrap_or("").to_string();
    let mut hoisted: Vec<(hyper::header::HeaderName, hyper::header::HeaderValue)> = Vec::new();
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let lower = name.to_ascii_lowercase();
        if !lower.starts_with("x-amz-") || QUERY_AUTH_PARAMS.contains(&lower.as_str()) {
            continue;
        }
        if crate::ssec::UNSIGNED_IN_PRESIGNED.contains(&lower.as_str()) {
            return Err(invalid(format!(
                "{lower} must be sent as a header, not in the URL"
            )));
        }
        let value = percent_encoding::percent_decode_str(value)
            .decode_utf8()
            .map_err(|_| invalid(format!("The {lower} parameter is not UTF-8")))?;
        let value = value.trim_matches([' ', '\t']);
        let name = hyper::header::HeaderName::from_bytes(lower.as_bytes())
            .map_err(|_| invalid(format!("{lower} is not a header name")))?;
        let value = hyper::header::HeaderValue::from_str(value)
            .map_err(|_| invalid(format!("The {lower} parameter is not a valid header value")))?;
        if hoisted.iter().any(|(given, _)| *given == name) {
            return Err(invalid(format!("The {lower} parameter is given twice")));
        }
        if request
            .headers()
            .get(&name)
            .is_some_and(|header| *header != value)
        {
            return Err(invalid(format!(
                "{lower} is given as a header and as a different query parameter"
            )));
        }
        hoisted.push((name, value));
    }
    for (name, value) in hoisted {
        request.headers_mut().insert(name, value);
    }
    Ok(())
}

fn header<'r>(request: &'r Request<Incoming>, name: &str) -> Option<&'r str> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

/// A request's parsed SigV4 authorization and its `x-amz-date`, checked
/// for `service` in this server's region and within the allowed clock skew.
pub fn signed_request(
    state: &S3State,
    request: &Request<Incoming>,
    service: &str,
) -> Result<(sigv4::AuthHeader, String), S3Error> {
    let authorization = header(request, "authorization")
        .ok_or_else(|| S3Error::access_denied("anonymous requests are not supported"))?;
    let auth =
        sigv4::parse_authorization(authorization).map_err(|sigv4::AuthError::Malformed(why)| {
            S3Error::new(400, "AuthorizationHeaderMalformed", why)
        })?;
    let amz_date = header(request, "x-amz-date")
        .ok_or_else(|| S3Error::access_denied("the request has no x-amz-date"))?;
    let signed_at = parse_amz_date(amz_date)
        .ok_or_else(|| S3Error::access_denied("x-amz-date is not YYYYMMDDTHHMMSSZ"))?;
    // IAM is global: the SDKs sign it (and STS's global endpoint) for
    // us-east-1, whatever this server's region.
    let region_ok = auth.credential.region == state.region
        || (matches!(service, "iam" | "sts") && auth.credential.region == "us-east-1");
    if !amz_date.starts_with(&auth.credential.date)
        || auth.credential.service != service
        || !region_ok
    {
        return Err(S3Error::new(
            400,
            "AuthorizationHeaderMalformed",
            format!(
                "the credential scope must be {}/{}/{service}/aws4_request",
                &amz_date[..8],
                state.region
            ),
        ));
    }
    if now_secs().abs_diff(signed_at) > MAX_SKEW_SECS {
        return Err(S3Error::new(
            403,
            "RequestTimeTooSkewed",
            "The difference between the request time and the server's time is too large.",
        ));
    }
    Ok((auth, amz_date.to_string()))
}

/// The SigV4 credential scope's service of a request, when it has one.
pub fn request_service(request: &Request<Incoming>) -> Option<String> {
    let authorization = header(request, "authorization")?;
    sigv4::parse_authorization(authorization)
        .ok()
        .map(|auth| auth.credential.service)
}

/// Checks that the host, every `x-amz-*` header present, and `required`
/// are signed: as AWS requires, so none can be changed in transit or the
/// request replayed against another endpoint.
pub fn check_signed_headers(
    request: &Request<Incoming>,
    auth: &sigv4::AuthHeader,
    required: &[&str],
    may_be_unsigned: &[&str],
) -> Result<(), S3Error> {
    for name in request
        .headers()
        .keys()
        .map(|name| name.as_str())
        .filter(|name| *name == "host" || name.starts_with("x-amz-"))
        .filter(|name| !may_be_unsigned.contains(name))
        .chain(required.iter().copied())
    {
        if !auth.signed_headers.iter().any(|signed| signed == name) {
            return Err(S3Error::access_denied(format!(
                "the {name} header must be signed"
            )));
        }
    }
    Ok(())
}

/// Verifies the request's signature over `payload_hash` against the access
/// key's secret; returns the signing key and who the key acts for.
#[allow(clippy::too_many_arguments)]
pub async fn verify_signature(
    state: &S3State,
    method: &hyper::Method,
    uri: &hyper::Uri,
    request_headers: &hyper::HeaderMap,
    auth: &sigv4::AuthHeader,
    amz_date: &str,
    payload_hash: &str,
    raw_query: &str,
) -> Result<(ring::hmac::Key, Principal), S3Error> {
    let access_key = auth.credential.access_key.clone();
    let (key, secret) = state
        .engine
        .run_object(move |file: &mut ObjFile<'_>| file.access_key(&access_key))
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .filter(|(key, _)| key.active)
        .ok_or_else(S3Error::invalid_access_key)?;
    let principal = Principal {
        access_key: key.id,
        account: key.account,
        user: key.user,
    };

    let headers: Vec<(String, String)> = request_headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();
    let canonical = sigv4::canonical_request_for(
        &auth.credential.service,
        method.as_str(),
        uri.path(),
        raw_query,
        &headers,
        &auth.signed_headers,
        payload_hash,
    )
    .map_err(|sigv4::AuthError::Malformed(why)| {
        S3Error::new(400, "AuthorizationHeaderMalformed", why)
    })?;
    let key = state.signing_keys.signing_key(&secret, &auth.credential);
    if !sigv4::verify(
        &key,
        &sigv4::string_to_sign(amz_date, &auth.credential.scope(), &canonical),
        &auth.signature,
    ) {
        return Err(S3Error::signature_mismatch());
    }
    Ok((key, principal))
}

/// Checks the request's SigV4 signature against the access key's secret.
pub async fn authenticate(
    state: &S3State,
    request: &Request<Incoming>,
) -> Result<Authenticated, S3Error> {
    let (auth, amz_date) = signed_request(state, request, "s3")?;
    let amz_date = amz_date.as_str();
    let payload_hash = header(request, "x-amz-content-sha256")
        .ok_or_else(|| S3Error::invalid_request("the request has no x-amz-content-sha256"))?
        .to_string();
    check_signed_headers(
        request,
        &auth,
        &["host", "x-amz-date", "x-amz-content-sha256"],
        &[],
    )?;
    let (key, principal) = verify_signature(
        state,
        request.method(),
        request.uri(),
        request.headers(),
        &auth,
        amz_date,
        &payload_hash,
        request.uri().query().unwrap_or(""),
    )
    .await?;
    let scope = auth.credential.scope();

    let framing = match payload_hash.as_str() {
        sigv4::UNSIGNED_PAYLOAD => Framing::Unsigned,
        sigv4::STREAMING_PAYLOAD => Framing::SignedChunks {
            key: Box::new(key),
            amz_date: amz_date.to_string(),
            scope,
            previous: auth.signature.clone(),
        },
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Framing::UnsignedTrailer,
        hash if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) => {
            Framing::Sha256(hash.to_ascii_lowercase())
        }
        other => {
            return Err(S3Error::not_implemented(format!(
                "x-amz-content-sha256 {other} is not supported"
            )));
        }
    };
    Ok(Authenticated {
        principal: Some(principal),
        signed: Some(Signed {
            auth_type: "REST-HEADER",
            at_ms: parse_amz_date(amz_date).map(|secs| secs * 1000),
        }),
        framing,
        checksum: checksum_of(request),
    })
}

/// The longest a presigned URL may be valid: seven days.
const MAX_PRESIGNED_EXPIRES: u64 = 7 * 24 * 60 * 60;

/// Checks a presigned request: its query-string SigV4 signature (over the
/// query without `X-Amz-Signature`, and an unsigned payload unless the URL
/// fixes its hash), within `X-Amz-Expires` (at most seven days) of its
/// `X-Amz-Date`. The access key is looked up as the request arrives, so a
/// dropped or deactivated key revokes its URLs. Once verified, the signed
/// `x-amz-*` query parameters become the request's headers (see
/// [`hoist_query_headers`]).
pub async fn authenticate_presigned(
    state: &S3State,
    request: &mut Request<Incoming>,
) -> Result<Authenticated, S3Error> {
    let raw_query = request.uri().query().unwrap_or("");
    let query =
        sigv4::parse_query_auth(raw_query).map_err(|sigv4::AuthError::Malformed(why)| {
            S3Error::new(400, "AuthorizationQueryParametersError", why)
        })?;
    if query.security_token {
        return Err(S3Error::access_denied(
            "temporary credentials (X-Amz-Security-Token) are not supported",
        ));
    }
    if !(1..=MAX_PRESIGNED_EXPIRES).contains(&query.expires) {
        return Err(S3Error::new(
            400,
            "AuthorizationQueryParametersError",
            "X-Amz-Expires must be from 1 to 604800 seconds",
        ));
    }
    let signed_at = parse_amz_date(&query.amz_date).ok_or_else(|| {
        S3Error::new(
            400,
            "AuthorizationQueryParametersError",
            "X-Amz-Date is not YYYYMMDDTHHMMSSZ",
        )
    })?;
    let credential = &query.auth.credential;
    if !query.amz_date.starts_with(&credential.date)
        || credential.service != "s3"
        || credential.region != state.region
    {
        return Err(S3Error::new(
            400,
            "AuthorizationQueryParametersError",
            format!(
                "the credential scope must be {}/{}/s3/aws4_request",
                &query.amz_date[..8.min(query.amz_date.len())],
                state.region
            ),
        ));
    }
    let now = now_secs();
    if signed_at > now + MAX_SKEW_SECS {
        return Err(S3Error::access_denied("Request is not valid yet"));
    }
    if now > signed_at + query.expires {
        return Err(S3Error::access_denied("Request has expired"));
    }
    // A presigned URL is made without the customer-provided key: only its
    // algorithm is signed, the key and its MD5 come with the request (as S3
    // documents), and the key is checked against the object's own.
    check_signed_headers(
        request,
        &query.auth,
        &["host"],
        &crate::ssec::UNSIGNED_IN_PRESIGNED,
    )?;
    let payload_hash = query
        .content_sha256
        .clone()
        .unwrap_or_else(|| sigv4::UNSIGNED_PAYLOAD.to_string());
    let framing = match payload_hash.as_str() {
        sigv4::UNSIGNED_PAYLOAD => Framing::Unsigned,
        hash if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) => {
            Framing::Sha256(hash.to_ascii_lowercase())
        }
        other => {
            return Err(S3Error::new(
                400,
                "AuthorizationQueryParametersError",
                format!("X-Amz-Content-Sha256 {other} is not supported in a presigned URL"),
            ));
        }
    };
    let (_, principal) = verify_signature(
        state,
        request.method(),
        request.uri(),
        request.headers(),
        &query.auth,
        &query.amz_date,
        &payload_hash,
        &sigv4::query_without_signature(raw_query),
    )
    .await?;
    hoist_query_headers(request)?;
    Ok(Authenticated {
        principal: Some(principal),
        signed: Some(Signed {
            auth_type: "REST-QUERY-STRING",
            at_ms: Some(signed_at * 1000),
        }),
        framing,
        checksum: checksum_of(request),
    })
}

/// The full-body checksum a request names: a trailer's name, or a checksum
/// header's name and value.
fn checksum_of(request: &Request<Incoming>) -> Option<(String, Option<String>)> {
    match header(request, "x-amz-trailer") {
        Some(name) => Some((name.trim().to_string(), None)),
        None => request
            .headers()
            .iter()
            .find(|(name, _)| CHECKSUM_HEADERS.contains(&name.as_str()))
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().ok().map(str::to_string),
                )
            }),
    }
}

/// Whether a request is anonymous: no `Authorization` header (readable or
/// not) and no query-string signature parameter (in any spelling). Its body
/// is checked against the `x-amz-content-sha256` it claims, if any.
fn anonymous(request: &Request<Incoming>) -> Result<Option<Authenticated>, S3Error> {
    let presigned = request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|pair| pair.split('=').next())
        .map(|name| {
            percent_encoding::percent_decode_str(name)
                .decode_utf8_lossy()
                .to_ascii_lowercase()
        })
        .any(|name| {
            matches!(
                name.as_str(),
                "x-amz-algorithm" | "x-amz-credential" | "x-amz-signature" | "x-amz-signedheaders"
            )
        });
    if request.headers().contains_key("authorization") || presigned {
        return Ok(None);
    }
    let framing = match header(request, "x-amz-content-sha256") {
        None | Some(sigv4::UNSIGNED_PAYLOAD) => Framing::Unsigned,
        Some(hash) if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) => {
            Framing::Sha256(hash.to_ascii_lowercase())
        }
        Some(other) => {
            return Err(S3Error::invalid_request(format!(
                "x-amz-content-sha256 {other} needs a signed request"
            )));
        }
    };
    Ok(Some(Authenticated {
        principal: None,
        signed: None,
        framing,
        checksum: checksum_of(request),
    }))
}

/// Reads and verifies a whole (small) request body.
pub async fn read_body(
    request: Request<Incoming>,
    auth: Authenticated,
    limit: usize,
) -> Result<Vec<u8>, S3Error> {
    let declared = request
        .headers()
        .get("x-amz-decoded-content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());
    let mut decoder = PayloadDecoder::new(auth.framing, auth.checksum)?;
    let mut body = request.into_body();
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|err| S3Error::invalid_request(err.to_string()))?;
        if let Ok(data) = frame.into_data() {
            decoder.feed(&data, &mut out)?;
            if out.len() > limit {
                return Err(S3Error::new(
                    400,
                    "EntityTooLarge",
                    "the request body is too large",
                ));
            }
        }
    }
    decoder.finish(declared)?;
    Ok(out)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `YYYYMMDDTHHMMSSZ` as Unix seconds.
pub(crate) fn parse_amz_date(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| value.get(range)?.parse::<u64>().ok();
    let (year, month, day) = (number(0..4)?, number(4..6)?, number(6..8)?);
    let (hour, minute, second) = (number(9..11)?, number(11..13)?, number(13..15)?);
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    Some(days_from_civil(year, month, day)? * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
/// algorithm).
fn days_from_civil(year: u64, month: u64, day: u64) -> Option<u64> {
    let year = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = year / 400;
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    (era * 146_097 + day_of_era).checked_sub(719_468)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amz_dates_parse_to_unix_seconds() {
        assert_eq!(parse_amz_date("19700101T000000Z"), Some(0));
        assert_eq!(parse_amz_date("20130524T000000Z"), Some(1_369_353_600));
        assert_eq!(parse_amz_date("20000229T235959Z"), Some(951_868_799));
        assert_eq!(parse_amz_date("2013-05-24T00:00:00Z"), None);
        assert_eq!(parse_amz_date("20131324T000000Z"), None);
    }
}
