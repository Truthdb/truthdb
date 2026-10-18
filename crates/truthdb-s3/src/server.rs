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
}

/// A request that passed authentication: who sent it and how to read its body.
pub struct Authenticated {
    pub access_key: String,
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
                Ok((stream, _)) => stream,
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
        let state = Arc::clone(&state);
        let tls = tls.clone();
        tokio::spawn(async move {
            match tls {
                Some(acceptor) => {
                    if let Ok(Ok(stream)) =
                        tokio::time::timeout(HEAD_TIMEOUT, acceptor.accept(stream)).await
                    {
                        serve_connection(stream, state).await;
                    }
                }
                None => serve_connection(stream, state).await,
            }
            drop(slot);
        });
    }
}

async fn serve_connection<I>(io: I, state: Arc<S3State>)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(handle(&state, request).await) }
    });
    let _ = http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HEAD_TIMEOUT)
        .serve_connection(TokioIo::new(io), service)
        .await;
}

async fn handle(state: &S3State, request: Request<Incoming>) -> Response<Body> {
    let resource = request.uri().path().to_string();
    match route(state, request).await {
        Ok(response) => response,
        Err(err) => error_response(&err, &resource),
    }
}

pub fn error_response(err: &S3Error, resource: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/xml")
        .body(full(err.to_xml(resource)))
        .expect("a valid response")
}

pub fn full(text: impl Into<Bytes>) -> Body {
    Full::new(text.into()).boxed()
}

async fn route(state: &S3State, request: Request<Incoming>) -> Result<Response<Body>, S3Error> {
    let auth = authenticate(state, &request).await?;
    crate::api::dispatch(state, request, auth).await
}

fn header<'r>(request: &'r Request<Incoming>, name: &str) -> Option<&'r str> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

/// Checks the request's SigV4 signature against the access key's secret.
pub async fn authenticate(
    state: &S3State,
    request: &Request<Incoming>,
) -> Result<Authenticated, S3Error> {
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
    if !amz_date.starts_with(&auth.credential.date)
        || auth.credential.service != "s3"
        || auth.credential.region != state.region
    {
        return Err(S3Error::new(
            400,
            "AuthorizationHeaderMalformed",
            format!(
                "the credential scope must be {}/{}/s3/aws4_request",
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
    let payload_hash = header(request, "x-amz-content-sha256")
        .ok_or_else(|| S3Error::invalid_request("the request has no x-amz-content-sha256"))?
        .to_string();

    // As AWS requires: the host and every x-amz-* header present are signed,
    // so none of them can be changed in transit or the request replayed
    // against another endpoint.
    for name in request
        .headers()
        .keys()
        .map(|name| name.as_str())
        .filter(|name| *name == "host" || name.starts_with("x-amz-"))
        .chain(["host", "x-amz-date", "x-amz-content-sha256"])
    {
        if !auth.signed_headers.iter().any(|signed| signed == name) {
            return Err(S3Error::access_denied(format!(
                "the {name} header must be signed"
            )));
        }
    }

    let access_key = auth.credential.access_key.clone();
    let secret = state
        .engine
        .run_object(move |file: &mut ObjFile<'_>| file.access_key_secret(&access_key))
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .ok_or_else(S3Error::invalid_access_key)?;

    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();
    let canonical = sigv4::canonical_request(
        request.method().as_str(),
        request.uri().path(),
        request.uri().query().unwrap_or(""),
        &headers,
        &auth.signed_headers,
        &payload_hash,
    )
    .map_err(|sigv4::AuthError::Malformed(why)| {
        S3Error::new(400, "AuthorizationHeaderMalformed", why)
    })?;
    let scope = auth.credential.scope();
    let key = sigv4::signing_key(&secret, &auth.credential);
    if !sigv4::verify(
        &key,
        &sigv4::string_to_sign(amz_date, &scope, &canonical),
        &auth.signature,
    ) {
        return Err(S3Error::signature_mismatch());
    }

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
    let checksum = match header(request, "x-amz-trailer") {
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
    };
    Ok(Authenticated {
        access_key: auth.credential.access_key,
        framing,
        checksum,
    })
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
fn parse_amz_date(value: &str) -> Option<u64> {
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
