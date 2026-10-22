//! Requests whose signature covers their body although the SDKs send no
//! `x-amz-content-sha256` (S3 Vectors, IAM, STS): the body is read and
//! hashed before the signature can be checked. So the access key must exist
//! first, the read must finish within a minute, and the bytes of all bodies
//! not yet verified are bounded (past that, a request is told to retry).

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::Request;
use hyper::body::Incoming;
use hyper::http::request::Parts;

use crate::error::S3Error;
use crate::server::{Principal, S3State, check_signed_headers, signed_request, verify_signature};

/// How long a request body may take to arrive.
const BODY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Bytes of request bodies held before their signatures are checked, and
/// their bound: a client that knows only an access key id must send this
/// much within [`BODY_TIMEOUT`] to turn others away.
static UNVERIFIED: AtomicUsize = AtomicUsize::new(0);
const UNVERIFIED_MAX: usize = 16 * (20 << 20);

/// A request whose signature, body included, checked out.
pub struct SignedBody {
    pub parts: Parts,
    pub body: Bytes,
    pub principal: Principal,
}

/// Reads and verifies a request signed for `service` with a body of at most
/// `max_body` bytes. Errors: InvalidAccessKeyId (403), RequestTimeout
/// (408), EntityTooLarge (413), ServiceUnavailable (503), and the signature
/// checks' own.
pub async fn read(
    state: &S3State,
    request: Request<Incoming>,
    service: &str,
    max_body: usize,
) -> Result<SignedBody, S3Error> {
    let (auth, amz_date) = signed_request(state, &request, service)?;
    check_signed_headers(&request, &auth, &["host", "x-amz-date"], &[])?;
    // An unknown key is refused before its body is read.
    let access_key = auth.credential.access_key.clone();
    if state
        .engine
        .run_object(move |file: &mut truthdb_object::ObjFile<'_>| file.access_key(&access_key))
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .is_none()
    {
        return Err(S3Error::invalid_access_key());
    }
    let mut held = Unverified(0);
    let (parts, body) = request.into_parts();
    let body = tokio::time::timeout(BODY_TIMEOUT, read_body(body, &mut held, max_body))
        .await
        .map_err(|_| {
            S3Error::new(
                408,
                "RequestTimeout",
                "The request body took too long to arrive.",
            )
        })??;
    let hash = crate::sigv4::sha256_hex(&body);
    if let Some(sent) = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        && !sent.eq_ignore_ascii_case(&hash)
    {
        return Err(S3Error::signature_mismatch());
    }
    let (_, principal) = verify_signature(
        state,
        &parts.method,
        &parts.uri,
        &parts.headers,
        &auth,
        &amz_date,
        &hash,
        parts.uri.query().unwrap_or(""),
    )
    .await?;
    drop(held);
    Ok(SignedBody {
        parts,
        body,
        principal,
    })
}

/// Body bytes held before the signature is checked, counted in
/// [`UNVERIFIED`] until dropped.
struct Unverified(usize);

impl Unverified {
    fn take(&mut self, bytes: usize) -> bool {
        if UNVERIFIED.fetch_add(bytes, Ordering::Relaxed) + bytes > UNVERIFIED_MAX {
            UNVERIFIED.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        self.0 += bytes;
        true
    }
}

impl Drop for Unverified {
    fn drop(&mut self) {
        UNVERIFIED.fetch_sub(self.0, Ordering::Relaxed);
    }
}

/// Reads the body, counting its bytes in `held` as they arrive.
async fn read_body(
    mut body: Incoming,
    held: &mut Unverified,
    max_body: usize,
) -> Result<Bytes, S3Error> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| {
            S3Error::new(400, "InvalidRequest", "The request body could not be read.")
        })?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if out.len() + data.len() > max_body {
            return Err(S3Error::new(
                413,
                "EntityTooLarge",
                format!("The request body is larger than {max_body} bytes."),
            ));
        }
        if !held.take(data.len()) {
            return Err(S3Error::new(
                503,
                "ServiceUnavailable",
                "The service is busy; retry the request.",
            ));
        }
        out.extend_from_slice(&data);
    }
    Ok(Bytes::from(out))
}
