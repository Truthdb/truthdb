//! Requester Pays buckets: Put and GetBucketRequestPayment (`?requestPayment`,
//! the bucket owner's to set: `Requester` or `BucketOwner`, the default),
//! and what a Requester Pays bucket asks of a request. A request from
//! outside the bucket owner's account must say its requester pays
//! (`x-amz-request-payer: requester`, the header or, presigned, the query
//! parameter), or it is refused with a 403; an anonymous one always is.
//! Its successful answer then says so (`x-amz-request-charged:
//! requester`), and it is counted for its requester's account (see
//! `sys.dm_s3_requester_charges`). There is no billing.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::BUCKET_REQUEST_PAYMENT;

use crate::authz::Authz;
use crate::error::S3Error;
use crate::server::{Authenticated, Body, S3State, full, read_body};

/// Whether `bucket` is a Requester Pays bucket.
pub fn requester_pays(
    file: &mut truthdb_object::ObjFile<'_>,
    bucket: &str,
) -> Result<bool, truthdb_core::storage::StorageError> {
    Ok(file
        .bucket_config(bucket, BUCKET_REQUEST_PAYMENT, "")?
        .is_some_and(|document| document.first() == Some(&1)))
}

fn malformed() -> S3Error {
    S3Error::new(
        400,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema",
    )
}

/// A RequestPaymentConfiguration's Payer: whether it is the requester.
fn parse(body: &[u8]) -> Result<bool, S3Error> {
    let elements = crate::access_points::elements(body)?;
    if elements.first().map(|(path, _)| path.as_str()) != Some("RequestPaymentConfiguration") {
        return Err(malformed());
    }
    let mut payer = None;
    for (path, text) in &elements[1..] {
        match path.as_str() {
            "RequestPaymentConfiguration/Payer" if payer.is_none() => {
                payer = Some(match text.trim() {
                    "Requester" => true,
                    "BucketOwner" => false,
                    _ => return Err(malformed()),
                });
            }
            _ => return Err(malformed()),
        }
    }
    payer.ok_or_else(malformed)
}

/// PUT or GET `?requestPayment` on `bucket` (of `owner`), by `account`.
pub async fn handle(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
    account: Option<String>,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    let Some(info) = crate::scope::object(state, move |file| file.bucket(&name)).await? else {
        return Err(S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {bucket} does not exist"),
        ));
    };
    match method {
        Method::PUT => {
            // "Only the bucket owner can set" it.
            if account.as_deref() != Some(info.owner.as_str()) {
                return Err(S3Error::access_denied("Access Denied"));
            }
            let body = read_body(request, auth, 64 * 1024).await?;
            let requester = parse(&body)?;
            crate::scope::object(state, move |file| {
                file.transaction(|tx| match requester {
                    true => tx.put_bucket_config(&bucket, BUCKET_REQUEST_PAYMENT, "", &[1]),
                    false => tx
                        .delete_bucket_config(&bucket, BUCKET_REQUEST_PAYMENT, "")
                        .map(|_| ()),
                })
            })
            .await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
        _ => {
            let requester =
                crate::scope::object(state, move |file| requester_pays(file, &bucket)).await?;
            let payer = if requester {
                "Requester"
            } else {
                "BucketOwner"
            };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<RequestPaymentConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Payer>{payer}</Payer></RequestPaymentConfiguration>"
                )))
                .expect("a valid response"))
        }
    }
}

/// What the Requester Pays buckets `request` names (its own, and a copy's
/// source) ask of it, as `authz` loaded them: refused (403) if one is a
/// Requester Pays bucket of another account than the caller's and the
/// request does not say its requester pays (an anonymous one always is);
/// else the Requester Pays buckets its requester is charged by.
pub(crate) fn check<B>(request: &Request<B>, authz: &Authz) -> Result<Vec<String>, S3Error> {
    let says = request
        .headers()
        .get("x-amz-request-payer")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("requester"));
    let caller = authz.identity.account();
    let mut charged = Vec::new();
    for (bucket, owner) in authz.requester_pays() {
        if caller == Some(owner.as_str()) {
            continue;
        }
        if caller.is_none() || !says {
            return Err(S3Error::access_denied("Access Denied"));
        }
        charged.push(bucket);
    }
    Ok(charged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payment_configuration_names_its_payer() {
        let body = |payer: &str| {
            format!(
                "<RequestPaymentConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Payer>{payer}</Payer></RequestPaymentConfiguration>"
            )
        };
        assert!(parse(body("Requester").as_bytes()).unwrap());
        assert!(!parse(body("BucketOwner").as_bytes()).unwrap());
        for bad in [
            body("Someone"),
            body("requester"),
            "<RequestPaymentConfiguration/>".to_string(),
            "<Other><Payer>Requester</Payer></Other>".to_string(),
            "<RequestPaymentConfiguration><Payer>Requester</Payer><Payer>Requester</Payer></RequestPaymentConfiguration>".to_string(),
        ] {
            assert!(parse(bad.as_bytes()).is_err(), "{bad}");
        }
    }
}
