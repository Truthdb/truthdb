//! Transfer acceleration: Put and GetBucketAccelerateConfiguration
//! (`?accelerate`: Enabled or Suspended), and the accelerate endpoints
//! `<bucket>.s3-accelerate.<domain>` and
//! `<bucket>.s3-accelerate.dualstack.<domain>` (virtual-hosted only), which
//! serve a bucket only while its acceleration is Enabled. Nothing is faster
//! through them: they are the same server.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::BUCKET_ACCELERATE;

use crate::error::S3Error;
use crate::server::{Authenticated, Body, S3State, full, read_body};

/// A request to an accelerate endpoint, in its extensions: the bucket its
/// host names (none: the endpoint itself).
#[derive(Clone)]
pub(crate) struct AccelerateHost(pub(crate) Option<String>);

/// What `host` names as an accelerate endpoint, if it is one: its bucket
/// (none: the endpoint itself). A bucket name with periods is none, as
/// acceleration takes none.
pub(crate) fn host(host: &str, domains: &[String]) -> Option<Option<String>> {
    let endpoints: Vec<String> = domains
        .iter()
        .flat_map(|domain| {
            [
                format!("s3-accelerate.dualstack.{domain}"),
                format!("s3-accelerate.{domain}"),
            ]
        })
        .collect();
    let name = host.to_ascii_lowercase();
    let name = match name.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|byte| byte.is_ascii_digit()) => name.to_string(),
        _ => name,
    };
    let name = name.strip_suffix('.').unwrap_or(&name);
    if endpoints.iter().any(|endpoint| endpoint == name) {
        return Some(None);
    }
    let bucket = crate::api::host_bucket(host, &endpoints)?;
    (!bucket.contains('.')).then_some(Some(bucket))
}

fn invalid(message: &str) -> S3Error {
    S3Error::new(400, "InvalidRequest", message)
}

/// Refuses a request to an accelerate endpoint its bucket's acceleration
/// does not allow, before anything else serves it: one not virtual-hosted
/// (on the endpoint itself: ListBuckets, or path-style), another service's
/// or S3 Control's, creating or deleting a bucket, or on a bucket (its
/// host's) whose acceleration is not Enabled.
pub(crate) async fn check(state: &S3State, request: &Request<Incoming>) -> Result<(), S3Error> {
    let Some(AccelerateHost(named)) = request.extensions().get::<AccelerateHost>() else {
        return Ok(());
    };
    let Some(bucket) = named.clone() else {
        return Err(invalid(
            "The accelerate endpoint takes virtual-hosted requests of a bucket only",
        ));
    };
    if crate::server::request_service(request).is_some_and(|service| service != "s3")
        || crate::api::control_route(request).is_some()
    {
        return Err(invalid(
            "The accelerate endpoint takes Amazon S3 requests only",
        ));
    }
    if request.uri().path() == "/"
        && crate::api::subresource_count(request) == 0
        && matches!(*request.method(), Method::PUT | Method::DELETE)
    {
        return Err(invalid(
            "CreateBucket and DeleteBucket are not supported by the accelerate endpoint",
        ));
    }
    let enabled = crate::scope::within(
        crate::scope::Scope {
            key: None,
            buckets: Vec::new(),
            origin: None,
        },
        crate::scope::object(state, move |file| {
            Ok(file
                .bucket_config(&bucket, BUCKET_ACCELERATE, "")?
                .as_deref()
                == Some(&[1][..]))
        }),
    )
    .await?;
    if !enabled {
        return Err(invalid(
            "S3 Transfer Acceleration is not configured on this bucket",
        ));
    }
    Ok(())
}

fn malformed() -> S3Error {
    S3Error::new(
        400,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema",
    )
}

/// An AccelerateConfiguration's Status: whether it is Enabled.
fn parse(body: &[u8]) -> Result<bool, S3Error> {
    let elements = crate::access_points::elements(body)?;
    if elements.first().map(|(path, _)| path.as_str()) != Some("AccelerateConfiguration") {
        return Err(malformed());
    }
    let mut status = None;
    for (path, text) in &elements[1..] {
        match path.as_str() {
            "AccelerateConfiguration/Status" if status.is_none() => {
                status = Some(match text.trim() {
                    "Enabled" => true,
                    "Suspended" => false,
                    _ => return Err(malformed()),
                });
            }
            _ => return Err(malformed()),
        }
    }
    status.ok_or_else(malformed)
}

/// PUT or GET `?accelerate` on `bucket`.
pub async fn handle(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    if crate::scope::object(state, move |file| file.bucket(&name))
        .await?
        .is_none()
    {
        return Err(S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {bucket} does not exist"),
        ));
    }
    match method {
        Method::PUT => {
            // "must be DNS-compliant and must not contain periods".
            if bucket.contains('.') {
                return Err(invalid(
                    "The name of a bucket used for Transfer Acceleration must not contain periods",
                ));
            }
            let body = read_body(request, auth, 64 * 1024).await?;
            let enabled = parse(&body)?;
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.put_bucket_config(&bucket, BUCKET_ACCELERATE, "", &[u8::from(enabled)])
                })
            })
            .await?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
        _ => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_ACCELERATE, "")
            })
            .await?;
            // None set: no status ("does not return a state value").
            let status = match stored.as_deref() {
                Some([1]) => "<Status>Enabled</Status>",
                Some(_) => "<Status>Suspended</Status>",
                None => "",
            };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<AccelerateConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{status}</AccelerateConfiguration>"
                )))
                .expect("a valid response"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accelerate_hosts_name_their_bucket() {
        let domains = vec!["s3.test".to_string()];
        assert_eq!(
            host("data.s3-accelerate.s3.test:9000", &domains),
            Some(Some("data".to_string()))
        );
        assert_eq!(
            host("data.s3-accelerate.dualstack.s3.test", &domains),
            Some(Some("data".to_string()))
        );
        assert_eq!(host("s3-accelerate.s3.test", &domains), Some(None));
        assert_eq!(
            host("s3-accelerate.dualstack.s3.test", &domains),
            Some(None)
        );
        for not in [
            "data.s3.test",
            "a.b.s3-accelerate.s3.test",
            "s3.test",
            "data.s3-accelerate.other",
        ] {
            assert_eq!(host(not, &domains), None, "{not}");
        }
    }

    #[test]
    fn an_accelerate_configuration_names_its_status() {
        let body = |status: &str| {
            format!("<AccelerateConfiguration><Status>{status}</Status></AccelerateConfiguration>")
        };
        assert!(parse(body("Enabled").as_bytes()).unwrap());
        assert!(!parse(body("Suspended").as_bytes()).unwrap());
        for bad in [
            body("enabled"),
            "<AccelerateConfiguration/>".to_string(),
            "<Other><Status>Enabled</Status></Other>".to_string(),
            "<AccelerateConfiguration><Status>Enabled</Status><Status>Enabled</Status></AccelerateConfiguration>".to_string(),
        ] {
            assert!(parse(bad.as_bytes()).is_err(), "{bad}");
        }
    }
}
