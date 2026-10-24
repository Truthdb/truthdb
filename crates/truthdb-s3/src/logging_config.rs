//! Server access logging's configuration: PUT and GET `?logging` on a
//! bucket. A configuration names the target bucket the logs go to (one of
//! the same owner, without Object Lock), the prefix of their keys and the
//! keys' format (`SimplePrefix`, or `PartitionedPrefix` by event or
//! delivery time); an empty `BucketLoggingStatus` turns logging off. Only
//! the bucket's owner sets it. Target grants are refused: object ownership
//! is always BucketOwnerEnforced, under which S3 takes none.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::BUCKET_LOGGING;

use crate::error::{S3Error, escape};
use crate::object_lock::{malformed, read_checked_body};
use crate::server::{Authenticated, Body, S3State, full};
use crate::xml;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const MAX_BODY: usize = 64 * 1024;

/// What dates a partitioned log key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateSource {
    EventTime,
    DeliveryTime,
}

/// A log key's format: `TargetObjectKeyFormat`, if given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFormat {
    Simple,
    Partitioned(DateSource),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggingConfig {
    pub target_bucket: String,
    pub target_prefix: String,
    pub key_format: Option<KeyFormat>,
}

fn invalid_target(message: &str) -> S3Error {
    S3Error::new(400, "InvalidTargetBucketForLogging", message)
}

/// Parses a BucketLoggingStatus: its configuration, or none (logging off).
pub fn parse(body: &[u8]) -> Result<Option<LoggingConfig>, S3Error> {
    let root = xml::parse(body, "BucketLoggingStatus")?;
    root.only(&["LoggingEnabled"])?;
    let Some(enabled) = root.one("LoggingEnabled")? else {
        return Ok(None);
    };
    enabled.only(&[
        "TargetBucket",
        "TargetGrants",
        "TargetPrefix",
        "TargetObjectKeyFormat",
    ])?;
    let grants = enabled.one("TargetGrants")?;
    if grants.is_some_and(|grants| !grants.text.trim().is_empty()) {
        return Err(malformed());
    }
    if grants.is_some_and(|grants| !grants.children.is_empty()) {
        return Err(S3Error::new(
            400,
            "AccessControlListNotSupported",
            "The bucket does not allow ACLs",
        ));
    }
    let target_bucket = enabled
        .text_of("TargetBucket")?
        .filter(|name| !name.is_empty())
        .ok_or_else(malformed)?
        .to_string();
    let target_prefix = enabled
        .text_of("TargetPrefix")?
        .ok_or_else(malformed)?
        .to_string();
    let key_format = match enabled.one("TargetObjectKeyFormat")? {
        None => None,
        Some(format) => {
            format.only(&["SimplePrefix", "PartitionedPrefix"])?;
            match (
                format.one("SimplePrefix")?,
                format.one("PartitionedPrefix")?,
            ) {
                (Some(simple), None)
                    if simple.children.is_empty() && simple.text.trim().is_empty() =>
                {
                    Some(KeyFormat::Simple)
                }
                (None, Some(partitioned)) => {
                    partitioned.only(&["PartitionDateSource"])?;
                    Some(KeyFormat::Partitioned(
                        match partitioned.text_of("PartitionDateSource")? {
                            Some("EventTime") => DateSource::EventTime,
                            Some("DeliveryTime") => DateSource::DeliveryTime,
                            _ => return Err(malformed()),
                        },
                    ))
                }
                _ => return Err(malformed()),
            }
        }
    };
    Ok(Some(LoggingConfig {
        target_bucket,
        target_prefix,
        key_format,
    }))
}

/// The configuration as GetBucketLogging answers it (as it is also stored).
pub fn render(config: &LoggingConfig) -> String {
    let format = match config.key_format {
        None => String::new(),
        Some(KeyFormat::Simple) => {
            "<TargetObjectKeyFormat><SimplePrefix></SimplePrefix></TargetObjectKeyFormat>"
                .to_string()
        }
        Some(KeyFormat::Partitioned(source)) => format!(
            "<TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>{}\
             </PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat>",
            match source {
                DateSource::EventTime => "EventTime",
                DateSource::DeliveryTime => "DeliveryTime",
            }
        ),
    };
    format!(
        "<BucketLoggingStatus xmlns=\"{XMLNS}\"><LoggingEnabled><TargetBucket>{}</TargetBucket>\
         <TargetPrefix>{}</TargetPrefix>{format}</LoggingEnabled></BucketLoggingStatus>",
        escape(&config.target_bucket),
        escape(&config.target_prefix),
    )
}

/// PUT or GET `?logging` on a bucket, by `account` (the caller's).
pub async fn bucket_logging(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
    account: Option<String>,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    let Some(owner) = crate::scope::object(state, move |file| {
        Ok(file.bucket(&name)?.map(|info| info.owner))
    })
    .await?
    else {
        return Err(S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {bucket} does not exist"),
        ));
    };
    match method {
        Method::PUT => {
            // Only the bucket's owner sets its logging.
            if account.as_deref() != Some(owner.as_str()) {
                return Err(S3Error::access_denied("Access Denied"));
            }
            let body = read_checked_body(request, auth, MAX_BODY).await?;
            let config = parse(&body)?;
            let refused = crate::scope::object(state, move |file| {
                if let Some(config) = &config {
                    match file.bucket(&config.target_bucket)? {
                        None => return Ok(Some("The target bucket for logging does not exist")),
                        Some(target) if target.owner != owner => {
                            return Ok(Some(
                                "The owner for the bucket to be logged and the target bucket \
                                 must be the same",
                            ));
                        }
                        Some(target) if target.object_lock => {
                            return Ok(Some(
                                "A bucket with Object Lock enabled cannot be a target for logging",
                            ));
                        }
                        Some(_) => {}
                    }
                }
                file.transaction(|tx| match &config {
                    Some(config) => {
                        tx.put_bucket_config(&bucket, BUCKET_LOGGING, "", render(config).as_bytes())
                    }
                    None => tx
                        .delete_bucket_config(&bucket, BUCKET_LOGGING, "")
                        .map(|_| ()),
                })?;
                Ok(None)
            })
            .await?;
            if let Some(message) = refused {
                return Err(invalid_target(message));
            }
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
        Method::GET => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_LOGGING, "")
            })
            .await?;
            let document = match stored {
                Some(stored) => String::from_utf8_lossy(&stored).into_owned(),
                None => format!("<BucketLoggingStatus xmlns=\"{XMLNS}\"></BucketLoggingStatus>"),
            };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{document}"
                )))
                .expect("a valid response"))
        }
        other => Err(S3Error::not_implemented(format!("{other} ?logging"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(inner: &str) -> Result<Option<LoggingConfig>, S3Error> {
        parse(
            format!("<BucketLoggingStatus xmlns=\"{XMLNS}\">{inner}</BucketLoggingStatus>")
                .as_bytes(),
        )
    }

    fn code(result: Result<Option<LoggingConfig>, S3Error>) -> String {
        result.expect_err("refused").code.to_string()
    }

    #[test]
    fn configurations_are_checked_as_s3_checks_them() {
        assert_eq!(status(""), Ok(None), "logging off");
        let plain = status(
            "<LoggingEnabled><TargetBucket>logs</TargetBucket>\
             <TargetPrefix>a/</TargetPrefix></LoggingEnabled>",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            plain,
            LoggingConfig {
                target_bucket: "logs".into(),
                target_prefix: "a/".into(),
                key_format: None,
            }
        );
        let partitioned = status(
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix></TargetPrefix>\
             <TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>DeliveryTime\
             </PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat></LoggingEnabled>",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            partitioned.key_format,
            Some(KeyFormat::Partitioned(DateSource::DeliveryTime))
        );
        // Rendered, then read back, it is the same.
        for config in [&plain, &partitioned] {
            assert_eq!(parse(render(config).as_bytes()), Ok(Some(config.clone())));
        }
        let simple = status(
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <TargetObjectKeyFormat><SimplePrefix/></TargetObjectKeyFormat></LoggingEnabled>",
        )
        .unwrap()
        .unwrap();
        assert_eq!(simple.key_format, Some(KeyFormat::Simple));
        assert_eq!(parse(render(&simple).as_bytes()), Ok(Some(simple)));
        for refused in [
            "<LoggingEnabled><TargetPrefix>p</TargetPrefix></LoggingEnabled>",
            "<LoggingEnabled><TargetBucket></TargetBucket><TargetPrefix>p</TargetPrefix></LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket></LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>kaboom\
             </PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat></LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <TargetObjectKeyFormat></TargetObjectKeyFormat></LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <TargetObjectKeyFormat><SimplePrefix/><PartitionedPrefix><PartitionDateSource>\
             EventTime</PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat>\
             </LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <Other/></LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <TargetObjectKeyFormat><SimplePrefix>junk</SimplePrefix></TargetObjectKeyFormat>\
             </LoggingEnabled>",
            "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
             <TargetGrants>junk</TargetGrants></LoggingEnabled>",
        ] {
            assert_eq!(code(status(refused)), "MalformedXML", "{refused}");
        }
        assert_eq!(
            code(status(
                "<LoggingEnabled><TargetBucket>logs</TargetBucket><TargetPrefix>p</TargetPrefix>\
                 <TargetGrants><Grant><Permission>READ</Permission></Grant></TargetGrants>\
                 </LoggingEnabled>"
            )),
            "AccessControlListNotSupported"
        );
    }
}
