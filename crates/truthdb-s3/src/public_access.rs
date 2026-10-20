//! Block Public Access: four settings, per bucket and per account, the
//! stricter of the two applying (S3's PutPublicAccessBlock, and S3 Control's
//! for an account). New buckets get all four on.
//!
//! - BlockPublicPolicy refuses a PutBucketPolicy whose policy is public.
//! - RestrictPublicBuckets: while the bucket's policy is public, it grants
//!   nothing to anyone outside the bucket owner's account (anonymous callers
//!   included), not even a grant to a named account.
//! - BlockPublicAcls and IgnorePublicAcls are kept and returned; with ACLs
//!   disabled (see `policy`) there is no public ACL for them to act on.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_core::storage::StorageError;
use truthdb_object::{ObjFile, PUBLIC_ACCESS_BLOCK};

use crate::error::S3Error;
use crate::server::{Authenticated, Body, S3State, full, read_body};

pub const BLOCK_PUBLIC_ACLS: u8 = 1;
pub const IGNORE_PUBLIC_ACLS: u8 = 2;
pub const BLOCK_PUBLIC_POLICY: u8 = 4;
pub const RESTRICT_PUBLIC_BUCKETS: u8 = 8;
/// All four settings: a new bucket's.
pub const ALL: u8 = 15;

const NAMES: [(&str, u8); 4] = [
    ("BlockPublicAcls", BLOCK_PUBLIC_ACLS),
    ("IgnorePublicAcls", IGNORE_PUBLIC_ACLS),
    ("BlockPublicPolicy", BLOCK_PUBLIC_POLICY),
    ("RestrictPublicBuckets", RESTRICT_PUBLIC_BUCKETS),
];

/// The settings that apply to `bucket` of `owner`: the bucket's and the
/// account's together.
pub fn effective(file: &mut ObjFile<'_>, bucket: &str, owner: &str) -> Result<u8, StorageError> {
    let setting =
        |document: Option<Vec<u8>>| document.and_then(|d| d.first().copied()).unwrap_or(0);
    Ok(
        setting(file.bucket_config(bucket, PUBLIC_ACCESS_BLOCK, "")?)
            | setting(file.bucket_config("", PUBLIC_ACCESS_BLOCK, owner)?),
    )
}

/// A PublicAccessBlockConfiguration document's settings (a missing element
/// is off).
fn parse(body: &[u8]) -> Result<u8, S3Error> {
    use quick_xml::Reader;
    use quick_xml::events::Event;
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed",
        )
    };
    let mut reader = Reader::from_reader(body);
    let (mut flags, mut current, mut root) = (0u8, None, false);
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(|_| malformed())? {
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if name == "PublicAccessBlockConfiguration" {
                    root = true;
                } else {
                    current = NAMES.iter().find(|(n, _)| *n == name).map(|(_, bit)| *bit);
                }
            }
            Event::Text(text) => {
                if let Some(bit) = current {
                    let text = text.unescape().map_err(|_| malformed())?;
                    match text.trim().to_ascii_lowercase().as_str() {
                        "true" => flags |= bit,
                        "false" => {}
                        _ => return Err(malformed()),
                    }
                }
            }
            Event::Empty(e) if e.local_name().as_ref() == b"PublicAccessBlockConfiguration" => {
                root = true;
            }
            Event::End(_) => current = None,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    if !root {
        return Err(malformed());
    }
    Ok(flags)
}

fn render(flags: u8, xmlns: &str) -> String {
    let fields: String = NAMES
        .iter()
        .map(|(name, bit)| format!("<{name}>{}</{name}>", flags & bit != 0))
        .collect();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PublicAccessBlockConfiguration xmlns=\"{xmlns}\">{fields}</PublicAccessBlockConfiguration>"
    )
}

fn no_configuration() -> S3Error {
    S3Error::new(
        404,
        "NoSuchPublicAccessBlockConfiguration",
        "The public access block configuration was not found",
    )
}

/// PUT, GET or DELETE `?publicAccessBlock` on `bucket` (`bucket` empty and
/// `account` set: S3 Control's account-level settings).
pub async fn handle(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    account: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let xmlns = if bucket.is_empty() {
        "http://awss3control.amazonaws.com/doc/2018-08-20/"
    } else {
        "http://s3.amazonaws.com/doc/2006-03-01/"
    };
    if !bucket.is_empty() {
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
    }
    let empty = |status: StatusCode| {
        Response::builder()
            .status(status)
            .body(full(bytes::Bytes::new()))
            .expect("a valid response")
    };
    match method {
        Method::PUT => {
            let body = read_body(request, auth, 64 * 1024).await?;
            let flags = parse(&body)?;
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.put_bucket_config(&bucket, PUBLIC_ACCESS_BLOCK, &account, &[flags])
                })
            })
            .await?;
            Ok(empty(StatusCode::OK))
        }
        Method::GET => {
            let flags = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, PUBLIC_ACCESS_BLOCK, &account)
            })
            .await?
            .and_then(|document| document.first().copied())
            .ok_or_else(no_configuration)?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(render(flags, xmlns)))
                .expect("a valid response"))
        }
        _ => {
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.delete_bucket_config(&bucket, PUBLIC_ACCESS_BLOCK, &account)
                })
            })
            .await?;
            Ok(empty(StatusCode::NO_CONTENT))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_parse_and_render_round_trip() {
        let body = b"<PublicAccessBlockConfiguration><BlockPublicAcls>TRUE</BlockPublicAcls><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>";
        let flags = parse(body).unwrap();
        assert_eq!(flags, BLOCK_PUBLIC_ACLS | BLOCK_PUBLIC_POLICY);
        assert_eq!(parse(render(flags, "x").as_bytes()).unwrap(), flags);
        assert!(parse(b"<Other/>").is_err());
        assert!(parse(b"<PublicAccessBlockConfiguration><BlockPublicAcls>maybe</BlockPublicAcls></PublicAccessBlockConfiguration>").is_err());
    }
}
