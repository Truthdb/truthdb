//! DeleteObject and DeleteObjects (`POST ?delete`, up to 1000 keys).

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_object::{Deleted, ObjectError, VersionSel, version_id};

use crate::error::{S3Error, escape};
use crate::objects::{object_error, version_sel};
use crate::server::{Authenticated, Body, S3State, full, read_body};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// Keys one DeleteObjects request may name (S3's limit).
const MAX_KEYS: usize = 1000;
/// Largest DeleteObjects request body read.
const MAX_BODY: usize = 4 * 1024 * 1024;
/// How long a deleted object's body outlives it (readers that resolved it
/// just before the delete may still be reading).
const DROP_GRACE_MS: u64 = 15 * 60 * 1000;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn delete_one(
    state: &S3State,
    bucket: &str,
    key: &str,
    which: Option<VersionSel>,
) -> Result<Deleted, S3Error> {
    let (bucket_name, key_name) = (bucket.to_string(), key.to_string());
    crate::scope::object(state, move |file| {
        file.transaction(|tx| {
            tx.delete_object(&bucket_name, &key_name, which, now_ms(), DROP_GRACE_MS)
        })
    })
    .await?
    .map_err(|err| object_error(err, bucket, key))
}

pub async fn delete_object(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let which = match version_sel(request.uri().query().unwrap_or(""))? {
        VersionSel::Latest => None,
        sel => Some(sel),
    };
    let deleted = delete_one(state, &bucket, &key, which).await?;
    let mut response = Response::builder().status(StatusCode::NO_CONTENT);
    if let Some(version) = deleted.version {
        response = response.header(
            "x-amz-version-id",
            version_id(version, deleted.null_version),
        );
    }
    if deleted.delete_marker {
        response = response.header("x-amz-delete-marker", "true");
    }
    Ok(response.body(full(Bytes::new())).expect("a valid response"))
}

/// One `<Object>` of a DeleteObjects request.
struct ToDelete {
    key: String,
    version: Option<String>,
}

fn parse_delete(xml: &[u8]) -> Result<(bool, Vec<ToDelete>), S3Error> {
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        )
    };
    let mut reader = Reader::from_reader(xml);
    let mut quiet: Option<String> = None;
    let mut seen_root = false;
    let mut objects = Vec::new();
    let mut path: Vec<Vec<u8>> = Vec::new();
    let (mut key, mut version): (Option<String>, Option<String>) = (None, None);
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|_| malformed())?;
        // An empty element is its start and its end.
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
                (None, b"Delete") => !std::mem::replace(&mut seen_root, true),
                (Some(b"Delete"), b"Quiet") => quiet.replace(String::new()).is_none(),
                (Some(b"Delete"), b"Object") => {
                    (key, version) = (None, None);
                    true
                }
                (Some(b"Object"), b"Key") => key.replace(String::new()).is_none(),
                (Some(b"Object"), b"VersionId") => version.replace(String::new()).is_none(),
                // Conditional deletes are not implemented: refused rather
                // than ignored.
                (Some(b"Object"), b"ETag" | b"LastModifiedTime" | b"Size") => {
                    return Err(S3Error::not_implemented(
                        "conditional deletes in DeleteObjects are not implemented",
                    ));
                }
                _ => false,
            };
            if !allowed {
                return Err(malformed());
            }
            path.push(name);
        }
        if let Some(name) = end {
            if name == b"Object" {
                let key = key
                    .take()
                    .filter(|key| !key.is_empty())
                    .ok_or_else(malformed)?;
                objects.push(ToDelete {
                    key,
                    version: version.take(),
                });
            }
            path.pop();
        }
        let text = match &event {
            Event::Text(text) => Some(text.unescape().map_err(|_| malformed())?.into_owned()),
            Event::CData(text) => Some(
                std::str::from_utf8(text)
                    .map_err(|_| malformed())?
                    .to_string(),
            ),
            _ => None,
        };
        if let Some(text) = text {
            match path.last().map(Vec::as_slice) {
                Some(b"Key") => key.get_or_insert_default().push_str(&text),
                Some(b"VersionId") => version.get_or_insert_default().push_str(&text),
                Some(b"Quiet") => quiet.get_or_insert_default().push_str(&text),
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
    if objects.is_empty() || objects.len() > MAX_KEYS {
        return Err(malformed());
    }
    let quiet = match quiet.as_deref().map(str::trim) {
        None => false,
        Some(value) if value.eq_ignore_ascii_case("true") => true,
        Some(value) if value.eq_ignore_ascii_case("false") => false,
        Some(_) => return Err(malformed()),
    };
    Ok((quiet, objects))
}

/// DeleteObjects: each key is authorized on its own (`s3:DeleteObject`, or
/// `s3:DeleteObjectVersion` for a version), a denied one reported in the
/// result.
pub async fn delete_objects(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    authz: &crate::authz::Authz,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    // S3 requires an integrity check on the request body here.
    let has_digest = request.headers().contains_key("content-md5")
        || request
            .headers()
            .keys()
            .any(|name| crate::server::CHECKSUM_HEADERS.contains(&name.as_str()))
        || request.headers().contains_key("x-amz-trailer");
    if !has_digest {
        return Err(S3Error::invalid_request(
            "Missing required header for this request: Content-MD5",
        ));
    }
    let content_md5 = request
        .headers()
        .get("content-md5")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = read_body(request, auth, MAX_BODY).await?;
    if let Some(expected) = content_md5 {
        use base64::Engine as _;
        use md5::Digest;
        let ours = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(&body));
        if ours != expected.trim() {
            return Err(S3Error::bad_digest(
                "The Content-MD5 you specified did not match what we received.",
            ));
        }
    }
    let (quiet, objects) = parse_delete(&body)?;
    // A missing bucket fails the request, not each key.
    let name = bucket.clone();
    if crate::scope::object(state, move |file| file.bucket(&name))
        .await?
        .is_none()
    {
        return Err(object_error(ObjectError::NoSuchBucket, &bucket, ""));
    }
    let mut xml = format!("<DeleteResult xmlns=\"{XMLNS}\">");
    for object in objects {
        let which = match object.version.as_deref() {
            None => None,
            Some("null") => Some(VersionSel::Null),
            Some(text) => match text.parse() {
                Ok(seq) => Some(VersionSel::Seq(seq)),
                Err(_) => {
                    xml.push_str(&format!(
                        "<Error><Key>{}</Key><VersionId>{}</VersionId><Code>NoSuchVersion</Code><Message>The specified version does not exist.</Message></Error>",
                        escape(&object.key),
                        escape(text)
                    ));
                    continue;
                }
            },
        };
        let action = if which.is_some() {
            "s3:DeleteObjectVersion"
        } else {
            "s3:DeleteObject"
        };
        // The key's own version as it is deleted (`001` is version 1), not
        // the request query's.
        let mut context = authz.context().clone();
        match which {
            None => context.remove("s3:VersionId"),
            Some(VersionSel::Null) => context.set("s3:VersionId", vec!["null".to_string()]),
            Some(VersionSel::Seq(version)) => {
                context.set("s3:VersionId", vec![version.to_string()])
            }
            Some(VersionSel::Latest) => {}
        }
        let outcome = match authz.check_with(
            action,
            crate::authz::Resource::Object(&bucket, &object.key),
            &context,
        ) {
            Ok(()) => delete_one(state, &bucket, &object.key, which).await,
            Err(err) => Err(err),
        };
        match outcome {
            Ok(deleted) => {
                if quiet {
                    continue;
                }
                xml.push_str(&format!("<Deleted><Key>{}</Key>", escape(&object.key)));
                if let Some(version) = &object.version {
                    xml.push_str(&format!("<VersionId>{}</VersionId>", escape(version)));
                }
                if deleted.delete_marker {
                    xml.push_str("<DeleteMarker>true</DeleteMarker>");
                    if let Some(version) = deleted.version {
                        xml.push_str(&format!(
                            "<DeleteMarkerVersionId>{}</DeleteMarkerVersionId>",
                            version_id(version, deleted.null_version)
                        ));
                    }
                }
                xml.push_str("</Deleted>");
            }
            Err(err) => {
                xml.push_str(&format!("<Error><Key>{}</Key>", escape(&object.key)));
                if let Some(version) = &object.version {
                    xml.push_str(&format!("<VersionId>{}</VersionId>", escape(version)));
                }
                xml.push_str(&format!(
                    "<Code>{}</Code><Message>{}</Message></Error>",
                    err.code,
                    escape(&err.message)
                ));
            }
        }
    }
    xml.push_str("</DeleteResult>");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}"
        )))
        .expect("a valid response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_requests_parse_keys_versions_and_quiet() {
        let (quiet, objects) = parse_delete(
            br#"<?xml version="1.0"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>true</Quiet><Object><Key>a &amp; b.txt</Key></Object><Object><Key>c</Key><VersionId>7</VersionId></Object></Delete>"#,
        )
        .unwrap();
        assert!(quiet);
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].key, "a & b.txt");
        assert_eq!(objects[1].version.as_deref(), Some("7"));
        assert!(parse_delete(b"<Delete></Delete>").is_err(), "no objects");
        assert!(
            parse_delete(b"<Delete><Object><VersionId>1</VersionId></Object></Delete>").is_err(),
            "an object without a key"
        );
        let (_, objects) =
            parse_delete(b"<Delete><Object><Key>x<![CDATA[y&z]]></Key></Object></Delete>").unwrap();
        assert_eq!(objects[0].key, "xy&z");
        for bad in [
            &b"<Delete><Object><Key>a</Key></Object><Object><Key>b</Key>"[..],
            b"<Foo><Object><Key>a</Key></Object></Foo>",
            b"<Delete><Object><Key>a</Key></Object></Delete><Delete><Object><Key>b</Key></Object></Delete>",
            b"<Delete><Object><Key>a</Key><Key>b</Key></Object></Delete>",
            b"<Delete><Object><Key>a</Key></Object><Key>z</Key></Delete>",
            b"<Delete><Quiet>maybe</Quiet><Object><Key>a</Key></Object></Delete>",
        ] {
            assert_eq!(
                parse_delete(bad).err().map(|err| err.code),
                Some("MalformedXML"),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
        assert_eq!(
            parse_delete(b"<Delete><Object><Key>a</Key><ETag>x</ETag></Object></Delete>")
                .err()
                .map(|err| err.code),
            Some("NotImplemented")
        );
    }
}
