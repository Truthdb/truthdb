//! Object tags: PutObjectTagging, GetObjectTagging, DeleteObjectTagging, and
//! the `x-amz-tagging` header of PutObject, CreateMultipartUpload and
//! CopyObject. A version's tags are stored with its attributes.

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_object::ObjectError;

use crate::error::{S3Error, escape};
use crate::multipart::xml;
use crate::objects::{object, object_error, version_sel};
use crate::server::{Authenticated, Body, S3State, full, read_body};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// Most tags on an object (S3's limit).
const MAX_TAGS: usize = 10;
/// Longest tag key and value, in characters (S3's limits).
const MAX_TAG_KEY: usize = 128;
const MAX_TAG_VALUE: usize = 256;

fn invalid_tag(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidTag", message)
}

/// Checks a tag set against S3's limits.
fn validate(tags: &[(String, String)]) -> Result<(), S3Error> {
    if tags.len() > MAX_TAGS {
        return Err(S3Error::new(
            400,
            "BadRequest",
            "Object tags cannot be greater than 10",
        ));
    }
    for (index, (key, value)) in tags.iter().enumerate() {
        let key_chars = key.chars().count();
        if key_chars == 0 || key_chars > MAX_TAG_KEY {
            return Err(invalid_tag("The TagKey you have provided is invalid"));
        }
        if value.chars().count() > MAX_TAG_VALUE {
            return Err(invalid_tag("The TagValue you have provided is invalid"));
        }
        if key.starts_with("aws:") {
            return Err(invalid_tag("Your TagKey cannot be prefixed with aws:"));
        }
        if tags[..index].iter().any(|(earlier, _)| earlier == key) {
            return Err(invalid_tag(
                "Cannot provide multiple Tags with the same key",
            ));
        }
    }
    Ok(())
}

/// The `x-amz-tagging` header: URL query form, `key1=value1&key2=value2`.
pub fn tags_from_header(value: &str) -> Result<Vec<(String, String)>, S3Error> {
    let decode = |text: &str| {
        percent_encoding::percent_decode_str(&text.replace('+', " "))
            .decode_utf8()
            .map(|text| text.into_owned())
            .map_err(|_| invalid_tag("The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates."))
    };
    let tags = value
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            Ok((decode(key)?, decode(value)?))
        })
        .collect::<Result<Vec<_>, S3Error>>()?;
    validate(&tags)?;
    Ok(tags)
}

/// The tag set of a PutObjectTagging body.
fn parse_tagging(body: &[u8]) -> Result<Vec<(String, String)>, S3Error> {
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
        )
    };
    let mut reader = Reader::from_reader(body);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let (mut key, mut value): (Option<String>, Option<String>) = (None, None);
    let mut tags = Vec::new();
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(|_| malformed())? {
            Event::Start(element) => {
                path.push(element.local_name().as_ref().to_vec());
                if path.last().map(Vec::as_slice) == Some(b"Tag") {
                    (key, value) = (None, None);
                }
            }
            Event::End(element) => {
                match element.local_name().as_ref() {
                    b"Tag" => tags.push((
                        key.take().ok_or_else(malformed)?,
                        value.take().unwrap_or_default(),
                    )),
                    // `<Key></Key>` and `<Value></Value>` carry no text event.
                    b"Key" => key = Some(key.take().unwrap_or_default()),
                    b"Value" => value = Some(value.take().unwrap_or_default()),
                    _ => {}
                }
                path.pop();
            }
            Event::Text(text) => {
                let text = text.unescape().map_err(|_| malformed())?;
                match path.last().map(Vec::as_slice) {
                    Some(b"Key") => key.get_or_insert_default().push_str(&text),
                    Some(b"Value") => value.get_or_insert_default().push_str(&text),
                    _ => {}
                }
            }
            Event::Eof if path.is_empty() => break,
            Event::Eof => return Err(malformed()),
            _ => {}
        }
        buf.clear();
    }
    validate(&tags)?;
    Ok(tags)
}

fn tagging_error(err: ObjectError, bucket: &str, key: &str) -> S3Error {
    match err {
        ObjectError::DeleteMarker => S3Error::new(
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        ),
        err => object_error(err, bucket, key),
    }
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(Bytes::new()))
        .expect("a valid response")
}

pub async fn put_tagging(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let body = read_body(request, auth, 64 * 1024).await?;
    let tags = parse_tagging(&body)?;
    let (tag_bucket, tag_key) = (bucket.clone(), key.clone());
    object(state, move |file| {
        file.transaction(|tx| tx.set_tags(&tag_bucket, &tag_key, which, tags))
    })
    .await?
    .map_err(|err| tagging_error(err, &bucket, &key))?;
    Ok(empty(StatusCode::OK))
}

pub async fn delete_tagging(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let (tag_bucket, tag_key) = (bucket.clone(), key.clone());
    object(state, move |file| {
        file.transaction(|tx| tx.set_tags(&tag_bucket, &tag_key, which, Vec::new()))
    })
    .await?
    .map_err(|err| tagging_error(err, &bucket, &key))?;
    Ok(empty(StatusCode::NO_CONTENT))
}

pub async fn get_tagging(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let (tag_bucket, tag_key) = (bucket.clone(), key.clone());
    let info = object(state, move |file| {
        file.head_object(&tag_bucket, &tag_key, which)
    })
    .await?
    .map_err(|err| tagging_error(err, &bucket, &key))?;
    if info.delete_marker {
        return Err(tagging_error(ObjectError::DeleteMarker, &bucket, &key));
    }
    let tags: String = info
        .attrs
        .tags
        .iter()
        .map(|(key, value)| {
            format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                escape(key),
                escape(value)
            )
        })
        .collect();
    Ok(xml(format!(
        "<Tagging xmlns=\"{XMLNS}\"><TagSet>{tags}</TagSet></Tagging>"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_headers_and_bodies_parse_within_s3_limits() {
        assert_eq!(
            tags_from_header("a=1&b%20c=d+e&empty=").unwrap(),
            [
                ("a".to_string(), "1".to_string()),
                ("b c".to_string(), "d e".to_string()),
                ("empty".to_string(), String::new()),
            ]
        );
        assert_eq!(tags_from_header("a=1&a=2").unwrap_err().code, "InvalidTag");
        assert_eq!(tags_from_header("aws:x=1").unwrap_err().code, "InvalidTag");
        let eleven: Vec<String> = (0..11).map(|i| format!("k{i}=v")).collect();
        assert_eq!(
            tags_from_header(&eleven.join("&")).unwrap_err().code,
            "BadRequest"
        );
        let long_key = format!("{}=v", "k".repeat(129));
        assert_eq!(tags_from_header(&long_key).unwrap_err().code, "InvalidTag");

        assert_eq!(
            parse_tagging(
                b"<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>a &amp; b</Key><Value>1</Value></Tag><Tag><Key>e</Key><Value></Value></Tag></TagSet></Tagging>"
            )
            .unwrap(),
            [
                ("a & b".to_string(), "1".to_string()),
                ("e".to_string(), String::new()),
            ]
        );
        assert_eq!(
            parse_tagging(b"<Tagging><TagSet></TagSet></Tagging>").unwrap(),
            []
        );
        for bad in [
            &b"<Tagging><TagSet><Tag><Value>1</Value></Tag></TagSet></Tagging>"[..],
            b"<Tagging><TagSet>",
        ] {
            assert_eq!(parse_tagging(bad).unwrap_err().code, "MalformedXML");
        }
    }
}
