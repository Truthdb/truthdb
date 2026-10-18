//! ListObjectsV2 and ListObjects (v1): a bucket's keys in order, with prefix,
//! delimiter (common prefixes), page size and the resume point (v2's
//! opaque continuation token or start-after, v1's marker).

use base64::Engine as _;
use bytes::Bytes;
use hyper::{Response, StatusCode};
use truthdb_object::ListAfter;

use crate::error::{S3Error, escape};
use crate::objects::object_error;
use crate::server::{Body, S3State, full};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const MAX_KEYS: usize = 1000;

/// The query string's parameters, percent-decoded.
pub fn query_params(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decode = |text: &str| {
                percent_encoding::percent_decode_str(text)
                    .decode_utf8_lossy()
                    .into_owned()
            };
            (decode(name), decode(value))
        })
        .collect()
}

fn param<'p>(params: &'p [(String, String)], name: &str) -> Option<&'p str> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

/// The continuation token: `K` + key or `P` + common prefix, base64.
fn token(last: &(String, bool)) -> String {
    let (entry, is_prefix) = last;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(format!("{}{entry}", if *is_prefix { 'P' } else { 'K' }))
}

fn parse_token(token: &str) -> Result<(String, bool), S3Error> {
    let invalid = || {
        S3Error::new(
            400,
            "InvalidArgument",
            "The continuation token provided is incorrect",
        )
    };
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| invalid())?;
    let text = String::from_utf8(bytes).map_err(|_| invalid())?;
    match text.split_at_checked(1) {
        Some(("K", key)) => Ok((key.to_string(), false)),
        Some(("P", prefix)) => Ok((prefix.to_string(), true)),
        _ => Err(invalid()),
    }
}

pub async fn list_objects(
    state: &S3State,
    bucket: String,
    query: &str,
) -> Result<Response<Body>, S3Error> {
    let params = query_params(query);
    let v2 = param(&params, "list-type") == Some("2");
    let prefix = param(&params, "prefix").unwrap_or("").to_string();
    let delimiter = param(&params, "delimiter").map(str::to_string);
    let max_keys = match param(&params, "max-keys") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| S3Error::new(400, "InvalidArgument", "max-keys must be a number"))?
            .min(MAX_KEYS),
        None => MAX_KEYS,
    };
    let url_encoding = match param(&params, "encoding-type") {
        None => false,
        Some("url") => true,
        Some(_) => {
            return Err(S3Error::new(
                400,
                "InvalidArgument",
                "Invalid Encoding Method specified in Request",
            ));
        }
    };
    let continuation = match (v2, param(&params, "continuation-token")) {
        (true, Some(token)) => Some(parse_token(token)?),
        _ => None,
    };
    let start = continuation.clone().or_else(|| {
        let key = if v2 {
            param(&params, "start-after")
        } else {
            param(&params, "marker")
        };
        key.map(|key| (key.to_string(), false))
    });

    let (bucket_name, list_prefix, list_delimiter) =
        (bucket.clone(), prefix.clone(), delimiter.clone());
    let listing = state
        .engine
        .run_object(move |file| {
            let after = start.as_ref().map(|(entry, is_prefix)| {
                if *is_prefix {
                    ListAfter::CommonPrefix(entry)
                } else {
                    ListAfter::Key(entry)
                }
            });
            file.list_objects(
                &bucket_name,
                &list_prefix,
                list_delimiter.as_deref(),
                after,
                max_keys,
            )
        })
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .map_err(|err| object_error(err, &bucket, ""))?;

    let encode = |text: &str| {
        if url_encoding {
            crate::sigv4::encode_key(text)
        } else {
            escape(text)
        }
    };
    let mut xml = format!(
        "<ListBucketResult xmlns=\"{XMLNS}\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{max_keys}</MaxKeys><IsTruncated>{}</IsTruncated>",
        escape(&bucket),
        encode(&prefix),
        listing.truncated
    );
    if let Some(delimiter) = &delimiter {
        xml.push_str(&format!("<Delimiter>{}</Delimiter>", encode(delimiter)));
    }
    if url_encoding {
        xml.push_str("<EncodingType>url</EncodingType>");
    }
    if v2 {
        xml.push_str(&format!(
            "<KeyCount>{}</KeyCount>",
            listing.objects.len() + listing.common_prefixes.len()
        ));
        if let Some(token) = param(&params, "continuation-token") {
            xml.push_str(&format!(
                "<ContinuationToken>{}</ContinuationToken>",
                escape(token)
            ));
        }
        if let Some(start_after) = param(&params, "start-after") {
            xml.push_str(&format!("<StartAfter>{}</StartAfter>", encode(start_after)));
        }
        if listing.truncated
            && let Some(last) = &listing.last
        {
            xml.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                token(last)
            ));
        }
    } else {
        xml.push_str(&format!(
            "<Marker>{}</Marker>",
            encode(param(&params, "marker").unwrap_or(""))
        ));
        // S3 returns NextMarker only with a delimiter, and a client without
        // one resumes after the last key it received. A page here can end on
        // a deleted key that is not in it, so NextMarker is always sent.
        if listing.truncated
            && let Some((last, _)) = &listing.last
        {
            xml.push_str(&format!("<NextMarker>{}</NextMarker>", encode(last)));
        }
    }
    for object in &listing.objects {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            encode(&object.key),
            crate::time::iso8601(object.info.mtime_ms),
            escape(&object.info.etag()),
            object.info.size
        ));
    }
    for common in &listing.common_prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            encode(common)
        ));
    }
    xml.push_str("</ListBucketResult>");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(Bytes::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{xml}"
        ))))
        .expect("a valid response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_tokens_round_trip_and_reject_garbage() {
        for last in [("dir/a.txt".to_string(), false), ("dir/".to_string(), true)] {
            assert_eq!(parse_token(&token(&last)).unwrap(), last);
        }
        assert!(parse_token("!!").is_err());
        assert!(
            parse_token(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("Xabc")).is_err()
        );
    }
}
