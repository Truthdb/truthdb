//! A bucket's request metrics configurations (`?metrics&id=`): up to 1000
//! of them, each naming by its id the filter (a prefix, object tags, an
//! access point, or all of those) of the requests counted under it in the
//! request metrics (`FilterId`).

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::BUCKET_METRICS;

use crate::error::{S3Error, escape};
use crate::object_lock::malformed;
use crate::server::{Authenticated, Body, S3State, full};
use crate::xml::{self, Element};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// The largest configuration taken.
const MAX_BODY: usize = 64 * 1024;
/// The configurations a bucket holds at most, and a listing's page.
const MAX_CONFIGURATIONS: usize = 1000;
const PAGE: usize = 100;

/// What a configuration counts: requests whose key starts with `prefix`,
/// whose object has every one of `tags`, made through `access_point`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsFilter {
    pub prefix: Option<String>,
    pub tags: Vec<(String, String)>,
    pub access_point: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsConfig {
    pub id: String,
    /// `None`: every request to the bucket.
    pub filter: Option<MetricsFilter>,
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// Whether `id` is a configuration id: 1 to 64 letters, digits, periods,
/// dashes and underscores.
fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn tag(element: &Element) -> Result<(String, String), S3Error> {
    element.only(&["Key", "Value"])?;
    let key = element.text_of("Key")?.ok_or_else(malformed)?;
    let value = element.text_of("Value")?.ok_or_else(malformed)?;
    if key.is_empty() || key.chars().count() > 128 || value.chars().count() > 256 {
        return Err(invalid(
            "A tag's key is 1 to 128 characters, its value at most 256",
        ));
    }
    Ok((key.to_string(), value.to_string()))
}

/// Parses and checks a MetricsConfiguration, which must be the one `id`
/// names.
pub fn parse(body: &[u8], id: &str) -> Result<MetricsConfig, S3Error> {
    let root = xml::parse(body, "MetricsConfiguration")?;
    root.only(&["Id", "Filter"])?;
    let named = root.text_of("Id")?.ok_or_else(malformed)?;
    if !valid_id(named) {
        return Err(invalid(
            "A metrics configuration's Id is 1 to 64 letters, numbers, periods, dashes and underscores",
        ));
    }
    if named != id {
        return Err(invalid(
            "The Id in the metrics configuration is not the id the request names",
        ));
    }
    let filter = match root.one("Filter")? {
        None => None,
        Some(filter) => {
            filter.only(&["Prefix", "Tag", "AccessPointArn", "And"])?;
            // One of them: a prefix, a tag, an access point, or all of
            // them joined.
            if filter.children.len() != 1 {
                return Err(malformed());
            }
            let condition = &filter.children[0];
            Some(match condition.name.as_str() {
                "Prefix" => MetricsFilter {
                    prefix: Some(condition.text.clone()),
                    ..MetricsFilter::default()
                },
                "Tag" => MetricsFilter {
                    tags: vec![tag(condition)?],
                    ..MetricsFilter::default()
                },
                "AccessPointArn" => MetricsFilter {
                    access_point: Some(condition.text.clone()),
                    ..MetricsFilter::default()
                },
                _ => {
                    condition.only(&["Prefix", "Tag", "AccessPointArn"])?;
                    let tags: Vec<(String, String)> =
                        condition.all("Tag").map(tag).collect::<Result<_, _>>()?;
                    MetricsFilter {
                        prefix: condition.text_of("Prefix")?.map(str::to_string),
                        tags,
                        access_point: condition.text_of("AccessPointArn")?.map(str::to_string),
                    }
                }
            })
        }
    };
    Ok(MetricsConfig {
        id: id.to_string(),
        filter,
    })
}

/// The configuration as XML (as it is also stored), its root named `root`.
fn render_as(config: &MetricsConfig, root: &str, xmlns: bool) -> String {
    let mut out = if xmlns {
        format!("<{root} xmlns=\"{XMLNS}\">")
    } else {
        format!("<{root}>")
    };
    out.push_str(&format!("<Id>{}</Id>", escape(&config.id)));
    if let Some(filter) = &config.filter {
        let conditions = usize::from(filter.prefix.is_some())
            + filter.tags.len()
            + usize::from(filter.access_point.is_some());
        let mut inner = String::new();
        if let Some(prefix) = &filter.prefix {
            inner.push_str(&format!("<Prefix>{}</Prefix>", escape(prefix)));
        }
        for (key, value) in &filter.tags {
            inner.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                escape(key),
                escape(value)
            ));
        }
        if let Some(arn) = &filter.access_point {
            inner.push_str(&format!("<AccessPointArn>{}</AccessPointArn>", escape(arn)));
        }
        if conditions > 1 {
            out.push_str(&format!("<Filter><And>{inner}</And></Filter>"));
        } else {
            out.push_str(&format!("<Filter>{inner}</Filter>"));
        }
    }
    out.push_str(&format!("</{root}>"));
    out
}

pub fn render(config: &MetricsConfig) -> String {
    render_as(config, "MetricsConfiguration", true)
}

/// The query's `id`, required and well formed.
fn query_id(request: &Request<Incoming>) -> Result<Option<String>, S3Error> {
    let found = request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(name, _)| *name == "id")
        .map(|(_, value)| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .map(|id| id.into_owned())
                .map_err(|_| invalid("The id is not UTF-8"))
        })
        .transpose()?;
    Ok(found)
}

fn query_value(request: &Request<Incoming>, name: &str) -> Option<String> {
    request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .and_then(|(_, value)| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .ok()
                .map(|value| value.into_owned())
        })
}

fn no_configuration() -> S3Error {
    S3Error::new(
        404,
        "NoSuchConfiguration",
        "The specified configuration does not exist.",
    )
}

/// PUT, GET or DELETE `?metrics&id=`, or GET `?metrics` (a listing).
pub async fn bucket_metrics(
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
    let id = query_id(&request)?;
    if method == Method::GET && id.is_none() {
        return list(state, &request, bucket).await;
    }
    let Some(id) = id else {
        return Err(S3Error::new(
            400,
            "InvalidArgument",
            "The request names no id",
        ));
    };
    if !valid_id(&id) {
        return Err(invalid(
            "A metrics configuration's id is 1 to 64 letters, numbers, periods, dashes and underscores",
        ));
    }
    let bucket_name = bucket.clone();
    match method {
        Method::PUT => {
            // No checksum needed (the SDKs send none).
            let body = crate::server::read_body(request, auth, MAX_BODY).await?;
            let document = render(&parse(&body, &id)?);
            let refused = crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    let ids = tx.bucket_config_ids(&bucket, BUCKET_METRICS)?;
                    if !ids.contains(&id) && ids.len() >= MAX_CONFIGURATIONS {
                        return Ok(true);
                    }
                    tx.put_bucket_config(&bucket, BUCKET_METRICS, &id, document.as_bytes())?;
                    Ok(false)
                })
            })
            .await?;
            state.metric_recorder.forget(&bucket_name);
            if refused {
                return Err(S3Error::new(
                    400,
                    "TooManyConfigurations",
                    "You are attempting to create a new configuration but have already reached the 1,000-configuration limit.",
                ));
            }
            Ok(empty(StatusCode::OK))
        }
        Method::GET => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_METRICS, &id)
            })
            .await?
            .ok_or_else(no_configuration)?;
            Ok(xml_answer(String::from_utf8_lossy(&stored).into_owned()))
        }
        Method::DELETE => {
            let deleted = crate::scope::object(state, move |file| {
                file.transaction(|tx| tx.delete_bucket_config(&bucket, BUCKET_METRICS, &id))
            })
            .await?;
            state.metric_recorder.forget(&bucket_name);
            if !deleted {
                return Err(no_configuration());
            }
            Ok(empty(StatusCode::NO_CONTENT))
        }
        other => Err(S3Error::not_implemented(format!("{other} ?metrics"))),
    }
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response")
}

fn xml_answer(document: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{document}"
        )))
        .expect("a valid response")
}

/// ListBucketMetricsConfigurations: 100 a page, in id order, continued
/// after the id a token names.
async fn list(
    state: &S3State,
    request: &Request<Incoming>,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    let token = query_value(request, "continuation-token");
    let after = token.clone();
    let (page, truncated) = crate::scope::object(state, move |file| {
        let ids = file.bucket_config_ids(&bucket, BUCKET_METRICS)?;
        let mut page = Vec::new();
        let mut remaining = ids
            .into_iter()
            .filter(|id| after.as_ref().is_none_or(|after| id > after));
        for id in remaining.by_ref().take(PAGE) {
            if let Some(document) = file.bucket_config(&bucket, BUCKET_METRICS, &id)? {
                page.push((id, document));
            }
        }
        Ok((page, remaining.next().is_some()))
    })
    .await?;
    let mut out = format!("<ListMetricsConfigurationsResult xmlns=\"{XMLNS}\">");
    out.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    if let Some(token) = &token {
        out.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            escape(token)
        ));
    }
    if truncated && let Some((last, _)) = page.last() {
        out.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape(last)
        ));
    }
    for (id, document) in &page {
        let config = parse(document, id)?;
        out.push_str(&render_as(&config, "MetricsConfiguration", false));
    }
    out.push_str("</ListMetricsConfigurationsResult>");
    Ok(xml_answer(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(id: &str, inner: &str) -> Result<MetricsConfig, S3Error> {
        parse(
            format!(
                "<MetricsConfiguration xmlns=\"{XMLNS}\"><Id>{id}</Id>{inner}</MetricsConfiguration>"
            )
            .as_bytes(),
            id,
        )
    }

    #[test]
    fn configurations_parse_render_and_refuse_as_s3_does() {
        let whole = config("EntireBucket", "").unwrap();
        assert_eq!(whole.filter, None);
        let joined = config(
            "Blue",
            "<Filter><And><Prefix>documents/</Prefix><Tag><Key>class</Key><Value>blue</Value></Tag>\
             <Tag><Key>priority</Key><Value>high</Value></Tag></And></Filter>",
        )
        .unwrap();
        assert_eq!(
            joined.filter,
            Some(MetricsFilter {
                prefix: Some("documents/".to_string()),
                tags: vec![
                    ("class".to_string(), "blue".to_string()),
                    ("priority".to_string(), "high".to_string())
                ],
                access_point: None,
            })
        );
        for parsed in [
            whole,
            joined,
            config("p", "<Filter><Prefix>a/</Prefix></Filter>").unwrap(),
        ] {
            assert_eq!(
                parse(render(&parsed).as_bytes(), &parsed.id).unwrap(),
                parsed
            );
        }
        let code = |result: Result<MetricsConfig, S3Error>| result.expect_err("refused").code;
        assert_eq!(code(config("bad id", "")), "InvalidArgument");
        assert_eq!(code(config(&"x".repeat(65), "")), "InvalidArgument");
        assert_eq!(
            code(parse(
                b"<MetricsConfiguration><Id>a</Id></MetricsConfiguration>",
                "b"
            )),
            "InvalidArgument"
        );
        assert_eq!(
            code(config(
                "a",
                "<Filter><Prefix>a</Prefix><Prefix>b</Prefix></Filter>"
            )),
            "MalformedXML"
        );
        assert_eq!(code(config("a", "<Filter></Filter>")), "MalformedXML");
        assert_eq!(
            code(config("a", "<Filter><Tag><Key>k</Key></Tag></Filter>")),
            "MalformedXML"
        );
        assert_eq!(code(config("a", "<Other/>")), "MalformedXML");
    }
}
