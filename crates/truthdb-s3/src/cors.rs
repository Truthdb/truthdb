//! Cross-origin resource sharing (CORS): a bucket's CORS configuration
//! (`?cors`: up to 100 rules of origins, methods, headers), the preflight
//! (an unauthenticated OPTIONS a browser sends before a cross-origin
//! request), and the `Access-Control-*` headers on every answer to a
//! request with an `Origin` the configuration allows, errors included.
//!
//! The first rule allowing the origin, the method and every header asked
//! for decides. An origin or header pattern holds at most one `*`, which
//! matches any run of characters.

use hyper::body::Incoming;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use truthdb_object::BUCKET_CORS;

use crate::error::{S3Error, escape};
use crate::object_lock::{malformed, read_checked_body};
use crate::server::{Authenticated, Body, S3State, full};
use crate::xml::{self, Element};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// The largest configuration taken, and its rules at most (S3's limits).
const MAX_BODY: usize = 64 * 1024;
const MAX_RULES: usize = 100;
const METHODS: [&str; 5] = ["GET", "PUT", "HEAD", "POST", "DELETE"];
/// The headers a preflight may ask for at most (matching them against
/// every rule is paid for by an unauthenticated request).
const MAX_REQUESTED_HEADERS: usize = 64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_headers: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorsConfig {
    pub rules: Vec<CorsRule>,
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidRequest", message)
}

/// Whether `text` matches `pattern` (at most one `*`, any run of
/// characters), case-sensitively.
fn matches(pattern: &str, text: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == text,
        Some((head, tail)) => {
            text.len() >= head.len() + tail.len() && text.starts_with(head) && text.ends_with(tail)
        }
    }
}

/// Parses and checks a CORSConfiguration as S3 does.
pub fn parse(body: &[u8]) -> Result<CorsConfig, S3Error> {
    let root = xml::parse(body, "CORSConfiguration")?;
    root.only(&["CORSRule"])?;
    let rules: Vec<CorsRule> = root.all("CORSRule").map(rule).collect::<Result<_, _>>()?;
    if rules.is_empty() {
        return Err(malformed());
    }
    if rules.len() > MAX_RULES {
        return Err(invalid(format!(
            "A CORS configuration has at most {MAX_RULES} rules"
        )));
    }
    Ok(CorsConfig { rules })
}

fn texts(element: &Element, name: &str) -> Vec<String> {
    element.all(name).map(|child| child.text.clone()).collect()
}

fn rule(element: &Element) -> Result<CorsRule, S3Error> {
    element.only(&[
        "ID",
        "AllowedHeader",
        "AllowedMethod",
        "AllowedOrigin",
        "ExposeHeader",
        "MaxAgeSeconds",
    ])?;
    let id = element.text_of("ID")?.map(str::to_string);
    if id.as_ref().is_some_and(|id| id.chars().count() > 255) {
        return Err(invalid("A CORS rule's ID is at most 255 characters"));
    }
    let allowed_methods = texts(element, "AllowedMethod");
    if allowed_methods.is_empty() {
        return Err(malformed());
    }
    if let Some(method) = allowed_methods
        .iter()
        .find(|method| !METHODS.contains(&method.as_str()))
    {
        return Err(invalid(format!(
            "Found unsupported HTTP method in CORS config. Unsupported method is {method}"
        )));
    }
    let allowed_origins = texts(element, "AllowedOrigin");
    if allowed_origins.is_empty() {
        return Err(malformed());
    }
    let allowed_headers = texts(element, "AllowedHeader");
    let expose_headers = texts(element, "ExposeHeader");
    // Each is answered in a header: it must be one.
    for (what, values) in [
        ("AllowedOrigin", &allowed_origins),
        ("AllowedHeader", &allowed_headers),
        ("ExposeHeader", &expose_headers),
    ] {
        if values
            .iter()
            .any(|value| hyper::header::HeaderValue::from_str(value).is_err())
        {
            return Err(invalid(format!("{what} is not a valid header value")));
        }
    }
    for (what, values) in [
        ("AllowedOrigin", &allowed_origins),
        ("AllowedHeader", &allowed_headers),
    ] {
        if values.iter().any(|value| value.matches('*').count() > 1) {
            return Err(invalid(format!(
                "{what} can not have more than one wildcard"
            )));
        }
    }
    let max_age_seconds = element
        .text_of("MaxAgeSeconds")?
        .map(|text| text.parse::<u32>().map_err(|_| malformed()))
        .transpose()?;
    Ok(CorsRule {
        id,
        allowed_headers,
        allowed_methods,
        allowed_origins,
        expose_headers,
        max_age_seconds,
    })
}

/// The configuration as XML (as it is also stored).
pub fn render(config: &CorsConfig) -> String {
    let mut out = format!("<CORSConfiguration xmlns=\"{XMLNS}\">");
    for rule in &config.rules {
        out.push_str("<CORSRule>");
        if let Some(id) = &rule.id {
            out.push_str(&format!("<ID>{}</ID>", escape(id)));
        }
        for (name, values) in [
            ("AllowedHeader", &rule.allowed_headers),
            ("AllowedMethod", &rule.allowed_methods),
            ("AllowedOrigin", &rule.allowed_origins),
            ("ExposeHeader", &rule.expose_headers),
        ] {
            for value in values {
                out.push_str(&format!("<{name}>{}</{name}>", escape(value)));
            }
        }
        if let Some(age) = rule.max_age_seconds {
            out.push_str(&format!("<MaxAgeSeconds>{age}</MaxAgeSeconds>"));
        }
        out.push_str("</CORSRule>");
    }
    out.push_str("</CORSConfiguration>");
    out
}

impl CorsConfig {
    /// The first rule allowing `origin`, `method` and every header of
    /// `headers` (case-insensitively).
    pub fn rule_for(&self, origin: &str, method: &str, headers: &[String]) -> Option<&CorsRule> {
        let headers: Vec<String> = headers
            .iter()
            .map(|header| header.to_ascii_lowercase())
            .collect();
        self.rules.iter().find(|rule| {
            if !(rule
                .allowed_origins
                .iter()
                .any(|pattern| matches(pattern, origin))
                && rule.allowed_methods.iter().any(|allowed| allowed == method))
            {
                return false;
            }
            let patterns: Vec<String> = rule
                .allowed_headers
                .iter()
                .map(|pattern| pattern.to_ascii_lowercase())
                .collect();
            headers
                .iter()
                .all(|header| patterns.iter().any(|pattern| matches(pattern, header)))
        })
    }
}

/// The `Access-Control-*` headers a rule answers `origin` with:
/// `Access-Control-Allow-Origin` (`*` when the rule allows every origin,
/// else the origin, with `Access-Control-Allow-Credentials`), the rule's
/// methods, exposed headers and age, and (for a preflight) the headers
/// asked for.
pub fn rule_headers(
    rule: &CorsRule,
    origin: &str,
    requested: &[String],
) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if rule.allowed_origins.iter().any(|pattern| pattern == "*") {
        out.push(("access-control-allow-origin", "*".to_string()));
    } else {
        out.push(("access-control-allow-origin", origin.to_string()));
        out.push(("access-control-allow-credentials", "true".to_string()));
    }
    out.push((
        "access-control-allow-methods",
        rule.allowed_methods.join(", "),
    ));
    if !requested.is_empty() {
        out.push(("access-control-allow-headers", requested.join(", ")));
    }
    if !rule.expose_headers.is_empty() {
        out.push((
            "access-control-expose-headers",
            rule.expose_headers.join(", "),
        ));
    }
    if let Some(age) = rule.max_age_seconds {
        out.push(("access-control-max-age", age.to_string()));
    }
    out
}

/// The headers a preflight's Vary names (its answer depends on them).
pub const VARY: &str = "Origin, Access-Control-Request-Headers, Access-Control-Request-Method";

fn header<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// The bucket's CORS configuration, if it has one (read without a request
/// scope: a preflight is not authenticated).
pub async fn load(state: &S3State, bucket: &str) -> Result<Option<CorsConfig>, S3Error> {
    let name = bucket.to_string();
    let stored = state
        .engine
        .run_object(move |file| file.bucket_config(&name, BUCKET_CORS, ""))
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?;
    Ok(stored.and_then(|document| parse(&document).ok()))
}

fn forbidden(message: &str) -> S3Error {
    S3Error::new(403, "AccessForbidden", format!("CORSResponse: {message}"))
}

/// A preflight (OPTIONS on a bucket or an object), answered before any
/// authentication.
pub async fn preflight(
    state: &S3State,
    request: &Request<Incoming>,
) -> Result<Response<Body>, S3Error> {
    let bucket = match crate::api::target(request)? {
        crate::api::Target::Bucket(bucket) | crate::api::Target::Object(bucket, _) => bucket,
        crate::api::Target::Service => {
            return Err(S3Error::new(
                400,
                "BadRequest",
                "A preflight names a bucket",
            ));
        }
    };
    let headers = request.headers();
    let (Some(origin), Some(method)) = (
        header(headers, "origin"),
        header(headers, "access-control-request-method"),
    ) else {
        return Err(S3Error::new(
            400,
            "BadRequest",
            "Insufficient information. Origin request header needed.",
        ));
    };
    let requested: Vec<String> = header(headers, "access-control-request-headers")
        .map(|list| {
            list.split(',')
                .map(|header| header.trim().to_string())
                .filter(|header| !header.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if requested.len() > MAX_REQUESTED_HEADERS {
        return Err(forbidden("This CORS request is not allowed."));
    }
    let Some(config) = load(state, &bucket).await? else {
        return Err(forbidden("CORS is not enabled for this bucket."));
    };
    // A bucket with a CORS configuration exists: the preflight counts in
    // the request metrics (answered at once, by the bucket of the name
    // now: no owner to check).
    crate::metering::mark_seen(request, None);
    let Some(rule) = config.rule_for(origin, method, &requested) else {
        return Err(forbidden("This CORS request is not allowed."));
    };
    let mut response = Response::new(full(bytes::Bytes::new()));
    let headers = response.headers_mut();
    headers.insert("vary", hyper::header::HeaderValue::from_static(VARY));
    // A value that cannot be a header (none is stored since checked) is
    // left out rather than failing the answer.
    for (name, value) in rule_headers(rule, origin, &requested) {
        if let Ok(value) = hyper::header::HeaderValue::from_str(&value) {
            headers.insert(name, value);
        }
    }
    Ok(response)
}

/// The CORS headers of the answer to a request with an `Origin`, when its
/// bucket's configuration allows the origin and the request's method
/// (none otherwise: the request itself is not refused for it).
pub async fn answer_headers(
    state: &S3State,
    bucket: &str,
    origin: &str,
    method: &Method,
) -> Vec<(&'static str, String)> {
    let Ok(Some(config)) = load(state, bucket).await else {
        return Vec::new();
    };
    let mut out = vec![("vary", "Origin".to_string())];
    if let Some(rule) = config.rule_for(origin, method.as_str(), &[]) {
        out.extend(rule_headers(rule, origin, &[]));
    }
    out
}

/// PUT, GET or DELETE `?cors` on a bucket.
pub async fn bucket_cors(
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
    let empty = |status: StatusCode| {
        Response::builder()
            .status(status)
            .body(full(bytes::Bytes::new()))
            .expect("a valid response")
    };
    match method {
        Method::PUT => {
            let body = read_checked_body(request, auth, MAX_BODY).await?;
            let document = render(&parse(&body)?);
            // Stored as rendered (escaped, so possibly larger than the
            // body), and read for every request with an Origin: held to
            // the same limit.
            if document.len() > MAX_BODY {
                return Err(invalid("The CORS configuration is too large"));
            }
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.put_bucket_config(&bucket, BUCKET_CORS, "", document.as_bytes())
                })
            })
            .await?;
            Ok(empty(StatusCode::OK))
        }
        Method::GET => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_CORS, "")
            })
            .await?
            .ok_or_else(|| {
                S3Error::new(
                    404,
                    "NoSuchCORSConfiguration",
                    "The CORS configuration does not exist",
                )
            })?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                    String::from_utf8_lossy(&stored)
                )))
                .expect("a valid response"))
        }
        Method::DELETE => {
            crate::scope::object(state, move |file| {
                file.transaction(|tx| tx.delete_bucket_config(&bucket, BUCKET_CORS, ""))
            })
            .await?;
            Ok(empty(StatusCode::NO_CONTENT))
        }
        other => Err(S3Error::not_implemented(format!("{other} ?cors"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(rules: &str) -> Result<CorsConfig, S3Error> {
        parse(
            format!("<CORSConfiguration xmlns=\"{XMLNS}\">{rules}</CORSConfiguration>").as_bytes(),
        )
    }

    #[test]
    fn origins_match_with_one_wildcard() {
        for (pattern, origin, expected) in [
            ("*suffix", "foo.suffix", true),
            ("*suffix", "foo.suffix.get", false),
            ("start*end", "startend", true),
            ("start*end", "start12end", true),
            ("start*end", "0start12end", false),
            ("prefix*", "prefix.suffix", true),
            ("prefix*", "bla.prefix", false),
            ("*", "anything", true),
            ("https://a.example", "https://a.example", true),
            ("https://a.example", "https://b.example", false),
            ("ab*ba", "aba", false),
        ] {
            assert_eq!(matches(pattern, origin), expected, "{pattern} {origin}");
        }
    }

    #[test]
    fn the_first_rule_allowing_origin_method_and_headers_decides() {
        let parsed = config(
            "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*suffix</AllowedOrigin></CORSRule>\
             <CORSRule><ID>put</ID><AllowedMethod>PUT</AllowedMethod><AllowedMethod>GET</AllowedMethod>\
             <AllowedOrigin>https://*.example</AllowedOrigin><AllowedHeader>x-amz-*</AllowedHeader>\
             <AllowedHeader>Content-Type</AllowedHeader><ExposeHeader>ETag</ExposeHeader>\
             <MaxAgeSeconds>300</MaxAgeSeconds></CORSRule>",
        )
        .unwrap();
        assert_eq!(parsed.rule_for("foo.suffix", "GET", &[]).unwrap().id, None);
        assert_eq!(parsed.rule_for("foo.suffix", "PUT", &[]), None, "method");
        let headers = vec!["X-Amz-Date".to_string(), "content-type".to_string()];
        let put = parsed
            .rule_for("https://a.example", "PUT", &headers)
            .unwrap();
        assert_eq!(put.id.as_deref(), Some("put"));
        assert_eq!(
            parsed.rule_for("https://a.example", "PUT", &["authorization".to_string()]),
            None,
            "a header not allowed"
        );
        let answer = rule_headers(put, "https://a.example", &headers);
        assert!(answer.contains(&("access-control-allow-origin", "https://a.example".into())));
        assert!(answer.contains(&("access-control-allow-credentials", "true".into())));
        assert!(answer.contains(&("access-control-allow-methods", "PUT, GET".into())));
        assert!(answer.contains(&(
            "access-control-allow-headers",
            "X-Amz-Date, content-type".into()
        )));
        assert!(answer.contains(&("access-control-expose-headers", "ETag".into())));
        assert!(answer.contains(&("access-control-max-age", "300".into())));
        let everyone =
            config("<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule>").unwrap();
        let answer = rule_headers(&everyone.rules[0], "https://x", &[]);
        assert!(answer.contains(&("access-control-allow-origin", "*".into())));
        assert!(
            !answer
                .iter()
                .any(|(name, _)| *name == "access-control-allow-credentials")
        );
    }

    #[test]
    fn configurations_are_checked_as_s3_checks_them() {
        let code = |rules: &str| config(rules).unwrap_err().code;
        assert_eq!(code(""), "MalformedXML", "no rule");
        assert_eq!(
            code("<CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule>"),
            "MalformedXML",
            "no method"
        );
        assert_eq!(
            code("<CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule>"),
            "MalformedXML",
            "no origin"
        );
        assert_eq!(
            code(
                "<CORSRule><AllowedMethod>PATCH</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule>"
            ),
            "InvalidRequest"
        );
        assert_eq!(
            code(
                "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*.*</AllowedOrigin></CORSRule>"
            ),
            "InvalidRequest",
            "two wildcards"
        );
        assert_eq!(
            code(
                "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin>\
                 <AllowedHeader>a*b*</AllowedHeader></CORSRule>"
            ),
            "InvalidRequest"
        );
        let many: String = (0..101)
            .map(|_| "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule>")
            .collect();
        assert_eq!(code(&many), "InvalidRequest", "over 100 rules");
        assert_eq!(
            code(
                "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin>\
                 <ExposeHeader>a&#10;b</ExposeHeader></CORSRule>"
            ),
            "InvalidRequest",
            "not a header value"
        );
        assert_eq!(
            code(
                "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin>\
                 <MaxAgeSeconds>-1</MaxAgeSeconds></CORSRule>"
            ),
            "MalformedXML"
        );
        assert_eq!(
            code(
                "<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin><Other/></CORSRule>"
            ),
            "MalformedXML"
        );
        // A configuration reads back as it was rendered.
        let parsed = config(
            "<CORSRule><ID>a&amp;b</ID><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin>\
             <ExposeHeader>x</ExposeHeader></CORSRule>",
        )
        .unwrap();
        assert_eq!(parse(render(&parsed).as_bytes()).unwrap(), parsed);
    }
}
