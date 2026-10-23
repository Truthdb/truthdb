//! Static website hosting: a bucket's website configuration (`?website`):
//! an index document, an error document and routing rules, or a redirect
//! of every request to another host.

use hyper::body::Incoming;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use truthdb_object::{BUCKET_WEBSITE, VersionSel};

use crate::authz::{Authz, Resource};
use crate::error::{S3Error, escape};
use crate::object_lock::{malformed, read_checked_body};
use crate::server::{Authenticated, Body, Connection, RequestIds, S3State, full};
use crate::xml::{self, Element};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// The largest configuration taken, and its routing rules at most (S3's
/// limits).
const MAX_BODY: usize = 128 * 1024;
const MAX_RULES: usize = 50;
/// The redirect codes a routing rule may answer with.
const REDIRECT_CODES: [u16; 7] = [301, 302, 303, 304, 305, 307, 308];

/// Where every request is redirected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedirectAll {
    pub host: String,
    /// `http` or `https`; the request's when none.
    pub protocol: Option<String>,
}

/// A routing rule's redirect.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Redirect {
    pub host: Option<String>,
    pub code: Option<u16>,
    pub protocol: Option<String>,
    pub replace_key_prefix: Option<String>,
    pub replace_key: Option<String>,
}

/// A routing rule: when its condition holds (each part given must), its
/// redirect is answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingRule {
    pub key_prefix: Option<String>,
    pub error_code: Option<u16>,
    pub redirect: Redirect,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebsiteConfig {
    pub redirect_all: Option<RedirectAll>,
    pub index_suffix: Option<String>,
    pub error_key: Option<String>,
    pub rules: Vec<RoutingRule>,
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// A value answered in a Location header must be one.
fn header_text(what: &str, text: &str) -> Result<String, S3Error> {
    if hyper::header::HeaderValue::from_str(text).is_err() {
        return Err(invalid(format!("{what} is not a valid header value")));
    }
    Ok(text.to_string())
}

fn protocol(element: &Element) -> Result<Option<String>, S3Error> {
    match element.text_of("Protocol")? {
        None => Ok(None),
        Some(protocol @ ("http" | "https")) => Ok(Some(protocol.to_string())),
        Some(_) => Err(S3Error::new(
            400,
            "InvalidRequest",
            "Invalid protocol, protocol can be http or https. If not defined the protocol will be selected automatically.",
        )),
    }
}

/// Parses and checks a WebsiteConfiguration.
pub fn parse(body: &[u8]) -> Result<WebsiteConfig, S3Error> {
    let root = xml::parse(body, "WebsiteConfiguration")?;
    root.only(&[
        "RedirectAllRequestsTo",
        "IndexDocument",
        "ErrorDocument",
        "RoutingRules",
    ])?;
    if let Some(all) = root.one("RedirectAllRequestsTo")? {
        if root.children.len() > 1 {
            return Err(invalid(
                "RedirectAllRequestsTo cannot be given with any other part of a website configuration",
            ));
        }
        all.only(&["HostName", "Protocol"])?;
        let host = match all.text_of("HostName")? {
            Some(host) if !host.is_empty() => header_text("HostName", host)?,
            _ => return Err(malformed()),
        };
        return Ok(WebsiteConfig {
            redirect_all: Some(RedirectAll {
                host,
                protocol: protocol(all)?,
            }),
            ..WebsiteConfig::default()
        });
    }
    let index = root.one("IndexDocument")?.ok_or_else(|| {
        invalid("A website configuration must name an IndexDocument or RedirectAllRequestsTo")
    })?;
    index.only(&["Suffix"])?;
    let index_suffix = match index.text_of("Suffix")? {
        Some(suffix) if !suffix.is_empty() && !suffix.contains('/') => suffix.to_string(),
        Some(_) => {
            return Err(invalid(
                "The IndexDocument Suffix must not be empty and must not include a slash character",
            ));
        }
        None => return Err(malformed()),
    };
    let error_key = match root.one("ErrorDocument")? {
        None => None,
        Some(error) => {
            error.only(&["Key"])?;
            match error.text_of("Key")? {
                Some(key) if !key.is_empty() => Some(key.to_string()),
                Some(_) => return Err(invalid("The ErrorDocument Key must not be empty")),
                None => return Err(malformed()),
            }
        }
    };
    let rules = match root.one("RoutingRules")? {
        None => Vec::new(),
        Some(rules) => {
            rules.only(&["RoutingRule"])?;
            if rules.children.len() > MAX_RULES {
                return Err(invalid(format!(
                    "A website configuration has at most {MAX_RULES} routing rules"
                )));
            }
            rules
                .children
                .iter()
                .map(routing_rule)
                .collect::<Result<_, _>>()?
        }
    };
    Ok(WebsiteConfig {
        redirect_all: None,
        index_suffix: Some(index_suffix),
        error_key,
        rules,
    })
}

fn routing_rule(element: &Element) -> Result<RoutingRule, S3Error> {
    element.only(&["Condition", "Redirect"])?;
    let (key_prefix, error_code) = match element.one("Condition")? {
        None => (None, None),
        Some(condition) => {
            condition.only(&["KeyPrefixEquals", "HttpErrorCodeReturnedEquals"])?;
            let key_prefix = condition.text_of("KeyPrefixEquals")?.map(str::to_string);
            let error_code = condition
                .text_of("HttpErrorCodeReturnedEquals")?
                .map(|code| match code.parse::<u16>() {
                    Ok(code @ 400..=599) => Ok(code),
                    _ => Err(invalid(format!(
                        "HttpErrorCodeReturnedEquals {code} is not an error code"
                    ))),
                })
                .transpose()?;
            if key_prefix.is_none() && error_code.is_none() {
                return Err(invalid(
                    "A routing rule's Condition names KeyPrefixEquals, HttpErrorCodeReturnedEquals or both",
                ));
            }
            (key_prefix, error_code)
        }
    };
    let redirect = element.one("Redirect")?.ok_or_else(malformed)?;
    redirect.only(&[
        "HostName",
        "HttpRedirectCode",
        "Protocol",
        "ReplaceKeyPrefixWith",
        "ReplaceKeyWith",
    ])?;
    if redirect.children.is_empty() {
        return Err(invalid("A routing rule's Redirect names where to"));
    }
    let code = redirect
        .text_of("HttpRedirectCode")?
        .map(|code| match code.parse::<u16>() {
            Ok(number) if REDIRECT_CODES.contains(&number) => Ok(number),
            _ => Err(S3Error::new(
                400,
                "InvalidRequest",
                format!(
                    "The provided HTTP redirect code ({code}) is not valid. Valid codes are 3XX except 300."
                ),
            )),
        })
        .transpose()?;
    let replace_key_prefix = redirect
        .text_of("ReplaceKeyPrefixWith")?
        .map(|text| header_text("ReplaceKeyPrefixWith", text))
        .transpose()?;
    let replace_key = redirect
        .text_of("ReplaceKeyWith")?
        .map(|text| header_text("ReplaceKeyWith", text))
        .transpose()?;
    if replace_key_prefix.is_some() && replace_key.is_some() {
        return Err(invalid(
            "A routing rule's Redirect names ReplaceKeyPrefixWith or ReplaceKeyWith, not both",
        ));
    }
    let host = redirect
        .text_of("HostName")?
        .map(|text| header_text("HostName", text))
        .transpose()?;
    Ok(RoutingRule {
        key_prefix,
        error_code,
        redirect: Redirect {
            host,
            code,
            protocol: protocol(redirect)?,
            replace_key_prefix,
            replace_key,
        },
    })
}

/// The configuration as XML (as it is also stored).
pub fn render(config: &WebsiteConfig) -> String {
    let mut out = format!("<WebsiteConfiguration xmlns=\"{XMLNS}\">");
    let leaf = |out: &mut String, name: &str, value: &Option<String>| {
        if let Some(value) = value {
            out.push_str(&format!("<{name}>{}</{name}>", escape(value)));
        }
    };
    if let Some(all) = &config.redirect_all {
        out.push_str("<RedirectAllRequestsTo>");
        leaf(&mut out, "HostName", &Some(all.host.clone()));
        leaf(&mut out, "Protocol", &all.protocol);
        out.push_str("</RedirectAllRequestsTo>");
    }
    if config.index_suffix.is_some() {
        out.push_str("<IndexDocument>");
        leaf(&mut out, "Suffix", &config.index_suffix);
        out.push_str("</IndexDocument>");
    }
    if config.error_key.is_some() {
        out.push_str("<ErrorDocument>");
        leaf(&mut out, "Key", &config.error_key);
        out.push_str("</ErrorDocument>");
    }
    if !config.rules.is_empty() {
        out.push_str("<RoutingRules>");
        for rule in &config.rules {
            out.push_str("<RoutingRule>");
            if rule.key_prefix.is_some() || rule.error_code.is_some() {
                out.push_str("<Condition>");
                leaf(
                    &mut out,
                    "HttpErrorCodeReturnedEquals",
                    &rule.error_code.map(|code| code.to_string()),
                );
                leaf(&mut out, "KeyPrefixEquals", &rule.key_prefix);
                out.push_str("</Condition>");
            }
            let redirect = &rule.redirect;
            out.push_str("<Redirect>");
            leaf(&mut out, "HostName", &redirect.host);
            leaf(
                &mut out,
                "HttpRedirectCode",
                &redirect.code.map(|code| code.to_string()),
            );
            leaf(&mut out, "Protocol", &redirect.protocol);
            leaf(
                &mut out,
                "ReplaceKeyPrefixWith",
                &redirect.replace_key_prefix,
            );
            leaf(&mut out, "ReplaceKeyWith", &redirect.replace_key);
            out.push_str("</Redirect>");
            out.push_str("</RoutingRule>");
        }
        out.push_str("</RoutingRules>");
    }
    out.push_str("</WebsiteConfiguration>");
    out
}

/// PUT, GET or DELETE `?website` on a bucket.
pub async fn bucket_website(
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
            // body), and read for every website request: held to the
            // same limit.
            if document.len() > MAX_BODY {
                return Err(invalid("The website configuration is too large"));
            }
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.put_bucket_config(&bucket, BUCKET_WEBSITE, "", document.as_bytes())
                })
            })
            .await?;
            Ok(empty(StatusCode::OK))
        }
        Method::GET => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_WEBSITE, "")
            })
            .await?
            .ok_or_else(|| {
                S3Error::new(
                    404,
                    "NoSuchWebsiteConfiguration",
                    "The specified bucket does not have a website configuration",
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
                file.transaction(|tx| tx.delete_bucket_config(&bucket, BUCKET_WEBSITE, ""))
            })
            .await?;
            Ok(empty(StatusCode::NO_CONTENT))
        }
        other => Err(S3Error::not_implemented(format!("{other} ?website"))),
    }
}

/// What a key keeps unencoded in a redirect's Location.
const LOCATION: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// The bucket a request to its website endpoint
/// (`<bucket>.s3-website.<domain>`) is for, in the request's extensions.
#[derive(Clone)]
pub struct WebsiteHost(pub String);

/// The bucket `host` names as `<bucket>.s3-website.<domain>`.
pub fn website_bucket(host: &str, domains: &[String]) -> Option<String> {
    let website: Vec<String> = domains
        .iter()
        .map(|domain| format!("s3-website.{domain}"))
        .collect();
    crate::api::host_bucket(host, &website)
}

fn reason(status: u16) -> &'static str {
    StatusCode::from_u16(status)
        .ok()
        .and_then(|status| status.canonical_reason())
        .unwrap_or("Error")
}

/// An error as the website endpoint answers it: an HTML page.
fn page(err: &S3Error, key: Option<&str>, ids: &RequestIds, head_only: bool) -> Response<Body> {
    let title = format!("{} {}", err.status, reason(err.status));
    let mut items = vec![
        format!("<li>Code: {}</li>", escape(err.code)),
        format!("<li>Message: {}</li>", escape(&err.message)),
    ];
    if let Some(key) = key {
        items.push(format!("<li>Key: {}</li>", escape(key)));
    }
    items.push(format!("<li>RequestId: {}</li>", escape(&ids.request_id)));
    items.push(format!("<li>HostId: {}</li>", escape(&ids.host_id)));
    let body = format!(
        "<html>\n<head><title>{title}</title></head>\n<body>\n<h1>{title}</h1>\n<ul>\n{}\n</ul>\n<hr/>\n</body>\n</html>\n",
        items.join("\n")
    );
    Response::builder()
        .status(StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "text/html; charset=utf-8")
        .header("x-amz-error-code", err.code)
        .body(full(if head_only { String::new() } else { body }))
        .expect("a valid response")
}

fn redirect(status: u16, location: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::MOVED_PERMANENTLY))
        .header("location", location)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response")
}

/// Where a request for `key` that a routing rule matched goes.
fn rule_location(rule: &RoutingRule, key: &str, protocol: &str, host: &str) -> String {
    let redirect = &rule.redirect;
    let new_key = match (&redirect.replace_key, &redirect.replace_key_prefix) {
        (Some(replacement), _) => replacement.clone(),
        (None, Some(prefix)) => {
            let rest = key
                .strip_prefix(rule.key_prefix.as_deref().unwrap_or(""))
                .unwrap_or(key);
            format!("{prefix}{rest}")
        }
        (None, None) => key.to_string(),
    };
    format!(
        "{}://{}/{}",
        redirect.protocol.as_deref().unwrap_or(protocol),
        redirect.host.as_deref().unwrap_or(host),
        percent_encoding::utf8_percent_encode(&new_key, LOCATION)
    )
}

/// The first routing rule for `key` when the request would be answered
/// with `error` (none: before it is looked up).
fn rule_for<'c>(
    config: &'c WebsiteConfig,
    key: &str,
    error: Option<u16>,
) -> Option<&'c RoutingRule> {
    config.rules.iter().find(|rule| {
        rule.error_code == error
            && rule
                .key_prefix
                .as_deref()
                .is_none_or(|prefix| key.starts_with(prefix))
    })
}

/// An object's answer, as a GET or HEAD reads it (its conditions and
/// Range from `headers`). The read runs in its own task, as GetObject's
/// does: a client that disconnects drops this handler, and the task still
/// releases the lease it takes.
async fn read(
    state: &S3State,
    headers: &HeaderMap,
    bucket: &str,
    key: String,
    head_only: bool,
) -> Result<Response<Body>, S3Error> {
    let task_state = S3State {
        engine: state.engine.clone(),
        region: state.region.clone(),
        signing_keys: state.signing_keys.clone(),
        storage: state.storage.clone(),
        restore_delays: state.restore_delays,
        domains: state.domains.clone(),
    };
    let (headers, bucket) = (headers.clone(), bucket.to_string());
    let scope = crate::scope::current().ok_or_else(|| S3Error::internal("no request scope"))?;
    tokio::spawn(crate::scope::within(scope, async move {
        crate::objects::read_object(
            &task_state,
            &headers,
            bucket,
            key,
            VersionSel::Latest,
            None,
            head_only,
            &[],
            None,
            crate::objects::LockVisible {
                retention: false,
                legal_hold: false,
            },
        )
        .await
    }))
    .await
    .map_err(|err| S3Error::internal(err.to_string()))?
}

/// A request to a bucket's website endpoint: GET and HEAD only, always
/// anonymous (what it reads must be public), answered from the bucket's
/// website configuration with HTML error pages.
pub async fn serve(state: &S3State, request: Request<Incoming>, bucket: String) -> Response<Body> {
    let ids = request
        .extensions()
        .get::<RequestIds>()
        .cloned()
        .expect("handle sets the request ids");
    let head_only = match *request.method() {
        Method::GET => false,
        Method::HEAD => true,
        _ => {
            let err = S3Error::new(
                405,
                "MethodNotAllowed",
                "The specified method is not allowed against this resource.",
            );
            return page(&err, None, &ids, false);
        }
    };
    let connection = request.extensions().get::<Connection>().copied();
    let protocol = if connection.is_some_and(|connection| connection.tls) {
        "https"
    } else {
        "http"
    };
    let host = request
        .headers()
        .get(hyper::header::HOST)
        .and_then(|host| host.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let name = bucket.clone();
    let loaded = state
        .engine
        .run_object(move |file| {
            Ok((
                file.bucket(&name)?.is_some(),
                file.bucket_config(&name, BUCKET_WEBSITE, "")?,
            ))
        })
        .await;
    let config = match loaded {
        Ok((false, _)) => {
            let err = S3Error::new(404, "NoSuchBucket", "The specified bucket does not exist");
            return page(&err, None, &ids, head_only);
        }
        Ok((true, None)) => {
            let err = S3Error::new(
                404,
                "NoSuchWebsiteConfiguration",
                "The specified bucket does not have a website configuration",
            );
            return page(&err, None, &ids, head_only);
        }
        Ok((true, Some(document))) => match parse(&document) {
            Ok(config) => config,
            Err(err) => return page(&err, None, &ids, head_only),
        },
        Err(err) => return page(&S3Error::internal(err.to_string()), None, &ids, head_only),
    };
    if let Some(all) = &config.redirect_all {
        let location = format!(
            "{}://{}{}",
            all.protocol.as_deref().unwrap_or(protocol),
            all.host,
            request.uri().path()
        );
        return redirect(301, &location);
    }
    let Some(path_key) = crate::sigv4::decode_path(request.uri().path()) else {
        let err = S3Error::invalid_request("the path is not UTF-8");
        return page(&err, None, &ids, head_only);
    };
    let path_key = path_key.strip_prefix('/').unwrap_or(&path_key).to_string();
    if let Some(rule) = rule_for(&config, &path_key, None) {
        let code = rule.redirect.code.unwrap_or(301);
        return redirect(code, &rule_location(rule, &path_key, protocol, &host));
    }
    let suffix = config.index_suffix.clone().unwrap_or_default();
    let key = if path_key.is_empty() || path_key.ends_with('/') {
        format!("{path_key}{suffix}")
    } else {
        path_key.clone()
    };
    // The folder's index a missing key may be redirected to.
    let folder_index = (key == path_key && !key.is_empty()).then(|| format!("{key}/{suffix}"));
    let mut objects = vec![(bucket.clone(), key.clone(), VersionSel::Latest)];
    if let Some(index) = &folder_index {
        objects.push((bucket.clone(), index.clone(), VersionSel::Latest));
    }
    if let Some(error_key) = &config.error_key {
        objects.push((bucket.clone(), error_key.clone(), VersionSel::Latest));
    }
    let authz = match Authz::load(
        state,
        None,
        None,
        request.headers(),
        "",
        connection,
        vec![bucket.clone()],
        objects,
    )
    .await
    {
        Ok(authz) => authz,
        Err(err) => return page(&err, Some(&key), &ids, head_only),
    };
    let scope = crate::scope::Scope {
        key: None,
        buckets: authz.owners(),
        origin: None,
    };
    let headers = request.headers().clone();
    crate::scope::within(scope, async move {
        let answer = async {
            authz.check("s3:GetObject", Resource::Object(&bucket, &key))?;
            let (name, wanted) = (bucket.clone(), key.clone());
            let found = crate::scope::object(state, move |file| {
                file.head_object(&name, &wanted, VersionSel::Latest)
            })
            .await?;
            match found {
                Ok(info) if !info.delete_marker => {
                    if let Some(location) = &info.attrs.website_redirect {
                        return Ok(redirect(301, location));
                    }
                    read(state, &headers, &bucket, key.clone(), head_only).await
                }
                _ => {
                    // A folder named without its slash, whose index exists
                    // and the public may read (else its existence is not
                    // told).
                    if let Some(index) = &folder_index
                        && authz
                            .check("s3:GetObject", Resource::Object(&bucket, index))
                            .is_ok()
                    {
                        let (name, wanted) = (bucket.clone(), index.clone());
                        let folder = crate::scope::object(state, move |file| {
                            file.head_object(&name, &wanted, VersionSel::Latest)
                        })
                        .await?;
                        if folder.is_ok_and(|info| !info.delete_marker) {
                            let location = format!(
                                "/{}/",
                                percent_encoding::utf8_percent_encode(&key, LOCATION)
                            );
                            return Ok(redirect(302, &location));
                        }
                    }
                    // Whether a key is missing is told only to those who
                    // may list the bucket.
                    Err(
                        if authz
                            .check("s3:ListBucket", Resource::Bucket(&bucket))
                            .is_ok()
                        {
                            S3Error::new(404, "NoSuchKey", "The specified key does not exist.")
                        } else {
                            S3Error::access_denied("Access Denied")
                        },
                    )
                }
            }
        }
        .await;
        let err = match answer {
            Ok(response) => return response,
            Err(err) => err,
        };
        if let Some(rule) = rule_for(&config, &path_key, Some(err.status)) {
            let code = rule.redirect.code.unwrap_or(301);
            return redirect(code, &rule_location(rule, &path_key, protocol, &host));
        }
        // A 4xx answers with the error document, when the public may read it.
        if (400..500).contains(&err.status)
            && let Some(error_key) = &config.error_key
            && authz
                .check("s3:GetObject", Resource::Object(&bucket, error_key))
                .is_ok()
            && let Ok(mut response) = read(
                state,
                &HeaderMap::new(),
                &bucket,
                error_key.clone(),
                head_only,
            )
            .await
            && response.status() == StatusCode::OK
        {
            *response.status_mut() =
                StatusCode::from_u16(err.status).unwrap_or(StatusCode::NOT_FOUND);
            return response;
        }
        page(&err, Some(&key), &ids, head_only)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(inner: &str) -> Result<WebsiteConfig, S3Error> {
        parse(
            format!("<WebsiteConfiguration xmlns=\"{XMLNS}\">{inner}</WebsiteConfiguration>")
                .as_bytes(),
        )
    }

    fn code(result: Result<WebsiteConfig, S3Error>) -> String {
        result.expect_err("refused").code.to_string()
    }

    #[test]
    fn configurations_parse_and_render_back() {
        let documents = "<IndexDocument><Suffix>index.html</Suffix></IndexDocument>\
            <ErrorDocument><Key>errors/4xx.html</Key></ErrorDocument>";
        let rules = "<RoutingRules><RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>\
            <Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect></RoutingRule>\
            <RoutingRule><Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>\
            <KeyPrefixEquals>images/</KeyPrefixEquals></Condition>\
            <Redirect><HostName>example.com</HostName><HttpRedirectCode>307</HttpRedirectCode>\
            <Protocol>https</Protocol><ReplaceKeyWith>gone.html</ReplaceKeyWith></Redirect></RoutingRule></RoutingRules>";
        let parsed = config(&format!("{documents}{rules}")).unwrap();
        assert_eq!(parsed.index_suffix.as_deref(), Some("index.html"));
        assert_eq!(parsed.error_key.as_deref(), Some("errors/4xx.html"));
        assert_eq!(parsed.rules.len(), 2);
        assert_eq!(parsed.rules[1].error_code, Some(404));
        assert_eq!(parsed.rules[1].redirect.code, Some(307));
        assert_eq!(parse(render(&parsed).as_bytes()).unwrap(), parsed);
        let all = config("<RedirectAllRequestsTo><HostName>www.example.com</HostName><Protocol>https</Protocol></RedirectAllRequestsTo>").unwrap();
        assert_eq!(
            all.redirect_all,
            Some(RedirectAll {
                host: "www.example.com".to_string(),
                protocol: Some("https".to_string()),
            })
        );
        assert_eq!(parse(render(&all).as_bytes()).unwrap(), all);
    }

    #[test]
    fn configurations_are_refused_as_s3_refuses_them() {
        let index = "<IndexDocument><Suffix>index.html</Suffix></IndexDocument>";
        let rule = |condition: &str, redirect: &str| {
            config(&format!(
                "{index}<RoutingRules><RoutingRule>{condition}<Redirect>{redirect}</Redirect></RoutingRule></RoutingRules>"
            ))
        };
        // RedirectAllRequestsTo stands alone.
        assert_eq!(
            code(config(&format!(
                "<RedirectAllRequestsTo><HostName>h</HostName></RedirectAllRequestsTo>{index}"
            ))),
            "InvalidArgument"
        );
        assert_eq!(
            code(config(
                "<RedirectAllRequestsTo><Protocol>http</Protocol></RedirectAllRequestsTo>"
            )),
            "MalformedXML"
        );
        assert_eq!(
            code(config(
                "<RedirectAllRequestsTo><HostName>h</HostName><Protocol>ftp</Protocol></RedirectAllRequestsTo>"
            )),
            "InvalidRequest"
        );
        // An index document, whose suffix is a name without a slash.
        assert_eq!(code(config("")), "InvalidArgument");
        assert_eq!(
            code(config("<ErrorDocument><Key>e.html</Key></ErrorDocument>")),
            "InvalidArgument"
        );
        for suffix in ["", "a/index.html"] {
            assert_eq!(
                code(config(&format!(
                    "<IndexDocument><Suffix>{suffix}</Suffix></IndexDocument>"
                ))),
                "InvalidArgument"
            );
        }
        assert_eq!(
            code(config(&format!(
                "{index}<ErrorDocument><Key></Key></ErrorDocument>"
            ))),
            "InvalidArgument"
        );
        assert_eq!(code(config(&format!("{index}<Other/>"))), "MalformedXML");
        // Routing rules.
        let codes = |code: &str| rule("", &format!("<HttpRedirectCode>{code}</HttpRedirectCode>"));
        for good in ["301", "302", "303", "304", "305", "307", "308"] {
            assert!(codes(good).is_ok(), "{good}");
        }
        for bad in ["300", "306", "314", "200", "x"] {
            let refused = codes(bad).expect_err(bad);
            assert_eq!(refused.code, "InvalidRequest");
            assert_eq!(
                refused.message,
                format!(
                    "The provided HTTP redirect code ({bad}) is not valid. Valid codes are 3XX except 300."
                )
            );
        }
        assert_eq!(code(rule("", "")), "InvalidArgument");
        assert_eq!(
            code(rule(
                "",
                "<ReplaceKeyPrefixWith>a/</ReplaceKeyPrefixWith><ReplaceKeyWith>b</ReplaceKeyWith>"
            )),
            "InvalidArgument"
        );
        assert_eq!(
            code(rule("<Condition></Condition>", "<HostName>h</HostName>")),
            "InvalidArgument"
        );
        for error in ["200", "302", "600", "x"] {
            assert_eq!(
                code(rule(
                    &format!(
                        "<Condition><HttpErrorCodeReturnedEquals>{error}</HttpErrorCodeReturnedEquals></Condition>"
                    ),
                    "<HostName>h</HostName>"
                )),
                "InvalidArgument",
                "{error}"
            );
        }
        assert_eq!(
            code(rule("", "<Protocol>gopher</Protocol>")),
            "InvalidRequest"
        );
        assert_eq!(
            code(rule("", "<HostName>a\u{7f}b</HostName>")),
            "InvalidArgument"
        );
        let many =
            "<RoutingRule><Redirect><HostName>h</HostName></Redirect></RoutingRule>".repeat(51);
        assert_eq!(
            code(config(&format!(
                "{index}<RoutingRules>{many}</RoutingRules>"
            ))),
            "InvalidArgument"
        );
        let fifty =
            "<RoutingRule><Redirect><HostName>h</HostName></Redirect></RoutingRule>".repeat(50);
        assert!(config(&format!("{index}<RoutingRules>{fifty}</RoutingRules>")).is_ok());
    }
}
