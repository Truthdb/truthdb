//! A small S3 client: the operations TruthDB's own tools need (buckets,
//! listings, reading, writing in one go or in parts, deleting), over hyper
//! and rustls, each request signed with SigV4 over its body's SHA-256.
//! Path-style addressing; no proxies, and redirects are not followed.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::client::legacy::connect::HttpConnector;
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_sigv4::{Credential, encode_key, sha256_hex};

/// Where and as whom the client connects.
#[derive(Debug, Clone)]
pub struct Config {
    /// `http://host[:port]` or `https://host[:port]`.
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret: String,
    /// PEM certificates to trust instead of the system's roots.
    pub ca_pem: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The configuration or a request could not be made.
    Invalid(String),
    /// The connection failed, or the answer could not be read.
    Transport(String),
    /// The service refused the request.
    Service {
        status: u16,
        code: String,
        message: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Invalid(why) => write!(f, "invalid request: {why}"),
            Error::Transport(why) => write!(f, "transport error: {why}"),
            Error::Service {
                status,
                code,
                message,
            } => write!(f, "{code} ({status}): {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// An object as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub size: u64,
    pub etag: String,
    /// ISO 8601, as the service lists it.
    pub last_modified: String,
}

/// One page of a listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Page {
    pub objects: Vec<Listed>,
    pub prefixes: Vec<String>,
    /// Where the next page starts, when there is one.
    pub next: Option<String>,
}

/// An object's headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub size: u64,
    pub etag: String,
    /// An HTTP date.
    pub last_modified: String,
    pub content_type: Option<String>,
    /// `x-amz-meta-*`, without the prefix.
    pub metadata: Vec<(String, String)>,
    /// `x-amz-server-side-encryption` (`AES256`, `aws:kms`, ...).
    pub server_side_encryption: Option<String>,
}

/// An object being read: its headers, and its body as it arrives.
pub struct Download {
    pub head: Head,
    pub body: Incoming,
}

impl Download {
    /// The body's next bytes (`None` at its end); a service that sends
    /// nothing for a while fails the read.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, Error> {
        loop {
            let frame = tokio::time::timeout(ANSWER_TIMEOUT, self.body.frame())
                .await
                .map_err(|_| Error::Transport("the body stalled".to_string()))?;
            match frame {
                None => return Ok(None),
                Some(frame) => {
                    if let Ok(data) = frame.map_err(transport)?.into_data() {
                        return Ok(Some(data));
                    }
                }
            }
        }
    }
}

/// The HTTP client every request goes through: TLS by rustls, trusting
/// `ca_pem` instead of the system's roots when given; no proxies.
fn http_client(
    ca_pem: Option<&[u8]>,
) -> Result<HttpClient<HttpsConnector<HttpConnector>, Full<Bytes>>, Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = hyper_rustls::HttpsConnectorBuilder::new();
    let builder = match ca_pem {
        Some(pem) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut &pem[..]) {
                roots
                    .add(cert.map_err(|err| invalid(format!("CA certificate: {err}")))?)
                    .map_err(|err| invalid(format!("CA certificate: {err}")))?;
            }
            if roots.is_empty() {
                return Err(invalid("no certificate in the CA PEM"));
            }
            let tls = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|err| invalid(format!("TLS: {err}")))?
                .with_root_certificates(roots)
                .with_no_client_auth();
            builder.with_tls_config(tls)
        }
        None => builder
            .with_provider_and_native_roots(provider)
            .map_err(|err| invalid(format!("TLS roots: {err}")))?,
    };
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_connect_timeout(Some(CONNECT_TIMEOUT));
    let connector = builder.https_or_http().enable_http1().wrap_connector(http);
    Ok(HttpClient::builder(hyper_util::rt::TokioExecutor::new()).build(connector))
}

/// `url` as a POST target: an `http://` or `https://` URL with a host.
pub fn post_target(url: &str) -> Result<Uri, Error> {
    let uri: Uri = url
        .parse()
        .map_err(|_| invalid(format!("{url} is not a URL")))?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.authority().is_none() {
        return Err(invalid(format!("{url} is not an http:// or https:// URL")));
    }
    Ok(uri)
}

/// A webhook POST's `x-truthdb-signature` value: `t=<unix seconds>,v1=<hex
/// HMAC-SHA256 under the webhook's secret of "<secs>.<delivery id>.<body>">`.
pub fn webhook_signature(secret: &str, secs: u64, delivery: &str, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut context = ring::hmac::Context::with_key(&key);
    context.update(format!("{secs}.{delivery}.").as_bytes());
    context.update(body);
    let digest: String = context
        .sign()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("t={secs},v1={digest}")
}

/// Posts bodies to URLs (webhooks), over the same HTTP stack: no proxies,
/// and redirects are not followed.
pub struct Poster {
    http: HttpClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

impl Poster {
    pub fn new(ca_pem: Option<&[u8]>) -> Result<Poster, Error> {
        Ok(Poster {
            http: http_client(ca_pem)?,
        })
    }

    /// POSTs `body` to `url` (`http://` or `https://`) with `headers`, and
    /// returns the answer's status, whatever it is; fails when the request
    /// cannot be made, the connection fails or no answer comes in time.
    pub async fn post(
        &self,
        url: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<u16, Error> {
        let uri = post_target(url)?;
        let body_len = body.len() as u64;
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-length", body_len);
        for (name, value) in headers {
            request = request.header(*name, value.as_str());
        }
        let request = request
            .body(Full::new(body))
            .map_err(|err| invalid(err.to_string()))?;
        let answer_within =
            ANSWER_TIMEOUT + std::time::Duration::from_secs(body_len / MIN_SEND_RATE);
        let response = tokio::time::timeout(answer_within, self.http.request(request))
            .await
            .map_err(|_| Error::Transport("no answer in time".to_string()))?
            .map_err(transport)?;
        let status = response.status().as_u16();
        // Read (a bounded part of) the answer, so the connection is free.
        let _ = tokio::time::timeout(
            ANSWER_TIMEOUT,
            http_body_util::Limited::new(response.into_body(), MAX_ERROR_BODY).collect(),
        )
        .await;
        Ok(status)
    }

    /// [`Self::post`], returning the answer's body too (at most `max`
    /// bytes: a longer one fails).
    pub async fn post_answer(
        &self,
        url: &str,
        headers: &[(&str, String)],
        body: Bytes,
        max: usize,
    ) -> Result<(u16, Bytes), Error> {
        let uri = post_target(url)?;
        let body_len = body.len() as u64;
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-length", body_len);
        for (name, value) in headers {
            request = request.header(*name, value.as_str());
        }
        let request = request
            .body(Full::new(body))
            .map_err(|err| invalid(err.to_string()))?;
        let answer_within =
            ANSWER_TIMEOUT + std::time::Duration::from_secs(body_len / MIN_SEND_RATE);
        let response = tokio::time::timeout(answer_within, self.http.request(request))
            .await
            .map_err(|_| Error::Transport("no answer in time".to_string()))?
            .map_err(transport)?;
        let status = response.status().as_u16();
        let answer = tokio::time::timeout(
            ANSWER_TIMEOUT,
            http_body_util::Limited::new(response.into_body(), max).collect(),
        )
        .await
        .map_err(|_| Error::Transport("the answer did not come in time".to_string()))?
        .map_err(|err| Error::Transport(format!("the answer could not be read: {err}")))?
        .to_bytes();
        Ok((status, answer))
    }
}

pub struct Client {
    http: HttpClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    /// The endpoint's scheme and authority.
    base: Uri,
    host: String,
    config: Config,
}

fn invalid(why: impl Into<String>) -> Error {
    Error::Invalid(why.into())
}

/// A transport error with its causes (hyper's own message hides them).
fn transport(err: impl std::error::Error) -> Error {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        text.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    Error::Transport(text)
}

/// `YYYYMMDDTHHMMSSZ` for seconds since the epoch (Howard Hinnant's
/// civil-from-days).
fn amz_date(secs: u64) -> String {
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

/// The texts of the elements at `path` (names from the root's children
/// down, `/`-joined) of an XML document, in order.
fn texts(xml: &[u8], path: &str) -> Result<Vec<String>, Error> {
    let mut reader = Reader::from_reader(xml);
    let mut open: Vec<String> = Vec::new();
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(transport)? {
            Event::Start(element) => {
                open.push(String::from_utf8_lossy(element.local_name().as_ref()).into_owned());
                if open.len() > 1 && open[1..].join("/") == path {
                    current = Some(String::new());
                }
            }
            Event::Empty(element) => {
                open.push(String::from_utf8_lossy(element.local_name().as_ref()).into_owned());
                if open.len() > 1 && open[1..].join("/") == path {
                    out.push(String::new());
                }
                open.pop();
            }
            Event::Text(text) => {
                if let Some(current) = &mut current {
                    current.push_str(&text.unescape().map_err(transport)?);
                }
            }
            Event::CData(text) => {
                if let Some(current) = &mut current {
                    current.push_str(std::str::from_utf8(&text).map_err(transport)?);
                }
            }
            Event::End(_) => {
                if open.len() > 1
                    && open[1..].join("/") == path
                    && let Some(text) = current.take()
                {
                    out.push(text);
                }
                open.pop();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

/// The listing entries of a ListObjectsV2 answer, field by field.
fn listed(xml: &[u8]) -> Result<Vec<Listed>, Error> {
    let keys = texts(xml, "Contents/Key")?;
    let sizes = texts(xml, "Contents/Size")?;
    let etags = texts(xml, "Contents/ETag")?;
    let dates = texts(xml, "Contents/LastModified")?;
    if [sizes.len(), etags.len(), dates.len()] != [keys.len(); 3] {
        return Err(Error::Transport(
            "a listing entry lacks a field".to_string(),
        ));
    }
    keys.into_iter()
        .zip(sizes)
        .zip(etags.into_iter().zip(dates))
        .map(|((key, size), (etag, last_modified))| {
            Ok(Listed {
                key,
                size: size
                    .parse()
                    .map_err(|_| Error::Transport(format!("size {size} is not a number")))?,
                etag,
                last_modified,
            })
        })
        .collect()
}

/// What a client may send as a bucket name: S3's characters, so the path
/// signed is the path sent.
fn check_bucket(bucket: &str) -> Result<(), Error> {
    if bucket.is_empty()
        || !bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
    {
        return Err(invalid(format!("{bucket:?} is not a bucket name")));
    }
    Ok(())
}

/// The `Host` a request to `base` carries, as hyper writes it: the port
/// only when it is not the scheme's own.
fn host_of(base: &Uri) -> Result<String, Error> {
    let authority = base
        .authority()
        .ok_or_else(|| invalid("the endpoint has no host"))?;
    if authority.as_str().contains('@') {
        return Err(invalid("the endpoint must not name a user"));
    }
    let default = match base.scheme_str() {
        Some("https") => 443,
        _ => 80,
    };
    Ok(match authority.port_u16() {
        Some(port) if port != default => format!("{}:{port}", authority.host()),
        _ => authority.host().to_string(),
    })
}

/// A ListObjectsV2 answer as a page.
fn page_of(body: &[u8]) -> Result<Page, Error> {
    let truncated = texts(body, "IsTruncated")?.first().map(String::as_str) == Some("true");
    let next = texts(body, "NextContinuationToken")?.into_iter().next();
    if truncated && next.is_none() {
        // Ending here would leave the rest of the listing out unnoticed.
        return Err(Error::Transport(
            "a truncated listing has no continuation token".to_string(),
        ));
    }
    Ok(Page {
        objects: listed(body)?,
        prefixes: texts(body, "CommonPrefixes/Prefix")?,
        next: if truncated { next } else { None },
    })
}

/// A CompleteMultipartUpload answer's ETag; S3 may report a failure in a
/// 200 answer.
fn completed(body: &[u8]) -> Result<String, Error> {
    if let Some(code) = texts(body, "Code")?.into_iter().next()
        && texts(body, "ETag")?.is_empty()
    {
        return Err(Error::Service {
            status: 200,
            code,
            message: texts(body, "Message")?
                .into_iter()
                .next()
                .unwrap_or_default(),
        });
    }
    texts(body, "ETag")?
        .into_iter()
        .next()
        .ok_or_else(|| Error::Transport("no ETag in the answer".to_string()))
}

/// How long connecting may take.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// How long the service may take to answer (its headers), and to send the
/// next piece of a body: a service that stalls fails the request rather
/// than holding its caller forever.
const ANSWER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The slowest sending a request body is allowed, in bytes a second.
const MIN_SEND_RATE: u64 = 32 * 1024;

/// The most of an error answer's body that is read.
const MAX_ERROR_BODY: usize = 64 * 1024;

/// `value` encoded for a query string.
fn query_value(value: &str) -> String {
    encode_key(value).replace('/', "%2F")
}

impl Client {
    pub fn new(config: Config) -> Result<Client, Error> {
        let base: Uri = config
            .endpoint
            .parse()
            .map_err(|_| invalid(format!("{} is not a URL", config.endpoint)))?;
        let (Some(scheme), Some(_)) = (base.scheme_str(), base.authority()) else {
            return Err(invalid(
                "the endpoint must be http:// or https://host[:port]",
            ));
        };
        if !matches!(scheme, "http" | "https") || base.path().len() > 1 {
            return Err(invalid(
                "the endpoint must be http:// or https://host[:port]",
            ));
        }
        let host = host_of(&base)?;
        let http = http_client(config.ca_pem.as_deref())?;
        Ok(Client {
            http,
            base,
            host,
            config,
        })
    }

    /// Sends a signed request; `path` and `query` are already encoded.
    /// Answers outside 2xx come back as [`Error::Service`].
    async fn send(
        &self,
        method: Method,
        path: &str,
        query: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<Response<Incoming>, Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| invalid("the clock is before 1970"))?
            .as_secs();
        let amz_date = amz_date(now);
        let payload_hash = sha256_hex(&body);
        let mut signed: Vec<(String, String)> = vec![
            ("host".to_string(), self.host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        signed.extend(
            headers
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), value.clone())),
        );
        let credential = Credential {
            access_key: self.config.access_key.clone(),
            date: amz_date[..8].to_string(),
            region: self.config.region.clone(),
            service: "s3".to_string(),
        };
        let authorization = truthdb_sigv4::authorization(
            &credential,
            &self.config.secret,
            &amz_date,
            method.as_str(),
            path,
            query,
            &signed,
            &payload_hash,
        )
        .map_err(|truthdb_sigv4::AuthError::Malformed(why)| invalid(why))?;
        let target = if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        };
        let uri = format!(
            "{}://{}{target}",
            self.base.scheme_str().unwrap_or("http"),
            self.host
        );
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in &signed {
            if name != "host" {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        let body_len = body.len() as u64;
        // Stated even when empty: hyper leaves it out of an empty PUT.
        let request = request
            .header("authorization", authorization)
            .header("content-length", body_len)
            .body(Full::new(body))
            .map_err(|err| invalid(err.to_string()))?;
        // The answer follows the body: a large one takes longer to send.
        let answer_within =
            ANSWER_TIMEOUT + std::time::Duration::from_secs(body_len / MIN_SEND_RATE);
        let response = tokio::time::timeout(answer_within, self.http.request(request))
            .await
            .map_err(|_| Error::Transport("the service did not answer in time".to_string()))?
            .map_err(transport)?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let body = tokio::time::timeout(
            ANSWER_TIMEOUT,
            http_body_util::Limited::new(response.into_body(), MAX_ERROR_BODY).collect(),
        )
        .await
        .map_err(|_| Error::Transport("the body stalled".to_string()))?
        .map_err(|err| Error::Transport(err.to_string()))?
        .to_bytes();
        // A HEAD answer has no body to name the error.
        let code = texts(&body, "Code")
            .ok()
            .and_then(|codes| codes.into_iter().next())
            .unwrap_or_else(|| format!("HTTP{status}"));
        let message = texts(&body, "Message")
            .ok()
            .and_then(|messages| messages.into_iter().next())
            .unwrap_or_default();
        Err(Error::Service {
            status,
            code,
            message,
        })
    }

    /// [`Self::send`], reading the whole answer body.
    async fn call(
        &self,
        method: Method,
        path: &str,
        query: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<(hyper::HeaderMap, Bytes), Error> {
        let response = self.send(method, path, query, headers, body).await?;
        let headers = response.headers().clone();
        // Answers read whole are small (listings, XML results).
        let body = tokio::time::timeout(ANSWER_TIMEOUT, response.into_body().collect())
            .await
            .map_err(|_| Error::Transport("the body stalled".to_string()))?
            .map_err(transport)?
            .to_bytes();
        Ok((headers, body))
    }

    /// An object's path; an empty key would name the bucket itself.
    fn object_path(bucket: &str, key: &str) -> Result<String, Error> {
        check_bucket(bucket)?;
        if key.is_empty() {
            return Err(invalid("an object key must not be empty"));
        }
        Ok(format!("/{bucket}/{}", encode_key(key)))
    }

    fn bucket_path(bucket: &str) -> Result<String, Error> {
        check_bucket(bucket)?;
        Ok(format!("/{bucket}"))
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Error> {
        let (_, body) = self.call(Method::GET, "/", "", &[], Bytes::new()).await?;
        texts(&body, "Buckets/Bucket/Name")
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<(), Error> {
        self.call(
            Method::PUT,
            &Self::bucket_path(bucket)?,
            "",
            &[],
            Bytes::new(),
        )
        .await
        .map(|_| ())
    }

    /// One page of the objects whose keys start with `prefix` (with
    /// `delimiter`, keys sharing a next segment come back as prefixes),
    /// from where `next` of the page before said.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        next: Option<&str>,
    ) -> Result<Page, Error> {
        self.list_page(bucket, prefix, delimiter, next, None).await
    }

    /// One page of the objects whose keys start with `prefix`, from the
    /// first key after `after` (a place that outlives a continuation token).
    pub async fn list_objects_after(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
    ) -> Result<Page, Error> {
        self.list_page(
            bucket,
            prefix,
            None,
            None,
            Some(after).filter(|key| !key.is_empty()),
        )
        .await
    }

    async fn list_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        next: Option<&str>,
        start_after: Option<&str>,
    ) -> Result<Page, Error> {
        let mut query = vec!["list-type=2".to_string()];
        if let Some(token) = next {
            query.push(format!("continuation-token={}", query_value(token)));
        }
        if let Some(delimiter) = delimiter {
            query.push(format!("delimiter={}", query_value(delimiter)));
        }
        if !prefix.is_empty() {
            query.push(format!("prefix={}", query_value(prefix)));
        }
        if let Some(after) = start_after {
            query.push(format!("start-after={}", query_value(after)));
        }
        let (_, body) = self
            .call(
                Method::GET,
                &Self::bucket_path(bucket)?,
                &query.join("&"),
                &[],
                Bytes::new(),
            )
            .await?;
        page_of(&body)
    }

    /// The object's headers; `None` when there is no such object (or no such
    /// bucket: a HEAD answer has no body to tell them apart).
    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<Option<Head>, Error> {
        match self
            .call(
                Method::HEAD,
                &Self::object_path(bucket, key)?,
                "",
                &[],
                Bytes::new(),
            )
            .await
        {
            Ok((headers, _)) => Ok(Some(head_of(&headers)?)),
            Err(Error::Service { status: 404, .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Download, Error> {
        let response = self
            .send(
                Method::GET,
                &Self::object_path(bucket, key)?,
                "",
                &[],
                Bytes::new(),
            )
            .await?;
        Ok(Download {
            head: head_of(response.headers())?,
            body: response.into_body(),
        })
    }

    /// Stores `body` as the object, with `headers` (content type, metadata,
    /// ...); returns its ETag.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> Result<String, Error> {
        let (headers, _) = self
            .call(
                Method::PUT,
                &Self::object_path(bucket, key)?,
                "",
                headers,
                body,
            )
            .await?;
        etag_of(&headers)
    }

    /// Starts a multipart upload; returns its id.
    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        headers: &[(&str, String)],
    ) -> Result<String, Error> {
        let (_, body) = self
            .call(
                Method::POST,
                &Self::object_path(bucket, key)?,
                "uploads=",
                headers,
                Bytes::new(),
            )
            .await?;
        texts(&body, "UploadId")?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Transport("no UploadId in the answer".to_string()))
    }

    /// Uploads part `number`; returns its ETag.
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload: &str,
        number: u32,
        body: Bytes,
    ) -> Result<String, Error> {
        let (headers, _) = self
            .call(
                Method::PUT,
                &Self::object_path(bucket, key)?,
                &format!("partNumber={number}&uploadId={}", query_value(upload)),
                &[],
                body,
            )
            .await?;
        etag_of(&headers)
    }

    /// Completes the upload from `parts` (number, ETag), in order; returns
    /// the object's ETag.
    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload: &str,
        parts: &[(u32, String)],
    ) -> Result<String, Error> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
                etag.replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('"', "&quot;")
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let (_, body) = self
            .call(
                Method::POST,
                &Self::object_path(bucket, key)?,
                &format!("uploadId={}", query_value(upload)),
                &[],
                Bytes::from(xml),
            )
            .await?;
        completed(&body)
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload: &str,
    ) -> Result<(), Error> {
        self.call(
            Method::DELETE,
            &Self::object_path(bucket, key)?,
            &format!("uploadId={}", query_value(upload)),
            &[],
            Bytes::new(),
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), Error> {
        self.call(
            Method::DELETE,
            &Self::object_path(bucket, key)?,
            "",
            &[],
            Bytes::new(),
        )
        .await
        .map(|_| ())
    }
}

fn header(headers: &hyper::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn etag_of(headers: &hyper::HeaderMap) -> Result<String, Error> {
    header(headers, "etag").ok_or_else(|| Error::Transport("no ETag in the answer".to_string()))
}

fn head_of(headers: &hyper::HeaderMap) -> Result<Head, Error> {
    Ok(Head {
        size: header(headers, "content-length")
            .and_then(|length| length.parse().ok())
            .ok_or_else(|| Error::Transport("no Content-Length in the answer".to_string()))?,
        etag: etag_of(headers)?,
        last_modified: header(headers, "last-modified").unwrap_or_default(),
        content_type: header(headers, "content-type"),
        server_side_encryption: header(headers, "x-amz-server-side-encryption"),
        metadata: headers
            .iter()
            .filter_map(|(name, value)| {
                Some((
                    name.as_str().strip_prefix("x-amz-meta-")?.to_string(),
                    value.to_str().ok()?.to_string(),
                ))
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_are_written_as_sigv4_wants_them() {
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
        assert_eq!(amz_date(951_782_400 + 3_661), "20000229T010101Z");
    }

    #[test]
    fn answers_read_as_the_service_writes_them() {
        let xml = b"<?xml version=\"1.0\"?><ListBucketResult xmlns=\"x\"><IsTruncated>true\
            </IsTruncated><Contents><Key>a&amp;b</Key><LastModified>2026-01-01T00:00:00.000Z\
            </LastModified><ETag>&quot;e&quot;</ETag><Size>3</Size></Contents>\
            <CommonPrefixes><Prefix>d/</Prefix></CommonPrefixes>\
            <NextContinuationToken>t</NextContinuationToken></ListBucketResult>";
        assert_eq!(
            listed(xml).unwrap(),
            [Listed {
                key: "a&b".to_string(),
                size: 3,
                etag: "\"e\"".to_string(),
                last_modified: "2026-01-01T00:00:00.000Z".to_string(),
            }]
        );
        assert_eq!(texts(xml, "CommonPrefixes/Prefix").unwrap(), ["d/"]);
        assert_eq!(texts(xml, "NextContinuationToken").unwrap(), ["t"]);
        assert!(listed(b"<R><Contents><Key>k</Key></Contents></R>").is_err());
    }

    #[test]
    fn pages_and_completions_read_as_s3_writes_them() {
        let page = page_of(
            b"<R><IsTruncated>true</IsTruncated><NextContinuationToken>t+1</NextContinuationToken>\
              <Contents><Key><![CDATA[c&d]]></Key><LastModified>x</LastModified>\
              <ETag>e</ETag><Size>1</Size></Contents></R>",
        )
        .unwrap();
        assert_eq!(page.next.as_deref(), Some("t+1"));
        assert_eq!(page.objects[0].key, "c&d");
        assert!(page_of(b"<R><IsTruncated>true</IsTruncated></R>").is_err());
        assert_eq!(
            page_of(b"<R><IsTruncated>false</IsTruncated></R>").unwrap(),
            Page::default()
        );
        assert_eq!(
            completed(
                b"<CompleteMultipartUploadResult><ETag>\"e-2\"</ETag>\
                  </CompleteMultipartUploadResult>"
            )
            .unwrap(),
            "\"e-2\""
        );
        assert_eq!(
            completed(b"<Error><Code>InternalError</Code><Message>m</Message></Error>"),
            Err(Error::Service {
                status: 200,
                code: "InternalError".to_string(),
                message: "m".to_string(),
            })
        );
        assert_eq!(query_value("a/b c+d"), "a%2Fb%20c%2Bd");
    }

    #[test]
    fn the_host_signed_is_the_host_hyper_sends() {
        let host = |endpoint: &str| host_of(&endpoint.parse().unwrap());
        assert_eq!(host("https://s3.example:443").unwrap(), "s3.example");
        assert_eq!(host("http://s3.example:80").unwrap(), "s3.example");
        assert_eq!(host("https://s3.example:80").unwrap(), "s3.example:80");
        assert_eq!(host("http://[::1]:9000").unwrap(), "[::1]:9000");
        assert!(host("https://user@s3.example").is_err());
    }

    #[test]
    fn a_bucket_or_key_that_would_name_something_else_is_refused() {
        assert!(Client::object_path("b", "").is_err());
        assert!(Client::object_path("b?x", "k").is_err());
        assert!(Client::bucket_path("").is_err());
        assert!(Client::bucket_path("UPPER").is_err());
        assert_eq!(Client::object_path("b.1-x", "k").unwrap(), "/b.1-x/k");
    }

    #[test]
    fn an_endpoint_is_a_scheme_and_a_host() {
        let config = |endpoint: &str| Config {
            endpoint: endpoint.to_string(),
            region: "us-east-1".to_string(),
            access_key: "a".to_string(),
            secret: "s".to_string(),
            ca_pem: None,
        };
        for bad in ["127.0.0.1:9000", "ftp://h", "http://h/bucket", "nonsense"] {
            assert!(Client::new(config(bad)).is_err(), "{bad}");
        }
    }
}
