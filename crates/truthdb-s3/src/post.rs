//! Browser form uploads (S3's POST Object, the SDKs' presigned POST): a
//! `multipart/form-data` POST to a bucket whose fields name the key and its
//! metadata and whose `file` part is the object.
//!
//! A signed form carries `x-amz-algorithm`, `x-amz-credential`,
//! `x-amz-date`, a base64 `policy` document and `x-amz-signature` (the
//! policy signed with the SigV4 signing key). The policy expires, and its
//! conditions bound the form: exact values (`{"field": "value"}`, `["eq",
//! "$field", "value"]`), prefixes (`["starts-with", "$field", "prefix"]`) and
//! the file's size (`["content-length-range", min, max]`); every field but
//! `x-amz-signature`, `file`, `policy` and `x-ignore-*` must be covered by
//! one. A form without a signature is anonymous: only a bucket policy can
//! let it in. Either way the upload is authorized as `s3:PutObject`, with
//! `s3:authType` `POST`.
//!
//! Fields before the file are read whole (at most 20 KiB); the file is
//! streamed into the object; fields after it are ignored.

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::{Request, Response, StatusCode};
use serde_json::Value;
use truthdb_object::{ObjectError, VersionSel};

use crate::authz::{Authz, Resource};
use crate::error::{S3Error, escape};
use crate::objects::{BodySink, MAX_PUT_BYTES, object, object_error, version_header};
use crate::server::{Body, Connection, Principal, S3State, Signed, full};

/// The most bytes of fields before the file.
const MAX_FIELDS: usize = 20 * 1024;

/// What a key keeps unencoded in the response's Location.
const LOCATION: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// Whether a request is a form upload: a POST of `multipart/form-data`.
pub fn is_form(request: &Request<Incoming>) -> bool {
    request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("multipart/form-data")
        })
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

fn denied(why: impl std::fmt::Display) -> S3Error {
    S3Error::access_denied(format!("Invalid according to Policy: {why}"))
}

/// The `multipart/form-data` body as it arrives.
struct Form {
    body: Incoming,
    /// Bytes read and not yet consumed.
    buf: Vec<u8>,
    /// `\r\n--` and the boundary: what ends a part.
    delimiter: Vec<u8>,
}

impl Form {
    /// Reads one more data frame into the buffer; false at the end.
    async fn fill(&mut self) -> Result<bool, S3Error> {
        while let Some(frame) = self.body.frame().await {
            let frame = frame.map_err(|err| S3Error::invalid_request(err.to_string()))?;
            if let Ok(data) = frame.into_data() {
                self.buf.extend_from_slice(&data);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Where the buffer's first delimiter starts: one followed by CRLF or
    /// `--` (the boundary followed by anything else is data). `Err(n)`: none
    /// is complete yet, and the first `n` bytes cannot be part of one.
    /// Bytes before `start` are known not to begin one.
    fn delimiter(&self, start: usize) -> Result<usize, usize> {
        let mut from = start;
        while let Some(found) = self.buf[from..]
            .windows(self.delimiter.len())
            .position(|w| w == self.delimiter.as_slice())
        {
            let at = from + found;
            let after = &self.buf[at + self.delimiter.len()..];
            if after.starts_with(b"\r\n") || after.starts_with(b"--") {
                return Ok(at);
            }
            if after.is_empty() || after == b"\r" || after == b"-" {
                return Err(at);
            }
            from = at + 1;
        }
        Err(from.max((self.buf.len() + 1).saturating_sub(self.delimiter.len())))
    }

    /// The position of the next delimiter, reading until it is there
    /// (`None` past `limit` bytes or at the end).
    async fn find_delimiter(&mut self, limit: usize) -> Result<Option<usize>, S3Error> {
        // Each read rescans only what could begin a delimiter.
        let mut start = 0;
        loop {
            match self.delimiter(start) {
                Ok(at) => return Ok(Some(at)),
                Err(clear) => start = clear,
            }
            if self.buf.len() > limit || !self.fill().await? {
                return Ok(None);
            }
        }
    }

    /// The position of `needle` in the buffer, reading until it is there
    /// (`None` past `limit` bytes or at the end).
    async fn find(&mut self, needle: &[u8], limit: usize) -> Result<Option<usize>, S3Error> {
        let mut start = 0;
        loop {
            if let Some(at) = self.buf[start..]
                .windows(needle.len())
                .position(|w| w == needle)
            {
                return Ok(Some(start + at));
            }
            start = (self.buf.len() + 1).saturating_sub(needle.len());
            if self.buf.len() > limit || !self.fill().await? {
                return Ok(None);
            }
        }
    }
}

/// A part's `Content-Disposition` name and file name.
fn disposition(headers: &str) -> Option<(String, Option<String>)> {
    let line = headers.lines().find(|line| {
        line.to_ascii_lowercase()
            .starts_with("content-disposition:")
    })?;
    // `; key=value` parameters, a quoted value holding `;`. Browsers send
    // a `"` in a file name as `%22` and a `\` as itself: it escapes nothing.
    let mut params: Vec<(String, String)> = Vec::new();
    let mut rest = line.split_once(';')?.1;
    while let Some((key, after)) = rest.split_once('=') {
        let key = key.trim().to_ascii_lowercase();
        let after = after.trim_start();
        let value;
        if let Some(quoted) = after.strip_prefix('"') {
            let (text, after) = quoted.split_once('"').unwrap_or((quoted, ""));
            value = text.to_string();
            rest = after.split_once(';').map_or("", |(_, rest)| rest);
        } else {
            let (text, after) = after.split_once(';').unwrap_or((after, ""));
            value = text.trim().to_string();
            rest = after;
        }
        params.push((key, value));
    }
    let param = |name: &str| {
        params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    };
    Some((param("name")?, param("filename")))
}

/// The form's fields before the file, and the file's name; the buffer is
/// left at the file's first byte.
async fn read_fields(form: &mut Form) -> Result<(Vec<(String, String)>, String), S3Error> {
    let too_large = || {
        S3Error::new(
            400,
            "MaxPostPreDataLengthExceeded",
            "Your POST request fields preceding the upload file were too large.",
        )
    };
    // The body starts with the boundary line, not a delimiter: lead with
    // the delimiter's CRLF so that every part is found the same way.
    form.buf.extend_from_slice(b"\r\n");
    let delimiter = form.delimiter.clone();
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut pending: Option<String> = None;
    let mut read = 0;
    loop {
        let at = form
            .find_delimiter(MAX_FIELDS.saturating_sub(read))
            .await?
            .ok_or_else(too_large)?;
        read += at + delimiter.len();
        if read > MAX_FIELDS {
            return Err(too_large());
        }
        // The previous part's value is everything before this delimiter.
        if let Some(name) = pending.take() {
            fields.push((name, String::from_utf8_lossy(&form.buf[..at]).into_owned()));
        }
        form.buf.drain(..at + delimiter.len());
        // `--` after the boundary closes the form.
        if form.buf.starts_with(b"--") {
            return Err(invalid("The form has no file field."));
        }
        let headers_end = form
            .find(b"\r\n\r\n", MAX_FIELDS.saturating_sub(read))
            .await?
            .ok_or_else(too_large)?;
        read += headers_end + 4;
        let headers = String::from_utf8_lossy(&form.buf[..headers_end]).into_owned();
        form.buf.drain(..headers_end + 4);
        let (name, filename) =
            disposition(&headers).ok_or_else(|| invalid("A form part has no name."))?;
        if name.eq_ignore_ascii_case("file") {
            return Ok((fields, filename.unwrap_or_default()));
        }
        // Each field is read once, so that the policy checks the value used.
        if field(&fields, &name).is_some() {
            return Err(invalid(format!("The form has more than one {name} field.")));
        }
        pending = Some(name);
    }
}

/// A form field's value (names compare without case).
fn field<'a>(fields: &'a [(String, String)], name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// Checks the policy document against the form: unexpired, every condition
/// met, every field covered. Returns the file's size bounds.
fn check_policy(
    document: &[u8],
    fields: &[(String, String)],
    bucket: &str,
    key: &str,
) -> Result<(u64, u64), S3Error> {
    let malformed = |why: &str| S3Error::new(400, "InvalidPolicyDocument", why.to_string());
    let policy: Value =
        serde_json::from_slice(document).map_err(|_| malformed("The policy is not JSON."))?;
    let expiration = policy
        .get("expiration")
        .and_then(Value::as_str)
        .and_then(truthdb_iam::parse_date_ms)
        .ok_or_else(|| malformed("The policy has no valid expiration."))?;
    if expiration < crate::objects::now_ms() as i64 {
        return Err(denied("Policy expired."));
    }
    let conditions = policy
        .get("conditions")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("The policy has no conditions."))?;
    let value_of = |name: &str| -> String {
        match name.to_ascii_lowercase().as_str() {
            "bucket" => bucket.to_string(),
            "key" => key.to_string(),
            _ => field(fields, name).unwrap_or("").to_string(),
        }
    };
    let (mut min, mut max) = (0, MAX_PUT_BYTES);
    let mut covered: Vec<String> = Vec::new();
    for condition in conditions {
        match condition {
            Value::Object(map) if map.len() == 1 => {
                let (name, expected) = map.iter().next().expect("one entry");
                let expected = expected
                    .as_str()
                    .ok_or_else(|| malformed("A condition value is not a string."))?;
                if value_of(name) != expected {
                    return Err(denied(format!(
                        "Policy Condition failed: [\"eq\", \"${name}\", \"{expected}\"]"
                    )));
                }
                covered.push(name.to_ascii_lowercase());
            }
            Value::Array(items) => match items.as_slice() {
                [
                    Value::String(op),
                    Value::String(name),
                    Value::String(expected),
                ] if name.starts_with('$') => {
                    let name = &name[1..];
                    let actual = value_of(name);
                    let holds = match op.to_ascii_lowercase().as_str() {
                        "eq" => actual == *expected,
                        "starts-with" => actual.starts_with(expected.as_str()),
                        _ => return Err(malformed("An unknown condition operator.")),
                    };
                    if !holds {
                        return Err(denied(format!(
                            "Policy Condition failed: [\"{op}\", \"${name}\", \"{expected}\"]"
                        )));
                    }
                    covered.push(name.to_ascii_lowercase());
                }
                [Value::String(op), low, high]
                    if op.eq_ignore_ascii_case("content-length-range") =>
                {
                    let number = |value: &Value| {
                        value
                            .as_u64()
                            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                    };
                    match (number(low), number(high)) {
                        (Some(low), Some(high)) if low <= high => {
                            min = min.max(low);
                            max = max.min(high);
                        }
                        _ => return Err(malformed("A content-length-range is not two numbers.")),
                    }
                }
                _ => return Err(malformed("A condition is not understood.")),
            },
            _ => return Err(malformed("A condition is not understood.")),
        }
    }
    for (name, _) in fields {
        let name = name.to_ascii_lowercase();
        if ["x-amz-signature", "policy", "file"].contains(&name.as_str())
            || name.starts_with("x-ignore-")
            || covered.contains(&name)
        {
            continue;
        }
        return Err(denied(format!("Extra input fields: {name}")));
    }
    Ok((min, max))
}

/// Checks a signed form's signature: the base64 policy signed with the
/// SigV4 signing key of `x-amz-credential`. Returns who signed it and when.
async fn verify(
    state: &S3State,
    fields: &[(String, String)],
    policy: &str,
) -> Result<(Principal, u64), S3Error> {
    let required = |name: &str| {
        field(fields, name)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid(format!("Bucket POST must contain a field named '{name}'.")))
    };
    if required("x-amz-algorithm")? != crate::sigv4::ALGORITHM {
        return Err(invalid("x-amz-algorithm must be AWS4-HMAC-SHA256."));
    }
    let credential = required("x-amz-credential")?;
    let amz_date = required("x-amz-date")?;
    let signature = required("x-amz-signature")?;
    let parts: Vec<&str> = credential.split('/').collect();
    let [access_key, date, region, "s3", "aws4_request"] = parts.as_slice() else {
        return Err(invalid(
            "x-amz-credential is not KEY/date/region/s3/aws4_request.",
        ));
    };
    let signed_at = crate::server::parse_amz_date(amz_date)
        .filter(|_| amz_date.starts_with(date) && *region == state.region)
        .ok_or_else(|| invalid("x-amz-date or the credential scope is not valid."))?;
    let key_id = access_key.to_string();
    let (key, secret) = state
        .engine
        .run_object(move |file| file.access_key(&key_id))
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .filter(|(key, _)| key.active)
        .ok_or_else(S3Error::invalid_access_key)?;
    let credential = crate::sigv4::Credential {
        access_key: access_key.to_string(),
        date: date.to_string(),
        region: region.to_string(),
        service: "s3".to_string(),
    };
    if !crate::sigv4::verify(
        &state.signing_keys.signing_key(&secret, &credential),
        policy,
        signature,
    ) {
        return Err(S3Error::signature_mismatch());
    }
    Ok((
        Principal {
            access_key: key.id,
            account: key.account,
            user: key.user,
        },
        signed_at * 1000,
    ))
}

/// The upload's system headers and user metadata, from its fields.
fn metadata(fields: &[(String, String)]) -> Result<truthdb_object::ObjectAttrs, S3Error> {
    let mut headers = HeaderMap::new();
    for (name, value) in fields {
        let lower = name.to_ascii_lowercase();
        let wanted = [
            "content-type",
            "cache-control",
            "content-disposition",
            "content-encoding",
            "content-language",
            "expires",
        ]
        .contains(&lower.as_str())
            || lower.starts_with("x-amz-meta-");
        if !wanted {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(lower.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            return Err(invalid(format!(
                "The {lower} field is not a valid header value."
            )));
        };
        headers.insert(name, value);
    }
    crate::objects::metadata_from(&headers)
}

/// POST Object: a form upload to `bucket`.
pub async fn post_object(
    state: &S3State,
    request: Request<Incoming>,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    let connection = request.extensions().get::<Connection>().copied();
    // The policy context sees only what the form acts on: the client's
    // identification here, the acl and encryption fields below (other
    // headers, such as a copy source, would satisfy conditions the upload
    // never honours).
    let mut headers = HeaderMap::new();
    for name in [hyper::header::USER_AGENT, hyper::header::REFERER] {
        if let Some(value) = request.headers().get(&name) {
            headers.insert(name, value.clone());
        }
    }
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let boundary = content_type
        .split(';')
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            key.eq_ignore_ascii_case("boundary")
                .then(|| value.trim().trim_matches('"').to_string())
        })
        // RFC 2046: 1 to 70 characters.
        .filter(|boundary| (1..=70).contains(&boundary.len()))
        .ok_or_else(|| invalid("The form has no valid multipart boundary."))?;
    let mut form = Form {
        body: request.into_body(),
        buf: Vec::new(),
        delimiter: [b"\r\n--".as_slice(), boundary.as_bytes()].concat(),
    };
    let (fields, filename) = read_fields(&mut form).await?;
    let key = field(&fields, "key")
        .filter(|key| !key.is_empty())
        .ok_or_else(|| invalid("Bucket POST must contain a field named 'key'."))?
        .replace("${filename}", &filename);

    // Who uploads: a signed form's signer, under its policy; or anyone.
    let signed =
        field(&fields, "x-amz-signature").is_some() || field(&fields, "x-amz-credential").is_some();
    let (principal, bounds) = if signed {
        let policy = field(&fields, "policy")
            .ok_or_else(|| invalid("Bucket POST must contain a field named 'policy'."))?
            .to_string();
        let (principal, at_ms) = verify(state, &fields, &policy).await?;
        use base64::Engine as _;
        let document = base64::engine::general_purpose::STANDARD
            .decode(policy.trim())
            .map_err(|_| S3Error::new(400, "InvalidPolicyDocument", "The policy is not base64."))?;
        let bounds = check_policy(&document, &fields, &bucket, &key)?;
        (Some((principal, at_ms)), bounds)
    } else {
        (None, (0, MAX_PUT_BYTES))
    };
    if let Some(acl) = field(&fields, "acl")
        && acl != "private"
        && acl != "bucket-owner-full-control"
    {
        return Err(S3Error::new(
            400,
            "AccessControlListNotSupported",
            "The bucket does not allow ACLs",
        ));
    }
    if let Some(acl) = field(&fields, "acl") {
        headers.insert(
            "x-amz-acl",
            HeaderValue::from_str(acl).expect("a checked acl"),
        );
    }
    crate::sse::refuse_customer_keys(fields.iter().map(|(name, _)| name.as_str()))?;
    // The encryption it asks for, which policies see as
    // s3:x-amz-server-side-encryption.
    let encrypted_at_rest = state.storage.is_encrypted();
    if let Some(value) =
        crate::sse::check_write_fields(|name| field(&fields, name), encrypted_at_rest)?
    {
        headers.insert(
            crate::sse::SSE_HEADER,
            HeaderValue::from_str(value).expect("a checked encryption"),
        );
    }
    let tags = match field(&fields, "tagging") {
        Some(xml) => Some(crate::tagging::parse_tagging(xml.as_bytes())?),
        None => None,
    };
    let mut attrs = metadata(&fields)?;
    attrs.tags = tags.clone().unwrap_or_default();

    let authz = Authz::load(
        state,
        principal.as_ref().map(|(principal, _)| principal),
        principal.as_ref().map(|(_, at_ms)| Signed {
            auth_type: "POST",
            at_ms: Some(*at_ms),
        }),
        &headers,
        "",
        connection,
        vec![bucket.clone()],
        Vec::new(),
    )
    .await?;
    authz.check_storing(
        "s3:PutObject",
        Resource::Object(&bucket, &key),
        tags.as_deref(),
    )?;
    let scope = crate::scope::Scope {
        key: principal
            .as_ref()
            .map(|(principal, _)| (principal.access_key.clone(), principal.account.clone())),
        buckets: authz.owners(),
    };
    let redirect = field(&fields, "success_action_redirect").map(str::to_string);
    // Refused before the upload, not after it is stored.
    if let Some(redirect) = &redirect
        && HeaderValue::from_str(redirect).is_err()
    {
        return Err(invalid("success_action_redirect is not a valid URL."));
    }
    let status = field(&fields, "success_action_status").map(str::to_string);
    crate::scope::within(scope, async move {
        let (min, max) = bounds;
        // Fail before reading the file when the bucket or key is wrong.
        let (lookup_bucket, lookup_key) = (bucket.clone(), key.clone());
        let check = object(state, move |file| {
            file.head_object(&lookup_bucket, &lookup_key, VersionSel::Latest)
        })
        .await?;
        if let Err(err @ (ObjectError::NoSuchBucket | ObjectError::InvalidKey(_))) = check {
            return Err(object_error(err, &bucket, &key));
        }
        let mut sink = BodySink::begin(state).await?;
        loop {
            match form.delimiter(0) {
                Ok(end) => {
                    let rest = form.buf[..end].to_vec();
                    push_bounded(state, &mut sink, &rest, max).await?;
                    break;
                }
                Err(ready) => {
                    // Keep what could be the start of the delimiter.
                    let ready: Vec<u8> = form.buf.drain(..ready).collect();
                    push_bounded(state, &mut sink, &ready, max).await?;
                    if !form.fill().await? {
                        return Err(invalid("The form ends inside the file."));
                    }
                }
            }
        }
        if sink.pushed() < min {
            return Err(S3Error::new(
                400,
                "EntityTooSmall",
                "Your proposed upload is smaller than the minimum allowed size",
            ));
        }
        let sealed = sink.finish(state).await?;
        let (bucket_name, key_name) = (bucket.clone(), key.clone());
        let (stored, versioning) = object(state, move |file| {
            let stored = file.transaction(|tx| {
                tx.put_object(
                    &bucket_name,
                    &key_name,
                    &sealed,
                    &attrs,
                    crate::objects::now_ms(),
                    crate::objects::DROP_GRACE_MS,
                )
            })?;
            Ok((stored, crate::objects::versioning_of(file, &bucket_name)?))
        })
        .await?;
        let info = stored.map_err(|err| object_error(err, &bucket, &key))?;
        let etag = info.etag();
        let location = format!(
            "/{bucket}/{}",
            percent_encoding::utf8_percent_encode(&key, LOCATION)
        );
        let mut response =
            crate::sse::with_header(Response::builder().header("etag", &etag), encrypted_at_rest);
        if let Some(version) = version_header(versioning, info.version, info.null_version) {
            response = response.header("x-amz-version-id", version);
        }
        if let Some(redirect) = redirect {
            let encode = |text: &str| {
                percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            };
            let separator = if redirect.contains('?') { '&' } else { '?' };
            let target = format!(
                "{redirect}{separator}bucket={}&key={}&etag={}",
                encode(&bucket),
                encode(&key),
                encode(&etag)
            );
            return Ok(response
                .status(StatusCode::SEE_OTHER)
                .header("location", target)
                .body(full(Bytes::new()))
                .expect("a valid response"));
        }
        let response = response.header("location", &location);
        Ok(match status.as_deref() {
            Some("201") => response
                .status(StatusCode::CREATED)
                .header("content-type", "application/xml")
                .body(full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></PostResponse>",
                    escape(&location),
                    escape(&bucket),
                    escape(&key),
                    escape(&etag)
                )))
                .expect("a valid response"),
            Some("200") => response
                .status(StatusCode::OK)
                .body(full(Bytes::new()))
                .expect("a valid response"),
            _ => response
                .status(StatusCode::NO_CONTENT)
                .body(full(Bytes::new()))
                .expect("a valid response"),
        })
    })
    .await
}

/// Pushes `data` into the object, refusing it past `max` bytes in all.
async fn push_bounded(
    state: &S3State,
    sink: &mut BodySink,
    data: &[u8],
    max: u64,
) -> Result<(), S3Error> {
    if sink.pushed() + data.len() as u64 > max {
        return Err(S3Error::new(
            400,
            "EntityTooLarge",
            "Your proposed upload exceeds the maximum allowed size",
        ));
    }
    sink.push(state, data).await
}
