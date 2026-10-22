//! Server-side encryption, as S3 states it. TruthDB's own encryption at rest
//! (the database's pages, log and backups, under a key the server manages)
//! is what S3 calls SSE-S3 (`AES256`): when the database is encrypted, every
//! object is, and the answers say so (`x-amz-server-side-encryption:
//! AES256`). A request may ask for it, and is refused when the database is
//! not encrypted. KMS is not offered.
//!
//! `?encryption` keeps a bucket's default encryption settings. Its default
//! (`AES256`) is the database's encryption at rest, so what is stored is the
//! rest: the bucket key setting (kept and returned; it only means something
//! with KMS) and whether the bucket blocks customer-provided keys (SSE-C).

use hyper::body::Incoming;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use truthdb_object::BUCKET_ENCRYPTION;

use crate::error::S3Error;
use crate::server::{Authenticated, Body, S3State, full, read_body};

/// The header naming the server-side encryption of a write, and of an object.
pub const SSE_HEADER: &str = "x-amz-server-side-encryption";
/// S3's SSE-S3.
pub const AES256: &str = "AES256";

/// The bucket's settings: BucketKeyEnabled.
const BUCKET_KEY_ENABLED: u8 = 1;
/// The bucket's settings: BlockedEncryptionTypes holds SSE-C.
pub const SSE_C_BLOCKED: u8 = 2;

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// The encryption values S3 knows that this server does not offer.
fn not_offered(value: &str) -> S3Error {
    invalid(format!(
        "Server-side encryption {value} is not supported: only {AES256} is"
    ))
}

/// Checks a write's `x-amz-server-side-encryption`: `AES256` is the
/// database's encryption at rest, so it needs the database encrypted; KMS and
/// the other values are refused.
pub fn check_write(headers: &HeaderMap, encrypted_at_rest: bool) -> Result<(), S3Error> {
    if let Some(value) = headers.get(SSE_HEADER) {
        check_write_value(value.to_str().unwrap_or(""), encrypted_at_rest)?;
    }
    if KMS_HEADERS.iter().any(|name| headers.contains_key(*name)) {
        return Err(not_offered("aws:kms"));
    }
    Ok(())
}

/// Headers (and POST form fields) that only KMS encryption takes.
pub const KMS_HEADERS: [&str; 2] = [
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-context",
];

/// [`check_write`] for a POST form's fields (`field` finds one by name).
pub fn check_write_fields<'a>(
    field: impl Fn(&str) -> Option<&'a str>,
    encrypted_at_rest: bool,
) -> Result<Option<&'a str>, S3Error> {
    if KMS_HEADERS.iter().any(|name| field(name).is_some()) {
        return Err(not_offered("aws:kms"));
    }
    let value = field(SSE_HEADER);
    if let Some(value) = value {
        check_write_value(value, encrypted_at_rest)?;
    }
    Ok(value)
}

/// [`check_write`] for a value (a POST form's field).
pub fn check_write_value(value: &str, encrypted_at_rest: bool) -> Result<(), S3Error> {
    match value {
        AES256 if encrypted_at_rest => Ok(()),
        AES256 => Err(invalid(
            "Server-side encryption AES256 needs the database encrypted at rest; this server's \
             is not",
        )),
        "aws:kms" | "aws:kms:dsse" | "aws:fsx" | "aws:backup" => Err(not_offered(value)),
        _ => Err(invalid(format!(
            "The server-side encryption {value:?} is not valid"
        ))),
    }
}

/// A GET or HEAD names no server-side encryption: an object's is fixed when
/// it is written (S3 answers 400).
pub fn check_read(headers: &HeaderMap) -> Result<(), S3Error> {
    if headers.contains_key(SSE_HEADER) {
        return Err(invalid(
            "The x-amz-server-side-encryption header is not allowed on a GET or HEAD request",
        ));
    }
    Ok(())
}

/// Adds `x-amz-server-side-encryption: AES256` when the database is
/// encrypted at rest.
pub fn with_header(
    builder: hyper::http::response::Builder,
    encrypted_at_rest: bool,
) -> hyper::http::response::Builder {
    if encrypted_at_rest {
        builder.header(SSE_HEADER, AES256)
    } else {
        builder
    }
}

/// [`with_header`] on a built response.
pub fn add_header(response: &mut Response<Body>, encrypted_at_rest: bool) {
    if encrypted_at_rest {
        response
            .headers_mut()
            .insert(SSE_HEADER, hyper::header::HeaderValue::from_static(AES256));
    }
}

/// A ServerSideEncryptionConfiguration document's settings.
fn parse(body: &[u8], encrypted_at_rest: bool) -> Result<u8, S3Error> {
    use quick_xml::Reader;
    use quick_xml::events::Event;
    let malformed = || {
        S3Error::new(
            400,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published \
             schema",
        )
    };
    let mut reader = Reader::from_reader(body);
    // The open elements, and the text of the innermost one so far.
    let mut path: Vec<String> = Vec::new();
    let mut text = String::new();
    let (mut roots, mut rules, mut flags) = (0, 0, 0u8);
    let (mut default_seen, mut algorithm, mut bucket_key) = (false, None::<String>, None::<bool>);
    let mut blocked: Vec<String> = Vec::new();
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|_| malformed())?;
        let empty = matches!(event, Event::Empty(_));
        match event {
            // An empty element is an element too: `<Rule/>` is a rule, and
            // `<ApplyServerSideEncryptionByDefault/>` a default without its
            // algorithm.
            Event::Start(e) | Event::Empty(e) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                let at: Vec<&str> = path.iter().map(String::as_str).collect();
                match (at.as_slice(), name.as_str()) {
                    ([], "ServerSideEncryptionConfiguration") => roots += 1,
                    ([], _) => return Err(malformed()),
                    ([_], "Rule") => rules += 1,
                    ([_, "Rule"], "ApplyServerSideEncryptionByDefault") => default_seen = true,
                    ([_, "Rule", "ApplyServerSideEncryptionByDefault"], "KMSMasterKeyID") => {
                        return Err(not_offered("aws:kms"));
                    }
                    _ => {}
                }
                text.clear();
                if !empty {
                    path.push(name);
                }
            }
            Event::Text(t) => text.push_str(&t.unescape().map_err(|_| malformed())?),
            Event::CData(t) => text.push_str(std::str::from_utf8(&t).map_err(|_| malformed())?),
            Event::End(_) => {
                let value = std::mem::take(&mut text).trim().to_string();
                let at: Vec<&str> = path.iter().map(String::as_str).collect();
                match at.as_slice() {
                    [
                        _,
                        "Rule",
                        "ApplyServerSideEncryptionByDefault",
                        "SSEAlgorithm",
                    ] => {
                        if algorithm.replace(value).is_some() {
                            return Err(malformed());
                        }
                    }
                    [_, "Rule", "BlockedEncryptionTypes", "EncryptionType"] => blocked.push(value),
                    [_, "Rule", "BucketKeyEnabled"] => {
                        let enabled = match value.to_ascii_lowercase().as_str() {
                            "true" => true,
                            "false" => false,
                            _ => return Err(malformed()),
                        };
                        if bucket_key.replace(enabled).is_some() {
                            return Err(malformed());
                        }
                    }
                    _ => {}
                }
                path.pop();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    // One document, closed, with one rule: what several would mean together
    // is not defined.
    if roots != 1 || !path.is_empty() || rules != 1 {
        return Err(malformed());
    }
    if default_seen {
        check_write_value(
            algorithm.as_deref().ok_or_else(malformed)?,
            encrypted_at_rest,
        )?;
    }
    if bucket_key == Some(true) {
        flags |= BUCKET_KEY_ENABLED;
    }
    match blocked
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] | ["NONE"] => {}
        ["SSE-C"] => flags |= SSE_C_BLOCKED,
        _ => {
            return Err(invalid("BlockedEncryptionTypes holds either NONE or SSE-C"));
        }
    }
    Ok(flags)
}

fn render(flags: u8, encrypted_at_rest: bool) -> String {
    let default = if encrypted_at_rest {
        format!(
            "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>{AES256}</SSEAlgorithm>\
             </ApplyServerSideEncryptionByDefault>"
        )
    } else {
        String::new()
    };
    let blocked = if flags & SSE_C_BLOCKED != 0 {
        "SSE-C"
    } else {
        "NONE"
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ServerSideEncryptionConfiguration \
         xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule>{default}\
         <BlockedEncryptionTypes><EncryptionType>{blocked}</EncryptionType>\
         </BlockedEncryptionTypes><BucketKeyEnabled>{}</BucketKeyEnabled></Rule>\
         </ServerSideEncryptionConfiguration>",
        flags & BUCKET_KEY_ENABLED != 0
    )
}

/// PUT, GET or DELETE `?encryption` on `bucket`.
pub async fn handle(
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
    let encrypted_at_rest = state.storage.is_encrypted();
    let empty = |status: StatusCode| {
        Response::builder()
            .status(status)
            .body(full(bytes::Bytes::new()))
            .expect("a valid response")
    };
    match method {
        Method::PUT => {
            let body = read_body(request, auth, 64 * 1024).await?;
            let flags = parse(&body, encrypted_at_rest)?;
            crate::scope::object(state, move |file| {
                file.transaction(|tx| {
                    tx.put_bucket_config(&bucket, BUCKET_ENCRYPTION, "", &[flags])
                })
            })
            .await?;
            Ok(empty(StatusCode::OK))
        }
        Method::GET => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_ENCRYPTION, "")
            })
            .await?;
            // Without settings of its own, a bucket of an encrypted database
            // still has its default: AES256.
            let flags = match stored {
                Some(document) => document.first().copied().unwrap_or(0),
                None if encrypted_at_rest => 0,
                None => {
                    return Err(S3Error::new(
                        400,
                        "ServerSideEncryptionConfigurationNotFoundError",
                        "The server-side encryption configuration was not found",
                    ));
                }
            };
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(render(flags, encrypted_at_rest)))
                .expect("a valid response"))
        }
        _ => {
            crate::scope::object(state, move |file| {
                file.transaction(|tx| tx.delete_bucket_config(&bucket, BUCKET_ENCRYPTION, ""))
            })
            .await?;
            Ok(empty(StatusCode::NO_CONTENT))
        }
    }
}

/// Whether `bucket` blocks customer-provided keys (SSE-C).
pub fn sse_c_blocked(
    file: &mut truthdb_object::ObjFile<'_>,
    bucket: &str,
) -> Result<bool, truthdb_core::storage::StorageError> {
    Ok(file
        .bucket_config(bucket, BUCKET_ENCRYPTION, "")?
        .and_then(|document| document.first().copied())
        .is_some_and(|flags| flags & SSE_C_BLOCKED != 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(rule: &str) -> String {
        format!(
            "<ServerSideEncryptionConfiguration><Rule>{rule}</Rule></ServerSideEncryptionConfiguration>"
        )
    }

    #[test]
    fn the_configuration_parses_validates_and_renders() {
        let aes = config(
            "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
             </ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled>",
        );
        assert_eq!(parse(aes.as_bytes(), true).unwrap(), BUCKET_KEY_ENABLED);
        // AES256 is the database's encryption at rest.
        assert_eq!(
            parse(aes.as_bytes(), false).unwrap_err().code,
            "InvalidArgument"
        );
        let kms = config(
            "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>\
             <KMSMasterKeyID>k</KMSMasterKeyID></ApplyServerSideEncryptionByDefault>",
        );
        assert_eq!(
            parse(kms.as_bytes(), true).unwrap_err().code,
            "InvalidArgument"
        );
        let blocked = config(
            "<BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType></BlockedEncryptionTypes>",
        );
        let flags = parse(blocked.as_bytes(), false).unwrap();
        assert_eq!(flags, SSE_C_BLOCKED);
        assert_eq!(parse(render(flags, true).as_bytes(), true).unwrap(), flags);
        assert_eq!(
            parse(render(flags, false).as_bytes(), false).unwrap(),
            flags
        );
        let both = config(
            "<BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType>\
             <EncryptionType>NONE</EncryptionType></BlockedEncryptionTypes>",
        );
        assert!(parse(both.as_bytes(), true).is_err());
        assert!(parse(b"<ServerSideEncryptionConfiguration/>", true).is_err());
        assert!(
            parse(
                config("<ApplyServerSideEncryptionByDefault/>").as_bytes(),
                true
            )
            .is_err()
        );
        assert!(parse(b"<Other><Rule/></Other>", true).is_err());
        // One closed document, each value once; CDATA and comments are text.
        let blocked_rule = "<Rule><BlockedEncryptionTypes><EncryptionType>SSE-C</EncryptionType>\
                            </BlockedEncryptionTypes></Rule>";
        let two_roots =
            format!("<ServerSideEncryptionConfiguration/><Other>{blocked_rule}</Other>");
        assert!(parse(two_roots.as_bytes(), true).is_err());
        assert!(parse(b"<ServerSideEncryptionConfiguration><Rule>", true).is_err());
        let twice = config(
            "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>\
             <SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault>",
        );
        assert!(parse(twice.as_bytes(), true).is_err());
        let key_twice = config(
            "<BucketKeyEnabled>true</BucketKeyEnabled><BucketKeyEnabled>false</BucketKeyEnabled>",
        );
        assert!(parse(key_twice.as_bytes(), true).is_err());
        for algorithm in ["<![CDATA[AES256]]>", "AES<!-- -->256"] {
            let rule = config(&format!(
                "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>{algorithm}</SSEAlgorithm>\
                 </ApplyServerSideEncryptionByDefault>"
            ));
            assert_eq!(parse(rule.as_bytes(), true).unwrap(), 0, "{algorithm}");
        }
        let empty_kms = config(
            "<ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
             <KMSMasterKeyID/></ApplyServerSideEncryptionByDefault>",
        );
        assert!(parse(empty_kms.as_bytes(), true).is_err());
    }

    #[test]
    fn a_write_may_ask_for_aes256_only_when_the_database_is_encrypted() {
        let headers = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(SSE_HEADER, value.parse().unwrap());
            headers
        };
        assert!(check_write(&HeaderMap::new(), false).is_ok());
        assert!(check_write(&headers("AES256"), true).is_ok());
        assert!(check_write(&headers("AES256"), false).is_err());
        assert!(check_write(&headers("aws:kms"), true).is_err());
        assert!(check_write(&headers("aes256"), true).is_err());
        assert!(check_read(&headers("AES256")).is_err());
        assert!(check_read(&HeaderMap::new()).is_ok());
    }
}
