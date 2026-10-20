//! AWS Signature Version 4 for S3: the canonical request, the string to sign,
//! the signing key, the `Authorization` header, and the per-chunk signatures
//! of a streamed (`aws-chunked`) upload.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use ring::{digest, hmac};

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// The `x-amz-content-sha256` value of a body sent without a hash.
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
/// The `x-amz-content-sha256` value of a streamed body whose chunks are
/// each signed.
pub const STREAMING_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Everything but the RFC 3986 unreserved characters is encoded.
const ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Why a request's signature could not be checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The `Authorization` header is not a well-formed SigV4 header.
    Malformed(String),
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, bytes).as_ref())
}

/// The credential scope's parts, from `Credential=AKID/date/region/service/aws4_request`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub access_key: String,
    pub date: String,
    pub region: String,
    pub service: String,
}

impl Credential {
    pub fn scope(&self) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            self.date, self.region, self.service
        )
    }
}

/// A parsed `Authorization: AWS4-HMAC-SHA256 ...` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthHeader {
    pub credential: Credential,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

pub fn parse_authorization(value: &str) -> Result<AuthHeader, AuthError> {
    let malformed = |why: &str| AuthError::Malformed(why.to_string());
    let rest = value
        .strip_prefix(ALGORITHM)
        .ok_or_else(|| malformed("unsupported signing algorithm"))?;
    let (mut credential, mut signed_headers, mut signature) = (None, None, None);
    for part in rest.split(',') {
        let (name, value) = part
            .trim()
            .split_once('=')
            .ok_or_else(|| malformed("a part has no '='"))?;
        match name {
            "Credential" => credential = Some(value),
            "SignedHeaders" => signed_headers = Some(value),
            "Signature" => signature = Some(value),
            _ => return Err(malformed("unknown part")),
        }
    }
    let credential = credential.ok_or_else(|| malformed("no Credential"))?;
    let fields: Vec<&str> = credential.split('/').collect();
    let [access_key, date, region, service, "aws4_request"] = fields.as_slice() else {
        return Err(malformed(
            "Credential is not KEY/date/region/service/aws4_request",
        ));
    };
    Ok(AuthHeader {
        credential: Credential {
            access_key: access_key.to_string(),
            date: date.to_string(),
            region: region.to_string(),
            service: service.to_string(),
        },
        signed_headers: signed_headers
            .ok_or_else(|| malformed("no SignedHeaders"))?
            .split(';')
            .map(str::to_string)
            .collect(),
        signature: signature
            .ok_or_else(|| malformed("no Signature"))?
            .to_string(),
    })
}

/// A presigned request's authorization, from its query string
/// (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
/// `X-Amz-SignedHeaders`, `X-Amz-Signature`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryAuth {
    pub auth: AuthHeader,
    pub amz_date: String,
    /// Seconds the signature is valid for, from `amz_date`.
    pub expires: u64,
    /// `X-Amz-Content-Sha256`, when the URL fixes the payload's hash.
    pub content_sha256: Option<String>,
    /// Whether it carries `X-Amz-Security-Token` (temporary credentials).
    pub security_token: bool,
}

/// The query-string authorization of a presigned request.
pub fn parse_query_auth(raw_query: &str) -> Result<QueryAuth, AuthError> {
    let malformed = |why: &str| AuthError::Malformed(why.to_string());
    let param = |name: &str| {
        raw_query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (key == name).then(|| percent_decode_str(value).decode_utf8_lossy().into_owned())
        })
    };
    if param("X-Amz-Algorithm").as_deref() != Some(ALGORITHM) {
        return Err(malformed("X-Amz-Algorithm must be AWS4-HMAC-SHA256"));
    }
    let credential = param("X-Amz-Credential").ok_or_else(|| malformed("no X-Amz-Credential"))?;
    let fields: Vec<&str> = credential.split('/').collect();
    let [access_key, date, region, service, "aws4_request"] = fields.as_slice() else {
        return Err(malformed(
            "X-Amz-Credential is not KEY/date/region/service/aws4_request",
        ));
    };
    let expires = param("X-Amz-Expires")
        .ok_or_else(|| malformed("no X-Amz-Expires"))?
        .parse::<u64>()
        .map_err(|_| malformed("X-Amz-Expires is not a number"))?;
    Ok(QueryAuth {
        auth: AuthHeader {
            credential: Credential {
                access_key: access_key.to_string(),
                date: date.to_string(),
                region: region.to_string(),
                service: service.to_string(),
            },
            signed_headers: param("X-Amz-SignedHeaders")
                .ok_or_else(|| malformed("no X-Amz-SignedHeaders"))?
                .split(';')
                .map(str::to_string)
                .collect(),
            signature: param("X-Amz-Signature").ok_or_else(|| malformed("no X-Amz-Signature"))?,
        },
        amz_date: param("X-Amz-Date").ok_or_else(|| malformed("no X-Amz-Date"))?,
        expires,
        content_sha256: param("X-Amz-Content-Sha256"),
        security_token: param("X-Amz-Security-Token").is_some(),
    })
}

/// A presigned request's query without its `X-Amz-Signature`: what the
/// signature covers.
pub fn query_without_signature(raw_query: &str) -> String {
    raw_query
        .split('&')
        .filter(|pair| pair.split('=').next() != Some("X-Amz-Signature"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The canonical URI: each path segment percent-encoded once (S3 does not
/// double-encode), from the path as the client sent it.
pub fn canonical_uri(raw_path: &str) -> String {
    let path = if raw_path.is_empty() { "/" } else { raw_path };
    path.split('/')
        .map(|segment| {
            let decoded = percent_decode_str(segment).collect::<Vec<u8>>();
            percent_encoding::percent_encode(&decoded, ENCODE).to_string()
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The canonical URI of every service but S3: the S3 canonical URI encoded
/// once more, as AWS's SDKs sign it.
fn double_encoded_uri(raw_path: &str) -> String {
    canonical_uri(raw_path)
        .split('/')
        .map(|segment| percent_encoding::utf8_percent_encode(segment, ENCODE).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// The canonical query string: every parameter encoded and sorted by name,
/// then value; a parameter without a value gets an empty one.
pub fn canonical_query(raw_query: &str) -> String {
    let mut pairs: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let encode = |part: &str| {
                let decoded = percent_decode_str(part).collect::<Vec<u8>>();
                percent_encoding::percent_encode(&decoded, ENCODE).to_string()
            };
            (encode(name), encode(value))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// A header value as signed: trimmed, inner runs of spaces collapsed.
fn canonical_header_value(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The canonical request. `headers` are the request's headers (any case);
/// `signed_headers` are the lowercase names the client signed, in order.
pub fn canonical_request(
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &[(String, String)],
    signed_headers: &[String],
    payload_hash: &str,
) -> Result<String, AuthError> {
    canonical_request_for(
        "s3",
        method,
        raw_path,
        raw_query,
        headers,
        signed_headers,
        payload_hash,
    )
}

/// [`canonical_request`] for `service`: every service but S3 encodes the
/// path twice.
pub fn canonical_request_for(
    service: &str,
    method: &str,
    raw_path: &str,
    raw_query: &str,
    headers: &[(String, String)],
    signed_headers: &[String],
    payload_hash: &str,
) -> Result<String, AuthError> {
    let mut canonical_headers = String::new();
    for name in signed_headers {
        let values: Vec<String> = headers
            .iter()
            .filter(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| canonical_header_value(value))
            .collect();
        if values.is_empty() {
            return Err(AuthError::Malformed(format!(
                "signed header {name} is not in the request"
            )));
        }
        canonical_headers.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    Ok(format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{}\n{payload_hash}",
        if service == "s3" {
            canonical_uri(raw_path)
        } else {
            double_encoded_uri(raw_path)
        },
        canonical_query(raw_query),
        signed_headers.join(";"),
    ))
}

pub fn string_to_sign(amz_date: &str, scope: &str, canonical_request: &str) -> String {
    format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    )
}

/// The derived signing key for one day, region and service.
pub fn signing_key(secret: &str, credential: &Credential) -> hmac::Key {
    let step = |key: &[u8], data: &str| {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data.as_bytes())
    };
    let date = step(format!("AWS4{secret}").as_bytes(), &credential.date);
    let region = step(date.as_ref(), &credential.region);
    let service = step(region.as_ref(), &credential.service);
    let signing = step(service.as_ref(), "aws4_request");
    hmac::Key::new(hmac::HMAC_SHA256, signing.as_ref())
}

pub fn sign(key: &hmac::Key, string_to_sign: &str) -> String {
    hex(hmac::sign(key, string_to_sign.as_bytes()).as_ref())
}

/// Whether `provided` (hex) is `key`'s signature of `string_to_sign`,
/// compared in constant time.
pub fn verify(key: &hmac::Key, string_to_sign: &str, provided: &str) -> bool {
    let Some(tag) = decode_hex(provided) else {
        return false;
    };
    hmac::verify(key, string_to_sign.as_bytes(), &tag).is_ok()
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
        .collect()
}

/// The string to sign for one chunk of a streamed upload, chained to the
/// previous chunk's signature (the request's own signature for the first).
pub fn chunk_string_to_sign(
    amz_date: &str,
    scope: &str,
    previous_signature: &str,
    chunk: &[u8],
) -> String {
    format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{previous_signature}\n{}\n{}",
        sha256_hex(b""),
        sha256_hex(chunk)
    )
}

/// The path as sent, decoded: the bucket and key it names.
pub fn decode_path(raw_path: &str) -> Option<String> {
    percent_decode_str(raw_path)
        .decode_utf8()
        .ok()
        .map(|path| path.into_owned())
}

/// Encodes a key for a URL or an XML `<Key>` in `encoding-type=url` mode.
pub fn encode_key(key: &str) -> String {
    utf8_percent_encode(key, ENCODE)
        .to_string()
        .replace("%2F", "/")
}

#[cfg(test)]
mod tests {
    #[test]
    fn other_services_sign_the_path_encoded_twice() {
        assert_eq!(
            super::double_encoded_uri("/tags/arn%3Aaws%3As3vectors"),
            "/tags/arn%253Aaws%253As3vectors"
        );
        assert_eq!(super::double_encoded_uri("/PutVectors"), "/PutVectors");
        assert_eq!(super::canonical_uri("/a%3Ab"), "/a%3Ab");
    }

    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const EMPTY_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn credential() -> Credential {
        Credential {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            date: "20130524".to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn signed(names: &str) -> Vec<String> {
        names.split(';').map(str::to_string).collect()
    }

    /// Signs the request the way a client does, for the documented examples.
    fn signature_of(
        method: &str,
        path: &str,
        query: &str,
        request_headers: &[(&str, &str)],
        names: &str,
        payload_hash: &str,
    ) -> String {
        let canonical = canonical_request(
            method,
            path,
            query,
            &headers(request_headers),
            &signed(names),
            payload_hash,
        )
        .unwrap();
        let to_sign = string_to_sign("20130524T000000Z", &credential().scope(), &canonical);
        sign(&signing_key(SECRET, &credential()), &to_sign)
    }

    // The examples in AWS's "Signature Calculations for the Authorization
    // Header" (S3 API reference).
    #[test]
    fn the_documented_get_object_example_signs() {
        assert_eq!(
            signature_of(
                "GET",
                "/test.txt",
                "",
                &[
                    ("Host", "examplebucket.s3.amazonaws.com"),
                    ("Range", "bytes=0-9"),
                    ("x-amz-content-sha256", EMPTY_HASH),
                    ("x-amz-date", "20130524T000000Z"),
                ],
                "host;range;x-amz-content-sha256;x-amz-date",
                EMPTY_HASH,
            ),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn the_documented_put_object_example_signs() {
        let payload_hash = sha256_hex(b"Welcome to Amazon S3.");
        assert_eq!(
            payload_hash,
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
        );
        assert_eq!(
            signature_of(
                "PUT",
                "/test$file.text",
                "",
                &[
                    ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
                    ("Host", "examplebucket.s3.amazonaws.com"),
                    ("x-amz-content-sha256", &payload_hash),
                    ("x-amz-date", "20130524T000000Z"),
                    ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
                ],
                "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
                &payload_hash,
            ),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    #[test]
    fn the_documented_subresource_and_list_examples_sign() {
        let request_headers = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", EMPTY_HASH),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let names = "host;x-amz-content-sha256;x-amz-date";
        assert_eq!(
            signature_of("GET", "/", "lifecycle", &request_headers, names, EMPTY_HASH),
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
        assert_eq!(
            signature_of(
                "GET",
                "/",
                "prefix=J&max-keys=2",
                &request_headers,
                names,
                EMPTY_HASH
            ),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    // The example in AWS's "Signature Calculations for the Authorization
    // Header: Transferring Payload in Multiple Chunks".
    #[test]
    fn the_documented_chunked_upload_example_signs() {
        let seed = signature_of(
            "PUT",
            "/examplebucket/chunkObject.txt",
            "",
            &[
                ("Host", "s3.amazonaws.com"),
                ("x-amz-date", "20130524T000000Z"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
                ("x-amz-content-sha256", STREAMING_PAYLOAD),
                ("Content-Encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "66560"),
                ("Content-Length", "66824"),
            ],
            "content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class",
            STREAMING_PAYLOAD,
        );
        assert_eq!(
            seed,
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
        );
        let key = signing_key(SECRET, &credential());
        let scope = credential().scope();
        let chunk_signature = |previous: &str, chunk: &[u8]| {
            sign(
                &key,
                &chunk_string_to_sign("20130524T000000Z", &scope, previous, chunk),
            )
        };
        let first = chunk_signature(&seed, &[b'a'; 65536]);
        assert_eq!(
            first,
            "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
        );
        let second = chunk_signature(&first, &[b'a'; 1024]);
        assert_eq!(
            second,
            "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
        );
        assert_eq!(
            chunk_signature(&second, b""),
            "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
        );
    }

    #[test]
    fn the_authorization_header_parses() {
        let header = parse_authorization(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024",
        )
        .unwrap();
        assert_eq!(header.credential, credential());
        assert_eq!(header.signed_headers.len(), 4);
        assert!(parse_authorization("AWS AKIA:sig").is_err());
        assert!(parse_authorization("AWS4-HMAC-SHA256 Credential=a/b/c").is_err());
    }

    #[test]
    fn a_presigned_url_from_botocore_verifies() {
        // botocore's S3SigV4QueryAuth, at 2013-05-24T00:00:00Z, for AWS's
        // example credentials.
        let url_query = "response-content-type=text%2Fplain&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=be1a448068ed38fdb7ee21493440d2ff1b0c71d3f08051e3ab06ef58434c9e62";
        let query = parse_query_auth(url_query).unwrap();
        assert_eq!(query.expires, 86400);
        assert_eq!(query.auth.credential.access_key, "AKIAIOSFODNN7EXAMPLE");
        let headers = vec![("host".to_string(), "127.0.0.1:9000".to_string())];
        let canonical = canonical_request(
            "GET",
            "/examplebucket/test.txt",
            &query_without_signature(url_query),
            &headers,
            &query.auth.signed_headers,
            UNSIGNED_PAYLOAD,
        )
        .unwrap();
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            &query.auth.credential,
        );
        let to_sign = string_to_sign(&query.amz_date, &query.auth.credential.scope(), &canonical);
        assert!(verify(&key, &to_sign, &query.auth.signature));
        // Any other query (a tampered override) does not.
        let tampered = canonical_request(
            "GET",
            "/examplebucket/test.txt",
            &query_without_signature(&url_query.replace("text%2Fplain", "text%2Fhtml")),
            &headers,
            &query.auth.signed_headers,
            UNSIGNED_PAYLOAD,
        )
        .unwrap();
        let to_sign = string_to_sign(&query.amz_date, &query.auth.credential.scope(), &tampered);
        assert!(!verify(&key, &to_sign, &query.auth.signature));
    }
}
