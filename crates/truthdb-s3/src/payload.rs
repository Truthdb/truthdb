//! Request bodies as S3 clients send them, decoded and verified as they
//! stream in: a plain body (unsigned, or checked against its SHA-256), or an
//! `aws-chunked` body whose chunks are each signed, or unsigned with a
//! trailing checksum. A full-body checksum (`x-amz-checksum-*` header or
//! trailer) is verified at the end.

use base64::Engine as _;
use ring::{digest, hmac};

use crate::error::S3Error;
use crate::sigv4;

/// The longest chunk header or trailer line accepted.
const MAX_LINE: usize = 4096;
/// The largest chunk accepted: a signed chunk is held whole until its
/// signature checks out, so its declared size must not be the client's to
/// choose freely (SDKs send 64 KiB to a few MiB).
const MAX_CHUNK: usize = 16 * 1024 * 1024;

/// How the body is framed and signed (from `x-amz-content-sha256`).
pub enum Framing {
    /// The body as is, unsigned.
    Unsigned,
    /// The body as is; its SHA-256 must equal this hex digest.
    Sha256(String),
    /// `aws-chunked`, each chunk signed in a chain seeded by the request's
    /// own signature.
    SignedChunks {
        key: Box<hmac::Key>,
        amz_date: String,
        scope: String,
        previous: String,
    },
    /// `aws-chunked`, unsigned, with the checksum in a trailer.
    UnsignedTrailer,
}

/// A full-body checksum to verify.
pub enum Checksum {
    Crc32(crc32fast::Hasher),
    Sha1(digest::Context),
    Sha256(digest::Context),
}

impl Checksum {
    /// The checksum named by an `x-amz-checksum-<algorithm>` header or trailer
    /// name, if this build supports it.
    pub fn for_header(name: &str) -> Result<Self, S3Error> {
        match name.to_ascii_lowercase().as_str() {
            "x-amz-checksum-crc32" => Ok(Checksum::Crc32(crc32fast::Hasher::new())),
            "x-amz-checksum-sha1" => Ok(Checksum::Sha1(digest::Context::new(
                &digest::SHA1_FOR_LEGACY_USE_ONLY,
            ))),
            "x-amz-checksum-sha256" => Ok(Checksum::Sha256(digest::Context::new(&digest::SHA256))),
            other => Err(S3Error::not_implemented(format!(
                "checksum {other} is not supported; use CRC32, SHA1 or SHA256"
            ))),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Checksum::Crc32(hasher) => hasher.update(bytes),
            Checksum::Sha1(context) | Checksum::Sha256(context) => context.update(bytes),
        }
    }

    /// The checksum, base64 as S3 sends it.
    fn finish(self) -> String {
        let bytes = match self {
            Checksum::Crc32(hasher) => hasher.finalize().to_be_bytes().to_vec(),
            Checksum::Sha1(context) | Checksum::Sha256(context) => {
                context.finish().as_ref().to_vec()
            }
        };
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }
}

enum State {
    /// Reading a chunk header line: `size-hex[;chunk-signature=sig]`.
    ChunkHeader,
    /// Reading `remaining` data bytes of a chunk.
    ChunkData {
        remaining: usize,
    },
    /// Expecting the CRLF after a chunk's data.
    ChunkEnd,
    /// Reading trailer lines up to the empty line.
    Trailer,
    Done,
}

/// Decodes one request body. Feed it the bytes as they arrive; it appends
/// the body's own bytes to `out` and fails as soon as anything does not
/// verify.
pub struct PayloadDecoder {
    framing: Framing,
    state: State,
    line: Vec<u8>,
    /// The current signed chunk's data and its claimed signature.
    chunk: Vec<u8>,
    chunk_signature: String,
    sha256: Option<digest::Context>,
    /// The checksum to verify and, for a header checksum, its expected value
    /// (a trailer brings its value at the end).
    checksum: Option<(String, Checksum, Option<String>)>,
    decoded: u64,
}

impl PayloadDecoder {
    /// `checksum` is the full-body checksum's header name and, for a header
    /// checksum, its value.
    pub fn new(
        framing: Framing,
        checksum: Option<(String, Option<String>)>,
    ) -> Result<Self, S3Error> {
        let chunked = matches!(
            framing,
            Framing::SignedChunks { .. } | Framing::UnsignedTrailer
        );
        let sha256 =
            matches!(framing, Framing::Sha256(_)).then(|| digest::Context::new(&digest::SHA256));
        let checksum = match checksum {
            Some((name, value)) => Some((
                name.to_ascii_lowercase(),
                Checksum::for_header(&name)?,
                value,
            )),
            None => None,
        };
        Ok(PayloadDecoder {
            framing,
            state: if chunked {
                State::ChunkHeader
            } else {
                State::ChunkData {
                    remaining: usize::MAX,
                }
            },
            line: Vec::new(),
            chunk: Vec::new(),
            chunk_signature: String::new(),
            sha256,
            checksum,
            decoded: 0,
        })
    }

    /// Body bytes decoded so far.
    pub fn decoded_len(&self) -> u64 {
        self.decoded
    }

    fn emit(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        if let Some(context) = &mut self.sha256 {
            context.update(bytes);
        }
        if let Some((_, checksum, _)) = &mut self.checksum {
            checksum.update(bytes);
        }
        self.decoded += bytes.len() as u64;
        out.extend_from_slice(bytes);
    }

    pub fn feed(&mut self, mut input: &[u8], out: &mut Vec<u8>) -> Result<(), S3Error> {
        if matches!(self.framing, Framing::Unsigned | Framing::Sha256(_)) {
            self.emit(input, out);
            return Ok(());
        }
        while !input.is_empty() {
            match self.state {
                State::ChunkHeader | State::ChunkEnd | State::Trailer => {
                    let Some(end) = input.iter().position(|byte| *byte == b'\n') else {
                        self.push_line(input)?;
                        return Ok(());
                    };
                    self.push_line(&input[..=end])?;
                    input = &input[end + 1..];
                    let line = std::mem::take(&mut self.line);
                    let line = line
                        .strip_suffix(b"\r\n")
                        .ok_or_else(|| malformed("a line does not end in CRLF"))?;
                    self.on_line(line)?;
                }
                State::ChunkData { remaining } => {
                    let take = remaining.min(input.len());
                    let data = &input[..take];
                    input = &input[take..];
                    if matches!(self.framing, Framing::SignedChunks { .. }) {
                        // A signed chunk's bytes are released only once its
                        // signature checks out.
                        self.chunk.extend_from_slice(data);
                    } else {
                        self.emit(data, out);
                    }
                    self.state = State::ChunkData {
                        remaining: remaining - take,
                    };
                    if remaining == take {
                        self.end_chunk(out)?;
                    }
                }
                State::Done => return Err(malformed("bytes after the final chunk")),
            }
        }
        Ok(())
    }

    fn push_line(&mut self, bytes: &[u8]) -> Result<(), S3Error> {
        if self.line.len() + bytes.len() > MAX_LINE {
            return Err(malformed("a chunk header or trailer line is too long"));
        }
        self.line.extend_from_slice(bytes);
        Ok(())
    }

    fn on_line(&mut self, line: &[u8]) -> Result<(), S3Error> {
        let line = std::str::from_utf8(line).map_err(|_| malformed("a line is not UTF-8"))?;
        match self.state {
            State::ChunkHeader => {
                let (size, extension) = line.split_once(';').unwrap_or((line, ""));
                let size = usize::from_str_radix(size.trim(), 16)
                    .map_err(|_| malformed("a chunk size is not hex"))?;
                if size > MAX_CHUNK {
                    return Err(malformed("a chunk is larger than 16 MiB"));
                }
                self.chunk_signature = match &self.framing {
                    Framing::SignedChunks { .. } => extension
                        .strip_prefix("chunk-signature=")
                        .ok_or_else(|| malformed("a signed chunk has no signature"))?
                        .to_string(),
                    _ => String::new(),
                };
                self.chunk.clear();
                self.state = State::ChunkData { remaining: size };
                if size == 0 {
                    // The final chunk: no data, no CRLF of its own before the
                    // trailer (or the closing empty line).
                    let mut sink = Vec::new();
                    self.verify_chunk(&mut sink)?;
                    self.state = State::Trailer;
                }
            }
            State::ChunkEnd => {
                if !line.is_empty() {
                    return Err(malformed("chunk data runs past its size"));
                }
                self.state = State::ChunkHeader;
            }
            State::Trailer => {
                if line.is_empty() {
                    self.state = State::Done;
                } else {
                    let (name, value) = line
                        .split_once(':')
                        .ok_or_else(|| malformed("a trailer line has no ':'"))?;
                    let name = name.trim().to_ascii_lowercase();
                    if let Some((expected_name, _, value_slot)) = &mut self.checksum
                        && *expected_name == name
                    {
                        *value_slot = Some(value.trim().to_string());
                    }
                }
            }
            State::ChunkData { .. } | State::Done => unreachable!("not a line state"),
        }
        Ok(())
    }

    /// A chunk's data is complete: verify a signed chunk and release it.
    fn end_chunk(&mut self, out: &mut Vec<u8>) -> Result<(), S3Error> {
        self.verify_chunk(out)?;
        self.state = State::ChunkEnd;
        Ok(())
    }

    fn verify_chunk(&mut self, out: &mut Vec<u8>) -> Result<(), S3Error> {
        if let Framing::SignedChunks {
            key,
            amz_date,
            scope,
            previous,
        } = &mut self.framing
        {
            let to_sign = sigv4::chunk_string_to_sign(amz_date, scope, previous, &self.chunk);
            if !sigv4::verify(key, &to_sign, &self.chunk_signature) {
                return Err(S3Error::signature_mismatch());
            }
            *previous = std::mem::take(&mut self.chunk_signature);
            let chunk = std::mem::take(&mut self.chunk);
            self.emit(&chunk, out);
        }
        Ok(())
    }

    /// The body ended: check that the framing is complete and every digest and
    /// checksum matches. `declared_len` is `x-amz-decoded-content-length` for
    /// a chunked body.
    pub fn finish(self, declared_len: Option<u64>) -> Result<u64, S3Error> {
        let chunked = matches!(
            self.framing,
            Framing::SignedChunks { .. } | Framing::UnsignedTrailer
        );
        if chunked && !matches!(self.state, State::Done) {
            return Err(malformed("the body ended before its final chunk"));
        }
        if let Some(declared) = declared_len
            && declared != self.decoded
        {
            return Err(S3Error::invalid_request(format!(
                "x-amz-decoded-content-length is {declared}, the body has {} bytes",
                self.decoded
            )));
        }
        if let (Framing::Sha256(expected), Some(context)) = (&self.framing, self.sha256)
            && sigv4::hex(context.finish().as_ref()) != *expected
        {
            return Err(S3Error::new(
                400,
                "XAmzContentSHA256Mismatch",
                "The provided 'x-amz-content-sha256' header does not match what was computed.",
            ));
        }
        if let Some((name, checksum, expected)) = self.checksum {
            let expected = expected
                .ok_or_else(|| S3Error::invalid_request(format!("no {name} value was sent")))?;
            if checksum.finish() != expected {
                return Err(S3Error::bad_digest(format!(
                    "The {name} you specified did not match the calculated checksum."
                )));
            }
        }
        Ok(self.decoded)
    }
}

fn malformed(why: &str) -> S3Error {
    S3Error::new(
        400,
        "IncompleteBody",
        format!("malformed aws-chunked body: {why}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn signed_framing() -> Framing {
        let credential = sigv4::Credential {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            date: "20130524".to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        };
        Framing::SignedChunks {
            key: Box::new(sigv4::signing_key(SECRET, &credential)),
            amz_date: "20130524T000000Z".to_string(),
            scope: credential.scope(),
            previous: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
                .to_string(),
        }
    }

    /// The documented chunked upload: 65536 + 1024 bytes of 'a', then the
    /// final chunk.
    fn documented_body() -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(b"10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n");
        body.extend_from_slice(&[b'a'; 65536]);
        body.extend_from_slice(b"\r\n400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n");
        body.extend_from_slice(&[b'a'; 1024]);
        body.extend_from_slice(b"\r\n0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n");
        body
    }

    fn decode(mut decoder: PayloadDecoder, body: &[u8], piece: usize) -> Result<Vec<u8>, S3Error> {
        let mut out = Vec::new();
        for part in body.chunks(piece) {
            decoder.feed(part, &mut out)?;
        }
        decoder.finish(None)?;
        Ok(out)
    }

    #[test]
    fn the_documented_signed_chunks_decode_in_any_pieces() {
        for piece in [1, 7, 4096, usize::MAX] {
            let decoder = PayloadDecoder::new(signed_framing(), None).unwrap();
            let out = decode(decoder, &documented_body(), piece).unwrap();
            assert_eq!(out, vec![b'a'; 66560], "piece size {piece}");
        }
    }

    #[test]
    fn a_chunk_declaring_more_than_16_mib_is_refused_up_front() {
        let mut decoder = PayloadDecoder::new(signed_framing(), None).unwrap();
        let err = decoder
            .feed(b"1000001;chunk-signature=00\r\n", &mut Vec::new())
            .unwrap_err();
        assert_eq!(err.code, "IncompleteBody");
    }

    #[test]
    fn a_tampered_signed_chunk_is_refused() {
        let mut body = documented_body();
        let at = body.len() - 200;
        body[at] = b'b';
        let decoder = PayloadDecoder::new(signed_framing(), None).unwrap();
        assert_eq!(
            decode(decoder, &body, 4096).unwrap_err().code,
            "SignatureDoesNotMatch"
        );

        let truncated = &documented_body()[..1000];
        let decoder = PayloadDecoder::new(signed_framing(), None).unwrap();
        assert_eq!(
            decode(decoder, truncated, 4096).unwrap_err().code,
            "IncompleteBody"
        );
    }

    fn crc32_b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(crc32fast::hash(bytes).to_be_bytes())
    }

    #[test]
    fn an_unsigned_trailer_body_decodes_and_its_checksum_is_checked() {
        let data = b"hello, trailer";
        let body = |checksum: &str| {
            format!("5\r\nhello\r\n9\r\n, trailer\r\n0\r\nx-amz-checksum-crc32:{checksum}\r\n\r\n")
        };
        let decoder = |_: ()| {
            PayloadDecoder::new(
                Framing::UnsignedTrailer,
                Some(("x-amz-checksum-crc32".to_string(), None)),
            )
            .unwrap()
        };
        let good = body(&crc32_b64(data));
        assert_eq!(decode(decoder(()), good.as_bytes(), 3).unwrap(), data);
        let bad = body(&crc32_b64(b"something else"));
        assert_eq!(
            decode(decoder(()), bad.as_bytes(), 3).unwrap_err().code,
            "BadDigest"
        );
    }

    #[test]
    fn a_plain_body_is_checked_against_its_sha256_and_header_checksum() {
        let data = b"Welcome to Amazon S3.";
        let decoder = |hash: &str| {
            PayloadDecoder::new(
                Framing::Sha256(hash.to_string()),
                Some(("x-amz-checksum-crc32".to_string(), Some(crc32_b64(data)))),
            )
            .unwrap()
        };
        assert_eq!(
            decode(decoder(&sigv4::sha256_hex(data)), data, 5).unwrap(),
            data
        );
        assert_eq!(
            decode(decoder(&sigv4::sha256_hex(b"other")), data, 5)
                .unwrap_err()
                .code,
            "XAmzContentSHA256Mismatch"
        );
        assert!(
            PayloadDecoder::new(
                Framing::Unsigned,
                Some((
                    "x-amz-checksum-crc64nvme".to_string(),
                    Some("x".to_string())
                ))
            )
            .is_err(),
            "an unsupported checksum is refused up front"
        );
    }
}
