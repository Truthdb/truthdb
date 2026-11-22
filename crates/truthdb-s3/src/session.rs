//! CreateSession (`GET /<bucket>?session`): a directory bucket's session
//! credentials (an access key id, a secret and a token, for five minutes),
//! kept in memory: requests signed with them carry the token in
//! `x-amz-s3session-token`, and are allowed what the session's mode allows
//! in its bucket.

use std::collections::HashMap;
use std::sync::Mutex;

use base64::Engine as _;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use ring::rand::SecureRandom;

use crate::authz::Authz;
use crate::error::{S3Error, escape};
use crate::server::{Body, Principal, S3State, full};

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// The header a session's requests carry its token in.
pub const TOKEN_HEADER: &str = "x-amz-s3session-token";

/// How long a session's credentials last: "Temporary security credentials
/// are scoped to the bucket and expire after 5 minutes."
pub const LIFETIME_MS: u64 = 5 * 60 * 1000;

/// A session: whose it is, of which bucket, whether ReadOnly, and until
/// when.
#[derive(Debug, Clone)]
pub struct Session {
    pub secret: String,
    pub token: String,
    pub bucket: String,
    pub principal: Principal,
    pub read_only: bool,
    pub expires_ms: u64,
}

/// The sessions not yet expired, by access key id.
#[derive(Default)]
pub struct Sessions(Mutex<HashMap<String, Session>>);

fn random(bytes: usize) -> Vec<u8> {
    let mut out = vec![0; bytes];
    ring::rand::SystemRandom::new()
        .fill(&mut out)
        .expect("the system's random numbers");
    out
}

/// Why a session's credentials were refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    Unknown,
    Expired,
    BadToken,
}

impl Sessions {
    /// A new session of `bucket` for `principal`, until five minutes after
    /// `now_ms` (the expired ones are dropped).
    pub fn create(
        &self,
        bucket: &str,
        principal: &Principal,
        read_only: bool,
        now_ms: u64,
    ) -> (String, Session) {
        let base64 = base64::engine::general_purpose::STANDARD;
        // As an access key id: upper case letters and digits.
        let id: String = random(16)
            .iter()
            .map(|byte| b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"[(*byte & 31) as usize] as char)
            .collect();
        let session = Session {
            secret: base64.encode(random(30)),
            token: base64.encode(random(48)),
            bucket: bucket.to_string(),
            principal: principal.clone(),
            read_only,
            expires_ms: now_ms + LIFETIME_MS,
        };
        let mut sessions = self.0.lock().expect("sessions poisoned");
        sessions.retain(|_, kept| kept.expires_ms > now_ms);
        let id = format!("TDBS{id}");
        sessions.insert(id.clone(), session.clone());
        (id, session)
    }

    /// The session of access key `id`, if `token` is its token and it has
    /// not expired at `now_ms`.
    pub fn verify(&self, id: &str, token: &str, now_ms: u64) -> Result<Session, Refused> {
        let sessions = self.0.lock().expect("sessions poisoned");
        let session = sessions.get(id).ok_or(Refused::Unknown)?;
        // Compared in constant time (as tags of one key).
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, id.as_bytes());
        let kept = ring::hmac::sign(&key, session.token.as_bytes());
        if ring::hmac::verify(&key, token.as_bytes(), kept.as_ref()).is_err() {
            return Err(Refused::BadToken);
        }
        if session.expires_ms <= now_ms {
            return Err(Refused::Expired);
        }
        Ok(session.clone())
    }
}

impl From<Refused> for S3Error {
    fn from(refused: Refused) -> S3Error {
        match refused {
            Refused::Unknown => S3Error::invalid_access_key(),
            Refused::Expired => {
                S3Error::new(400, "ExpiredToken", "The provided token has expired.")
            }
            Refused::BadToken => S3Error::new(
                400,
                "InvalidToken",
                "The provided token is malformed or otherwise invalid.",
            ),
        }
    }
}

/// CreateSession: a session of directory bucket `bucket`, of the mode
/// `x-amz-create-session-mode` asks for (none: ReadWrite if the caller may
/// have it, else ReadOnly), as `s3express:CreateSession` allows it.
pub async fn create_session(
    state: &S3State,
    request: &Request<Incoming>,
    authz: &Authz,
    principal: &Principal,
    bucket: &str,
) -> Result<Response<Body>, S3Error> {
    let modes: &[bool] =
        match crate::objects::header(request.headers(), "x-amz-create-session-mode").as_deref() {
            None => &[false, true],
            Some("ReadWrite") => &[false],
            Some("ReadOnly") => &[true],
            Some(_) => {
                return Err(S3Error::new(
                    400,
                    "InvalidArgument",
                    "x-amz-create-session-mode is ReadOnly or ReadWrite",
                ));
            }
        };
    match crate::objects::header(request.headers(), "x-amz-server-side-encryption").as_deref() {
        None | Some("AES256") => {}
        Some("aws:kms" | "aws:kms:dsse") => {
            return Err(S3Error::not_implemented("SSE-KMS is not supported"));
        }
        Some(_) => {
            return Err(S3Error::new(
                400,
                "InvalidArgument",
                "The server-side encryption is AES256 or aws:kms",
            ));
        }
    }
    let mut allowed = Err(S3Error::access_denied("Access Denied"));
    for read_only in modes {
        allowed = authz.check_session(bucket, *read_only).map(|()| *read_only);
        if allowed.is_ok() {
            break;
        }
    }
    let read_only = allowed?;
    let (id, session) =
        state
            .sessions
            .create(bucket, principal, read_only, crate::objects::now_ms());
    let body = format!(
        "<CreateSessionResult xmlns=\"{XMLNS}\"><Credentials><SessionToken>{}</SessionToken><SecretAccessKey>{}</SecretAccessKey><AccessKeyId>{}</AccessKeyId><Expiration>{}</Expiration></Credentials></CreateSessionResult>",
        escape(&session.token),
        escape(&session.secret),
        escape(&id),
        crate::time::iso8601(session.expires_ms)
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_is_its_tokens_for_five_minutes() {
        let sessions = Sessions::default();
        let principal = Principal {
            access_key: "AK".into(),
            account: "111122223333".into(),
            user: None,
        };
        let (id, session) = sessions.create("b--use1-az4--x-s3", &principal, true, 1_000);
        assert!(id.starts_with("TDBS") && id.len() == 20, "{id}");
        let kept = sessions.verify(&id, &session.token, 1_000).unwrap();
        assert_eq!(
            (kept.bucket.as_str(), kept.read_only),
            ("b--use1-az4--x-s3", true)
        );
        assert_eq!(
            sessions.verify(&id, "other", 1_000).unwrap_err(),
            Refused::BadToken
        );
        assert_eq!(
            sessions
                .verify("TDBSNONE", &session.token, 1_000)
                .unwrap_err(),
            Refused::Unknown
        );
        assert!(
            sessions
                .verify(&id, &session.token, 1_000 + LIFETIME_MS - 1)
                .is_ok()
        );
        assert_eq!(
            sessions
                .verify(&id, &session.token, 1_000 + LIFETIME_MS)
                .unwrap_err(),
            Refused::Expired
        );
        // Expired ones are dropped when another is made.
        sessions.create("b--use1-az4--x-s3", &principal, false, 1_000 + LIFETIME_MS);
        assert_eq!(
            sessions.verify(&id, &session.token, 0).unwrap_err(),
            Refused::Unknown
        );
    }
}
