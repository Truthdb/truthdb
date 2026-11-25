//! RenameObject (`PUT /<key>?renameObject`): a directory bucket's object
//! renamed in place, atomically, its source named by `x-amz-rename-source`.
//! The destination's conditions are the request's own (`If-Match`,
//! `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`); the
//! source's are `x-amz-rename-source-if-*`. A client token
//! (`x-amz-client-token`) makes a retry of a rename that was done a no-op.

use std::collections::HashMap;
use std::sync::Mutex;

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use truthdb_object::VersionSel;

use crate::error::S3Error;
use crate::objects::{DROP_GRACE_MS, header, now_ms, object_error, precondition};
use crate::server::{Body, S3State, full};

/// How long a client token is remembered (AWS states none).
const TOKEN_MS: u64 = 10 * 60 * 1000;
/// Tokens remembered at most, in all and of one account: past either a
/// rename is done but its token not kept (the expired ones dropped first).
/// One account cannot take every other's.
const MAX_TOKENS: usize = 100_000;
const MAX_PER_ACCOUNT: usize = 10_000;

/// What a client token's rename was: a digest of the bucket, key, source
/// and conditions it named (whatever their length).
type Done = [u8; 32];

/// The client tokens of renames done, by account and token, until they
/// expire; and how many each account has.
#[derive(Default)]
pub struct RenameTokens(Mutex<Kept>);

#[derive(Default)]
struct Kept {
    by_token: HashMap<(String, String), (Done, u64)>,
    of_account: HashMap<String, usize>,
    /// When the first of them expires (none: never), so a full store is
    /// scanned only when one has.
    earliest: Option<u64>,
}

impl Kept {
    fn full(&self, account: &str) -> bool {
        self.by_token.len() >= MAX_TOKENS
            || self.of_account.get(account).copied().unwrap_or(0) >= MAX_PER_ACCOUNT
    }

    /// Drops the tokens expired at `now_ms`, uncounting them.
    fn prune(&mut self, now_ms: u64) {
        let Kept {
            by_token,
            of_account,
            earliest,
        } = self;
        *earliest = None;
        by_token.retain(|(account, _), (_, until)| {
            let kept = *until > now_ms;
            if !kept && let Some(count) = of_account.get_mut(account) {
                *count -= 1;
            }
            if kept {
                *earliest = Some(earliest.map_or(*until, |at| at.min(*until)));
            }
            kept
        });
    }
}

/// A client token's verdict on a rename.
enum Seen {
    /// Not seen (or expired): the rename is to be done.
    New,
    /// The same rename, done: a no-op.
    Done,
    /// Another rename under that token.
    Mismatch,
}

impl RenameTokens {
    fn seen(&self, account: &str, token: &str, done: &Done, now_ms: u64) -> Seen {
        let kept = self.0.lock().expect("rename tokens poisoned");
        match kept.by_token.get(&(account.to_string(), token.to_string())) {
            Some((kept, until)) if *until > now_ms => {
                if kept == done {
                    Seen::Done
                } else {
                    Seen::Mismatch
                }
            }
            _ => Seen::New,
        }
    }

    fn keep(&self, account: &str, token: &str, done: Done, now_ms: u64) {
        let mut kept = self.0.lock().expect("rename tokens poisoned");
        if kept.full(account) {
            if kept.earliest.is_some_and(|at| at <= now_ms) {
                kept.prune(now_ms);
            }
            if kept.full(account) {
                return;
            }
        }
        let key = (account.to_string(), token.to_string());
        let until = now_ms + TOKEN_MS;
        kept.earliest = Some(kept.earliest.map_or(until, |at| at.min(until)));
        if kept.by_token.insert(key, (done, until)).is_none() {
            *kept.of_account.entry(account.to_string()).or_default() += 1;
        }
    }
}

fn invalid(message: &str) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// The key `x-amz-rename-source` names in `bucket`: `[/]<bucket>/<key>`, as
/// its pattern has it, or (as AWS's examples also write it) the key alone.
fn source_key(request: &Request<Incoming>, bucket: &str) -> Result<String, S3Error> {
    let raw = header(request.headers(), "x-amz-rename-source")
        .ok_or_else(|| invalid("x-amz-rename-source is required"))?;
    let decoded = crate::sigv4::decode_path(&raw)
        .ok_or_else(|| invalid("x-amz-rename-source is not URL-encoded UTF-8"))?;
    let path = decoded.strip_prefix('/').unwrap_or(&decoded);
    let key = path
        .strip_prefix(bucket)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(path);
    if key.is_empty() {
        return Err(invalid("x-amz-rename-source names no key"));
    }
    Ok(key.to_string())
}

/// The conditions of a rename, as its token's record compares them.
const CONDITIONS: [&str; 8] = [
    "if-match",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
    "x-amz-rename-source-if-match",
    "x-amz-rename-source-if-none-match",
    "x-amz-rename-source-if-modified-since",
    "x-amz-rename-source-if-unmodified-since",
];

/// The digest of a rename's parameters, as its client token keeps them:
/// each one's presence (an empty header is not an absent one), then its
/// length, so none runs into the next.
fn digest(bucket: &str, key: &str, from: &str, headers: &hyper::HeaderMap) -> Done {
    let mut parameters = ring::digest::Context::new(&ring::digest::SHA256);
    let conditions = CONDITIONS.iter().map(|name| header(headers, name));
    for part in [bucket, key, from]
        .into_iter()
        .map(|part| Some(part.to_string()))
        .chain(conditions)
    {
        match part {
            None => parameters.update(&[0]),
            Some(part) => {
                parameters.update(&[1]);
                parameters.update(&(part.len() as u64).to_le_bytes());
                parameters.update(part.as_bytes());
            }
        }
    }
    parameters
        .finish()
        .as_ref()
        .try_into()
        .expect("a SHA-256 digest is 32 bytes")
}

fn failed() -> S3Error {
    S3Error::new(
        412,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
    )
}

/// Renames `bucket`'s `key`'s source to it: 200 with no body.
pub async fn rename_object(
    state: &S3State,
    request: Request<Incoming>,
    account: &str,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let from = source_key(&request, &bucket)?;
    // As AWS: an object ending in the delimiter is not renamed (nor named so).
    if from.ends_with('/') || key.ends_with('/') {
        return Err(S3Error::invalid_request(
            "An object whose key ends with / is not renamed",
        ));
    }
    let token = header(request.headers(), "x-amz-client-token");
    if let Some(token) = &token
        && (token.is_empty()
            || token.len() > 64
            || !token.bytes().all(|byte| (33..=126).contains(&byte)))
    {
        return Err(invalid(
            "x-amz-client-token is 1 to 64 characters of ASCII 33 to 126",
        ));
    }
    let done = digest(&bucket, &key, &from, request.headers());
    let now = now_ms();
    if let Some(token) = &token {
        match state.rename_tokens.seen(account, token, &done, now) {
            Seen::Done => return Ok(ok()),
            Seen::Mismatch => {
                return Err(S3Error::new(
                    400,
                    "IdempotencyParameterMismatch",
                    "Parameters on this idempotent request are inconsistent with parameters used in previous request(s).",
                ));
            }
            Seen::New => {}
        }
    }
    let headers = request.headers().clone();
    let (name, to, source_name) = (bucket.clone(), key.clone(), from.clone());
    crate::scope::object_as(state, "RenameObject", move |file| {
        let source = match file.head_object(&name, &source_name, VersionSel::Latest)? {
            Ok(source) => source,
            Err(err) => return Ok(Err(object_error(err, &name, &source_name))),
        };
        // The source's conditions: any that does not hold fails it (its
        // If-None-Match `*` always does).
        if precondition(&headers, "x-amz-rename-source-", &source).is_some() {
            return Ok(Err(failed()));
        }
        // The destination's: If-Match and If-Modified-Since need it there;
        // If-None-Match (`*`) and If-Unmodified-Since hold without it.
        match file.head_object(&name, &to, VersionSel::Latest)? {
            Ok(current) => {
                if precondition(&headers, "", &current).is_some() {
                    return Ok(Err(failed()));
                }
            }
            // (A date that does not parse is no condition, as when it is there.)
            Err(_) => {
                if header(&headers, "if-match").is_some()
                    || header(&headers, "if-modified-since")
                        .and_then(|date| crate::objects::parse_http_date(&date))
                        .is_some()
                {
                    return Ok(Err(failed()));
                }
            }
        }
        // Onto itself: it is already so.
        if source_name == to {
            return Ok(Ok(()));
        }
        let attrs = file.version_attrs(&name, &source_name, source.version)?;
        let renamed = file.transaction(|tx| {
            tx.rename_object(
                &name,
                (&source_name, &source),
                &attrs,
                &to,
                now_ms(),
                DROP_GRACE_MS,
            )
        })?;
        Ok(renamed
            .map(|_| ())
            .map_err(|err| object_error(err, &name, &to)))
    })
    .await??;
    if let Some(token) = &token {
        state.rename_tokens.keep(account, token, done, now);
    }
    Ok(ok())
}

fn ok() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_is_its_renames_for_ten_minutes_and_an_account_keeps_its_share() {
        let tokens = RenameTokens::default();
        let (one, other) = ([1u8; 32], [2u8; 32]);
        tokens.keep("111122223333", "t", one, 1_000);
        assert!(matches!(
            tokens.seen("111122223333", "t", &one, 1_000),
            Seen::Done
        ));
        assert!(matches!(
            tokens.seen("111122223333", "t", &other, 1_000),
            Seen::Mismatch
        ));
        // Another account's token of the same name is its own.
        assert!(matches!(
            tokens.seen("444455556666", "t", &one, 1_000),
            Seen::New
        ));
        assert!(matches!(
            tokens.seen("111122223333", "t", &one, 1_000 + TOKEN_MS),
            Seen::New
        ));
        // An account's share: past it, its tokens are not kept, another's
        // are; the expired ones go to make room.
        // One that outlives the prune below.
        tokens.keep("111122223333", "mid", one, 1_000 + TOKEN_MS / 2);
        for n in 1..MAX_PER_ACCOUNT {
            tokens.keep("111122223333", &n.to_string(), one, 1_000);
        }
        tokens.keep("111122223333", "full", one, 1_000);
        assert!(matches!(
            tokens.seen("111122223333", "full", &one, 1_000),
            Seen::New
        ));
        tokens.keep("444455556666", "theirs", one, 1_000);
        assert!(matches!(
            tokens.seen("444455556666", "theirs", &one, 1_000),
            Seen::Done
        ));
        // Nothing had expired until then: the store kept the first expiry,
        // and scanned only once it came.
        tokens.keep("111122223333", "later", one, 1_000 + TOKEN_MS);
        assert!(matches!(
            tokens.seen("111122223333", "later", &one, 1_000 + TOKEN_MS),
            Seen::Done
        ));
        let earliest = tokens.0.lock().unwrap().earliest;
        assert_eq!(earliest, Some(1_000 + TOKEN_MS / 2 + TOKEN_MS));
    }

    #[test]
    fn a_renames_digest_tells_an_empty_header_from_an_absent_one() {
        let mut headers = hyper::HeaderMap::new();
        let absent = digest("b", "k", "s", &headers);
        headers.insert("if-match", hyper::header::HeaderValue::from_static(""));
        assert_ne!(digest("b", "k", "s", &headers), absent);
        // Nor does one part run into the next.
        let none = hyper::HeaderMap::new();
        assert_ne!(digest("b", "ks", "", &none), digest("b", "k", "s", &none));
    }
}
