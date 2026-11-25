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
/// Tokens remembered at most: past it a rename is done but its token not
/// kept (the expired ones dropped first).
const MAX_TOKENS: usize = 100_000;

/// What a client token's rename was: the account that made it, and the
/// bucket, key, source and conditions it named.
type Done = (String, String, String, String, String);

/// The client tokens of renames done, until they expire.
#[derive(Default)]
pub struct RenameTokens(Mutex<HashMap<(String, String), (Done, u64)>>);

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
        let tokens = self.0.lock().expect("rename tokens poisoned");
        match tokens.get(&(account.to_string(), token.to_string())) {
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
        let mut tokens = self.0.lock().expect("rename tokens poisoned");
        if tokens.len() >= MAX_TOKENS {
            tokens.retain(|_, (_, until)| *until > now_ms);
            if tokens.len() >= MAX_TOKENS {
                return;
            }
        }
        tokens.insert(
            (account.to_string(), token.to_string()),
            (done, now_ms + TOKEN_MS),
        );
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
    let conditions: String = CONDITIONS
        .iter()
        .map(|name| header(request.headers(), name).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    let done: Done = (
        account.to_string(),
        bucket.clone(),
        key.clone(),
        from.clone(),
        conditions,
    );
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
            Err(_) => {
                if header(&headers, "if-match").is_some()
                    || header(&headers, "if-modified-since").is_some()
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
