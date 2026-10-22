//! Who a request acts for, checked in every engine call it makes.
//!
//! A request is authenticated when its head arrives, but its operation runs
//! later, after its body: a key dropped or a bucket deleted and created again
//! by another account meanwhile must not let the operation through. So each
//! engine call of a request, in the same storage-lock hold as its work,
//! checks that the request's access key is still there and active, and that
//! every bucket the request names has the owner it had when the request was
//! authorized (or is still missing).
//!
//! The scope is task-local, set by `api::dispatch` around the operation (and
//! carried into the tasks it spawns); an engine call outside one fails. It
//! also holds the request's origin, which the changes it makes carry in the
//! change journal.

use std::future::Future;

use truthdb_core::storage::StorageError;
use truthdb_object::{ObjFile, Origin};

use crate::error::S3Error;
use crate::server::{Connection, Principal, RequestIds, S3State};

/// A request's access key and its account (none for an anonymous request),
/// and the buckets it names with the owners they had when it was authorized
/// (`None`: missing).
#[derive(Clone)]
pub struct Scope {
    pub key: Option<(String, String)>,
    pub buckets: Vec<(String, Option<String>)>,
    /// The request's origin (its operation named by the engine call that
    /// makes a change); `None` for requests that change no object.
    pub origin: Option<Origin>,
}

/// The origin of a request's changes: its request id (`ids`, from its
/// extensions), its principal's account and user, and the address it came
/// from.
pub fn origin(
    ids: Option<&RequestIds>,
    connection: Option<Connection>,
    principal: Option<&Principal>,
) -> Origin {
    Origin {
        operation: String::new(),
        request_id: ids.map(|ids| ids.request_id.clone()).unwrap_or_default(),
        account: principal.map(|principal| principal.account.clone()),
        user: principal.and_then(|principal| principal.user.clone()),
        source_ip: connection.map(|connection| connection.peer.ip().to_canonical().to_string()),
    }
}

tokio::task_local! {
    static SCOPE: Scope;
}

/// Runs `future` within `scope`.
pub async fn within<F: Future>(scope: Scope, future: F) -> F::Output {
    SCOPE.scope(scope, future).await
}

/// The current request's scope, to carry into a spawned task.
pub fn current() -> Option<Scope> {
    SCOPE.try_with(Scope::clone).ok()
}

/// Whether `scope` still holds: its key exists, is active and acts for its
/// account, and its buckets have the owners they had.
fn holds(file: &mut ObjFile<'_>, scope: &Scope) -> Result<bool, StorageError> {
    if let Some((access_key, account)) = &scope.key {
        match file.access_key(access_key)? {
            Some((key, _)) if key.active && &key.account == account => {}
            _ => return Ok(false),
        }
    }
    for (bucket, owner) in &scope.buckets {
        if file.bucket(bucket)?.map(|info| info.owner) != *owner {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Runs `op` on the object engine for the current request, after checking
/// its scope in the same lock hold: AccessDenied when it no longer holds.
pub async fn object<T, F>(state: &S3State, op: F) -> Result<T, S3Error>
where
    T: Send + 'static,
    F: FnOnce(&mut ObjFile<'_>) -> Result<T, StorageError> + Send + 'static,
{
    object_as(state, "", op).await
}

/// [`object`] for an engine call that changes objects as S3 operation
/// `operation` (`PutObject`, ...): the changes carry it, with the rest of the
/// request's origin, in the change journal.
pub async fn object_as<T, F>(state: &S3State, operation: &str, op: F) -> Result<T, S3Error>
where
    T: Send + 'static,
    F: FnOnce(&mut ObjFile<'_>) -> Result<T, StorageError> + Send + 'static,
{
    let scope = current().ok_or_else(|| S3Error::internal("an engine call outside a request"))?;
    let origin = scope.origin.clone().map(|origin| Origin {
        operation: operation.to_string(),
        ..origin
    });
    state
        .engine
        .run_object(move |file| {
            if !holds(file, &scope)? {
                return Ok(None);
            }
            if let Some(origin) = origin {
                file.set_origin(origin);
            }
            op(file).map(Some)
        })
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .ok_or_else(|| S3Error::access_denied("Access Denied"))
}
