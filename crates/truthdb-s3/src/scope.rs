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
//! carried into the tasks it spawns); an engine call outside one fails.

use std::future::Future;

use truthdb_core::storage::StorageError;
use truthdb_object::ObjFile;

use crate::error::S3Error;
use crate::server::S3State;

/// A request's access key, its account, and the buckets it names with the
/// owners they had when it was authorized (`None`: missing).
#[derive(Clone)]
pub struct Scope {
    pub access_key: String,
    pub account: String,
    pub buckets: Vec<(String, Option<String>)>,
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
    match file.access_key(&scope.access_key)? {
        Some((key, _)) if key.active && key.account == scope.account => {}
        _ => return Ok(false),
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
    let scope = current().ok_or_else(|| S3Error::internal("an engine call outside a request"))?;
    state
        .engine
        .run_object(move |file| {
            if !holds(file, &scope)? {
                return Ok(None);
            }
            op(file).map(Some)
        })
        .await
        .map_err(|err| S3Error::internal(err.to_string()))?
        .ok_or_else(|| S3Error::access_denied("Access Denied"))
}
