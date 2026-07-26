use truthdb_sql::error::SqlError;

thread_local! {
    /// The cancellation flag for the batch running on this worker thread — set by
    /// the connection task when a TDS Attention (cancel) arrives. Executor loops
    /// poll it via [`check_cancelled`] so a running statement can be aborted.
    static CANCEL_FLAG: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// Binds a cancellation flag to the current thread for one batch, clearing it on
/// drop so a later batch on the same pooled worker never sees a stale flag.
pub struct CancelScope;

impl CancelScope {
    pub fn enter(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) -> CancelScope {
        CANCEL_FLAG.with(|c| *c.borrow_mut() = Some(flag));
        CancelScope
    }
}

impl Drop for CancelScope {
    fn drop(&mut self) {
        CANCEL_FLAG.with(|c| *c.borrow_mut() = None);
    }
}

/// True if the batch on this thread has been asked to cancel (Attention).
pub(super) fn is_cancelled() -> bool {
    CANCEL_FLAG.with(|c| {
        c.borrow()
            .as_ref()
            .is_some_and(|f| f.load(std::sync::atomic::Ordering::Relaxed))
    })
}

/// Errors if the current batch has been cancelled (TDS Attention). Executor
/// loops call this periodically so a long statement aborts mid-flight. The
/// client is answered with a `DONE(attention)`, not this error — it is an
/// internal marker the batch driver recognises to stop without dooming the txn.
pub fn check_cancelled() -> Result<(), SqlError> {
    if is_cancelled() {
        Err(SqlError::message_only(
            CANCEL_ERROR,
            "The query was canceled.",
        ))
    } else {
        Ok(())
    }
}

/// The error number [`check_cancelled`] raises. The batch driver keys on this
/// (not the raw cancel flag) so a concurrent Attention can't suppress the
/// `XACT_ABORT`/severity dooming of an *unrelated* statement failure.
pub(super) const CANCEL_ERROR: i32 = 3617;

/// Sets the current thread's cancel flag (test helper — execution runs on the
/// calling thread in tests, so this simulates an Attention).
#[cfg(test)]
pub(crate) fn set_test_cancel(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    CANCEL_FLAG.with(|c| *c.borrow_mut() = Some(flag));
}

/// Clears the current thread's cancel flag (test helper — reset before other
/// tests reuse the thread).
#[cfg(test)]
pub(crate) fn clear_test_cancel() {
    CANCEL_FLAG.with(|c| *c.borrow_mut() = None);
}
