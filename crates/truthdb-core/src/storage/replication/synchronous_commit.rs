use super::super::*;

/// D2 synchronous-commit coordination (primary side). Committers wait on
/// `acked` (a standby's highest acknowledged durable LSN, published by the
/// sender's ack reader) after their local fsync. Availability-first: a wait
/// that exceeds the armed timeout degrades the link (`degraded = true`,
/// logged once) and every commit passes straight through until an
/// acknowledgement catches up to the primary's durable watermark, which
/// re-synchronizes the link (logged once).
pub(in crate::storage) struct SyncCommitState {
    armed: std::sync::atomic::AtomicBool,
    timeout_ms: std::sync::atomic::AtomicU64,
    degraded: std::sync::atomic::AtomicBool,
    /// The LSN whose acknowledgement re-synchronizes a degraded link: the
    /// target of the wait that timed out. Comparing against the LIVE durable
    /// watermark instead would latch the degradation forever under sustained
    /// writes — an ack always trails the live watermark by one round trip.
    resync_target: std::sync::atomic::AtomicU64,
    acked: std::sync::Mutex<u64>,
    acked_cv: std::sync::Condvar,
}

impl Default for SyncCommitState {
    fn default() -> Self {
        SyncCommitState {
            armed: std::sync::atomic::AtomicBool::new(false),
            timeout_ms: std::sync::atomic::AtomicU64::new(0),
            degraded: std::sync::atomic::AtomicBool::new(false),
            resync_target: std::sync::atomic::AtomicU64::new(0),
            acked: std::sync::Mutex::new(0),
            acked_cv: std::sync::Condvar::new(),
        }
    }
}

impl SyncCommitState {
    fn arm(&self, timeout: std::time::Duration) {
        self.timeout_ms.store(
            timeout.as_millis().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.armed.store(true, std::sync::atomic::Ordering::Release);
    }

    fn publish(&self, lsn: u64) {
        use std::sync::atomic::Ordering;
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        {
            let mut acked = self.acked.lock().expect("sync-commit state poisoned");
            if lsn > *acked {
                *acked = lsn;
            }
        }
        self.acked_cv.notify_all();
        // Re-synchronize once the acknowledgement covers the wait that
        // degraded the link — the incident point, NOT the live watermark
        // (which a loaded primary keeps permanently ahead of any ack).
        if self.degraded.load(Ordering::Acquire)
            && lsn >= self.resync_target.load(Ordering::Acquire)
            && self.degraded.swap(false, Ordering::AcqRel)
        {
            eprintln!(
                "synchronous commit: a standby caught up (acknowledged {lsn}); the link is \
                 SYNCHRONIZED again"
            );
        }
    }

    /// Waits for an acknowledgement covering `target`, honoring the
    /// availability-first timeout. Never returns an error: degradation is a
    /// logged state change, not a commit failure.
    pub(in crate::storage) fn wait_for_ack(&self, target: u64) {
        use std::sync::atomic::Ordering;
        if !self.armed.load(Ordering::Acquire) || self.degraded.load(Ordering::Acquire) {
            return;
        }
        let timeout =
            std::time::Duration::from_millis(self.timeout_ms.load(Ordering::Relaxed).max(1));
        let deadline = std::time::Instant::now() + timeout;
        let mut acked = self.acked.lock().expect("sync-commit state poisoned");
        while *acked < target {
            // A concurrent timeout already degraded the link: stop waiting.
            if self.degraded.load(Ordering::Acquire) {
                return;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                self.resync_target.store(target, Ordering::Release);
                if !self.degraded.swap(true, Ordering::AcqRel) {
                    eprintln!(
                        "synchronous commit: no standby acknowledged LSN {target} within \
                         {timeout:?} — the link is NOT_SYNCHRONIZED; commits proceed on \
                         local durability alone until a standby catches up"
                    );
                }
                // Wake concurrently parked committers: the link is degraded,
                // they must stop waiting too.
                drop(acked);
                self.acked_cv.notify_all();
                return;
            }
            let (guard, _timed_out) = self
                .acked_cv
                .wait_timeout(acked, deadline - now)
                .expect("sync-commit state poisoned");
            acked = guard;
        }
    }
}
impl Storage {
    /// Arms D2 synchronous commit: every commit waits (after local durability)
    /// for a standby `FlushAck` at or past its target. `timeout` is the
    /// availability-first knob: a commit that waits longer marks the link
    /// NOT_SYNCHRONIZED and proceeds — as do all commits after it — until a
    /// standby's acknowledgements catch back up to the durable watermark.
    pub fn arm_sync_commit(&self, timeout: std::time::Duration) {
        self.sync_commit.arm(timeout);
    }

    /// Whether the synchronous-commit link is degraded (NOT_SYNCHRONIZED).
    #[cfg(test)]
    pub(crate) fn sync_commit_degraded(&self) -> bool {
        self.sync_commit
            .degraded
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Publishes a standby's acknowledged durable LSN (the sender's ack path
    /// calls this beside the slot advance). Re-synchronizes the link when the
    /// acknowledgement has caught up to the primary's durable watermark.
    pub(crate) fn publish_sync_ack(&self, acked: u64) {
        self.sync_commit.publish(acked);
    }

    /// Synchronous-commit status: `None` when not armed, else whether the link
    /// is currently degraded (NOT_SYNCHRONIZED).
    pub(crate) fn sync_commit_status(&self) -> Option<bool> {
        use std::sync::atomic::Ordering;
        self.sync_commit
            .armed
            .load(Ordering::Acquire)
            .then(|| self.sync_commit.degraded.load(Ordering::Acquire))
    }
}
