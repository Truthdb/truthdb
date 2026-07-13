//! Group commit: many transactions share one WAL fsync.
//!
//! A committing transaction appends its commit record to the WAL *without*
//! fsyncing, then calls [`GroupCommit::ensure_durable`] with the WAL tail past
//! its commit record and blocks on a `flushed` watermark. A dedicated
//! log-writer thread fsyncs the WAL — through a duplicated file descriptor, so
//! it never touches the storage lock — and advances the watermark, waking every
//! committer the fsync covered. One fsync thus makes every commit that landed
//! in the window durable, instead of one fsync per commit.
//!
//! Durability ordering: a committer's WAL page write completes (io_uring
//! completion consumed) before it appends the next record and, later, before it
//! publishes its target under the state mutex. The log-writer reads that target
//! under the same mutex before issuing the fsync, so every write up to a
//! published target is on the device before the fsync flushes it. Crediting
//! `flushed = target` afterwards is therefore sound.

use std::fs::File;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use crate::storage::StorageError;

struct State {
    /// Highest WAL tail any committer is waiting to have fsynced.
    requested: u64,
    /// Highest WAL tail known fsync-durable (advanced by the log-writer).
    flushed: u64,
    /// An fsync failed: every waiter fails and the store must wedge.
    failed: bool,
    /// The store is shutting down: wake everyone and stop.
    shutdown: bool,
    /// Number of fsyncs the log-writer has issued (test observability).
    fsyncs: u64,
}

pub(crate) struct GroupCommit {
    state: Mutex<State>,
    /// Committers wait here for `flushed` to reach their target.
    flushed_cv: Condvar,
    /// The log-writer waits here for a higher `requested`.
    wake_cv: Condvar,
}

impl GroupCommit {
    /// Spawns the log-writer thread over a duplicated WAL file descriptor and
    /// returns the shared coordinator plus the thread's join handle.
    pub(crate) fn start(wal_fd: File) -> (Arc<GroupCommit>, JoinHandle<()>) {
        let gc = Arc::new(GroupCommit {
            state: Mutex::new(State {
                requested: 0,
                flushed: 0,
                failed: false,
                shutdown: false,
                fsyncs: 0,
            }),
            flushed_cv: Condvar::new(),
            wake_cv: Condvar::new(),
        });
        let writer_gc = Arc::clone(&gc);
        let writer = std::thread::Builder::new()
            .name("truthdb-log-writer".to_string())
            .spawn(move || writer_gc.log_writer_loop(wal_fd))
            .expect("spawn log-writer thread");
        (gc, writer)
    }

    /// Blocks until the WAL is fsync-durable up to `target` (a tail past a
    /// committed record). Registers the demand so the log-writer wakes, then
    /// waits on the watermark. Fails if an fsync failed or the store is
    /// shutting down.
    pub(crate) fn ensure_durable(&self, target: u64) -> Result<(), StorageError> {
        let mut state = self.state.lock().expect("group-commit state poisoned");
        if target > state.requested {
            state.requested = target;
            self.wake_cv.notify_one();
        }
        loop {
            if state.failed {
                return Err(StorageError::InvalidFile(
                    "group-commit log-writer failed to fsync the WAL; restart to recover"
                        .to_string(),
                ));
            }
            if state.flushed >= target {
                return Ok(());
            }
            if state.shutdown {
                return Err(StorageError::InvalidFile(
                    "storage is shutting down; commit durability could not be confirmed"
                        .to_string(),
                ));
            }
            state = self
                .flushed_cv
                .wait(state)
                .expect("group-commit state poisoned");
        }
    }

    /// Signals the log-writer to stop and wakes every waiter.
    pub(crate) fn shutdown(&self) {
        let mut state = self.state.lock().expect("group-commit state poisoned");
        state.shutdown = true;
        self.wake_cv.notify_all();
        self.flushed_cv.notify_all();
    }

    /// The number of fsyncs the log-writer has issued so far.
    #[cfg(test)]
    pub(crate) fn fsync_count(&self) -> u64 {
        self.state
            .lock()
            .expect("group-commit state poisoned")
            .fsyncs
    }

    fn log_writer_loop(&self, wal_fd: File) {
        let mut state = self.state.lock().expect("group-commit state poisoned");
        loop {
            while !state.shutdown && !state.failed && state.requested <= state.flushed {
                state = self
                    .wake_cv
                    .wait(state)
                    .expect("group-commit state poisoned");
            }
            if state.shutdown || state.failed {
                return;
            }
            // Snapshot the demand, then release the lock across the (slow) fsync
            // so committers keep appending and registering while it runs.
            let target = state.requested;
            drop(state);
            let result = wal_fd.sync_data();
            state = self.state.lock().expect("group-commit state poisoned");
            state.fsyncs += 1;
            match result {
                Ok(()) => {
                    if target > state.flushed {
                        state.flushed = target;
                    }
                    self.flushed_cv.notify_all();
                }
                Err(_) => {
                    state.failed = true;
                    self.flushed_cv.notify_all();
                    return;
                }
            }
        }
    }
}
