//! The engine worker pool: a bank of OS threads shares an `Arc<Engine>` and a
//! [`Scheduler`] (sessions + lock table + parked queue behind one mutex) and
//! serves [`EngineCall`]s off a channel. Workers hold the scheduler mutex only
//! to make lock decisions (acquire / park / release / wake); a batch's actual
//! execution runs with the mutex *released*, so non-conflicting batches run
//! concurrently. Per-connection session state (transaction / isolation) lives
//! in the [`SessionManager`], and the synchronous io_uring work runs off the
//! async reactor on these threads.
//!
//! ## Locking without blocking a worker
//!
//! A worker must never block in place waiting for a lock — the lock's holder
//! could only release by having its own work processed, and while workers exist
//! to do that, a batch that parked mid-execution could not be restarted
//! cleanly. Instead a batch acquires *all* the table/database locks it needs up
//! front (see [`crate::engine::analyze_locks`]) before running any statement, so a
//! running batch never blocks on a lock. If a lock conflicts, the whole
//! [`EngineCall::RunBatch`] is *parked* — its reply deferred — and the worker
//! moves on. Releasing locks (commit / rollback / disconnect) makes parked
//! batches grantable; the releasing worker drains them in FIFO order, running
//! each. Since a parked batch never ran, restarting it is exact.
//!
//! Because a running batch never waits on a lock, only *parked* batches can
//! form a lock-wait cycle. A deadlock is broken by a waits-for-graph cycle
//! detector that runs the instant a parking batch closes a cycle: the youngest
//! transaction in the cycle is rolled back as the victim (error 1205). A 5 s
//! per-wait deadline remains as a backstop for any stall the graph does not
//! model.
//!
//! ## Thread-safety of shared state
//!
//! Two locks, always taken in this order (never the reverse), so they cannot
//! deadlock: the **scheduler** mutex (lock decisions) may briefly take the
//! **storage** mutex under it (catalog lookup in `analyze_locks`, rollback in
//! `abort`); batch execution takes only storage (and the engine's execution
//! gate), never the scheduler. See [`Engine`] for the execution gate that keeps
//! the native path from observing a relational batch's torn writes.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use truthdb_sql::error::SqlError;

use crate::engine::{BatchOutcome, Isolation, ResultColumn, RowSet, StatementResult, TxnContext};
use crate::engine::{Engine, EngineError};
use crate::lock::{LockManager, LockMode, Resource};
use crate::relstore::types::Datum;

mod api;
mod dispatch;
mod handle;
mod inbox;
mod pool;
mod reply;
mod scheduler;
mod state;

pub use api::{BatchEvent, BatchReply, LoginRecord, PreparedRpc, SessionId};
pub use handle::EngineHandle;
pub use pool::spawn_engine;
pub use reply::BatchSink;

use api::EVENT_ROWS;
use dispatch::{bind_decl_names, dispatch_batch, dispatch_rpc, drain_ready};
use inbox::{EngineCall, HandleToken, Inbox, ProcRpcTail, Work};
use pool::{MIN_SWEEP_INTERVAL, Shared};
use reply::collect_reply;
use scheduler::{Parked, Runnable, Scheduler};
use state::SessionManager;

#[cfg(test)]
use pool::{
    LOCK_WAIT_TIMEOUT, MAINTENANCE_STARTS, maintenance_loop, spawn_engine_pool,
    spawn_engine_with_idle_timeout, spawn_engine_with_timeout,
};
#[cfg(test)]
use scheduler::find_cycle;

#[cfg(test)]
mod tests;
