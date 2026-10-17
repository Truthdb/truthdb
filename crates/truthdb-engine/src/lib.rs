//! The combined engine: relational and search over one shared log, the
//! session worker pool, and the native protocol listener (TCP/9623).

pub mod client_listener;
pub mod dispatcher;
pub mod engine;
pub mod session;

// Core modules under their old `crate::` paths, so the moved engine and
// session code (and its tests) keep resolving them unchanged.
#[allow(unused_imports)]
use truthdb_core::{backup, repl, storage_layout, wal};
#[allow(unused_imports)]
use truthdb_relational::{auth, lock, relstore, storage};
