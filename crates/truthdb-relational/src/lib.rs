//! The relational engine: T-SQL execution over clustered B+ trees, heaps,
//! secondary indexes and a row-version store, kept on the shared log.

pub mod auth;
pub mod engine;
pub mod lock;
pub mod relstore;
pub mod storage;

// Shared-log modules under their `crate::` paths, so the moved relational
// code keeps resolving them unchanged.
#[allow(unused_imports)]
use truthdb_core::{allocator, backup, repl, storage_layout, wal};
