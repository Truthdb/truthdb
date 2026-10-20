//! The combined engine: relational, object and search over one shared log,
//! the session worker pool, and the native protocol listener (TCP/9623).

pub mod client_listener;
pub mod dispatcher;
pub mod engine;
pub mod session;

// Core modules under their old `crate::` paths, so the moved engine and
// session code (and its tests) keep resolving them unchanged.
#[allow(unused_imports)]
use truthdb_core::{backup, repl, storage_layout, wal};

/// The engines every database file runs, in every open, create, restore and
/// promotion: a file holding one engine's data refuses to open without it.
pub fn database_engines() -> Vec<Box<dyn truthdb_core::storage::EngineParticipant>> {
    vec![
        truthdb_relational::storage::relational_participant(),
        truthdb_object::ObjEngine::boxed(),
        truthdb_vector::VecEngine::boxed(),
    ]
}
#[allow(unused_imports)]
use truthdb_relational::{auth, lock, relstore, storage};
