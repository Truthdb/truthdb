//! The hooks the shared log calls into the storage engine that owns the
//! page-structured state (today: the relational engine). Core storage drives
//! open, ARIES restart, checkpoints and standby apply; the engine supplies what
//! only it knows — where its catalog root lives, how to rebuild its caches from
//! durable pages, how to undo its logical (tree) operations, and how to fold a
//! shipped range into its read state. All hooks run under the storage mutex.

use crate::relstore::recovery::LogicalUndo;
use crate::storage::{StorageError, StorageFile};
use crate::wal::records::RelRecord;

/// Builds a fresh engine for an open, create or validating reopen.
pub(super) type EngineFactory = fn() -> Box<dyn EngineParticipant>;

pub(super) trait EngineParticipant: std::any::Any + Send {
    /// Open, before recovery: the superblock's catalog root (a data-region
    /// page number, already range-checked) and its database-options byte.
    fn open(&mut self, metadata_root: Option<u64>, db_options: u8);

    /// The catalog root, stamped into the superblock by checkpoints and
    /// restartpoints. `None` with an empty log means there is nothing to recover.
    fn metadata_root(&self) -> Option<u64>;

    /// A `SET_CATALOG_ROOT` record was applied (restart analysis or standby
    /// apply); the last one wins.
    fn set_metadata_root(&mut self, page: u64);

    /// The engine's bits of the database-options byte.
    fn options_byte(&self) -> u8;

    /// Rebuilds in-memory caches from durable pages: after redo, after loser
    /// undo, and after each standby apply.
    fn reload(&mut self, core: &mut StorageFile) -> Result<(), StorageError>;

    /// The logical undo restart recovery uses for loser transactions, with the
    /// tree roots as of this point.
    fn recovery_undo(&self) -> Box<dyn LogicalUndo>;

    /// Readable standby: folds a fully redone range into the engine's read
    /// state; `floor` is the LSN up to which the log is now folded in.
    fn capture_standby(&mut self, core: &mut StorageFile, records: &[(u64, RelRecord)], floor: u64);

    /// End of a restart recovery that ran (not the empty-store early-out).
    fn finish_recovery(&mut self);

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
