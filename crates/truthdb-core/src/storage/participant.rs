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
pub type EngineFactory = fn() -> Box<dyn EngineParticipant>;

pub trait EngineParticipant: std::any::Any + Send {
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

/// What the storage core offers an engine running under the storage mutex.
impl StorageFile {
    /// The database's default collation, as stamped into the file header.
    pub fn default_collation(&self) -> Option<String> {
        self.default_collation.clone()
    }

    /// Whether the database is in the FULL recovery model.
    pub fn recovery_full(&self) -> bool {
        self.recovery_full
    }

    /// The WAL's direct-sync durable watermark (group commit is tracked
    /// separately, see [`crate::storage::Storage::group_commit_flushed`]).
    pub fn wal_flushed(&self) -> u64 {
        self.wal.flushed_lsn()
    }

    /// Fsyncs the WAL.
    pub fn wal_sync_all(&mut self) -> Result<(), StorageError> {
        self.wal.sync_all()
    }

    /// Hands out the next transaction id.
    pub fn alloc_txn_id(&mut self) -> u64 {
        let txn_id = self.pages.next_txn_id;
        self.pages.next_txn_id += 1;
        txn_id
    }

    /// Records an open explicit transaction's BEGIN LSN, so checkpoints keep
    /// its undo records in the ring.
    pub fn register_active_txn(&mut self, txn_id: u64, begin_lsn: u64) {
        self.pages.active_txn_begins.insert(txn_id, begin_lsn);
    }

    /// The explicit transaction ended (committed or rolled back).
    pub fn end_active_txn(&mut self, txn_id: u64) {
        self.pages.active_txn_begins.remove(&txn_id);
    }

    /// Whether any explicit transaction is open.
    pub fn has_active_txns(&self) -> bool {
        !self.pages.active_txn_begins.is_empty()
    }

    /// Marks the store wedged: the pool may hold state the WAL cannot explain,
    /// so nothing may proceed (above all no checkpoint) until a restart.
    pub fn set_wedged(&mut self) {
        self.pages.wedged = true;
    }

    /// Flushes the buffer pool (and fsyncs the data file) without a
    /// checkpoint, leaving the WAL head where it is (tests of the crash
    /// window where torn pages are possible but their FPIs are still in the log).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn flush_pool_only(&mut self) -> Result<(), StorageError> {
        self.wal.sync_all()?;
        let crate::relstore::ctx::PageState { pool, .. } = &mut self.pages;
        let mut io = crate::relstore::ctx::PoolIo {
            file: &mut self.file,
            wal: &mut self.wal,
            data_offset: self.layout.data_offset,
            data_pages: self.layout.data_size / crate::storage_layout::PAGE_SIZE as u64,
        };
        pool.flush_all(&mut io)?;
        self.file.sync_data()?;
        Ok(())
    }

    /// The data region's size in pages (tests).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn data_pages(&self) -> u64 {
        self.layout.data_size / crate::storage_layout::PAGE_SIZE as u64
    }

    /// The in-flight online backup's truncation hold, if any (tests).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn backup_hold(&self) -> Option<u64> {
        self.truncation_gate.backup
    }

    /// The page allocator (tests that stage allocations directly).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn allocator_mut(&mut self) -> &mut crate::allocator::PageAllocator {
        &mut self.allocator
    }
}

impl crate::storage::Locked {
    /// The core file and the engine, borrowed disjointly.
    pub fn parts(&mut self) -> (&mut StorageFile, &mut dyn EngineParticipant) {
        (&mut self.core, &mut *self.eng)
    }
}
