use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

use crate::allocator::{EXTENT_PAGES, PageAllocator};
use crate::direct_io::{AlignedPageBuf, DirectFile};
use crate::group_commit::GroupCommit;
use crate::relstore::buffer_pool::DEFAULT_CAPACITY_BYTES;
use crate::relstore::ctx::{PageState, PoolIo, RelCtx};
use crate::relstore::recovery as rel_recovery;
use crate::storage_layout::{
    FileHeader, PAGE_SIZE, SNAPSHOT_DESCRIPTOR_SIZE, SUPERBLOCK_ACTIVE_A, SUPERBLOCK_ACTIVE_B,
    SnapshotDescriptor, Superblock, WAL_ENTRY_HEADER_SIZE, WAL_ENTRY_TYPE_REL, WAL_MAX_BYTES,
    WAL_MIN_BYTES, WalEntryHeader, align_down, assert_layout_invariants, wal_entry_padded_len,
};
use crate::wal::records::{
    REL_KIND_ALLOC_EXTENT, REL_KIND_FREE_EXTENT, REL_KIND_SET_CATALOG_ROOT,
    REL_KIND_SET_ENGINE_ROOT, RelRecord,
};
use crate::wal::{WalWriter, scan_ring};

pub use crate::wal::WalRecord;

mod allocation;
mod backup_restore;
mod checkpoint;
mod durability;
mod lifecycle;
mod participant;
mod recovery;
mod replication;

#[cfg(test)]
use lifecycle::compute_layout;
use lifecycle::live_descriptor_slot;
pub use participant::{
    CATALOG_ENGINE_ID, EngineFactory, EngineParticipant, FIRST_ENGINE_OBJECT_ID,
};
use participant::{RoutedUndo, validate_engines};
use replication::SyncCommitState;

/// Version stamped in REL wal entries (entry-level, distinct from the record
/// kinds inside). v2 adds a commit-record timestamp for point-in-time restore;
/// v1 records decode unchanged (nothing gates on the version).
const REL_WAL_ENTRY_VERSION: u16 = 2;

#[derive(Debug, Clone)]
pub struct StorageOptions {
    pub size_gib: u64,
    pub wal_ratio: f64,
    pub metadata_ratio: f64,
    pub snapshot_ratio: f64,
    pub allocator_ratio: f64,
    pub reserved_ratio: f64,
    /// The database's default collation: what a character column declared
    /// without an explicit `COLLATE` gets. `None` uses the built-in default.
    ///
    /// It is stamped into the file at creation and read back on open, never
    /// taken from the running config, because it decides the sort-key bytes of
    /// every column that inherited it — changing it under existing data would
    /// silently invalidate their keys.
    pub default_collation: Option<String>,
}

impl StorageOptions {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.size_gib == 0 {
            return Err(StorageError::InvalidConfig(
                "storage.size_gib must be > 0".to_string(),
            ));
        }
        for (name, value) in [
            ("wal_ratio", self.wal_ratio),
            ("metadata_ratio", self.metadata_ratio),
            ("snapshot_ratio", self.snapshot_ratio),
            ("allocator_ratio", self.allocator_ratio),
            ("reserved_ratio", self.reserved_ratio),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(StorageError::InvalidConfig(format!(
                    "storage.{name} must be between 0.0 and 1.0"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageLayout {
    pub total_size: u64,
    pub header_offset: u64,
    pub superblock_a_offset: u64,
    pub superblock_b_offset: u64,
    pub wal_offset: u64,
    pub wal_size: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub metadata_offset: u64,
    pub metadata_size: u64,
    pub allocator_offset: u64,
    pub allocator_size: u64,
    pub snapshot_offset: u64,
    pub snapshot_size: u64,
    pub reserved_offset: u64,
    pub reserved_size: u64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("invalid storage file: {0}")]
    InvalidFile(String),

    #[error("wal ring full: {0}")]
    WalFull(String),

    #[error("constraint violation: {0}")]
    Constraint(String),

    /// A SNAPSHOT transaction touched a table whose schema a later-committed
    /// DDL changed (its version images cannot decode under the new schema).
    /// Maps to SQL Server's 3961.
    #[error("schema of '{0}' changed under the snapshot")]
    SnapshotSchemaChange(String),

    /// A full backup was requested while one is already running. Only one
    /// backup may hold the WAL truncation gate's single backup slot at a time.
    #[error("a backup is already in progress")]
    BackupInProgress,
}

/// Thread-safe handle to the storage engine. All mutable state lives in a
/// [`StorageFile`] behind a mutex, so `Storage` is `Send + Sync` and its methods
/// take `&self`: a worker pool can share one `Arc<Storage>`. Each public method
/// locks once for the duration of its operation (coarse, per-operation locking;
/// finer-grained latches arrive in a later stage). `path` is kept outside the
/// mutex so [`Storage::path`] can hand back a borrow.
pub struct Storage {
    path: PathBuf,
    inner: std::sync::Mutex<Locked>,
    /// Group-commit coordinator: commits register their WAL tail here and wait
    /// for the log-writer to fsync past it. Shared with the log-writer thread.
    gc: Arc<GroupCommit>,
    /// D2 synchronous commit (primary side): when armed, a commit additionally
    /// waits — after LOCAL durability — for a standby's `FlushAck` to cover its
    /// target, with an availability-first timeout (see [`SyncCommitState`]).
    sync_commit: SyncCommitState,
    /// Node ids with a live replication sender (one connection per id; the
    /// monitoring DMVs report connectedness from here).
    repl_connected: std::sync::Mutex<std::collections::HashSet<u32>>,
    /// The log-writer thread's join handle, taken in `Drop` after signalling it.
    log_writer: Option<JoinHandle<()>>,
}

// The point of the mutex: `Storage` is shareable across worker threads. Assert
// it at compile time so a future non-`Send` field is caught here.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Storage>();
};

impl Drop for Storage {
    fn drop(&mut self) {
        // Stop the log-writer and wait for it to exit before the WAL file (and
        // its duplicated fd) go away.
        self.gc.shutdown();
        if let Some(writer) = self.log_writer.take() {
            let _ = writer.join();
        }
    }
}

/// Opaque handle to a stored row, addressing it for a targeted UPDATE/DELETE.
pub struct SnapshotData {
    pub data: Vec<u8>,
    pub checkpoint_seq: u64,
    pub next_seq_no: u64,
    pub next_doc_id: u64,
}

/// Holds that pin the WAL ring's truncation floor below what a checkpoint would
/// otherwise reclaim, so a subsystem that still needs a stretch of log keeps it.
/// The floor is `min` over all active holds; the active-transaction hold (the
/// oldest open `BEGIN` LSN) is computed separately from `active_txn_begins` and
/// combined in [`StorageFile::checkpoint_wal_head`]. Built for Stage 17 backup
/// and reused by Stage 18 replication slots.
#[derive(Debug, Default)]
struct LogTruncationGate {
    /// An in-progress full backup's `redo_start_lsn` — the log it must ship
    /// before the ring can reclaim it. `None` when no backup is running.
    backup: Option<u64>,
    /// The FULL-recovery-model log-backup floor: `last_log_backup_lsn`, the log
    /// past which nothing has been shipped to a log archive yet. Held so a
    /// checkpoint cannot truncate log a future `BACKUP LOG` still owes. `None`
    /// in the SIMPLE model (log is reclaimable as soon as it is checkpointed).
    log_backup: Option<u64>,
    /// Replication slots (id → held LSN): each pins the ring at the LSN a standby
    /// has received, so the primary keeps log the standby still needs. Re-seeded
    /// from the superblock on open; persisted at each checkpoint.
    repl_slots: std::collections::BTreeMap<u32, u64>,
}

impl LogTruncationGate {
    /// The lowest LSN any hold pins, or `None` if no hold is registered.
    fn min_hold(&self) -> Option<u64> {
        [self.backup, self.log_backup]
            .into_iter()
            .flatten()
            .chain(self.repl_slots.values().copied())
            .min()
    }
}

pub struct StorageFile {
    /// Handle for data-region, superblock and descriptor I/O.
    file: DirectFile,
    /// WAL writer with its own dedicated file handle, so log writes do not
    /// serialize behind page flushes.
    wal: WalWriter,
    /// Holds that keep the WAL ring from truncating log a backup (or, later, a
    /// replication slot) still needs.
    truncation_gate: LogTruncationGate,
    /// A replication slot lagging the WAL tail by more than this is invalidated
    /// (dropped) at the next checkpoint so the ring can advance — the standby
    /// must then reseed. `u64::MAX` (the default) = unlimited retention: a slot
    /// holds truncation until explicitly dropped, matching the backup/log-backup
    /// holds (a deployment configures a finite cap to protect the primary).
    ///
    /// A meaningful finite cap must be strictly BELOW the ring's usable capacity
    /// (`wal_size - wal.reserve()`): a pinned slot keeps the tail within that
    /// capacity of its LSN (appends stall with `WalFull` first), so the reap
    /// window `tail - lsn > cap` can never open at or above it — the primary
    /// would wedge rather than shed the slot. The setter (test-only here; the
    /// transport slice wires the real one) must reject/clamp to that bound.
    max_slot_retain_bytes: u64,
    /// The standby's own active-transaction table over the SHIPPED log:
    /// txn id → BEGIN LSN, inserted as TXN_BEGIN records are applied and
    /// removed at TXN_COMMIT/TXN_END — the same resolution rules recovery's
    /// analysis uses. Its minimum is the restartpoint's undo floor: everything
    /// below it is resolved, so a promotion's analysis+undo never needs log
    /// past it. Computed HERE, at the standby's own applied position — a floor
    /// computed on the primary describes the primary's tail, not the shipped
    /// prefix, and could truncate undo of a transaction whose resolution has
    /// not shipped yet. Seeded at open from the same records recovery scans.
    standby_att: std::collections::HashMap<u64, u64>,
    /// The first ring LSN of a search-subsystem (`entry_type == 1`) record the
    /// seed snapshot does not cover: a restartpoint must not advance the head
    /// past it, or a reopen's search replay would lose the event (a standby
    /// writes no search snapshots — a locally allocated snapshot extent would
    /// collide with the primary's future logged allocations).
    standby_search_floor: Option<u64>,
    /// The seed snapshot's `next_seq_no` (0 = no snapshot): search records at
    /// or above it are NOT covered and pin `standby_search_floor`.
    snapshot_next_seq_no: u64,
    /// FULL-model log-backup floor (mirrors the active superblock's
    /// `last_log_backup_lsn`): the LSN up to which the log has been shipped to
    /// a log archive. `0` in the SIMPLE model / before the first log backup.
    last_log_backup_lsn: u64,
    /// Single-flight guard: set for the duration of a `BACKUP LOG` (which
    /// releases the storage lock while writing the archive), so a second one
    /// cannot begin from the same marker and produce an overlapping archive.
    log_backup_in_progress: bool,
    /// FULL recovery model (vs SIMPLE, the default): bit 2 of the
    /// superblock's database-options byte. Drives the log-backup hold.
    recovery_full: bool,
    layout: StorageLayout,
    superblock_a: Superblock,
    superblock_b: Superblock,
    active_superblock: ActiveSuperblock,
    allocator: PageAllocator,
    /// Buffer pool, dirty-page table and transaction counters.
    pages: PageState,
    /// WAL records recovered at open, waiting for the engine to replay them.
    replay_cache: Vec<WalRecord>,
    /// The database's default collation, read from the file header at open. A
    /// character column declared without an explicit `COLLATE` is resolved to
    /// this at CREATE TABLE and stored with it, so the column keeps the
    /// collation it was created under even if the default later changes.
    default_collation: Option<String>,
}

/// What the storage mutex guards: the core file next to the page-structured
/// engine's in-memory state. One mutex, so a statement's page changes and its
/// version publishing (and a checkpoint's flush and catalog-root stamp) stay
/// one critical section. Derefs to the core file.
pub struct Locked {
    core: StorageFile,
    engines: Vec<Box<dyn EngineParticipant>>,
}

impl std::ops::Deref for Locked {
    type Target = StorageFile;
    fn deref(&self) -> &StorageFile {
        &self.core
    }
}

impl std::ops::DerefMut for Locked {
    fn deref_mut(&mut self) -> &mut StorageFile {
        &mut self.core
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveSuperblock {
    A,
    B,
}

impl ActiveSuperblock {
    fn from_superblocks(a: &Superblock, b: &Superblock, a_valid: bool, b_valid: bool) -> Self {
        match (a_valid, b_valid) {
            (true, true) => {
                if b.generation > a.generation {
                    ActiveSuperblock::B
                } else {
                    ActiveSuperblock::A
                }
            }
            (true, false) => ActiveSuperblock::A,
            (false, true) => ActiveSuperblock::B,
            (false, false) => ActiveSuperblock::A,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
