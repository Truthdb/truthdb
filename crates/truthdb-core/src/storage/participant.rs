//! The hooks the shared log calls into the storage engines that own
//! page-structured state (the relational engine, and any engine beside it).
//! Core storage drives open, ARIES restart, checkpoints and standby apply; each
//! engine supplies what only it knows — where its root lives, how to rebuild its
//! caches from durable pages, how to undo its logical (tree) operations, and how
//! to fold a shipped range into its read state. All hooks run under the storage
//! mutex.

use crate::relstore::ctx::{OpMode, RelCtx};
use crate::relstore::recovery::LogicalUndo;
use crate::storage::{StorageError, StorageFile};
use crate::storage_layout::{MAX_ENGINE_ROOTS, PAGE_SIZE};
use crate::wal::records::{PageOpUndo, RelRecord};

/// Builds fresh engines for an open, create or validating reopen. Engine ids
/// must be distinct, and exactly one engine must have [`CATALOG_ENGINE_ID`].
pub type EngineFactory = fn() -> Vec<Box<dyn EngineParticipant>>;

/// The engine whose root is the superblock's `metadata_root`, moved by
/// `SET_CATALOG_ROOT` records (the relational catalog). Every other engine's
/// root lives in the superblock's engine-roots table, moved by
/// `SET_ENGINE_ROOT` records.
pub const CATALOG_ENGINE_ID: u32 = 0;

/// Tree object ids at and above this belong to engines other than the catalog
/// engine, which allocates its own ids below it. Page headers and tree undo
/// records carry the id, so the two ranges must never meet.
pub const FIRST_ENGINE_OBJECT_ID: u32 = 0xF000_0000;

pub trait EngineParticipant: std::any::Any + Send {
    /// This engine's id: [`CATALOG_ENGINE_ID`], or a stable non-zero id that
    /// names its root in the superblock and in `SET_ENGINE_ROOT` records.
    fn engine_id(&self) -> u32;

    /// Whether the tree with this object id is one of this engine's, so
    /// restart recovery hands its logical undo to this engine. Exactly one
    /// engine owns any object id that appears in the log.
    fn owns_object(&self, object_id: u32) -> bool;

    /// Open, before recovery: the engine's root (a data-region page number,
    /// already range-checked) and the database-options byte.
    fn open(&mut self, metadata_root: Option<u64>, db_options: u8);

    /// The engine's root, stamped into the superblock by checkpoints and
    /// restartpoints. `None` for every engine with an empty log means there is
    /// nothing to recover.
    fn metadata_root(&self) -> Option<u64>;

    /// A `SET_CATALOG_ROOT` (catalog engine) or `SET_ENGINE_ROOT` record for
    /// this engine was applied (restart analysis or standby apply); the last
    /// one wins.
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
    /// The core file and the engine of type `T`, borrowed disjointly.
    ///
    /// # Panics
    /// If no engine of type `T` was opened with this storage.
    pub fn engine_parts<T: EngineParticipant>(&mut self) -> (&mut StorageFile, &mut T) {
        let eng = self
            .engines
            .iter_mut()
            .find_map(|eng| eng.as_any_mut().downcast_mut::<T>())
            .unwrap_or_else(|| {
                panic!(
                    "storage was opened without engine {}",
                    std::any::type_name::<T>()
                )
            });
        (&mut self.core, eng)
    }
}

impl crate::storage::Locked {
    /// The catalog engine's root as an absolute file offset (0 = none), for
    /// the superblock's `metadata_root`.
    pub(in crate::storage) fn catalog_root_offset(&self) -> u64 {
        self.engines
            .iter()
            .find(|eng| eng.engine_id() == CATALOG_ENGINE_ID)
            .and_then(|eng| eng.metadata_root())
            .map(|page| self.core.layout.data_offset + page * PAGE_SIZE as u64)
            .unwrap_or(0)
    }

    /// Every other engine's root as `(engine id, absolute file offset)`, for
    /// the superblock's engine-roots table.
    pub(in crate::storage) fn engine_root_offsets(&self) -> Vec<(u32, u64)> {
        self.engines
            .iter()
            .filter(|eng| eng.engine_id() != CATALOG_ENGINE_ID)
            .filter_map(|eng| {
                eng.metadata_root().map(|page| {
                    (
                        eng.engine_id(),
                        self.core.layout.data_offset + page * PAGE_SIZE as u64,
                    )
                })
            })
            .collect()
    }

    /// Whether any engine has a root (an empty log and no root = nothing to
    /// recover).
    pub(in crate::storage) fn any_engine_root(&self) -> bool {
        self.engines.iter().any(|eng| eng.metadata_root().is_some())
    }

    /// Applies the root moves found in a redone range: the catalog root, then
    /// each other engine's (the last record per engine wins). A root for an
    /// engine this storage does not run means the log belongs to a build with
    /// engines this one lacks.
    pub(in crate::storage) fn apply_root_moves(
        &mut self,
        catalog_root: Option<u64>,
        engine_roots: &std::collections::HashMap<u32, u64>,
    ) -> Result<(), StorageError> {
        // Refuse an unknown engine before moving any root, so a failed apply
        // leaves every root where it was.
        for &engine_id in engine_roots.keys() {
            self.engine_by_id(engine_id)?;
        }
        if let Some(root) = catalog_root {
            self.engine_by_id(CATALOG_ENGINE_ID)?
                .set_metadata_root(root);
        }
        for (&engine_id, &root) in engine_roots {
            self.engine_by_id(engine_id)?.set_metadata_root(root);
        }
        Ok(())
    }

    fn engine_by_id(&mut self, engine_id: u32) -> Result<&mut dyn EngineParticipant, StorageError> {
        self.engines
            .iter_mut()
            .find(|eng| eng.engine_id() == engine_id)
            .map(|eng| &mut **eng)
            .ok_or_else(|| {
                StorageError::InvalidFile(format!(
                    "the file holds engine {engine_id}, which this build does not run"
                ))
            })
    }

    /// Rebuilds every engine's caches from durable pages.
    pub(in crate::storage) fn reload_engines(&mut self) -> Result<(), StorageError> {
        for eng in &mut self.engines {
            eng.reload(&mut self.core)?;
        }
        Ok(())
    }

    /// Folds a fully redone range into every engine's standby read state.
    pub(in crate::storage) fn capture_standby_engines(
        &mut self,
        records: &[(u64, RelRecord)],
        floor: u64,
    ) {
        for eng in &mut self.engines {
            eng.capture_standby(&mut self.core, records, floor);
        }
    }
}

/// Checks an [`EngineFactory`]'s output: exactly one catalog engine, distinct
/// ids, and no more other engines than the superblock's engine-roots table
/// holds (a root past it would be dropped at the next checkpoint).
pub(in crate::storage) fn validate_engines(
    engines: &[Box<dyn EngineParticipant>],
) -> Result<(), StorageError> {
    let mut ids: Vec<u32> = engines.iter().map(|eng| eng.engine_id()).collect();
    ids.sort_unstable();
    let catalog = ids.iter().filter(|&&id| id == CATALOG_ENGINE_ID).count();
    if catalog != 1 {
        return Err(StorageError::InvalidConfig(format!(
            "storage needs exactly one catalog engine, got {catalog}"
        )));
    }
    if let Some(pair) = ids.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(StorageError::InvalidConfig(format!(
            "two storage engines share id {}",
            pair[0]
        )));
    }
    if ids.len() - 1 > MAX_ENGINE_ROOTS {
        return Err(StorageError::InvalidConfig(format!(
            "{} non-catalog storage engines; the superblock holds {MAX_ENGINE_ROOTS} roots",
            ids.len() - 1
        )));
    }
    Ok(())
}

/// Restart recovery's logical undo over every engine: each tree undo goes to
/// the engine that owns its object id.
pub(crate) struct RoutedUndo<'a> {
    engines: &'a [Box<dyn EngineParticipant>],
    undos: Vec<Box<dyn LogicalUndo>>,
}

impl<'a> RoutedUndo<'a> {
    pub(crate) fn new(engines: &'a [Box<dyn EngineParticipant>]) -> Self {
        let undos = engines.iter().map(|eng| eng.recovery_undo()).collect();
        RoutedUndo { engines, undos }
    }
}

impl LogicalUndo for RoutedUndo<'_> {
    fn undo(
        &self,
        ctx: &mut RelCtx<'_>,
        mode: &mut OpMode<'_>,
        object_id: u32,
        undo: &PageOpUndo,
    ) -> Result<(), StorageError> {
        let index = self
            .engines
            .iter()
            .position(|eng| eng.owns_object(object_id))
            .ok_or_else(|| {
                StorageError::InvalidFile(format!("no engine owns tree object {object_id}"))
            })?;
        self.undos[index].undo(ctx, mode, object_id, undo)
    }
}
