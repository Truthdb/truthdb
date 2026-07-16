use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

use crate::allocator::{EXTENT_PAGES, PageAllocator};
use crate::direct_io::{AlignedPageBuf, DirectFile};
use crate::group_commit::GroupCommit;
use crate::relstore::RelState;
use crate::relstore::btree::{BTree, ScanCursor, TreeInsert};
use crate::relstore::buffer_pool::DEFAULT_CAPACITY_BYTES;
use crate::relstore::catalog::{self, FIRST_USER_OBJECT_ID, IndexDef, TableDef};
use crate::relstore::ctx::{OpMode, PoolIo, RelCtx, TxnLink};
use crate::relstore::heap::{Heap, Rid};
use crate::relstore::index::{self, Locator};
use crate::relstore::key::encode_key;
use crate::relstore::recovery as rel_recovery;
use crate::relstore::row::{Column, Schema, decode_row, decode_row_projected, encode_row};
use crate::relstore::types::{Datum, TypeError};
use crate::relstore::version::{
    PendingVersion, ReadSnapshot, Resolved, RowChange, decode_rid_identity, rid_identity,
};
use crate::storage_layout::{
    FileHeader, PAGE_SIZE, SNAPSHOT_DESCRIPTOR_SIZE, SUPERBLOCK_ACTIVE_A, SUPERBLOCK_ACTIVE_B,
    SnapshotDescriptor, Superblock, WAL_ENTRY_TYPE_REL, WAL_MAX_BYTES, WAL_MIN_BYTES, align_down,
    assert_layout_invariants,
};
use crate::wal::records::{REL_KIND_ALLOC_EXTENT, REL_KIND_FREE_EXTENT, RelRecord};
use crate::wal::{WalWriter, scan_ring};

pub use crate::wal::WalRecord;

impl From<TypeError> for StorageError {
    fn from(err: TypeError) -> Self {
        StorageError::InvalidConfig(err.0)
    }
}

/// Version stamped in REL wal entries (entry-level, distinct from the record
/// kinds inside).
const REL_WAL_ENTRY_VERSION: u16 = 1;

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
}

/// Thread-safe handle to the storage engine. All mutable state lives in a
/// [`StorageFile`] behind a mutex, so `Storage` is `Send + Sync` and its methods
/// take `&self`: a worker pool can share one `Arc<Storage>`. Each public method
/// locks once for the duration of its operation (coarse, per-operation locking;
/// finer-grained latches arrive in a later stage). `path` is kept outside the
/// mutex so [`Storage::path`] can hand back a borrow.
pub struct Storage {
    path: PathBuf,
    inner: std::sync::Mutex<StorageFile>,
    /// Group-commit coordinator: commits register their WAL tail here and wait
    /// for the log-writer to fsync past it. Shared with the log-writer thread.
    gc: Arc<GroupCommit>,
    /// The log-writer thread's join handle, taken in `Drop` after signalling it.
    log_writer: Option<JoinHandle<()>>,
    /// `READ_COMMITTED_SNAPSHOT` / `ALLOW_SNAPSHOT_ISOLATION` mirrors of the
    /// version store's options, readable without the storage mutex (lock
    /// analysis and the per-statement snapshot gate are on the hot path).
    rcsi: std::sync::atomic::AtomicBool,
    allow_snapshot: std::sync::atomic::AtomicBool,
    /// Bumped whenever the options change: a parked batch whose lock set was
    /// analyzed under an older epoch is re-analyzed before it can be granted
    /// (its versioned-read decision may no longer match execution).
    lock_epoch: std::sync::atomic::AtomicU64,
    /// Scan slices read, so a test can prove a scan stopped early rather than
    /// reading the table and discarding the rest. On the instance and not in a
    /// `static`: the suite runs in parallel in one binary, so a static would
    /// count every other test's scans as well as this one's.
    #[cfg(test)]
    scan_slices: std::sync::atomic::AtomicUsize,
    /// Times a SELECT took the row-at-a-time path, so a test comparing it with
    /// the collecting path can prove it actually ran — an A/B whose two sides
    /// are the same code agrees with itself. Per-instance for the same reason.
    #[cfg(test)]
    scan_selects: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    covering_scans: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    scan_materializations: std::sync::atomic::AtomicUsize,
    /// Columns the last scan slice asked for (`usize::MAX` = the whole row), so
    /// a test can prove the planner pruned the projection. The rows returned are
    /// identical either way, so nothing else can see the difference.
    #[cfg(test)]
    last_scan_width: std::sync::atomic::AtomicUsize,
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

/// Decodes a row, honouring a caller's projection: `None` means every column.
///
/// The read paths take `Option<&[usize]>` rather than always being handed a
/// full list, so a caller that wants the whole row neither builds one nor pays
/// to walk it.
fn decode_projected(
    schema: &Schema,
    row: &[u8],
    projection: Option<&[usize]>,
) -> Result<Vec<Datum>, crate::relstore::types::TypeError> {
    match projection {
        Some(projection) => decode_row_projected(schema, row, projection),
        None => decode_row(schema, row),
    }
}

impl Storage {
    fn lock(&self) -> std::sync::MutexGuard<'_, StorageFile> {
        self.inner.lock().expect("storage mutex poisoned")
    }

    /// Wraps an opened/created [`StorageFile`] in a `Storage`, spawning the
    /// group-commit log-writer over a duplicated WAL fd.
    fn with_log_writer(path: PathBuf, file: StorageFile) -> Result<Self, StorageError> {
        let wal_fd = file.try_clone_wal_fd()?;
        let (gc, log_writer) = GroupCommit::start(wal_fd);
        let rcsi = file.version.rcsi;
        let allow_snapshot = file.version.allow_snapshot;
        Ok(Storage {
            path,
            inner: std::sync::Mutex::new(file),
            gc,
            log_writer: Some(log_writer),
            rcsi: std::sync::atomic::AtomicBool::new(rcsi),
            allow_snapshot: std::sync::atomic::AtomicBool::new(allow_snapshot),
            lock_epoch: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            scan_slices: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            scan_selects: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            covering_scans: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            scan_materializations: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            last_scan_width: std::sync::atomic::AtomicUsize::new(usize::MAX),
        })
    }

    /// Scan slices this store has read.
    #[cfg(test)]
    pub(crate) fn scan_slices(&self) -> usize {
        self.scan_slices.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// SELECTs this store has answered on the row-at-a-time path.
    #[cfg(test)]
    pub(crate) fn scan_selects(&self) -> usize {
        self.scan_selects.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Index scans answered from the leaves alone (covering, no base lookup).
    #[cfg(test)]
    pub(crate) fn covering_scans(&self) -> usize {
        self.covering_scans
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Lazily-scanned sources drained WHOLE (`SourceRows::materialize` on a
    /// scan): what the join operators do, and what the streamed input path
    /// must NOT do.
    #[cfg(test)]
    pub(crate) fn scan_materializations(&self) -> usize {
        self.scan_materializations
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Counts one whole-scan drain (called by `SourceRows::materialize`).
    #[cfg(test)]
    pub(crate) fn count_scan_materialization(&self) {
        self.scan_materializations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Columns the last scan slice decoded per row (`usize::MAX` = every one).
    #[cfg(test)]
    pub(crate) fn last_scan_width(&self) -> usize {
        self.last_scan_width
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Counts one row-at-a-time SELECT (called by `rel::scan_select`).
    #[cfg(test)]
    pub(crate) fn count_scan_select(&self) {
        self.scan_selects
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        assert_layout_invariants();
        let file = StorageFile::open_existing(path.clone())?;
        Self::with_log_writer(path, file)
    }

    pub fn create(path: PathBuf, opts: StorageOptions) -> Result<Self, StorageError> {
        Self::create_with_wal_bounds(path, opts, WAL_MIN_BYTES, WAL_MAX_BYTES)
    }

    /// Test hook: create with custom WAL ring bounds so ring-wrap paths can
    /// be exercised without writing hundreds of MiB.
    pub(crate) fn create_with_wal_bounds(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
    ) -> Result<Self, StorageError> {
        assert_layout_invariants();
        opts.validate()?;
        let file = StorageFile::create_new(path.clone(), opts, wal_min_bytes, wal_max_bytes)?;
        Self::with_log_writer(path, file)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Blocks until the WAL is fsync-durable up to `target` — the tail past a
    /// committed record. The executor calls this once per batch that committed,
    /// so one log-writer fsync makes many concurrent commits durable.
    pub(crate) fn ensure_durable(&self, target: u64) -> Result<(), StorageError> {
        self.gc.ensure_durable(target)
    }

    /// The current WAL tail — the durability target for a batch that committed.
    pub(crate) fn wal_tail(&self) -> u64 {
        self.lock().wal_tail()
    }

    /// Wedges the relational store after a durability (fsync) failure so no
    /// further op serves state the log does not back. See `ensure_rel_usable`.
    pub(crate) fn wedge(&self) {
        self.lock().wedge();
    }

    #[cfg(test)]
    pub(crate) fn group_commit_fsyncs(&self) -> u64 {
        self.gc.fsync_count()
    }

    pub fn append_wal_entry(
        &self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        payload: &[u8],
    ) -> Result<u64, StorageError> {
        self.lock()
            .append_wal_entry(entry_type, entry_version, seq_no, payload)
    }

    pub fn replay_wal_entries(&self) -> Result<Vec<WalRecord>, StorageError> {
        self.lock().replay_wal_entries()
    }

    pub fn write_checkpoint(
        &self,
        data: &[u8],
        checkpoint_seq: u64,
        next_seq_no: u64,
        next_doc_id: u64,
    ) -> Result<(), StorageError> {
        self.lock()
            .write_checkpoint(data, checkpoint_seq, next_seq_no, next_doc_id)
    }

    /// Writes a checkpoint if the WAL is at least `threshold` full. This is a
    /// *fuzzy* checkpoint: it may run with open explicit transactions — it
    /// flushes their (uncommitted) pages under the steal policy and clamps the
    /// WAL head to the oldest open transaction's begin LSN, so their undo records
    /// survive a crash. Decided and written under one lock hold (a transaction
    /// cannot `begin`, changing the oldest begin LSN, in the window between the
    /// clamp computation and the truncation). Returns whether it wrote.
    pub fn checkpoint_if_wal_full(
        &self,
        data: &[u8],
        checkpoint_seq: u64,
        next_seq_no: u64,
        next_doc_id: u64,
        threshold: f64,
    ) -> Result<bool, StorageError> {
        let mut file = self.lock();
        // A wedged store's in-memory state is ahead of the durable log after a
        // failed fsync; checkpointing would flush and re-fsync exactly the data
        // whose durability failed (and was reported to the client as failed).
        if file.rel.wedged || file.wal_usage_ratio() < threshold {
            return Ok(false);
        }
        file.write_checkpoint(data, checkpoint_seq, next_seq_no, next_doc_id)?;
        Ok(true)
    }

    pub fn load_snapshot(&self) -> Result<Option<SnapshotData>, StorageError> {
        self.lock().load_snapshot()
    }

    pub fn wal_usage_ratio(&self) -> f64 {
        self.lock().wal_usage_ratio()
    }

    pub fn allocate_extent(&self, temp: bool) -> Result<u64, StorageError> {
        self.lock().allocate_extent(temp)
    }

    pub fn free_extent(&self, start_page: u64) -> Result<(), StorageError> {
        self.lock().free_extent(start_page)
    }

    /// Writes one raw page (`PAGE_SIZE` bytes) to a data-region page — used by
    /// the spill spool over temp extents. Bypasses the buffer pool and the WAL
    /// (spill pages are query-scratch, never recovered).
    pub(crate) fn spill_write_page(&self, page: u64, data: &[u8]) -> Result<(), StorageError> {
        self.lock().spill_write_page(page, data)
    }

    /// Reads one raw data-region page (`PAGE_SIZE` bytes) into `out`.
    pub(crate) fn spill_read_page(&self, page: u64, out: &mut [u8]) -> Result<(), StorageError> {
        self.lock().spill_read_page(page, out)
    }

    pub fn is_page_allocated(&self, page: u64) -> bool {
        self.lock().is_page_allocated(page)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rel_create_table(
        &self,
        name: &str,
        columns: Vec<Column>,
        key_names: &[String],
        defaults: Vec<Option<String>>,
        identity: Option<catalog::IdentitySpec>,
        check_constraints: Vec<catalog::CheckDef>,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_table(
            name,
            columns,
            key_names,
            defaults,
            identity,
            check_constraints,
            foreign_keys,
        )
    }

    pub fn rel_create_view(&self, name: &str, query_text: &str) -> Result<(), StorageError> {
        self.lock().rel_create_view(name, query_text)
    }

    pub fn rel_table(&self, name: &str) -> Option<TableDef> {
        self.lock().rel_table(name)
    }

    /// The database's default collation, as stamped into the file at creation.
    /// `None` means the built-in default. A character column declared without an
    /// explicit `COLLATE` is resolved to this at CREATE TABLE and stored with
    /// it, so a column keeps the collation it was created under even if a later
    /// database is created with a different default.
    pub fn default_collation(&self) -> Option<String> {
        self.lock().default_collation.clone()
    }

    pub fn rel_tables(&self) -> Vec<TableDef> {
        self.lock().rel_tables()
    }

    pub fn rel_drop_table(&self, name: &str) -> Result<bool, StorageError> {
        self.lock().rel_drop_table(name)
    }

    pub(crate) fn rel_create_index(
        &self,
        table: &str,
        index_name: String,
        columns: Vec<(usize, bool)>,
        unique: bool,
        include: Vec<usize>,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_create_index(table, index_name, columns, unique, include)
    }

    pub(crate) fn rel_drop_index(
        &self,
        table: &str,
        index_name: &str,
    ) -> Result<bool, StorageError> {
        self.lock().rel_drop_index(table, index_name)
    }

    pub(crate) fn rel_alter_add_column(
        &self,
        table: &str,
        column: Column,
        default_text: Option<String>,
        fill: Datum,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_alter_add_column(table, column, default_text, fill)
    }

    /// The table's committed row count, when it has a counter page (tables
    /// created before counters existed do not — the planner then applies no
    /// tie-break). Errors degrade to `None`: the count is a statistic, never
    /// load-bearing for results.
    pub(crate) fn rel_row_count(&self, table: &str) -> Option<u64> {
        self.lock().rel_row_count(table)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rel_index_scan(
        &self,
        table: &str,
        index_object_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        projection: Option<&[usize]>,
        covering: bool,
        snapshot: Option<ReadSnapshot>,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        #[cfg(test)]
        if covering {
            self.covering_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.lock().rel_index_scan(
            table,
            index_object_id,
            lower,
            upper,
            projection,
            covering,
            snapshot,
        )
    }

    /// Whether `READ_COMMITTED_SNAPSHOT` is on (readable without the storage
    /// mutex — checked per statement and during lock analysis).
    pub(crate) fn rcsi_enabled(&self) -> bool {
        self.rcsi.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether `ALLOW_SNAPSHOT_ISOLATION` is on.
    pub(crate) fn snapshot_isolation_allowed(&self) -> bool {
        self.allow_snapshot
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Applies `ALTER DATABASE SET` option changes: updates the version
    /// store, persists the options in the superblocks, and refreshes the
    /// lock-free mirrors. The caller holds Database X, so no snapshot is live
    /// and no writer is mid-transaction.
    pub(crate) fn rel_set_db_options(
        &self,
        rcsi: Option<bool>,
        allow_snapshot: Option<bool>,
    ) -> Result<(), StorageError> {
        let mut guard = self.lock();
        guard.set_db_options(rcsi, allow_snapshot)?;
        let (rcsi_now, allow_now) = (guard.version.rcsi, guard.version.allow_snapshot);
        drop(guard);
        self.rcsi
            .store(rcsi_now, std::sync::atomic::Ordering::Relaxed);
        self.allow_snapshot
            .store(allow_now, std::sync::atomic::Ordering::Relaxed);
        // After the mirrors, so a batch analyzed against a stale epoch is
        // always re-analyzed against the settled options.
        self.lock_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// The lock-analysis epoch: parked batches analyzed under an older value
    /// are re-analyzed before grant (see the scheduler).
    pub(crate) fn lock_analysis_epoch(&self) -> u64 {
        self.lock_epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Captures a read snapshot: the durable commit prefix as of now, plus
    /// the session's own open transaction. Registered so pruning cannot drop
    /// versions the snapshot may still need; the caller MUST pair this with
    /// [`Self::release_read_snapshot`].
    pub(crate) fn capture_read_snapshot(&self, own_txn: Option<u64>) -> ReadSnapshot {
        let durable = self.gc.flushed();
        let mut guard = self.lock();
        // Whichever watermark is ahead: the group-commit fsync or a direct
        // WAL sync (rollbacks, checkpoints) — both are durability floors.
        let durable = durable.max(guard.wal.flushed_lsn());
        let seq = guard.version.durable_seq(durable);
        guard.version.register_snapshot(seq);
        ReadSnapshot { seq, own_txn }
    }

    pub(crate) fn release_read_snapshot(&self, seq: u64) {
        self.lock().version.release_snapshot(seq);
    }

    /// Atomic snapshot scan: the whole table under one storage-lock hold
    /// (a versioned reader holds no table lock, so a sliced cursor could be
    /// restructured under it mid-walk), merged against the version store.
    pub(crate) fn rel_scan_snapshot(
        &self,
        name: &str,
        projection: Option<&[usize]>,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.lock().rel_scan_snapshot(name, projection, snapshot)
    }

    /// Drops version history no live snapshot can need (runs on the
    /// maintenance thread; cheap when nothing is versioned).
    pub(crate) fn version_prune(&self) {
        let durable = self.gc.flushed();
        let mut guard = self.lock();
        let durable = durable.max(guard.wal.flushed_lsn());
        let fallback = guard.version.durable_seq(durable);
        let watermark = guard.version.watermark(fallback);
        let alive: std::collections::HashSet<u32> =
            guard.rel.tables.values().map(|def| def.object_id).collect();
        guard.version.prune(watermark, &alive);
    }

    /// Test observability: version chains held for `table`.
    #[cfg(test)]
    pub(crate) fn version_chain_count(&self, table: &str) -> usize {
        let guard = self.lock();
        guard
            .rel
            .tables
            .get(table)
            .map_or(0, |def| guard.version.chain_count(def.object_id))
    }

    pub fn rel_insert(&self, name: &str, values: Vec<Datum>) -> Result<(), StorageError> {
        self.lock().rel_insert(name, values)
    }

    pub(crate) fn rel_insert_many(
        &self,
        name: &str,
        rows: Vec<Vec<Datum>>,
        scope: &mut TxnScope,
    ) -> Result<(), StorageError> {
        self.lock().rel_insert_many(name, rows, scope)
    }

    pub fn rel_get(
        &self,
        name: &str,
        key_values: &[Datum],
    ) -> Result<Option<Vec<Datum>>, StorageError> {
        self.lock().rel_get(name, key_values)
    }

    pub fn rel_scan(&self, name: &str) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.lock().rel_scan(name)
    }

    /// Scans a table in bounded slices, dropping the storage lock between them,
    /// so one large read stops blocking every other session for its whole
    /// duration.
    ///
    /// Only for readers that hold the table's lock — which a SELECT does at
    /// every isolation level except READ UNCOMMITTED, whose whole contract is
    /// that it sees in-flight change. Nothing pins a page between slices, so a
    /// concurrent writer could restructure the tree; the cursor checks each
    /// resumed page still belongs to the table and stops rather than read
    /// another object's rows. The integrity checks (FK probes, WITH CHECK) keep
    /// using [`Self::rel_scan`], whose single lock hold makes them atomic — a
    /// validation that missed a row because a page split mid-walk would admit a
    /// violating row.
    pub fn rel_scan_sliced(
        &self,
        name: &str,
        budget: usize,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        let mut out = Vec::new();
        let mut cursor = ScanCursor::start();
        while !cursor.done() {
            cursor = self.rel_scan_slice(name, cursor, budget, None, &mut out)?;
        }
        Ok(out)
    }

    /// One slice of a scan: reads up to `budget` rows from `cursor`, appends
    /// them to `out`, and returns where to resume (`done()` once the table is
    /// exhausted).
    ///
    /// The storage lock is taken for this call alone, so a caller that loops
    /// lets other sessions in between slices. That is [`Self::rel_scan_sliced`]
    /// with the loop handed to the caller — for a reader that consumes rows as
    /// it goes rather than wanting them all at once — and it carries the same
    /// contract: only for readers holding the table's lock, since nothing pins
    /// a page between slices.
    pub(crate) fn rel_scan_slice(
        &self,
        name: &str,
        cursor: ScanCursor,
        budget: usize,
        projection: Option<&[usize]>,
        out: &mut Vec<Vec<Datum>>,
    ) -> Result<ScanCursor, StorageError> {
        #[cfg(test)]
        {
            self.scan_slices
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.last_scan_width.store(
                projection.map_or(usize::MAX, <[usize]>::len),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        self.lock()
            .rel_scan_slice(name, cursor, budget, projection, out)
    }

    /// Test hook: a table's definition + schema, for driving a batched scan.
    #[cfg(test)]
    pub(crate) fn rel_def_for_test(
        &self,
        name: &str,
    ) -> Result<
        (
            crate::relstore::catalog::TableDef,
            crate::relstore::row::Schema,
        ),
        StorageError,
    > {
        self.lock().rel_def(name)
    }

    /// Test hook: runs `f` against a page context, taking the storage lock for
    /// that call only — the shape a batched scan uses, one acquisition per
    /// slice rather than one across the whole table.
    #[cfg(test)]
    pub(crate) fn with_rel_ctx_for_test<R>(
        &self,
        f: impl FnOnce(&mut crate::relstore::ctx::RelCtx<'_>) -> R,
    ) -> R {
        let mut guard = self.lock();
        let mut ctx = guard.rel_ctx();
        f(&mut ctx)
    }

    pub fn rel_delete_where(
        &self,
        name: &str,
        column: &str,
        value: &Datum,
    ) -> Result<usize, StorageError> {
        self.lock().rel_delete_where(name, column, value)
    }

    pub fn rel_update_where(
        &self,
        name: &str,
        column: &str,
        value: &Datum,
        assignments: &[(String, Datum)],
    ) -> Result<usize, StorageError> {
        self.lock()
            .rel_update_where(name, column, value, assignments)
    }

    pub(crate) fn rel_scan_located(
        &self,
        name: &str,
    ) -> Result<Vec<(RowLocator, Vec<Datum>)>, StorageError> {
        self.lock().rel_scan_located(name)
    }

    /// SNAPSHOT-isolation DML target scan: snapshot rows plus a conflict mark
    /// per row whose current state a snapshot-invisible writer produced.
    pub(crate) fn rel_scan_located_snapshot(
        &self,
        name: &str,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, StorageError> {
        self.lock().rel_scan_located_snapshot(name, snapshot)
    }

    pub(crate) fn rel_delete_located(
        &self,
        name: &str,
        targets: Vec<(RowLocator, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.lock().rel_delete_located(name, targets, scope)
    }

    pub(crate) fn rel_update_located(
        &self,
        name: &str,
        updates: Vec<(RowLocator, Vec<Datum>, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.lock().rel_update_located(name, updates, scope)
    }

    pub(crate) fn rel_begin(&self) -> Result<StorageTxn, StorageError> {
        self.lock().rel_begin()
    }

    pub(crate) fn rel_commit(&self, txn: StorageTxn) -> Result<(), StorageError> {
        self.lock().rel_commit(txn)
    }

    pub(crate) fn rel_rollback(&self, txn: StorageTxn) -> Result<(), StorageError> {
        self.lock().rel_rollback(txn)
    }

    /// Captures a savepoint in a caller-held transaction (`SAVE TRANSACTION`).
    pub(crate) fn rel_savepoint(&self, txn: &StorageTxn) -> crate::relstore::ctx::Savepoint {
        txn.txn.savepoint()
    }

    /// Rolls a caller-held transaction back to a savepoint (`ROLLBACK
    /// TRANSACTION <name>`), undoing only the work done since; the transaction
    /// stays open.
    pub(crate) fn rel_rollback_to(
        &self,
        txn: &mut StorageTxn,
        savepoint: crate::relstore::ctx::Savepoint,
    ) -> Result<(), StorageError> {
        self.lock().rollback_txn_to(txn, savepoint)
    }

    #[cfg(test)]
    pub(crate) fn has_active_transactions(&self) -> bool {
        self.lock().has_active_transactions()
    }

    pub(crate) fn rel_reserve_identity(
        &self,
        name: &str,
        count: usize,
    ) -> Result<Option<i64>, StorageError> {
        self.lock().rel_reserve_identity(name, count)
    }

    pub(crate) fn rel_set_check_constraints(
        &self,
        name: &str,
        check_constraints: Vec<catalog::CheckDef>,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_set_check_constraints(name, check_constraints)
    }

    pub(crate) fn rel_set_foreign_keys(
        &self,
        name: &str,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.lock().rel_set_foreign_keys(name, foreign_keys)
    }

    #[cfg(test)]
    pub(crate) fn rel_insert_without_commit(
        &self,
        name: &str,
        values: Vec<Datum>,
    ) -> Result<(), StorageError> {
        self.lock().rel_insert_without_commit(name, values)
    }

    #[cfg(test)]
    pub(crate) fn rel_flush_pool_only(&self) -> Result<(), StorageError> {
        self.lock().rel_flush_pool_only()
    }

    /// Test hook: the relational WAL records currently recoverable from the
    /// ring, exactly as restart's analysis would see them.
    #[cfg(test)]
    pub(crate) fn rel_wal_records(
        &self,
    ) -> Result<Vec<(u64, crate::wal::records::RelRecord)>, StorageError> {
        self.lock().rel_records()
    }

    #[cfg(test)]
    pub(crate) fn data_page_offset(&self, page: u64) -> u64 {
        self.lock().data_page_offset(page)
    }
}

/// Inserts one row's entries into every secondary index. A duplicate on a
/// UNIQUE index surfaces as a constraint error the SQL layer maps to 2601.
fn index_insert_row(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    schema: &Schema,
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let include = if index.include.is_empty() {
            None
        } else {
            Some(
                index::encode_include(schema, &index.include, values)
                    .map_err(|err| StorageError::InvalidConfig(err.0))?,
            )
        };
        let (key, value) = index::leaf_entry(&index_key, locator, index.unique, include.as_deref());
        let tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &key, &value)? {
            TreeInsert::Inserted => {}
            TreeInsert::DuplicateKey => {
                return Err(StorageError::Constraint(format!(
                    "duplicate unique index '{}'",
                    index.name
                )));
            }
        }
    }
    Ok(())
}

/// Reindexes a set of updated rows: deletes every old entry first, then
/// inserts every new one, so a UNIQUE index tolerates value swaps within one
/// statement.
fn apply_index_updates(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    schema: &Schema,
    collations: &[Option<String>],
    ops: &[(Vec<Datum>, Locator, Vec<Datum>, Locator)],
) -> Result<(), StorageError> {
    if indexes.is_empty() {
        return Ok(());
    }
    for (old_values, old_locator, _, _) in ops {
        index_delete_row(ctx, txn, indexes, collations, old_values, old_locator)?;
    }
    for (_, _, new_values, new_locator) in ops {
        index_insert_row(
            ctx,
            txn,
            indexes,
            schema,
            collations,
            new_values,
            new_locator,
        )?;
    }
    Ok(())
}

/// Removes one row's entries from every secondary index.
fn index_delete_row(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let (key, _) = index::leaf_entry(&index_key, locator, index.unique, None);
        let tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        tree.delete(ctx, &mut OpMode::Txn(txn), &key)?;
    }
    Ok(())
}

fn column_index(schema: &Schema, name: &str) -> Result<usize, StorageError> {
    column_index_by(schema, name)
}

fn column_index_by(schema: &Schema, name: &str) -> Result<usize, StorageError> {
    schema
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| StorageError::InvalidConfig(format!("unknown column '{name}'")))
}

fn validate_not_null(schema: &Schema, values: &[Datum]) -> Result<(), StorageError> {
    for (column, value) in schema.columns.iter().zip(values) {
        if !column.nullable && value.is_null() {
            return Err(StorageError::Constraint(format!(
                "column '{}' does not allow NULL",
                column.name
            )));
        }
    }
    Ok(())
}

/// Opaque handle to a stored row, addressing it for a targeted UPDATE/DELETE.
/// Clustered tables locate by encoded PK key; heaps by RID.
#[derive(Debug, Clone)]
pub(crate) enum RowLocator {
    Key(Vec<u8>),
    Rid(Rid),
}

/// A caller-held (multi-statement) relational transaction: the WAL/undo chain
/// plus the tree-root snapshot taken at BEGIN (used to re-descend trees during
/// rollback).
pub(crate) struct StorageTxn {
    txn: TxnLink,
    roots: std::collections::HashMap<u32, u64>,
}

impl StorageTxn {
    /// The transaction's id — a versioned reader's "own transaction" for
    /// visibility of its own uncommitted writes.
    pub(crate) fn txn_id(&self) -> u64 {
        self.txn.txn_id
    }
}

/// The transaction a statement runs under.
pub(crate) enum TxnScope<'a> {
    /// Autocommit: begin + commit around the single statement.
    Auto,
    /// A caller-held transaction; the statement's ops are appended and NOT
    /// committed. A statement error leaves its partial ops in place — the
    /// caller dooms the transaction and a later ROLLBACK undoes everything.
    Explicit(&'a mut StorageTxn),
}

pub struct SnapshotData {
    pub data: Vec<u8>,
    pub checkpoint_seq: u64,
    pub next_seq_no: u64,
    pub next_doc_id: u64,
}

struct StorageFile {
    /// Handle for data-region, superblock and descriptor I/O.
    file: DirectFile,
    /// WAL writer with its own dedicated file handle, so log writes do not
    /// serialize behind page flushes.
    wal: WalWriter,
    layout: StorageLayout,
    superblock_a: Superblock,
    superblock_b: Superblock,
    active_superblock: ActiveSuperblock,
    allocator: PageAllocator,
    /// Relational store state (buffer pool, dirty-page table, catalog cache).
    rel: RelState,
    /// WAL records recovered at open, waiting for the engine to replay them.
    replay_cache: Vec<WalRecord>,
    /// The database's default collation, read from the file header at open. A
    /// character column declared without an explicit `COLLATE` is resolved to
    /// this at CREATE TABLE and stored with it, so the column keeps the
    /// collation it was created under even if the default later changes.
    default_collation: Option<String>,
    /// Stage 13 version store: row-version chains for snapshot reads, plus
    /// the RCSI / ALLOW_SNAPSHOT_ISOLATION options (persisted in the
    /// superblock reserved area; the chains themselves are memory-only — no
    /// snapshot survives a restart, so neither must they).
    version: crate::relstore::version::VersionState,
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

impl StorageFile {
    /// Returns the WAL records recovered at open (head..tail order). Drains
    /// the recovery cache; subsequent calls return an empty vec.
    pub fn replay_wal_entries(&mut self) -> Result<Vec<WalRecord>, StorageError> {
        Ok(std::mem::take(&mut self.replay_cache))
    }

    pub fn wal_usage_ratio(&self) -> f64 {
        self.wal.usage_ratio()
    }

    /// The current WAL tail (append position).
    fn wal_tail(&self) -> u64 {
        self.wal.tail()
    }

    /// Duplicates the WAL file descriptor for the group-commit log-writer.
    fn try_clone_wal_fd(&self) -> Result<std::fs::File, StorageError> {
        Ok(self.wal.try_clone_file()?)
    }

    /// Whether a data-region page is currently allocated (test/diagnostic
    /// hook).
    pub fn is_page_allocated(&self, page: u64) -> bool {
        self.allocator.is_allocated(page)
    }

    /// Creates a table: with `key_names` it becomes a clustered B+ tree on
    /// those columns, without it a heap.
    #[allow(clippy::too_many_arguments)]
    pub fn rel_create_table(
        &mut self,
        name: &str,
        mut columns: Vec<Column>,
        key_names: &[String],
        defaults: Vec<Option<String>>,
        identity: Option<catalog::IdentitySpec>,
        check_constraints: Vec<catalog::CheckDef>,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        // A character column declared without an explicit COLLATE inherits the
        // database default *by name*, recorded now rather than resolved on each
        // read: the column's key bytes are that collation's sort keys, so it has
        // to keep the collation it was created under. Resolved here, at the one
        // point every CREATE TABLE passes through, so the SQL path and the
        // native path cannot disagree.
        if let Some(default) = self.default_collation.clone() {
            for column in &mut columns {
                if column.collation.is_none()
                    && matches!(
                        column.column_type,
                        crate::relstore::types::ColumnType::VarChar { .. }
                            | crate::relstore::types::ColumnType::NVarChar { .. }
                    )
                {
                    column.collation = Some(default.clone());
                }
            }
        }
        if self.rel.tables.contains_key(name) {
            return Err(StorageError::Constraint(format!(
                "table '{name}' already exists"
            )));
        }
        if columns.is_empty() {
            return Err(StorageError::InvalidConfig(
                "a table needs at least one column".to_string(),
            ));
        }
        let mut key_columns = Vec::new();
        for key_name in key_names {
            let index = columns
                .iter()
                .position(|c| &c.name == key_name)
                .ok_or_else(|| {
                    StorageError::InvalidConfig(format!("unknown key column '{key_name}'"))
                })?;
            if columns[index].nullable {
                return Err(StorageError::InvalidConfig(format!(
                    "primary key column '{key_name}' must be NOT NULL"
                )));
            }
            key_columns.push(index);
        }

        // The catalog tree itself is created outside the statement (system
        // records, not undoable) so a rolled-back CREATE TABLE still leaves
        // a valid catalog.
        if self.rel.catalog_root.is_none() {
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let def_columns: Vec<(String, String, bool)> = columns
            .iter()
            .map(|c| (c.name.clone(), c.column_type.name(), c.nullable))
            .collect();
        let collations: Vec<Option<String>> = columns.iter().map(|c| c.collation.clone()).collect();
        let table_name = name.to_string();
        let is_tree = !key_columns.is_empty();

        let def = self.rel_statement(move |ctx, txn| {
            let root_page = if is_tree {
                BTree::create(ctx, object_id)?.root
            } else {
                Heap::create(ctx, object_id)?.first_page
            };
            let counter_page = ctx.counter_create(object_id)?;
            let def = TableDef {
                object_id,
                name: table_name,
                columns: def_columns,
                key_columns,
                root_page,
                defaults,
                collations,
                identity,
                indexes: Vec::new(),
                check_constraints,
                foreign_keys,
                view_query: None,
                counter_page: Some(counter_page),
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Creates a VIEW: a catalog entry that stores its `SELECT` source text and
    /// owns no data pages. The name shares the table namespace (a view and a
    /// table cannot share a name).
    pub fn rel_create_view(&mut self, name: &str, query_text: &str) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        if self.rel.tables.contains_key(name) {
            return Err(StorageError::Constraint(format!(
                "object '{name}' already exists"
            )));
        }
        if self.rel.catalog_root.is_none() {
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let view_name = name.to_string();
        let query = query_text.to_string();

        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: view_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: Some(query),
                counter_page: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// The table's definition (schema and layout), if it exists.
    pub fn rel_table(&self, name: &str) -> Option<TableDef> {
        self.rel.tables.get(name).cloned()
    }

    /// All user table definitions, ordered by object id (for sys.tables /
    /// sys.columns).
    pub fn rel_tables(&self) -> Vec<TableDef> {
        let mut defs: Vec<TableDef> = self.rel.tables.values().cloned().collect();
        defs.sort_by_key(|d| d.object_id);
        defs
    }

    /// Drops a table (logical: removes the catalog row; data pages leak
    /// until a later reclamation stage). Returns false if the table does not
    /// exist.
    pub fn rel_drop_table(&mut self, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        let Some(def) = self.rel.tables.get(name).cloned() else {
            return Ok(false);
        };
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        self.rel_statement(move |ctx, txn| {
            catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, def.object_id)
        })?;
        self.rel.tables.remove(name);
        Ok(true)
    }

    /// Creates a secondary index over `table` and backfills it from the
    /// current rows (blocking build). A duplicate on a UNIQUE index during the
    /// build fails the whole statement (error 2601). The index is persisted in
    /// the table's catalog row.
    pub(crate) fn rel_create_index(
        &mut self,
        table: &str,
        index_name: String,
        columns: Vec<(usize, bool)>,
        unique: bool,
        include: Vec<usize>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let mut def = self
            .rel
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{table}'")))?;
        if def
            .indexes
            .iter()
            .any(|i| i.name.eq_ignore_ascii_case(&index_name))
        {
            return Err(StorageError::Constraint(format!(
                "index '{index_name}' already exists"
            )));
        }
        let catalog_root = self
            .rel
            .catalog_root
            .ok_or_else(|| StorageError::InvalidFile("catalog root missing".to_string()))?;
        let object_id = self.rel.next_object_id;
        // Snapshot the rows to backfill (materialized before any mutation).
        let located = self.rel_scan_located(table)?;

        let schema = def.schema()?;
        let updated = self.rel_statement(move |ctx, txn| {
            let tree = BTree::create(ctx, object_id)?;
            for (loc, values) in &located {
                let locator = match loc {
                    RowLocator::Key(key) => Locator::Key(key.clone()),
                    RowLocator::Rid(rid) => Locator::Rid(*rid),
                };
                let index_key = index::encode_index_columns(values, &columns, &def.collations)
                    .map_err(|err| StorageError::InvalidConfig(err.0))?;
                let include_bytes = if include.is_empty() {
                    None
                } else {
                    Some(
                        index::encode_include(&schema, &include, values)
                            .map_err(|err| StorageError::InvalidConfig(err.0))?,
                    )
                };
                let (key, value) =
                    index::leaf_entry(&index_key, &locator, unique, include_bytes.as_deref());
                // Backfill is system-logged: the fresh tree is not in the
                // rollback roots, so a failure leaks it (the catalog entry
                // below is undone, leaving it unreferenced).
                match tree.insert_unique_bulk(ctx, &key, &value)? {
                    TreeInsert::Inserted => {}
                    TreeInsert::DuplicateKey => {
                        return Err(StorageError::Constraint(format!(
                            "duplicate unique index '{index_name}'"
                        )));
                    }
                }
            }
            def.indexes.push(IndexDef {
                object_id,
                name: index_name,
                columns,
                unique,
                root_page: tree.root,
                include,
            });
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(table.to_string(), updated);
        Ok(())
    }

    /// `ALTER TABLE ADD <column>`: appends the column to the table's catalog
    /// entry and rewrites every existing row under the widened schema, all in
    /// one transactional statement — the row codec is positional, so an old
    /// row cannot be read under the new schema without re-encoding. Keys and
    /// index entries are untouched: appending a column shifts no schema index
    /// the key or any secondary index refers to, tree rewrites are in-place
    /// by key, and heap RIDs are stable across an update.
    pub(crate) fn rel_alter_add_column(
        &mut self,
        table: &str,
        column: Column,
        default_text: Option<String>,
        fill: Datum,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let mut column = column;
        // A character column without an explicit COLLATE inherits the database
        // default by name, exactly as CREATE TABLE records it.
        if column.collation.is_none()
            && matches!(
                column.column_type,
                crate::relstore::types::ColumnType::VarChar { .. }
                    | crate::relstore::types::ColumnType::NVarChar { .. }
            )
        {
            column.collation = self.default_collation.clone();
        }
        let mut def = self
            .rel
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{table}'")))?;
        let catalog_root = self
            .rel
            .catalog_root
            .ok_or_else(|| StorageError::InvalidFile("catalog root missing".to_string()))?;
        // Snapshot every row under the OLD schema (with its locator), before
        // the definition widens.
        let located = self.rel_scan_located(table)?;

        // Parallel catalog arrays: `defaults`/`collations` may be shorter than
        // `columns` (serde(default) on pre-upgrade tables) — pad before push.
        def.defaults.resize(def.columns.len(), None);
        def.collations.resize(def.columns.len(), None);
        def.columns.push((
            column.name.clone(),
            column.column_type.name(),
            column.nullable,
        ));
        def.defaults.push(default_text);
        def.collations.push(column.collation.clone());
        let new_schema = def.schema()?;

        // Re-encode every row with the frozen fill appended.
        let mut tree_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut heap_rows: Vec<(Rid, Vec<u8>)> = Vec::new();
        for (loc, mut values) in located {
            values.push(fill.clone());
            let row = encode_row(&new_schema, &values)?;
            match loc {
                RowLocator::Key(key) => tree_rows.push((key, row)),
                RowLocator::Rid(rid) => heap_rows.push((rid, row)),
            }
        }

        let is_tree = def.is_tree();
        let object_id = def.object_id;
        let root_page = def.root_page;
        let updated = self.rel_statement(move |ctx, txn| {
            if is_tree {
                let tree = BTree {
                    object_id,
                    root: root_page,
                };
                for (key, row) in &tree_rows {
                    tree.update(ctx, &mut OpMode::Txn(txn), key, row)?;
                }
            } else {
                let heap = Heap {
                    object_id,
                    first_page: root_page,
                };
                for (rid, row) in &heap_rows {
                    heap.update(ctx, txn, *rid, row)?;
                }
            }
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.tables.insert(table.to_string(), updated);
        // ALTER ADD re-encodes every row: version images from before it
        // cannot decode under the widened schema, so a SNAPSHOT transaction
        // whose view predates this commit gets 3961 at its next access
        // (statement snapshots cannot be live here — the ALTER holds
        // Database X). Stamped with this ALTER's own commit sequence, the
        // newest assigned (recorded a moment ago in `rel_statement`).
        self.version.stamp_schema(object_id);
        Ok(())
    }

    /// Drops a secondary index by name (logical: index pages leak). Returns
    /// false if no such index exists on any table.
    pub(crate) fn rel_drop_index(
        &mut self,
        table: &str,
        index_name: &str,
    ) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        // Index names are scoped to their table, so confine the lookup there.
        // The caller passes the table's canonical name.
        let Some((table_key, mut def)) = self
            .rel
            .tables
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(table))
            .map(|(name, def)| (name.clone(), def.clone()))
        else {
            return Ok(false);
        };
        if !def
            .indexes
            .iter()
            .any(|i| i.name.eq_ignore_ascii_case(index_name))
        {
            return Ok(false);
        }
        def.indexes
            .retain(|i| !i.name.eq_ignore_ascii_case(index_name));
        let updated = self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.tables.insert(table_key, updated);
        Ok(true)
    }

    /// Candidate rows for an index access path: walks the index tree over
    /// `[lower, upper]`, then fetches each row by its locator. Returns a
    /// superset the caller re-filters with the full WHERE (so loose bounds are
    /// safe).
    pub(crate) fn rel_row_count(&mut self, table: &str) -> Option<u64> {
        if self.ensure_rel_usable().is_err() {
            return None;
        }
        let page = self.rel.tables.get(table)?.counter_page?;
        self.rel_ctx().counter_read(page).ok()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rel_index_scan(
        &mut self,
        table: &str,
        index_object_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        projection: Option<&[usize]>,
        covering: bool,
        snapshot: Option<ReadSnapshot>,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(table)?;
        let index = def
            .indexes
            .iter()
            .find(|i| i.object_id == index_object_id)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig("unknown index".to_string()))?;
        if let Some(snap) = snapshot
            && self.version.schema_changed_after(def.object_id, snap)
        {
            return Err(StorageError::SnapshotSchemaChange(def.name));
        }
        let entries = {
            let mut ctx = self.rel_ctx();
            let index_tree = BTree {
                object_id: index.object_id,
                root: index.root_page,
            };
            index_tree.scan_range(&mut ctx, lower.as_deref(), upper.as_deref())?
        };
        // The leaf-value format depends on the index: an INCLUDE index
        // length-prefixes its locator (a Key locator's payload would
        // otherwise swallow the include bytes that follow it).
        let locator_of = |value: &[u8]| -> Locator {
            if index.include.is_empty() {
                index::decode_locator(value)
            } else {
                index::decode_leaf_value_with_include(value).0
            }
        };
        // Resolve each entry against the version store first (a snapshot
        // reader may need an entry's row served from an older image, or
        // dropped when its writer is invisible), then do the page lookups.
        // Rows the seek could not encounter — their index entry was moved or
        // removed by a writer the snapshot does not see — are appended from
        // their chain images; the executor's predicate re-checks every row,
        // so over-returning is filtered, never wrong.
        enum Entry {
            Physical(Vec<u8>),
            Image(Vec<u8>),
        }
        let merging = snapshot.is_some_and(|_| self.version.table_has_chains(def.object_id));
        let mut decided: Vec<Entry> = Vec::with_capacity(entries.len());
        let mut extra_images: Vec<Vec<u8>> = Vec::new();
        if let (Some(snap), true) = (snapshot, merging) {
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for (_, value) in entries {
                let identity = match locator_of(&value) {
                    Locator::Key(pk) => pk,
                    Locator::Rid(rid) => rid_identity(rid),
                };
                match self.version.resolve(def.object_id, &identity, snap) {
                    None | Some(Resolved::Current) => decided.push(Entry::Physical(value)),
                    Some(Resolved::Image(image)) => decided.push(Entry::Image(image)),
                    Some(Resolved::Gone) => {}
                }
                seen.insert(identity);
            }
            extra_images = self.version.unseen_images(def.object_id, &seen, snap);
        } else {
            decided.extend(entries.into_iter().map(|(_, value)| Entry::Physical(value)));
        }

        let mut rows = Vec::with_capacity(decided.len());
        if covering {
            // Answer from the leaves alone: every projected column's original
            // value is stored in the entry (after the length-prefixed
            // locator), so the base-table lookup is skipped entirely. The
            // planner only chooses covering when projection ⊆ include; this
            // re-checks so a planner bug reads as an error, not wrong data.
            let projection = projection.ok_or_else(|| {
                StorageError::InvalidConfig("covering scan requires a projection".to_string())
            })?;
            let positions: Vec<usize> = projection
                .iter()
                .map(|col| {
                    index.include.iter().position(|i| i == col).ok_or_else(|| {
                        StorageError::InvalidConfig(format!(
                            "column {col} is not included in index '{}'",
                            index.name
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            for entry in decided {
                match entry {
                    Entry::Physical(value) => {
                        let (_, include_bytes) = index::decode_leaf_value_with_include(&value);
                        let decoded = index::decode_include(&schema, &index.include, include_bytes)
                            .map_err(|err| StorageError::InvalidFile(err.0))?;
                        rows.push(positions.iter().map(|&p| decoded[p].clone()).collect());
                    }
                    // A version image is the full row: project it directly.
                    Entry::Image(image) => {
                        rows.push(decode_row_projected(&schema, &image, projection)?);
                    }
                }
            }
        } else {
            let mut ctx = self.rel_ctx();
            if def.is_tree() {
                let base = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                for entry in decided {
                    match entry {
                        Entry::Physical(value) => {
                            if let Locator::Key(pk) = locator_of(&value)
                                && let Some(row) = base.get(&mut ctx, &pk)?
                            {
                                rows.push(decode_projected(&schema, &row, projection)?);
                            }
                        }
                        Entry::Image(image) => {
                            rows.push(decode_projected(&schema, &image, projection)?);
                        }
                    }
                }
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                for entry in decided {
                    match entry {
                        Entry::Physical(value) => {
                            if let Locator::Rid(rid) = locator_of(&value)
                                && let Some(row) = heap.read_row(&mut ctx, rid)?
                            {
                                rows.push(decode_projected(&schema, &row, projection)?);
                            }
                        }
                        Entry::Image(image) => {
                            rows.push(decode_projected(&schema, &image, projection)?);
                        }
                    }
                }
            }
        }
        for image in extra_images {
            rows.push(decode_projected(&schema, &image, projection)?);
        }
        Ok(rows)
    }

    pub fn rel_insert(&mut self, name: &str, values: Vec<Datum>) -> Result<(), StorageError> {
        self.rel_insert_many(name, vec![values], &mut TxnScope::Auto)
    }

    /// Inserts many rows as ONE atomic statement: all rows land or none do
    /// (a later row's constraint failure rolls back the whole statement,
    /// matching T-SQL multi-row `INSERT ... VALUES` semantics).
    pub(crate) fn rel_insert_many(
        &mut self,
        name: &str,
        rows: Vec<Vec<Datum>>,
        scope: &mut TxnScope,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        // Encode and validate every row up front (cheap failures before any
        // mutation), keeping the key alongside for tree tables.
        let mut encoded: Vec<(Option<Vec<u8>>, Vec<u8>)> = Vec::with_capacity(rows.len());
        for values in &rows {
            validate_not_null(&schema, values)?;
            let row = encode_row(&schema, values)?;
            let key = if def.is_tree() {
                Some(encode_key(&schema, &def.key_columns, values)?)
            } else {
                None
            };
            encoded.push((key, row));
        }

        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        let counter_page = def.counter_page;
        let inserted = rows.len() as i64;
        let publishing = self.version.publishing();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for ((key, row), values) in encoded.iter().zip(rows.iter()) {
                    let key = key.as_ref().expect("tree row has a key");
                    match tree.insert_unique(ctx, &mut OpMode::Txn(txn), key, row)? {
                        TreeInsert::Inserted => {}
                        TreeInsert::DuplicateKey => {
                            return Err(StorageError::Constraint(
                                "duplicate primary key".to_string(),
                            ));
                        }
                    }
                    // Clustered rows locate by PK key.
                    index_insert_row(
                        ctx,
                        txn,
                        &indexes,
                        &schema,
                        &collations,
                        values,
                        &Locator::Key(key.clone()),
                    )?;
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id: tree.object_id,
                            identity: key.clone(),
                            change: RowChange::Insert,
                        });
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, inserted)?;
                }
                Ok(())
            })
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for ((_, row), values) in encoded.iter().zip(rows.iter()) {
                    // Heap rows locate by their home RID.
                    let rid = heap.insert(ctx, txn, row)?;
                    index_insert_row(
                        ctx,
                        txn,
                        &indexes,
                        &schema,
                        &collations,
                        values,
                        &Locator::Rid(rid),
                    )?;
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id: heap.object_id,
                            identity: rid_identity(rid),
                            change: RowChange::Insert,
                        });
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, inserted)?;
                }
                Ok(())
            })
        }
    }

    /// Point lookup by primary key (clustered tables only).
    pub fn rel_get(
        &mut self,
        name: &str,
        key_values: &[Datum],
    ) -> Result<Option<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        if !def.is_tree() {
            return Err(StorageError::InvalidConfig(format!(
                "table '{name}' has no primary key"
            )));
        }
        if key_values.len() != def.key_columns.len() {
            return Err(StorageError::InvalidConfig(
                "wrong number of key values".to_string(),
            ));
        }
        // Encode each key column under its collation, exactly as the stored key
        // was, so a character PK lookup matches whatever the collation calls
        // equal.
        let mut key = Vec::new();
        for (value, &col) in key_values.iter().zip(&def.key_columns) {
            crate::relstore::key::encode_datum_collated(
                value,
                schema.columns[col].collation.as_deref(),
                &mut key,
            )?;
        }
        let tree = BTree {
            object_id: def.object_id,
            root: def.root_page,
        };
        let mut ctx = self.rel_ctx();
        match tree.get(&mut ctx, &key)? {
            Some(row) => Ok(Some(decode_row(&schema, &row)?)),
            None => Ok(None),
        }
    }

    /// Full scan: rows as typed datums (key order for trees, chain order
    /// for heaps).
    /// One slice of a scan: appends at most `budget` rows from `cursor` and
    /// returns where to resume. The caller loops, so the storage lock is taken
    /// once per slice instead of once for the whole table.
    pub(crate) fn rel_scan_slice(
        &mut self,
        name: &str,
        cursor: ScanCursor,
        budget: usize,
        projection: Option<&[usize]>,
        out: &mut Vec<Vec<Datum>>,
    ) -> Result<ScanCursor, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let mut ctx = self.rel_ctx();
        let mut raw: Vec<Vec<u8>> = Vec::new();
        let next = if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            let mut keyed = Vec::new();
            let next = tree.scan_from(&mut ctx, cursor, budget, &mut keyed)?;
            raw.extend(keyed.into_iter().map(|(_, row)| row));
            next
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let mut located = Vec::new();
            let next = heap.scan_from(&mut ctx, cursor, budget, &mut located)?;
            raw.extend(located.into_iter().map(|(_, row)| row));
            next
        };
        for row in raw {
            out.push(decode_projected(&schema, &row, projection)?);
        }
        Ok(next)
    }

    pub fn rel_scan(&mut self, name: &str) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let mut ctx = self.rel_ctx();
        let raw: Vec<Vec<u8>> = if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            tree.scan(&mut ctx)?
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            heap.scan(&mut ctx)?
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        };
        raw.into_iter()
            .map(|row| decode_row(&schema, &row).map_err(StorageError::from))
            .collect()
    }

    /// Snapshot scan (Stage 13): the whole table read atomically under this
    /// one lock hold, each row resolved through the version store — a row
    /// last written by a transaction the snapshot cannot see is served from
    /// its chain image instead — plus the images of rows the physical walk
    /// could not encounter (deleted or re-keyed by writers the snapshot does
    /// not see). Atomic because a versioned reader holds no table lock, so a
    /// sliced cursor could be restructured under it mid-walk.
    fn rel_scan_snapshot(
        &mut self,
        name: &str,
        projection: Option<&[usize]>,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        if self.version.schema_changed_after(def.object_id, snapshot) {
            return Err(StorageError::SnapshotSchemaChange(def.name));
        }
        let physical: Vec<(Vec<u8>, Vec<u8>)> = {
            let mut ctx = self.rel_ctx();
            if def.is_tree() {
                let tree = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                tree.scan(&mut ctx)?
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                heap.scan(&mut ctx)?
                    .into_iter()
                    .map(|(rid, row)| (rid_identity(rid), row))
                    .collect()
            }
        };
        let mut out = Vec::with_capacity(physical.len());
        if !self.version.table_has_chains(def.object_id) {
            for (_, row) in physical {
                out.push(decode_projected(&schema, &row, projection)?);
            }
            return Ok(out);
        }
        let mut seen: std::collections::HashSet<Vec<u8>> =
            std::collections::HashSet::with_capacity(physical.len());
        for (identity, row) in physical {
            match self.version.resolve(def.object_id, &identity, snapshot) {
                None | Some(Resolved::Current) => {
                    out.push(decode_projected(&schema, &row, projection)?);
                }
                Some(Resolved::Image(image)) => {
                    out.push(decode_projected(&schema, &image, projection)?);
                }
                Some(Resolved::Gone) => {}
            }
            seen.insert(identity);
        }
        for image in self.version.unseen_images(def.object_id, &seen, snapshot) {
            out.push(decode_projected(&schema, &image, projection)?);
        }
        Ok(out)
    }

    /// Deletes every row where `column = value`; returns the count. Targets
    /// are materialized before any mutation (Halloween avoidance).
    pub fn rel_delete_where(
        &mut self,
        name: &str,
        column: &str,
        value: &Datum,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let column_index = column_index(&schema, column)?;
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            let keys = {
                let mut ctx = self.rel_ctx();
                let mut keys = Vec::new();
                for (key, row) in tree.scan(&mut ctx)? {
                    if decode_row(&schema, &row)?[column_index] == *value {
                        keys.push(key);
                    }
                }
                keys
            };
            let count = keys.len();
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for key in &keys {
                        tree.delete(ctx, &mut OpMode::Txn(txn), key)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let rids = {
                let mut ctx = self.rel_ctx();
                let mut rids = Vec::new();
                for (rid, row) in heap.scan(&mut ctx)? {
                    if decode_row(&schema, &row)?[column_index] == *value {
                        rids.push(rid);
                    }
                }
                rids
            };
            let count = rids.len();
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for rid in &rids {
                        heap.delete(ctx, txn, *rid)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        }
    }

    /// Updates every row where `column = value` with the given column
    /// assignments; returns the count. Key columns of clustered tables are
    /// immutable here (delete + insert to change a key).
    pub fn rel_update_where(
        &mut self,
        name: &str,
        column: &str,
        value: &Datum,
        assignments: &[(String, Datum)],
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let column_index = column_index(&schema, column)?;
        let mut set: Vec<(usize, Datum)> = Vec::new();
        for (set_name, set_value) in assignments {
            let index = column_index_by(&schema, set_name)?;
            if def.key_columns.contains(&index) {
                return Err(StorageError::InvalidConfig(format!(
                    "cannot update primary key column '{set_name}'"
                )));
            }
            set.push((index, set_value.clone()));
        }

        let apply_set = |mut values: Vec<Datum>| -> Vec<Datum> {
            for (index, new_value) in &set {
                values[*index] = new_value.clone();
            }
            values
        };

        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            let targets = {
                let mut ctx = self.rel_ctx();
                let mut targets = Vec::new();
                for (key, row) in tree.scan(&mut ctx)? {
                    let values = decode_row(&schema, &row)?;
                    if values[column_index] == *value {
                        targets.push((key, values));
                    }
                }
                targets
            };
            let count = targets.len();
            let mut encoded = Vec::with_capacity(count);
            for (key, values) in targets {
                let new_values = apply_set(values);
                validate_not_null(&schema, &new_values)?;
                encoded.push((key, encode_row(&schema, &new_values)?));
            }
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for (key, row) in &encoded {
                        tree.update(ctx, &mut OpMode::Txn(txn), key, row)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let targets = {
                let mut ctx = self.rel_ctx();
                let mut targets = Vec::new();
                for (rid, row) in heap.scan(&mut ctx)? {
                    let values = decode_row(&schema, &row)?;
                    if values[column_index] == *value {
                        targets.push((rid, values));
                    }
                }
                targets
            };
            let count = targets.len();
            let mut encoded = Vec::with_capacity(count);
            for (rid, values) in targets {
                let new_values = apply_set(values);
                validate_not_null(&schema, &new_values)?;
                encoded.push((rid, encode_row(&schema, &new_values)?));
            }
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for (rid, row) in &encoded {
                        heap.update(ctx, txn, *rid, row)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        }
    }

    /// Full scan returning each row with an opaque locator that addresses it
    /// for a later targeted delete/update. The caller filters the whole
    /// materialized set before any mutation, so this is Halloween-safe by
    /// construction (matched targets are chosen from a snapshot of the table).
    pub(crate) fn rel_scan_located(
        &mut self,
        name: &str,
    ) -> Result<Vec<(RowLocator, Vec<Datum>)>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let mut ctx = self.rel_ctx();
        let mut out = Vec::new();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            for (key, row) in tree.scan(&mut ctx)? {
                out.push((RowLocator::Key(key), decode_row(&schema, &row)?));
            }
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            for (rid, row) in heap.scan(&mut ctx)? {
                out.push((RowLocator::Rid(rid), decode_row(&schema, &row)?));
            }
        }
        Ok(out)
    }

    /// The SNAPSHOT-isolation DML target scan: like [`Self::rel_scan_located`]
    /// but rows are the snapshot's versions, and each carries a conflict mark
    /// when its current state was produced by a writer the snapshot cannot
    /// see — physically present rows served from an older image, and rows
    /// deleted (or re-keyed away) since the snapshot, whose locators are
    /// synthesized from their identities. Targeting a marked row is a 3960
    /// update conflict; the mark is computed here because only this layer
    /// sees both the physical state and the chains, atomically under the
    /// storage mutex. (A marked row can also mean a live writer is mid-flight
    /// on it — but the caller holds the statement's X locks, so a marked row
    /// it actually targets can only be a committed-invisible change.)
    fn rel_scan_located_snapshot(
        &mut self,
        name: &str,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        if self.version.schema_changed_after(def.object_id, snapshot) {
            return Err(StorageError::SnapshotSchemaChange(def.name));
        }
        let physical: Vec<(Vec<u8>, RowLocator, Vec<u8>)> = {
            let mut ctx = self.rel_ctx();
            if def.is_tree() {
                let tree = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                tree.scan(&mut ctx)?
                    .into_iter()
                    .map(|(key, row)| (key.clone(), RowLocator::Key(key), row))
                    .collect()
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                heap.scan(&mut ctx)?
                    .into_iter()
                    .map(|(rid, row)| (rid_identity(rid), RowLocator::Rid(rid), row))
                    .collect()
            }
        };
        let merging = self.version.table_has_chains(def.object_id);
        let mut out = Vec::with_capacity(physical.len());
        let mut seen: std::collections::HashSet<Vec<u8>> =
            std::collections::HashSet::with_capacity(if merging { physical.len() } else { 0 });
        for (identity, locator, row) in physical {
            if !merging {
                out.push((locator, decode_row(&schema, &row)?, false));
                continue;
            }
            match self.version.resolve(def.object_id, &identity, snapshot) {
                None | Some(Resolved::Current) => {
                    out.push((locator, decode_row(&schema, &row)?, false));
                }
                // Served from an older image: the current row belongs to a
                // writer the snapshot cannot see.
                Some(Resolved::Image(image)) => {
                    out.push((locator, decode_row(&schema, &image)?, true));
                }
                Some(Resolved::Gone) => {}
            }
            seen.insert(identity);
        }
        if merging {
            for (identity, image) in
                self.version
                    .unseen_images_with_identity(def.object_id, &seen, snapshot)
            {
                // Deleted or re-keyed since the snapshot: visible to it, but
                // its current state is gone — always a conflict if targeted.
                let locator = if def.is_tree() {
                    RowLocator::Key(identity)
                } else {
                    RowLocator::Rid(decode_rid_identity(&identity))
                };
                out.push((locator, decode_row(&schema, &image)?, true));
            }
        }
        Ok(out)
    }

    /// Deletes the located rows (each carrying its old values for index
    /// upkeep) in one atomic statement; returns the count.
    pub(crate) fn rel_delete_located(
        &mut self,
        name: &str,
        targets: Vec<(RowLocator, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let count = targets.len();
        if count == 0 {
            return Ok(0);
        }
        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        let counter_page = def.counter_page;
        // Version staging: the deleted rows' images, encoded up front (the
        // schema stays out of the closure), indexed alongside `targets`.
        let publishing = self.version.publishing();
        let priors: Vec<Option<Vec<u8>>> = if publishing {
            targets
                .iter()
                .map(|(_, values)| encode_row(&schema, values).map(Some))
                .collect::<Result<_, _>>()?
        } else {
            targets.iter().map(|_| None).collect()
        };
        // The counter follows the rows actually removed inside the statement,
        // which the arms count as they go (a locator of the wrong kind is
        // skipped, exactly as the row loop skips it).
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                let mut removed: i64 = 0;
                for ((loc, values), prior) in targets.iter().zip(priors) {
                    if let RowLocator::Key(key) = loc {
                        tree.delete(ctx, &mut OpMode::Txn(txn), key)?;
                        index_delete_row(
                            ctx,
                            txn,
                            &indexes,
                            &collations,
                            values,
                            &Locator::Key(key.clone()),
                        )?;
                        if let Some(prior) = prior {
                            txn.pending_versions.push(PendingVersion {
                                object_id: tree.object_id,
                                identity: key.clone(),
                                change: RowChange::Delete { prior },
                            });
                        }
                        removed += 1;
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, -removed)?;
                }
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                let mut removed: i64 = 0;
                for ((loc, values), prior) in targets.iter().zip(priors) {
                    if let RowLocator::Rid(rid) = loc {
                        heap.delete(ctx, txn, *rid)?;
                        index_delete_row(
                            ctx,
                            txn,
                            &indexes,
                            &collations,
                            values,
                            &Locator::Rid(*rid),
                        )?;
                        if let Some(prior) = prior {
                            txn.pending_versions.push(PendingVersion {
                                object_id: heap.object_id,
                                identity: rid_identity(*rid),
                                change: RowChange::Delete { prior },
                            });
                        }
                        removed += 1;
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, -removed)?;
                }
                Ok(())
            })?;
        }
        Ok(count)
    }

    /// Applies full-row updates (each carrying its old and new values; already
    /// type-checked and NOT-NULL-checked by the caller) in one atomic
    /// statement. For a clustered table a row whose key changed is re-keyed
    /// (delete + insert with uniqueness enforced); heaps update in place by
    /// RID. Secondary indexes are maintained by deleting every old entry then
    /// inserting every new one (so a unique index tolerates value swaps).
    /// Returns the count.
    pub(crate) fn rel_update_located(
        &mut self,
        name: &str,
        updates: Vec<(RowLocator, Vec<Datum>, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(name)?;
        let count = updates.len();
        if count == 0 {
            return Ok(0);
        }
        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        let publishing = self.version.publishing();
        // (old values, old locator, new values, new locator) for index upkeep.
        let mut idx_ops: Vec<(Vec<Datum>, Locator, Vec<Datum>, Locator)> = Vec::new();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            // Partition into in-place (key unchanged) and re-key (key changed).
            let mut in_place: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut rekey: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = Vec::new();
            // Version staging, split to mirror the closure's op order (all
            // re-key deletes, then re-key inserts, then in-place updates), so
            // an identity touched twice in one statement — a key swap — has
            // its chain built in the order the physical states change.
            let mut staged_rekey_del: Vec<PendingVersion> = Vec::new();
            let mut staged_rekey_ins: Vec<PendingVersion> = Vec::new();
            let mut staged_in_place: Vec<PendingVersion> = Vec::new();
            for (loc, old_values, new_values) in updates {
                let RowLocator::Key(old_key) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected key locator for clustered table".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                let row = encode_row(&schema, &new_values)?;
                let new_key = encode_key(&schema, &def.key_columns, &new_values)?;
                let prior = if publishing {
                    Some(encode_row(&schema, &old_values)?)
                } else {
                    None
                };
                if !indexes.is_empty() {
                    idx_ops.push((
                        old_values,
                        Locator::Key(old_key.clone()),
                        new_values,
                        Locator::Key(new_key.clone()),
                    ));
                }
                if new_key == old_key {
                    if let Some(prior) = prior {
                        staged_in_place.push(PendingVersion {
                            object_id: tree.object_id,
                            identity: old_key.clone(),
                            change: RowChange::Update { prior },
                        });
                    }
                    in_place.push((old_key, row));
                } else {
                    if let Some(prior) = prior {
                        // A key change is a delete of the old identity and an
                        // insert of the new one, exactly as the tree applies it.
                        staged_rekey_del.push(PendingVersion {
                            object_id: tree.object_id,
                            identity: old_key.clone(),
                            change: RowChange::Delete { prior },
                        });
                        staged_rekey_ins.push(PendingVersion {
                            object_id: tree.object_id,
                            identity: new_key.clone(),
                            change: RowChange::Insert,
                        });
                    }
                    rekey.push((old_key, new_key, row));
                }
            }
            self.rel_statement_scoped(scope, move |ctx, txn| {
                // Delete all re-keyed olds first so a new key may reuse one.
                for (old_key, _, _) in &rekey {
                    tree.delete(ctx, &mut OpMode::Txn(txn), old_key)?;
                }
                for (_, new_key, row) in &rekey {
                    match tree.insert_unique(ctx, &mut OpMode::Txn(txn), new_key, row)? {
                        TreeInsert::Inserted => {}
                        TreeInsert::DuplicateKey => {
                            return Err(StorageError::Constraint(
                                "duplicate primary key".to_string(),
                            ));
                        }
                    }
                }
                for (key, row) in &in_place {
                    tree.update(ctx, &mut OpMode::Txn(txn), key, row)?;
                }
                apply_index_updates(ctx, txn, &indexes, &schema, &collations, &idx_ops)?;
                txn.pending_versions.extend(staged_rekey_del);
                txn.pending_versions.extend(staged_rekey_ins);
                txn.pending_versions.extend(staged_in_place);
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let mut encoded: Vec<(Rid, Vec<u8>)> = Vec::with_capacity(count);
            let mut staged: Vec<PendingVersion> = Vec::new();
            for (loc, old_values, new_values) in updates {
                let RowLocator::Rid(rid) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected rid locator for heap".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                encoded.push((rid, encode_row(&schema, &new_values)?));
                if publishing {
                    staged.push(PendingVersion {
                        object_id: heap.object_id,
                        identity: rid_identity(rid),
                        change: RowChange::Update {
                            prior: encode_row(&schema, &old_values)?,
                        },
                    });
                }
                if !indexes.is_empty() {
                    // Heap RIDs are stable across an update.
                    idx_ops.push((old_values, Locator::Rid(rid), new_values, Locator::Rid(rid)));
                }
            }
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for (rid, row) in &encoded {
                    heap.update(ctx, txn, *rid, row)?;
                }
                apply_index_updates(ctx, txn, &indexes, &schema, &collations, &idx_ops)?;
                txn.pending_versions.extend(staged);
                Ok(())
            })?;
        }
        Ok(count)
    }

    /// Opens a multi-statement transaction (`BEGIN TRAN`).
    pub(crate) fn rel_begin(&mut self) -> Result<StorageTxn, StorageError> {
        self.ensure_rel_usable()?;
        self.begin_txn()
    }

    /// Commits a caller-held transaction.
    pub(crate) fn rel_commit(&mut self, txn: StorageTxn) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.commit_txn(txn)
    }

    /// Rolls back a caller-held transaction.
    pub(crate) fn rel_rollback(&mut self, txn: StorageTxn) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.rollback_txn(txn)
    }

    /// Reserves `count` identity values for a table's IDENTITY column,
    /// advancing and persisting the counter in its own committed statement so
    /// the values survive a crash and are never reused. Returns the first
    /// value; the caller steps subsequent rows by `increment`. Returns `None`
    /// if the table has no identity column.
    pub(crate) fn rel_reserve_identity(
        &mut self,
        name: &str,
        count: usize,
    ) -> Result<Option<i64>, StorageError> {
        self.ensure_rel_usable()?;
        let mut def = self
            .rel
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{name}'")))?;
        let Some(mut spec) = def.identity else {
            return Ok(None);
        };
        let first = spec.next;
        if count > 0 {
            let advance = (count as i64)
                .checked_mul(spec.increment)
                .and_then(|delta| spec.next.checked_add(delta))
                .ok_or_else(|| {
                    StorageError::InvalidConfig("identity value overflow".to_string())
                })?;
            spec.next = advance;
            def.identity = Some(spec);
            let catalog_root = self
                .rel
                .catalog_root
                .ok_or_else(|| StorageError::InvalidConfig("catalog root missing".to_string()))?;
            let persisted = def.clone();
            self.rel_statement(move |ctx, txn| {
                catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &persisted)
            })?;
            self.rel.tables.insert(name.to_string(), def);
        }
        Ok(Some(first))
    }

    /// Replaces a table's CHECK constraints (ALTER TABLE ADD/DROP CONSTRAINT)
    /// and persists the mutated catalog row. Undoable within its own statement.
    pub(crate) fn rel_set_check_constraints(
        &mut self,
        name: &str,
        check_constraints: Vec<catalog::CheckDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let mut def = self
            .rel
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{name}'")))?;
        def.check_constraints = check_constraints;
        let catalog_root = self
            .rel
            .catalog_root
            .ok_or_else(|| StorageError::InvalidConfig("catalog root missing".to_string()))?;
        let persisted = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &persisted)
        })?;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Replaces a table's FOREIGN KEY constraints (ALTER TABLE ADD/DROP
    /// CONSTRAINT) and persists the mutated catalog row.
    pub(crate) fn rel_set_foreign_keys(
        &mut self,
        name: &str,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let mut def = self
            .rel
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{name}'")))?;
        def.foreign_keys = foreign_keys;
        let catalog_root = self
            .rel
            .catalog_root
            .ok_or_else(|| StorageError::InvalidConfig("catalog root missing".to_string()))?;
        let persisted = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &persisted)
        })?;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Test hook: run an insert's ops durably but never commit — the state a
    /// crash mid-statement leaves behind (loser transaction for recovery).
    #[cfg(test)]
    pub(crate) fn rel_insert_without_commit(
        &mut self,
        name: &str,
        values: Vec<Datum>,
    ) -> Result<(), StorageError> {
        let (def, schema) = self.rel_def(name)?;
        let row = encode_row(&schema, &values)?;
        let txn_id = self.rel.next_txn_id;
        self.rel.next_txn_id += 1;
        let mut ctx = self.rel_ctx();
        let mut txn = ctx.begin(txn_id)?;
        if def.is_tree() {
            let key = encode_key(&schema, &def.key_columns, &values)?;
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            tree.insert_unique(&mut ctx, &mut OpMode::Txn(&mut txn), &key, &row)?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            heap.insert(&mut ctx, &mut txn, &row)?;
        }
        // A real statement's op stream includes the counter op, so the crash
        // window this hook simulates must too.
        if let Some(page) = def.counter_page {
            ctx.counter_add(&mut txn, page, 1)?;
        }
        // Durable ops, no commit record: exactly the crash window.
        ctx.io.wal.sync_all()?;
        Ok(())
    }

    /// Test hook: flush dirty relational pages to disk WITHOUT advancing the
    /// WAL head (the mid-checkpoint crash window where torn pages are
    /// possible but their FPIs are still in the log).
    #[cfg(test)]
    pub(crate) fn rel_flush_pool_only(&mut self) -> Result<(), StorageError> {
        self.wal.sync_all()?;
        let RelState { pool, .. } = &mut self.rel;
        let mut io = PoolIo {
            file: &mut self.file,
            wal: &mut self.wal,
            data_offset: self.layout.data_offset,
            data_pages: self.layout.data_size / PAGE_SIZE as u64,
        };
        pool.flush_all(&mut io)?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Test hook: the absolute file offset of a data-region page.
    #[cfg(test)]
    pub(crate) fn data_page_offset(&self, page: u64) -> u64 {
        self.layout.data_offset + page * PAGE_SIZE as u64
    }

    fn rel_def(&self, name: &str) -> Result<(TableDef, Schema), StorageError> {
        let def = self
            .rel
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{name}'")))?;
        let schema = def.schema()?;
        Ok((def, schema))
    }

    fn open_existing(path: PathBuf) -> Result<Self, StorageError> {
        let mut file = DirectFile::open_existing(path.clone())?;
        let mut header_bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
        file.read_exact_at(0, &mut header_bytes)?;
        let header = FileHeader::from_le_bytes(&header_bytes);

        if header.magic != crate::storage_layout::FILE_MAGIC {
            return Err(StorageError::InvalidFile("bad magic".to_string()));
        }
        if header.page_size as usize != crate::storage_layout::PAGE_SIZE {
            return Err(StorageError::InvalidFile("page size mismatch".to_string()));
        }
        if header.header_checksum != header.compute_checksum() {
            return Err(StorageError::InvalidFile(
                "header checksum mismatch".to_string(),
            ));
        }
        // Validate the layout before any destructive step (the v1 upgrade
        // mutates the file; a file we cannot operate must be rejected while
        // it is still untouched v1).
        let layout = layout_from_header(&header, file.len());
        validate_allocator_region(&layout)?;

        let mut sb_a_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
        file.read_exact_at(header.superblock_a_offset, &mut sb_a_bytes)?;
        let mut superblock_a = Superblock::from_le_bytes(&sb_a_bytes);
        let sb_a_valid = superblock_a.checksum == superblock_a.compute_checksum();

        let mut sb_b_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
        file.read_exact_at(header.superblock_b_offset, &mut sb_b_bytes)?;
        let mut superblock_b = Superblock::from_le_bytes(&sb_b_bytes);
        let sb_b_valid = superblock_b.checksum == superblock_b.compute_checksum();

        if !sb_a_valid && !sb_b_valid {
            return Err(StorageError::InvalidFile(
                "both superblocks have checksum mismatch".to_string(),
            ));
        }

        let mut active_superblock = ActiveSuperblock::from_superblocks(
            &superblock_a,
            &superblock_b,
            sb_a_valid,
            sb_b_valid,
        );

        if header.version == crate::storage_layout::FILE_VERSION_V1 {
            let active_sb = match active_superblock {
                ActiveSuperblock::A => superblock_a,
                ActiveSuperblock::B => superblock_b,
            };
            (superblock_a, superblock_b) =
                upgrade_v1_to_v2(&mut file, header, &layout, &active_sb)?;
            active_superblock = ActiveSuperblock::A;
        }
        let active_sb = match active_superblock {
            ActiveSuperblock::A => &superblock_a,
            ActiveSuperblock::B => &superblock_b,
        };

        // Recover the true WAL tail: trust the superblock's tail as a lower
        // bound and scan forward (CRC + LSN self-identity) past it.
        let mut log_file = DirectFile::open_existing(path.clone())?;
        let scan = scan_ring(
            &mut log_file,
            layout.wal_offset,
            layout.wal_size,
            active_sb.wal_head,
            active_sb.wal_tail,
        )?;
        let wal = WalWriter::open(
            log_file,
            layout.wal_offset,
            layout.wal_size,
            active_sb.wal_head,
            scan.tail,
        )?;
        let recorded_tail = active_sb.wal_tail;

        // The catalog root is stored as an absolute file offset (0 = none).
        let mut rel = RelState::new(DEFAULT_CAPACITY_BYTES);
        if active_sb.metadata_root != 0 {
            if active_sb.metadata_root < layout.data_offset
                || active_sb.metadata_root >= layout.data_offset + layout.data_size
            {
                return Err(StorageError::InvalidFile(
                    "catalog root outside the data region".to_string(),
                ));
            }
            rel.catalog_root =
                Some((active_sb.metadata_root - layout.data_offset) / PAGE_SIZE as u64);
        }

        let mut version = crate::relstore::version::VersionState::default();
        version.set_options_byte(active_sb.db_options());
        let mut storage = StorageFile {
            default_collation: header.default_collation(),
            file,
            wal,
            layout,
            superblock_a,
            superblock_b,
            active_superblock,
            allocator: PageAllocator::new(layout.data_size),
            rel,
            replay_cache: scan.records,
            version,
        };
        storage.recover_allocator()?;

        // A tail below the superblock's recorded one means part of the
        // trusted region was lost (media corruption, or a v1 file whose
        // superblock ran ahead of durability). Persist the corrected tail
        // now: otherwise entries appended at it could crash-recover with the
        // old superblock and stale entries beyond them would be replayed as
        // trusted.
        if scan.tail < recorded_tail {
            let last_seq = storage
                .replay_cache
                .iter()
                .map(|r| r.seq_no)
                .max()
                .unwrap_or(0);
            storage.write_active_superblock(last_seq)?;
            storage.file.sync_data()?;
        }

        // ARIES restart for the relational store: analysis + redo, catalog
        // reload, undo of losers with compensation logging.
        storage.recover_rel()?;
        Ok(storage)
    }

    fn create_new(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
    ) -> Result<Self, StorageError> {
        let layout = compute_layout(opts.clone(), wal_min_bytes, wal_max_bytes)?;
        validate_allocator_region(&layout)?;
        let mut header = FileHeader::default();
        // Stamp the database's default collation into the file. Every character
        // column declared without an explicit COLLATE is keyed under it, so it
        // belongs to the data, not to whatever the config says at the next boot.
        if let Some(name) = opts.default_collation.as_deref() {
            header
                .set_default_collation(name)
                .map_err(StorageError::InvalidConfig)?;
        }
        header.superblock_a_offset = layout.superblock_a_offset;
        header.superblock_b_offset = layout.superblock_b_offset;
        header.wal_offset = layout.wal_offset;
        header.wal_size = layout.wal_size;
        header.data_offset = layout.data_offset;
        header.data_size = layout.data_size;
        header.metadata_offset = layout.metadata_offset;
        header.metadata_size = layout.metadata_size;
        header.allocator_offset = layout.allocator_offset;
        header.allocator_size = layout.allocator_size;
        header.snapshot_offset = layout.snapshot_offset;
        header.snapshot_size = layout.snapshot_size;
        header.reserved_offset = layout.reserved_offset;
        header.reserved_size = layout.reserved_size;
        header.header_checksum = header.compute_checksum();

        let mut superblock_a = Superblock::default();
        superblock_a.checksum = superblock_a.compute_checksum();
        let mut superblock_b = Superblock::default();
        superblock_b.active = SUPERBLOCK_ACTIVE_B;
        superblock_b.checksum = superblock_b.compute_checksum();

        let mut file = DirectFile::create_new(path.clone(), layout.total_size)?;
        file.write_all_at(layout.header_offset, &header.to_le_bytes_with_checksum())?;
        file.write_all_at(
            layout.superblock_a_offset,
            &superblock_a.to_le_bytes_with_checksum(),
        )?;
        file.write_all_at(
            layout.superblock_b_offset,
            &superblock_b.to_le_bytes_with_checksum(),
        )?;
        file.sync_data()?;

        let log_file = DirectFile::open_existing(path.clone())?;
        let wal = WalWriter::open(log_file, layout.wal_offset, layout.wal_size, 0, 0)?;

        Ok(StorageFile {
            default_collation: header.default_collation(),
            file,
            wal,
            layout,
            superblock_a,
            superblock_b,
            active_superblock: ActiveSuperblock::A,
            allocator: PageAllocator::new(layout.data_size),
            rel: RelState::new(DEFAULT_CAPACITY_BYTES),
            replay_cache: Vec::new(),
            version: crate::relstore::version::VersionState::default(),
        })
    }

    /// Builds the relational execution context over this file's parts.
    fn rel_ctx(&mut self) -> RelCtx<'_> {
        RelCtx {
            pool: &mut self.rel.pool,
            io: PoolIo {
                file: &mut self.file,
                wal: &mut self.wal,
                data_offset: self.layout.data_offset,
                data_pages: self.layout.data_size / PAGE_SIZE as u64,
            },
            allocator: &mut self.allocator,
            dpt: &mut self.rel.dpt,
            use_reserve: false,
        }
    }

    /// Decodes the relational records (with their LSNs) from the recovery
    /// scan.
    fn rel_records(&self) -> Result<Vec<(u64, RelRecord)>, StorageError> {
        self.replay_cache
            .iter()
            .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
            .map(|record| Ok((record.logical_ts, RelRecord::decode(&record.payload)?)))
            .collect()
    }

    /// ARIES restart: analysis + redo (repeating history), then undo of
    /// loser transactions with CLRs. The catalog is loaded between redo and
    /// undo (undo needs tree roots) and reloaded after (undo may have
    /// removed catalog rows).
    fn recover_rel(&mut self) -> Result<(), StorageError> {
        let records = self.rel_records()?;
        if records.is_empty() && self.rel.catalog_root.is_none() {
            return Ok(());
        }

        let outcome = {
            let mut ctx = self.rel_ctx();
            rel_recovery::analyze_and_redo(&mut ctx, &records)?
        };
        if let Some(root) = outcome.catalog_root {
            self.rel.catalog_root = Some(root);
        }
        self.rel.next_txn_id = outcome.max_txn_id + 1;

        self.reload_catalog()?;
        if !outcome.losers.is_empty() {
            let roots = self.rel.tree_roots();
            let mut ctx = self.rel_ctx();
            rel_recovery::undo_losers(&mut ctx, &records, &outcome.losers, &roots)?;
            self.reload_catalog()?;
        }
        // Object ids are shared by tables and their secondary indexes, so the
        // next id must clear both (an index can outrank every table).
        self.rel.next_object_id = self
            .rel
            .tables
            .values()
            .flat_map(|def| {
                std::iter::once(def.object_id)
                    .chain(def.indexes.iter().map(|index| index.object_id))
            })
            .map(|object_id| object_id + 1)
            .max()
            .unwrap_or(FIRST_USER_OBJECT_ID)
            .max(FIRST_USER_OBJECT_ID);
        self.wal.sync_all()?;
        Ok(())
    }

    fn reload_catalog(&mut self) -> Result<(), StorageError> {
        let Some(root) = self.rel.catalog_root else {
            self.rel.tables.clear();
            return Ok(());
        };
        let defs = {
            let mut ctx = self.rel_ctx();
            catalog::load_tables(&mut ctx, root)?
        };
        self.rel.tables = defs
            .into_iter()
            .map(|def| (def.name.clone(), def))
            .collect();
        Ok(())
    }

    /// Runs one autocommit relational statement: begin, ops, commit (force
    /// log); statement failure rolls back through the in-memory undo log.
    fn rel_statement<T>(
        &mut self,
        f: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let txn_id = self.rel.next_txn_id;
        self.rel.next_txn_id += 1;
        let roots = self.rel.tree_roots();
        let mut ctx = self.rel_ctx();
        let mut txn = ctx.begin(txn_id)?;
        // Staged versions publish only on a successful commit — and inside
        // this same mutex hold, so no reader can see pages and version
        // chains disagree. A statement error discards them with `txn`.
        let mut publish: Option<(Vec<PendingVersion>, u64)> = None;
        let (result, wedged) = match f(&mut ctx, &mut txn) {
            Ok(value) => {
                let pending = std::mem::take(&mut txn.pending_versions);
                match ctx.commit(txn) {
                    Ok(commit_lsn) => {
                        publish = Some((pending, commit_lsn));
                        (Ok(value), false)
                    }
                    // The commit record may or may not have reached the disk;
                    // writing CLRs now could undo a durable commit. Wedge and
                    // let restart recovery decide (commit durable -> winner,
                    // else -> loser undone).
                    Err(err) => (Err(err), true),
                }
            }
            Err(err) => match rel_recovery::rollback(&mut ctx, txn, &roots) {
                Ok(()) => {
                    let _ = ctx.io.wal.sync_all();
                    (Err(err), false)
                }
                // Half-rolled-back state in the pool that the WAL cannot
                // explain: nothing relational may proceed (a checkpoint
                // would make it permanent).
                Err(rollback_err) => (Err(rollback_err), true),
            },
        };
        let _ = ctx;
        if wedged {
            self.rel.wedged = true;
        }
        if let Some((pending, commit_lsn)) = publish {
            for version in pending {
                // Autocommit: the transaction is already committed, so the
                // publish records (rollback bookkeeping) are not needed.
                let _ = self.version.publish(version, txn_id);
            }
            self.version.record_commit(txn_id, commit_lsn);
        }
        result
    }

    /// Runs one statement under `scope`: autocommit (begin+commit) or appended
    /// to a caller-held transaction. In both cases the statement is *atomic* — an
    /// error undoes its own partial writes. For the explicit transaction, the
    /// undo is a partial rollback to a savepoint taken before the statement; the
    /// transaction stays open (the SQL layer decides, per `SET XACT_ABORT`,
    /// whether to continue or doom it), so a failed statement never leaves partial
    /// rows behind.
    fn rel_statement_scoped<T>(
        &mut self,
        scope: &mut TxnScope,
        f: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        match scope {
            TxnScope::Auto => self.rel_statement(f),
            TxnScope::Explicit(stx) => {
                let roots = self.rel.tree_roots();
                let mut ctx = self.rel_ctx();
                let savepoint = stx.txn.savepoint();
                let (result, wedged) = match f(&mut ctx, &mut stx.txn) {
                    Ok(value) => (Ok(value), false),
                    Err(err) => {
                        // The failed statement's staged versions die with it.
                        stx.txn.pending_versions.clear();
                        match rel_recovery::rollback_to(&mut ctx, &mut stx.txn, savepoint, &roots) {
                            Ok(()) => (Err(err), false),
                            // A half-undone statement the WAL cannot explain:
                            // wedge the engine (a checkpoint would make it
                            // permanent), mirroring the autocommit path.
                            Err(rollback_err) => (Err(rollback_err), true),
                        }
                    }
                };
                let _ = ctx;
                if wedged {
                    self.rel.wedged = true;
                }
                // Publish the successful statement's versions inside this
                // mutex hold (atomic with its page mutations, as far as any
                // reader can tell), stamped with the still-open transaction —
                // invisible to every snapshot until the commit is recorded,
                // which is exactly how a versioned reader sees the pre-image
                // of a row a running transaction has already changed.
                if result.is_ok() && !stx.txn.pending_versions.is_empty() {
                    let txn_id = stx.txn.txn_id;
                    let pending = std::mem::take(&mut stx.txn.pending_versions);
                    for version in pending {
                        let record = self.version.publish(version, txn_id);
                        stx.txn.published_versions.push(record);
                    }
                }
                result
            }
        }
    }

    /// Opens a multi-statement transaction (BEGIN TRAN), snapshotting tree roots
    /// for a later rollback.
    fn begin_txn(&mut self) -> Result<StorageTxn, StorageError> {
        let txn_id = self.rel.next_txn_id;
        self.rel.next_txn_id += 1;
        let roots = self.rel.tree_roots();
        let mut ctx = self.rel_ctx();
        let txn = ctx.begin(txn_id)?;
        // Track the transaction's BEGIN LSN so a checkpoint clamps the WAL head
        // to the oldest open transaction, preserving its undo records for crash
        // rollback (its uncommitted pages may still be flushed under steal).
        self.rel.active_txn_begins.insert(txn.txn_id, txn.last_lsn);
        Ok(StorageTxn { txn, roots })
    }

    /// Commits a caller-held transaction (forces the log). A failure wedges the
    /// store, as for autocommit commits.
    fn commit_txn(&mut self, stx: StorageTxn) -> Result<(), StorageError> {
        // The transaction is ending (the `StorageTxn` is consumed either way).
        self.rel.active_txn_begins.remove(&stx.txn.txn_id);
        let txn_id = stx.txn.txn_id;
        debug_assert!(
            stx.txn.pending_versions.is_empty(),
            "versions staged but never published by their statement"
        );
        let commit = {
            let mut ctx = self.rel_ctx();
            ctx.commit(stx.txn)
        };
        match commit {
            Ok(commit_lsn) => {
                // The recorded sequence is what flips this transaction's
                // published versions visible, atomically under this hold.
                self.version.record_commit(txn_id, commit_lsn);
                Ok(())
            }
            Err(err) => {
                self.rel.wedged = true;
                Err(err)
            }
        }
    }

    /// Rolls back a caller-held transaction via its in-memory undo log (CLRs).
    fn rollback_txn(&mut self, mut stx: StorageTxn) -> Result<(), StorageError> {
        self.rel.active_txn_begins.remove(&stx.txn.txn_id);
        let txn_id = stx.txn.txn_id;
        let published = std::mem::take(&mut stx.txn.published_versions);
        let roots = stx.roots;
        let result = {
            let mut ctx = self.rel_ctx();
            match rel_recovery::rollback(&mut ctx, stx.txn, &roots) {
                Ok(()) => {
                    let _ = ctx.io.wal.sync_all();
                    Ok(())
                }
                Err(err) => Err(err),
            }
        };
        // Reverse the publications (newest first, so nested demotions unwind)
        // whether or not the physical rollback succeeded — a failure wedges
        // the store and nothing reads it again, but the chains must not claim
        // a rolled-back writer owns current rows.
        for record in published.into_iter().rev() {
            self.version.unpublish(record, txn_id);
        }
        if result.is_err() {
            self.rel.wedged = true;
        }
        result
    }

    /// Rolls a still-open transaction back to a savepoint (partial rollback,
    /// `ROLLBACK TRANSACTION <name>`). The transaction remains active — its count
    /// is untouched — so only the work done since the savepoint is undone.
    fn rollback_txn_to(
        &mut self,
        stx: &mut StorageTxn,
        savepoint: crate::relstore::ctx::Savepoint,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let txn_id = stx.txn.txn_id;
        let published = stx.txn.published_versions.split_off(
            savepoint
                .published_len
                .min(stx.txn.published_versions.len()),
        );
        let roots = stx.roots.clone();
        let result = {
            let mut ctx = self.rel_ctx();
            match rel_recovery::rollback_to(&mut ctx, &mut stx.txn, savepoint, &roots) {
                Ok(()) => {
                    let _ = ctx.io.wal.sync_all();
                    Ok(())
                }
                Err(err) => Err(err),
            }
        };
        for record in published.into_iter().rev() {
            self.version.unpublish(record, txn_id);
        }
        if result.is_err() {
            self.rel.wedged = true;
        }
        result
    }

    /// Whether any explicit transaction is open.
    #[cfg(test)]
    fn has_active_transactions(&self) -> bool {
        !self.rel.active_txn_begins.is_empty()
    }

    /// The WAL LSN a checkpoint may truncate up to: the oldest open transaction's
    /// BEGIN LSN (so its undo records survive), or the WAL tail if none is open.
    fn checkpoint_wal_head(&self) -> u64 {
        self.rel
            .active_txn_begins
            .values()
            .min()
            .copied()
            .map_or(self.wal.tail(), |oldest| oldest.min(self.wal.tail()))
    }

    fn ensure_rel_usable(&self) -> Result<(), StorageError> {
        if self.rel.wedged {
            return Err(StorageError::InvalidFile(
                "relational store wedged after a failed commit/rollback; restart to recover from the log"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Wedges the relational store: every subsequent relational op (reads too)
    /// fails until restart recovery. Reached from a group-commit fsync failure,
    /// where the commit record was already appended (so the commit-time wedge in
    /// `rel_statement`/`commit_txn` never fired) but never became durable — the
    /// in-memory state is now ahead of the log and must not be served.
    fn wedge(&mut self) {
        self.rel.wedged = true;
    }

    /// Rebuilds the live allocator: persisted bitmap, then reconciliation
    /// with the snapshot descriptors and the WAL.
    ///
    /// Order matters:
    /// 1. free the stale snapshot descriptor's extent — logically this free
    ///    belongs to the checkpoint that superseded it, which precedes every
    ///    replayed WAL record;
    /// 2. replay logged alloc/free extents (all idempotent bit operations);
    /// 3. mark the live snapshot's extent allocated last, healing the crash
    ///    window where the descriptor was written but the bitmap was not.
    fn recover_allocator(&mut self) -> Result<(), StorageError> {
        let bitmap_len = (self.layout.data_size / PAGE_SIZE as u64).div_ceil(8) as usize;
        let mut bitmap = vec![0u8; bitmap_len];
        self.file
            .read_exact_at(self.layout.allocator_offset, &mut bitmap)?;
        self.allocator = PageAllocator::from_bitmap(bitmap, self.layout.data_size);

        let descriptors = self.read_snapshot_descriptors()?;
        let live_slot = live_descriptor_slot(&descriptors);
        for (slot, desc) in descriptors.iter().enumerate() {
            let Some(desc) = desc else { continue };
            if Some(slot) != live_slot {
                let (start, pages) = self.descriptor_page_range(desc)?;
                self.allocator.free(start, pages);
            }
        }

        let rel_records: Vec<RelRecord> = self
            .replay_cache
            .iter()
            .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
            .map(|record| RelRecord::decode(&record.payload))
            .collect::<Result<_, _>>()?;
        for record in rel_records {
            match record.kind {
                REL_KIND_ALLOC_EXTENT => {
                    let (start, pages) = record.decode_extent_redo()?;
                    self.allocator.mark_used(start, pages);
                }
                REL_KIND_FREE_EXTENT => {
                    let (start, pages) = record.decode_extent_redo()?;
                    self.allocator.free(start, pages);
                }
                // Transaction/page records are ARIES recovery's business
                // (recover_rel); the allocator only replays extent state.
                _ => {}
            }
        }

        if let Some(live) = live_slot.and_then(|slot| descriptors[slot]) {
            let (start, pages) = self.descriptor_page_range(&live)?;
            self.allocator.mark_used(start, pages);
        }
        Ok(())
    }

    /// Converts a snapshot descriptor's byte extent into data-region pages.
    fn descriptor_page_range(&self, desc: &SnapshotDescriptor) -> Result<(u64, u64), StorageError> {
        let page = PAGE_SIZE as u64;
        if desc.data_offset < self.layout.data_offset
            || !desc.data_offset.is_multiple_of(page)
            || desc.data_offset + desc.data_len > self.layout.data_offset + self.layout.data_size
        {
            return Err(StorageError::InvalidFile(
                "snapshot descriptor extent outside data region".to_string(),
            ));
        }
        let start = (desc.data_offset - self.layout.data_offset) / page;
        let pages = desc.data_len.div_ceil(page);
        Ok((start, pages))
    }

    fn append_wal_entry(
        &mut self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        payload: &[u8],
    ) -> Result<u64, StorageError> {
        let lsn = self
            .wal
            .append_entry(entry_type, entry_version, seq_no, payload)?;
        if self.wal.take_superblock_due() {
            // Best-effort hint: the entry is already durable, so a failed
            // superblock rewrite must not fail the append (callers would
            // roll back state whose WAL record is durable). Recovery only
            // scans a little further.
            let _ = self.write_active_superblock(seq_no);
        }
        Ok(lsn)
    }

    fn allocate_extent(&mut self, temp: bool) -> Result<u64, StorageError> {
        if temp {
            return self.allocator.allocate_temp_extent().ok_or_else(|| {
                StorageError::InvalidConfig("data region full: cannot allocate extent".to_string())
            });
        }
        let start = self.allocator.allocate_extent().ok_or_else(|| {
            StorageError::InvalidConfig("data region full: cannot allocate extent".to_string())
        })?;
        let record = RelRecord::alloc_extent(start, EXTENT_PAGES);
        if let Err(err) = self.append_wal_entry(
            WAL_ENTRY_TYPE_REL,
            REL_WAL_ENTRY_VERSION,
            0,
            &record.encode(),
        ) {
            self.allocator.free(start, EXTENT_PAGES);
            return Err(err);
        }
        Ok(start)
    }

    fn free_extent(&mut self, start_page: u64) -> Result<(), StorageError> {
        // Log first, then mutate: a free whose record never became durable
        // must not leave the pages reusable in memory.
        let record = RelRecord::free_extent(start_page, EXTENT_PAGES);
        self.append_wal_entry(
            WAL_ENTRY_TYPE_REL,
            REL_WAL_ENTRY_VERSION,
            0,
            &record.encode(),
        )?;
        self.allocator.free(start_page, EXTENT_PAGES);
        Ok(())
    }

    fn spill_write_page(&mut self, page: u64, data: &[u8]) -> Result<(), StorageError> {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let mut frame = crate::direct_io::AlignedPageBuf::new();
        frame.as_mut_slice().copy_from_slice(data);
        let offset = self.layout.data_offset + page * PAGE_SIZE as u64;
        self.file.write_page_from(offset, &frame)?;
        Ok(())
    }

    fn spill_read_page(&mut self, page: u64, out: &mut [u8]) -> Result<(), StorageError> {
        debug_assert_eq!(out.len(), PAGE_SIZE);
        let mut frame = crate::direct_io::AlignedPageBuf::new();
        let offset = self.layout.data_offset + page * PAGE_SIZE as u64;
        self.file.read_page_into(offset, &mut frame)?;
        out.copy_from_slice(frame.as_slice());
        Ok(())
    }

    /// Applies `ALTER DATABASE SET` option changes and persists them durably
    /// in both superblocks (generation-bumped, active slot first with an
    /// fsync between — a torn first write falls back to the backup with the
    /// old options, and the un-acknowledged ALTER is simply lost).
    fn set_db_options(
        &mut self,
        rcsi: Option<bool>,
        allow_snapshot: Option<bool>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        // Build the new superblocks in LOCALS and write them BEFORE mutating
        // any in-memory state: a failed write must leave the version store,
        // the option mirrors, and the cached superblocks exactly as they
        // were (a half-applied OFF would otherwise stop publishing while
        // readers still take the versioned path — silent dirty reads), and
        // a later lazy active-slot rewrite must not leak the failed ALTER's
        // options to disk.
        let byte = {
            let mut next = self.version.options_byte();
            if let Some(on) = rcsi {
                next = (next & !1) | (on as u8);
            }
            if let Some(on) = allow_snapshot {
                next = (next & !2) | ((on as u8) << 1);
            }
            next
        };
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);
        // The lazy active-slot rewrite leaves the backup's dynamic fields
        // stale; both slots get the same generation here, so equalize them
        // from the active slot (whichever wins at open must carry the
        // freshest recovery hints).
        let (active, backup_flag) = match self.active_superblock {
            ActiveSuperblock::A => (self.superblock_a, SUPERBLOCK_ACTIVE_B),
            ActiveSuperblock::B => (self.superblock_b, SUPERBLOCK_ACTIVE_A),
        };
        let mut primary = active;
        let mut backup = active;
        backup.active = backup_flag;
        for sb in [&mut primary, &mut backup] {
            sb.set_db_options(byte);
            sb.generation = generation;
            sb.checksum = sb.compute_checksum();
        }
        let (primary_offset, backup_offset) = match self.active_superblock {
            ActiveSuperblock::A => (
                self.layout.superblock_a_offset,
                self.layout.superblock_b_offset,
            ),
            ActiveSuperblock::B => (
                self.layout.superblock_b_offset,
                self.layout.superblock_a_offset,
            ),
        };
        self.file
            .write_all_at(primary_offset, &primary.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;
        self.file
            .write_all_at(backup_offset, &backup.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;
        // Durable on disk — now commit to memory.
        match self.active_superblock {
            ActiveSuperblock::A => {
                self.superblock_a = primary;
                self.superblock_b = backup;
            }
            ActiveSuperblock::B => {
                self.superblock_b = primary;
                self.superblock_a = backup;
            }
        }
        self.version.set_options(rcsi, allow_snapshot);
        Ok(())
    }

    /// Lazily rewrites the active superblock in place (no fsync: it is a
    /// recovery-scan optimization, not a durability point; a torn write
    /// falls back to the other superblock).
    fn write_active_superblock(&mut self, last_committed_seq: u64) -> Result<(), StorageError> {
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);

        let (head, tail) = (self.wal.head(), self.wal.tail());
        let (sb, offset) = match self.active_superblock {
            ActiveSuperblock::A => (&mut self.superblock_a, self.layout.superblock_a_offset),
            ActiveSuperblock::B => (&mut self.superblock_b, self.layout.superblock_b_offset),
        };
        sb.generation = generation;
        sb.active = match self.active_superblock {
            ActiveSuperblock::A => SUPERBLOCK_ACTIVE_A,
            ActiveSuperblock::B => SUPERBLOCK_ACTIVE_B,
        };
        sb.wal_head = head;
        sb.wal_tail = tail;
        sb.last_committed_seq = last_committed_seq;
        sb.checksum = sb.compute_checksum();
        let bytes = sb.to_le_bytes_with_checksum();
        self.wal.file_mut().write_all_at(offset, &bytes)?;
        Ok(())
    }

    fn write_checkpoint(
        &mut self,
        data: &[u8],
        checkpoint_seq: u64,
        next_seq_no: u64,
        next_doc_id: u64,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        if data.is_empty() {
            // An empty snapshot would produce a descriptor that
            // SnapshotDescriptor::is_valid rejects while the WAL head still
            // advances — silently reviving the previous snapshot on reopen.
            return Err(StorageError::InvalidConfig(
                "checkpoint data must not be empty".to_string(),
            ));
        }
        let page = PAGE_SIZE as u64;
        let num_pages = (data.len() as u64).div_ceil(page);

        // Mint the generation above everything durable: both superblocks AND
        // both descriptors. A crash between descriptor fsync and superblock
        // publish leaves a descriptor generation ahead of the superblocks;
        // minting from superblocks alone could then duplicate a live
        // descriptor generation.
        let descriptors = self.read_snapshot_descriptors()?;
        let previous = live_descriptor_slot(&descriptors).and_then(|slot| descriptors[slot]);
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .max(
                descriptors
                    .iter()
                    .flatten()
                    .map(|d| d.generation)
                    .max()
                    .unwrap_or(0),
            )
            .saturating_add(1);

        // The snapshot is an ordinary allocator extent now; the old snapshot
        // stays allocated (and readable) until the new one is durable.
        let alloc_start = self.allocator.allocate(num_pages).ok_or_else(|| {
            StorageError::InvalidConfig(
                "cannot allocate contiguous pages for checkpoint".to_string(),
            )
        })?;
        let data_write_offset = self.layout.data_offset + alloc_start * page;

        // Phase 1a — snapshot pages + fsync. A failure here may roll the
        // allocation back: nothing references the extent yet.
        let write_data = self
            .write_data_pages(data_write_offset, data)
            .and_then(|()| self.file.sync_data().map_err(StorageError::from));
        if let Err(err) = write_data {
            self.allocator.free(alloc_start, num_pages);
            return Err(err);
        }

        // Phase 1b — descriptor write + fsync. From the moment the write is
        // *issued* the descriptor may be durable regardless of any error we
        // observe, so from here on there is no rollback: a failure leaves the
        // extent allocated (worst case a leak until the next successful
        // checkpoint) and recovery reconciles from whichever descriptor won.
        let desc_offset = self.write_snapshot_descriptor(
            data,
            data_write_offset,
            &previous,
            generation,
            checkpoint_seq,
            next_seq_no,
            next_doc_id,
        )?;

        // Phase 2 — the new snapshot is authoritative from here on. A crash
        // or error leaves state that `recover_allocator` reconciles on the
        // next open.
        self.finish_checkpoint(
            previous,
            generation,
            checkpoint_seq,
            desc_offset,
            data_write_offset,
        )
    }

    /// Writes the new snapshot descriptor into the slot not currently live
    /// and fsyncs it. Once durable, its higher generation makes the new
    /// snapshot authoritative.
    #[allow(clippy::too_many_arguments)]
    fn write_snapshot_descriptor(
        &mut self,
        data: &[u8],
        data_write_offset: u64,
        previous: &Option<SnapshotDescriptor>,
        generation: u64,
        checkpoint_seq: u64,
        next_seq_no: u64,
        next_doc_id: u64,
    ) -> Result<u64, StorageError> {
        let target_slot: u8 = match previous {
            Some(desc) if desc.slot == 0 => 1,
            Some(_) => 0,
            None => 0,
        };
        let mut desc = SnapshotDescriptor::default();
        desc.generation = generation;
        desc.slot = target_slot;
        desc.checkpoint_seq = checkpoint_seq;
        desc.data_offset = data_write_offset;
        desc.data_len = data.len() as u64;
        desc.data_checksum = xxh64(data, 0);
        desc.next_seq_no = next_seq_no;
        desc.next_doc_id = next_doc_id;
        desc.checksum = desc.compute_checksum();
        let desc_offset =
            self.layout.snapshot_offset + (target_slot as u64) * SNAPSHOT_DESCRIPTOR_SIZE as u64;
        self.file
            .write_all_at(desc_offset, &desc.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;
        Ok(desc_offset)
    }

    /// Checkpoint phase 2: reclaim the previous snapshot, persist the
    /// allocator bitmap, advance the WAL head and publish both superblocks.
    fn finish_checkpoint(
        &mut self,
        previous: Option<SnapshotDescriptor>,
        generation: u64,
        checkpoint_seq: u64,
        desc_offset: u64,
        data_write_offset: u64,
    ) -> Result<(), StorageError> {
        // Flush every dirty relational page (WAL-before-data enforced per page
        // by the pool) and reset the dirty-page table: the next change to any
        // page starts a fresh FPI epoch. Flushing an *uncommitted* page is safe
        // (ARIES steal) because the WAL head is clamped below to the oldest open
        // transaction's begin LSN, so its undo records survive to roll it back.
        self.wal.sync_all()?;
        {
            let RelState { pool, dpt, .. } = &mut self.rel;
            let mut io = PoolIo {
                file: &mut self.file,
                wal: &mut self.wal,
                data_offset: self.layout.data_offset,
                data_pages: self.layout.data_size / PAGE_SIZE as u64,
            };
            pool.flush_all(&mut io)?;
            dpt.clear();
        }

        // Free the previous snapshot's extent now that the new one is
        //    durable, and persist the allocator bitmap (temp extents
        //    excluded). The bitmap must be durable before the WAL head
        //    advances, otherwise logged alloc/free records could be
        //    reclaimed before their effects are persisted anywhere.
        if let Some(prev) = &previous {
            let (start, pages) = self.descriptor_page_range(prev)?;
            self.allocator.free(start, pages);
        }
        let bitmap = self.allocator.persistable_bitmap();
        if bitmap.len() as u64 > self.layout.allocator_size {
            return Err(StorageError::InvalidFile(
                "allocator bitmap exceeds allocator region".to_string(),
            ));
        }
        self.file
            .write_all_at(self.layout.allocator_offset, &bitmap)?;
        self.file.sync_data()?;

        // 4. Advance the WAL head — to the tail, or clamped to the oldest open
        //    transaction's begin LSN so its undo survives (fuzzy checkpoint) —
        //    and publish both superblocks (new active first).
        self.wal.set_head(self.checkpoint_wal_head());
        let new_active = match self.active_superblock {
            ActiveSuperblock::A => ActiveSuperblock::B,
            ActiveSuperblock::B => ActiveSuperblock::A,
        };
        self.active_superblock = new_active;

        let (head, tail) = (self.wal.head(), self.wal.tail());
        // Catalog root as an absolute file offset (0 = none).
        let metadata_root = self
            .rel
            .catalog_root
            .map(|page| self.layout.data_offset + page * PAGE_SIZE as u64)
            .unwrap_or(0);
        let db_options = self.version.options_byte();
        let new_sb = |active_flag: u8| -> Superblock {
            let mut sb = Superblock {
                generation,
                active: active_flag,
                wal_head: head,
                wal_tail: tail,
                last_committed_seq: checkpoint_seq,
                snapshot_root: desc_offset,
                data_root: data_write_offset,
                metadata_root,
                ..Superblock::default()
            };
            sb.set_db_options(db_options);
            sb.checksum = sb.compute_checksum();
            sb
        };
        self.superblock_a = new_sb(SUPERBLOCK_ACTIVE_A);
        self.superblock_b = new_sb(SUPERBLOCK_ACTIVE_B);

        let (primary_offset, primary_sb, backup_offset, backup_sb) = match new_active {
            ActiveSuperblock::A => (
                self.layout.superblock_a_offset,
                self.superblock_a,
                self.layout.superblock_b_offset,
                self.superblock_b,
            ),
            ActiveSuperblock::B => (
                self.layout.superblock_b_offset,
                self.superblock_b,
                self.layout.superblock_a_offset,
                self.superblock_a,
            ),
        };
        self.file
            .write_all_at(primary_offset, &primary_sb.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;
        self.file
            .write_all_at(backup_offset, &backup_sb.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;

        Ok(())
    }

    /// Writes `data` (zero-padded to whole pages) at a page-aligned offset
    /// using batched page writes.
    fn write_data_pages(&mut self, offset: u64, data: &[u8]) -> Result<(), StorageError> {
        const BATCH_FRAMES: usize = 64;
        let mut frames: Vec<AlignedPageBuf> = Vec::with_capacity(BATCH_FRAMES);
        let mut batch_start = offset;
        let mut cursor = 0usize;
        let total_pages = data.len().div_ceil(PAGE_SIZE);
        for _ in 0..total_pages {
            let mut frame = AlignedPageBuf::new();
            let len = (data.len() - cursor).min(PAGE_SIZE);
            frame.as_mut_slice()[..len].copy_from_slice(&data[cursor..cursor + len]);
            cursor += len;
            frames.push(frame);
            if frames.len() == BATCH_FRAMES {
                let refs: Vec<&AlignedPageBuf> = frames.iter().collect();
                self.file.write_pages_from(batch_start, &refs)?;
                batch_start += (BATCH_FRAMES * PAGE_SIZE) as u64;
                frames.clear();
            }
        }
        if !frames.is_empty() {
            let refs: Vec<&AlignedPageBuf> = frames.iter().collect();
            self.file.write_pages_from(batch_start, &refs)?;
        }
        Ok(())
    }

    fn read_snapshot_descriptors(
        &mut self,
    ) -> Result<[Option<SnapshotDescriptor>; 2], StorageError> {
        let mut out = [None, None];
        for (slot, entry) in out.iter_mut().enumerate() {
            let desc_offset =
                self.layout.snapshot_offset + slot as u64 * SNAPSHOT_DESCRIPTOR_SIZE as u64;
            if desc_offset + SNAPSHOT_DESCRIPTOR_SIZE as u64
                > self.layout.snapshot_offset + self.layout.snapshot_size
            {
                continue;
            }
            let mut desc_bytes = [0u8; SNAPSHOT_DESCRIPTOR_SIZE];
            self.file.read_exact_at(desc_offset, &mut desc_bytes)?;
            let desc = SnapshotDescriptor::from_le_bytes(&desc_bytes);
            if desc.is_valid() {
                *entry = Some(desc);
            }
        }
        Ok(out)
    }

    fn load_active_snapshot_descriptor(
        &mut self,
    ) -> Result<Option<SnapshotDescriptor>, StorageError> {
        let descriptors = self.read_snapshot_descriptors()?;
        Ok(live_descriptor_slot(&descriptors).and_then(|slot| descriptors[slot]))
    }

    fn load_snapshot(&mut self) -> Result<Option<SnapshotData>, StorageError> {
        let desc = match self.load_active_snapshot_descriptor()? {
            Some(d) => d,
            None => return Ok(None),
        };

        let mut data = vec![0u8; desc.data_len as usize];
        self.file.read_exact_at(desc.data_offset, &mut data)?;

        let actual_checksum = xxh64(&data, 0);
        if actual_checksum != desc.data_checksum {
            return Err(StorageError::InvalidFile(
                "snapshot data checksum mismatch".to_string(),
            ));
        }

        Ok(Some(SnapshotData {
            data,
            checkpoint_seq: desc.checkpoint_seq,
            next_seq_no: desc.next_seq_no,
            next_doc_id: desc.next_doc_id,
        }))
    }
}

/// In-place v1 -> v2 upgrade:
/// 1. zero the allocator bitmap (v1's half-slot bookkeeping is meaningless
///    in v2 — `recover_allocator` re-marks the live snapshot extent);
/// 2. rewrite BOTH superblocks from the active one — v1's backup superblock
///    may be arbitrarily stale, and any later fallback to it would silently
///    drop fsync-acknowledged v1 WAL entries (v1 entries beyond a stale tail
///    cannot pass the v2 forward scan);
/// 3. stamp the new version.
///
/// Each step is fsync-fenced; a crash before step 3 leaves a v1 file and the
/// upgrade re-runs idempotently.
fn upgrade_v1_to_v2(
    file: &mut DirectFile,
    mut header: FileHeader,
    layout: &StorageLayout,
    active_sb: &Superblock,
) -> Result<(Superblock, Superblock), StorageError> {
    let zero_page = AlignedPageBuf::new();
    let mut offset = header.allocator_offset;
    let end = header.allocator_offset + header.allocator_size;
    while offset < end {
        file.write_page_from(offset, &zero_page)?;
        offset += PAGE_SIZE as u64;
    }
    file.sync_data()?;

    let generation = active_sb.generation.saturating_add(1);
    let new_sb = |active_flag: u8| -> Superblock {
        let mut sb = *active_sb;
        sb.generation = generation;
        sb.active = active_flag;
        sb.checksum = sb.compute_checksum();
        sb
    };
    let superblock_a = new_sb(SUPERBLOCK_ACTIVE_A);
    let superblock_b = new_sb(SUPERBLOCK_ACTIVE_B);
    file.write_all_at(
        layout.superblock_a_offset,
        &superblock_a.to_le_bytes_with_checksum(),
    )?;
    file.sync_data()?;
    file.write_all_at(
        layout.superblock_b_offset,
        &superblock_b.to_le_bytes_with_checksum(),
    )?;
    file.sync_data()?;

    header.version = crate::storage_layout::FILE_VERSION;
    header.header_checksum = header.compute_checksum();
    file.write_all_at(0, &header.to_le_bytes_with_checksum())?;
    file.sync_data()?;
    Ok((superblock_a, superblock_b))
}

/// Picks the live snapshot descriptor slot by the highest
/// `(generation, slot index)` — one total order shared by every consumer
/// (allocator recovery and snapshot loading), so they can never disagree on
/// which descriptor is authoritative, even in the face of legacy duplicate
/// generations.
fn live_descriptor_slot(descriptors: &[Option<SnapshotDescriptor>; 2]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (slot, desc) in descriptors.iter().enumerate() {
        let Some(desc) = desc else { continue };
        let better = match best {
            None => true,
            Some(b) => {
                let current = descriptors[b].as_ref().expect("best slot is valid");
                (desc.generation, slot) > (current.generation, b)
            }
        };
        if better {
            best = Some(slot);
        }
    }
    best
}

fn layout_from_header(header: &FileHeader, total_size: u64) -> StorageLayout {
    StorageLayout {
        total_size,
        header_offset: 0,
        superblock_a_offset: header.superblock_a_offset,
        superblock_b_offset: header.superblock_b_offset,
        wal_offset: header.wal_offset,
        wal_size: header.wal_size,
        data_offset: header.data_offset,
        data_size: header.data_size,
        metadata_offset: header.metadata_offset,
        metadata_size: header.metadata_size,
        allocator_offset: header.allocator_offset,
        allocator_size: header.allocator_size,
        snapshot_offset: header.snapshot_offset,
        snapshot_size: header.snapshot_size,
        reserved_offset: header.reserved_offset,
        reserved_size: header.reserved_size,
    }
}

fn validate_allocator_region(layout: &StorageLayout) -> Result<(), StorageError> {
    let bitmap_len = (layout.data_size / PAGE_SIZE as u64).div_ceil(8);
    if bitmap_len > layout.allocator_size {
        return Err(StorageError::InvalidConfig(format!(
            "allocator region ({} bytes) too small for data-region bitmap ({bitmap_len} bytes)",
            layout.allocator_size
        )));
    }
    Ok(())
}

fn compute_layout(
    opts: StorageOptions,
    wal_min_bytes: u64,
    wal_max_bytes: u64,
) -> Result<StorageLayout, StorageError> {
    let total_size = opts
        .size_gib
        .checked_mul(crate::storage_layout::GIB)
        .ok_or_else(|| StorageError::InvalidConfig("storage.size_gib overflow".to_string()))?;

    let page = crate::storage_layout::PAGE_SIZE as u64;
    let fixed_size = page * 3;
    if total_size <= fixed_size {
        return Err(StorageError::InvalidConfig(
            "storage.size_gib too small for header/superblocks".to_string(),
        ));
    }

    let wal_raw = (total_size as f64 * opts.wal_ratio) as u64;
    let wal_clamped = wal_raw.clamp(wal_min_bytes, wal_max_bytes);
    let wal_size = align_down(wal_clamped, page);

    let metadata_size = align_down((total_size as f64 * opts.metadata_ratio) as u64, page);
    let snapshot_size = align_down((total_size as f64 * opts.snapshot_ratio) as u64, page);
    let allocator_size = align_down((total_size as f64 * opts.allocator_ratio) as u64, page);
    let reserved_target = align_down((total_size as f64 * opts.reserved_ratio) as u64, page);

    let mut remaining = total_size
        .saturating_sub(fixed_size)
        .saturating_sub(wal_size)
        .saturating_sub(metadata_size)
        .saturating_sub(snapshot_size)
        .saturating_sub(allocator_size)
        .saturating_sub(reserved_target);

    remaining = align_down(remaining, page);

    if remaining == 0 {
        return Err(StorageError::InvalidConfig(
            "storage ratios leave no space for data region".to_string(),
        ));
    }

    let reserved_size = total_size
        .saturating_sub(fixed_size)
        .saturating_sub(wal_size)
        .saturating_sub(metadata_size)
        .saturating_sub(snapshot_size)
        .saturating_sub(allocator_size)
        .saturating_sub(remaining);

    let header_offset = 0;
    let superblock_a_offset = page;
    let superblock_b_offset = page * 2;
    let wal_offset = page * 3;
    let data_offset = wal_offset + wal_size;
    let metadata_offset = data_offset + remaining;
    let allocator_offset = metadata_offset + metadata_size;
    let snapshot_offset = allocator_offset + allocator_size;
    let reserved_offset = snapshot_offset + snapshot_size;

    Ok(StorageLayout {
        total_size,
        header_offset,
        superblock_a_offset,
        superblock_b_offset,
        wal_offset,
        wal_size,
        data_offset,
        data_size: remaining,
        metadata_offset,
        metadata_size,
        allocator_offset,
        allocator_size,
        snapshot_offset,
        snapshot_size,
        reserved_offset,
        reserved_size,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::storage_layout::{
        FILE_HEADER_SIZE, FILE_VERSION, FILE_VERSION_V1, SUPERBLOCK_SIZE, WAL_ENTRY_TYPE_RECORD,
        WalEntryFooter, WalEntryHeader, wal_entry_padded_len, wal_payload_crc,
    };

    /// Small ring so wrap/full paths are cheap to reach.
    const TEST_WAL_BYTES: u64 = 64 * 1024;

    fn test_storage_options() -> StorageOptions {
        StorageOptions {
            size_gib: 1,
            wal_ratio: 0.05,
            metadata_ratio: 0.08,
            snapshot_ratio: 0.02,
            allocator_ratio: 0.02,
            reserved_ratio: 0.17,
            default_collation: None,
        }
    }

    /// ALTER TABLE ADD survives a restart — the widened schema and the frozen
    /// fills are one durable statement — and a failure mid-rewrite rolls the
    /// whole ALTER back: old schema, old rows, fully readable.
    #[test]
    fn alter_add_column_is_durable_and_atomic() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("alter-add");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))",
            "INSERT INTO t VALUES (1, 'one'), (2, 'two')",
            "ALTER TABLE t ADD score INT DEFAULT 7",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        drop(storage);

        // Reopen: the widened schema and the frozen fill survived.
        let storage = Storage::open(path.clone()).expect("reopen");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT id, score FROM t ORDER BY id", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => assert_eq!(
                rowset.rows,
                vec![
                    vec![Datum::Int(1), Datum::Int(7)],
                    vec![Datum::Int(2), Datum::Int(7)],
                ]
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // A failure mid-rewrite (fault injection inside the ALTER's statement)
        // rolls the whole thing back: the new column does not exist and the
        // old rows are intact.
        crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(Some(1)));
        let outcome = execute_batch(&storage, "ALTER TABLE t ADD flag BIT DEFAULT 1", &mut ctx);
        crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(None));
        assert!(outcome.error.is_some(), "the injected failure must surface");

        let outcome = execute_batch(&storage, "SELECT flag FROM t", &mut ctx);
        assert!(
            outcome.error.is_some(),
            "the rolled-back column must not exist: {:?}",
            outcome.results
        );
        let outcome = execute_batch(&storage, "SELECT id, score FROM t ORDER BY id", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => assert_eq!(rowset.rows.len(), 2),
            other => panic!("expected rows, got {other:?}"),
        }

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The EXEC text path's two load-bearing lock properties, pinned: a
    /// variable @stmt cannot be analyzed up front and takes the conservative
    /// database-exclusive lock, and isolation escalation crosses the EXEC
    /// boundary in both directions (mutating either previously went green).
    #[test]
    fn exec_lock_analysis_never_under_locks() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("exec-locks");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // A variable statement text is unknowable before it runs: the batch
        // locks the database exclusively rather than ever under-locking.
        let needs = crate::rel::analyze_locks(
            &storage,
            "DECLARE @s NVARCHAR(50) = N'SELECT v FROM t'; EXEC sp_executesql @s",
            Isolation::ReadCommitted,
        );
        assert!(
            needs.contains(&(Resource::Database, LockMode::Exclusive)),
            "variable @stmt must take Database X: {needs:?}"
        );

        // Direction 1: a SET raise BEFORE the EXEC locks the inner reads.
        let needs = crate::rel::analyze_locks(
            &storage,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; EXEC sp_executesql N'SELECT v FROM t'",
            Isolation::ReadUncommitted,
        );
        assert!(
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
            "escalated isolation must lock the inner SELECT: {needs:?}"
        );

        // Direction 2 (intra-EXEC): a SET raise INSIDE the literal locks the
        // statements after it inside the same literal.
        let needs = crate::rel::analyze_locks(
            &storage,
            "EXEC sp_executesql N'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT v FROM t'",
            Isolation::ReadUncommitted,
        );
        assert!(
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
            "an inner SET raise must lock the inner reads: {needs:?}"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The Stage 13 database options round-trip the superblock: they survive
    /// a reopen, and a checkpoint — which rebuilds both superblocks from
    /// scratch — must carry them forward rather than silently resetting them.
    #[test]
    fn db_options_persist_across_reopen_and_checkpoint() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("db-options");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        assert!(!storage.rcsi_enabled());
        assert!(!storage.snapshot_isolation_allowed());
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON, ALLOW_SNAPSHOT_ISOLATION ON",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert!(storage.rcsi_enabled());
        assert!(storage.snapshot_isolation_allowed());
        drop(storage);

        let storage = Storage::open(path.clone()).expect("reopen");
        assert!(storage.rcsi_enabled(), "RCSI survives a restart");
        assert!(storage.snapshot_isolation_allowed());

        // One option off, then a checkpoint, then a reopen: the checkpoint's
        // fresh superblocks must keep the surviving option.
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        storage
            .write_checkpoint(b"cp", 1, 2, 1)
            .expect("checkpoint");
        drop(storage);
        let storage = Storage::open(path.clone()).expect("reopen after checkpoint");
        assert!(
            storage.rcsi_enabled(),
            "the checkpoint must not reset the option"
        );
        assert!(!storage.snapshot_isolation_allowed());
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// A READ COMMITTED SELECT under RCSI takes only Database IS — no Table
    /// S — which is the entire readers-don't-block mechanism; and the other
    /// levels are untouched by the option.
    #[test]
    fn analyze_locks_drops_table_s_under_rcsi() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("rcsi-locks");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        let table_s = |needs: &[(Resource, LockMode)]| {
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared)
        };

        // Off: the SELECT read-locks, as ever.
        let needs =
            crate::rel::analyze_locks(&storage, "SELECT v FROM t", Isolation::ReadCommitted);
        assert!(
            table_s(&needs),
            "without RCSI a RC SELECT takes Table S: {needs:?}"
        );

        let outcome = execute_batch(
            &storage,
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // On: Database IS only — the DDL fence — and no Table S.
        let needs =
            crate::rel::analyze_locks(&storage, "SELECT v FROM t", Isolation::ReadCommitted);
        assert!(
            !table_s(&needs),
            "under RCSI a RC SELECT takes no Table S: {needs:?}"
        );
        assert!(
            needs.contains(&(Resource::Database, LockMode::IntentShared)),
            "the Database IS fence stays: {needs:?}"
        );

        // The other levels are untouched: RR still read-locks, RU still
        // takes nothing, and a batch that raises isolation falls back to
        // locking even though the session level is RC.
        let needs =
            crate::rel::analyze_locks(&storage, "SELECT v FROM t", Isolation::RepeatableRead);
        assert!(table_s(&needs), "RR is not versioned: {needs:?}");
        let needs =
            crate::rel::analyze_locks(&storage, "SELECT v FROM t", Isolation::ReadUncommitted);
        assert!(needs.is_empty(), "RU takes no locks at all: {needs:?}");
        let needs = crate::rel::analyze_locks(
            &storage,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT v FROM t",
            Isolation::ReadCommitted,
        );
        assert!(
            table_s(&needs),
            "a raising SET disables the snapshot path: {needs:?}"
        );

        // SNAPSHOT isolation is versioned regardless of RCSI: Database IS
        // only, and the EXEC-literal recursion preserves the level (the
        // #120 review's collapse bug, from the other direction).
        let needs = crate::rel::analyze_locks(&storage, "SELECT v FROM t", Isolation::Snapshot);
        assert!(
            !table_s(&needs),
            "SNAPSHOT reads take no Table S: {needs:?}"
        );
        assert!(needs.contains(&(Resource::Database, LockMode::IntentShared)));
        let needs = crate::rel::analyze_locks(
            &storage,
            "EXEC sp_executesql N'SELECT v FROM t'",
            Isolation::Snapshot,
        );
        assert!(
            !table_s(&needs),
            "the recursion must not turn SNAPSHOT into a locking level: {needs:?}"
        );
        // ...and a SET SNAPSHOT inside a batch is not a lock-escalating
        // raise, but the batch still holds the Database IS fence.
        let needs = crate::rel::analyze_locks(
            &storage,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; SELECT v FROM t",
            Isolation::ReadUncommitted,
        );
        assert!(
            needs.contains(&(Resource::Database, LockMode::IntentShared)),
            "SET SNAPSHOT from RU keeps the DDL fence: {needs:?}"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// A covering (INCLUDE) seek must serve the snapshot's image, not the
    /// index leaf's freshly-updated include payload — pinned on the covering
    /// path itself via the covering-scan counter.
    #[test]
    fn covering_seek_serves_the_snapshot_image() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("rcsi-covering");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut writer = TxnContext::default();
        let mut fill = String::from("INSERT INTO c VALUES ");
        for i in 1..=25 {
            fill.push_str(&format!("({}, {}, {}),", i, i * 10, i * 1000));
        }
        fill.pop();
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE TABLE c (id INT NOT NULL PRIMARY KEY, v INT, w INT)",
            fill.as_str(),
            "CREATE INDEX ix_cv ON c (v) INCLUDE (v, w)",
            // The open transaction updates only the INCLUDE column: the
            // seek still finds the entry under v = 10, now carrying the NEW
            // include bytes.
            "BEGIN TRAN; UPDATE c SET w = 777 WHERE id = 1;",
        ] {
            let outcome = execute_batch(&storage, sql, &mut writer);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }

        let select = |label: &str| {
            let mut reader = TxnContext::default();
            let outcome = execute_batch(&storage, "SELECT w FROM c WHERE v = 10", &mut reader);
            assert!(outcome.error.is_none(), "{label}: {:?}", outcome.error);
            match &outcome.results[0] {
                StatementResult::Rows(rowset) => rowset.rows.clone(),
                other => panic!("{label}: expected rows, got {other:?}"),
            }
        };

        let covering_before = storage.covering_scans();
        let rows = select("during the writer's transaction");
        assert!(
            storage.covering_scans() > covering_before,
            "the read must have gone down the covering path for this to test anything"
        );
        assert_eq!(
            rows,
            vec![vec![Datum::Int(1000)]],
            "the snapshot image, not the new include payload"
        );

        let outcome = execute_batch(&storage, "COMMIT", &mut writer);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(rows_i32(&select("after the commit")), vec![777]);

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    fn rows_i32(rows: &[Vec<Datum>]) -> Vec<i32> {
        rows.iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected INT, got {other:?}"),
            })
            .collect()
    }

    /// Rollback unpublishes: a rolled-back transaction leaves no version
    /// chains behind (pruning would otherwise treat its entries as an open
    /// transaction's and pin them forever), and pruning drops the history of
    /// committed transactions once no snapshot is live.
    #[test]
    fn rollback_unpublishes_and_prune_drops_settled_history() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("rcsi-prune");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            "INSERT INTO t VALUES (1, 10), (2, 20)",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }

        // The seed INSERTs published their own (committed) chains; settle
        // them first so the rollback assertion sees only the rollback's work.
        storage
            .ensure_durable(storage.wal_tail())
            .expect("durability");
        storage.version_prune();
        assert_eq!(storage.version_chain_count("t"), 0);

        // Rolled back: the chains its statements published are reversed.
        let outcome = execute_batch(
            &storage,
            "BEGIN TRAN; UPDATE t SET v = 99 WHERE id = 1; ROLLBACK",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            storage.version_chain_count("t"),
            0,
            "a rolled-back transaction leaves no chains"
        );

        // Committed: the chain exists until pruning decides nothing can need
        // it (no live snapshot, commit durable).
        let outcome = execute_batch(&storage, "UPDATE t SET v = 11 WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(storage.version_chain_count("t"), 1);
        storage
            .ensure_durable(storage.wal_tail())
            .expect("durability");
        storage.version_prune();
        assert_eq!(
            storage.version_chain_count("t"),
            0,
            "settled history is dropped by the maintenance prune"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Publishing versions changes nothing about crash recovery: the store
    /// (chains and commit table) is memory-only, so a kill-and-reopen with
    /// RCSI on recovers exactly the committed state, options intact, chains
    /// empty.
    #[test]
    fn rcsi_survives_a_crash_with_clean_recovery() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("rcsi-crash");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            "INSERT INTO t VALUES (1, 10), (2, 20)",
            "UPDATE t SET v = 11 WHERE id = 1",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        // An open transaction with published versions dies with the crash.
        let outcome = execute_batch(
            &storage,
            "BEGIN TRAN; UPDATE t SET v = 999 WHERE id = 2;",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        // Kill without checkpoint: drop the handle mid-transaction.
        drop(ctx);
        drop(storage);

        let storage = Storage::open(path.clone()).expect("recovery");
        assert!(storage.rcsi_enabled());
        assert_eq!(
            storage.version_chain_count("t"),
            0,
            "chains do not survive a restart"
        );
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT v FROM t ORDER BY id", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => assert_eq!(
                rows_i32(&rowset.rows),
                vec![11, 20],
                "committed wins, the in-flight update is undone"
            ),
            other => panic!("expected rows, got {other:?}"),
        }
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Row-lock escalation (Stage 12), pinned at its actual threshold: a
    /// statement naming more than 1000 row keys takes ONE table lock instead
    /// of flooding the lock table; at or below the threshold it takes row
    /// locks. (The plan sketched ~5000; 1000 is the shipped value — this
    /// test is the record of that divergence.)
    #[test]
    fn row_locks_escalate_past_the_threshold() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("escalation");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        let insert = |n: usize| {
            let tuples: Vec<String> = (0..n).map(|i| format!("({i}, 0)")).collect();
            format!("INSERT INTO t VALUES {}", tuples.join(", "))
        };

        // At the threshold: per-row locks (plus the table intent).
        let needs = crate::rel::analyze_locks(&storage, &insert(1000), Isolation::ReadCommitted);
        let rows = needs
            .iter()
            .filter(|(r, _)| matches!(r, Resource::Row(_, _)))
            .count();
        assert_eq!(rows, 1000, "at the threshold every key gets a row lock");
        assert!(
            !needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive),
            "no table X below escalation: {:?}",
            needs.len()
        );

        // A single statement past the threshold: the per-statement cap
        // declines to enumerate 1001 row hashes and the INSERT falls back to
        // one table-exclusive lock. (Reachable since the node budget became
        // per-expression — a 1001-tuple INSERT parses now.)
        let needs = crate::rel::analyze_locks(&storage, &insert(1001), Isolation::ReadCommitted);
        assert!(
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive),
            "a single over-threshold statement takes table X: {needs:?}"
        );
        assert_eq!(
            needs
                .iter()
                .filter(|(r, _)| matches!(r, Resource::Row(_, _)))
                .count(),
            0
        );

        // Past it — summed across the WHOLE BATCH: a 1000-tuple INSERT plus
        // 20 point DELETEs on DISTINCT keys wants 1020 row locks on one
        // table (the needs map dedups by key hash, so overlapping keys would
        // not count twice), and the batch-level pass replaces them all with
        // one table-exclusive lock.
        let deletes: Vec<String> = (2000..2020)
            .map(|i| format!("DELETE FROM t WHERE id = {i}"))
            .collect();
        let over = format!("{}; {}", insert(1000), deletes.join("; "));
        let needs = crate::rel::analyze_locks(&storage, &over, Isolation::ReadCommitted);
        assert!(
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive),
            "past the threshold the statement takes table X: {needs:?}"
        );
        assert_eq!(
            needs
                .iter()
                .filter(|(r, _)| matches!(r, Resource::Row(_, _)))
                .count(),
            0,
            "row locks are replaced, not added to"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// EXEC scope semantics, pinned: outer variables are restored after the
    /// inner batch (a regression previously went green), and SET options
    /// revert at scope exit as SQL Server reverts them — an inner
    /// `SET XACT_ABORT ON` must not doom the outer batch's transaction.
    #[test]
    fn exec_scope_restores_variables_and_set_options() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("exec-scope");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // Inner @o shadows; the outer @o is intact after the EXEC returns.
        let outcome = execute_batch(
            &storage,
            "DECLARE @o INT = 1; EXEC sp_executesql N'DECLARE @o INT = 99; SELECT @o AS inner_o'; SELECT @o AS outer_o",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let values: Vec<i64> = outcome
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rs) => match rs.rows[0][0] {
                    Datum::Int(v) => Some(v as i64),
                    Datum::BigInt(v) => Some(v),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(values, [99, 1], "{:?}", outcome.results);

        // An inner SET XACT_ABORT ON reverts at EXEC exit: the later duplicate
        // key fails its own statement but the batch continues and commits.
        let outcome = execute_batch(
            &storage,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (1); \
             EXEC sp_executesql N'SET XACT_ABORT ON'; \
             INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT",
            &mut ctx,
        );
        assert!(outcome.error.is_some(), "the dup insert reports its error");
        let outcome = execute_batch(&storage, "SELECT COUNT(*) FROM t", &mut ctx);
        match &outcome.results[0] {
            StatementResult::Rows(rs) => assert_eq!(
                rs.rows[0][0],
                Datum::BigInt(2),
                "XACT_ABORT must revert at scope exit; the transaction commits"
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    fn unique_temp_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        path.push(format!("truthdb-storage-{label}-{nanos}.db"));
        path
    }

    fn create_small(path: &Path) -> Storage {
        Storage::create_with_wal_bounds(
            path.to_path_buf(),
            test_storage_options(),
            TEST_WAL_BYTES,
            TEST_WAL_BYTES,
        )
        .expect("create storage")
    }

    /// A covering seek (every read column INCLUDEd) answers from the index
    /// leaves alone — the counter proves the covering path ran — and returns
    /// exactly what a table scan returns, original case and NULLs included.
    #[test]
    fn a_covering_seek_answers_from_the_index_alone_and_matches_a_scan() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("include-covering");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, v INT); \
             CREATE INDEX ix ON t (email) INCLUDE (email, v); \
             INSERT INTO t VALUES (1, 'a@x.com', 10), (2, 'B@X.com', NULL), (3, 'c@x.com', 30)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);

        let rows_of = |outcome: &crate::rel::BatchOutcome| match &outcome.results[0] {
            StatementResult::Rows(rowset) => rowset.rows.clone(),
            other => panic!("expected rows, got {other:?}"),
        };

        // Sought case-insensitively, answered with the stored ORIGINAL value.
        let covered = execute_batch(
            &storage,
            "SELECT email, v FROM t WHERE email = 'b@x.com'",
            &mut ctx,
        );
        assert!(covered.error.is_none(), "{:?}", covered.error);
        assert_eq!(storage.covering_scans(), 1, "the covering path answered");
        let covered = rows_of(&covered);
        assert_eq!(
            covered,
            vec![vec![Datum::VarChar("B@X.com".into()), Datum::Null]]
        );

        // A/B: without the index the same query scans — identical rows.
        let dropped = execute_batch(&storage, "DROP INDEX ix ON t", &mut ctx);
        assert!(dropped.error.is_none(), "{:?}", dropped.error);
        let scanned = execute_batch(
            &storage,
            "SELECT email, v FROM t WHERE email = 'b@x.com'",
            &mut ctx,
        );
        assert!(scanned.error.is_none(), "{:?}", scanned.error);
        assert_eq!(rows_of(&scanned), covered, "covering == scan");
        assert_eq!(storage.covering_scans(), 1, "the scan path took over");

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The streamed input path: a filtered aggregate over a plain base table
    /// pulls the scan slice by slice through the WHERE — it must never drain
    /// the scan whole (that is what the join operators do, and what the old
    /// path did for every shape). Pinned by the materialization counter: the
    /// streamed query performs zero whole-scan drains; a join performs one
    /// per scanned input.
    #[test]
    fn a_filtered_aggregate_streams_its_input_instead_of_materializing() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("stream-input");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);
        for chunk in (1..=3000).collect::<Vec<i64>>().chunks(500) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({i}, {})", i % 7)).collect();
            let outcome = execute_batch(
                &storage,
                &format!("INSERT INTO t VALUES {}", values.join(", ")),
                &mut ctx,
            );
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
        }

        // An aggregate with a WHERE over 3000 rows (three scan slices): the
        // input streams; nothing drains the scan whole.
        let outcome = execute_batch(
            &storage,
            "SELECT COUNT(*), SUM(v) FROM t WHERE v > 3",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            storage.scan_materializations(),
            0,
            "the filtered aggregate's input streamed"
        );
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                // v cycles 1..7 over 3000 rows minus the id=1 seed row's v=1:
                // v in {4,5,6} appears 428|429 times; exact values pin the walk.
                assert_eq!(rowset.rows.len(), 1);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // A join materializes only its BUILD side (the counter's positive
        // control); the probe side streams. INNER probes from the left, so the
        // right input is the one drained.
        let outcome = execute_batch(
            &storage,
            "SELECT COUNT(*) FROM t a JOIN t b ON a.id = b.id WHERE a.v > 5",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            storage.scan_materializations(),
            1,
            "an inner hash join materializes only its build side"
        );

        // RIGHT reverses orientation: the probe is the right input, the left
        // input is built — still exactly one materialization.
        let outcome = execute_batch(
            &storage,
            "SELECT COUNT(*) FROM t a RIGHT JOIN t b ON a.id = b.id",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            storage.scan_materializations(),
            2,
            "a right join materializes only its (left) build side"
        );

        // The nested loop (no equi key) streams its probe side too. A small
        // table keeps the O(n·m) loop cheap; its base scan is still lazy.
        let setup = execute_batch(
            &storage,
            "CREATE TABLE s (id INT NOT NULL PRIMARY KEY); INSERT INTO s VALUES (1), (2), (3), (4)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);
        let outcome = execute_batch(
            &storage,
            "SELECT COUNT(*) FROM s a JOIN s b ON a.id < b.id",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => assert_eq!(rowset.rows[0][0], Datum::BigInt(6)),
            other => panic!("expected rows, got {other:?}"),
        }
        assert_eq!(
            storage.scan_materializations(),
            3,
            "a nested-loop join materializes only its build side"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The grace-hash spill path partitions the probe side straight off the
    /// scan stream: still exactly one materialization (the build side), and
    /// the same result as the in-memory join.
    #[test]
    fn a_spilling_join_streams_its_probe_side_into_partitions() {
        use crate::rel::{StatementResult, TxnContext, execute_batch, set_test_sort_budget};

        let path = unique_temp_path("grace-probe-stream");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);
        for chunk in (1..=2000).collect::<Vec<i64>>().chunks(500) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({i}, {})", i % 7)).collect();
            let outcome = execute_batch(
                &storage,
                &format!("INSERT INTO t VALUES {}", values.join(", ")),
                &mut ctx,
            );
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
        }

        set_test_sort_budget(Some(4000));
        let outcome = execute_batch(
            &storage,
            "SELECT COUNT(*) FROM t a LEFT JOIN t b ON a.id = b.id AND b.v = 3",
            &mut ctx,
        );
        set_test_sort_budget(None);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            // Every a-row appears once: matched where b.v = 3, null-extended
            // otherwise — 2000 either way.
            StatementResult::Rows(rowset) => assert_eq!(rowset.rows[0][0], Datum::BigInt(2000)),
            other => panic!("expected rows, got {other:?}"),
        }
        assert_eq!(
            storage.scan_materializations(),
            1,
            "the spilling join materialized only its build side"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The row counter tracks every DML shape through the SQL layer — and
    /// because it is an ordinary transactional page op, statement atomicity,
    /// savepoints, transaction rollback and crash recovery all keep it exact
    /// without counter-specific recovery code.
    #[test]
    fn row_counts_track_dml_transactions_and_recovery() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("row-count");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let run = |storage: &Storage, ctx: &mut TxnContext, sql: &str| {
            let outcome = execute_batch(storage, sql, ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        };

        run(
            &storage,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        );
        assert_eq!(storage.rel_row_count("t"), Some(0));

        run(
            &storage,
            &mut ctx,
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
        );
        assert_eq!(storage.rel_row_count("t"), Some(3));

        run(&storage, &mut ctx, "DELETE FROM t WHERE id = 3");
        run(&storage, &mut ctx, "UPDATE t SET v = 99 WHERE id = 1");
        assert_eq!(storage.rel_row_count("t"), Some(2), "delete -1, update 0");

        // A failing multi-row statement (duplicate key on its last row) is
        // atomic: no rows land, and neither does its count.
        let dup = execute_batch(&storage, "INSERT INTO t VALUES (5, 1), (1, 1)", &mut ctx);
        assert!(dup.error.is_some(), "duplicate key must fail");
        assert_eq!(storage.rel_row_count("t"), Some(2));

        // Transaction rollback restores the count with the rows.
        run(
            &storage,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (10, 1), (11, 1)",
        );
        assert_eq!(storage.rel_row_count("t"), Some(4), "in-flight rows count");
        run(&storage, &mut ctx, "ROLLBACK");
        assert_eq!(storage.rel_row_count("t"), Some(2));

        // A savepoint rollback restores exactly the statements behind it.
        run(
            &storage,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (20, 1); SAVE TRANSACTION sp; \
             INSERT INTO t VALUES (21, 1); ROLLBACK TRANSACTION sp; COMMIT",
        );
        assert_eq!(storage.rel_row_count("t"), Some(3));

        // Crash (no checkpoint, pool never flushed): recovery replays the ops,
        // counter page included.
        drop(storage);
        let storage = Storage::open(path.clone()).expect("reopen");
        assert_eq!(
            storage.rel_row_count("t"),
            Some(3),
            "count survives recovery"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Rows of a transaction still open at the crash are undone by recovery —
    /// and so is their count.
    #[test]
    fn an_uncommitted_transactions_rows_are_uncounted_after_crash() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("row-count-crash");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY); INSERT INTO t VALUES (1), (2)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);

        let mut open_txn = TxnContext::default();
        let pending = execute_batch(
            &storage,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (10), (11), (12)",
            &mut open_txn,
        );
        assert!(pending.error.is_none(), "{:?}", pending.error);
        assert_eq!(storage.rel_row_count("t"), Some(5), "in-flight rows count");
        drop(storage); // crash with the transaction open

        let storage = Storage::open(path.clone()).expect("reopen");
        assert_eq!(
            storage.rel_row_count("t"),
            Some(2),
            "the loser transaction's rows and count are both undone"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// "Row counts as tie-breakers only": a table at or under the tiny
    /// threshold plans its seek as the scan it ties with, grows into the seek
    /// past the threshold — and a covering seek keeps its win at any size,
    /// since it reads less than the table either way.
    #[test]
    fn a_tiny_table_scans_until_it_grows_into_its_seek() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("row-count-tiebreak");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let run = |storage: &Storage, ctx: &mut TxnContext, sql: &str| {
            let outcome = execute_batch(storage, sql, ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
            outcome
        };
        let plan_of = |storage: &Storage, ctx: &mut TxnContext, sql: &str| -> String {
            let outcome = run(storage, ctx, &format!("SET SHOWPLAN_TEXT ON; {sql}"));
            let mut ctx2 = TxnContext::default();
            let _ = execute_batch(storage, "SET SHOWPLAN_TEXT OFF", &mut ctx2);
            match &outcome.results[1] {
                StatementResult::Rows(rowset) => rowset
                    .rows
                    .iter()
                    .map(|r| format!("{:?}", r[0]))
                    .collect::<Vec<_>>()
                    .join("\n"),
                other => panic!("expected plan rows, got {other:?}"),
            }
        };

        run(
            &storage,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, v INT); \
             CREATE INDEX ix_a ON t (a); \
             CREATE INDEX ix_cover ON t (v) INCLUDE (v, id); \
             INSERT INTO t VALUES (1, 10, 5), (2, 20, 6), (3, 30, 7)",
        );

        // Tiny: the non-covering seek ties with the scan; the tie goes to the
        // scan. The covering seek still wins — it reads less than the table.
        let tiny = plan_of(&storage, &mut ctx, "SELECT id FROM t WHERE a = 20");
        assert!(tiny.contains("Table Scan"), "tiny table scans: {tiny}");
        let covering = plan_of(&storage, &mut ctx, "SELECT v, id FROM t WHERE v = 6");
        assert!(
            covering.contains("Index Seek (covering)"),
            "covering exempt from the tie-break: {covering}"
        );

        // Past the threshold the same query seeks.
        let mut pad = String::from("INSERT INTO t VALUES (100, 900, 900)");
        for i in 1..20 {
            pad.push_str(&format!(", ({}, 900, 900)", 100 + i));
        }
        run(&storage, &mut ctx, &pad);
        let grown = plan_of(&storage, &mut ctx, "SELECT id FROM t WHERE a = 20");
        assert!(
            grown.contains("Index Seek") && grown.contains("Key Lookup"),
            "grown table seeks: {grown}"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// A covering index wins its tie against an equal-scoring non-INCLUDE
    /// index — the "add a covering index to an existing database" workflow.
    /// Coverage breaks equality ties only: it never outranks a fully-matched
    /// UNIQUE seek (one row plus one lookup beats a covering scan).
    #[test]
    fn a_covering_index_wins_the_tie_against_an_older_plain_index() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("include-tiebreak");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        // The plain index is created FIRST, so a first-wins tie keeps it.
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, v INT); \
             CREATE INDEX ix ON t (email); \
             CREATE INDEX ix2 ON t (email) INCLUDE (email, v); \
             INSERT INTO t VALUES (1, 'a@x.com', 10)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);
        let outcome = execute_batch(
            &storage,
            "SELECT email, v FROM t WHERE email = 'a@x.com'",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            storage.covering_scans(),
            1,
            "the covering index wins the equality tie"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// CREATE INDEX with a column that does not exist on the table reports
    /// SQL Server's 1911 (not the generic 207) — for the key list and the
    /// INCLUDE list alike; a duplicate INCLUDE column reports 1909.
    #[test]
    fn create_index_errors_carry_sql_server_numbers() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("include-errors");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL)",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);

        let cases = [
            ("CREATE INDEX ix ON t (nope)", 1911),
            ("CREATE INDEX ix ON t (email) INCLUDE (nope)", 1911),
            ("CREATE INDEX ix ON t (email) INCLUDE (id, id)", 1909),
        ];
        for (sql, number) in cases {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert_eq!(
                outcome.error.as_ref().map(|e| e.number),
                Some(number),
                "{sql}: {:?}",
                outcome.error
            );
        }

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Included leaf values follow UPDATE and DELETE, and survive a restart
    /// (the include list rides the catalog; the leaf format rides the pages).
    #[test]
    fn included_values_survive_update_delete_and_restart() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("include-restart");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, v INT); \
             CREATE INDEX ix ON t (email) INCLUDE (email, v); \
             INSERT INTO t VALUES (1, 'a@x.com', 10), (2, 'b@x.com', 20); \
             UPDATE t SET v = 99 WHERE id = 1; \
             DELETE FROM t WHERE id = 2",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);
        drop(storage);

        let storage = Storage::open(path.clone()).expect("reopen");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "SELECT email, v FROM t WHERE email = 'a@x.com'",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(storage.covering_scans(), 1, "covering after reopen");
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => assert_eq!(
                rowset.rows,
                vec![vec![Datum::VarChar("a@x.com".into()), Datum::Int(99)]],
                "the UPDATE reached the leaf; the DELETEd row is gone"
            ),
            other => panic!("expected rows, got {other:?}"),
        }
        let gone = execute_batch(
            &storage,
            "SELECT email, v FROM t WHERE email = 'b@x.com'",
            &mut ctx,
        );
        match &gone.results[0] {
            StatementResult::Rows(rowset) => assert!(rowset.rows.is_empty()),
            other => panic!("expected rows, got {other:?}"),
        }

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// A query reading a column that is NOT included falls back to the key
    /// lookup — through the INCLUDE index's length-prefixed leaf value, whose
    /// `Locator::Key` payload would be swallowed by the old bare decode.
    /// SHOWPLAN tells the two apart: covering has no Key Lookup line.
    #[test]
    fn a_non_covering_read_on_an_include_index_still_finds_rows() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("include-fallback");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let setup = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, name VARCHAR(20)); \
             CREATE INDEX ix ON t (email) INCLUDE (email, id); \
             INSERT INTO t VALUES (1, 'a@x.com', 'Alice')",
            &mut ctx,
        );
        assert!(setup.error.is_none(), "{:?}", setup.error);
        // Pad past the tiny-table tie-break: a <= 16-row table plans as a
        // scan, and this test is about the seek's two plan shapes.
        let mut pad = String::from("INSERT INTO t VALUES (100, 'z0@x.com', 'p')");
        for i in 1..20 {
            pad.push_str(&format!(", ({}, 'z{i}@x.com', 'p')", 100 + i));
        }
        let setup = execute_batch(&storage, &pad, &mut ctx);
        assert!(setup.error.is_none(), "{:?}", setup.error);

        // `name` is not included: the seek fetches the base row by PK key.
        let outcome = execute_batch(
            &storage,
            "SELECT name FROM t WHERE email = 'a@x.com'",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(storage.covering_scans(), 0, "not covering");
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows, vec![vec![Datum::VarChar("Alice".into())]]);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // SHOWPLAN: the covering shape has no Key Lookup; the fallback does.
        let plans = execute_batch(
            &storage,
            "SET SHOWPLAN_TEXT ON; \
             SELECT id FROM t WHERE email = 'a@x.com'; \
             SELECT name FROM t WHERE email = 'a@x.com'",
            &mut ctx,
        );
        assert!(plans.error.is_none(), "{:?}", plans.error);
        let lines_of = |result: &StatementResult| -> Vec<String> {
            match result {
                StatementResult::Rows(rowset) => {
                    rowset.rows.iter().map(|r| format!("{:?}", r[0])).collect()
                }
                other => panic!("expected plan rows, got {other:?}"),
            }
        };
        let covering = lines_of(&plans.results[1]).join("\n");
        assert!(
            covering.contains("Index Seek (covering)") && !covering.contains("Key Lookup"),
            "covering plan: {covering}"
        );
        let lookup = lines_of(&plans.results[2]).join("\n");
        assert!(
            lookup.contains("Key Lookup"),
            "non-covering plan keeps the lookup: {lookup}"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The DONE acknowledging an autocommit write is never emitted before the
    /// write's commit record is fsync-durable: the executor's deferred DONEs
    /// flush — fsyncing first — before the next result set opens, so a client
    /// can never act on an acknowledgment a crash then silently revokes.
    #[test]
    fn a_commit_acknowledgement_never_precedes_its_fsync() {
        use crate::rel::{
            BatchEmitter, ResultColumn, TxnContext, execute_batch, execute_batch_streamed,
        };
        use crate::relstore::types::Datum;

        /// Logs each emitted event alongside the fsync count at that moment.
        struct Probe<'a> {
            storage: &'a Storage,
            log: Vec<(&'static str, u64)>,
        }
        impl Probe<'_> {
            fn note(&mut self, what: &'static str) {
                self.log.push((what, self.storage.group_commit_fsyncs()));
            }
        }
        impl BatchEmitter for Probe<'_> {
            fn columns(&mut self, _columns: Vec<ResultColumn>) {
                self.note("columns");
            }
            fn rows(&mut self, _rows: Vec<Vec<Datum>>) {
                self.note("rows");
            }
            fn statement_done(
                &mut self,
                _count: Option<u64>,
                _in_transaction: bool,
                _command: crate::rel::DoneCommand,
            ) {
                self.note("done");
            }
            fn statement_aborted(&mut self, _in_transaction: bool) {
                self.note("aborted");
            }
        }

        let path = unique_temp_path("stream-durability");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut setup = TxnContext::default();
        let create = execute_batch(&storage, "CREATE TABLE t (v INT NOT NULL)", &mut setup);
        assert!(create.error.is_none(), "create table: {:?}", create.error);
        let baseline = storage.group_commit_fsyncs();

        let mut ctx = TxnContext::default();
        let mut probe = Probe {
            storage: &storage,
            log: Vec::new(),
        };
        let error = execute_batch_streamed(
            &storage,
            "INSERT INTO t VALUES (1); SELECT v FROM t",
            &mut ctx,
            &[],
            &mut probe,
        );
        assert!(error.is_none(), "{error:?}");
        // The INSERT's DONE comes first — already past its fsync — and only
        // then does the SELECT's result set open.
        assert_eq!(
            probe.log.first(),
            Some(&("done", baseline + 1)),
            "the write's acknowledgment waits for its fsync: {:?}",
            probe.log
        );
        assert_eq!(
            probe.log.get(1).map(|(what, _)| *what),
            Some("columns"),
            "the rowset opens after the acknowledgment: {:?}",
            probe.log
        );
        // One fsync total: the mid-batch flush covered the batch's only commit.
        assert_eq!(storage.group_commit_fsyncs(), baseline + 1);

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// An identity value reserved by an in-transaction INSERT (a mini-commit,
    /// durable independently of the transaction) must not stream to the client
    /// before its reservation is fsynced: a value the client has seen must
    /// never be reissued after a crash, and it escapes through the *rows* of a
    /// following SELECT, not through any commit-acknowledging DONE. This is
    /// why the mid-batch flush gates on the kind-based `committed` flag rather
    /// than on "some DONE acknowledges a commit".
    #[test]
    fn an_identity_value_never_streams_before_its_reservation_fsync() {
        use crate::rel::{
            BatchEmitter, ResultColumn, TxnContext, execute_batch, execute_batch_streamed,
        };
        use crate::relstore::types::Datum;

        struct Probe<'a> {
            storage: &'a Storage,
            log: Vec<(&'static str, u64)>,
        }
        impl Probe<'_> {
            fn note(&mut self, what: &'static str) {
                self.log.push((what, self.storage.group_commit_fsyncs()));
            }
        }
        impl BatchEmitter for Probe<'_> {
            fn columns(&mut self, _columns: Vec<ResultColumn>) {
                self.note("columns");
            }
            fn rows(&mut self, _rows: Vec<Vec<Datum>>) {
                self.note("rows");
            }
            fn statement_done(
                &mut self,
                _count: Option<u64>,
                _in_transaction: bool,
                _command: crate::rel::DoneCommand,
            ) {
                self.note("done");
            }
            fn statement_aborted(&mut self, _in_transaction: bool) {
                self.note("aborted");
            }
        }

        let path = unique_temp_path("stream-identity");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut setup = TxnContext::default();
        let create = execute_batch(
            &storage,
            "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, v INT NOT NULL)",
            &mut setup,
        );
        assert!(create.error.is_none(), "create table: {:?}", create.error);
        let baseline = storage.group_commit_fsyncs();

        let mut ctx = TxnContext::default();
        let mut probe = Probe {
            storage: &storage,
            log: Vec::new(),
        };
        // The INSERT's own DONE promises nothing durable (the transaction is
        // open), but its identity reservation is already a mini-commit — and
        // the SELECT's rows carry the reserved value out of the server.
        let error = execute_batch_streamed(
            &storage,
            "BEGIN TRANSACTION; INSERT INTO t (v) VALUES (10); SELECT id FROM t; ROLLBACK",
            &mut ctx,
            &[],
            &mut probe,
        );
        assert!(error.is_none(), "{error:?}");
        for (what, fsyncs) in &probe.log {
            if *what == "columns" || *what == "rows" {
                assert!(
                    *fsyncs > baseline,
                    "the rowset streamed before the reservation's fsync: {:?}",
                    probe.log
                );
            }
        }

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// A batch of autocommit writes with nothing to stream between them still
    /// coalesces to a single fsync at the end of the batch: the DONEs are
    /// deferred to that durability point rather than each buying an fsync.
    #[test]
    fn a_write_only_batch_still_fsyncs_once() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("stream-one-fsync");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut setup = TxnContext::default();
        let create = execute_batch(&storage, "CREATE TABLE t (v INT NOT NULL)", &mut setup);
        assert!(create.error.is_none(), "create table: {:?}", create.error);
        let baseline = storage.group_commit_fsyncs();

        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            storage.group_commit_fsyncs(),
            baseline + 1,
            "three autocommit writes, one batch-end fsync"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Group commit: many transactions whose commit records are already in the
    /// WAL, all waiting for durability up to the same point, are made durable by
    /// a single log-writer fsync. Deterministic (independent of fsync latency):
    /// the commit records are appended WITHOUT fsyncing first — `rel_insert`
    /// appends its commit record but does not force the log — then every
    /// committer waits on the same tail, so exactly one fsync serves them all.
    #[test]
    fn group_commit_coalesces_many_commits_into_one_fsync() {
        use crate::rel::{TxnContext, execute_batch};
        use std::sync::Arc;

        const THREADS: usize = 16;

        let path = unique_temp_path("group-commit");
        let storage =
            Arc::new(Storage::create(path.clone(), test_storage_options()).expect("create"));

        let mut setup = TxnContext::default();
        let create = execute_batch(&storage, "CREATE TABLE t (v INT NOT NULL)", &mut setup);
        assert!(create.error.is_none(), "create table: {:?}", create.error);
        let baseline = storage.group_commit_fsyncs();

        // Raw autocommit inserts append a commit record each with `sync=false`
        // and never call `ensure_durable`, so the WAL tail advances while
        // `flushed` stays put — nothing fsyncs.
        for i in 0..THREADS {
            storage
                .rel_insert("t", vec![Datum::Int(i as i32)])
                .expect("insert");
        }
        let target = storage.wal_tail();
        assert_eq!(
            storage.group_commit_fsyncs(),
            baseline,
            "appending commit records must not fsync"
        );

        // Every committer waits for durability up to the same tail; one fsync
        // covers them all.
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let storage = Arc::clone(&storage);
            handles.push(std::thread::spawn(move || {
                storage.ensure_durable(target).expect("durable")
            }));
        }
        for handle in handles {
            handle.join().expect("thread panicked");
        }

        let fsyncs = storage.group_commit_fsyncs() - baseline;
        assert!(
            (1..=2).contains(&fsyncs),
            "{THREADS} commits should coalesce into a single fsync, got {fsyncs}"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// An identity value is consumed permanently (SQL Server semantics), so its
    /// reservation — a mini-commit made even inside an open transaction — must be
    /// fsynced. Regression: group commit skipped `ensure_durable` for a batch
    /// whose only durable effect was the identity reservation (an INSERT inside
    /// a still-open transaction), so a crash would revert and reuse the value.
    #[test]
    fn identity_reservation_is_made_durable_even_inside_a_transaction() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("identity-durable");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");

        let mut ctx = TxnContext::default();
        let create = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY IDENTITY(1,1), v INT NOT NULL)",
            &mut ctx,
        );
        assert!(create.error.is_none(), "create: {:?}", create.error);

        let before = storage.group_commit_fsyncs();
        // INSERT inside an open transaction: no row commits (the COMMIT is a
        // later batch), but the identity reservation does and must be fsynced.
        let out = execute_batch(
            &storage,
            "BEGIN TRAN; INSERT INTO t (v) VALUES (1)",
            &mut ctx,
        );
        assert!(out.error.is_none(), "insert: {:?}", out.error);
        assert!(
            storage.group_commit_fsyncs() > before,
            "identity reservation inside a transaction must be made durable"
        );

        let _ = execute_batch(&storage, "ROLLBACK", &mut ctx);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Buffered write into the file (test-side corruption / fixtures). The
    /// sync makes it visible to subsequent O_DIRECT reads.
    fn overwrite_bytes(path: &Path, offset: u64, bytes: &[u8]) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open for corruption");
        file.seek(SeekFrom::Start(offset)).expect("seek");
        file.write_all(bytes).expect("write");
        file.sync_all().expect("sync");
    }

    fn read_bytes(path: &Path, offset: u64, len: usize) -> Vec<u8> {
        let mut file = std::fs::File::open(path).expect("open for read");
        file.seek(SeekFrom::Start(offset)).expect("seek");
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).expect("read");
        buf
    }

    fn append_search_entry(storage: &mut Storage, seq_no: u64, payload: &[u8]) -> u64 {
        storage
            .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, seq_no, payload)
            .expect("append wal entry")
    }

    /// Reads both superblocks from disk and returns the active (highest
    /// valid generation) one.
    fn read_active_superblock(path: &Path, layout: &StorageLayout) -> Superblock {
        let read_sb = |offset: u64| -> Option<Superblock> {
            let bytes = read_bytes(path, offset, SUPERBLOCK_SIZE);
            let sb = Superblock::from_le_bytes(bytes.as_slice().try_into().unwrap());
            (sb.checksum == sb.compute_checksum()).then_some(sb)
        };
        let a = read_sb(layout.superblock_a_offset);
        let b = read_sb(layout.superblock_b_offset);
        match (a, b) {
            (Some(a), Some(b)) => {
                if b.generation > a.generation {
                    b
                } else {
                    a
                }
            }
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => panic!("no valid superblock"),
        }
    }

    fn test_layout() -> StorageLayout {
        compute_layout(test_storage_options(), TEST_WAL_BYTES, TEST_WAL_BYTES).expect("layout")
    }

    /// Writes a v1-format file: v1 header, superblocks, optional snapshot in
    /// half-slot 0, WAL entries with v1 stamping (`logical_ts = seq_no`) and
    /// garbage in the allocator bitmap (v1 half-slot bookkeeping the upgrade
    /// must discard).
    fn write_v1_fixture(
        path: &Path,
        wal_events: &[(u64, Vec<u8>)],
        snapshot: Option<(&[u8], u64, u64, u64)>,
    ) -> StorageLayout {
        let layout = test_layout();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .expect("create fixture");
        file.set_len(layout.total_size).expect("set_len");
        drop(file);

        let mut header = FileHeader::default();
        header.version = FILE_VERSION_V1;
        header.superblock_a_offset = layout.superblock_a_offset;
        header.superblock_b_offset = layout.superblock_b_offset;
        header.wal_offset = layout.wal_offset;
        header.wal_size = layout.wal_size;
        header.data_offset = layout.data_offset;
        header.data_size = layout.data_size;
        header.metadata_offset = layout.metadata_offset;
        header.metadata_size = layout.metadata_size;
        header.allocator_offset = layout.allocator_offset;
        header.allocator_size = layout.allocator_size;
        header.snapshot_offset = layout.snapshot_offset;
        header.snapshot_size = layout.snapshot_size;
        header.reserved_offset = layout.reserved_offset;
        header.reserved_size = layout.reserved_size;
        header.header_checksum = header.compute_checksum();
        overwrite_bytes(path, 0, &header.to_le_bytes_with_checksum());

        // WAL entries, sequentially from ring position 0, v1 stamping.
        let mut tail = 0u64;
        let mut last_seq = 0u64;
        for (seq_no, payload) in wal_events {
            let padded = wal_entry_padded_len(payload.len());
            let crc = wal_payload_crc(payload);
            let entry_header = WalEntryHeader::new(
                WAL_ENTRY_TYPE_RECORD,
                1,
                payload.len() as u32,
                *seq_no,
                *seq_no, // v1: logical_ts carried the engine seq
                crc,
            );
            let footer = WalEntryFooter {
                payload_len: payload.len() as u32,
                payload_crc: crc,
            };
            let mut bytes = Vec::with_capacity(padded);
            bytes.extend_from_slice(&entry_header.to_le_bytes());
            bytes.extend_from_slice(payload);
            bytes.extend_from_slice(&footer.to_le_bytes());
            bytes.resize(padded, 0);
            overwrite_bytes(path, layout.wal_offset + tail, &bytes);
            tail += padded as u64;
            last_seq = *seq_no;
        }

        // Snapshot in v1 half-slot 0 (start of the data region).
        if let Some((data, checkpoint_seq, next_seq_no, next_doc_id)) = snapshot {
            overwrite_bytes(path, layout.data_offset, data);
            let mut desc = SnapshotDescriptor::default();
            desc.generation = 1;
            desc.slot = 0;
            desc.checkpoint_seq = checkpoint_seq;
            desc.data_offset = layout.data_offset;
            desc.data_len = data.len() as u64;
            desc.data_checksum = xxh64(data, 0);
            desc.next_seq_no = next_seq_no;
            desc.next_doc_id = next_doc_id;
            desc.checksum = desc.compute_checksum();
            overwrite_bytes(
                path,
                layout.snapshot_offset,
                &desc.to_le_bytes_with_checksum(),
            );
        }

        // v1 half-slot allocator garbage the upgrade must wipe.
        overwrite_bytes(path, layout.allocator_offset, &[0xFF; 512]);

        let mut sb_a = Superblock::default();
        sb_a.generation = 2;
        sb_a.active = SUPERBLOCK_ACTIVE_A;
        sb_a.wal_tail = tail;
        sb_a.last_committed_seq = last_seq;
        sb_a.checksum = sb_a.compute_checksum();
        overwrite_bytes(
            path,
            layout.superblock_a_offset,
            &sb_a.to_le_bytes_with_checksum(),
        );
        let mut sb_b = Superblock::default();
        sb_b.active = SUPERBLOCK_ACTIVE_B;
        sb_b.checksum = sb_b.compute_checksum();
        overwrite_bytes(
            path,
            layout.superblock_b_offset,
            &sb_b.to_le_bytes_with_checksum(),
        );
        layout
    }

    #[test]
    fn v1_fixture_upgrades_in_place_and_preserves_state() {
        let path = unique_temp_path("v1-upgrade");
        let snapshot_data = b"v1-snapshot-payload".as_slice();
        let events = vec![(6u64, vec![1u8; 100]), (7u64, vec![2u8; 50])];
        write_v1_fixture(&path, &events, Some((snapshot_data, 5, 8, 3)));

        let mut storage = Storage::open(path.clone()).expect("open v1 file");

        // Snapshot survives the upgrade.
        let snapshot = storage
            .load_snapshot()
            .expect("load snapshot")
            .expect("snapshot present");
        assert_eq!(snapshot.data, snapshot_data);
        assert_eq!(snapshot.checkpoint_seq, 5);
        assert_eq!(snapshot.next_seq_no, 8);
        assert_eq!(snapshot.next_doc_id, 3);

        // WAL entries survive and replay in order.
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq_no, 6);
        assert_eq!(records[0].payload, vec![1u8; 100]);
        assert_eq!(records[1].seq_no, 7);
        assert_eq!(records[1].payload, vec![2u8; 50]);

        // The allocator was rebuilt: only the live snapshot extent is
        // allocated; the v1 half-slot garbage is gone.
        assert!(storage.is_page_allocated(0), "snapshot extent must be live");
        assert!(
            !storage.is_page_allocated(1),
            "pages past the snapshot extent must be free"
        );
        assert!(
            !storage.is_page_allocated(100),
            "v1 bitmap garbage must have been wiped"
        );

        // New work post-upgrade: append + checkpoint + reopen.
        append_search_entry(&mut storage, 8, b"post-upgrade");
        storage
            .write_checkpoint(b"v2-snapshot", 8, 9, 4)
            .expect("checkpoint");
        drop(storage);

        // On-disk version is now v2; upgraded file opens cleanly.
        let header_bytes = read_bytes(&path, 0, FILE_HEADER_SIZE);
        let header = FileHeader::from_le_bytes(header_bytes.as_slice().try_into().unwrap());
        assert_eq!(header.version, FILE_VERSION);
        assert_eq!(header.header_checksum, header.compute_checksum());

        let mut storage = Storage::open(path.clone()).expect("reopen upgraded");
        let snapshot = storage
            .load_snapshot()
            .expect("load")
            .expect("second snapshot");
        assert_eq!(snapshot.data, b"v2-snapshot");
        assert!(
            storage.replay_wal_entries().expect("replay").is_empty(),
            "checkpoint reclaimed the wal"
        );
        // The upgraded snapshot's extent was freed once the v2 one became
        // durable; page 0 belonged to the v1 snapshot.
        assert!(!storage.is_page_allocated(0));
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn torn_tail_is_stopped_at_and_healed_by_whole_page_flush() {
        let path = unique_temp_path("torn-tail");
        let layout = test_layout();
        let mut storage = create_small(&path);
        let payload_a = vec![0xAAu8; 100];
        append_search_entry(&mut storage, 1, &payload_a);
        drop(storage); // crash: superblock still says tail = 0

        // Simulate a torn write of a follow-up entry: garbage on the tail
        // page right after entry A.
        let entry_a_len = wal_entry_padded_len(payload_a.len()) as u64;
        overwrite_bytes(&path, layout.wal_offset + entry_a_len, &[0x5Au8; 200]);

        // Recovery must stop at the garbage and keep A.
        let mut storage = Storage::open(path.clone()).expect("reopen after tear");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 1, "only entry A must survive the torn tail");
        assert_eq!(records[0].payload, payload_a);

        // The next append rewrites the whole tail page from memory, healing
        // the torn bytes: B lands exactly where the garbage was.
        let payload_b = vec![0xBBu8; 60];
        append_search_entry(&mut storage, 2, &payload_b);
        drop(storage);

        let mut storage = Storage::open(path.clone()).expect("reopen after heal");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, payload_a);
        assert_eq!(records[1].payload, payload_b);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn superblock_is_lazy_and_forward_scan_recovers_past_it() {
        let path = unique_temp_path("lazy-superblock");
        let layout = test_layout();
        let mut storage = create_small(&path);
        storage.lock().wal.set_superblock_interval(400);

        // 152 bytes per entry: the cadence write fires once, on entry 3
        // (456 bytes appended), and entry 4 stays past the recorded tail.
        let payloads: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i + 1; 100]).collect();
        let mut lsns = Vec::new();
        for (i, payload) in payloads.iter().enumerate() {
            lsns.push(append_search_entry(&mut storage, i as u64 + 1, payload));
        }
        drop(storage); // crash without checkpoint

        // The on-disk superblock lags the true tail (laziness) but is not 0
        // (the cadence rewrite fired).
        let sb = read_active_superblock(&path, &layout);
        let true_tail = lsns[3] + wal_entry_padded_len(payloads[3].len()) as u64;
        assert!(sb.wal_tail > 0, "cadence superblock write must have fired");
        assert!(
            sb.wal_tail < true_tail,
            "superblock tail {} must lag the true tail {true_tail}",
            sb.wal_tail
        );

        // Recovery scans forward past the stale superblock tail and finds
        // every entry.
        let mut storage = Storage::open(path.clone()).expect("reopen");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 4, "forward scan must recover all entries");
        for (record, payload) in records.iter().zip(&payloads) {
            assert_eq!(&record.payload, payload);
        }
        // LSN self-identity stamping.
        for (record, lsn) in records.iter().zip(&lsns) {
            assert_eq!(record.logical_ts, *lsn);
        }
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn wal_wraps_after_checkpoint_and_recovers_across_the_lap() {
        let path = unique_temp_path("wal-wrap");
        let mut storage = create_small(&path);

        // Fill ~60% of the 64 KiB ring, then reclaim it via checkpoint.
        for seq in 1..=5u64 {
            append_search_entry(&mut storage, seq, &vec![seq as u8; 8000]);
        }
        storage
            .write_checkpoint(b"state-at-5", 5, 6, 1)
            .expect("checkpoint");

        // These cross the lap boundary (one entry forces a wrap gap).
        let post: Vec<Vec<u8>> = (6..=9u64).map(|seq| vec![seq as u8; 8000]).collect();
        for (i, payload) in post.iter().enumerate() {
            append_search_entry(&mut storage, 6 + i as u64, payload);
        }
        drop(storage); // crash

        let mut storage = Storage::open(path.clone()).expect("reopen");
        let snapshot = storage
            .load_snapshot()
            .expect("load")
            .expect("snapshot present");
        assert_eq!(snapshot.data, b"state-at-5");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(
            records.len(),
            4,
            "exactly the post-checkpoint entries replay"
        );
        for (record, payload) in records.iter().zip(&post) {
            assert_eq!(&record.payload, payload);
        }
        // The ring stays usable after recovery on the wrapped lap.
        append_search_entry(&mut storage, 10, b"after-recovery");
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn wal_full_errors_and_checkpoint_reclaims() {
        let path = unique_temp_path("wal-full");
        let mut storage = create_small(&path);
        let payload = vec![7u8; 8000];
        let mut appended = 0;
        let err = loop {
            match storage.append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, appended + 1, &payload) {
                Ok(_) => appended += 1,
                Err(err) => break err,
            }
            assert!(appended < 100, "ring must fill up");
        };
        assert!(matches!(err, StorageError::WalFull(_)), "got: {err}");

        storage
            .write_checkpoint(b"reclaim", appended, appended + 1, 1)
            .expect("checkpoint");
        append_search_entry(&mut storage, appended + 1, &payload);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn extent_alloc_free_replays_from_wal() {
        let path = unique_temp_path("extent-replay");
        let mut storage = create_small(&path);
        let durable = storage.allocate_extent(false).expect("durable extent");
        let temp = storage.allocate_extent(true).expect("temp extent");
        assert_ne!(durable, temp);
        drop(storage); // crash: bitmap never persisted, only the WAL knows

        let mut storage = Storage::open(path.clone()).expect("reopen");
        assert!(
            storage.is_page_allocated(durable),
            "logged alloc must replay"
        );
        assert!(
            !storage.is_page_allocated(temp),
            "temp extents must vanish on restart"
        );

        storage.free_extent(durable).expect("free extent");
        drop(storage); // crash again

        let mut storage = Storage::open(path.clone()).expect("reopen after free");
        assert!(
            !storage.is_page_allocated(durable),
            "logged free must replay"
        );

        // Alloc + checkpoint: the bitmap carries the state once the WAL is
        // reclaimed.
        let kept = storage.allocate_extent(false).expect("extent");
        storage
            .write_checkpoint(b"with-extent", 1, 2, 1)
            .expect("checkpoint");
        drop(storage);

        let mut storage = Storage::open(path.clone()).expect("reopen after checkpoint");
        assert!(
            storage.replay_wal_entries().expect("replay").is_empty(),
            "wal reclaimed"
        );
        assert!(
            storage.is_page_allocated(kept),
            "bitmap must carry extents across checkpoints"
        );
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Regression (review finding): the whole-tail-page flush must never
    /// let the page's zero suffix alias onto live entries at the head. With
    /// a mid-page head and a nearly-full ring, the append that would have
    /// clobbered the head must instead report WalFull, and every previously
    /// acknowledged entry must survive a crash.
    #[test]
    fn tail_page_flush_never_overwrites_live_head_entries() {
        let path = unique_temp_path("tail-page-alias");
        let mut storage = create_small(&path);

        // Head becomes mid-page: one 5056-byte entry, then checkpoint.
        append_search_entry(&mut storage, 1, &vec![1u8; 5000]);
        storage
            .write_checkpoint(b"cp", 1, 2, 1)
            .expect("checkpoint");

        // Fill the ring almost entirely (the 15th entry wraps).
        let mut acked = Vec::new();
        for i in 0..15u64 {
            let payload = vec![(i + 2) as u8; 4000];
            append_search_entry(&mut storage, i + 2, &payload);
            acked.push(payload);
        }

        // This append fits the naive byte count but its tail-page zero
        // suffix would overwrite the oldest live entries; it must be
        // rejected.
        let err = storage
            .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, 17, &vec![9u8; 900])
            .expect_err("append aliasing the head must fail");
        assert!(matches!(err, StorageError::WalFull(_)), "got: {err}");
        drop(storage); // crash

        let mut storage = Storage::open(path.clone()).expect("reopen");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 15, "every acked entry must survive");
        for (record, payload) in records.iter().zip(&acked) {
            assert_eq!(&record.payload, payload);
        }
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Regression (review finding): a crash between descriptor fsync and
    /// superblock publish leaves descriptor generations ahead of the
    /// superblocks. The next checkpoint must mint a strictly higher
    /// generation (no duplicates), and later opens must agree on the live
    /// snapshot.
    #[test]
    fn checkpoint_after_superblock_publish_crash_mints_higher_generation() {
        let path = unique_temp_path("gen-minting");
        let layout = test_layout();
        let mut storage = create_small(&path);
        storage.write_checkpoint(b"one", 1, 2, 1).expect("cp 1");
        drop(storage);

        // Save the checkpoint-1-era superblocks.
        let sb_a = read_bytes(&path, layout.superblock_a_offset, SUPERBLOCK_SIZE);
        let sb_b = read_bytes(&path, layout.superblock_b_offset, SUPERBLOCK_SIZE);

        let mut storage = Storage::open(path.clone()).expect("reopen");
        storage.write_checkpoint(b"two", 2, 3, 1).expect("cp 2");
        drop(storage);

        // Simulate the crash window: descriptor of checkpoint 2 durable,
        // superblocks rolled back to checkpoint 1.
        overwrite_bytes(&path, layout.superblock_a_offset, &sb_a);
        overwrite_bytes(&path, layout.superblock_b_offset, &sb_b);

        let mut storage = Storage::open(path.clone()).expect("reopen in crash window");
        let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
        assert_eq!(snapshot.data, b"two", "newest descriptor must win");

        // The next checkpoint must not duplicate checkpoint 2's generation.
        storage.write_checkpoint(b"three", 3, 4, 1).expect("cp 3");
        drop(storage);

        let mut storage = Storage::open(path.clone()).expect("final reopen");
        let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
        assert_eq!(snapshot.data, b"three");
        // Allocator agrees with the snapshot choice: the live extent is
        // allocated and loadable, and further checkpoints keep working.
        storage.write_checkpoint(b"four", 4, 5, 1).expect("cp 4");
        let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
        assert_eq!(snapshot.data, b"four");
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Review finding: a durable wrap gap whose next-lap entry never made it
    /// to disk must rewind the recovered tail to the gap start.
    #[test]
    fn wrap_gap_without_next_lap_entry_rewinds_tail() {
        let path = unique_temp_path("gap-rewind");
        let layout = test_layout();
        let mut storage = create_small(&path);
        for seq in 1..=5u64 {
            append_search_entry(&mut storage, seq, &vec![seq as u8; 8000]);
        }
        storage
            .write_checkpoint(b"cp", 5, 6, 1)
            .expect("checkpoint");
        let post: Vec<Vec<u8>> = (6..=9u64).map(|seq| vec![seq as u8; 8000]).collect();
        for (i, payload) in post.iter().enumerate() {
            append_search_entry(&mut storage, 6 + i as u64, payload);
        }
        drop(storage);

        // Entry 9 wrapped to the ring start. Erase its lap-2 pages as if the
        // gap reached disk but the entry itself never did.
        let entry_len = wal_entry_padded_len(8000);
        overwrite_bytes(&path, layout.wal_offset, &vec![0u8; entry_len]);

        let mut storage = Storage::open(path.clone()).expect("reopen");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 3, "the lost wrap entry must not replay");
        for (record, payload) in records.iter().zip(&post[..3]) {
            assert_eq!(&record.payload, payload);
        }

        // The rewound tail must be usable: a new append re-wraps and
        // survives another crash.
        append_search_entry(&mut storage, 9, b"after-rewind");
        drop(storage);
        let mut storage = Storage::open(path.clone()).expect("second reopen");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 4);
        assert_eq!(records[3].payload, b"after-rewind");
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Review finding: the in-place lazy superblock rewrite stakes its
    /// safety on falling back to the other superblock plus the forward
    /// scan. Exercise exactly that: corrupt the active superblock and
    /// recover everything through the stale one.
    #[test]
    fn torn_active_superblock_falls_back_and_forward_scan_recovers() {
        let path = unique_temp_path("torn-superblock");
        let layout = test_layout();
        let mut storage = create_small(&path);
        storage.lock().wal.set_superblock_interval(400);
        let payloads: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i + 1; 100]).collect();
        for (i, payload) in payloads.iter().enumerate() {
            append_search_entry(&mut storage, i as u64 + 1, payload);
        }
        drop(storage);

        // The lazy writes all went to the active superblock (A). Tear it.
        overwrite_bytes(
            &path,
            layout.superblock_a_offset,
            &[0xEEu8; SUPERBLOCK_SIZE],
        );

        let mut storage = Storage::open(path.clone()).expect("reopen on backup superblock");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 4, "forward scan from the stale superblock");
        for (record, payload) in records.iter().zip(&payloads) {
            assert_eq!(&record.payload, payload);
        }
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Corruption inside the trusted region truncates the log there; the
    /// corrected tail must be persisted at open so stale entries beyond it
    /// can never re-enter a future trusted region.
    #[test]
    fn trusted_region_corruption_truncates_and_persists_corrected_tail() {
        let path = unique_temp_path("trusted-corruption");
        let layout = test_layout();
        let mut storage = create_small(&path);
        storage.lock().wal.set_superblock_interval(400);
        for seq in 1..=4u64 {
            append_search_entry(&mut storage, seq, &[seq as u8; 100]);
        }
        drop(storage);
        let entry_len = wal_entry_padded_len(100) as u64;

        // Corrupt entry 2, inside the superblock-trusted region.
        overwrite_bytes(&path, layout.wal_offset + entry_len + 8, &[0xDDu8; 32]);

        let mut storage = Storage::open(path.clone()).expect("reopen after corruption");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 1, "log truncates at the corrupt entry");
        assert_eq!(records[0].payload, vec![1u8; 100]);
        drop(storage);

        // The corrected (smaller) tail must now be on disk.
        let sb = read_active_superblock(&path, &layout);
        assert_eq!(
            sb.wal_tail, entry_len,
            "open must persist the corrected tail"
        );

        // New history: append a differently-sized entry and crash. Recovery
        // must see [entry 1, new entry] and never resurrect old entries 3/4.
        let mut storage = Storage::open(path.clone()).expect("reopen");
        storage.replay_wal_entries().expect("drain");
        append_search_entry(&mut storage, 2, &[9u8; 60]);
        drop(storage);
        let mut storage = Storage::open(path.clone()).expect("final reopen");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, vec![1u8; 100]);
        assert_eq!(records[1].payload, vec![9u8; 60]);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Review finding: allocate_extent's rollback when the WAL append fails
    /// must leave the allocator exactly as before.
    #[test]
    fn allocate_extent_rolls_back_when_wal_is_full() {
        let path = unique_temp_path("extent-rollback");
        let mut storage = create_small(&path);
        // Fill the ring for large and small entries alike.
        let mut seq = 1u64;
        for payload_len in [8000usize, 1000, 100, 8] {
            while storage
                .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, seq, &vec![7u8; payload_len])
                .is_ok()
            {
                seq += 1;
                assert!(seq < 1000, "ring must fill up");
            }
        }

        let err = storage
            .allocate_extent(false)
            .expect_err("extent alloc must fail when its record cannot be logged");
        assert!(matches!(err, StorageError::WalFull(_)), "got: {err}");
        for page in 0..EXTENT_PAGES {
            assert!(
                !storage.is_page_allocated(page),
                "rolled-back extent must leave page {page} free"
            );
        }

        // After reclaiming the ring, extent allocation works again and the
        // rolled-back range stays free (the next-fit cursor moved past it,
        // so it is not the range reused here).
        storage
            .write_checkpoint(b"reclaim", seq, seq + 1, 1)
            .expect("checkpoint");
        let start = storage.allocate_extent(false).expect("extent");
        for page in 0..EXTENT_PAGES {
            assert!(!storage.is_page_allocated(page));
        }
        assert!(storage.is_page_allocated(start));
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn empty_checkpoint_data_is_rejected() {
        let path = unique_temp_path("empty-checkpoint");
        let mut storage = create_small(&path);
        storage.write_checkpoint(b"valid", 1, 2, 1).expect("cp");
        let err = storage
            .write_checkpoint(b"", 2, 3, 1)
            .expect_err("empty checkpoint must be rejected");
        assert!(matches!(err, StorageError::InvalidConfig(_)), "got: {err}");
        // The previous snapshot is untouched.
        let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
        assert_eq!(snapshot.data, b"valid");
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    /// Review finding: the v1 upgrade must refresh BOTH superblocks — a v1
    /// backup superblock is arbitrarily stale, and a later fallback to it
    /// would lose every v1 WAL entry (they cannot pass the v2 forward scan).
    #[test]
    fn upgraded_v1_file_survives_active_superblock_loss() {
        let path = unique_temp_path("v1-superblock-refresh");
        let events = vec![(1u64, vec![5u8; 80]), (2u64, vec![6u8; 40])];
        let layout = write_v1_fixture(&path, &events, None);

        // First open performs the upgrade.
        let mut storage = Storage::open(path.clone()).expect("upgrade open");
        assert_eq!(storage.replay_wal_entries().expect("replay").len(), 2);
        drop(storage);

        // Lose the active superblock; the refreshed backup must carry the
        // v1 tail so the trusted scan still finds the v1-stamped entries.
        overwrite_bytes(
            &path,
            layout.superblock_a_offset,
            &[0xEEu8; SUPERBLOCK_SIZE],
        );
        let mut storage = Storage::open(path.clone()).expect("reopen on backup");
        let records = storage.replay_wal_entries().expect("replay");
        assert_eq!(records.len(), 2, "v1 entries must survive the fallback");
        assert_eq!(records[0].payload, vec![5u8; 80]);
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn successive_checkpoints_recycle_snapshot_extents() {
        let path = unique_temp_path("snapshot-recycle");
        let mut storage = create_small(&path);
        storage.write_checkpoint(b"first", 1, 2, 1).expect("cp 1");
        let first_desc = storage
            .lock()
            .load_active_snapshot_descriptor()
            .expect("desc")
            .expect("present");
        let (first_start, first_pages) = storage
            .lock()
            .descriptor_page_range(&first_desc)
            .expect("range");
        assert!(storage.is_page_allocated(first_start));

        storage
            .write_checkpoint(b"second-snapshot", 2, 3, 1)
            .expect("cp 2");
        for page in first_start..first_start + first_pages {
            assert!(
                !storage.is_page_allocated(page),
                "first snapshot extent must be freed after the second checkpoint"
            );
        }
        drop(storage);

        let mut storage = Storage::open(path.clone()).expect("reopen");
        let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
        assert_eq!(snapshot.data, b"second-snapshot");
        assert!(!storage.is_page_allocated(first_start));
        drop(storage);
        let _ = std::fs::remove_file(path);
    }
}
