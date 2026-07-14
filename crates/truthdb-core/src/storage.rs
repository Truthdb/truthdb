use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;

use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

use crate::allocator::{EXTENT_PAGES, PageAllocator};
use crate::direct_io::{AlignedPageBuf, DirectFile};
use crate::group_commit::GroupCommit;
use crate::relstore::RelState;
use crate::relstore::btree::{BTree, TreeInsert};
use crate::relstore::buffer_pool::DEFAULT_CAPACITY_BYTES;
use crate::relstore::catalog::{self, FIRST_USER_OBJECT_ID, IndexDef, TableDef};
use crate::relstore::ctx::{OpMode, PoolIo, RelCtx, TxnLink};
use crate::relstore::heap::{Heap, Rid};
use crate::relstore::index::{self, Locator};
use crate::relstore::key::encode_key;
use crate::relstore::recovery as rel_recovery;
use crate::relstore::row::{Column, Schema, decode_row, encode_row};
use crate::relstore::types::{Datum, TypeError};
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

#[derive(Debug, Clone, Copy)]
pub struct StorageOptions {
    pub size_gib: u64,
    pub wal_ratio: f64,
    pub metadata_ratio: f64,
    pub snapshot_ratio: f64,
    pub allocator_ratio: f64,
    pub reserved_ratio: f64,
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

impl Storage {
    fn lock(&self) -> std::sync::MutexGuard<'_, StorageFile> {
        self.inner.lock().expect("storage mutex poisoned")
    }

    /// Wraps an opened/created [`StorageFile`] in a `Storage`, spawning the
    /// group-commit log-writer over a duplicated WAL fd.
    fn with_log_writer(path: PathBuf, file: StorageFile) -> Result<Self, StorageError> {
        let wal_fd = file.try_clone_wal_fd()?;
        let (gc, log_writer) = GroupCommit::start(wal_fd);
        Ok(Storage {
            path,
            inner: std::sync::Mutex::new(file),
            gc,
            log_writer: Some(log_writer),
        })
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

    /// Writes a checkpoint only if no transaction is active and the WAL is at
    /// least `threshold` full — decided and written under a single lock hold, so
    /// a transaction cannot `begin` (which also takes this lock) in the window
    /// between the check and the WAL truncation. Without that atomicity a
    /// concurrent worker could open a transaction just after the check, and the
    /// checkpoint would flush its uncommitted pages and discard its undo,
    /// resurrecting uncommitted data after a crash. Returns whether it wrote.
    pub fn checkpoint_if_quiescent(
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
        if file.rel.wedged || file.has_active_transactions() || file.wal_usage_ratio() < threshold {
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
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_create_index(table, index_name, columns, unique)
    }

    pub(crate) fn rel_drop_index(
        &self,
        table: &str,
        index_name: &str,
    ) -> Result<bool, StorageError> {
        self.lock().rel_drop_index(table, index_name)
    }

    pub(crate) fn rel_index_scan(
        &self,
        table: &str,
        index_object_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.lock()
            .rel_index_scan(table, index_object_id, lower, upper)
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
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let (key, value) = index::leaf_entry(&index_key, locator, index.unique);
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
        index_insert_row(ctx, txn, indexes, collations, new_values, new_locator)?;
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
        let (key, _) = index::leaf_entry(&index_key, locator, index.unique);
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
        columns: Vec<Column>,
        key_names: &[String],
        defaults: Vec<Option<String>>,
        identity: Option<catalog::IdentitySpec>,
        check_constraints: Vec<catalog::CheckDef>,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
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

        let updated = self.rel_statement(move |ctx, txn| {
            let tree = BTree::create(ctx, object_id)?;
            for (loc, values) in &located {
                let locator = match loc {
                    RowLocator::Key(key) => Locator::Key(key.clone()),
                    RowLocator::Rid(rid) => Locator::Rid(*rid),
                };
                let index_key = index::encode_index_columns(values, &columns, &def.collations)
                    .map_err(|err| StorageError::InvalidConfig(err.0))?;
                let (key, value) = index::leaf_entry(&index_key, &locator, unique);
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
            });
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(table.to_string(), updated);
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
    pub(crate) fn rel_index_scan(
        &mut self,
        table: &str,
        index_object_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(table)?;
        let index = def
            .indexes
            .iter()
            .find(|i| i.object_id == index_object_id)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig("unknown index".to_string()))?;
        let mut ctx = self.rel_ctx();
        let index_tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        let entries = index_tree.scan_range(&mut ctx, lower.as_deref(), upper.as_deref())?;
        let mut rows = Vec::with_capacity(entries.len());
        if def.is_tree() {
            let base = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            for (_, value) in entries {
                if let Locator::Key(pk) = index::decode_locator(&value)
                    && let Some(row) = base.get(&mut ctx, &pk)?
                {
                    rows.push(decode_row(&schema, &row)?);
                }
            }
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            for (_, value) in entries {
                if let Locator::Rid(rid) = index::decode_locator(&value)
                    && let Some(row) = heap.read_row(&mut ctx, rid)?
                {
                    rows.push(decode_row(&schema, &row)?);
                }
            }
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
                        &collations,
                        values,
                        &Locator::Key(key.clone()),
                    )?;
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
                    index_insert_row(ctx, txn, &indexes, &collations, values, &Locator::Rid(rid))?;
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
        // Fold each key column so a case-insensitive character PK lookup matches
        // the stored (folded) key — the seek literal must fold exactly as the
        // stored key did.
        let mut key = Vec::new();
        for (value, &col) in key_values.iter().zip(&def.key_columns) {
            let folded = crate::relstore::key::fold_key_datum(
                value,
                schema.columns[col].collation.as_deref(),
            );
            crate::relstore::key::encode_datum(&folded, &mut key)?;
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

    /// Deletes the located rows (each carrying its old values for index
    /// upkeep) in one atomic statement; returns the count.
    pub(crate) fn rel_delete_located(
        &mut self,
        name: &str,
        targets: Vec<(RowLocator, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        let (def, _schema) = self.rel_def(name)?;
        let count = targets.len();
        if count == 0 {
            return Ok(0);
        }
        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for (loc, values) in &targets {
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
                    }
                }
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for (loc, values) in &targets {
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
                    }
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
            for (loc, old_values, new_values) in updates {
                let RowLocator::Key(old_key) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected key locator for clustered table".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                let row = encode_row(&schema, &new_values)?;
                let new_key = encode_key(&schema, &def.key_columns, &new_values)?;
                if !indexes.is_empty() {
                    idx_ops.push((
                        old_values,
                        Locator::Key(old_key.clone()),
                        new_values,
                        Locator::Key(new_key.clone()),
                    ));
                }
                if new_key == old_key {
                    in_place.push((old_key, row));
                } else {
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
                apply_index_updates(ctx, txn, &indexes, &collations, &idx_ops)?;
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let mut encoded: Vec<(Rid, Vec<u8>)> = Vec::with_capacity(count);
            for (loc, old_values, new_values) in updates {
                let RowLocator::Rid(rid) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected rid locator for heap".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                encoded.push((rid, encode_row(&schema, &new_values)?));
                if !indexes.is_empty() {
                    // Heap RIDs are stable across an update.
                    idx_ops.push((old_values, Locator::Rid(rid), new_values, Locator::Rid(rid)));
                }
            }
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for (rid, row) in &encoded {
                    heap.update(ctx, txn, *rid, row)?;
                }
                apply_index_updates(ctx, txn, &indexes, &collations, &idx_ops)?;
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

        let mut storage = StorageFile {
            file,
            wal,
            layout,
            superblock_a,
            superblock_b,
            active_superblock,
            allocator: PageAllocator::new(layout.data_size),
            rel,
            replay_cache: scan.records,
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
        let layout = compute_layout(opts, wal_min_bytes, wal_max_bytes)?;
        validate_allocator_region(&layout)?;
        let mut header = FileHeader::default();
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
            file,
            wal,
            layout,
            superblock_a,
            superblock_b,
            active_superblock: ActiveSuperblock::A,
            allocator: PageAllocator::new(layout.data_size),
            rel: RelState::new(DEFAULT_CAPACITY_BYTES),
            replay_cache: Vec::new(),
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
        let (result, wedged) = match f(&mut ctx, &mut txn) {
            Ok(value) => match ctx.commit(txn) {
                Ok(()) => (Ok(value), false),
                // The commit record may or may not have reached the disk;
                // writing CLRs now could undo a durable commit. Wedge and
                // let restart recovery decide (commit durable -> winner,
                // else -> loser undone).
                Err(err) => (Err(err), true),
            },
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
        result
    }

    /// Runs one statement under `scope`: autocommit (begin+commit) or appended
    /// to a caller-held transaction (no commit; partial ops survive an error).
    fn rel_statement_scoped<T>(
        &mut self,
        scope: &mut TxnScope,
        f: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        match scope {
            TxnScope::Auto => self.rel_statement(f),
            TxnScope::Explicit(stx) => {
                let mut ctx = self.rel_ctx();
                f(&mut ctx, &mut stx.txn)
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
        // The transaction is now open (its undo records must survive until it
        // commits or rolls back, which gates checkpoints — see `active_txns`).
        self.rel.active_txns += 1;
        Ok(StorageTxn { txn, roots })
    }

    /// Commits a caller-held transaction (forces the log). A failure wedges the
    /// store, as for autocommit commits.
    fn commit_txn(&mut self, stx: StorageTxn) -> Result<(), StorageError> {
        // The transaction is ending (the `StorageTxn` is consumed either way).
        self.rel.active_txns = self.rel.active_txns.saturating_sub(1);
        let mut ctx = self.rel_ctx();
        match ctx.commit(stx.txn) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.rel.wedged = true;
                Err(err)
            }
        }
    }

    /// Rolls back a caller-held transaction via its in-memory undo log (CLRs).
    fn rollback_txn(&mut self, stx: StorageTxn) -> Result<(), StorageError> {
        self.rel.active_txns = self.rel.active_txns.saturating_sub(1);
        let roots = stx.roots;
        let mut ctx = self.rel_ctx();
        match rel_recovery::rollback(&mut ctx, stx.txn, &roots) {
            Ok(()) => {
                let _ = ctx.io.wal.sync_all();
                Ok(())
            }
            Err(err) => {
                self.rel.wedged = true;
                Err(err)
            }
        }
    }

    /// Whether any explicit transaction is open. A checkpoint must be skipped
    /// while this is true (its WAL truncation would discard undo records still
    /// needed to roll the open transaction back after a crash).
    fn has_active_transactions(&self) -> bool {
        self.rel.active_txns > 0
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
        // Flush every dirty relational page (WAL-before-data enforced per
        // page by the pool) and reset the dirty-page table: the next change
        // to any page starts a fresh FPI epoch. With the engine single-
        // threaded, no relational transaction can span a checkpoint, so the
        // WAL head may advance to the tail below (a future concurrent engine
        // must clamp it to the oldest active transaction's begin LSN).
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

        // 4. Advance the WAL head and publish both superblocks (new active
        //    first).
        self.wal.set_head(self.wal.tail());
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
        let new_sb = |active_flag: u8| -> Superblock {
            let mut sb = Superblock::default();
            sb.generation = generation;
            sb.active = active_flag;
            sb.wal_head = head;
            sb.wal_tail = tail;
            sb.last_committed_seq = checkpoint_seq;
            sb.snapshot_root = desc_offset;
            sb.data_root = data_write_offset;
            sb.metadata_root = metadata_root;
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
        }
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
