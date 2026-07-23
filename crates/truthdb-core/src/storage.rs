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
use crate::relstore::overflow::{self, OVERFLOW_INLINE_MAX};
use crate::relstore::recovery as rel_recovery;
use crate::relstore::row::{Column, Schema, decode_row, decode_row_projected, encode_row};
use crate::relstore::types::{ColumnType, Datum, TypeError};
use crate::relstore::version::{
    PendingVersion, ReadSnapshot, Resolved, RowChange, decode_rid_identity, rid_identity,
};
use crate::storage_layout::{
    FileHeader, PAGE_SIZE, SNAPSHOT_DESCRIPTOR_SIZE, SUPERBLOCK_ACTIVE_A, SUPERBLOCK_ACTIVE_B,
    SnapshotDescriptor, Superblock, WAL_ENTRY_HEADER_SIZE, WAL_ENTRY_TYPE_REL, WAL_MAX_BYTES,
    WAL_MIN_BYTES, WalEntryHeader, align_down, assert_layout_invariants, wal_entry_padded_len,
};
use crate::wal::records::{
    REL_KIND_ALLOC_EXTENT, REL_KIND_FREE_EXTENT, REL_KIND_SET_CATALOG_ROOT, RelRecord,
};
use crate::wal::{WalWriter, scan_ring};

pub use crate::wal::WalRecord;

impl From<TypeError> for StorageError {
    fn from(err: TypeError) -> Self {
        StorageError::InvalidConfig(err.0)
    }
}

/// Version stamped in REL wal entries (entry-level, distinct from the record
/// kinds inside). v2 adds a commit-record timestamp for point-in-time restore;
/// v1 records decode unchanged (nothing gates on the version).
const REL_WAL_ENTRY_VERSION: u16 = 2;

// Fixed (built-in) principals are SYNTHESIZED at read time, never stored. Their
// principal_ids sit in a reserved band far above any real `object_id` (which
// starts at FIRST_USER_OBJECT_ID=2 and increments by one), so they can never
// collide with a created table, index, login, user, or role.
pub(crate) const FIXED_PRINCIPAL_BASE: u32 = 0x7000_0000;
pub(crate) const DBO_ID: u32 = FIXED_PRINCIPAL_BASE; // the dbo database user
pub(crate) const SYSADMIN_ID: u32 = FIXED_PRINCIPAL_BASE + 1; // server role, bypass-all
pub(crate) const DB_OWNER_ID: u32 = FIXED_PRINCIPAL_BASE + 2;
pub(crate) const DB_DATAREADER_ID: u32 = FIXED_PRINCIPAL_BASE + 3;
pub(crate) const DB_DATAWRITER_ID: u32 = FIXED_PRINCIPAL_BASE + 4;
pub(crate) const DB_DDLADMIN_ID: u32 = FIXED_PRINCIPAL_BASE + 5;
pub(crate) const PUBLIC_ID: u32 = FIXED_PRINCIPAL_BASE + 6; // every user is a member

/// A built-in principal, synthesized (not stored) into `sys.database_principals`,
/// name resolution for membership DDL, and the membership edge set.
pub(crate) struct FixedPrincipal {
    pub id: u32,
    pub name: &'static str,
    pub kind: crate::relstore::catalog::PrincipalKind,
    /// True for the sysadmin SERVER role (a login's role), false for database
    /// roles/users (a user's role). Governs which view/intrinsic family sees it.
    pub is_server: bool,
}

/// The built-in principals: the dbo user, the sysadmin server role, and the
/// fixed database roles. `public` is implicit (every database principal belongs
/// to it) and is listed so it appears in the catalog view.
pub(crate) const FIXED_PRINCIPALS: &[FixedPrincipal] = {
    use crate::relstore::catalog::PrincipalKind::{Role, User};
    &[
        FixedPrincipal {
            id: DBO_ID,
            name: "dbo",
            kind: User,
            is_server: false,
        },
        FixedPrincipal {
            id: SYSADMIN_ID,
            name: "sysadmin",
            kind: Role,
            is_server: true,
        },
        FixedPrincipal {
            id: DB_OWNER_ID,
            name: "db_owner",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: DB_DATAREADER_ID,
            name: "db_datareader",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: DB_DATAWRITER_ID,
            name: "db_datawriter",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: DB_DDLADMIN_ID,
            name: "db_ddladmin",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: PUBLIC_ID,
            name: "public",
            kind: Role,
            is_server: false,
        },
    ]
};

/// The fixed principal with this (case-insensitive) name, if any.
pub(crate) fn fixed_principal_by_name(name: &str) -> Option<&'static FixedPrincipal> {
    FIXED_PRINCIPALS
        .iter()
        .find(|p| p.name.eq_ignore_ascii_case(name))
}

/// The fixed principal with this id, if any.
pub(crate) fn fixed_principal_by_id(id: u32) -> Option<&'static FixedPrincipal> {
    FIXED_PRINCIPALS.iter().find(|p| p.id == id)
}

/// An empty-schema catalog row carrying a principal payload (login / user / role).
fn principal_table_def(
    object_id: u32,
    name: String,
    principal: crate::relstore::catalog::PrincipalDef,
) -> TableDef {
    TableDef {
        object_id,
        name,
        columns: Vec::new(),
        key_columns: Vec::new(),
        root_page: 0,
        defaults: Vec::new(),
        collations: Vec::new(),
        identity: None,
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        view_query: None,
        procedure: None,
        function: None,
        trigger: None,
        principal: Some(principal),
        permissions: Vec::new(),
        counter_page: None,
    }
}

/// Memoized role-membership closures, tagged with the security version they were
/// computed under (a mismatch discards `closure` and rebuilds `edges`). `version`
/// is `None` until first loaded — distinct from `Some(0)`, the valid initial
/// security version, so the very first query on a fresh or restarted database
/// (still at version 0) rebuilds from the live catalog rather than serving the
/// empty default.
#[derive(Default)]
struct MembershipCache {
    version: Option<u64>,
    /// principal_id -> its DIRECT role principal_ids (stored + synthesized).
    edges: std::collections::HashMap<u32, Vec<u32>>,
    /// principal_id -> its transitively-closed role set (computed on demand).
    closure: std::collections::HashMap<u32, std::collections::HashSet<u32>>,
}

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
    /// FULL recovery model mirror (vs SIMPLE). Read without the mutex by
    /// `sys.databases` and (later) the log-backup hold / 9002 decision.
    recovery_full: std::sync::atomic::AtomicBool,
    /// Bumped whenever the options change: a parked batch whose lock set was
    /// analyzed under an older epoch is re-analyzed before it can be granted
    /// (its versioned-read decision may no longer match execution).
    lock_epoch: std::sync::atomic::AtomicU64,
    /// Bumped by every security DDL (CREATE/DROP USER/ROLE, ALTER ROLE ADD/DROP
    /// MEMBER, and later GRANT/DENY/REVOKE). Separate from `lock_epoch` because
    /// authorization changes no batch's lock set; it invalidates the effective-
    /// membership cache instead. In-memory only (rebuilt from the catalog on
    /// restart, so a 0 reset is correct — nothing membership-cached is durable).
    security_version: std::sync::atomic::AtomicU64,
    /// Memoized transitive-closure of role membership, tagged with the
    /// `security_version` it was computed under; a version mismatch discards it.
    membership: std::sync::Mutex<MembershipCache>,
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
        let recovery_full = file.version.recovery_full;
        Ok(Storage {
            path,
            inner: std::sync::Mutex::new(file),
            gc,
            log_writer: Some(log_writer),
            rcsi: std::sync::atomic::AtomicBool::new(rcsi),
            allow_snapshot: std::sync::atomic::AtomicBool::new(allow_snapshot),
            recovery_full: std::sync::atomic::AtomicBool::new(recovery_full),
            lock_epoch: std::sync::atomic::AtomicU64::new(0),
            security_version: std::sync::atomic::AtomicU64::new(0),
            membership: std::sync::Mutex::new(MembershipCache::default()),
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
        let file = StorageFile::open_existing(path.clone(), None)?;
        Self::with_log_writer(path, file)
    }

    /// Opens with recovery stopped at a point in time: transactions whose commit
    /// timestamp is past `stop_at` are undone (point-in-time restore). Used by
    /// the restore path to validate + finalize a `--stopat` recovery.
    fn open_with_stop_at(path: PathBuf, stop_at: u64) -> Result<Self, StorageError> {
        assert_layout_invariants();
        let file = StorageFile::open_existing(path.clone(), Some(stop_at))?;
        Self::with_log_writer(path, file)
    }

    /// OFFLINE file growth (Stage 14): extends the data region of a CLOSED
    /// store by `add_gib` GiB. The server must not have the file open.
    ///
    /// The data region ends where the tail regions (metadata, allocator,
    /// snapshot, reserved) begin, so growth shifts the tail right by the
    /// delta. Crash safety comes from a size floor, not ordering tricks: the
    /// delta must be at least the tail's whole span, which puts every
    /// relocated region entirely in the fresh extension — nothing the OLD
    /// header points at is touched until the new header is stamped last
    /// (fsync-fenced, the v1→v2 upgrade's commit-point pattern). A crash
    /// before the stamp leaves a longer file under the old, fully valid
    /// layout; re-running the grow completes it.
    ///
    /// Everything inside the regions survives untouched: page numbers, RIDs
    /// and catalog roots are data-region-relative or absolute below the tail,
    /// the WAL sits before the data region, and the allocator bitmap is
    /// copied with zero (= free) extension bits — recovery replays pending
    /// WAL allocations on top exactly as it would have.
    pub fn grow_data_region(path: &Path, add_gib: u64) -> Result<u64, StorageError> {
        if add_gib == 0 {
            return Err(StorageError::InvalidConfig(
                "growth must be at least 1 GiB".to_string(),
            ));
        }
        let delta = add_gib.checked_mul(1024 * 1024 * 1024).ok_or_else(|| {
            StorageError::InvalidConfig(format!("growth of {add_gib} GiB overflows"))
        })?;
        let mut file = DirectFile::open_existing(path.to_path_buf())?;
        let mut header_bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
        file.read_exact_at(0, &mut header_bytes)?;
        let mut header = FileHeader::from_le_bytes(&header_bytes);
        if header.magic != crate::storage_layout::FILE_MAGIC {
            return Err(StorageError::InvalidFile("bad magic".to_string()));
        }
        if header.version != crate::storage_layout::FILE_VERSION {
            return Err(StorageError::InvalidFile(format!(
                "grow requires a v{} file, found v{}",
                crate::storage_layout::FILE_VERSION,
                header.version
            )));
        }
        if header.header_checksum != header.compute_checksum() {
            return Err(StorageError::InvalidFile(
                "header checksum mismatch".to_string(),
            ));
        }

        let tail_span = header.metadata_size
            + header.allocator_size
            + header.snapshot_size
            + header.reserved_size;
        if delta < tail_span {
            return Err(StorageError::InvalidConfig(format!(
                "growth of {add_gib} GiB is below the safe minimum of {} GiB for this file \
                 (the relocated regions must clear the old layout entirely)",
                tail_span.div_ceil(1024 * 1024 * 1024)
            )));
        }
        let new_data_size = header.data_size + delta;
        let new_data_pages = new_data_size / PAGE_SIZE as u64;
        let new_bitmap_len = new_data_pages.div_ceil(8);
        if new_bitmap_len > header.allocator_size {
            return Err(StorageError::InvalidConfig(format!(
                "the allocator region ({} bytes) cannot hold the bitmap for {} data pages",
                header.allocator_size, new_data_pages
            )));
        }

        // Read the payloads that move BEFORE any write: the allocator bitmap
        // (as much of it as the old data region used) and both snapshot
        // descriptor pages, verbatim.
        let old_bitmap_len = (header.data_size / PAGE_SIZE as u64).div_ceil(8) as usize;
        let mut bitmap = vec![0u8; old_bitmap_len];
        file.read_exact_at(header.allocator_offset, &mut bitmap)?;
        let mut descriptors = vec![0u8; 2 * PAGE_SIZE];
        file.read_exact_at(header.snapshot_offset, &mut descriptors)?;

        // Extend the file (a separate buffered handle; O_DIRECT is for page
        // I/O, not metadata), fsynced before anything lands in the extension.
        let old_total = header.reserved_offset + header.reserved_size;
        let plain = std::fs::OpenOptions::new().write(true).open(path)?;
        plain.set_len(old_total + delta)?;
        plain.sync_all()?;
        drop(plain);

        // Write the relocated payloads into the extension (all beyond the
        // old file end by the size floor above). The bitmap's new bytes stay
        // zero — the grown pages are free.
        file.write_all_at(header.allocator_offset + delta, &bitmap)?;
        file.write_all_at(header.snapshot_offset + delta, &descriptors)?;
        file.sync_data()?;

        // Commit point: the header flips to the new layout.
        header.data_size = new_data_size;
        header.metadata_offset += delta;
        header.allocator_offset += delta;
        header.snapshot_offset += delta;
        header.reserved_offset += delta;
        header.header_checksum = header.compute_checksum();
        file.write_all_at(0, &header.to_le_bytes_with_checksum())?;
        file.sync_data()?;
        Ok(new_data_pages)
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

    /// The current WAL head (the reclaim floor) — for tests that verify the
    /// FULL-model log-backup hold pins truncation.
    #[cfg(test)]
    pub(crate) fn wal_head(&self) -> u64 {
        self.lock().wal.head()
    }

    /// The persisted log-backup floor (tests).
    #[cfg(test)]
    pub(crate) fn last_log_backup_lsn(&self) -> u64 {
        self.lock().last_log_backup_lsn
    }

    /// The active FULL-model log-backup truncation hold, if any (tests).
    #[cfg(test)]
    pub(crate) fn log_backup_hold(&self) -> Option<u64> {
        self.lock().truncation_gate.log_backup
    }

    /// Online full backup: writes a self-describing `TDBBAK1` file at `dst`
    /// capturing the database as of a consistent recovery point.
    ///
    /// The copy is fuzzy: pages are read in bounded chunks, each under the
    /// storage lock but releasing it between chunks so writers proceed. A
    /// truncation hold pins the WAL at `redo_start` for the duration, and the
    /// log `[redo_start, backup_end)` is shipped into the backup. `backup_end`
    /// is captured *after* the page copy, so it is at least the latest-change
    /// LSN of every page copied; ARIES redo therefore heals every page image —
    /// however stale, or with a future change already baked in — to the single
    /// `backup_end` point on restore, then undoes the transactions in flight
    /// there. `dst` is created; an existing file is truncated.
    pub fn backup_full(&self, dst: &Path) -> Result<crate::backup::BackupSummary, StorageError> {
        self.backup_full_with(dst, true, false)
    }

    /// Online full backup with explicit `WITH` options (`checksum` = verify
    /// every page as copied; `copy_only` = do not disturb the log-backup chain).
    pub fn backup_full_with(
        &self,
        dst: &Path,
        checksum: bool,
        copy_only: bool,
    ) -> Result<crate::backup::BackupSummary, StorageError> {
        let plan = self.lock().begin_backup(checksum, copy_only)?;
        // Release the hold on EVERY exit — normal return, an early `?` error out
        // of write_backup, or an unwind — via the guard's Drop. A leaked hold
        // permanently freezes WAL truncation and eventually wedges writes.
        let _hold = BackupHoldGuard { storage: self };
        self.write_backup(dst, &plan)
    }

    /// `BACKUP LOG`: ships the FULL-model log tail to a `TDBBAK1` log archive
    /// and advances the log-backup floor, releasing the ring it held. Requires
    /// the FULL recovery model.
    pub fn backup_log(
        &self,
        dst: &Path,
        checksum: bool,
        copy_only: bool,
    ) -> Result<crate::backup::BackupSummary, StorageError> {
        // Phase 1 (locked): capture the log range + header. The OLD marker still
        // pins `[start, ...)`, so no checkpoint can truncate the range we are
        // about to ship, even after we release the lock.
        let (header, start, end, log) = self.lock().begin_log_backup(checksum, copy_only)?;
        // Release the single-flight guard on EVERY exit — an early `?` out of
        // write_log_archive, a panic, or normal completion.
        let _guard = LogBackupGuard { storage: self };
        // Phase 2 (UNLOCKED): write and fsync the archive — including its parent
        // directory — so concurrent DML proceeds during the copy-out (BACKUP LOG
        // is online, like BACKUP DATABASE).
        write_log_archive(dst, &header, start, &log)?;
        // Phase 3 (locked): the archive is durable — durably advance the marker
        // and the hold (unless the FULL-model state changed meanwhile), releasing
        // `[start, end)` for reclamation. Copy-out strictly before truncate.
        self.lock().finish_log_backup(start, end)?;
        Ok(crate::backup::BackupSummary {
            redo_start_lsn: start,
            backup_end_lsn: end,
            pages_copied: 0,
            log_bytes: log.len() as u64,
            finished_at_millis: header.finished_at_millis,
        })
    }

    fn write_backup(
        &self,
        dst: &Path,
        plan: &BackupPlan,
    ) -> Result<crate::backup::BackupSummary, StorageError> {
        use crate::backup::{BlockType, encode_alloc_map, encode_log_chunk, encode_page_run};
        let file = std::fs::File::create(dst)?;
        let mut writer =
            crate::backup::BackupWriter::new(std::io::BufWriter::new(file), &plan.header())?;
        writer.write_block(BlockType::AllocMap, &encode_alloc_map(&plan.runs))?;

        let mut pages_copied = 0u64;
        let mut buf = vec![0u8; BACKUP_CHUNK_PAGES as usize * PAGE_SIZE];
        for &(run_start, run_count) in &plan.runs {
            let mut page = run_start;
            let end = run_start + run_count;
            while page < end {
                let chunk = (end - page).min(BACKUP_CHUNK_PAGES);
                let bytes = &mut buf[..chunk as usize * PAGE_SIZE];
                self.lock()
                    .read_pages_for_backup(page, chunk, bytes, plan.checksum)?;
                writer.write_block(BlockType::PageData, &encode_page_run(page, bytes))?;
                pages_copied += chunk;
                page += chunk;
            }
        }

        let (backup_end, log) = self.lock().ship_backup_log(plan.redo_start)?;
        writer.write_block(
            BlockType::LogChunk,
            &encode_log_chunk(plan.redo_start, &log),
        )?;
        writer.finish()?;
        Ok(crate::backup::BackupSummary {
            redo_start_lsn: plan.redo_start,
            backup_end_lsn: backup_end,
            pages_copied,
            log_bytes: log.len() as u64,
            finished_at_millis: plan.finished_at_millis,
        })
    }

    /// Offline restore: rebuilds a fresh database file at `dst_path` from the
    /// `TDBBAK1` backup at `bak_path`, then opens it once to run ARIES recovery,
    /// validating that the restored file is recoverable. `dst_path` must not
    /// already exist.
    /// Offline restore of a full backup with no log chain (recover to the full
    /// backup's own end).
    pub fn restore_full(dst_path: &Path, bak_path: &Path) -> Result<(), StorageError> {
        Self::restore_full_with_logs(dst_path, bak_path, &[], None)
    }

    /// Offline restore of a full backup followed by an ordered chain of
    /// `BACKUP LOG` archives. Recovers to the end of the last log, or — when
    /// `stop_at` is set — to that wall-clock point in time (transactions that
    /// committed past it are undone). Each archive must continue from where the
    /// previous coverage ended (no gap, error 4305); the whole recoverable range
    /// must fit in the WAL ring (a longer chain needs incremental restore, not
    /// yet supported).
    pub fn restore_full_with_logs(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
        stop_at: Option<u64>,
    ) -> Result<(), StorageError> {
        Self::restore_full_inner(dst_path, bak_path, log_paths, &[], stop_at, false)
    }

    /// Offline restore of a full backup (plus an optional `BACKUP LOG` chain)
    /// as a replication STANDBY seed: the file is stamped `is_standby` before
    /// its validating open, so recovery REPEATS history only (redo, no ARIES
    /// undo) and the file opens read-only. A plain restore's undo would roll
    /// back a transaction that was in flight at backup time with CLRs; if the
    /// primary later committed it, the CLRs' page LSNs would mask the shipped
    /// redo and the replica would silently diverge. Point-in-time restore is
    /// meaningless for a seed (the standby must match the primary, not a past
    /// point), so there is no `stop_at` here.
    pub fn restore_full_standby(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
    ) -> Result<(), StorageError> {
        Self::restore_full_inner(dst_path, bak_path, log_paths, &[], None, true)
    }

    /// Offline restore of a full backup followed by raw shipped WAL ring ranges —
    /// the physical-replication apply path (a standby seeded from a backup, fed
    /// the primary's `read_ring_range` bytes). Each range must continue from the
    /// current coverage (no gap, 4305); the whole recoverable range must fit the
    /// ring. Recovers to the end of the last range on open.
    pub fn restore_full_with_wal_ranges(
        dst_path: &Path,
        bak_path: &Path,
        wal_ranges: &[(u64, Vec<u8>)],
    ) -> Result<(), StorageError> {
        Self::restore_full_inner(dst_path, bak_path, &[], wal_ranges, None, false)
    }

    fn restore_full_inner(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
        wal_ranges: &[(u64, Vec<u8>)],
        stop_at: Option<u64>,
        standby: bool,
    ) -> Result<(), StorageError> {
        assert_layout_invariants();
        let reader = std::io::BufReader::new(std::fs::File::open(bak_path)?);
        let (backup, header) = crate::backup::BackupReader::new(reader)?;
        if header.page_size as usize != PAGE_SIZE {
            return Err(StorageError::InvalidFile(
                "backup page size mismatch".to_string(),
            ));
        }
        let layout = layout_from_backup_header(&header);
        // The header is only integrity-checked (xxh64), not authenticated, so a
        // tampered-but-valid backup could carry inconsistent sizes or drive
        // writes outside the data region. Reject a header whose regions do not
        // tile the file exactly before creating anything (a bogus total_size
        // would otherwise `set_len` a huge sparse file).
        if layout.reserved_offset.checked_add(layout.reserved_size) != Some(layout.total_size)
            || layout.data_size == 0
        {
            return Err(StorageError::InvalidFile(
                "backup header region sizes are inconsistent".to_string(),
            ));
        }
        let file = StorageFile::create_from_layout(
            dst_path.to_path_buf(),
            layout,
            header.default_collation.clone(),
        )?;
        // The destination now exists and is ours: remove the partial file if any
        // later step fails, so a retry (which requires a fresh destination) can
        // proceed. Everything ABOVE this point errors without having created it.
        let outcome = Self::restore_body(
            file, backup, &header, log_paths, wal_ranges, dst_path, stop_at, standby,
        );
        if outcome.is_err() {
            let _ = std::fs::remove_file(dst_path);
        }
        outcome
    }

    /// The part of a restore that owns the (already-created) destination: lays
    /// down page images + the log, applies the log chain and any raw WAL ranges,
    /// writes the superblock, and validates by opening (running recovery). A
    /// failure here leaves a partial file the caller removes.
    #[allow(clippy::too_many_arguments)]
    fn restore_body(
        mut file: StorageFile,
        mut backup: crate::backup::BackupReader<std::io::BufReader<std::fs::File>>,
        header: &crate::backup::BackupHeader,
        log_paths: &[std::path::PathBuf],
        wal_ranges: &[(u64, Vec<u8>)],
        dst_path: &Path,
        stop_at: Option<u64>,
        standby: bool,
    ) -> Result<(), StorageError> {
        use crate::backup::{BlockType, decode_alloc_map, decode_log_chunk, decode_page_run};
        let data_pages = header.data_size / PAGE_SIZE as u64;
        let mut runs: Vec<(u64, u64)> = Vec::new();
        let mut log: Option<(u64, Vec<u8>)> = None;
        while let Some((block_type, payload)) = backup.next_block()? {
            match block_type {
                BlockType::AllocMap => {
                    runs = decode_alloc_map(&payload)?;
                    for &(start, count) in &runs {
                        if start.checked_add(count).is_none_or(|end| end > data_pages) {
                            return Err(StorageError::InvalidFile(
                                "backup allocation run is outside the data region".to_string(),
                            ));
                        }
                    }
                }
                BlockType::PageData => {
                    let (start, count, bytes) = decode_page_run(&payload)?;
                    if start.checked_add(count).is_none_or(|end| end > data_pages) {
                        return Err(StorageError::InvalidFile(
                            "backup page run is outside the data region".to_string(),
                        ));
                    }
                    file.restore_pages(start, count, bytes)?;
                }
                BlockType::LogChunk => {
                    let (start_lsn, bytes) = decode_log_chunk(&payload)?;
                    // One chunk today; a future split emitter must produce them
                    // contiguously in LSN order — enforce it so a gap or a
                    // reorder fails loudly rather than seeding a wrong range.
                    match &mut log {
                        Some((first_start, acc)) => {
                            if start_lsn != *first_start + acc.len() as u64 {
                                return Err(StorageError::InvalidFile(
                                    "backup log chunks are not contiguous".to_string(),
                                ));
                            }
                            acc.extend_from_slice(bytes);
                        }
                        None => log = Some((start_lsn, bytes.to_vec())),
                    }
                }
                BlockType::Header | BlockType::Trailer => {}
            }
        }

        file.restore_allocator_bitmap(&runs)?;
        let mut tail = match &log {
            Some((start_lsn, bytes)) => file.seed_ring(*start_lsn, bytes)?,
            None => header.redo_start_lsn,
        };
        // Apply the log chain in order, extending the seeded ring past the full
        // backup's end. Each archive continues from the current coverage (no
        // gap) and the whole range must fit the ring.
        for log_path in log_paths {
            apply_log_archive(&mut file, log_path, header.redo_start_lsn, &mut tail)?;
        }
        // Apply raw shipped WAL ranges (physical replication) after the log
        // chain, on the same seeded ring, extending the tail further.
        for (from_lsn, bytes) in wal_ranges {
            apply_wal_range(
                &mut file,
                *from_lsn,
                bytes,
                header.redo_start_lsn,
                &mut tail,
            )?;
        }
        // The restored superblock brackets the ring at `[redo_start, tail)`; the
        // log-backup floor is the end of the applied chain (a fresh chain). A
        // standby seed is stamped BEFORE the validating open, so that open is
        // redo-only + read-only from the file's first moment.
        file.restore_superblock(header, tail, standby)?;
        file.sync_file()?;
        drop(file);

        // Validate + finalize: opening reruns allocator recovery + ARIES
        // relational recovery over the seeded ring. For a point-in-time restore,
        // recovery stops at `stop_at`, undoing later transactions with CLRs that
        // persist the point-in-time state across a normal reopen (their undo is
        // replayed and each undone txn is sealed with a TXN_END). Failing loudly
        // if the restored file is not recoverable.
        let storage = match stop_at {
            Some(ts) => Storage::open_with_stop_at(dst_path.to_path_buf(), ts)?,
            None => Storage::open(dst_path.to_path_buf())?,
        };
        drop(storage);
        Ok(())
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
        // A standby cannot checkpoint (it must keep the in-flight undo log until
        // promotion), so the AUTOMATIC path skips gracefully — a read batch that
        // triggers it must not fail with a checkpoint-refused error. An explicit
        // `write_checkpoint` still errors, telling an operator it is unsupported.
        if file.active_sb().is_standby() {
            return Ok(false);
        }
        // A wedged store's in-memory state is ahead of the durable log after a
        // failed fsync; checkpointing would flush and re-fsync exactly the data
        // whose durability failed (and was reported to the client as failed).
        if file.rel.wedged || file.wal_usage_ratio() < threshold {
            return Ok(false);
        }
        file.write_checkpoint(data, checkpoint_seq, next_seq_no, next_doc_id)?;
        Ok(true)
    }

    /// A standby's checkpoint-equivalent: flushes redone pages, persists the
    /// allocator bitmap, and advances the WAL ring head to the standby's OWN
    /// undo floor — reclaiming ring space without discarding the undo log an
    /// eventual promotion needs, without truncating search records the seed
    /// snapshot does not cover, and without ever appending to the (read-only)
    /// WAL or allocating anything durable (a standby writes no search
    /// snapshot: a locally chosen extent would collide with the primary's
    /// future logged allocations). Everything runs under one storage-lock
    /// hold, so no apply can interleave between the floor computation and the
    /// head advance. Returns whether it reclaimed anything.
    pub fn standby_restartpoint(&self) -> Result<bool, StorageError> {
        let mut file = self.lock();
        if !file.active_sb().is_standby() {
            return Ok(false);
        }
        // The PERSISTED tail, not the live one: a failed apply leaves the live
        // ring tail past the last fully-applied (decoded + redone + committed)
        // range, and a restartpoint must never publish — let alone advance the
        // head over — bytes whose redo never ran.
        let tail = file.active_sb().wal_tail;
        let att_floor = file.standby_att.values().min().copied().unwrap_or(tail);
        let search_floor = file.standby_search_floor.unwrap_or(tail);
        // `checkpoint_wal_head` folds the tail and the truncation-gate holds
        // (a backup in progress); the standby's own local ATT there is empty.
        let target = att_floor.min(search_floor).min(file.checkpoint_wal_head());
        if target <= file.wal.head() {
            return Ok(false);
        }
        // The same WAL-before-data discipline as a checkpoint: fsync the log,
        // flush every dirty redone page, persist the allocator bitmap — then
        // and only then move the head. A crash between any of these steps
        // reopens redo-only from the OLD head over already-flushed pages
        // (page-LSN-gated no-ops), which is consistent.
        file.wal.sync_all()?;
        {
            let layout_data_offset = file.layout.data_offset;
            let layout_data_pages = file.layout.data_size / PAGE_SIZE as u64;
            let StorageFile {
                rel,
                file: dfile,
                wal,
                ..
            } = &mut *file;
            let RelState { pool, dpt, .. } = rel;
            let mut io = PoolIo {
                file: dfile,
                wal,
                data_offset: layout_data_offset,
                data_pages: layout_data_pages,
            };
            pool.flush_all(&mut io)?;
            dpt.clear();
        }
        let bitmap = file.allocator.persistable_bitmap();
        if bitmap.len() as u64 > file.layout.allocator_size {
            return Err(StorageError::InvalidFile(
                "allocator bitmap exceeds allocator region".to_string(),
            ));
        }
        let allocator_offset = file.layout.allocator_offset;
        file.file.write_all_at(allocator_offset, &bitmap)?;
        file.file.sync_data()?;
        // Stamp the LIVE catalog root (exactly as a checkpoint does): records
        // below the new head — including any applied SET_CATALOG_ROOT — are
        // about to leave the ring, so a reopen must find the root in the
        // superblock rather than re-deriving it from redo.
        let metadata_root = file
            .rel
            .catalog_root
            .map(|page| file.layout.data_offset + page * PAGE_SIZE as u64)
            .unwrap_or(0);
        file.commit_superblock(|sb| {
            sb.wal_head = target;
            sb.wal_tail = tail;
            sb.metadata_root = metadata_root;
            sb.set_applied_lsn(tail);
            // A FULL-model seed carries the primary's frozen log-backup marker;
            // left below the advancing head, promotion's reopen would re-arm
            // the truncation hold BELOW the head and the first checkpoint
            // would drive `set_head` backward into reclaimed ring space. The
            // standby's marker is meaningless (its log chain belongs to the
            // primary; a promoted node starts a fresh chain with a fresh full
            // backup), so it rides the head.
            if sb.last_log_backup_lsn() < target {
                sb.set_last_log_backup_lsn(target);
            }
        })?;
        if file.last_log_backup_lsn < target {
            file.last_log_backup_lsn = target;
        }
        file.wal.set_head(target);
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

    pub fn rel_create_procedure(
        &self,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_create_procedure(name, procedure);
        // A parked batch analyzed against the OLD catalog could carry a stale
        // lock set for an EXEC of this name — same class as the option-flip
        // epoch (Stage 13): bump so the grant path re-analyzes.
        self.bump_lock_epoch();
        result
    }

    pub fn rel_alter_procedure(
        &self,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_alter_procedure(name, procedure);
        self.bump_lock_epoch();
        result
    }

    pub fn rel_create_function(
        &self,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_create_function(name, function);
        // Like a procedure: a table-reading function changes which locks a batch
        // that references it must hold, so a parked batch analyzed against the
        // old catalog carries a stale lock set — bump so the grant path
        // re-analyzes.
        self.bump_lock_epoch();
        result
    }

    pub fn rel_alter_function(
        &self,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_alter_function(name, function);
        self.bump_lock_epoch();
        result
    }

    pub fn rel_create_trigger(
        &self,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_create_trigger(name, trigger);
        // A trigger changes which locks a DML statement on its parent table must
        // hold (its body reads/writes other tables), so a parked batch analyzed
        // against the old catalog carries a stale lock set — bump to re-analyze.
        self.bump_lock_epoch();
        result
    }

    pub fn rel_alter_trigger(
        &self,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_alter_trigger(name, trigger);
        self.bump_lock_epoch();
        result
    }

    // Logins do not participate in lock analysis, so — unlike table/proc DDL —
    // they do not bump the lock-analysis epoch. They DO bump the security
    // version: which login is named `sa` decides the synthesized sa→sysadmin
    // membership edge, so creating/dropping/re-keying a login must invalidate the
    // membership cache.
    pub fn rel_create_login(
        &self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_login(name, principal)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_alter_login(
        &self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.lock().rel_alter_login(name, principal)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_drop_login(&self, name: &str) -> Result<bool, StorageError> {
        let dropped = self.lock().rel_drop_login(name)?;
        if dropped {
            self.bump_security_version();
        }
        Ok(dropped)
    }

    pub fn rel_login(&self, name: &str) -> Option<TableDef> {
        self.lock().rel_login(name)
    }

    pub fn rel_logins(&self) -> Vec<TableDef> {
        self.lock().rel_logins()
    }

    // Database-principal / role DDL bumps the security version (invalidating the
    // membership cache), NOT the lock epoch — authorization changes no lock set.
    pub fn rel_create_database_principal(
        &self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_database_principal(name, principal)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_drop_database_principal(&self, name: &str) -> Result<bool, StorageError> {
        let dropped = self.lock().rel_drop_database_principal(name)?;
        if dropped {
            self.bump_security_version();
        }
        Ok(dropped)
    }

    pub fn rel_add_role_member(&self, role: &str, member: &str) -> Result<(), StorageError> {
        self.lock().rel_add_role_member(role, member)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_drop_role_member(&self, role: &str, member: &str) -> Result<(), StorageError> {
        self.lock().rel_drop_role_member(role, member)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_database_principal(&self, name: &str) -> Option<TableDef> {
        self.lock().rel_database_principal(name)
    }

    pub fn rel_database_principals(&self) -> Vec<TableDef> {
        self.lock().rel_database_principals()
    }

    // GRANT/DENY/REVOKE bump the security version so a per-batch security context
    // is recomputed next batch (like membership DDL).
    pub fn rel_grant_object(
        &self,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
        deny: bool,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_grant_object(object, grantee, action, deny)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_revoke_object(
        &self,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
    ) -> Result<(), StorageError> {
        self.lock().rel_revoke_object(object, grantee, action)?;
        self.bump_security_version();
        Ok(())
    }

    /// The name of the principal with this id — a fixed principal, a login, or a
    /// database user/role. `None` for an unknown id.
    pub(crate) fn principal_name(&self, id: u32) -> Option<String> {
        if let Some(fixed) = fixed_principal_by_id(id) {
            return Some(fixed.name.to_string());
        }
        let guard = self.lock();
        guard
            .rel
            .principals
            .values()
            .chain(guard.rel.database_principals.values())
            .find(|d| d.object_id == id)
            .map(|d| d.name.clone())
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

    /// True if any trigger exists in the catalog — a cheap no-clone check that
    /// keeps the common no-trigger DML path off the firing machinery.
    pub fn rel_has_triggers(&self) -> bool {
        self.lock().rel_has_triggers()
    }

    /// The enabled triggers attached to `parent_object_id` that fire on `event`,
    /// in creation (object_id) order.
    pub fn rel_triggers_for(
        &self,
        parent_object_id: u32,
        event: crate::relstore::catalog::TriggerEvent,
    ) -> Vec<TableDef> {
        self.lock().rel_triggers_for(parent_object_id, event)
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

    /// Whether the database is in the FULL recovery model (vs SIMPLE).
    pub(crate) fn recovery_model_full(&self) -> bool {
        self.recovery_full
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
        recovery_full: Option<bool>,
    ) -> Result<(), StorageError> {
        let mut guard = self.lock();
        guard.set_db_options(rcsi, allow_snapshot, recovery_full)?;
        let (rcsi_now, allow_now, recovery_now) = (
            guard.version.rcsi,
            guard.version.allow_snapshot,
            guard.version.recovery_full,
        );
        drop(guard);
        self.rcsi
            .store(rcsi_now, std::sync::atomic::Ordering::Relaxed);
        self.allow_snapshot
            .store(allow_now, std::sync::atomic::Ordering::Relaxed);
        self.recovery_full
            .store(recovery_now, std::sync::atomic::Ordering::Relaxed);
        // After the mirrors, so a batch analyzed against a stale epoch is
        // always re-analyzed against the settled options.
        self.lock_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Bumps the lock-analysis epoch: a parked batch analyzed before a
    /// catalog/option change re-analyzes at grant instead of running under a
    /// stale lock set.
    fn bump_lock_epoch(&self) {
        self.lock_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// The lock-analysis epoch: parked batches analyzed under an older value
    /// are re-analyzed before grant (see the scheduler).
    pub(crate) fn lock_analysis_epoch(&self) -> u64 {
        self.lock_epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Bumps the security version after a security DDL commits. Release-ordered
    /// so a reader that observes the new version also observes the new catalog
    /// rows (the bump happens after the inner mutation returns).
    fn bump_security_version(&self) {
        self.security_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// The security version: the membership cache tagged with an older value is
    /// discarded and recomputed on the next query.
    pub(crate) fn security_version(&self) -> u64 {
        self.security_version
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The effective (transitively-closed) set of role principal_ids a principal
    /// belongs to, memoized and invalidated by [`Self::security_version`]. Does
    /// NOT include the principal itself. Cycle-safe (visited-set DFS), so a role
    /// graph that somehow contains a cycle terminates rather than hanging.
    pub(crate) fn effective_roles(&self, principal_id: u32) -> std::collections::HashSet<u32> {
        let version = self.security_version();
        // Fast path: a cache hit under the current version needs only the
        // membership lock.
        {
            let cache = self.membership.lock().expect("membership cache poisoned");
            if cache.version == Some(version)
                && let Some(hit) = cache.closure.get(&principal_id)
            {
                return hit.clone();
            }
        }
        // Slow path: (re)build. The edge set is collected WITHOUT the membership
        // lock held (it takes the storage lock) so the two locks are never nested
        // — collect first, then lock the cache. A version move between the read
        // and here just costs one recompute, corrected on the next call, exactly
        // like the lock-epoch discipline.
        let edges = self.collect_membership_edges();
        let mut cache = self.membership.lock().expect("membership cache poisoned");
        if cache.version != Some(version) {
            cache.version = Some(version);
            cache.closure.clear();
            cache.edges = edges;
        }
        if let Some(hit) = cache.closure.get(&principal_id) {
            return hit.clone();
        }
        let mut result = std::collections::HashSet::new();
        let mut stack = cache.edges.get(&principal_id).cloned().unwrap_or_default();
        while let Some(role) = stack.pop() {
            if result.insert(role)
                && let Some(parents) = cache.edges.get(&role)
            {
                stack.extend(parents.iter().copied());
            }
        }
        cache.closure.insert(principal_id, result.clone());
        result
    }

    /// The raw membership edge set `principal_id -> direct role principal_ids`,
    /// unioning stored `member_of` edges (logins + database principals) with the
    /// synthesized fixed-principal bootstrap edges (`sa -> sysadmin`,
    /// `dbo -> db_owner`).
    fn collect_membership_edges(&self) -> std::collections::HashMap<u32, Vec<u32>> {
        let guard = self.lock();
        let mut edges: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
        for def in guard
            .rel
            .principals
            .values()
            .chain(guard.rel.database_principals.values())
        {
            if let Some(principal) = &def.principal
                && !principal.member_of.is_empty()
            {
                edges
                    .entry(def.object_id)
                    .or_default()
                    .extend(principal.member_of.iter().copied());
            }
        }
        // Synthesized bootstrap: whichever login is `sa` is a member of the
        // sysadmin server role, and the dbo user is a member of db_owner.
        if let Some(sa) = guard.rel.principals.get("sa") {
            edges.entry(sa.object_id).or_default().push(SYSADMIN_ID);
        }
        edges.entry(DBO_ID).or_default().push(DB_OWNER_ID);
        edges
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

    /// Applies a shipped raw WAL ring range to this OPEN standby, live — without
    /// a reopen. Under one storage-lock hold it places the bytes durably in the
    /// ring, redoes their effects into the live buffer pool, refreshes the
    /// catalog cache, and records the advanced tail, so the standby stays
    /// queryable and its state matches the primary's up to `from_lsn +
    /// bytes.len()`. Idempotent: a re-shipped overlapping range is a no-op (redo
    /// is page-LSN-gated). The range must be contiguous with what the standby has
    /// already applied (a gap is 4305) and start/end on entry boundaries (the
    /// primary ships `read_wal_range` output at a flushed watermark).
    ///
    /// Redo only — no analysis or undo (those need the whole log and mutate the
    /// WAL); in-flight transactions are resolved at promotion, not here. So a
    /// shipped range ending mid-transaction leaves that transaction's rows
    /// applied but uncommitted: a plain standby `SELECT` can read them (a
    /// read-uncommitted anomaly). Serving consistent snapshot reads at the last
    /// applied commit is the readable-standby slice; until then a standby is a
    /// failover target, not a query replica.
    pub fn apply_wal_stream(&self, from_lsn: u64, bytes: &[u8]) -> Result<(), StorageError> {
        self.lock().apply_wal_stream_locked(from_lsn, bytes)
    }

    /// The durable WAL watermark: the greatest LSN fsynced to disk (group-commit
    /// or a direct WAL sync). A replication sender may ship the ring up to here;
    /// bytes past it are not yet durable on the primary and must not be applied
    /// to a standby.
    pub(crate) fn wal_flushed_lsn(&self) -> u64 {
        let durable = self.gc.flushed();
        self.lock().wal.flushed_lsn().max(durable)
    }

    /// Subscribes to group-commit durable-watermark advances so a tokio task
    /// (the replication sender) can await new shippable WAL. The carried value
    /// is a wake-up hint: WAL made durable by a direct sync bypasses the
    /// channel, so re-read [`Self::wal_flushed_lsn`] after each wake and pair
    /// the watch with a periodic tick.
    pub(crate) fn subscribe_wal_flushed(&self) -> tokio::sync::watch::Receiver<u64> {
        self.gc.subscribe_flushed()
    }

    /// Reads raw WAL ring bytes `[from, to)`. Test scaffolding: the production
    /// ship primitive is [`Self::read_wal_chunk`], which cuts on entry
    /// boundaries and fences against the WAL head.
    #[cfg(test)]
    pub(crate) fn read_wal_range(&self, from: u64, to: u64) -> Result<Vec<u8>, StorageError> {
        self.lock().read_ring_range(from, to)
    }

    /// The physical-replication ship primitive: reads one chunk of raw WAL
    /// ring bytes starting at `from`, ending on a WAL-ENTRY BOUNDARY at most
    /// `max_bytes` past `from` (a single oversized entry is returned whole),
    /// and never past `to_cap`. Returns the bytes and the chunk's end LSN —
    /// the only LSNs a sender may hand a standby, since
    /// [`Storage::apply_wal_stream`] persists its range end as the standby's
    /// applied tail and a mid-entry tail would make every later decode
    /// silently fail. The head fence and the read happen under one lock hold:
    /// if `from` is below the WAL head the log is already truncated (the
    /// standby's slot was reaped) and shipping would return recycled
    /// ring bytes from a newer lap — the error tells the standby to reseed.
    pub(crate) fn read_wal_chunk(
        &self,
        from: u64,
        to_cap: u64,
        max_bytes: u64,
    ) -> Result<(Vec<u8>, u64), StorageError> {
        let mut guard = self.lock();
        let head = guard.wal.head();
        if from < head {
            return Err(StorageError::InvalidConfig(format!(
                "replication position {from} is behind the WAL head ({head}): the log the \
                 standby needs was truncated (its slot lapsed); reseed the standby from a \
                 fresh backup"
            )));
        }
        let end = guard.wal_chunk_end(from, to_cap, max_bytes)?;
        let bytes = guard.read_ring_range(from, end)?;
        Ok((bytes, end))
    }

    /// The persisted replication restartpoint (the active superblock's
    /// `applied_lsn`): the LSN up to which this file's WAL is present and
    /// recovered. (Test-only until the monitoring slice reads it.)
    #[cfg(test)]
    pub(crate) fn applied_lsn(&self) -> u64 {
        let guard = self.lock();
        let active = match guard.active_superblock {
            ActiveSuperblock::A => &guard.superblock_a,
            ActiveSuperblock::B => &guard.superblock_b,
        };
        active.applied_lsn()
    }

    /// The LSN a standby resumes shipping from: the PERSISTED tail (the active
    /// superblock's), not the live one. A crash between an apply's ring fsync
    /// and its superblock commit — or a redo-only reopen that recovered extra
    /// durable ring bytes — leaves the live tail ahead of the persisted tail,
    /// and the apply continuity check compares against the persisted value; a
    /// resume from the live tail would then be a permanent 4305 gap. Resuming
    /// from the persisted tail re-ships the overlap, which is idempotent.
    pub(crate) fn standby_resume_lsn(&self) -> u64 {
        let guard = self.lock();
        let active = match guard.active_superblock {
            ActiveSuperblock::A => &guard.superblock_a,
            ActiveSuperblock::B => &guard.superblock_b,
        };
        active.wal_tail
    }

    /// Whether this file is a replication standby (redo-only, read-only until
    /// promotion).
    pub fn is_standby(&self) -> bool {
        let guard = self.lock();
        let active = match guard.active_superblock {
            ActiveSuperblock::A => &guard.superblock_a,
            ActiveSuperblock::B => &guard.superblock_b,
        };
        active.is_standby()
    }

    /// The persisted replication epoch (bumped once at each promotion; zero
    /// until the first failover). Both sides of the replication handshake
    /// exchange it so a diverged old primary's stream can be fenced off.
    pub(crate) fn epoch(&self) -> u64 {
        let guard = self.lock();
        let active = match guard.active_superblock {
            ActiveSuperblock::A => &guard.superblock_a,
            ActiveSuperblock::B => &guard.superblock_b,
        };
        active.epoch()
    }

    /// Durably sets the replication epoch (a promotion bumps it by one;
    /// test-only until the failover slice performs promotions).
    #[cfg(test)]
    pub(crate) fn set_epoch(&self, epoch: u64) -> Result<(), StorageError> {
        self.lock().commit_superblock(|sb| sb.set_epoch(epoch))
    }

    /// Registers (or resets) a replication slot at `lsn`, holding WAL-ring
    /// truncation there. Fails if `lsn` is behind the WAL head (the log the
    /// standby needs is already truncated — it must reseed) or if the slot
    /// table is full; the check and the insert happen under one lock hold, so
    /// a concurrent checkpoint cannot truncate between them.
    pub(crate) fn try_register_repl_slot(&self, id: u32, lsn: u64) -> Result<(), StorageError> {
        self.lock().try_register_repl_slot(id, lsn)
    }

    /// Advances a slot's held LSN (never backward). A no-op if the slot does
    /// not exist — an ack racing a reap must not resurrect a reaped slot.
    pub(crate) fn advance_repl_slot(&self, id: u32, lsn: u64) {
        self.lock().advance_repl_slot(id, lsn);
    }

    #[cfg(test)]
    pub(crate) fn drop_repl_slot(&self, id: u32) {
        self.lock().drop_repl_slot(id);
    }

    /// A slot's held LSN. (Test-only until the monitoring slice reads it.)
    #[cfg(test)]
    pub(crate) fn repl_slot_lsn(&self, id: u32) -> Option<u64> {
        self.lock().repl_slot_lsn(id)
    }

    /// Sets the slot-retention cap that the checkpoint reap enforces. The cap
    /// must be strictly below the ring's usable capacity (`wal_size -
    /// reserve`): at or above it, appends hit `WalFull` before any slot lags
    /// far enough to reap, wedging the primary behind a dead standby.
    pub fn set_max_slot_retain_bytes(&self, bytes: u64) -> Result<(), StorageError> {
        let mut guard = self.lock();
        let usable = guard.layout.wal_size.saturating_sub(guard.wal.reserve());
        if bytes >= usable {
            return Err(StorageError::InvalidConfig(format!(
                "max_slot_retain_bytes ({bytes}) must be below the WAL ring's usable capacity ({usable}); \
                 a cap at or above it wedges the primary with WalFull before the slot reap can run"
            )));
        }
        guard.max_slot_retain_bytes = bytes;
        Ok(())
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

    /// Whether any read snapshot is registered (an idle SNAPSHOT transaction
    /// between batches — running batches are excluded by the caller's
    /// Database X). `ALTER DATABASE` option flips refuse while one lives.
    pub(crate) fn has_registered_snapshots(&self) -> bool {
        self.lock().version.has_snapshots()
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

/// A row staged for insert: its clustered key (trees) and its encoding —
/// `None` when the table has (MAX) columns, whose oversize values must spill
/// inside the statement before the row can encode.
type StagedInsert = (Option<Vec<u8>>, Option<Vec<u8>>);
/// An in-place tree update: key, pre-encoded row or the values to encode
/// in-statement ((MAX) tables).
type StagedInPlace = (Vec<u8>, Option<Vec<u8>>, Option<Vec<Datum>>);
/// A re-keying tree update: old key, new key, then as [`StagedInPlace`].
type StagedRekey = (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<Vec<Datum>>);
/// A heap update: RID, then as [`StagedInPlace`]'s tail.
type StagedHeapUpdate = (Rid, Option<Vec<u8>>, Option<Vec<Datum>>);

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

struct StorageFile {
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
                procedure: None,
                function: None,
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: Some(counter_page),
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        // Stamp the new object: a SNAPSHOT transaction whose view predates
        // this CREATE must 3961 rather than silently read the (possibly
        // same-named, post-DROP) new table as empty — its snapshot has no
        // history for an object that did not exist.
        self.version.stamp_schema(def.object_id);
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
                procedure: None,
                function: None,
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Creates a stored procedure: a catalog entry whose stored form is its
    /// parameter list and body text (the view posture — re-parsed at EXEC).
    pub fn rel_create_procedure(
        &mut self,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
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
        let proc_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: proc_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: Some(procedure),
                function: None,
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Records a user-defined function in the catalog (`CREATE FUNCTION`): its
    /// parameters, return shape, and body text (the view posture — re-parsed at
    /// each call).
    pub fn rel_create_function(
        &mut self,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
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
        let func_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: func_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: Some(function),
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Replaces an existing function's definition (`ALTER FUNCTION`): the object
    /// id is kept, the stored definition swapped.
    pub fn rel_alter_function(
        &mut self,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let Some(existing) = self.rel.tables.get(name) else {
            return Err(StorageError::Constraint(format!(
                "function '{name}' does not exist"
            )));
        };
        if !existing.is_function() {
            return Err(StorageError::Constraint(format!(
                "object '{name}' is not a function"
            )));
        }
        let mut def = existing.clone();
        def.function = Some(function);
        let catalog_root = self
            .rel
            .catalog_root
            .expect("functions live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Replaces an existing procedure's parameters and body (`ALTER
    /// PROCEDURE`): the object id is kept, the stored text swapped.
    pub fn rel_alter_procedure(
        &mut self,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let Some(existing) = self.rel.tables.get(name) else {
            return Err(StorageError::Constraint(format!(
                "procedure '{name}' does not exist"
            )));
        };
        if !existing.is_procedure() {
            return Err(StorageError::Constraint(format!(
                "object '{name}' is not a procedure"
            )));
        }
        let mut def = existing.clone();
        def.procedure = Some(procedure);
        let catalog_root = self
            .rel
            .catalog_root
            .expect("procedures live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Creates a trigger: a catalog entry (its own object_id, like a procedure)
    /// whose stored form is its parent table, event set, and body text.
    pub fn rel_create_trigger(
        &mut self,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
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
        let trig_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: trig_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: None,
                trigger: Some(trigger),
                principal: None,
                permissions: Vec::new(),
                counter_page: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Replaces a trigger's definition (`ALTER TRIGGER`).
    pub fn rel_alter_trigger(
        &mut self,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let Some(existing) = self.rel.tables.get(name) else {
            return Err(StorageError::Constraint(format!(
                "trigger '{name}' does not exist"
            )));
        };
        if !existing.is_trigger() {
            return Err(StorageError::Constraint(format!(
                "object '{name}' is not a trigger"
            )));
        }
        let mut def = existing.clone();
        def.trigger = Some(trigger);
        let catalog_root = self.rel.catalog_root.expect("triggers live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.tables.insert(name.to_string(), def);
        Ok(())
    }

    /// Creates a server login: a catalog row (its own object_id, like a
    /// procedure) carrying the hashed password, routed into the principals map
    /// so it never enters the object namespace.
    pub fn rel_create_login(
        &mut self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let key = name.to_ascii_lowercase();
        if self.rel.principals.contains_key(&key) {
            return Err(StorageError::Constraint(format!(
                "login '{name}' already exists"
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
        let login_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: login_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: None,
                trigger: None,
                principal: Some(principal),
                permissions: Vec::new(),
                counter_page: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.principals.insert(key, def);
        Ok(())
    }

    /// Replaces a login's payload (`ALTER LOGIN` — new password or enable/
    /// disable). The name (and object_id) is preserved.
    pub fn rel_alter_login(
        &mut self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let key = name.to_ascii_lowercase();
        let Some(existing) = self.rel.principals.get(&key) else {
            return Err(StorageError::Constraint(format!(
                "login '{name}' does not exist"
            )));
        };
        let mut def = existing.clone();
        def.principal = Some(principal);
        let catalog_root = self.rel.catalog_root.expect("logins live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.principals.insert(key, def);
        Ok(())
    }

    /// Drops a login. Returns false if it does not exist.
    pub fn rel_drop_login(&mut self, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        let key = name.to_ascii_lowercase();
        let Some(def) = self.rel.principals.get(&key).cloned() else {
            return Ok(false);
        };
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        self.rel_statement(move |ctx, txn| {
            catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, def.object_id)
        })?;
        self.rel.principals.remove(&key);
        Ok(true)
    }

    /// A login's definition, by (case-insensitive) name.
    pub fn rel_login(&self, name: &str) -> Option<TableDef> {
        self.rel.principals.get(&name.to_ascii_lowercase()).cloned()
    }

    /// All logins, ordered by object id (for sys.server_principals).
    pub fn rel_logins(&self) -> Vec<TableDef> {
        let mut defs: Vec<TableDef> = self.rel.principals.values().cloned().collect();
        defs.sort_by_key(|d| d.object_id);
        defs
    }

    /// Creates a database user or role (an empty-schema row carrying the given
    /// principal payload), routed into the `database_principals` map. Rejects a
    /// name already taken by a user/role or a fixed principal.
    pub fn rel_create_database_principal(
        &mut self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let key = name.to_ascii_lowercase();
        if self.rel.database_principals.contains_key(&key)
            || fixed_principal_by_name(&key).is_some()
        {
            return Err(StorageError::Constraint(format!(
                "principal '{name}' already exists"
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
        let def = principal_table_def(object_id, name.to_string(), principal);
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.next_object_id += 1;
        self.rel.database_principals.insert(key, def);
        Ok(())
    }

    /// Drops a database user or role. A role that still has members is refused
    /// (dropping it would dangle their `member_of` edges — SQL Server 15144).
    /// Returns false if no such principal exists.
    pub fn rel_drop_database_principal(&mut self, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        let key = name.to_ascii_lowercase();
        let Some(def) = self.rel.database_principals.get(&key).cloned() else {
            return Ok(false);
        };
        let role_id = def.object_id;
        let is_role = matches!(
            def.principal.as_ref().map(|p| p.kind),
            Some(crate::relstore::catalog::PrincipalKind::Role)
        );
        if is_role && self.principal_has_members(role_id) {
            return Err(StorageError::Constraint(format!(
                "the role '{name}' has members and cannot be dropped"
            )));
        }
        let catalog_root = self
            .rel
            .catalog_root
            .expect("database principals live in the catalog");
        // Scrub the principal's object permission entries BEFORE deleting it.
        // Object_ids can be reused after a restart (next_object_id is recomputed
        // from the surviving max), so a dangling grantee id must never outlive
        // its principal. These are separate WAL transactions; doing the scrub
        // first means a crash mid-drop can leave a still-present principal with
        // fewer grants (harmless, and a re-run finishes) but never the dangerous
        // deleted-principal-with-dangling-grant state.
        self.scrub_grants_for(role_id)?;
        self.rel_statement(move |ctx, txn| {
            catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, def.object_id)
        })?;
        self.rel.database_principals.remove(&key);
        Ok(true)
    }

    /// Removes every object permission entry whose grantee is `grantee` (a
    /// dropped principal), rewriting each affected object's catalog row.
    fn scrub_grants_for(&mut self, grantee: u32) -> Result<(), StorageError> {
        let affected: Vec<String> = self
            .rel
            .tables
            .iter()
            .filter(|(_, def)| def.permissions.iter().any(|p| p.grantee == grantee))
            .map(|(name, _)| name.clone())
            .collect();
        for name in affected {
            let mut def = self.rel.tables.get(&name).cloned().expect("just listed");
            def.permissions.retain(|p| p.grantee != grantee);
            self.persist_object_permissions(&name, def)?;
        }
        Ok(())
    }

    /// Adds `member` (a user or role) to `role` (a database or fixed role).
    /// Rejects: an unknown role/member, a non-role target, self-membership, and
    /// any addition that would close a cycle in the role graph. Idempotent if the
    /// edge already exists.
    pub fn rel_add_role_member(&mut self, role: &str, member: &str) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let role_id = self
            .resolve_db_principal_id(role)
            .ok_or_else(|| StorageError::Constraint(format!("the role '{role}' does not exist")))?;
        if !self.is_role_id(role_id) {
            return Err(StorageError::Constraint(format!("'{role}' is not a role")));
        }
        let member_key = member.to_ascii_lowercase();
        let Some(existing) = self.rel.database_principals.get(&member_key).cloned() else {
            return Err(StorageError::Constraint(format!(
                "the member '{member}' does not exist"
            )));
        };
        let member_id = existing.object_id;
        if member_id == role_id {
            return Err(StorageError::Constraint(
                "a role cannot be a member of itself".into(),
            ));
        }
        let mut principal = existing
            .principal
            .clone()
            .expect("database principal has a payload");
        if principal.member_of.contains(&role_id) {
            return Ok(()); // already a member
        }
        // A cycle would form iff `role` can already reach `member` by following
        // membership (member is an ancestor of role).
        if self.reachable_roles(role_id).contains(&member_id) {
            return Err(StorageError::Constraint(format!(
                "adding '{member}' to role '{role}' would create a membership cycle"
            )));
        }
        principal.member_of.push(role_id);
        let mut def = existing;
        def.principal = Some(principal);
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.database_principals.insert(member_key, def);
        Ok(())
    }

    /// Removes `member` from `role`. Idempotent (no error if not a member); errors
    /// only on an unknown role or member.
    pub fn rel_drop_role_member(&mut self, role: &str, member: &str) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let role_id = self
            .resolve_db_principal_id(role)
            .ok_or_else(|| StorageError::Constraint(format!("the role '{role}' does not exist")))?;
        let member_key = member.to_ascii_lowercase();
        let Some(existing) = self.rel.database_principals.get(&member_key).cloned() else {
            return Err(StorageError::Constraint(format!(
                "the member '{member}' does not exist"
            )));
        };
        let mut principal = existing
            .principal
            .clone()
            .expect("database principal has a payload");
        if !principal.member_of.contains(&role_id) {
            return Ok(()); // not a member
        }
        principal.member_of.retain(|&r| r != role_id);
        let mut def = existing;
        def.principal = Some(principal);
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.database_principals.insert(member_key, def);
        Ok(())
    }

    /// A database user/role definition, by (case-insensitive) name.
    pub fn rel_database_principal(&self, name: &str) -> Option<TableDef> {
        self.rel
            .database_principals
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    /// All stored database users and roles, ordered by object id (fixed
    /// principals are synthesized by the caller, not stored here).
    pub fn rel_database_principals(&self) -> Vec<TableDef> {
        let mut defs: Vec<TableDef> = self.rel.database_principals.values().cloned().collect();
        defs.sort_by_key(|d| d.object_id);
        defs
    }

    /// Resolves a database-scoped principal NAME (a stored user/role or a fixed
    /// database principal — dbo, db_owner, …, public; NOT the server sysadmin
    /// role) to its principal_id.
    fn resolve_db_principal_id(&self, name: &str) -> Option<u32> {
        if let Some(def) = self.rel_database_principal(name) {
            return Some(def.object_id);
        }
        fixed_principal_by_name(name)
            .filter(|p| !p.is_server)
            .map(|p| p.id)
    }

    /// True if the id is a role (a stored role or a fixed database role).
    fn is_role_id(&self, id: u32) -> bool {
        if let Some(p) = fixed_principal_by_id(id) {
            return matches!(p.kind, crate::relstore::catalog::PrincipalKind::Role);
        }
        self.rel
            .database_principals
            .values()
            .any(|d| d.object_id == id && d.is_role())
    }

    /// GRANT or DENY an action on an object to a grantee (a database user/role or
    /// a fixed database principal, incl. `public`). Replaces any existing entry
    /// for the same (grantee, action) — a GRANT and a DENY of the same action to
    /// the same grantee do not coexist. Errors on an unknown grantee or object.
    pub fn rel_grant_object(
        &mut self,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
        deny: bool,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let grantee_id = self.resolve_grantee_id(grantee)?;
        let Some(mut def) = self.rel.tables.get(object).cloned() else {
            return Err(StorageError::Constraint(format!(
                "cannot find the object '{object}', because it does not exist or you do not have permission"
            )));
        };
        def.permissions
            .retain(|p| !(p.grantee == grantee_id && p.action == action));
        def.permissions
            .push(crate::relstore::catalog::PermissionEntry {
                grantee: grantee_id,
                action,
                deny,
            });
        self.persist_object_permissions(object, def)
    }

    /// REVOKE an action on an object from a grantee — removes both the GRANT and
    /// the DENY of that (grantee, action). Idempotent (no error if absent).
    pub fn rel_revoke_object(
        &mut self,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        let grantee_id = self.resolve_grantee_id(grantee)?;
        let Some(mut def) = self.rel.tables.get(object).cloned() else {
            return Err(StorageError::Constraint(format!(
                "cannot find the object '{object}', because it does not exist or you do not have permission"
            )));
        };
        let before = def.permissions.len();
        def.permissions
            .retain(|p| !(p.grantee == grantee_id && p.action == action));
        if def.permissions.len() == before {
            return Ok(()); // nothing to revoke
        }
        self.persist_object_permissions(object, def)
    }

    /// Resolves a grantee NAME to a database principal_id (a stored user/role or
    /// a fixed database principal, incl. `public`; NOT the server sysadmin role).
    fn resolve_grantee_id(&self, grantee: &str) -> Result<u32, StorageError> {
        self.resolve_db_principal_id(grantee).ok_or_else(|| {
            StorageError::Constraint(format!(
                "cannot find the principal '{grantee}', because it does not exist"
            ))
        })
    }

    /// Writes an object's mutated permission list back through the catalog and
    /// the in-memory cache (one whole-row rewrite, like a role-member edit).
    fn persist_object_permissions(
        &mut self,
        object: &str,
        def: TableDef,
    ) -> Result<(), StorageError> {
        let catalog_root = self.rel.catalog_root.expect("objects live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.tables.insert(object.to_string(), def);
        Ok(())
    }

    /// True if any principal (login or database) is a direct member of `role_id`.
    fn principal_has_members(&self, role_id: u32) -> bool {
        self.rel
            .database_principals
            .values()
            .chain(self.rel.principals.values())
            .any(|d| {
                d.principal
                    .as_ref()
                    .is_some_and(|p| p.member_of.contains(&role_id))
            })
    }

    /// The transitive ancestors of `start` (the roles it belongs to, directly or
    /// indirectly), over stored `member_of` edges. Visited-set guarded, so a
    /// pre-existing cycle terminates. Used to detect a would-be cycle before
    /// adding an edge.
    fn reachable_roles(&self, start: u32) -> std::collections::HashSet<u32> {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![start];
        while let Some(id) = stack.pop() {
            let parents = self
                .rel
                .database_principals
                .values()
                .chain(self.rel.principals.values())
                .find(|d| d.object_id == id)
                .and_then(|d| d.principal.as_ref())
                .map(|p| p.member_of.clone())
                .unwrap_or_default();
            for r in parents {
                if seen.insert(r) {
                    stack.push(r);
                }
            }
        }
        seen
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

    /// True if any trigger exists in the catalog (no clone).
    pub fn rel_has_triggers(&self) -> bool {
        self.rel.tables.values().any(|d| d.is_trigger())
    }

    /// The enabled triggers attached to `parent_object_id` firing on `event`, in
    /// creation (object_id) order.
    pub fn rel_triggers_for(
        &self,
        parent_object_id: u32,
        event: crate::relstore::catalog::TriggerEvent,
    ) -> Vec<TableDef> {
        let mut trigs: Vec<TableDef> = self
            .rel
            .tables
            .values()
            .filter(|d| {
                d.trigger.as_ref().is_some_and(|t| {
                    !t.is_disabled
                        && t.parent_object_id == parent_object_id
                        && t.events.contains(&event)
                })
            })
            .cloned()
            .collect();
        trigs.sort_by_key(|d| d.object_id);
        trigs
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

        // Every row gets the frozen fill appended; the re-encode happens
        // INSIDE the statement so a (MAX) value the located scan resolved
        // re-spills to a fresh overflow chain instead of blowing the in-row
        // caps (the #123 review's finding: ALTER was unusable once a table
        // held a real payload). Non-MAX tables pay one no-op spill scan.
        let mut tree_rows: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
        let mut heap_rows: Vec<(Rid, Vec<Datum>)> = Vec::new();
        for (loc, mut values) in located {
            values.push(fill.clone());
            match loc {
                RowLocator::Key(key) => tree_rows.push((key, values)),
                RowLocator::Rid(rid) => heap_rows.push((rid, values)),
            }
        }

        let is_tree = def.is_tree();
        let object_id = def.object_id;
        let root_page = def.root_page;
        let closure_schema = new_schema.clone();
        let updated = self.rel_statement(move |ctx, txn| {
            if is_tree {
                let tree = BTree {
                    object_id,
                    root: root_page,
                };
                for (key, mut values) in tree_rows {
                    Self::spill_max_values(ctx, &closure_schema, &mut values)?;
                    let row = encode_row(&closure_schema, &values)?;
                    tree.update(ctx, &mut OpMode::Txn(txn), &key, &row)?;
                }
            } else {
                let heap = Heap {
                    object_id,
                    first_page: root_page,
                };
                for (rid, mut values) in heap_rows {
                    Self::spill_max_values(ctx, &closure_schema, &mut values)?;
                    let row = encode_row(&closure_schema, &values)?;
                    heap.update(ctx, txn, rid, &row)?;
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
        let types = Self::projected_types(&schema, projection);
        self.resolve_overflow_rows(&types, &mut rows)?;
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
        // mutation), keeping the key alongside for tree tables. Rows with
        // (MAX) columns encode inside the statement instead: their oversize
        // values spill to overflow chains first, which needs the page
        // context.
        let has_max = schema.columns.iter().any(|c| c.column_type.is_max());
        let mut encoded: Vec<StagedInsert> = Vec::with_capacity(rows.len());
        for values in &rows {
            validate_not_null(&schema, values)?;
            let row = if has_max {
                None
            } else {
                Some(encode_row(&schema, values)?)
            };
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
                for ((key, pre), mut values) in encoded.into_iter().zip(rows.into_iter()) {
                    let key = key.expect("tree row has a key");
                    let row = match pre {
                        Some(row) => row,
                        None => {
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            encode_row(&schema, &values)?
                        }
                    };
                    match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &key, &row)? {
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
                        &values,
                        &Locator::Key(key.clone()),
                    )?;
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id: tree.object_id,
                            identity: key,
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
                for ((_, pre), mut values) in encoded.into_iter().zip(rows.into_iter()) {
                    let row = match pre {
                        Some(row) => row,
                        None => {
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            encode_row(&schema, &values)?
                        }
                    };
                    // Heap rows locate by their home RID.
                    let rid = heap.insert(ctx, txn, &row)?;
                    index_insert_row(
                        ctx,
                        txn,
                        &indexes,
                        &schema,
                        &collations,
                        &values,
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
        let fetched = {
            let mut ctx = self.rel_ctx();
            tree.get(&mut ctx, &key)?
        };
        match fetched {
            Some(row) => {
                let mut rows = vec![decode_row(&schema, &row)?];
                let types = Self::projected_types(&schema, None);
                self.resolve_overflow_rows(&types, &mut rows)?;
                Ok(rows.pop())
            }
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
        let start = out.len();
        for row in raw {
            out.push(decode_projected(&schema, &row, projection)?);
        }
        let _ = ctx;
        let types = Self::projected_types(&schema, projection);
        self.resolve_overflow_rows(&types, &mut out[start..])?;
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
        let mut rows = raw
            .into_iter()
            .map(|row| decode_row(&schema, &row).map_err(StorageError::from))
            .collect::<Result<Vec<_>, _>>()?;
        let types = Self::projected_types(&schema, None);
        self.resolve_overflow_rows(&types, &mut rows)?;
        Ok(rows)
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
            let types = Self::projected_types(&schema, projection);
            self.resolve_overflow_rows(&types, &mut out)?;
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
        let types = Self::projected_types(&schema, projection);
        self.resolve_overflow_rows(&types, &mut out)?;
        Ok(out)
    }

    /// Deletes every row where `column = value`; returns the count. Targets
    /// are materialized before any mutation (Halloween avoidance).
    ///
    /// Test-only surface (no SQL path reaches it): it compares UNRESOLVED
    /// rows, so a chained (MAX) value never matches, and it bypasses version
    /// staging. Resolve and stage before wiring it to anything real.
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
        let _ = ctx;
        if schema.columns.iter().any(|c| c.column_type.is_max()) {
            let types = Self::projected_types(&schema, None);
            let mut ctx = self.rel_ctx();
            for (_, row) in out.iter_mut() {
                for (column_type, value) in types.iter().zip(row.iter_mut()) {
                    if let Datum::OverflowRef {
                        total_len,
                        first_page,
                    } = *value
                    {
                        let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                        let base = match column_type {
                            ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                            ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                            _ => ColumnType::VarBinary { max_len: u16::MAX },
                        };
                        *value = Datum::decode_var(&base, &bytes)?;
                    }
                }
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
        if schema.columns.iter().any(|c| c.column_type.is_max()) {
            let types = Self::projected_types(&schema, None);
            let mut ctx = self.rel_ctx();
            for (_, row, _) in out.iter_mut() {
                for (column_type, value) in types.iter().zip(row.iter_mut()) {
                    if let Datum::OverflowRef {
                        total_len,
                        first_page,
                    } = *value
                    {
                        let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                        let base = match column_type {
                            ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                            ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                            _ => ColumnType::VarBinary { max_len: u16::MAX },
                        };
                        *value = Datum::decode_var(&base, &bytes)?;
                    }
                }
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
        // Version priors come from the physical pre-images inside the
        // statement (raw bytes, overflow refs intact), not a re-encode.
        let publishing = self.version.publishing();
        let _ = &schema;
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
                for (loc, values) in &targets {
                    if let RowLocator::Key(key) = loc {
                        let prior = tree.delete(ctx, &mut OpMode::Txn(txn), key)?;
                        index_delete_row(
                            ctx,
                            txn,
                            &indexes,
                            &collations,
                            values,
                            &Locator::Key(key.clone()),
                        )?;
                        if publishing && let Some(prior) = prior {
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
                for (loc, values) in &targets {
                    if let RowLocator::Rid(rid) = loc {
                        let prior = if publishing {
                            heap.read_row(ctx, *rid)?
                        } else {
                            None
                        };
                        heap.delete(ctx, txn, *rid)?;
                        index_delete_row(
                            ctx,
                            txn,
                            &indexes,
                            &collations,
                            values,
                            &Locator::Rid(*rid),
                        )?;
                        if publishing && let Some(prior) = prior {
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
        let has_max = schema.columns.iter().any(|c| c.column_type.is_max());
        // (old values, old locator, new values, new locator) for index upkeep.
        let mut idx_ops: Vec<(Vec<Datum>, Locator, Vec<Datum>, Locator)> = Vec::new();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            // Partition into in-place (key unchanged) and re-key (key
            // changed). Version priors are captured INSIDE the statement
            // from the physical ops' returned pre-images — raw row bytes,
            // so a (MAX) image keeps its overflow reference instead of
            // re-inlining the whole value. (MAX) rows also encode inside,
            // after their oversize values spill.
            let mut in_place: Vec<StagedInPlace> = Vec::new();
            let mut rekey: Vec<StagedRekey> = Vec::new();
            for (loc, old_values, new_values) in updates {
                let RowLocator::Key(old_key) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected key locator for clustered table".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                let new_key = encode_key(&schema, &def.key_columns, &new_values)?;
                let (row, carried) = if has_max {
                    (None, Some(new_values.clone()))
                } else {
                    (Some(encode_row(&schema, &new_values)?), None)
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
                    in_place.push((old_key, row, carried));
                } else {
                    rekey.push((old_key, new_key, row, carried));
                }
            }
            let object_id = tree.object_id;
            self.rel_statement_scoped(scope, move |ctx, txn| {
                let encode_new = |ctx: &mut RelCtx<'_>,
                                  row: Option<Vec<u8>>,
                                  carried: Option<Vec<Datum>>|
                 -> Result<Vec<u8>, StorageError> {
                    match row {
                        Some(row) => Ok(row),
                        None => {
                            let mut values = carried.expect("carried values for a MAX row");
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            Ok(encode_row(&schema, &values)?)
                        }
                    }
                };
                // Delete all re-keyed olds first so a new key may reuse one.
                for (old_key, _, _, _) in &rekey {
                    let prior = tree.delete(ctx, &mut OpMode::Txn(txn), old_key)?;
                    if publishing && let Some(prior) = prior {
                        txn.pending_versions.push(PendingVersion {
                            object_id,
                            identity: old_key.clone(),
                            change: RowChange::Delete { prior },
                        });
                    }
                }
                for (_, new_key, row, carried) in rekey {
                    let row = encode_new(ctx, row, carried)?;
                    match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &new_key, &row)? {
                        TreeInsert::Inserted => {}
                        TreeInsert::DuplicateKey => {
                            return Err(StorageError::Constraint(
                                "duplicate primary key".to_string(),
                            ));
                        }
                    }
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id,
                            identity: new_key,
                            change: RowChange::Insert,
                        });
                    }
                }
                for (key, row, carried) in in_place {
                    let row = encode_new(ctx, row, carried)?;
                    let prior = tree.update(ctx, &mut OpMode::Txn(txn), &key, &row)?;
                    if publishing && let Some(prior) = prior {
                        txn.pending_versions.push(PendingVersion {
                            object_id,
                            identity: key,
                            change: RowChange::Update { prior },
                        });
                    }
                }
                apply_index_updates(ctx, txn, &indexes, &schema, &collations, &idx_ops)?;
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let mut encoded: Vec<StagedHeapUpdate> = Vec::with_capacity(count);
            for (loc, old_values, new_values) in updates {
                let RowLocator::Rid(rid) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected rid locator for heap".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                if has_max {
                    encoded.push((rid, None, Some(new_values.clone())));
                } else {
                    encoded.push((rid, Some(encode_row(&schema, &new_values)?), None));
                }
                if !indexes.is_empty() {
                    // Heap RIDs are stable across an update.
                    idx_ops.push((old_values, Locator::Rid(rid), new_values, Locator::Rid(rid)));
                }
            }
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for (rid, pre, carried) in encoded {
                    let row = match pre {
                        Some(row) => row,
                        None => {
                            let mut values = carried.expect("carried values for a MAX row");
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            encode_row(&schema, &values)?
                        }
                    };
                    // The pre-image (raw bytes, overflow refs intact) is the
                    // version prior; read before the in-place update.
                    let prior = if publishing {
                        heap.read_row(ctx, rid)?
                    } else {
                        None
                    };
                    heap.update(ctx, txn, rid, &row)?;
                    if publishing && let Some(prior) = prior {
                        txn.pending_versions.push(PendingVersion {
                            object_id: heap.object_id,
                            identity: rid_identity(rid),
                            change: RowChange::Update { prior },
                        });
                    }
                }
                apply_index_updates(ctx, txn, &indexes, &schema, &collations, &idx_ops)?;
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

    fn open_existing(path: PathBuf, stop_at: Option<u64>) -> Result<Self, StorageError> {
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
        let mut log_file = DirectFile::open_existing_unlocked(path.clone())?;
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
        let last_log_backup_lsn = active_sb.last_log_backup_lsn();
        let repl_slots = active_sb.repl_slots();
        let recovery_full = version.recovery_full;
        let mut storage = StorageFile {
            default_collation: header.default_collation(),
            file,
            wal,
            truncation_gate: LogTruncationGate::default(),
            max_slot_retain_bytes: u64::MAX,
            standby_att: std::collections::HashMap::new(),
            standby_search_floor: None,
            snapshot_next_seq_no: 0,
            last_log_backup_lsn,
            log_backup_in_progress: false,
            layout,
            superblock_a,
            superblock_b,
            active_superblock,
            allocator: PageAllocator::new(layout.data_size),
            rel,
            replay_cache: scan.records,
            version,
        };
        // Re-establish the FULL-model log-backup hold so a checkpoint cannot
        // reclaim un-backed-up log. The floor is >= the persisted wal_head
        // (checkpoints were already clamped to it), so it never moves the head
        // backward.
        // A standby skips the hold: its seeded marker is the PRIMARY's log
        // chain (frozen at the backup point), and holding there would cap
        // every restartpoint at the seed forever, running the ring full. The
        // hold re-arms at promotion (a full reopen as a non-standby).
        if recovery_full && !active_sb.is_standby() {
            storage.register_log_backup_hold(last_log_backup_lsn);
        }
        // Re-seed the replication slots so their truncation hold survives the
        // restart (the persisted LSN is <= the live one — conservative, holds
        // more log, safe: redo is idempotent).
        for (id, lsn) in repl_slots {
            storage.truncation_gate.repl_slots.insert(id, lsn);
        }
        // A standby re-derives its restartpoint floors from the same records
        // recovery is about to scan: the active-transaction table (unresolved
        // BEGINs) and the first search record the seed snapshot does not cover.
        if active_sb.is_standby() {
            let descriptors = storage.read_snapshot_descriptors()?;
            storage.snapshot_next_seq_no = live_descriptor_slot(&descriptors)
                .and_then(|slot| descriptors[slot])
                .map(|desc| desc.next_seq_no)
                .unwrap_or(0);
            let rel_records: Vec<(u64, RelRecord)> = storage
                .replay_cache
                .iter()
                .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
                .map(|record| Ok((record.logical_ts, RelRecord::decode(&record.payload)?)))
                .collect::<Result<_, StorageError>>()?;
            for (lsn, record) in &rel_records {
                storage.standby_track_rel_record(*lsn, record);
            }
            storage.standby_search_floor = storage
                .replay_cache
                .iter()
                .find(|record| {
                    record.entry_type != WAL_ENTRY_TYPE_REL
                        && record.seq_no >= storage.snapshot_next_seq_no
                })
                .map(|record| record.logical_ts);
        }
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
        // reload, undo of losers with compensation logging. A standby (one that
        // has applied a live WAL stream) recovers REDO-ONLY — repeat history but
        // do not undo in-flight transactions, which the primary will commit and
        // whose continuation resumes above this standby's applied point.
        let redo_only = storage.active_sb().is_standby();
        // A standby is read-only until promotion: it appends nothing to its own
        // WAL (only the primary's log, via apply_wal_stream). Set before
        // recover_rel — which for a standby is redo-only and never appends, so
        // this blocks only later local writes.
        storage.wal.set_read_only(redo_only);
        storage.recover_rel(stop_at, redo_only)?;
        Ok(storage)
    }

    fn create_new(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
    ) -> Result<Self, StorageError> {
        let layout = compute_layout(opts.clone(), wal_min_bytes, wal_max_bytes)?;
        Self::create_from_layout(path, layout, opts.default_collation)
    }

    /// Creates a fresh file with an explicit layout. The restore path
    /// reconstructs the source's exact region sizes from the backup header
    /// rather than recomputing them from ratios, so it lays regions back
    /// byte-for-byte. Mirrors [`Self::create_new`] from
    /// `validate_allocator_region` onward.
    fn create_from_layout(
        path: PathBuf,
        layout: StorageLayout,
        default_collation: Option<String>,
    ) -> Result<Self, StorageError> {
        validate_allocator_region(&layout)?;
        let mut header = FileHeader::default();
        // Stamp the database's default collation into the file. Every character
        // column declared without an explicit COLLATE is keyed under it, so it
        // belongs to the data, not to whatever the config says at the next boot.
        if let Some(name) = default_collation.as_deref() {
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

        let log_file = DirectFile::open_existing_unlocked(path.clone())?;
        let wal = WalWriter::open(log_file, layout.wal_offset, layout.wal_size, 0, 0)?;

        Ok(StorageFile {
            default_collation: header.default_collation(),
            file,
            wal,
            truncation_gate: LogTruncationGate::default(),
            max_slot_retain_bytes: u64::MAX,
            standby_att: std::collections::HashMap::new(),
            standby_search_floor: None,
            snapshot_next_seq_no: 0,
            last_log_backup_lsn: 0,
            log_backup_in_progress: false,
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
    fn recover_rel(&mut self, stop_at: Option<u64>, redo_only: bool) -> Result<(), StorageError> {
        let records = self.rel_records()?;
        if records.is_empty() && self.rel.catalog_root.is_none() {
            return Ok(());
        }

        let outcome = {
            let mut ctx = self.rel_ctx();
            rel_recovery::analyze_and_redo(&mut ctx, &records, stop_at)?
        };
        if let Some(root) = outcome.catalog_root {
            self.rel.catalog_root = Some(root);
        }
        self.rel.next_txn_id = outcome.max_txn_id + 1;

        self.reload_catalog()?;
        // A standby repeats history but never undoes: an in-flight transaction at
        // the applied tail is the primary's, to be committed (and shipped onward)
        // or resolved at promotion — undoing it here would drop committed data
        // that the streaming protocol never re-ships.
        if !redo_only && !outcome.losers.is_empty() {
            let roots = self.rel.tree_roots();
            let mut ctx = self.rel_ctx();
            rel_recovery::undo_losers(&mut ctx, &records, &outcome.losers, &roots)?;
            self.reload_catalog()?;
        }
        // Object ids are shared by tables, their secondary indexes, and logins
        // (principals draw from the same counter), so the next id must clear all
        // three — an index or a login can outrank every table.
        self.rel.next_object_id = self
            .rel
            .tables
            .values()
            .flat_map(|def| {
                std::iter::once(def.object_id)
                    .chain(def.indexes.iter().map(|index| index.object_id))
            })
            .chain(self.rel.principals.values().map(|def| def.object_id))
            .chain(
                self.rel
                    .database_principals
                    .values()
                    .map(|def| def.object_id),
            )
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
            self.rel.principals.clear();
            self.rel.database_principals.clear();
            return Ok(());
        };
        let defs = {
            let mut ctx = self.rel_ctx();
            catalog::load_tables(&mut ctx, root)?
        };
        self.rel.tables.clear();
        self.rel.principals.clear();
        self.rel.database_principals.clear();
        for def in defs {
            if def.is_login() {
                // Logins live in their own map, keyed case-insensitively — never
                // in the object namespace.
                self.rel
                    .principals
                    .insert(def.name.to_ascii_lowercase(), def);
            } else if def.is_database_principal() {
                // Users and roles: a second map, also out of the object namespace.
                self.rel
                    .database_principals
                    .insert(def.name.to_ascii_lowercase(), def);
            } else {
                self.rel.tables.insert(def.name.clone(), def);
            }
        }
        Ok(())
    }

    /// Spills every (MAX) value above the inline threshold to an overflow
    /// chain, replacing the datum with a reference. Runs inside statement
    /// closures, before the row is encoded; chain pages are WAL-imaged, so
    /// they are crash-durable with the statement (and leak if it fails —
    /// the drop-table posture).
    fn spill_max_values(
        ctx: &mut RelCtx<'_>,
        schema: &Schema,
        values: &mut [Datum],
    ) -> Result<(), StorageError> {
        for (column, value) in schema.columns.iter().zip(values.iter_mut()) {
            if !column.column_type.is_max() || value.is_null() {
                continue;
            }
            let bytes = match value {
                Datum::VarChar(_) | Datum::NVarChar(_) | Datum::VarBinary(_) => value.encode_var(),
                _ => continue,
            };
            if bytes.len() <= OVERFLOW_INLINE_MAX {
                continue;
            }
            let first_page = overflow::write_chain(ctx, &bytes)?;
            *value = Datum::OverflowRef {
                total_len: bytes.len() as u64,
                first_page,
            };
        }
        Ok(())
    }

    /// Resolves overflow references in decoded rows back to their values.
    /// `types` must align with the rows' columns (the projection's types for
    /// projected reads).
    fn resolve_overflow_rows(
        &mut self,
        types: &[ColumnType],
        rows: &mut [Vec<Datum>],
    ) -> Result<(), StorageError> {
        if !types.iter().any(ColumnType::is_max) {
            return Ok(());
        }
        let mut ctx = self.rel_ctx();
        for row in rows.iter_mut() {
            for (column_type, value) in types.iter().zip(row.iter_mut()) {
                if let Datum::OverflowRef {
                    total_len,
                    first_page,
                } = *value
                {
                    let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                    let base = match column_type {
                        ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                        ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                        ColumnType::VarBinaryMax => ColumnType::VarBinary { max_len: u16::MAX },
                        other => {
                            return Err(StorageError::InvalidFile(format!(
                                "overflow reference under non-MAX column type {}",
                                other.name()
                            )));
                        }
                    };
                    *value = Datum::decode_var(&base, &bytes)?;
                }
            }
        }
        Ok(())
    }

    /// The column types a projection selects (`None` = every column).
    fn projected_types(schema: &Schema, projection: Option<&[usize]>) -> Vec<ColumnType> {
        match projection {
            None => schema.columns.iter().map(|c| c.column_type).collect(),
            Some(projection) => projection
                .iter()
                .map(|&i| schema.columns[i].column_type)
                .collect(),
        }
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
        // The floor is the min over every truncation hold, clamped to the tail:
        // the oldest open transaction's BEGIN (so its undo survives a crash), and
        // any gate hold (an in-progress backup's redo_start_lsn, later also log
        // backup and replication slots). A checkpoint never truncates past this.
        let mut floor = self.wal.tail();
        if let Some(oldest_txn) = self.rel.active_txn_begins.values().min().copied() {
            floor = floor.min(oldest_txn);
        }
        if let Some(hold) = self.truncation_gate.min_hold() {
            floor = floor.min(hold);
        }
        floor
    }

    /// Registers an in-progress backup's `redo_start_lsn` as a truncation hold,
    /// so a concurrent checkpoint cannot reclaim log the backup still has to ship.
    /// Paired with [`Self::release_backup_hold`].
    fn register_backup_hold(&mut self, redo_start_lsn: u64) {
        self.truncation_gate.backup = Some(redo_start_lsn);
    }

    /// Releases the backup truncation hold (the backup finished or failed).
    fn release_backup_hold(&mut self) {
        self.truncation_gate.backup = None;
    }

    /// Pins the FULL-model log-backup floor at `last_log_backup_lsn` so a
    /// checkpoint cannot reclaim log a `BACKUP LOG` still has to ship. Set when
    /// FULL is enabled and re-set (advanced) after each `BACKUP LOG`.
    fn register_log_backup_hold(&mut self, last_log_backup_lsn: u64) {
        self.truncation_gate.log_backup = Some(last_log_backup_lsn);
    }

    /// Drops the log-backup floor (recovery model set back to SIMPLE): log is
    /// reclaimable as soon as it is checkpointed.
    fn release_log_backup_hold(&mut self) {
        self.truncation_gate.log_backup = None;
    }

    /// Maintains the standby's active-transaction table as shipped records are
    /// applied — the SAME begin/resolve rules recovery's analysis uses
    /// (`analyze_and_redo`): TXN_BEGIN opens at its own LSN, TXN_COMMIT and
    /// TXN_END resolve. A page op for a transaction whose BEGIN was not seen
    /// cannot arise (a chunk never precedes a transaction's BEGIN — chunks
    /// apply in order and BEGIN is its first record); the defensive
    /// `or_insert` still clamps the floor at that record if it ever did.
    fn standby_track_rel_record(&mut self, lsn: u64, record: &RelRecord) {
        use crate::wal::records::{REL_KIND_TXN_BEGIN, REL_KIND_TXN_COMMIT, REL_KIND_TXN_END};
        match record.kind {
            REL_KIND_TXN_BEGIN => {
                self.standby_att.insert(record.txn_id, lsn);
            }
            REL_KIND_TXN_COMMIT | REL_KIND_TXN_END => {
                self.standby_att.remove(&record.txn_id);
            }
            _ => {
                if record.txn_id != 0 {
                    self.standby_att.entry(record.txn_id).or_insert(lsn);
                }
            }
        }
    }

    /// Drops any replication slot lagging the WAL tail by more than
    /// `max_slot_retain_bytes` — it no longer pins the truncation floor, so the
    /// ring can advance past it (the standby must reseed). Run at the start of a
    /// checkpoint, before the floor is computed. A no-op under the default
    /// unlimited retention.
    fn reap_stale_slots(&mut self) {
        if self.max_slot_retain_bytes == u64::MAX {
            return;
        }
        let tail = self.wal.tail();
        let max = self.max_slot_retain_bytes;
        self.truncation_gate
            .repl_slots
            .retain(|_, lsn| tail.saturating_sub(*lsn) <= max);
    }

    /// Registers (or resets) a replication slot at `lsn`. `lsn` must be `>=` the
    /// current WAL head (a standby's received LSN is always within the retained
    /// window — the primary cannot have already truncated it); a below-head slot
    /// would drive `set_head` below the current head, which it forbids. Checked
    /// here, under the storage lock, so a checkpoint cannot truncate between
    /// the check and the insert. The table is bounded by [`MAX_REPL_SLOTS`]
    /// (the superblock persists at most that many; a silent in-memory overflow
    /// would lose a slot's hold across a restart).
    fn try_register_repl_slot(&mut self, id: u32, lsn: u64) -> Result<(), StorageError> {
        let head = self.wal.head();
        if lsn < head {
            return Err(StorageError::InvalidConfig(format!(
                "replication slot {id} at LSN {lsn} is behind the WAL head ({head}): \
                 the log the standby needs is already truncated; reseed the standby \
                 from a fresh backup"
            )));
        }
        if self.truncation_gate.repl_slots.len() >= crate::storage_layout::MAX_REPL_SLOTS
            && !self.truncation_gate.repl_slots.contains_key(&id)
        {
            return Err(StorageError::InvalidConfig(format!(
                "replication slot table is full ({} slots): drop a stale slot before \
                 registering slot {id}",
                crate::storage_layout::MAX_REPL_SLOTS
            )));
        }
        // The LSN comes off the wire (Hello.last_received_lsn) and becomes the
        // truncation floor — which a checkpoint persists as the WAL head, the
        // very position the next restart scans from. A mid-entry floor would
        // make that scan read garbage and silently truncate every commit since
        // the checkpoint. Only a verifiable ENTRY BOUNDARY may be registered;
        // an honest standby's persisted tail always is one.
        if !self.is_wal_entry_boundary(lsn)? {
            return Err(StorageError::InvalidConfig(format!(
                "replication slot {id} at LSN {lsn} is not on a WAL entry boundary: \
                 the standby's resume state is corrupt or forged; reseed the standby \
                 from a fresh backup"
            )));
        }
        self.truncation_gate.repl_slots.insert(id, lsn);
        Ok(())
    }

    /// Whether `lsn` sits on a WAL entry boundary of THIS log: the tail
    /// itself, a position carrying a CRC-valid entry header that self-identifies
    /// (`logical_ts == lsn`), or a ring-wrap gap start whose next lap opens
    /// with a self-identifying valid entry. A forger cannot fabricate any of
    /// these without controlling the actual log contents.
    fn is_wal_entry_boundary(&mut self, lsn: u64) -> Result<bool, StorageError> {
        let tail = self.wal.tail();
        if lsn == tail {
            return Ok(true);
        }
        if lsn > tail {
            return Ok(false);
        }
        let wal_offset = self.layout.wal_offset;
        let wal_size = self.layout.wal_size;
        let check_entry_at =
            |file: &mut crate::direct_io::DirectFile, pos: u64| -> Result<bool, StorageError> {
                let ring_pos = pos % wal_size;
                if wal_size - ring_pos < WAL_ENTRY_HEADER_SIZE as u64 {
                    return Ok(false);
                }
                let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
                file.read_exact_at(wal_offset + ring_pos, &mut header_bytes)?;
                if header_bytes.iter().all(|b| *b == 0) {
                    return Ok(false);
                }
                let header = WalEntryHeader::from_le_bytes(&header_bytes);
                Ok(header.verify_header_crc() && header.logical_ts == pos)
            };
        let ring_pos = lsn % wal_size;
        let bytes_to_lap_end = wal_size - ring_pos;
        if bytes_to_lap_end >= WAL_ENTRY_HEADER_SIZE as u64 {
            let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
            self.wal
                .file_mut()
                .read_exact_at(wal_offset + ring_pos, &mut header_bytes)?;
            if !header_bytes.iter().all(|b| *b == 0) {
                let header = WalEntryHeader::from_le_bytes(&header_bytes);
                return Ok(header.verify_header_crc() && header.logical_ts == lsn);
            }
        }
        // Zeros (or no room for a header): a genuine boundary here is a wrap-gap
        // start, provable by the next lap opening with a real entry below the
        // tail. Mid-entry zeros cannot fake that — the jump target's entry must
        // self-identify at exactly that position.
        let jump = lsn + bytes_to_lap_end;
        if jump == tail {
            return Ok(true);
        }
        if jump > tail {
            return Ok(false);
        }
        check_entry_at(self.wal.file_mut(), jump)
    }

    /// Advances a slot forward to `lsn` — a slot never moves backward (a
    /// standby's received watermark only grows), never past the WAL tail (an
    /// absurd acked LSN must not unpin log that exists), and a missing slot is
    /// not created (an ack arriving after a reap must not resurrect the slot
    /// without the registration checks).
    fn advance_repl_slot(&mut self, id: u32, lsn: u64) {
        let lsn = lsn.min(self.wal.tail());
        if let Some(held) = self.truncation_gate.repl_slots.get_mut(&id) {
            *held = (*held).max(lsn);
        }
    }

    #[cfg(test)]
    fn drop_repl_slot(&mut self, id: u32) {
        self.truncation_gate.repl_slots.remove(&id);
    }

    #[cfg(test)]
    fn repl_slot_lsn(&self, id: u32) -> Option<u64> {
        self.truncation_gate.repl_slots.get(&id).copied()
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
            // Spill scratch shares the data region and the allocator bitmap
            // with REPLICATED extents. On a standby, an extent that is free at
            // the applied LSN may already be allocated on the primary (its
            // ALLOC record still in flight) — a spool there would be clobbered
            // by the arriving redo, and the spool's raw writes would clobber
            // replicated pages. Refuse until the readable-standby slice gives
            // scratch its own storage.
            if self.active_sb().is_standby() {
                return Err(StorageError::InvalidConfig(
                    "a query on this replication standby needed spill scratch, which shares \
                     the data region the primary's stream writes into; re-run with more \
                     memory or run it on the primary (readable-standby spill arrives with \
                     snapshot standby reads)"
                        .to_string(),
                ));
            }
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

    // --- Backup / restore (Stage 17) ---

    /// Captures a backup plan under the storage lock, rejecting a second
    /// concurrent backup, and registers the WAL truncation hold LAST (after all
    /// fallible work) so a failure here leaves no stale hold. On success the
    /// caller arms a [`BackupHoldGuard`] to release the hold on every exit.
    fn begin_backup(
        &mut self,
        checksum: bool,
        copy_only: bool,
    ) -> Result<BackupPlan, StorageError> {
        self.ensure_rel_usable()?;
        // Single-flight: the gate has one backup hold slot. A second concurrent
        // backup would overwrite the first's hold (and its release would clear
        // the survivor's), leaving a backup with a wrong or absent truncation
        // floor — a silently truncated restore. Reject it.
        if self.truncation_gate.backup.is_some() {
            return Err(StorageError::BackupInProgress);
        }
        let redo_start = self.wal.head();

        // The active superblock is the checkpoint whose head is redo_start, so
        // its roots are the checkpoint-consistent state recovery redoes onto.
        let (metadata_root, last_committed_seq, db_options) = {
            let active = match self.active_superblock {
                ActiveSuperblock::A => &self.superblock_a,
                ActiveSuperblock::B => &self.superblock_b,
            };
            debug_assert_eq!(
                redo_start, active.wal_head,
                "redo_start must equal the active checkpoint's wal_head"
            );
            (
                active.metadata_root,
                active.last_committed_seq,
                active.db_options(),
            )
        };

        let mut runs = self.allocator.allocated_runs();
        // The search-snapshot extent is a durable allocation (so allocated_runs
        // includes it) but holds raw, non-page-formatted bytes. This slice does
        // not preserve the search snapshot (Stage 19 retires it), so drop its
        // extent from the backup rather than copy pages that would fail checksum
        // verification and dangle unreferenced on restore.
        if let Some(desc) = self.load_active_snapshot_descriptor()? {
            let (start, pages) = self.descriptor_page_range(&desc)?;
            runs = subtract_run(runs, start, pages);
        }

        // Register the hold LAST, after every fallible step above: a failure
        // here must leave no stale hold behind (a leaked hold freezes WAL
        // truncation for the life of the process). We are still under the same
        // lock, so redo_start is unchanged.
        self.register_backup_hold(redo_start);
        Ok(BackupPlan {
            layout: self.layout,
            default_collation: self.default_collation.clone(),
            redo_start,
            metadata_root,
            last_committed_seq,
            db_options,
            runs,
            checksum,
            copy_only,
            finished_at_millis: now_millis(),
        })
    }

    /// Reads `count` data pages starting at `start_page` into `out`, verifying
    /// each page's checksum unless `checksum` is off (an all-zero page is an
    /// unwritten allocation and always passes).
    fn read_pages_for_backup(
        &mut self,
        start_page: u64,
        count: u64,
        out: &mut [u8],
        checksum: bool,
    ) -> Result<(), StorageError> {
        debug_assert_eq!(out.len(), count as usize * PAGE_SIZE);
        for i in 0..count as usize {
            let page = start_page + i as u64;
            let slot = &mut out[i * PAGE_SIZE..(i + 1) * PAGE_SIZE];
            self.spill_read_page(page, slot)?;
            if checksum
                && !crate::relstore::page::is_zero_page(slot)
                && !crate::relstore::page::verify_checksum(slot)
                && self.page_is_live_regular(page)?
            {
                return Err(StorageError::InvalidFile(format!(
                    "backup aborted: data page {page} failed checksum (corrupt source page)"
                )));
            }
        }
        Ok(())
    }

    /// True iff `page` is still a live, page-formatted data page — so a checksum
    /// failure genuinely means source corruption. A page that has been freed
    /// since the backup began, or reused as the raw (non-page-formatted)
    /// search-snapshot extent by a between-chunk checkpoint, legitimately fails
    /// page-checksum verification and is NOT corrupt: on restore its stale
    /// image is irrelevant because redo frees or overwrites it.
    fn page_is_live_regular(&mut self, page: u64) -> Result<bool, StorageError> {
        if !self.allocator.is_allocated(page) {
            return Ok(false); // freed since the backup began
        }
        if let Some(desc) = self.load_active_snapshot_descriptor()? {
            let (start, pages) = self.descriptor_page_range(&desc)?;
            if page >= start && page < start + pages {
                return Ok(false); // reused as the raw snapshot extent
            }
        }
        Ok(true)
    }

    /// Forces the log durable, captures `backup_end = tail`, and returns the raw
    /// ring bytes for `[redo_start, backup_end)`. The physical copy handles the
    /// ring wrap; the range never exceeds one ring lap (the truncation hold
    /// keeps `tail - head <= wal_size`).
    fn ship_backup_log(&mut self, redo_start: u64) -> Result<(u64, Vec<u8>), StorageError> {
        self.wal.sync_all()?;
        let backup_end = self.wal.tail();
        debug_assert!(backup_end >= redo_start);
        let len = backup_end - redo_start;
        // Cap the shipped range so the restored ring leaves the reserve free:
        // restore's undo pass appends its own compensation records into that
        // reserve. Because head is pinned at redo_start for the whole backup,
        // a rollback storm can push the tail into the reserve (forward appends
        // stop short of it, but undo CLRs use it), which would otherwise yield
        // a backup that fails only at restore time. Fail cleanly here instead.
        let max_len = self.layout.wal_size.saturating_sub(self.wal.reserve());
        if len > max_len {
            return Err(StorageError::WalFull(
                "backup log range fills the WAL reserve (ring under pressure); \
                 checkpoint and retry the backup"
                    .to_string(),
            ));
        }
        let out = self.read_ring_range(redo_start, backup_end)?;
        Ok((backup_end, out))
    }

    /// Reads the raw WAL ring bytes for the logical range `[start, end)`,
    /// handling the physical wrap. The caller must have synced the log to at
    /// least `end` first (`DirectFile` bypasses the page cache).
    fn read_ring_range(&mut self, start: u64, end: u64) -> Result<Vec<u8>, StorageError> {
        debug_assert!(end >= start);
        let len = end - start;
        let mut out = vec![0u8; len as usize];
        if len > 0 {
            let wal_offset = self.layout.wal_offset;
            let wal_size = self.layout.wal_size;
            let start_phys = start % wal_size;
            let first = ((wal_size - start_phys).min(len)) as usize;
            self.wal
                .file_mut()
                .read_exact_at(wal_offset + start_phys, &mut out[..first])?;
            if first < out.len() {
                self.wal
                    .file_mut()
                    .read_exact_at(wal_offset, &mut out[first..])?;
            }
        }
        Ok(out)
    }

    /// Walks WAL-entry headers from `from` (which must itself be an entry
    /// boundary within the valid log) and returns the furthest ENTRY BOUNDARY
    /// reachable within `max_bytes`, never past `to_cap`. A ring-wrap gap
    /// (zero-fill to the lap end) is crossed together with the first entry of
    /// the next lap, so a returned boundary is never inside or at the start of
    /// a gap a chunk does not also bridge. If the very first step (one entry,
    /// or gap + one entry) exceeds `max_bytes`, it is returned anyway — the
    /// chunk must make progress, and the caller bounds the frame size.
    fn wal_chunk_end(
        &mut self,
        from: u64,
        to_cap: u64,
        max_bytes: u64,
    ) -> Result<u64, StorageError> {
        let wal_offset = self.layout.wal_offset;
        let wal_size = self.layout.wal_size;
        let mut cursor = from;
        let mut last_boundary = from;
        while cursor < to_cap {
            let ring_pos = cursor % wal_size;
            let bytes_to_lap_end = wal_size - ring_pos;
            // A wrap gap (too small for a header, or zero-filled): cross it as
            // part of the next entry's step.
            let gap_jump = if bytes_to_lap_end < WAL_ENTRY_HEADER_SIZE as u64 {
                true
            } else {
                let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
                self.wal
                    .file_mut()
                    .read_exact_at(wal_offset + ring_pos, &mut header_bytes)?;
                if header_bytes.iter().all(|b| *b == 0) {
                    true
                } else {
                    let header = WalEntryHeader::from_le_bytes(&header_bytes);
                    if !header.verify_header_crc() {
                        return Err(StorageError::InvalidFile(format!(
                            "WAL entry header at LSN {cursor} fails its CRC inside the \
                             durable range [{from}, {to_cap}): cannot ship the log"
                        )));
                    }
                    let entry_len = wal_entry_padded_len(header.payload_len as usize) as u64;
                    let next = cursor + entry_len;
                    if next > to_cap {
                        // The durable cap always sits on an entry boundary; an
                        // entry crossing it means `from` was not a boundary.
                        return Err(StorageError::InvalidFile(format!(
                            "WAL entry at LSN {cursor} extends past the durable watermark \
                             {to_cap}: misaligned ship position"
                        )));
                    }
                    if next - from > max_bytes && last_boundary > from {
                        return Ok(last_boundary);
                    }
                    cursor = next;
                    last_boundary = next;
                    continue;
                }
            };
            if gap_jump {
                // Jump to the lap end; the loop then takes the next lap's first
                // entry before this jump can become a boundary.
                let next = cursor + bytes_to_lap_end;
                if next >= to_cap {
                    // Nothing but gap remains below the cap.
                    return Ok(last_boundary);
                }
                if next - from > max_bytes && last_boundary > from {
                    return Ok(last_boundary);
                }
                cursor = next;
                // NOT a boundary: fall through to read the next lap's entry.
            }
        }
        Ok(last_boundary)
    }

    /// Phase 1 of `BACKUP LOG`: under the storage lock, capture the log range
    /// `[last_log_backup_lsn, tail)` and its bytes plus the archive header. Does
    /// NOT advance the marker or hold, so the old marker keeps pinning the range
    /// while the caller writes the archive with the lock released. Returns
    /// `(header, start, end, log_bytes)`. Requires the FULL recovery model.
    fn begin_log_backup(
        &mut self,
        checksum: bool,
        copy_only: bool,
    ) -> Result<(crate::backup::BackupHeader, u64, u64, Vec<u8>), StorageError> {
        self.ensure_rel_usable()?;
        if !self.version.recovery_full {
            return Err(StorageError::InvalidConfig(
                "BACKUP LOG requires the FULL recovery model".to_string(),
            ));
        }
        if self.active_sb().is_standby() {
            return Err(StorageError::InvalidConfig(
                "BACKUP LOG is not supported on a replication standby (its log chain \
                 belongs to the primary); run log backups there"
                    .to_string(),
            ));
        }
        if self.log_backup_in_progress {
            return Err(StorageError::BackupInProgress);
        }
        self.wal.sync_all()?;
        let start = self.last_log_backup_lsn;
        let end = self.wal.tail();
        debug_assert!(end >= start);
        let log = self.read_ring_range(start, end)?;
        // Reserve the single-flight slot only after the fallible reads above, so
        // a failure never strands the guard.
        self.log_backup_in_progress = true;
        // A log-only archive: no page/region data — the header carries the range
        // start in `redo_start_lsn` and the `log_backup` flag; the end is derived
        // from the LogChunk length on read.
        let header = crate::backup::BackupHeader {
            format_version: crate::backup::FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            total_size: 0,
            wal_size: self.layout.wal_size,
            data_size: 0,
            metadata_size: 0,
            allocator_size: 0,
            snapshot_size: 0,
            reserved_size: 0,
            default_collation: None,
            redo_start_lsn: start,
            metadata_root: 0,
            last_committed_seq: 0,
            db_options: self.version.options_byte(),
            finished_at_millis: now_millis(),
            flags: crate::backup::BackupFlags {
                checksum,
                copy_only,
                log_backup: true,
            },
        };
        Ok((header, start, end, log))
    }

    /// Phase 3 of `BACKUP LOG`: the archive at `end` is durable, so durably
    /// advance the persisted marker then the in-memory marker and hold, letting
    /// the ring reclaim `[start, end)`.
    ///
    /// ORPHANS the backup (no marker/hold change) if the FULL-model state
    /// changed during the unlocked archive write — a concurrent `ALTER DATABASE
    /// SET RECOVERY SIMPLE` released the hold (and a checkpoint then advanced
    /// the head), or a re-enable moved the marker. Re-arming the hold at `end`
    /// in those cases could sit it below the advanced head, which `set_head`
    /// forbids. The single-flight guard is released by `LogBackupGuard` on every
    /// exit path, not here.
    fn finish_log_backup(&mut self, start: u64, end: u64) -> Result<(), StorageError> {
        if !self.version.recovery_full || self.last_log_backup_lsn != start {
            return Ok(());
        }
        self.persist_last_log_backup_lsn(end)?;
        self.last_log_backup_lsn = end;
        self.register_log_backup_hold(end);
        Ok(())
    }

    /// Releases the `BACKUP LOG` single-flight guard (idempotent). Called by
    /// [`LogBackupGuard`] on every exit — error, panic, or success.
    fn cancel_log_backup(&mut self) {
        self.log_backup_in_progress = false;
    }

    /// Lays a run of page images back at their page numbers (restore).
    fn restore_pages(
        &mut self,
        start_page: u64,
        count: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        debug_assert_eq!(bytes.len(), count as usize * PAGE_SIZE);
        for i in 0..count as usize {
            let page = start_page + i as u64;
            self.spill_write_page(page, &bytes[i * PAGE_SIZE..(i + 1) * PAGE_SIZE])?;
        }
        Ok(())
    }

    /// Rebuilds and persists the allocation bitmap from the shipped run list
    /// (restore).
    fn restore_allocator_bitmap(&mut self, runs: &[(u64, u64)]) -> Result<(), StorageError> {
        let mut allocator = PageAllocator::new(self.layout.data_size);
        for &(start, count) in runs {
            allocator.mark_used(start, count);
        }
        let bitmap = allocator.persistable_bitmap();
        if bitmap.len() as u64 > self.layout.allocator_size {
            return Err(StorageError::InvalidFile(
                "restored allocator bitmap exceeds allocator region".to_string(),
            ));
        }
        self.file
            .write_all_at(self.layout.allocator_offset, &bitmap)?;
        self.allocator = allocator;
        Ok(())
    }

    /// Writes the shipped WAL bytes into the ring at their physical positions,
    /// handling the ring wrap (restore). Returns `start_lsn + bytes.len()`, the
    /// restored `backup_end`.
    fn seed_ring(&mut self, start_lsn: u64, bytes: &[u8]) -> Result<u64, StorageError> {
        let wal_size = self.layout.wal_size;
        if bytes.len() as u64 > wal_size {
            return Err(StorageError::WalFull(
                "restored log exceeds the WAL ring size".to_string(),
            ));
        }
        if !bytes.is_empty() {
            let wal_offset = self.layout.wal_offset;
            let start_phys = start_lsn % wal_size;
            let first = ((wal_size - start_phys) as usize).min(bytes.len());
            self.file
                .write_all_at(wal_offset + start_phys, &bytes[..first])?;
            if first < bytes.len() {
                self.file.write_all_at(wal_offset, &bytes[first..])?;
            }
        }
        Ok(start_lsn + bytes.len() as u64)
    }

    /// The active superblock (the authoritative in-memory copy).
    fn active_sb(&self) -> &Superblock {
        match self.active_superblock {
            ActiveSuperblock::A => &self.superblock_a,
            ActiveSuperblock::B => &self.superblock_b,
        }
    }

    /// The live standby apply (see [`Storage::apply_wal_stream`]). Runs under the
    /// storage lock held by the caller.
    fn apply_wal_stream_locked(&mut self, from_lsn: u64, bytes: &[u8]) -> Result<(), StorageError> {
        if bytes.is_empty() {
            return Ok(());
        }
        // The standby's applied tail lives in the persisted superblock — the live
        // WalWriter tail does not advance (the standby never appends).
        let current_tail = self.active_sb().wal_tail;
        if from_lsn > current_tail {
            return Err(StorageError::InvalidFile(format!(
                "WAL stream gap (4305): range begins at LSN {from_lsn} but the standby has applied to {current_tail}"
            )));
        }
        let new_end = from_lsn + bytes.len() as u64;
        let advanced = current_tail.max(new_end);
        let head = self.wal.head();
        let max_range = self.layout.wal_size.saturating_sub(self.wal.reserve());
        if advanced.saturating_sub(head) > max_range {
            return Err(StorageError::WalFull(
                "the applied WAL stream exceeds the standby ring's usable size; the standby \
                 must checkpoint to reclaim ring space (not yet automatic)"
                    .to_string(),
            ));
        }

        // 0. Persist the standby flag BEFORE seeding any bytes (on the first
        //    apply). Otherwise a crash between the seed's fsync and the tail
        //    commit would reopen as a normal database and ARIES-undo the shipped
        //    in-flight records — the very divergence this mode prevents. Once the
        //    flag is durable, a crash anywhere reopens redo-only.
        if !self.active_sb().is_standby() {
            self.commit_superblock(|sb| sb.set_standby(true))?;
            self.wal.set_read_only(true);
        }

        // 1. Place the bytes in the ring and fsync BEFORE recording the advanced
        //    tail. A crash after this re-scans and re-redoes them (idempotent);
        //    recording an un-fsynced tail would trust torn bytes on reopen.
        self.seed_ring(from_lsn, bytes)?;
        self.file.sync_data()?;
        // Advance the in-memory WAL tail to match the seeded ring, so `tail()` /
        // `flushed_lsn()` (read by a backup, and by continuity above) reflect
        // reality — a standby never appends, so nothing else would move them.
        self.wal.resync_tail(advanced)?;

        // 2. Decode only the newly seeded range: a scan starting at `from_lsn`
        //    self-terminates where the stale bytes past `new_end` begin (their
        //    logical_ts no longer equals the cursor).
        let scan = scan_ring(
            self.wal.file_mut(),
            self.layout.wal_offset,
            self.layout.wal_size,
            from_lsn,
            from_lsn,
        )?;
        // The shipped range must decode END TO END. A short scan means the
        // range was cut mid-entry (a misaligned sender) or carries recycled
        // bytes from another ring lap (a lapsed slot): advancing the tail over
        // undecoded bytes would silently skip their redo forever — the page-LSN
        // gate then masks the loss on every future record. Fail the apply
        // instead; the connection drops and the operator sees it.
        if scan.tail < advanced {
            return Err(StorageError::InvalidFile(format!(
                "shipped WAL range [{from_lsn}, {new_end}) only decodes to {}: the range is \
                 cut mid-entry or holds recycled bytes; refusing to apply it",
                scan.tail
            )));
        }
        let records: Vec<(u64, RelRecord)> = scan
            .records
            .iter()
            .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
            .map(|record| Ok((record.logical_ts, RelRecord::decode(&record.payload)?)))
            .collect::<Result<_, StorageError>>()?;

        // Replay allocation state: the live pool redo below writes page images,
        // but the ALLOCATOR is only rebuilt at open — without this, an extent
        // the primary allocated after the standby opened stays free in the
        // standby's in-memory bitmap, and a spilling read on the standby could
        // allocate scratch space over replicated pages. (Safe to do before the
        // fallible redo: on failure the extra marked-used extents are merely
        // conservative, and the re-apply re-marks them idempotently.)
        for (_, record) in &records {
            match record.kind {
                REL_KIND_ALLOC_EXTENT => {
                    let (start, pages) = record.decode_extent_redo()?;
                    self.allocator.mark_used(start, pages);
                }
                REL_KIND_FREE_EXTENT => {
                    let (start, pages) = record.decode_extent_redo()?;
                    self.allocator.free(start, pages);
                }
                _ => {}
            }
        }

        // A catalog-root change in the range moves the standby's catalog root
        // (the last one wins).
        let new_catalog_root = records
            .iter()
            .rev()
            .find(|(_, record)| record.kind == REL_KIND_SET_CATALOG_ROOT)
            .map(|(_, record)| record.decode_catalog_root())
            .transpose()?;

        // 3. Redo into the LIVE pool (page-LSN-gated, idempotent, appends
        //    nothing), then refresh the catalog cache so standby reads see any
        //    new tables/columns.
        {
            let mut ctx = self.rel_ctx();
            rel_recovery::redo_records(&mut ctx, &records)?;
        }
        if let Some(root) = new_catalog_root {
            self.rel.catalog_root = Some(root);
        }
        self.reload_catalog()?;

        // Only now — with every fallible step behind us — fold the range into
        // the restartpoint floors. Tracking it any earlier would let a FAILED
        // apply lift the undo floor over records whose redo never executed (a
        // resolution in the failed chunk would mark its transaction resolved),
        // and a restartpoint could then truncate undo a promotion still needs.
        for (lsn, record) in &records {
            self.standby_track_rel_record(*lsn, record);
        }
        // Track the first UNCOVERED search record (the restartpoint's search
        // floor): the seed snapshot covers seq numbers below
        // `snapshot_next_seq_no`; anything at or above must stay in the ring
        // for the reopen replay.
        if self.standby_search_floor.is_none() {
            for record in &scan.records {
                if record.entry_type != WAL_ENTRY_TYPE_REL
                    && record.seq_no >= self.snapshot_next_seq_no
                {
                    self.standby_search_floor = Some(record.logical_ts);
                    break;
                }
            }
        }

        // 4. Record the advanced tail durably (light dual-write, no page flush —
        //    the pages are recoverable from the now-durable ring). `applied_lsn`
        //    tracks the tail for the replication restartpoint.
        self.commit_superblock(|sb| {
            sb.wal_tail = advanced;
            sb.set_applied_lsn(advanced);
            // Mark this file a standby: its reopen must be redo-only, since the
            // shipped range can end mid-transaction and undoing it would diverge
            // the replica.
            sb.set_standby(true);
        })?;
        Ok(())
    }

    /// Writes the restored superblock: the source's catalog root and options as
    /// of `redo_start`, the ring bracketed at `[redo_start, backup_end)`. Slot A
    /// is active; slot B is a valid lower-generation mirror. The search-related
    /// roots are cleared — this slice does not restore the search snapshot.
    fn restore_superblock(
        &mut self,
        header: &crate::backup::BackupHeader,
        backup_end: u64,
        standby: bool,
    ) -> Result<(), StorageError> {
        let mut base = Superblock {
            wal_head: header.redo_start_lsn,
            wal_tail: backup_end,
            last_committed_seq: header.last_committed_seq,
            metadata_root: header.metadata_root,
            ..Superblock::default()
        };
        base.set_db_options(header.db_options);
        // Seed the log-backup floor at the restore point (`backup_end`), not 0:
        // a restored FULL-model database starts a fresh log chain here. Leaving
        // it 0 would make the on-open log-backup hold sit BELOW wal_head, which
        // `set_head` forbids (the floor can only move forward).
        base.set_last_log_backup_lsn(backup_end);
        // The restartpoint = the end of everything laid down (full backup + any
        // applied log chain / shipped WAL ranges), which is the restored tail.
        base.set_applied_lsn(backup_end);
        base.set_standby(standby);

        let mut a = base;
        a.generation = 1;
        a.active = SUPERBLOCK_ACTIVE_A;
        a.checksum = a.compute_checksum();

        let mut b = base;
        b.generation = 0;
        b.active = SUPERBLOCK_ACTIVE_B;
        b.checksum = b.compute_checksum();

        self.file.write_all_at(
            self.layout.superblock_a_offset,
            &a.to_le_bytes_with_checksum(),
        )?;
        self.file.write_all_at(
            self.layout.superblock_b_offset,
            &b.to_le_bytes_with_checksum(),
        )?;
        Ok(())
    }

    /// Fsyncs the restored file's data handle.
    fn sync_file(&mut self) -> Result<(), StorageError> {
        self.file.sync_data()?;
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
        recovery_full: Option<bool>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        // Enabling FULL starts a fresh log chain at the current tail; the
        // marker (and its hold) advance only via BACKUP LOG thereafter. An
        // already-FULL ALTER, or one that disables FULL / touches only the
        // snapshot options, leaves the marker where it was. Computed against
        // the OLD recovery model (before `version.set_options` below) and
        // stamped into the same durable superblock write as the option byte,
        // so a crash never leaves FULL set with a stale/zero marker.
        let enabling_full = recovery_full == Some(true) && !self.version.recovery_full;
        let new_marker = if enabling_full {
            self.wal.tail()
        } else {
            self.last_log_backup_lsn
        };
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
            if let Some(on) = recovery_full {
                next = (next & !4) | ((on as u8) << 2);
            }
            next
        };
        self.commit_superblock(|sb| {
            sb.set_db_options(byte);
            sb.set_last_log_backup_lsn(new_marker);
        })?;
        self.version
            .set_options(rcsi, allow_snapshot, recovery_full);
        // Now that the model byte and marker are durable, sync the in-memory
        // marker and the FULL-model log-truncation hold. FULL pins the ring at
        // the marker; SIMPLE releases it.
        self.last_log_backup_lsn = new_marker;
        if self.version.recovery_full {
            self.register_log_backup_hold(new_marker);
        } else {
            self.release_log_backup_hold();
        }
        Ok(())
    }

    /// Rebuilds both superblocks from the active slot, applies `mutate` to each,
    /// bumps the generation, and dual-writes them durably (active slot first,
    /// fsync between — a torn first write falls back to the other slot with the
    /// old state). Commits the new superblocks to memory only after both are
    /// durable. The single discipline behind every reserved-field update.
    fn commit_superblock(&mut self, mutate: impl Fn(&mut Superblock)) -> Result<(), StorageError> {
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);
        let (active, backup_flag) = match self.active_superblock {
            ActiveSuperblock::A => (self.superblock_a, SUPERBLOCK_ACTIVE_B),
            ActiveSuperblock::B => (self.superblock_b, SUPERBLOCK_ACTIVE_A),
        };
        let mut primary = active;
        let mut backup = active;
        backup.active = backup_flag;
        for sb in [&mut primary, &mut backup] {
            mutate(sb);
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
        Ok(())
    }

    /// Durably advances the persisted log-backup floor in both superblocks —
    /// the copy-out-before-truncate commit point for `BACKUP LOG`.
    fn persist_last_log_backup_lsn(&mut self, lsn: u64) -> Result<(), StorageError> {
        self.commit_superblock(|sb| sb.set_last_log_backup_lsn(lsn))
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
        // A checkpoint on a standby is refused: it would advance the WAL head
        // past the in-flight transactions the standby has applied but not
        // resolved, discarding the undo log they need at promotion. A standby
        // reclaims ring space with `Storage::standby_restartpoint` instead.
        if self.active_sb().is_standby() {
            return Err(StorageError::InvalidConfig(
                "checkpoint is not supported on a replication standby (apply-only until promotion)"
                    .to_string(),
            ));
        }
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
        //    and publish both superblocks (new active first). Reap over-lagging
        //    replication slots first, so an invalidated one no longer pins the
        //    floor and its log becomes reclaimable this checkpoint.
        self.reap_stale_slots();
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
        // The closure builds from Superblock::default() (reserved zeroed), so
        // the log-backup floor must be stamped back in or a checkpoint would
        // silently reset it to 0 and drop the FULL-model hold across a restart.
        let last_log_backup_lsn = self.last_log_backup_lsn;
        // Carry the standby (redo-only) mode and the replication epoch across
        // the checkpoint (the closure builds from a default superblock that
        // would otherwise clear them).
        let standby = self.active_sb().is_standby();
        let epoch = self.active_sb().epoch();
        // Persist the (post-reap) replication slots, so their truncation hold is
        // re-established on the next open. Snapshotted after the reap above, so an
        // invalidated slot is not written back.
        let repl_slots: Vec<(u32, u64)> = self
            .truncation_gate
            .repl_slots
            .iter()
            .map(|(&id, &lsn)| (id, lsn))
            .collect();
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
            sb.set_last_log_backup_lsn(last_log_backup_lsn);
            // Re-stamp the replication restartpoint from the same default-built
            // superblock (else a checkpoint would silently reset it to 0). On a
            // primary this is exactly the checkpoint tail.
            sb.set_applied_lsn(tail);
            // Re-stamp the replication slot table (same checkpoint-wipe carry).
            sb.set_repl_slots(&repl_slots);
            sb.set_standby(standby);
            sb.set_epoch(epoch);
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

/// Pages copied per storage-lock acquisition during a backup (1 MiB at 4 KiB
/// pages): bounds both the lock hold and the in-flight copy buffer.
const BACKUP_CHUNK_PAGES: u64 = 256;

/// Releases the WAL truncation hold registered by `begin_backup` when the
/// backup ends — on every path, including an early error return or a panic
/// unwind. A leaked hold freezes WAL truncation for the life of the process.
struct BackupHoldGuard<'a> {
    storage: &'a Storage,
}

impl Drop for BackupHoldGuard<'_> {
    fn drop(&mut self) {
        self.storage.lock().release_backup_hold();
    }
}

/// Releases the `BACKUP LOG` single-flight guard when the operation ends — on
/// every path, including an early error return or a panic during the unlocked
/// archive write. A leaked guard would reject every later `BACKUP LOG`.
struct LogBackupGuard<'a> {
    storage: &'a Storage,
}

impl Drop for LogBackupGuard<'_> {
    fn drop(&mut self) {
        self.storage.lock().cancel_log_backup();
    }
}

/// Everything captured under the storage lock at the start of a backup, so the
/// bulk page copy can proceed while releasing the lock between chunks.
struct BackupPlan {
    layout: StorageLayout,
    default_collation: Option<String>,
    redo_start: u64,
    metadata_root: u64,
    last_committed_seq: u64,
    db_options: u8,
    runs: Vec<(u64, u64)>,
    checksum: bool,
    copy_only: bool,
    finished_at_millis: u64,
}

impl BackupPlan {
    fn header(&self) -> crate::backup::BackupHeader {
        crate::backup::BackupHeader {
            format_version: crate::backup::FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            total_size: self.layout.total_size,
            wal_size: self.layout.wal_size,
            data_size: self.layout.data_size,
            metadata_size: self.layout.metadata_size,
            allocator_size: self.layout.allocator_size,
            snapshot_size: self.layout.snapshot_size,
            reserved_size: self.layout.reserved_size,
            default_collation: self.default_collation.clone(),
            redo_start_lsn: self.redo_start,
            metadata_root: self.metadata_root,
            last_committed_seq: self.last_committed_seq,
            db_options: self.db_options,
            finished_at_millis: self.finished_at_millis,
            flags: crate::backup::BackupFlags {
                checksum: self.checksum,
                copy_only: self.copy_only,
                log_backup: false,
            },
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Writes a log-only `TDBBAK1` archive (one `LogChunk`) at `dst` and makes it
/// FULLY durable — data, then the parent directory's entry — before returning.
/// Called with the storage lock RELEASED. Directory durability matters because
/// the caller then advances the log-backup marker, which makes the shipped log
/// range reclaimable; if the archive's directory entry were not durable, a crash
/// could reclaim the log while the only archive copy lost its name.
fn write_log_archive(
    dst: &Path,
    header: &crate::backup::BackupHeader,
    start: u64,
    log: &[u8],
) -> Result<(), StorageError> {
    use crate::backup::{BackupWriter, BlockType, encode_log_chunk};
    let file = std::fs::File::create(dst)?;
    let mut writer = BackupWriter::new(file, header)?;
    writer.write_block(BlockType::LogChunk, &encode_log_chunk(start, log))?;
    writer.finish()?.sync_all()?;
    fsync_parent_dir(dst)?;
    Ok(())
}

/// Fsyncs the parent directory of `path` so a newly created file's name is
/// durable (POSIX: `fsync` on a file does not persist its directory entry).
fn fsync_parent_dir(path: &Path) -> Result<(), StorageError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Applies one `BACKUP LOG` archive to a restore in progress: reads its log
/// range, verifies it continues from the current coverage (no gap — error 4305)
/// and that the whole recoverable range still fits the ring, then seeds it and
/// extends `tail`. Overlap with already-seeded log is harmless (same bytes).
/// Applies a raw shipped WAL ring range `[from_lsn, from_lsn + bytes.len())` to a
/// restore/standby file, extending the seeded ring. The sibling of
/// [`apply_log_archive`] for physical replication: the bytes are the primary's
/// own ring bytes (from `read_ring_range` at a flushed watermark), not a
/// `TDBBAK1` archive, so there is no per-block framing to decode — the range
/// starts and ends on WAL entry boundaries and recovery's forward scan validates
/// it (self-identity + CRC) on the next open.
///
/// `from_lsn` may be `<= tail` (a re-shipped, overlapping range overwrites
/// identical bytes — idempotent); a `from_lsn > tail` is a chain gap (4305). The
/// covered range must fit the ring's usable size, leaving the CLR reserve free
/// for recovery's undo, exactly as [`apply_log_archive`] caps a log chain.
fn apply_wal_range(
    file: &mut StorageFile,
    from_lsn: u64,
    bytes: &[u8],
    head: u64,
    tail: &mut u64,
) -> Result<(), StorageError> {
    if bytes.is_empty() {
        return Ok(());
    }
    if from_lsn > *tail {
        return Err(StorageError::InvalidFile(format!(
            "WAL range gap (4305): range begins at LSN {from_lsn} but the standby has reached {tail}"
        )));
    }
    let new_end = from_lsn + bytes.len() as u64;
    let max_range = file.layout.wal_size.saturating_sub(file.wal.reserve());
    if new_end.saturating_sub(head) > max_range {
        return Err(StorageError::InvalidFile(
            "the applied WAL range exceeds the ring's usable size; \
             incremental standby restore is not yet supported"
                .to_string(),
        ));
    }
    file.seed_ring(from_lsn, bytes)?;
    *tail = (*tail).max(new_end);
    Ok(())
}

fn apply_log_archive(
    file: &mut StorageFile,
    path: &Path,
    head: u64,
    tail: &mut u64,
) -> Result<(), StorageError> {
    use crate::backup::{BlockType, decode_log_chunk};
    let reader = std::io::BufReader::new(std::fs::File::open(path)?);
    let (mut r, header) = crate::backup::BackupReader::new(reader)?;
    if !header.flags.log_backup {
        return Err(StorageError::InvalidFile(format!(
            "{}: --log expects a BACKUP LOG archive, but this is a full backup",
            path.display()
        )));
    }
    // Partial identity guard: a log archive whose page or ring geometry differs
    // was taken against a differently-configured database and cannot belong to
    // this chain. (A full database-identity match — a persisted DB uuid checked
    // against the full backup — is future work; today the LSN-continuity check
    // and geometry are the only cross-database guards.)
    if header.page_size as usize != PAGE_SIZE || header.wal_size != file.layout.wal_size {
        return Err(StorageError::InvalidFile(format!(
            "{}: log archive geometry does not match the restored database",
            path.display()
        )));
    }
    // Concatenate the archive's (contiguous) log chunks.
    let mut chunk: Option<(u64, Vec<u8>)> = None;
    while let Some((block_type, payload)) = r.next_block()? {
        if block_type == BlockType::LogChunk {
            let (start_lsn, bytes) = decode_log_chunk(&payload)?;
            match &mut chunk {
                Some((first, acc)) => {
                    if start_lsn != *first + acc.len() as u64 {
                        return Err(StorageError::InvalidFile(
                            "log archive chunks are not contiguous".to_string(),
                        ));
                    }
                    acc.extend_from_slice(bytes);
                }
                None => chunk = Some((start_lsn, bytes.to_vec())),
            }
        }
    }
    let Some((start_lsn, bytes)) = chunk else {
        return Ok(()); // an empty log backup contributes nothing
    };
    // Continuity: a start LATER than the current tail is a broken chain (4305).
    if start_lsn > *tail {
        return Err(StorageError::InvalidFile(format!(
            "log chain gap (4305): {} begins at LSN {start_lsn} but the restore has reached {tail}",
            path.display()
        )));
    }
    let new_end = start_lsn + bytes.len() as u64;
    // Leave the CLR reserve free: on open, ARIES undo appends compensation
    // records for the transactions in flight at the chain end (use_reserve), so
    // the recoverable range must fit `wal_size - reserve`, exactly as the full
    // backup caps its own shipped log. Otherwise a legitimate long chain fills
    // the ring and recovery's first undo append hits WalFull.
    let max_range = file.layout.wal_size.saturating_sub(file.wal.reserve());
    if new_end.saturating_sub(head) > max_range {
        return Err(StorageError::InvalidFile(
            "the full backup plus its log chain exceed the WAL ring's usable size; \
             incremental restore is not yet supported"
                .to_string(),
        ));
    }
    file.seed_ring(start_lsn, &bytes)?;
    *tail = (*tail).max(new_end);
    Ok(())
}

/// Rebuilds a [`StorageLayout`] from a backup header's region sizes. The
/// offsets follow the fixed region order (see [`compute_layout`]).
fn layout_from_backup_header(header: &crate::backup::BackupHeader) -> StorageLayout {
    let page = PAGE_SIZE as u64;
    let wal_offset = page * 3;
    let data_offset = wal_offset + header.wal_size;
    let metadata_offset = data_offset + header.data_size;
    let allocator_offset = metadata_offset + header.metadata_size;
    let snapshot_offset = allocator_offset + header.allocator_size;
    let reserved_offset = snapshot_offset + header.snapshot_size;
    StorageLayout {
        total_size: header.total_size,
        header_offset: 0,
        superblock_a_offset: page,
        superblock_b_offset: page * 2,
        wal_offset,
        wal_size: header.wal_size,
        data_offset,
        data_size: header.data_size,
        metadata_offset,
        metadata_size: header.metadata_size,
        allocator_offset,
        allocator_size: header.allocator_size,
        snapshot_offset,
        snapshot_size: header.snapshot_size,
        reserved_offset,
        reserved_size: header.reserved_size,
    }
}

/// Removes the page range `[start, start + len)` from a set of ascending,
/// disjoint allocated runs, splitting a run that straddles it.
fn subtract_run(runs: Vec<(u64, u64)>, start: u64, len: u64) -> Vec<(u64, u64)> {
    let cut_end = start + len;
    let mut out = Vec::with_capacity(runs.len());
    for (run_start, run_count) in runs {
        let run_end = run_start + run_count;
        if run_end <= start || run_start >= cut_end {
            out.push((run_start, run_count)); // disjoint
            continue;
        }
        if run_start < start {
            out.push((run_start, start - run_start)); // left remainder
        }
        if run_end > cut_end {
            out.push((cut_end, run_end - cut_end)); // right remainder
        }
    }
    out
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

    #[test]
    fn backup_is_single_flight_and_releases_its_hold() {
        let path = unique_temp_path("backup-hold");
        let bak = unique_temp_path("backup-hold-bak");
        let bak2 = unique_temp_path("backup-hold-bak2");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");

        // A second backup while a hold is set is rejected, and — critically —
        // must NOT clear the in-flight backup's hold (the guard arms only after
        // begin_backup succeeds, so a rejected backup touches nothing).
        storage.lock().register_backup_hold(42);
        assert!(matches!(
            storage.backup_full(&bak),
            Err(StorageError::BackupInProgress)
        ));
        assert_eq!(
            storage.lock().truncation_gate.backup,
            Some(42),
            "a rejected backup leaves the existing hold intact"
        );
        storage.lock().release_backup_hold();

        // A successful backup releases its own hold, so a second one succeeds.
        storage.backup_full(&bak).expect("first backup");
        assert_eq!(
            storage.lock().truncation_gate.backup,
            None,
            "the hold is released after a successful backup"
        );
        storage
            .backup_full(&bak2)
            .expect("second backup after release");

        drop(storage);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(bak);
        let _ = std::fs::remove_file(bak2);
    }

    #[test]
    fn log_backup_orphans_when_recovery_flips_to_simple_mid_flight() {
        // Reproduces the race the lock-dance opened: BACKUP LOG releases the
        // storage lock to write its archive; a concurrent ALTER ... SET RECOVERY
        // SIMPLE releases the log hold and a checkpoint advances the head; phase
        // 3 must then ORPHAN the backup rather than re-arm the hold below head.
        let path = unique_temp_path("backuplog-orphan");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        storage
            .rel_set_db_options(None, None, Some(true))
            .expect("enable FULL");
        for seq in 0..8 {
            storage
                .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, seq, b"logbytes")
                .expect("append");
        }
        // Phase 1: capture the range under the lock (marker = start).
        let (_, start, end, _log) = storage.lock().begin_log_backup(true, false).expect("begin");
        assert!(end > start);
        // The concurrent ALTER SET RECOVERY SIMPLE during the unlocked window.
        storage
            .rel_set_db_options(None, None, Some(false))
            .expect("disable FULL");
        assert_eq!(storage.log_backup_hold(), None, "SIMPLE released the hold");
        // A checkpoint now advances the head to the tail (no hold pins it).
        storage
            .write_checkpoint(b"cp", 1, 2, 1)
            .expect("checkpoint after SIMPLE");
        assert!(
            storage.wal_head() > start,
            "the head advanced past the old marker"
        );
        // Phase 3: finish must orphan (recovery_full is false) — no re-arm.
        storage
            .lock()
            .finish_log_backup(start, end)
            .expect("finish orphans cleanly");
        assert_eq!(
            storage.log_backup_hold(),
            None,
            "finish did not re-arm the hold on the now-SIMPLE database"
        );
        // The next checkpoint must not move the head backward (would panic
        // without the orphan guard).
        storage
            .write_checkpoint(b"cp2", 2, 3, 2)
            .expect("checkpoint after orphan does not panic");
        storage.lock().cancel_log_backup();
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_page_freed_since_the_backup_began_is_not_treated_as_corrupt() {
        let path = unique_temp_path("backup-freed");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        {
            let mut file = storage.lock();
            let page = file.allocator.allocate(1).expect("allocate a page");
            assert!(
                file.page_is_live_regular(page).unwrap(),
                "a live, allocated data page is regular (a checksum failure there is real corruption)"
            );
            file.allocator.free(page, 1);
            assert!(
                !file.page_is_live_regular(page).unwrap(),
                "a page freed since the backup began is tolerated, not flagged corrupt"
            );
        }
        drop(storage);
        let _ = std::fs::remove_file(path);
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
    /// Lock analysis descends control-flow bodies: a WHILE body's INSERT and
    /// an IF condition's EXISTS table are in the batch's up-front lock set.
    /// EXEC of a user procedure locks the STORED BODY's tables up front —
    /// parsed with the in-procedure grammar (a plain parse would 178 on
    /// `RETURN <value>`, yield no locks, and the body would run unlocked).
    /// Recursive procedures terminate analysis via the visited set.
    #[test]
    fn analyze_locks_covers_procedure_bodies() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("proc-locks");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE plt (id INT NOT NULL PRIMARY KEY)",
            "CREATE PROCEDURE writer @v INT AS INSERT INTO plt VALUES (@v); \
             EXEC writer @v; RETURN 5",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let needs = crate::rel::analyze_locks(&storage, "EXEC writer 1", Isolation::ReadCommitted);
        assert!(
            needs.iter().any(
                |(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive
                    || matches!(r, Resource::Row(..))
            ),
            "the recursive body's INSERT locks its table (and analysis \
             terminated): {needs:?}"
        );
        // An unknown procedure contributes no locks (2812 at execution).
        let needs =
            crate::rel::analyze_locks(&storage, "EXEC no_such_proc", Isolation::ReadCommitted);
        assert!(
            !needs
                .iter()
                .any(|(r, _)| matches!(r, Resource::Table(_) | Resource::Row(..))),
            "unknown proc: {needs:?}"
        );
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Adversarial review probe: the visited set dedups a procedure's lock
    /// contribution, but a body's lock set DEPENDS on the effective
    /// isolation. Under RCSI, `EXEC pread` analyzed first contributes only
    /// Database IS (versioned read); a later `EXEC pser` — whose body raises
    /// to SERIALIZABLE and EXECs pread — finds pread already visited and
    /// skips it, so the lock-based re-analysis (Table S) is dropped. At
    /// execution the SET is live inside pser and pread's SELECT reads
    /// lock-based with no Table S held: the 2PL under-lock class.
    #[test]
    fn review_poc_analyze_locks_procedure_reanalyzed_under_escalated_isolation() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("proc-visited-isolation");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE PROCEDURE pread AS SELECT v FROM t",
            "CREATE PROCEDURE pser AS \
             SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; EXEC pread",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let table_s = |needs: &[(Resource, LockMode)]| {
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared)
        };
        // Control: analyzed alone, the escalated body read-locks.
        let needs = crate::rel::analyze_locks(&storage, "EXEC pser", Isolation::ReadCommitted);
        assert!(
            table_s(&needs),
            "control: pser's escalated body takes Table S: {needs:?}"
        );
        // The seam: pread analyzed first under the versioned regime, then
        // pser's escalated re-entry is dropped by the visited set.
        let needs =
            crate::rel::analyze_locks(&storage, "EXEC pread; EXEC pser", Isolation::ReadCommitted);
        assert!(
            table_s(&needs),
            "pser still runs pread's SELECT under SERIALIZABLE — Table S \
             must be in the up-front set: {needs:?}"
        );
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Adversarial review probe: a stored procedure EXEC'd from INSIDE a
    /// dynamic-SQL literal is still resolved by lock analysis (the literal
    /// recursion's Exec arm hits the procedure branch).
    #[test]
    fn review_poc_analyze_locks_procedure_via_dynamic_sql() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("proc-dyn-locks");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
            "CREATE PROCEDURE wtr @v INT AS INSERT INTO t VALUES (@v)",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let needs = crate::rel::analyze_locks(
            &storage,
            "EXEC sp_executesql N'EXEC wtr 1'",
            Isolation::ReadCommitted,
        );
        assert!(
            needs.iter().any(|(r, m)| matches!(r, Resource::Table(_))
                && matches!(m, LockMode::IntentExclusive | LockMode::Exclusive)
                || matches!(r, Resource::Row(..))),
            "the proc body's INSERT is in the up-front set via the literal \
             path: {needs:?}"
        );
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Adversarial review probe: analysis resolves the catalog FIRST, but
    /// execution checks the sp_executesql builtin FIRST. A user procedure
    /// named sp_executesql makes the two disagree: analysis analyzes the
    /// user body, execution runs the builtin over the literal — whose locks
    /// were never analyzed (under-lock). Either the CREATE must be refused
    /// or the analysis must mirror execution's builtin-first order.
    #[test]
    fn review_poc_user_procedure_named_sp_executesql_cannot_shadow_builtin() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("proc-spexec-shadow");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let outcome = execute_batch(
            &storage,
            "CREATE PROCEDURE sp_executesql AS SELECT 1 AS n",
            &mut ctx,
        );
        if outcome.error.is_none() {
            // The shadow exists: analysis and execution must still agree.
            // Execution runs the BUILTIN (its name check comes first), so the
            // literal's INSERT locks must be in the analyzed set.
            let needs = crate::rel::analyze_locks(
                &storage,
                "EXEC sp_executesql N'INSERT INTO t VALUES (1)'",
                Isolation::ReadCommitted,
            );
            assert!(
                needs.iter().any(|(r, m)| matches!(r, Resource::Table(_))
                    && matches!(m, LockMode::IntentExclusive | LockMode::Exclusive)
                    || matches!(r, Resource::Row(..))
                    || (matches!(r, Resource::Database) && *m == LockMode::Exclusive)),
                "execution runs the builtin INSERT; analysis followed the \
                 user proc's body instead: {needs:?}"
            );
        }
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn analyze_locks_descends_control_flow() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("flow-locks");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE locked_t (id INT NOT NULL PRIMARY KEY)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        let needs = crate::rel::analyze_locks(
            &storage,
            "DECLARE @i INT = 0; WHILE @i < 3 BEGIN INSERT INTO locked_t VALUES (@i); \
             SET @i = @i + 1; END",
            Isolation::ReadCommitted,
        );
        assert!(
            needs.iter().any(
                |(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive
                    || matches!(r, Resource::Row(..))
            ),
            "the WHILE body's INSERT locks its table: {needs:?}"
        );

        let needs = crate::rel::analyze_locks(
            &storage,
            "IF EXISTS (SELECT * FROM locked_t) SELECT 1",
            Isolation::ReadCommitted,
        );
        assert!(
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
            "the IF condition's EXISTS table takes Table S: {needs:?}"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Adversarial probes (control-flow review): condition shapes whose table
    /// reads must be in the up-front lock set — a WHILE condition, a derived
    /// table and an IN-subquery inside the condition, a CASE-wrapped EXISTS,
    /// a view, an untaken ELSE branch's write, and an EXEC literal inside a
    /// WHILE body.
    #[test]
    fn cf_review_analyze_locks_condition_shapes() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("cf-flow-lock-shapes");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE lt (id INT NOT NULL PRIMARY KEY)",
            "CREATE VIEW lv AS SELECT id FROM lt",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let table_s = |needs: &[(Resource, LockMode)]| {
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared)
        };
        let table_write = |needs: &[(Resource, LockMode)]| {
            needs.iter().any(|(r, m)| {
                matches!(r, Resource::Table(_)) && *m == LockMode::Exclusive
                    || matches!(r, Resource::Row(..))
            })
        };
        for sql in [
            "WHILE EXISTS (SELECT * FROM lt) SELECT 1",
            "IF EXISTS (SELECT * FROM (SELECT id FROM lt) d) SELECT 1",
            "IF 1 IN (SELECT id FROM lt) SELECT 1",
            "IF CASE WHEN EXISTS (SELECT * FROM lt) THEN 1 ELSE 0 END = 1 SELECT 1",
            "IF EXISTS (SELECT * FROM lv) SELECT 1",
            "IF (SELECT COUNT(*) FROM lt) = 0 SELECT 1",
        ] {
            let needs = crate::rel::analyze_locks(&storage, sql, Isolation::ReadCommitted);
            assert!(
                table_s(&needs),
                "{sql}: condition read takes Table S: {needs:?}"
            );
        }
        // Both IF branches analyze — an untaken ELSE's INSERT is still locked.
        let needs = crate::rel::analyze_locks(
            &storage,
            "IF 1 = 2 SELECT 1 ELSE INSERT INTO lt VALUES (9)",
            Isolation::ReadCommitted,
        );
        assert!(
            table_write(&needs),
            "the ELSE branch's INSERT locks its table: {needs:?}"
        );
        // An EXEC literal inside a WHILE body analyzes through the Exec arm.
        let needs = crate::rel::analyze_locks(
            &storage,
            "DECLARE @i INT = 0; WHILE @i < 1 BEGIN \
             EXEC sp_executesql N'INSERT INTO lt VALUES (7)'; SET @i = @i + 1; END",
            Isolation::ReadCommitted,
        );
        assert!(
            table_write(&needs),
            "the EXEC'd INSERT inside the loop locks its table: {needs:?}"
        );
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// A CTE inside an IF condition's subquery: the executor inlines it and
    /// reads the base table (engine.rs pins that half), so analysis must lock
    /// that table like the Select arm does — the expectation here is the
    /// FIXED behavior.
    #[test]
    fn cf_review_analyze_locks_condition_cte() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::{Isolation, TxnContext, execute_batch};

        let path = unique_temp_path("cf-flow-lock-cte");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE lt (id INT NOT NULL PRIMARY KEY)",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let needs = crate::rel::analyze_locks(
            &storage,
            "IF EXISTS (WITH x AS (SELECT id FROM lt) SELECT id FROM x) SELECT 1",
            Isolation::ReadCommitted,
        );
        assert!(
            needs
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(_)) && *m == LockMode::Shared),
            "the CTE's base table is read at runtime and must be locked: {needs:?}"
        );
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

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

    /// Version cleanup under load (Stage 13 exit): a live snapshot pins
    /// exactly the history it may still read through sustained churn — and
    /// keeps reading its own consistent view — while releasing it lets the
    /// maintenance prune drop everything.
    #[test]
    fn version_cleanup_under_load_pins_then_drops_history() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("cleanup-load");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let mut seed = String::from("INSERT INTO t VALUES ");
        for i in 0..100 {
            seed.push_str(&format!("({i}, 0),"));
        }
        seed.pop();
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            seed.as_str(),
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        storage
            .ensure_durable(storage.wal_tail())
            .expect("durability");
        storage.version_prune();
        assert_eq!(storage.version_chain_count("t"), 0, "settled baseline");

        // A long-lived snapshot (an idle SNAPSHOT transaction's view) while
        // a writer churns every row, five rounds, pruning between rounds.
        let pinned = storage.capture_read_snapshot(None);
        for round in 1..=5 {
            let outcome = execute_batch(&storage, &format!("UPDATE t SET v = {round}"), &mut ctx);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            storage
                .ensure_durable(storage.wal_tail())
                .expect("durability");
            storage.version_prune();
        }
        assert_eq!(
            storage.version_chain_count("t"),
            100,
            "the live snapshot pins one chain per churned row"
        );
        // The pinned view still reads its consistent state through all of it.
        let rows = storage
            .rel_scan_snapshot("t", Some(&[1]), pinned)
            .expect("snapshot scan");
        assert_eq!(rows.len(), 100);
        assert!(
            rows.iter().all(|r| r == &vec![Datum::Int(0)]),
            "the snapshot sees the pre-churn value on every row"
        );

        // Released: the next prune drops the whole store.
        storage.release_read_snapshot(pinned.seq);
        storage.version_prune();
        assert_eq!(
            storage.version_chain_count("t"),
            0,
            "released history is dropped, the store is bounded"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// SI review PoC: the 3960 auto-abort path must release the
    /// transaction's snapshot registration - a leak would pin the prune
    /// watermark forever. Observable through pruning: while the snapshot is
    /// registered the conflicting chain must survive a prune; after the 3960
    /// rolled the transaction back, the same prune must drop it.
    #[test]
    fn a_3960_abort_releases_the_snapshot_registration() {
        use crate::rel::{TxnContext, execute_batch};

        let path = unique_temp_path("si-3960-release");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut setup = TxnContext::default();
        for sql in [
            "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON",
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
            "INSERT INTO t VALUES (1, 10)",
        ] {
            let outcome = execute_batch(&storage, sql, &mut setup);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        storage
            .ensure_durable(storage.wal_tail())
            .expect("durability");
        storage.version_prune();
        assert_eq!(storage.version_chain_count("t"), 0);

        // B: SNAPSHOT transaction, snapshot captured at first access.
        let mut b = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; BEGIN TRAN; SELECT v FROM t",
            &mut b,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // A: a conflicting committed write B's snapshot cannot see.
        let mut a = TxnContext::default();
        let outcome = execute_batch(&storage, "UPDATE t SET v = 99 WHERE id = 1", &mut a);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // While B's snapshot is registered, its history is pinned.
        storage
            .ensure_durable(storage.wal_tail())
            .expect("durability");
        storage.version_prune();
        assert_eq!(
            storage.version_chain_count("t"),
            1,
            "a registered snapshot pins the chain"
        );

        // B writes the same row: 3960, and the whole transaction (with its
        // snapshot registration) must be gone.
        let outcome = execute_batch(&storage, "UPDATE t SET v = 100 WHERE id = 1", &mut b);
        assert_eq!(
            outcome.error.as_ref().map(|e| e.number),
            Some(3960),
            "{:?}",
            outcome.error
        );

        storage.version_prune();
        assert_eq!(
            storage.version_chain_count("t"),
            0,
            "the 3960 abort must release the snapshot registration"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The Stage 14 exit criterion: a 10 MiB value round-trips through an
    /// overflow chain, survives a kill-and-reopen, and a mid-transaction
    /// update of it recovers to the committed value.
    #[test]
    fn ten_mib_value_round_trips_and_survives_a_crash() {
        use crate::rel::RpcParam;
        use crate::rel::{StatementResult, TxnContext, execute_batch, execute_batch_with_params};

        let path = unique_temp_path("max-10mib");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE big (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))",
            "INSERT INTO big VALUES (1, N'seed')",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        // 10 MiB of UTF-16 payload = 5 * 1024 * 1024 characters.
        let big: String = "abcdefgh".repeat(5 * 1024 * 1024 / 8);
        assert_eq!(big.encode_utf16().count() * 2, 10 * 1024 * 1024);
        let outcome = execute_batch_with_params(
            &storage,
            "UPDATE big SET body = @v WHERE id = 1",
            &mut ctx,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: crate::relstore::types::Datum::NVarChar(big.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        let fetch = |storage: &Storage, ctx: &mut TxnContext| -> String {
            let outcome = execute_batch(storage, "SELECT body FROM big WHERE id = 1", ctx);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            match &outcome.results[0] {
                StatementResult::Rows(rowset) => match &rowset.rows[0][0] {
                    Datum::NVarChar(s) => s.clone(),
                    other => panic!("expected NVARCHAR, got {other:?}"),
                },
                other => panic!("expected rows, got {other:?}"),
            }
        };
        assert_eq!(
            fetch(&storage, &mut ctx),
            big,
            "round-trip before the crash"
        );

        // An in-flight update dies with the crash; the committed 10 MiB
        // value must recover intact.
        let outcome = execute_batch(
            &storage,
            "BEGIN TRAN; UPDATE big SET body = N'doomed' WHERE id = 1;",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let _ = ctx;
        drop(storage);

        let storage = Storage::open(path.clone()).expect("recovery");
        let mut ctx = TxnContext::default();
        let recovered = fetch(&storage, &mut ctx);
        assert_eq!(recovered.len(), big.len(), "length after recovery");
        assert_eq!(
            recovered, big,
            "the committed chain survives, the loser is undone"
        );

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Versioning over (MAX): an RCSI reader sees the pre-update big value
    /// through the version image's overflow REFERENCE (images carry raw row
    /// bytes; chains are immutable and never freed, so the ref stays valid).
    #[test]
    fn rcsi_reads_the_old_big_value_through_the_image_reference() {
        use crate::rel::{
            RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
        };

        let path = unique_temp_path("max-rcsi");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut writer = TxnContext::default();
        let big_old: String = "old-value".repeat(20_000); // ~180 KB
        let big_new: String = "new-value".repeat(20_000);
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE TABLE big (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))",
        ] {
            let outcome = execute_batch(&storage, sql, &mut writer);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let outcome = execute_batch_with_params(
            &storage,
            "INSERT INTO big VALUES (1, @v)",
            &mut writer,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big_old.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // Writer holds an uncommitted update to the big value...
        let outcome = execute_batch(&storage, "BEGIN TRAN", &mut writer);
        assert!(outcome.error.is_none());
        let outcome = execute_batch_with_params(
            &storage,
            "UPDATE big SET body = @v WHERE id = 1",
            &mut writer,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big_new.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // ...and a snapshot reader gets the OLD value, resolved through the
        // image's overflow reference.
        let mut reader = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT body FROM big WHERE id = 1", &mut reader);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::NVarChar(big_old.clone()));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let outcome = execute_batch(&storage, "COMMIT", &mut writer);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let outcome = execute_batch(&storage, "SELECT body FROM big WHERE id = 1", &mut reader);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::NVarChar(big_new.clone()));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Review PoC: HEAP tables' version priors come from `heap.read_row`
    /// pre-images. An RCSI reader must see the pre-update big value of a heap
    /// row (resolved through the image's overflow reference), and a deleted
    /// heap row must stay visible to the open snapshot.
    #[test]
    fn heap_rcsi_reads_old_big_value_through_preimage() {
        use crate::rel::{
            RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
        };

        let path = unique_temp_path("review-heap-rcsi");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut writer = TxnContext::default();
        let big_old: String = "old-heap".repeat(10_000); // 80k chars -> chain
        let big_new: String = "new-heap".repeat(10_000);
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            // No PRIMARY KEY: a heap.
            "CREATE TABLE hbig (id INT NOT NULL, body NVARCHAR(MAX))",
        ] {
            let outcome = execute_batch(&storage, sql, &mut writer);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let outcome = execute_batch_with_params(
            &storage,
            "INSERT INTO hbig VALUES (1, @v)",
            &mut writer,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big_old.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        let fetch = |ctx: &mut TxnContext| -> Option<Datum> {
            let outcome = execute_batch(&storage, "SELECT body FROM hbig WHERE id = 1", ctx);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            match &outcome.results[0] {
                StatementResult::Rows(rowset) => rowset.rows.first().map(|r| r[0].clone()),
                other => panic!("expected rows, got {other:?}"),
            }
        };

        // Uncommitted UPDATE: the reader sees the old value via the image.
        let outcome = execute_batch(&storage, "BEGIN TRAN", &mut writer);
        assert!(outcome.error.is_none());
        let outcome = execute_batch_with_params(
            &storage,
            "UPDATE hbig SET body = @v WHERE id = 1",
            &mut writer,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big_new.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let mut reader = TxnContext::default();
        assert_eq!(
            fetch(&mut reader),
            Some(Datum::NVarChar(big_old.clone())),
            "heap RCSI reader must get the pre-update value"
        );
        let outcome = execute_batch(&storage, "COMMIT", &mut writer);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(fetch(&mut reader), Some(Datum::NVarChar(big_new.clone())));

        // Uncommitted DELETE: the reader still sees the (new) value.
        let outcome = execute_batch(
            &storage,
            "BEGIN TRAN; DELETE FROM hbig WHERE id = 1;",
            &mut writer,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(
            fetch(&mut reader),
            Some(Datum::NVarChar(big_new.clone())),
            "heap RCSI reader must see the row an open txn deleted"
        );
        let outcome = execute_batch(&storage, "COMMIT", &mut writer);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(fetch(&mut reader), None);

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Review PoC: a heap row that has MOVED (forwarding stub at its home
    /// RID) must still read correctly, version correctly (the pre-image is
    /// read through the stub), and keep its overflow value.
    #[test]
    fn moved_heap_row_versions_and_resolves_through_the_stub() {
        use crate::rel::{
            RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
        };

        let path = unique_temp_path("review-heap-moved");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut writer = TxnContext::default();
        let big_old: String = "moved-old".repeat(5_000); // 45k chars -> chain
        let big_new: String = "moved-new".repeat(5_000);
        for sql in [
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
            "CREATE TABLE hm (id INT NOT NULL, pad VARCHAR(3000), body NVARCHAR(MAX))",
        ] {
            let outcome = execute_batch(&storage, sql, &mut writer);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        // Row 1 small; row 2 fills most of the first heap page, so growing
        // row 1's pad to 3000 bytes cannot fit and must move the row.
        let outcome = execute_batch_with_params(
            &storage,
            "INSERT INTO hm VALUES (1, 'a', @v)",
            &mut writer,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big_old.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let filler = "f".repeat(3000);
        let outcome = execute_batch(
            &storage,
            &format!("INSERT INTO hm VALUES (2, '{filler}', NULL)"),
            &mut writer,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let grown = "g".repeat(3000);
        let outcome = execute_batch(
            &storage,
            &format!("UPDATE hm SET pad = '{grown}' WHERE id = 1"),
            &mut writer,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        // The moved row still resolves its chain.
        let fetch_body = |ctx: &mut TxnContext| -> Option<Datum> {
            let outcome = execute_batch(&storage, "SELECT body FROM hm WHERE id = 1", ctx);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            match &outcome.results[0] {
                StatementResult::Rows(rowset) => rowset.rows.first().map(|r| r[0].clone()),
                other => panic!("expected rows, got {other:?}"),
            }
        };
        assert_eq!(
            fetch_body(&mut writer),
            Some(Datum::NVarChar(big_old.clone())),
            "moved row reads its chain"
        );

        // Version an update of the moved row: the prior must be read through
        // the forwarding stub, and the reader must get the old big value.
        let outcome = execute_batch(&storage, "BEGIN TRAN", &mut writer);
        assert!(outcome.error.is_none());
        let outcome = execute_batch_with_params(
            &storage,
            "UPDATE hm SET body = @v WHERE id = 1",
            &mut writer,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big_new.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let mut reader = TxnContext::default();
        assert_eq!(
            fetch_body(&mut reader),
            Some(Datum::NVarChar(big_old.clone())),
            "RCSI reader must get the pre-update value of a MOVED heap row"
        );
        // The pad read through the same image must be the grown one.
        let outcome = execute_batch(&storage, "SELECT pad FROM hm WHERE id = 1", &mut reader);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::VarChar(grown.clone()));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let outcome = execute_batch(&storage, "COMMIT", &mut writer);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(fetch_body(&mut reader), Some(Datum::NVarChar(big_new)));

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Review PoC: a statement that spills a chain and THEN fails (duplicate
    /// key on a later row) must roll back cleanly — the chain leaks, the rows
    /// do not land, the store stays usable, and recovery after a kill agrees.
    #[test]
    fn failed_statement_after_spill_rolls_back_cleanly() {
        use crate::rel::{
            RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
        };

        let path = unique_temp_path("review-spill-fail");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE sf (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))",
            "INSERT INTO sf VALUES (7, N'anchor')",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let big: String = "spilled".repeat(10_000); // 70k chars -> chain
        // Row 1 spills its chain, row 2 hits the duplicate key: the whole
        // statement must fail and undo row 1.
        let outcome = execute_batch_with_params(
            &storage,
            "INSERT INTO sf VALUES (1, @v), (7, N'dup')",
            &mut ctx,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big.clone()),
            }],
        );
        assert!(outcome.error.is_some(), "duplicate key must fail");

        let count = |ctx: &mut TxnContext| -> Datum {
            let outcome = execute_batch(&storage, "SELECT COUNT(*) FROM sf", ctx);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            match &outcome.results[0] {
                StatementResult::Rows(rowset) => rowset.rows[0][0].clone(),
                other => panic!("expected rows, got {other:?}"),
            }
        };
        assert_eq!(count(&mut ctx), Datum::BigInt(1), "only the anchor row");

        // The store is still usable: the same big value inserts fine now.
        let outcome = execute_batch_with_params(
            &storage,
            "INSERT INTO sf VALUES (1, @v)",
            &mut ctx,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::NVarCharMax,
                value: Datum::NVarChar(big.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(count(&mut ctx), Datum::BigInt(2));

        // Kill and reopen: recovery replays the leaked chain's images and
        // the committed rows; the value survives.
        drop(storage);
        let storage = Storage::open(path.clone()).expect("recovery");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT body FROM sf WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::NVarChar(big));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Review PoC: ALTER TABLE ADD on a table with spilled (MAX) values.
    /// The rewrite resolves every row and re-encodes it WITHOUT re-spilling,
    /// so a chain value small enough for the row cap is silently re-inlined
    /// (and must survive), while a big one fails the whole ALTER — which must
    /// fail CLEANLY, leaving the table intact and the store usable.
    #[test]
    fn alter_add_column_respills_max_values() {
        use crate::rel::{
            RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
        };

        let path = unique_temp_path("review-alter-max");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();

        // Case A: a 300-byte chain value fits back inline; ALTER succeeds.
        let small_chain = "s".repeat(300); // VARCHAR: 300 bytes > 256 -> chain
        for sql in [
            "CREATE TABLE amax (id INT NOT NULL PRIMARY KEY, body VARCHAR(MAX))".to_string(),
            format!("INSERT INTO amax VALUES (1, '{small_chain}')"),
            "ALTER TABLE amax ADD extra INT NULL".to_string(),
        ] {
            let outcome = execute_batch(&storage, &sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let outcome = execute_batch(&storage, "SELECT body, extra FROM amax", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::VarChar(small_chain));
                assert_eq!(rowset.rows[0][1], Datum::Null);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // Case B: a 10k value re-spills to a fresh chain during the ALTER's
        // rewrite (the review found the original rewrite re-inlined and
        // failed; the re-encode now runs inside the statement with
        // spill_max_values, like every other write path).
        let big: String = "b".repeat(10_000);
        let outcome = execute_batch_with_params(
            &storage,
            "CREATE TABLE bmax (id INT NOT NULL PRIMARY KEY, body VARCHAR(MAX)); \
             INSERT INTO bmax VALUES (1, @v);",
            &mut ctx,
            &[RpcParam {
                name: "@v".into(),
                column_type: ColumnType::VarCharMax,
                value: Datum::VarChar(big.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let outcome = execute_batch(&storage, "ALTER TABLE bmax ADD extra INT NULL", &mut ctx);
        assert!(
            outcome.error.is_none(),
            "the rewrite must spill, not re-inline: {:?}",
            outcome.error
        );
        let outcome = execute_batch(
            &storage,
            "SELECT body, extra FROM bmax WHERE id = 1",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::VarChar(big.clone()));
                assert_eq!(rowset.rows[0][1], Datum::Null, "the frozen fill");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // ...and the widened row survives a reopen (the fresh chain is
        // durable with the ALTER's statement).
        drop(ctx);
        drop(storage);
        let storage = Storage::open(path.clone()).expect("reopen");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT body FROM bmax WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::VarChar(big));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let outcome = execute_batch(&storage, "INSERT INTO bmax VALUES (2, 'ok', 5)", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Review PoC: codec edges — empty string, the 256/257 inline threshold,
    /// NULL, and a VARBINARY(MAX) value — all round-trip, including across a
    /// reopen.
    #[test]
    fn max_codec_edges_round_trip() {
        use crate::rel::{
            RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
        };

        let path = unique_temp_path("review-max-edges");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        let at_threshold = "t".repeat(256); // inline boundary
        let over_threshold = "u".repeat(257); // first chained length
        for sql in [
            "CREATE TABLE edges (id INT NOT NULL PRIMARY KEY, v VARCHAR(MAX), b VARBINARY(MAX))"
                .to_string(),
            "INSERT INTO edges VALUES (1, '', NULL)".to_string(),
            format!("INSERT INTO edges VALUES (2, '{at_threshold}', NULL)"),
            format!("INSERT INTO edges VALUES (3, '{over_threshold}', NULL)"),
            "INSERT INTO edges VALUES (4, NULL, NULL)".to_string(),
        ] {
            let outcome = execute_batch(&storage, &sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let blob = vec![0xABu8; 300]; // > 256 -> chain
        let outcome = execute_batch_with_params(
            &storage,
            "INSERT INTO edges VALUES (5, NULL, @b)",
            &mut ctx,
            &[RpcParam {
                name: "@b".into(),
                column_type: ColumnType::VarBinaryMax,
                value: Datum::VarBinary(blob.clone()),
            }],
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);

        let check = |storage: &Storage, ctx: &mut TxnContext| {
            let outcome = execute_batch(storage, "SELECT v, b FROM edges ORDER BY id", ctx);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
            match &outcome.results[0] {
                StatementResult::Rows(rowset) => {
                    assert_eq!(rowset.rows[0][0], Datum::VarChar(String::new()), "empty");
                    assert_eq!(rowset.rows[1][0], Datum::VarChar(at_threshold.clone()));
                    assert_eq!(rowset.rows[2][0], Datum::VarChar(over_threshold.clone()));
                    assert_eq!(rowset.rows[3][0], Datum::Null);
                    assert_eq!(rowset.rows[4][1], Datum::VarBinary(blob.clone()));
                }
                other => panic!("expected rows, got {other:?}"),
            }
        };
        check(&storage, &mut ctx);
        drop(storage);
        let storage = Storage::open(path.clone()).expect("reopen");
        let mut ctx = TxnContext::default();
        check(&storage, &mut ctx);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Offline growth (Stage 14): the data region extends, everything in it
    /// survives, and the grown space is allocatable. Includes a second grow
    /// (the re-run shape an interrupted grow needs).
    #[test]
    fn offline_grow_extends_the_data_region() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("grow");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, v NVARCHAR(MAX))",
            "INSERT INTO g VALUES (1, N'before growth')",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        let old_pages = storage.lock().layout.data_size / PAGE_SIZE as u64;
        drop(ctx);
        drop(storage);

        let new_pages = Storage::grow_data_region(&path, 1).expect("grow");
        assert_eq!(
            new_pages,
            old_pages + (1u64 << 30) / PAGE_SIZE as u64,
            "one GiB of new data pages"
        );

        let storage = Storage::open(path.clone()).expect("reopen after grow");
        assert_eq!(
            storage.lock().layout.data_size / PAGE_SIZE as u64,
            new_pages
        );
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT v FROM g WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::NVarChar("before growth".into()));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        let outcome = execute_batch(&storage, "INSERT INTO g VALUES (2, N'after')", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        drop(ctx);
        drop(storage);

        // Growing again works (the re-run an interrupted grow performs).
        let newer = Storage::grow_data_region(&path, 1).expect("second grow");
        assert_eq!(newer, new_pages + (1u64 << 30) / PAGE_SIZE as u64);
        let storage = Storage::open(path.clone()).expect("reopen after second grow");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT COUNT(*) FROM g", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The safe minimum: a delta below the tail span is refused with the
    /// minimum named; nothing is touched.
    #[test]
    fn grow_refuses_below_the_safe_minimum() {
        let path = unique_temp_path("grow-min");
        // 4 GiB file: the tail regions span ~1.16 GiB, so +1 GiB is unsafe.
        let mut opts = test_storage_options();
        opts.size_gib = 4;
        let storage = Storage::create(path.clone(), opts).expect("create");
        drop(storage);
        let err = Storage::grow_data_region(&path, 1).expect_err("must refuse");
        assert!(
            err.to_string().contains("safe minimum"),
            "names the floor: {err}"
        );
        let storage = Storage::open(path.clone()).expect("file untouched");
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// Growth with a pending WAL (a crash-interrupted transaction) is safe:
    /// the WAL sits before the data region and replays against the moved
    /// bitmap exactly as it would have.
    #[test]
    fn grow_with_pending_wal_recovers_cleanly() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("grow-wal");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, v INT)",
            "INSERT INTO g VALUES (1, 10)",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        // An open transaction dies with the "crash" (drop without commit).
        let outcome = execute_batch(
            &storage,
            "BEGIN TRAN; UPDATE g SET v = 999 WHERE id = 1;",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        drop(ctx);
        drop(storage);

        Storage::grow_data_region(&path, 1).expect("grow with pending WAL");
        let storage = Storage::open(path.clone()).expect("recovery after grow");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT v FROM g WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::Int(10), "the loser is undone");
            }
            other => panic!("expected rows, got {other:?}"),
        }
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// The advisory file lock fences grow against a running server: grow
    /// refuses while the store is open, and works after it closes. (flock is
    /// per open file description, so two opens in one process conflict too.)
    #[test]
    fn grow_refuses_while_the_store_is_open() {
        let path = unique_temp_path("grow-flock");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let err = Storage::grow_data_region(&path, 2).expect_err("must refuse while open");
        assert!(
            err.to_string()
                .contains("locked by another TruthDB process"),
            "names the lock: {err}"
        );
        drop(storage);
        Storage::grow_data_region(&path, 2).expect("grow after close");
        let storage = Storage::open(path.clone()).expect("reopen");
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// REVIEW POC: a crash after the extension writes but BEFORE the header
    /// stamp leaves a longer file under the old header. The file must open
    /// under the old layout, and a re-run of the grow must complete it.
    /// Simulated by saving the original header page before the grow and
    /// writing it back afterwards.
    #[test]
    fn grow_crash_before_header_stamp_is_recoverable() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("grow-crash");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE g (id INT NOT NULL PRIMARY KEY, v INT)",
            "INSERT INTO g VALUES (1, 42)",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        // A real checkpoint makes the descriptor and bitmap copies
        // load-bearing (fresh files have neither on disk).
        storage
            .write_checkpoint(b"crash-window-probe", 1, 2, 1)
            .expect("checkpoint");
        drop(ctx);
        drop(storage);

        // Save the pre-grow header page (the commit point the "crash" loses).
        let old_header = {
            let mut f = std::fs::File::open(&path).expect("open for header save");
            let mut buf = vec![0u8; FILE_HEADER_SIZE];
            f.read_exact(&mut buf).expect("read header");
            buf
        };

        let old_pages = {
            let storage = Storage::open(path.clone()).expect("preflight open");
            let pages = storage.lock().layout.data_size / PAGE_SIZE as u64;
            drop(storage);
            pages
        };

        Storage::grow_data_region(&path, 1).expect("grow");

        // Crash simulation: the extension writes are durable, the header
        // stamp is not.
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open for header restore");
            f.seek(SeekFrom::Start(0)).expect("seek");
            f.write_all(&old_header).expect("restore old header");
            f.sync_all().expect("sync");
        }

        // The old layout must be fully valid: data readable, snapshot loads.
        let storage = Storage::open(path.clone()).expect("open under the OLD header");
        assert_eq!(
            storage.lock().layout.data_size / PAGE_SIZE as u64,
            old_pages,
            "still the old layout"
        );
        let snap = storage
            .load_snapshot()
            .expect("load snapshot under old header")
            .expect("snapshot present");
        assert_eq!(snap.data, b"crash-window-probe");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT v FROM g WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::Int(42));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        drop(ctx);
        drop(storage);

        // Re-running the grow completes it.
        let new_pages = Storage::grow_data_region(&path, 1).expect("re-run grow");
        assert_eq!(new_pages, old_pages + (1u64 << 30) / PAGE_SIZE as u64);
        let storage = Storage::open(path.clone()).expect("open after completed grow");
        assert_eq!(
            storage.lock().layout.data_size / PAGE_SIZE as u64,
            new_pages
        );
        let snap = storage
            .load_snapshot()
            .expect("load snapshot after grow")
            .expect("snapshot survived");
        assert_eq!(snap.data, b"crash-window-probe");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(&storage, "INSERT INTO g VALUES (2, 43)", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// REVIEW POC (teeth): the checkpointed search snapshot must survive a
    /// grow — the descriptor pages are the only pointer to it, and they move.
    /// Fails if the grow skips the descriptor copy.
    #[test]
    fn grow_preserves_the_checkpointed_snapshot() {
        let path = unique_temp_path("grow-snap");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        storage
            .write_checkpoint(b"survives-the-move", 1, 2, 1)
            .expect("checkpoint");
        drop(storage);

        Storage::grow_data_region(&path, 1).expect("grow");

        let storage = Storage::open(path.clone()).expect("reopen");
        let snap = storage
            .load_snapshot()
            .expect("load")
            .expect("snapshot survived the grow");
        assert_eq!(snap.data, b"survives-the-move");
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    /// REVIEW POC (teeth): after a checkpoint the persisted bitmap is the
    /// ONLY record of table extents (the WAL head has advanced past their
    /// alloc records). If the grow loses the bitmap, a reopen sees every page
    /// free and new allocations clobber existing tables. Fails if the grow
    /// skips the bitmap copy.
    #[test]
    fn grow_preserves_the_persisted_allocator_bitmap() {
        use crate::rel::{StatementResult, TxnContext, execute_batch};

        let path = unique_temp_path("grow-bitmap");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v NVARCHAR(60))",
            "INSERT INTO t1 VALUES (1, N'pre-grow row')",
        ] {
            let outcome = execute_batch(&storage, sql, &mut ctx);
            assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        }
        drop(ctx);
        // The checkpoint persists the bitmap and advances the WAL head past
        // t1's extent-alloc records.
        storage
            .write_checkpoint(b"bitmap-probe", 1, 2, 1)
            .expect("checkpoint");
        drop(storage);

        Storage::grow_data_region(&path, 1).expect("grow");

        // Reopen and allocate heavily; with a lost bitmap these allocations
        // land on t1's pages.
        let storage = Storage::open(path.clone()).expect("reopen");
        let mut ctx = TxnContext::default();
        let outcome = execute_batch(
            &storage,
            "CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v NVARCHAR(200))",
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        for batch in 0..20 {
            let mut sql = String::from("INSERT INTO t2 VALUES ");
            for i in 0..50 {
                if i > 0 {
                    sql.push(',');
                }
                let id = batch * 50 + i;
                sql.push_str(&format!("({id}, N'filler row {id} {}')", "x".repeat(120)));
            }
            let outcome = execute_batch(&storage, &sql, &mut ctx);
            assert!(
                outcome.error.is_none(),
                "batch {batch}: {:?}",
                outcome.error
            );
        }
        let outcome = execute_batch(&storage, "SELECT v FROM t1 WHERE id = 1", &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(
                    rowset.rows[0][0],
                    Datum::NVarChar("pre-grow row".into()),
                    "t1 must not be clobbered by post-grow allocations"
                );
            }
            other => panic!("expected rows, got {other:?}"),
        }
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
        let _ = ctx;
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
