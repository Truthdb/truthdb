mod catalog;
mod dml;
mod recovery;
pub(super) mod security;
mod transaction;

use super::*;

/// The engine every relational open, create and validating reopen recovers with.
pub(in crate::storage) fn rel_engine() -> Box<dyn EngineParticipant> {
    Box::new(RelEngine::new())
}

impl RelStorage {
    /// Wraps an opened core, seeding the lock-free option mirrors from the
    /// recovered state.
    fn from_core(core: Storage) -> Self {
        let (rcsi, allow_snapshot, recovery_full) = {
            let mut guard = core.lock();
            let recovery_full = guard.recovery_full;
            let rel = guard.rel();
            (rel.version.rcsi, rel.version.allow_snapshot, recovery_full)
        };
        RelStorage {
            core: Arc::new(core),
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
        }
    }

    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        Storage::open(path, rel_engine).map(Self::from_core)
    }

    pub fn create(path: PathBuf, opts: StorageOptions) -> Result<Self, StorageError> {
        Storage::create(path, opts, rel_engine).map(Self::from_core)
    }

    /// Test hook: create with custom WAL ring bounds.
    #[cfg(test)]
    pub(crate) fn create_with_wal_bounds(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
    ) -> Result<Self, StorageError> {
        Storage::create_with_wal_bounds(path, opts, wal_min_bytes, wal_max_bytes, rel_engine)
            .map(Self::from_core)
    }

    /// The shared core, for the subsystems that run on the log alone
    /// (replication).
    pub fn core_arc(&self) -> Arc<Storage> {
        Arc::clone(&self.core)
    }

    /// Offline restore of a full backup into a fresh file.
    pub fn restore_full(dst_path: &Path, bak_path: &Path) -> Result<(), StorageError> {
        Storage::restore_full(dst_path, bak_path, rel_engine)
    }

    pub fn restore_full_with_logs(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
        stop_at: Option<u64>,
    ) -> Result<(), StorageError> {
        Storage::restore_full_with_logs(dst_path, bak_path, log_paths, stop_at, rel_engine)
    }

    pub fn restore_full_standby(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
    ) -> Result<(), StorageError> {
        Storage::restore_full_standby(dst_path, bak_path, log_paths, rel_engine)
    }

    pub fn restore_full_with_wal_ranges(
        dst_path: &Path,
        bak_path: &Path,
        wal_ranges: &[(u64, Vec<u8>)],
    ) -> Result<(), StorageError> {
        Storage::restore_full_with_wal_ranges(dst_path, bak_path, wal_ranges, rel_engine)
    }

    /// Offline standby promotion; returns the new epoch.
    pub fn promote(path: &Path) -> Result<u64, StorageError> {
        Storage::promote(path, rel_engine)
    }
}
