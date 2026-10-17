mod catalog;
mod dml;
mod recovery;
pub(super) mod security;
mod transaction;

use super::*;

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
        Storage::open(path).map(Self::from_core)
    }

    pub fn create(path: PathBuf, opts: StorageOptions) -> Result<Self, StorageError> {
        Storage::create(path, opts).map(Self::from_core)
    }

    /// Test hook: create with custom WAL ring bounds.
    #[cfg(test)]
    pub(crate) fn create_with_wal_bounds(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
    ) -> Result<Self, StorageError> {
        Storage::create_with_wal_bounds(path, opts, wal_min_bytes, wal_max_bytes)
            .map(Self::from_core)
    }

    /// The shared core, for the subsystems that run on the log alone
    /// (replication).
    pub fn core_arc(&self) -> Arc<Storage> {
        Arc::clone(&self.core)
    }
}
