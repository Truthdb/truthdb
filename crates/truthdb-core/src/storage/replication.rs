/// D2 synchronous-commit coordination (primary side). Committers wait on
/// `acked` (a standby's highest acknowledged durable LSN, published by the
/// sender's ack reader) after their local fsync. Availability-first: a wait
/// that exceeds the armed timeout degrades the link (`degraded = true`,
/// logged once) and every commit passes straight through until an
/// acknowledgement catches up to the primary's durable watermark, which
/// re-synchronizes the link (logged once).
pub(super) struct SyncCommitState {
    armed: std::sync::atomic::AtomicBool,
    timeout_ms: std::sync::atomic::AtomicU64,
    degraded: std::sync::atomic::AtomicBool,
    /// The LSN whose acknowledgement re-synchronizes a degraded link: the
    /// target of the wait that timed out. Comparing against the LIVE durable
    /// watermark instead would latch the degradation forever under sustained
    /// writes — an ack always trails the live watermark by one round trip.
    resync_target: std::sync::atomic::AtomicU64,
    acked: std::sync::Mutex<u64>,
    acked_cv: std::sync::Condvar,
}

impl Default for SyncCommitState {
    fn default() -> Self {
        SyncCommitState {
            armed: std::sync::atomic::AtomicBool::new(false),
            timeout_ms: std::sync::atomic::AtomicU64::new(0),
            degraded: std::sync::atomic::AtomicBool::new(false),
            resync_target: std::sync::atomic::AtomicU64::new(0),
            acked: std::sync::Mutex::new(0),
            acked_cv: std::sync::Condvar::new(),
        }
    }
}

impl SyncCommitState {
    fn arm(&self, timeout: std::time::Duration) {
        self.timeout_ms.store(
            timeout.as_millis().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.armed.store(true, std::sync::atomic::Ordering::Release);
    }

    fn publish(&self, lsn: u64) {
        use std::sync::atomic::Ordering;
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        {
            let mut acked = self.acked.lock().expect("sync-commit state poisoned");
            if lsn > *acked {
                *acked = lsn;
            }
        }
        self.acked_cv.notify_all();
        // Re-synchronize once the acknowledgement covers the wait that
        // degraded the link — the incident point, NOT the live watermark
        // (which a loaded primary keeps permanently ahead of any ack).
        if self.degraded.load(Ordering::Acquire)
            && lsn >= self.resync_target.load(Ordering::Acquire)
            && self.degraded.swap(false, Ordering::AcqRel)
        {
            eprintln!(
                "synchronous commit: a standby caught up (acknowledged {lsn}); the link is \
                 SYNCHRONIZED again"
            );
        }
    }

    /// Waits for an acknowledgement covering `target`, honoring the
    /// availability-first timeout. Never returns an error: degradation is a
    /// logged state change, not a commit failure.
    pub(super) fn wait_for_ack(&self, target: u64) {
        use std::sync::atomic::Ordering;
        if !self.armed.load(Ordering::Acquire) || self.degraded.load(Ordering::Acquire) {
            return;
        }
        let timeout =
            std::time::Duration::from_millis(self.timeout_ms.load(Ordering::Relaxed).max(1));
        let deadline = std::time::Instant::now() + timeout;
        let mut acked = self.acked.lock().expect("sync-commit state poisoned");
        while *acked < target {
            // A concurrent timeout already degraded the link: stop waiting.
            if self.degraded.load(Ordering::Acquire) {
                return;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                self.resync_target.store(target, Ordering::Release);
                if !self.degraded.swap(true, Ordering::AcqRel) {
                    eprintln!(
                        "synchronous commit: no standby acknowledged LSN {target} within \
                         {timeout:?} — the link is NOT_SYNCHRONIZED; commits proceed on \
                         local durability alone until a standby catches up"
                    );
                }
                // Wake concurrently parked committers: the link is degraded,
                // they must stop waiting too.
                drop(acked);
                self.acked_cv.notify_all();
                return;
            }
            let (guard, _timed_out) = self
                .acked_cv
                .wait_timeout(acked, deadline - now)
                .expect("sync-commit state poisoned");
            acked = guard;
        }
    }
}

use super::*;

impl Storage {
    /// Arms D2 synchronous commit: every commit waits (after local durability)
    /// for a standby `FlushAck` at or past its target. `timeout` is the
    /// availability-first knob: a commit that waits longer marks the link
    /// NOT_SYNCHRONIZED and proceeds — as do all commits after it — until a
    /// standby's acknowledgements catch back up to the durable watermark.
    pub fn arm_sync_commit(&self, timeout: std::time::Duration) {
        self.sync_commit.arm(timeout);
    }

    /// Whether the synchronous-commit link is degraded (NOT_SYNCHRONIZED).
    #[cfg(test)]
    pub(crate) fn sync_commit_degraded(&self) -> bool {
        self.sync_commit
            .degraded
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Publishes a standby's acknowledged durable LSN (the sender's ack path
    /// calls this beside the slot advance). Re-synchronizes the link when the
    /// acknowledgement has caught up to the primary's durable watermark.
    pub(crate) fn publish_sync_ack(&self, acked: u64) {
        self.sync_commit.publish(acked);
    }

    /// The current WAL tail — the durability target for a batch that committed.
    pub(crate) fn wal_tail(&self) -> u64 {
        self.lock().wal_tail()
    }

    /// The current WAL head (the reclaim floor) — for tests that verify the
    /// FULL-model log-backup hold pins truncation.
    pub(crate) fn wal_head(&self) -> u64 {
        self.lock().wal.head()
    }

    /// Offline PROMOTION of a replication standby to a read-write primary
    /// (manual failover). The server must be stopped — the advisory file lock
    /// fails this fast if it is not. Two phases, each crash-safe:
    ///
    /// 1. Flip the superblocks: clear `is_standby` and bump the epoch by one,
    ///    in a single dual-write (active slot first, fsync between — a torn
    ///    write falls back to the still-standby backup slot and the promote is
    ///    simply retried). The epoch bump fences the old timeline: the
    ///    handshake accepts only EQUAL epochs, so the old primary — whose log
    ///    may hold records this timeline never had — cannot rejoin without a
    ///    reseed, and neither can standbys seeded before the failover.
    /// 2. A validating [`Storage::open`]: with the standby flag gone, recovery
    ///    runs IN FULL — redo, then undo of the shipped in-flight transactions
    ///    (with CLRs, on the now-writable WAL). This is where "drain, finish
    ///    redo+undo" happens; a crash mid-recovery re-runs it on the next open.
    ///
    /// Returns the new epoch.
    pub fn promote(path: &Path) -> Result<u64, StorageError> {
        let new_epoch = {
            let mut file = DirectFile::open_existing(path.to_path_buf())?;
            let mut header_bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
            file.read_exact_at(0, &mut header_bytes)?;
            let header = FileHeader::from_le_bytes(&header_bytes);
            if header.magic != crate::storage_layout::FILE_MAGIC {
                return Err(StorageError::InvalidFile("bad magic".to_string()));
            }
            if header.header_checksum != header.compute_checksum() {
                return Err(StorageError::InvalidFile(
                    "header checksum mismatch".to_string(),
                ));
            }
            if header.version != crate::storage_layout::FILE_VERSION {
                return Err(StorageError::InvalidFile(format!(
                    "file version {} is not promotable (open it with the server once to \
                     upgrade, then retry)",
                    header.version
                )));
            }
            let mut sb_a_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
            file.read_exact_at(header.superblock_a_offset, &mut sb_a_bytes)?;
            let superblock_a = Superblock::from_le_bytes(&sb_a_bytes);
            let sb_a_valid = superblock_a.checksum == superblock_a.compute_checksum();
            let mut sb_b_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
            file.read_exact_at(header.superblock_b_offset, &mut sb_b_bytes)?;
            let superblock_b = Superblock::from_le_bytes(&sb_b_bytes);
            let sb_b_valid = superblock_b.checksum == superblock_b.compute_checksum();
            if !sb_a_valid && !sb_b_valid {
                return Err(StorageError::InvalidFile(
                    "both superblocks have checksum mismatch".to_string(),
                ));
            }
            let active_slot = ActiveSuperblock::from_superblocks(
                &superblock_a,
                &superblock_b,
                sb_a_valid,
                sb_b_valid,
            );
            let (active, primary_offset, backup_flag, backup_offset) = match active_slot {
                ActiveSuperblock::A => (
                    superblock_a,
                    header.superblock_a_offset,
                    SUPERBLOCK_ACTIVE_B,
                    header.superblock_b_offset,
                ),
                ActiveSuperblock::B => (
                    superblock_b,
                    header.superblock_b_offset,
                    SUPERBLOCK_ACTIVE_A,
                    header.superblock_a_offset,
                ),
            };
            if !active.is_standby() {
                // A crash between promote's two superblock writes leaves the
                // ACTIVE slot promoted and the backup slot still standby: the
                // promotion durably won (the active slot's higher generation
                // decides), so FINISH it — rewrite the backup slot to match —
                // rather than telling the operator their failover failed. (An
                // in-place active-slot rewrite tearing later could otherwise
                // fall back to the stale standby state.)
                let backup_slot = match active_slot {
                    ActiveSuperblock::A => superblock_b,
                    ActiveSuperblock::B => superblock_a,
                };
                if backup_slot.is_standby() {
                    let mut backup = active;
                    backup.active = backup_flag;
                    backup.checksum = backup.compute_checksum();
                    file.write_all_at(backup_offset, &backup.to_le_bytes_with_checksum())?;
                    file.sync_data()?;
                    drop(file);
                    drop(Storage::open(path.to_path_buf())?);
                    return Ok(active.epoch());
                }
                return Err(StorageError::InvalidConfig(format!(
                    "this database is not a replication standby; nothing to promote (if a \
                     previous promote was interrupted, it already took effect — the current \
                     replication epoch is {}; start the server normally)",
                    active.epoch()
                )));
            }
            // Promotion's undo appends CLRs for every shipped in-flight
            // transaction, and undo volume is roughly the loser records'
            // forward volume. Refuse while the retained ring exceeds half its
            // size: below that, headroom (>= half the ring) covers the worst
            // case (every retained byte a loser). The remedy is real — the
            // standby restartpoints at 50% usage, so this fires only right
            // after a large catch-up; start the standby server briefly (its
            // maintenance thread restartpoints) and retry.
            let occupancy = active.wal_tail.saturating_sub(active.wal_head);
            if occupancy > header.wal_size / 2 {
                return Err(StorageError::InvalidConfig(format!(
                    "the standby's retained WAL ({occupancy} bytes) exceeds half its ring \
                     ({} bytes): promotion's undo could run the ring full and leave the \
                     file unopenable. Start the standby server briefly (it will take a \
                     restartpoint) and retry the promote",
                    header.wal_size
                )));
            }
            let generation = superblock_a
                .generation
                .max(superblock_b.generation)
                .saturating_add(1);
            let new_epoch = active.epoch().saturating_add(1);
            let mut primary = active;
            let mut backup = active;
            backup.active = backup_flag;
            for sb in [&mut primary, &mut backup] {
                sb.set_standby(false);
                sb.set_epoch(new_epoch);
                sb.generation = generation;
                sb.checksum = sb.compute_checksum();
            }
            file.write_all_at(primary_offset, &primary.to_le_bytes_with_checksum())?;
            file.sync_data()?;
            file.write_all_at(backup_offset, &backup.to_le_bytes_with_checksum())?;
            file.sync_data()?;
            new_epoch
            // The file handle (and its advisory lock) drops here.
        };
        // Validate + finalize: a full open runs redo AND undo (the standby flag
        // is gone), sealing the shipped in-flight transactions with CLRs.
        drop(Storage::open(path.to_path_buf())?);
        Ok(new_epoch)
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
    /// recovered.
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

    /// Joins the connected-standby registry (one live sender per node id).
    /// Returns false if the id is already connected.
    pub(crate) fn try_join_repl_node(&self, id: u32) -> bool {
        self.repl_connected
            .lock()
            .expect("repl-connected set poisoned")
            .insert(id)
    }

    /// Leaves the connected-standby registry.
    pub(crate) fn leave_repl_node(&self, id: u32) {
        self.repl_connected
            .lock()
            .expect("repl-connected set poisoned")
            .remove(&id);
    }

    /// Node ids with a live sender (for the monitoring DMVs).
    pub(crate) fn repl_connected_nodes(&self) -> std::collections::HashSet<u32> {
        self.repl_connected
            .lock()
            .expect("repl-connected set poisoned")
            .clone()
    }

    /// The registered replication slots `(id, held LSN)` (for the monitoring
    /// DMVs).
    pub(crate) fn repl_slots_snapshot(&self) -> Vec<(u32, u64)> {
        self.lock()
            .truncation_gate
            .repl_slots
            .iter()
            .map(|(&id, &lsn)| (id, lsn))
            .collect()
    }

    /// Synchronous-commit status: `None` when not armed, else whether the link
    /// is currently degraded (NOT_SYNCHRONIZED).
    pub(crate) fn sync_commit_status(&self) -> Option<bool> {
        use std::sync::atomic::Ordering;
        self.sync_commit
            .armed
            .load(Ordering::Acquire)
            .then(|| self.sync_commit.degraded.load(Ordering::Acquire))
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
}

impl StorageFile {
    /// Maintains the standby's active-transaction table as shipped records are
    /// applied — the SAME begin/resolve rules recovery's analysis uses
    /// (`analyze_and_redo`): TXN_BEGIN opens at its own LSN, TXN_COMMIT and
    /// TXN_END resolve. A page op for a transaction whose BEGIN was not seen
    /// cannot arise (a chunk never precedes a transaction's BEGIN — chunks
    /// apply in order and BEGIN is its first record); the defensive
    /// `or_insert` still clamps the floor at that record if it ever did.
    pub(super) fn standby_track_rel_record(&mut self, lsn: u64, record: &RelRecord) {
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
    /// Readable standby: mirrors a fully-redone shipped range into the version
    /// store — the pre-image of every row change is already in the record's
    /// UNDO payload, so `publish` + `record_commit` reproduce exactly what the
    /// primary's own commit path builds. Uncommitted (in-flight) writers have
    /// no commit seq, so a snapshot reader at the last-applied-commit sequence
    /// resolves past their chain heads to the pre-image — the committed state.
    /// CLRs pop and unpublish the compensated suffix (savepoint/statement
    /// rollbacks inside a transaction that later commits); a commit-less
    /// TXN_END unwinds the rest (a full abort). Heap undo payloads are CELL
    /// bytes — decoded (tag stripped, moved rows re-homed) before publishing.
    /// Only live TABLE objects get chains (index-maintenance undos carry the
    /// index's object id; nothing resolves index chains, and pruning clears
    /// them), a page freed anywhere in the range is skipped (its historical
    /// owner is not derivable from the post-range header), and a DDL in the
    /// range stamps every live table so an older pinned snapshot fails 3961
    /// instead of decoding rows under the wrong schema. Per-record failures
    /// are logged and skipped — a capture problem must not wedge the stream
    /// (the cost is one row's pre-image, not divergence).
    pub(super) fn standby_capture_versions(&mut self, records: &[(u64, RelRecord)]) {
        use crate::wal::records::{
            REL_KIND_CLR, REL_KIND_FREE_EXTENT, REL_KIND_PAGE_IMAGE, REL_KIND_PAGE_OP,
            REL_KIND_SET_CATALOG_ROOT, REL_KIND_TXN_COMMIT, REL_KIND_TXN_END,
        };
        let alive: std::collections::HashSet<u32> =
            self.rel.all_tables().map(|def| def.object_id).collect();
        let mut freed_pages: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (_, record) in records {
            if record.kind == REL_KIND_FREE_EXTENT
                && let Ok((start, pages)) = record.decode_extent_redo()
            {
                for page in start..start.saturating_add(pages) {
                    freed_pages.insert(page);
                }
            }
        }
        let mut catalog_changed = false;
        for (lsn, record) in records {
            if *lsn < self.standby_version_floor {
                continue;
            }
            match record.kind {
                REL_KIND_PAGE_OP | REL_KIND_PAGE_IMAGE if record.txn_id != 0 => {
                    let pending = self
                        .standby_pending_version(record, &alive, &freed_pages)
                        .unwrap_or_else(|err| {
                            eprintln!(
                                "standby version capture: skipping record at LSN {lsn}: {err}"
                            );
                            None
                        });
                    let published = pending.map(|p| self.version.publish(p, record.txn_id));
                    self.standby_published
                        .entry(record.txn_id)
                        .or_default()
                        .push((*lsn, published));
                }
                REL_KIND_CLR if record.txn_id != 0 => {
                    // The CLR's redo opens with the `undo_next` LSN: everything
                    // the transaction logged ABOVE it is now compensated.
                    if record.redo.len() >= 8 {
                        let undo_next = u64::from_le_bytes(record.redo[0..8].try_into().unwrap());
                        if let Some(stack) = self.standby_published.get_mut(&record.txn_id) {
                            while stack.last().is_some_and(|(l, _)| *l > undo_next) {
                                if let Some((_, Some(rec))) = stack.pop() {
                                    self.version.unpublish(rec, record.txn_id);
                                }
                            }
                        }
                    }
                }
                REL_KIND_TXN_COMMIT => {
                    self.version.record_commit(record.txn_id, *lsn);
                    self.standby_published.remove(&record.txn_id);
                }
                REL_KIND_TXN_END => {
                    if let Some(stack) = self.standby_published.remove(&record.txn_id) {
                        for (_, published) in stack.into_iter().rev() {
                            if let Some(rec) = published {
                                self.version.unpublish(rec, record.txn_id);
                            }
                        }
                    }
                }
                REL_KIND_SET_CATALOG_ROOT => catalog_changed = true,
                _ => {}
            }
        }
        if catalog_changed {
            // A shipped DDL cannot wait out pinned snapshots the way the
            // primary's Database X does; fence them instead (3961 on the next
            // access), for every live table — conservative and correct.
            for object_id in alive {
                self.version.stamp_schema(object_id);
            }
        }
    }

    /// Builds the version-store change for one shipped row op, or `None` for
    /// structural/system/foreign records.
    pub(super) fn standby_pending_version(
        &mut self,
        record: &RelRecord,
        alive: &std::collections::HashSet<u32>,
        freed_pages: &std::collections::HashSet<u64>,
    ) -> Result<Option<crate::relstore::version::PendingVersion>, StorageError> {
        use crate::relstore::version::{PendingVersion, RowChange};
        use crate::wal::records::PageOpUndo;
        type HeapChange = Option<(u32, Vec<u8>, Option<Vec<u8>>)>;
        let heap_change = |this: &mut Self,
                           page: u64,
                           slot: u16,
                           cell: Option<Vec<u8>>|
         -> Result<HeapChange, StorageError> {
            if freed_pages.contains(&page) {
                return Ok(None);
            }
            let Some(object_id) = this.heap_page_object_id(page)? else {
                return Ok(None);
            };
            if !alive.contains(&object_id) {
                return Ok(None);
            }
            match cell {
                None => Ok(Some((
                    object_id,
                    rid_identity(crate::relstore::heap::Rid { page, slot }),
                    None,
                ))),
                Some(cell) => {
                    // The undo payload is a heap CELL: strip the tag, and home
                    // a MOVED copy's identity to the RID readers scan under.
                    let Some((home, row)) = crate::relstore::heap::cell_row(&cell) else {
                        return Ok(None); // a stub — a pointer, not a row
                    };
                    let identity =
                        rid_identity(home.unwrap_or(crate::relstore::heap::Rid { page, slot }));
                    Ok(Some((object_id, identity, Some(row.to_vec()))))
                }
            }
        };
        let pending =
            match record.decode_page_op_undo()? {
                PageOpUndo::TreeDeleteKey { object_id, key } if alive.contains(&object_id) => {
                    Some(PendingVersion {
                        object_id,
                        identity: key,
                        change: RowChange::Insert,
                    })
                }
                PageOpUndo::TreeInsertRow {
                    object_id,
                    key,
                    row,
                } if alive.contains(&object_id) => Some(PendingVersion {
                    object_id,
                    identity: key,
                    change: RowChange::Delete { prior: row },
                }),
                PageOpUndo::TreeUpdateRow {
                    object_id,
                    key,
                    row,
                } if alive.contains(&object_id) => Some(PendingVersion {
                    object_id,
                    identity: key,
                    change: RowChange::Update { prior: row },
                }),
                PageOpUndo::HeapDeleteSlot { page, slot } => heap_change(self, page, slot, None)?
                    .map(|(object_id, identity, _)| PendingVersion {
                        object_id,
                        identity,
                        change: RowChange::Insert,
                    }),
                PageOpUndo::HeapInsertRow { page, slot, bytes } => {
                    heap_change(self, page, slot, Some(bytes))?.map(|(object_id, identity, row)| {
                        PendingVersion {
                            object_id,
                            identity,
                            change: RowChange::Delete {
                                prior: row.expect("cell decoded"),
                            },
                        }
                    })
                }
                PageOpUndo::HeapUpdateRow { page, slot, bytes } => {
                    heap_change(self, page, slot, Some(bytes))?.map(|(object_id, identity, row)| {
                        PendingVersion {
                            object_id,
                            identity,
                            change: RowChange::Update {
                                prior: row.expect("cell decoded"),
                            },
                        }
                    })
                }
                _ => None,
            };
        Ok(pending)
    }

    /// The owning object of a heap page, from its self-identifying header
    /// (`None` for a page that is not heap-formatted — a stale undo against a
    /// since-freed page).
    pub(super) fn heap_page_object_id(&mut self, page: u64) -> Result<Option<u32>, StorageError> {
        let mut ctx = self.rel_ctx();
        let frame = ctx.fetch(page)?;
        let header = crate::relstore::page::read_header(ctx.pool.page(frame));
        ctx.pool.unpin(frame);
        Ok((header.page_type == crate::relstore::page::PAGE_TYPE_HEAP).then_some(header.object_id))
    }

    /// `max_slot_retain_bytes` — it no longer pins the truncation floor, so the
    /// ring can advance past it (the standby must reseed). Run at the start of a
    /// checkpoint, before the floor is computed. A no-op under the default
    /// unlimited retention.
    pub(super) fn reap_stale_slots(&mut self) {
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
    pub(super) fn try_register_repl_slot(&mut self, id: u32, lsn: u64) -> Result<(), StorageError> {
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
    pub(super) fn is_wal_entry_boundary(&mut self, lsn: u64) -> Result<bool, StorageError> {
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
    pub(super) fn advance_repl_slot(&mut self, id: u32, lsn: u64) {
        let lsn = lsn.min(self.wal.tail());
        if let Some(held) = self.truncation_gate.repl_slots.get_mut(&id) {
            *held = (*held).max(lsn);
        }
    }

    #[cfg(test)]
    pub(super) fn drop_repl_slot(&mut self, id: u32) {
        self.truncation_gate.repl_slots.remove(&id);
    }

    #[cfg(test)]
    pub(super) fn repl_slot_lsn(&self, id: u32) -> Option<u64> {
        self.truncation_gate.repl_slots.get(&id).copied()
    }

    /// Reads the raw WAL ring bytes for the logical range `[start, end)`,
    /// handling the physical wrap. The caller must have synced the log to at
    /// least `end` first (`DirectFile` bypasses the page cache).
    pub(super) fn read_ring_range(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, StorageError> {
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
    pub(super) fn wal_chunk_end(
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

    /// Writes the shipped WAL bytes into the ring at their physical positions,
    /// handling the ring wrap (restore). Returns `start_lsn + bytes.len()`, the
    /// restored `backup_end`.
    pub(super) fn seed_ring(&mut self, start_lsn: u64, bytes: &[u8]) -> Result<u64, StorageError> {
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

    /// The live standby apply (see [`Storage::apply_wal_stream`]). Runs under the
    /// storage lock held by the caller.
    pub(super) fn apply_wal_stream_locked(
        &mut self,
        from_lsn: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
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
        // Readable standby: mirror the range into the version store (pre-images
        // from the undo payloads), so snapshot reads at the last-applied-commit
        // sequence see only committed state. Same post-redo discipline: a
        // failed apply must feed the store nothing.
        self.version.standby_reads = true;
        self.standby_capture_versions(&records);
        self.standby_version_floor = advanced;
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
}
