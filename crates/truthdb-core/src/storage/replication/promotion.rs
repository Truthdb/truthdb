use super::super::*;

impl Storage {
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
}
