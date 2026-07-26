use super::*;

impl Storage {
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

    pub fn load_snapshot(&self) -> Result<Option<SnapshotData>, StorageError> {
        self.lock().load_snapshot()
    }
}

impl StorageFile {
    /// The WAL LSN a checkpoint may truncate up to: the oldest open transaction's
    /// BEGIN LSN (so its undo records survive), or the WAL tail if none is open.
    pub(super) fn checkpoint_wal_head(&self) -> u64 {
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

    /// Applies `ALTER DATABASE SET` option changes and persists them durably
    /// in both superblocks (generation-bumped, active slot first with an
    /// fsync between — a torn first write falls back to the backup with the
    /// old options, and the un-acknowledged ALTER is simply lost).
    pub(super) fn set_db_options(
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

    pub(super) fn write_checkpoint(
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
    pub(super) fn write_snapshot_descriptor(
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
    pub(super) fn finish_checkpoint(
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
}
