use super::super::*;

impl Storage {
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
}

impl StorageFile {
    /// Reads the raw WAL ring bytes for the logical range `[start, end)`,
    /// handling the physical wrap. The caller must have synced the log to at
    /// least `end` first (`DirectFile` bypasses the page cache).
    pub(in crate::storage) fn read_ring_range(
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
    pub(in crate::storage) fn wal_chunk_end(
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
    pub(in crate::storage) fn seed_ring(
        &mut self,
        start_lsn: u64,
        bytes: &[u8],
    ) -> Result<u64, StorageError> {
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
    pub(in crate::storage) fn apply_wal_stream_locked(
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
