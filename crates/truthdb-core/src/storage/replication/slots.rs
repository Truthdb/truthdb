use super::super::*;

impl Storage {
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
    /// `max_slot_retain_bytes` — it no longer pins the truncation floor, so the
    /// ring can advance past it (the standby must reseed). Run at the start of a
    /// checkpoint, before the floor is computed. A no-op under the default
    /// unlimited retention.
    pub(in crate::storage) fn reap_stale_slots(&mut self) {
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
    pub(in crate::storage) fn try_register_repl_slot(
        &mut self,
        id: u32,
        lsn: u64,
    ) -> Result<(), StorageError> {
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
    pub(in crate::storage) fn is_wal_entry_boundary(
        &mut self,
        lsn: u64,
    ) -> Result<bool, StorageError> {
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
    pub(in crate::storage) fn advance_repl_slot(&mut self, id: u32, lsn: u64) {
        let lsn = lsn.min(self.wal.tail());
        if let Some(held) = self.truncation_gate.repl_slots.get_mut(&id) {
            *held = (*held).max(lsn);
        }
    }

    #[cfg(test)]
    pub(in crate::storage) fn drop_repl_slot(&mut self, id: u32) {
        self.truncation_gate.repl_slots.remove(&id);
    }

    #[cfg(test)]
    pub(in crate::storage) fn repl_slot_lsn(&self, id: u32) -> Option<u64> {
        self.truncation_gate.repl_slots.get(&id).copied()
    }
}
