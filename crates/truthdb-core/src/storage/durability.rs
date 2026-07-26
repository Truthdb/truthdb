use super::*;

impl Storage {
    /// Blocks until the WAL is fsync-durable up to `target` — the tail past a
    /// committed record. The executor calls this once per batch that committed,
    /// so one log-writer fsync makes many concurrent commits durable. With
    /// synchronous commit armed, the commit then also waits for a standby's
    /// acknowledgement of `target` — unless the link is already degraded
    /// (NOT_SYNCHRONIZED), in which case commits proceed on local durability
    /// alone until a standby catches back up.
    pub(crate) fn ensure_durable(&self, target: u64) -> Result<(), StorageError> {
        self.gc.ensure_durable(target)?;
        self.sync_commit.wait_for_ack(target);
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

    pub fn wal_usage_ratio(&self) -> f64 {
        self.lock().wal_usage_ratio()
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

    pub(super) fn ensure_rel_usable(&self) -> Result<(), StorageError> {
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
    pub(super) fn wedge(&mut self) {
        self.rel.wedged = true;
    }

    pub(super) fn append_wal_entry(
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
}
