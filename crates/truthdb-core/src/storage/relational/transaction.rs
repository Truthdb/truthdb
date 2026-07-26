use super::super::*;

impl Storage {
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
    pub(in crate::storage) fn bump_lock_epoch(&self) {
        self.lock_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// The lock-analysis epoch: parked batches analyzed under an older value
    /// are re-analyzed before grant (see the scheduler).
    pub(crate) fn lock_analysis_epoch(&self) -> u64 {
        self.lock_epoch.load(std::sync::atomic::Ordering::Acquire)
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
            guard.rel.all_tables().map(|def| def.object_id).collect();
        guard.version.prune(watermark, &alive);
    }

    /// Test observability: version chains held for `table`.
    #[cfg(test)]
    pub(crate) fn version_chain_count(&self, table: &str) -> usize {
        let guard = self.lock();
        guard
            .rel
            .table(catalog::DEFAULT_DATABASE_ID, table)
            .map_or(0, |def| guard.version.chain_count(def.object_id))
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
        db_id: u32,
        name: &str,
        count: usize,
    ) -> Result<Option<i64>, StorageError> {
        self.lock().rel_reserve_identity(db_id, name, count)
    }

    pub(crate) fn rel_set_check_constraints(
        &self,
        db_id: u32,
        name: &str,
        check_constraints: Vec<catalog::CheckDef>,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_set_check_constraints(db_id, name, check_constraints)
    }

    pub(crate) fn rel_set_foreign_keys(
        &self,
        db_id: u32,
        name: &str,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.lock().rel_set_foreign_keys(db_id, name, foreign_keys)
    }

    #[cfg(test)]
    pub(crate) fn rel_insert_without_commit(
        &self,
        name: &str,
        values: Vec<Datum>,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_insert_without_commit(catalog::DEFAULT_DATABASE_ID, name, values)
    }

    #[cfg(test)]
    pub(crate) fn rel_flush_pool_only(&self) -> Result<(), StorageError> {
        self.lock().rel_flush_pool_only()
    }
}

impl StorageFile {
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
        db_id: u32,
        name: &str,
        count: usize,
    ) -> Result<Option<i64>, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let mut def = self
            .rel
            .table(db_id, name)
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
            self.rel.cache_table(def);
        }
        Ok(Some(first))
    }

    /// Replaces a table's CHECK constraints (ALTER TABLE ADD/DROP CONSTRAINT)
    /// and persists the mutated catalog row. Undoable within its own statement.
    pub(crate) fn rel_set_check_constraints(
        &mut self,
        db_id: u32,
        name: &str,
        check_constraints: Vec<catalog::CheckDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let mut def = self
            .rel
            .table(db_id, name)
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
        self.rel.cache_table(def);
        Ok(())
    }

    /// Replaces a table's FOREIGN KEY constraints (ALTER TABLE ADD/DROP
    /// CONSTRAINT) and persists the mutated catalog row.
    pub(crate) fn rel_set_foreign_keys(
        &mut self,
        db_id: u32,
        name: &str,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let mut def = self
            .rel
            .table(db_id, name)
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
        self.rel.cache_table(def);
        Ok(())
    }

    /// Test hook: run an insert's ops durably but never commit — the state a
    /// crash mid-statement leaves behind (loser transaction for recovery).
    #[cfg(test)]
    pub(crate) fn rel_insert_without_commit(
        &mut self,
        db_id: u32,
        name: &str,
        values: Vec<Datum>,
    ) -> Result<(), StorageError> {
        let (def, schema) = self.rel_def(db_id, name)?;
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

    pub(in crate::storage) fn rel_def(
        &self,
        db_id: u32,
        name: &str,
    ) -> Result<(TableDef, Schema), StorageError> {
        let def = self
            .rel
            .table(db_id, name)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{name}'")))?;
        let schema = def.schema()?;
        Ok((def, schema))
    }

    /// Builds the relational execution context over this file's parts.
    pub(in crate::storage) fn rel_ctx(&mut self) -> RelCtx<'_> {
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
            container: self.current_container,
        }
    }

    /// Runs one autocommit relational statement: begin, ops, commit (force
    /// log); statement failure rolls back through the in-memory undo log.
    pub(in crate::storage) fn rel_statement<T>(
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
    pub(in crate::storage) fn rel_statement_scoped<T>(
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
    pub(in crate::storage) fn begin_txn(&mut self) -> Result<StorageTxn, StorageError> {
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
    pub(in crate::storage) fn commit_txn(&mut self, stx: StorageTxn) -> Result<(), StorageError> {
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
    pub(in crate::storage) fn rollback_txn(
        &mut self,
        mut stx: StorageTxn,
    ) -> Result<(), StorageError> {
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
    pub(in crate::storage) fn rollback_txn_to(
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
    pub(in crate::storage) fn has_active_transactions(&self) -> bool {
        !self.rel.active_txn_begins.is_empty()
    }
}
