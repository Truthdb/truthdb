use super::*;

impl StorageFile {
    /// Decodes the page-structured (REL) records, with their LSNs, from the
    /// recovery scan.
    pub fn rel_records(&self) -> Result<Vec<(u64, RelRecord)>, StorageError> {
        self.replay_cache
            .iter()
            .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
            .map(|record| Ok((record.logical_ts, RelRecord::decode(&record.payload)?)))
            .collect()
    }

    /// A page context over this file's pool, WAL and allocator. `container`
    /// is the tag stamped into forward page-scoped records it appends.
    pub fn page_ctx(&mut self, container: u16) -> RelCtx<'_> {
        RelCtx {
            pool: &mut self.pages.pool,
            io: PoolIo {
                file: &mut self.file,
                wal: &mut self.wal,
                data_offset: self.layout.data_offset,
                data_pages: self.layout.data_size / PAGE_SIZE as u64,
            },
            allocator: &mut self.allocator,
            dpt: &mut self.pages.dpt,
            use_reserve: false,
            container,
        }
    }
}

impl Locked {
    /// ARIES restart: analysis + redo (repeating history), then undo of
    /// loser transactions with CLRs. The engine reloads its caches between
    /// redo and undo (undo needs tree roots) and after (undo may have removed
    /// catalog rows).
    pub(in crate::storage) fn recover(
        &mut self,
        stop_at: Option<u64>,
        redo_only: bool,
    ) -> Result<(), StorageError> {
        let records = self.core.rel_records()?;
        if records.is_empty() && !self.any_engine_root() {
            return Ok(());
        }

        let outcome = {
            let mut ctx = self.core.page_ctx(0);
            rel_recovery::analyze_and_redo(&mut ctx, &records, stop_at)?
        };
        self.apply_root_moves(outcome.catalog_root, &outcome.engine_roots)?;
        self.core.pages.next_txn_id = outcome.max_txn_id + 1;

        self.reload_engines()?;
        // A standby repeats history but never undoes: an in-flight transaction at
        // the applied tail is the primary's, to be committed (and shipped onward)
        // or resolved at promotion — undoing it here would drop committed data
        // that the streaming protocol never re-ships.
        if !redo_only && !outcome.losers.is_empty() {
            {
                let undo = RoutedUndo::new(&self.engines);
                let mut ctx = self.core.page_ctx(0);
                rel_recovery::undo_losers(&mut ctx, &records, &outcome.losers, &undo)?;
            }
            self.reload_engines()?;
        }
        // Readable standby: rebuild the read state from the retained ring
        // (it is in-memory only — without this a reopen would serve the
        // redo-applied in-flight rows raw). Runs AFTER redo, so heap pages
        // self-identify their owner even when they were formatted within the
        // replayed range.
        if redo_only {
            let floor = self.core.wal.tail();
            self.capture_standby_engines(&records, floor);
        }
        for eng in &mut self.engines {
            eng.finish_recovery();
        }
        self.core.wal.sync_all()?;
        Ok(())
    }
}
