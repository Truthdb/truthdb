use super::super::*;

impl Storage {
    /// Test hook: the relational WAL records currently recoverable from the
    /// ring, exactly as restart's analysis would see them.
    #[cfg(test)]
    pub(crate) fn rel_wal_records(
        &self,
    ) -> Result<Vec<(u64, crate::wal::records::RelRecord)>, StorageError> {
        self.lock().rel_records()
    }
}

impl StorageFile {
    /// Decodes the relational records (with their LSNs) from the recovery
    /// scan.
    pub(in crate::storage) fn rel_records(&self) -> Result<Vec<(u64, RelRecord)>, StorageError> {
        self.replay_cache
            .iter()
            .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
            .map(|record| Ok((record.logical_ts, RelRecord::decode(&record.payload)?)))
            .collect()
    }

    /// ARIES restart: analysis + redo (repeating history), then undo of
    /// loser transactions with CLRs. The catalog is loaded between redo and
    /// undo (undo needs tree roots) and reloaded after (undo may have
    /// removed catalog rows).
    pub(in crate::storage) fn recover_rel(
        &mut self,
        stop_at: Option<u64>,
        redo_only: bool,
    ) -> Result<(), StorageError> {
        let records = self.rel_records()?;
        if records.is_empty() && self.rel.catalog_root.is_none() {
            return Ok(());
        }

        let outcome = {
            let mut ctx = self.rel_ctx();
            rel_recovery::analyze_and_redo(&mut ctx, &records, stop_at)?
        };
        if let Some(root) = outcome.catalog_root {
            self.rel.catalog_root = Some(root);
        }
        self.rel.next_txn_id = outcome.max_txn_id + 1;

        self.reload_catalog()?;
        // A standby repeats history but never undoes: an in-flight transaction at
        // the applied tail is the primary's, to be committed (and shipped onward)
        // or resolved at promotion — undoing it here would drop committed data
        // that the streaming protocol never re-ships.
        if !redo_only && !outcome.losers.is_empty() {
            let roots = self.rel.tree_roots();
            let mut ctx = self.rel_ctx();
            rel_recovery::undo_losers(&mut ctx, &records, &outcome.losers, &roots)?;
            self.reload_catalog()?;
        }
        // Readable standby: rebuild the version store from the retained ring
        // (the store is in-memory only — without this a reopen would serve the
        // redo-applied in-flight rows raw). Runs AFTER redo, so heap pages
        // self-identify their owner even when they were formatted within the
        // replayed range.
        if redo_only {
            self.version.standby_reads = true;
            self.standby_capture_versions(&records);
            self.standby_version_floor = self.wal.tail();
        }
        // Object ids are shared by tables, their secondary indexes, logins,
        // database principals, and database rows (all draw from the same
        // counter), so the next id must clear every kind — an index or a login
        // can outrank every table.
        self.rel.next_object_id = self
            .rel
            .all_tables()
            .flat_map(|def| {
                std::iter::once(def.object_id)
                    .chain(def.indexes.iter().map(|index| index.object_id))
            })
            .chain(self.rel.principals.values().map(|def| def.object_id))
            .chain(
                self.rel
                    .database_principals
                    .values()
                    .map(|def| def.object_id),
            )
            .chain(self.rel.databases.values().map(|def| def.object_id))
            .chain(self.rel.dropped_databases.iter().map(|def| def.object_id))
            .map(|object_id| object_id + 1)
            .max()
            .unwrap_or(FIRST_USER_OBJECT_ID)
            .max(FIRST_USER_OBJECT_ID);
        self.wal.sync_all()?;
        Ok(())
    }

    pub(in crate::storage) fn reload_catalog(&mut self) -> Result<(), StorageError> {
        let Some(root) = self.rel.catalog_root else {
            self.rel.tables.clear();
            self.rel.databases.clear();
            self.rel.dropped_databases.clear();
            self.rel.principals.clear();
            self.rel.database_principals.clear();
            return Ok(());
        };
        let defs = {
            let mut ctx = self.rel_ctx();
            catalog::load_tables(&mut ctx, root)?
        };
        self.rel.tables.clear();
        self.rel.databases.clear();
        self.rel.dropped_databases.clear();
        self.rel.principals.clear();
        self.rel.database_principals.clear();
        for def in defs {
            if def.is_login() {
                // Logins live in their own map, keyed case-insensitively — never
                // in the object namespace.
                self.rel
                    .principals
                    .insert(def.name.to_ascii_lowercase(), def);
            } else if def.is_database_principal() {
                // Users and roles: a second map, also out of the object namespace.
                self.rel
                    .database_principals
                    .insert(def.name.to_ascii_lowercase(), def);
            } else if def.is_database() {
                // Databases: namespace containers, out of the object
                // namespace. A tombstone (dropped) only retires its id.
                if def.database.as_ref().is_some_and(|db| db.dropped) {
                    self.rel.dropped_databases.push(def);
                } else {
                    self.rel
                        .databases
                        .insert(def.name.to_ascii_lowercase(), def);
                }
            } else {
                self.rel.cache_table(def);
            }
        }
        Ok(())
    }
}
