use super::super::*;

impl RelStorage {
    /// Test hook: the relational WAL records currently recoverable from the
    /// ring, exactly as restart's analysis would see them.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn rel_wal_records(
        &self,
    ) -> Result<Vec<(u64, crate::wal::records::RelRecord)>, StorageError> {
        self.lock().rel_records()
    }
}

impl EngineParticipant for RelEngine {
    fn open(&mut self, metadata_root: Option<u64>, db_options: u8) {
        self.rel.catalog_root = metadata_root;
        self.version.set_options_byte(db_options);
    }

    fn metadata_root(&self) -> Option<u64> {
        self.rel.catalog_root
    }

    fn set_metadata_root(&mut self, page: u64) {
        self.rel.catalog_root = Some(page);
    }

    fn options_byte(&self) -> u8 {
        self.version.options_byte()
    }

    fn reload(&mut self, core: &mut StorageFile) -> Result<(), StorageError> {
        RelFile { core, eng: self }.reload_catalog()
    }

    fn recovery_undo(&self) -> Box<dyn LogicalUndo> {
        Box::new(TreeRoots(self.rel.tree_roots()))
    }

    fn capture_standby(
        &mut self,
        core: &mut StorageFile,
        records: &[(u64, RelRecord)],
        floor: u64,
    ) {
        self.version.standby_reads = true;
        RelFile { core, eng: self }.standby_capture_versions(records);
        self.standby_version_floor = floor;
    }

    fn finish_recovery(&mut self) {
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
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl RelFile<'_> {
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
