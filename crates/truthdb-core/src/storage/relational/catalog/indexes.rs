use super::super::super::*;

impl Storage {
    pub(crate) fn rel_create_index(
        &self,
        db_id: u32,
        table: &str,
        index_name: String,
        columns: Vec<(usize, bool)>,
        unique: bool,
        include: Vec<usize>,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_create_index(db_id, table, index_name, columns, unique, include)
    }

    pub(crate) fn rel_drop_index(
        &self,
        db_id: u32,
        table: &str,
        index_name: &str,
    ) -> Result<bool, StorageError> {
        self.lock().rel_drop_index(db_id, table, index_name)
    }

    pub(crate) fn rel_alter_add_column(
        &self,
        db_id: u32,
        table: &str,
        column: Column,
        default_text: Option<String>,
        fill: Datum,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_alter_add_column(db_id, table, column, default_text, fill)
    }
}

impl StorageFile {
    /// Creates a secondary index over `table` and backfills it from the
    /// current rows (blocking build). A duplicate on a UNIQUE index during the
    /// build fails the whole statement (error 2601). The index is persisted in
    /// the table's catalog row.
    pub(crate) fn rel_create_index(
        &mut self,
        db_id: u32,
        table: &str,
        index_name: String,
        columns: Vec<(usize, bool)>,
        unique: bool,
        include: Vec<usize>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let mut def = self
            .rel
            .table(db_id, table)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{table}'")))?;
        if def
            .indexes
            .iter()
            .any(|i| i.name.eq_ignore_ascii_case(&index_name))
        {
            return Err(StorageError::Constraint(format!(
                "index '{index_name}' already exists"
            )));
        }
        let catalog_root = self
            .rel
            .catalog_root
            .ok_or_else(|| StorageError::InvalidFile("catalog root missing".to_string()))?;
        let object_id = self.rel.next_object_id;
        // Snapshot the rows to backfill (materialized before any mutation).
        let located = self.rel_scan_located(db_id, table)?;

        let schema = def.schema()?;
        let updated = self.rel_statement(move |ctx, txn| {
            let tree = BTree::create(ctx, object_id)?;
            for (loc, values) in &located {
                let locator = match loc {
                    RowLocator::Key(key) => Locator::Key(key.clone()),
                    RowLocator::Rid(rid) => Locator::Rid(*rid),
                };
                let index_key = index::encode_index_columns(values, &columns, &def.collations)
                    .map_err(|err| StorageError::InvalidConfig(err.0))?;
                let include_bytes = if include.is_empty() {
                    None
                } else {
                    Some(
                        index::encode_include(&schema, &include, values)
                            .map_err(|err| StorageError::InvalidConfig(err.0))?,
                    )
                };
                let (key, value) =
                    index::leaf_entry(&index_key, &locator, unique, include_bytes.as_deref());
                // Backfill is system-logged: the fresh tree is not in the
                // rollback roots, so a failure leaks it (the catalog entry
                // below is undone, leaving it unreferenced).
                match tree.insert_unique_bulk(ctx, &key, &value)? {
                    TreeInsert::Inserted => {}
                    TreeInsert::DuplicateKey => {
                        return Err(StorageError::Constraint(format!(
                            "duplicate unique index '{index_name}'"
                        )));
                    }
                }
            }
            def.indexes.push(IndexDef {
                object_id,
                name: index_name,
                columns,
                unique,
                root_page: tree.root,
                include,
            });
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.cache_table(updated);
        Ok(())
    }

    /// `ALTER TABLE ADD <column>`: appends the column to the table's catalog
    /// entry and rewrites every existing row under the widened schema, all in
    /// one transactional statement — the row codec is positional, so an old
    /// row cannot be read under the new schema without re-encoding. Keys and
    /// index entries are untouched: appending a column shifts no schema index
    /// the key or any secondary index refers to, tree rewrites are in-place
    /// by key, and heap RIDs are stable across an update.
    pub(crate) fn rel_alter_add_column(
        &mut self,
        db_id: u32,
        table: &str,
        column: Column,
        default_text: Option<String>,
        fill: Datum,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let mut column = column;
        // A character column without an explicit COLLATE inherits the database
        // default by name, exactly as CREATE TABLE records it.
        if column.collation.is_none()
            && matches!(
                column.column_type,
                crate::relstore::types::ColumnType::VarChar { .. }
                    | crate::relstore::types::ColumnType::NVarChar { .. }
            )
        {
            column.collation = self.default_collation.clone();
        }
        let mut def = self
            .rel
            .table(db_id, table)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig(format!("unknown table '{table}'")))?;
        let catalog_root = self
            .rel
            .catalog_root
            .ok_or_else(|| StorageError::InvalidFile("catalog root missing".to_string()))?;
        // Snapshot every row under the OLD schema (with its locator), before
        // the definition widens.
        let located = self.rel_scan_located(db_id, table)?;

        // Parallel catalog arrays: `defaults`/`collations` may be shorter than
        // `columns` (serde(default) on pre-upgrade tables) — pad before push.
        def.defaults.resize(def.columns.len(), None);
        def.collations.resize(def.columns.len(), None);
        def.columns.push((
            column.name.clone(),
            column.column_type.name(),
            column.nullable,
        ));
        def.defaults.push(default_text);
        def.collations.push(column.collation.clone());
        let new_schema = def.schema()?;

        // Every row gets the frozen fill appended; the re-encode happens
        // INSIDE the statement so a (MAX) value the located scan resolved
        // re-spills to a fresh overflow chain instead of blowing the in-row
        // caps (the #123 review's finding: ALTER was unusable once a table
        // held a real payload). Non-MAX tables pay one no-op spill scan.
        let mut tree_rows: Vec<(Vec<u8>, Vec<Datum>)> = Vec::new();
        let mut heap_rows: Vec<(Rid, Vec<Datum>)> = Vec::new();
        for (loc, mut values) in located {
            values.push(fill.clone());
            match loc {
                RowLocator::Key(key) => tree_rows.push((key, values)),
                RowLocator::Rid(rid) => heap_rows.push((rid, values)),
            }
        }

        let is_tree = def.is_tree();
        let object_id = def.object_id;
        let root_page = def.root_page;
        let closure_schema = new_schema.clone();
        let updated = self.rel_statement(move |ctx, txn| {
            if is_tree {
                let tree = BTree {
                    object_id,
                    root: root_page,
                };
                for (key, mut values) in tree_rows {
                    Self::spill_max_values(ctx, &closure_schema, &mut values)?;
                    let row = encode_row(&closure_schema, &values)?;
                    tree.update(ctx, &mut OpMode::Txn(txn), &key, &row)?;
                }
            } else {
                let heap = Heap {
                    object_id,
                    first_page: root_page,
                };
                for (rid, mut values) in heap_rows {
                    Self::spill_max_values(ctx, &closure_schema, &mut values)?;
                    let row = encode_row(&closure_schema, &values)?;
                    heap.update(ctx, txn, rid, &row)?;
                }
            }
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.cache_table(updated);
        // ALTER ADD re-encodes every row: version images from before it
        // cannot decode under the widened schema, so a SNAPSHOT transaction
        // whose view predates this commit gets 3961 at its next access
        // (statement snapshots cannot be live here — the ALTER holds
        // Database X). Stamped with this ALTER's own commit sequence, the
        // newest assigned (recorded a moment ago in `rel_statement`).
        self.version.stamp_schema(object_id);
        Ok(())
    }

    /// Drops a secondary index by name (logical: index pages leak). Returns
    /// false if no such index exists on any table.
    pub(crate) fn rel_drop_index(
        &mut self,
        db_id: u32,
        table: &str,
        index_name: &str,
    ) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        // Index names are scoped to their table, so confine the lookup there.
        // The caller passes the table's canonical name.
        let Some(mut def) = self
            .rel
            .tables_in(db_id)
            .find(|def| def.name.eq_ignore_ascii_case(table))
            .cloned()
        else {
            return Ok(false);
        };
        if !def
            .indexes
            .iter()
            .any(|i| i.name.eq_ignore_ascii_case(index_name))
        {
            return Ok(false);
        }
        def.indexes
            .retain(|i| !i.name.eq_ignore_ascii_case(index_name));
        let updated = self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.cache_table(updated);
        Ok(true)
    }
}
