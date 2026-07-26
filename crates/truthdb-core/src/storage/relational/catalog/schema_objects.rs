use super::super::super::*;

impl Storage {
    #[allow(clippy::too_many_arguments)]
    pub fn rel_create_table(
        &self,
        db_id: u32,
        name: &str,
        columns: Vec<Column>,
        key_names: &[String],
        defaults: Vec<Option<String>>,
        identity: Option<catalog::IdentitySpec>,
        check_constraints: Vec<catalog::CheckDef>,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_table(
            db_id,
            name,
            columns,
            key_names,
            defaults,
            identity,
            check_constraints,
            foreign_keys,
        )
    }

    pub fn rel_create_view(
        &self,
        db_id: u32,
        name: &str,
        query_text: &str,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_view(db_id, name, query_text)
    }

    pub fn rel_table(&self, db_id: u32, name: &str) -> Option<TableDef> {
        self.lock().rel_table(db_id, name)
    }

    /// The database's default collation, as stamped into the file at creation.
    /// `None` means the built-in default. A character column declared without an
    /// explicit `COLLATE` is resolved to this at CREATE TABLE and stored with
    /// it, so a column keeps the collation it was created under even if a later
    /// database is created with a different default.
    pub fn default_collation(&self) -> Option<String> {
        self.lock().default_collation.clone()
    }

    pub fn rel_tables(&self) -> Vec<TableDef> {
        self.lock().rel_tables()
    }

    pub fn rel_drop_table(&self, db_id: u32, name: &str) -> Result<bool, StorageError> {
        self.lock().rel_drop_table(db_id, name)
    }

    /// The table's committed row count, when it has a counter page (tables
    /// created before counters existed do not — the planner then applies no
    /// tie-break). Errors degrade to `None`: the count is a statistic, never
    /// load-bearing for results.
    pub(crate) fn rel_row_count(&self, db_id: u32, table: &str) -> Option<u64> {
        self.lock().rel_row_count(db_id, table)
    }
}
impl StorageFile {
    /// Creates a table: with `key_names` it becomes a clustered B+ tree on
    /// those columns, without it a heap.
    #[allow(clippy::too_many_arguments)]
    pub fn rel_create_table(
        &mut self,
        db_id: u32,
        name: &str,
        mut columns: Vec<Column>,
        key_names: &[String],
        defaults: Vec<Option<String>>,
        identity: Option<catalog::IdentitySpec>,
        check_constraints: Vec<catalog::CheckDef>,
        foreign_keys: Vec<catalog::ForeignKeyDef>,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        // A character column declared without an explicit COLLATE inherits the
        // database default *by name*, recorded now rather than resolved on each
        // read: the column's key bytes are that collation's sort keys, so it has
        // to keep the collation it was created under. Resolved here, at the one
        // point every CREATE TABLE passes through, so the SQL path and the
        // native path cannot disagree.
        if let Some(default) = self.default_collation.clone() {
            for column in &mut columns {
                if column.collation.is_none()
                    && matches!(
                        column.column_type,
                        crate::relstore::types::ColumnType::VarChar { .. }
                            | crate::relstore::types::ColumnType::NVarChar { .. }
                    )
                {
                    column.collation = Some(default.clone());
                }
            }
        }
        if self.rel.contains_table(db_id, name) {
            return Err(StorageError::Constraint(format!(
                "table '{name}' already exists"
            )));
        }
        if columns.is_empty() {
            return Err(StorageError::InvalidConfig(
                "a table needs at least one column".to_string(),
            ));
        }
        let mut key_columns = Vec::new();
        for key_name in key_names {
            let index = columns
                .iter()
                .position(|c| &c.name == key_name)
                .ok_or_else(|| {
                    StorageError::InvalidConfig(format!("unknown key column '{key_name}'"))
                })?;
            if columns[index].nullable {
                return Err(StorageError::InvalidConfig(format!(
                    "primary key column '{key_name}' must be NOT NULL"
                )));
            }
            key_columns.push(index);
        }

        // The catalog tree itself is created outside the statement (system
        // records, not undoable) so a rolled-back CREATE TABLE still leaves
        // a valid catalog.
        if self.rel.catalog_root.is_none() {
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let def_columns: Vec<(String, String, bool)> = columns
            .iter()
            .map(|c| (c.name.clone(), c.column_type.name(), c.nullable))
            .collect();
        let collations: Vec<Option<String>> = columns.iter().map(|c| c.collation.clone()).collect();
        let table_name = name.to_string();
        let is_tree = !key_columns.is_empty();

        let def = self.rel_statement(move |ctx, txn| {
            let root_page = if is_tree {
                BTree::create(ctx, object_id)?.root
            } else {
                Heap::create(ctx, object_id)?.first_page
            };
            let counter_page = ctx.counter_create(object_id)?;
            let def = TableDef {
                object_id,
                name: table_name,
                columns: def_columns,
                key_columns,
                root_page,
                defaults,
                collations,
                identity,
                indexes: Vec::new(),
                check_constraints,
                foreign_keys,
                view_query: None,
                procedure: None,
                function: None,
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: Some(counter_page),
                database_id: db_id,
                database: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        // Stamp the new object: a SNAPSHOT transaction whose view predates
        // this CREATE must 3961 rather than silently read the (possibly
        // same-named, post-DROP) new table as empty — its snapshot has no
        // history for an object that did not exist.
        self.version.stamp_schema(def.object_id);
        self.rel.cache_table(def);
        Ok(())
    }

    /// Creates a VIEW: a catalog entry that stores its `SELECT` source text and
    /// owns no data pages. The name shares the table namespace (a view and a
    /// table cannot share a name).
    pub fn rel_create_view(
        &mut self,
        db_id: u32,
        name: &str,
        query_text: &str,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        if self.rel.contains_table(db_id, name) {
            return Err(StorageError::Constraint(format!(
                "object '{name}' already exists"
            )));
        }
        if self.rel.catalog_root.is_none() {
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let view_name = name.to_string();
        let query = query_text.to_string();

        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: view_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: Some(query),
                procedure: None,
                function: None,
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: None,
                database_id: db_id,
                database: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.cache_table(def);
        Ok(())
    }

    /// Creates a stored procedure: a catalog entry whose stored form is its
    /// parameter list and body text (the view posture — re-parsed at EXEC).
    /// The named object's definition in the given database, if it exists.
    pub fn rel_table(&self, db_id: u32, name: &str) -> Option<TableDef> {
        self.rel.table(db_id, name).cloned()
    }

    /// All user table definitions across every database, ordered by object id
    /// (for sys.tables / sys.columns).
    pub fn rel_tables(&self) -> Vec<TableDef> {
        let mut defs: Vec<TableDef> = self.rel.all_tables().cloned().collect();
        defs.sort_by_key(|d| d.object_id);
        defs
    }

    /// True if any trigger exists in the catalog (no clone).
    /// Drops a table (logical: removes the catalog row; data pages leak
    /// until a later reclamation stage). Returns false if the table does not
    /// exist.
    pub fn rel_drop_table(&mut self, db_id: u32, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let Some(def) = self.rel.table(db_id, name).cloned() else {
            return Ok(false);
        };
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        self.rel_statement(move |ctx, txn| {
            catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, def.object_id)
        })?;
        self.rel.uncache_table(db_id, name);
        Ok(true)
    }

    /// Candidate rows for an index access path: walks the index tree over
    /// `[lower, upper]`, then fetches each row by its locator. Returns a
    /// superset the caller re-filters with the full WHERE (so loose bounds are
    /// safe).
    pub(crate) fn rel_row_count(&mut self, db_id: u32, table: &str) -> Option<u64> {
        if self.ensure_rel_usable().is_err() {
            return None;
        }
        let page = self.rel.table(db_id, table)?.counter_page?;
        self.rel_ctx().counter_read(page).ok()
    }
}
