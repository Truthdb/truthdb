use super::super::*;

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

    pub fn rel_create_procedure(
        &self,
        db_id: u32,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_create_procedure(db_id, name, procedure);
        // A parked batch analyzed against the OLD catalog could carry a stale
        // lock set for an EXEC of this name — same class as the option-flip
        // epoch (Stage 13): bump so the grant path re-analyzes.
        self.bump_lock_epoch();
        result
    }

    pub fn rel_alter_procedure(
        &self,
        db_id: u32,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_alter_procedure(db_id, name, procedure);
        self.bump_lock_epoch();
        result
    }

    pub fn rel_create_function(
        &self,
        db_id: u32,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_create_function(db_id, name, function);
        // Like a procedure: a table-reading function changes which locks a batch
        // that references it must hold, so a parked batch analyzed against the
        // old catalog carries a stale lock set — bump so the grant path
        // re-analyzes.
        self.bump_lock_epoch();
        result
    }

    pub fn rel_alter_function(
        &self,
        db_id: u32,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_alter_function(db_id, name, function);
        self.bump_lock_epoch();
        result
    }

    pub fn rel_create_trigger(
        &self,
        db_id: u32,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_create_trigger(db_id, name, trigger);
        // A trigger changes which locks a DML statement on its parent table must
        // hold (its body reads/writes other tables), so a parked batch analyzed
        // against the old catalog carries a stale lock set — bump to re-analyze.
        self.bump_lock_epoch();
        result
    }

    pub fn rel_alter_trigger(
        &self,
        db_id: u32,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
        let result = self.lock().rel_alter_trigger(db_id, name, trigger);
        self.bump_lock_epoch();
        result
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

    /// True if any trigger exists in the catalog — a cheap no-clone check that
    /// keeps the common no-trigger DML path off the firing machinery.
    pub fn rel_has_triggers(&self) -> bool {
        self.lock().rel_has_triggers()
    }

    /// The enabled triggers attached to `parent_object_id` that fire on `event`,
    /// in creation (object_id) order.
    pub fn rel_triggers_for(
        &self,
        parent_object_id: u32,
        event: crate::relstore::catalog::TriggerEvent,
    ) -> Vec<TableDef> {
        self.lock().rel_triggers_for(parent_object_id, event)
    }

    pub fn rel_drop_table(&self, db_id: u32, name: &str) -> Result<bool, StorageError> {
        self.lock().rel_drop_table(db_id, name)
    }

    /// Creates a database (a naming namespace over the shared log and file);
    /// returns its id. Bumps the lock epoch: a parked batch analyzed against
    /// the old database list could resolve names differently.
    pub fn rel_create_database(&self, name: &str) -> Result<u32, StorageError> {
        let result = self.lock().rel_create_database(name);
        self.bump_lock_epoch();
        result
    }

    /// Drops a database and everything in it. Returns false if absent.
    pub fn rel_drop_database(&self, name: &str) -> Result<bool, StorageError> {
        let result = self.lock().rel_drop_database(name);
        self.bump_lock_epoch();
        result
    }

    /// Resolves a database name (case-insensitive) to its id.
    pub fn rel_database_id_by_name(&self, name: &str) -> Option<u32> {
        self.lock().rel_database_id_by_name(name)
    }

    /// Every database as `(id, canonical name)`, default database first.
    pub fn rel_databases(&self) -> Vec<(u32, String)> {
        self.lock().rel_databases()
    }

    /// The default database's (id 1) canonical name.
    pub fn default_database_name(&self) -> String {
        self.lock().default_db_name.clone()
    }

    /// Stamps the default database's name (id 1) from the instance
    /// configuration. Called once at startup, before sessions. Refuses a name
    /// a stored `CREATE DATABASE` row already uses — the default database
    /// would otherwise shadow it (the name→id resolution checks the default
    /// first), leaving the stored database unreachable and undroppable.
    pub fn set_default_database_name(&self, name: &str) -> Result<(), StorageError> {
        let mut guard = self.lock();
        if guard.rel.databases.contains_key(&name.to_ascii_lowercase()) {
            return Err(StorageError::InvalidConfig(format!(
                "the configured default database name '{name}' collides with a database                  created by CREATE DATABASE; rename one of them"
            )));
        }
        guard.default_db_name = name.to_string();
        Ok(())
    }

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
    pub fn rel_create_procedure(
        &mut self,
        db_id: u32,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
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
        let proc_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: proc_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: Some(procedure),
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

    /// Records a user-defined function in the catalog (`CREATE FUNCTION`): its
    /// parameters, return shape, and body text (the view posture — re-parsed at
    /// each call).
    pub fn rel_create_function(
        &mut self,
        db_id: u32,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
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
        let func_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: func_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: Some(function),
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

    /// Replaces an existing function's definition (`ALTER FUNCTION`): the object
    /// id is kept, the stored definition swapped.
    pub fn rel_alter_function(
        &mut self,
        db_id: u32,
        name: &str,
        function: crate::relstore::catalog::FunctionDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let Some(existing) = self.rel.table(db_id, name) else {
            return Err(StorageError::Constraint(format!(
                "function '{name}' does not exist"
            )));
        };
        if !existing.is_function() {
            return Err(StorageError::Constraint(format!(
                "object '{name}' is not a function"
            )));
        }
        let mut def = existing.clone();
        def.function = Some(function);
        let catalog_root = self
            .rel
            .catalog_root
            .expect("functions live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.cache_table(def);
        Ok(())
    }

    /// Replaces an existing procedure's parameters and body (`ALTER
    /// PROCEDURE`): the object id is kept, the stored text swapped.
    pub fn rel_alter_procedure(
        &mut self,
        db_id: u32,
        name: &str,
        procedure: crate::relstore::catalog::ProcedureDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let Some(existing) = self.rel.table(db_id, name) else {
            return Err(StorageError::Constraint(format!(
                "procedure '{name}' does not exist"
            )));
        };
        if !existing.is_procedure() {
            return Err(StorageError::Constraint(format!(
                "object '{name}' is not a procedure"
            )));
        }
        let mut def = existing.clone();
        def.procedure = Some(procedure);
        let catalog_root = self
            .rel
            .catalog_root
            .expect("procedures live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.cache_table(def);
        Ok(())
    }

    /// Creates a trigger: a catalog entry (its own object_id, like a procedure)
    /// whose stored form is its parent table, event set, and body text.
    pub fn rel_create_trigger(
        &mut self,
        db_id: u32,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
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
        let trig_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: trig_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: None,
                trigger: Some(trigger),
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

    /// Replaces a trigger's definition (`ALTER TRIGGER`).
    pub fn rel_alter_trigger(
        &mut self,
        db_id: u32,
        name: &str,
        trigger: crate::relstore::catalog::TriggerDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let Some(existing) = self.rel.table(db_id, name) else {
            return Err(StorageError::Constraint(format!(
                "trigger '{name}' does not exist"
            )));
        };
        if !existing.is_trigger() {
            return Err(StorageError::Constraint(format!(
                "object '{name}' is not a trigger"
            )));
        }
        let mut def = existing.clone();
        def.trigger = Some(trigger);
        let catalog_root = self.rel.catalog_root.expect("triggers live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.cache_table(def);
        Ok(())
    }

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
    pub fn rel_has_triggers(&self) -> bool {
        self.rel.all_tables().any(|d| d.is_trigger())
    }

    /// The enabled triggers attached to `parent_object_id` firing on `event`, in
    /// creation (object_id) order.
    pub fn rel_triggers_for(
        &self,
        parent_object_id: u32,
        event: crate::relstore::catalog::TriggerEvent,
    ) -> Vec<TableDef> {
        let mut trigs: Vec<TableDef> = self
            .rel
            .all_tables()
            .filter(|d| {
                d.trigger.as_ref().is_some_and(|t| {
                    !t.is_disabled
                        && t.parent_object_id == parent_object_id
                        && t.events.contains(&event)
                })
            })
            .cloned()
            .collect();
        trigs.sort_by_key(|d| d.object_id);
        trigs
    }

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

    /// Creates a database: a catalog row carrying a [`catalog::DatabaseDef`],
    /// routed into the databases map so it never enters the object namespace.
    /// Database ids allocate max+1 over stored rows (the synthesized default
    /// database is id 1) and are capped at `u16::MAX` so an id always fits the
    /// WAL container tag.
    pub fn rel_create_database(&mut self, name: &str) -> Result<u32, StorageError> {
        self.ensure_rel_usable()?;
        // Reserved names: `sys` would defeat every `sys.`-prefix dispatch and
        // the lock analysis skip; `dbo` would make a three-part name's schema
        // part ambiguous; the SQL Server system names stay recognizable.
        const RESERVED: [&str; 6] = ["sys", "dbo", "master", "model", "msdb", "tempdb"];
        if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name)) {
            return Err(StorageError::Constraint(format!(
                "the database name '{name}' is reserved"
            )));
        }
        if self.rel_database_id_by_name(name).is_some() {
            return Err(StorageError::Constraint(format!(
                "database '{name}' already exists"
            )));
        }
        // max+1 over LIVE rows AND tombstones: a dropped database's id is
        // never reallocated (see DatabaseDef::dropped).
        let db_id = self
            .rel
            .databases
            .values()
            .chain(self.rel.dropped_databases.iter())
            .filter_map(|d| d.database.as_ref().map(|db| db.db_id))
            .max()
            .map_or(catalog::FIRST_USER_DATABASE_ID, |max| max + 1);
        if db_id > u16::MAX as u32 {
            return Err(StorageError::Constraint(
                "too many databases: the database id space is exhausted".to_string(),
            ));
        }
        if self.rel.catalog_root.is_none() {
            // The catalog tree itself is global (container 0) — it holds
            // every database's rows.
            self.current_container = 0;
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        self.current_container = db_id as u16;
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let db_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: db_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: None,
                trigger: None,
                principal: None,
                permissions: Vec::new(),
                counter_page: None,
                database_id: catalog::DEFAULT_DATABASE_ID,
                database: Some(catalog::DatabaseDef {
                    db_id,
                    dropped: false,
                }),
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel
            .databases
            .insert(def.name.to_ascii_lowercase(), def);
        Ok(db_id)
    }

    /// Drops a database and every object in it, in one statement transaction
    /// (all-or-nothing; data pages leak like `rel_drop_table`'s). The default
    /// database is refused. Returns false if no such database exists.
    pub fn rel_drop_database(&mut self, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        let key = name.to_ascii_lowercase();
        if key == self.default_db_name.to_ascii_lowercase() {
            return Err(StorageError::Constraint(format!(
                "cannot drop the database '{name}' because it is a system database"
            )));
        }
        let Some(row) = self.rel.databases.get(&key).cloned() else {
            return Ok(false);
        };
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        let db_id = row.database.expect("database row").db_id;
        self.current_container = db_id as u16;
        let objects: Vec<(u32, String)> = self
            .rel
            .tables_in(db_id)
            .map(|d| (d.object_id, d.name.clone()))
            .collect();
        let object_ids: Vec<u32> = objects.iter().map(|(id, _)| *id).collect();
        let mut tombstone = row.clone();
        tombstone.database = Some(catalog::DatabaseDef {
            db_id,
            dropped: true,
        });
        let write = tombstone.clone();
        self.rel_statement(move |ctx, txn| {
            for object_id in &object_ids {
                catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, *object_id)?;
            }
            // The database row becomes a TOMBSTONE (id retired forever),
            // freeing the name while pinning the id — see DatabaseDef::dropped.
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)
        })?;
        for (object_id, name) in objects {
            // Same fence as DROP TABLE: a snapshot whose view predates the
            // drop must 3961 rather than read a same-named successor.
            self.version.stamp_schema(object_id);
            self.rel.uncache_table(db_id, &name);
        }
        self.rel.databases.remove(&key);
        self.rel.dropped_databases.push(tombstone);
        Ok(true)
    }

    /// Resolves a database NAME to its id, case-insensitively: the synthesized
    /// default database (id 1, named by the instance configuration) or a
    /// stored `CREATE DATABASE` row. The single name→id derivation every
    /// consumer (USE, login, three-part names) must share.
    pub fn rel_database_id_by_name(&self, name: &str) -> Option<u32> {
        if name.eq_ignore_ascii_case(&self.default_db_name) {
            return Some(catalog::DEFAULT_DATABASE_ID);
        }
        self.rel
            .databases
            .get(&name.to_ascii_lowercase())
            .and_then(|d| d.database.as_ref())
            .map(|db| db.db_id)
    }

    /// Every database as `(id, canonical name)`, the synthesized default
    /// first, then stored rows by id.
    pub fn rel_databases(&self) -> Vec<(u32, String)> {
        let mut out = vec![(catalog::DEFAULT_DATABASE_ID, self.default_db_name.clone())];
        let mut stored: Vec<(u32, String)> = self
            .rel
            .databases
            .values()
            .filter_map(|d| d.database.as_ref().map(|db| (db.db_id, d.name.clone())))
            .collect();
        stored.sort_by_key(|(id, _)| *id);
        out.extend(stored);
        out
    }

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
