use super::super::super::*;

impl Storage {
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
}

impl StorageFile {
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
}
