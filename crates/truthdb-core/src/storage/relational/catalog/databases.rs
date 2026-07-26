use super::super::super::*;

impl Storage {
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
}

impl StorageFile {
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
}
