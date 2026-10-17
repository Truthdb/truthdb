//! Relational on-disk structures: row/key codecs, clustered B+ trees, heaps,
//! secondary indexes, overflow chains, catalog storage and the row-version
//! store, over the shared log's pages. Stage 2 exposes them through
//! `Storage`'s `rel_*` methods and temporary debug commands; the SQL layer
//! (Stage 3+) replaces the command surface, not the storage.

pub use truthdb_core::relstore::{buffer_pool, ctx, page, recovery, slotted};

pub mod btree;
pub mod catalog;
pub mod heap;
pub mod index;
pub mod key;
pub mod overflow;
pub mod row;
pub mod spill;
#[cfg(test)]
mod tests;
pub mod types;
pub mod version;

use std::collections::HashMap;

use catalog::{CATALOG_OBJECT_ID, FIRST_USER_OBJECT_ID, TableDef};

/// In-memory relational state owned by the storage file.
pub(crate) struct RelState {
    pub catalog_root: Option<u64>,
    /// Catalog cache: database id -> (table name -> definition). Object names
    /// are unique per database; the nested map is the namespace boundary.
    pub tables: HashMap<u32, HashMap<String, TableDef>>,
    /// Databases (`CREATE DATABASE` rows), keyed by lowercased database name.
    /// The default database (id 1) is synthesized, never stored, and never in
    /// this map. Persisted in the same catalog b-tree (a row with
    /// `database: Some(..)`), partitioned out on load, never in the object
    /// namespace.
    pub databases: HashMap<String, TableDef>,
    /// Dropped databases' tombstone rows (`DatabaseDef::dropped`): their ids
    /// are retired forever, their names free. Not keyed by name — several
    /// drops can share one.
    pub dropped_databases: Vec<TableDef>,
    /// Server logins (principals), keyed by lowercased login name. Persisted in
    /// the SAME catalog b-tree as `tables` (a row with `principal: Some(..)`),
    /// but partitioned into a separate map on load so a login never enters the
    /// object namespace (name resolution, sys.tables, DROP TABLE).
    pub principals: HashMap<String, TableDef>,
    /// Database principals — users and roles — keyed by lowercased name. A
    /// separate map from `principals` (server logins) because a login and a user
    /// commonly share a name (`CREATE USER alice FOR LOGIN alice`). Persisted in
    /// the same catalog b-tree, partitioned out on load, and never in the object
    /// namespace. Membership edges live on each row's `PrincipalDef.member_of`.
    pub database_principals: HashMap<String, TableDef>,
    pub next_object_id: u32,
}

impl RelState {
    pub fn new() -> Self {
        RelState {
            catalog_root: None,
            tables: HashMap::new(),
            databases: HashMap::new(),
            dropped_databases: Vec::new(),
            principals: HashMap::new(),
            database_principals: HashMap::new(),
            next_object_id: FIRST_USER_OBJECT_ID,
        }
    }

    /// Stable root pages by object id (for logical tree undos), including
    /// the catalog tree itself and every secondary index (index trees are
    /// undone the same way as clustered tables).
    pub fn tree_roots(&self) -> HashMap<u32, u64> {
        let mut roots = HashMap::new();
        if let Some(root) = self.catalog_root {
            roots.insert(CATALOG_OBJECT_ID, root);
        }
        for def in self.all_tables() {
            if def.is_tree() {
                roots.insert(def.object_id, def.root_page);
            }
            for index in &def.indexes {
                roots.insert(index.object_id, index.root_page);
            }
        }
        roots
    }

    /// The named object in the given database, if any (exact-case).
    pub fn table(&self, db_id: u32, name: &str) -> Option<&TableDef> {
        self.tables.get(&db_id)?.get(name)
    }

    /// True if the database holds an object with this exact-case name.
    pub fn contains_table(&self, db_id: u32, name: &str) -> bool {
        self.table(db_id, name).is_some()
    }

    /// Caches a definition under its own `database_id` and `name`.
    pub fn cache_table(&mut self, def: TableDef) {
        self.tables
            .entry(def.database_id)
            .or_default()
            .insert(def.name.clone(), def);
    }

    /// Removes a cached definition; drops the database's (empty) inner map so
    /// a dropped database leaves nothing behind.
    pub fn uncache_table(&mut self, db_id: u32, name: &str) {
        if let Some(inner) = self.tables.get_mut(&db_id) {
            inner.remove(name);
            if inner.is_empty() {
                self.tables.remove(&db_id);
            }
        }
    }

    /// Every object definition across all databases.
    pub fn all_tables(&self) -> impl Iterator<Item = &TableDef> {
        self.tables.values().flat_map(|inner| inner.values())
    }

    /// Every object definition in one database.
    pub fn tables_in(&self, db_id: u32) -> impl Iterator<Item = &TableDef> {
        self.tables.get(&db_id).into_iter().flat_map(|m| m.values())
    }
}
