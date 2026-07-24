//! Relational on-disk structures: page format, buffer pool, row/key codecs,
//! slotted pages, clustered B+ trees, heaps, catalog storage and ARIES
//! restart. Stage 2 exposes them through `Storage`'s `rel_*` methods and
//! temporary debug commands; the SQL layer (Stage 3+) replaces the command
//! surface, not the storage.

pub mod btree;
pub mod buffer_pool;
pub mod catalog;
pub(crate) mod ctx;
pub mod heap;
pub mod index;
pub mod key;
pub(crate) mod overflow;
pub mod page;
pub mod recovery;
pub mod row;
pub mod slotted;
pub mod spill;
#[cfg(test)]
mod tests;
pub mod types;
pub(crate) mod version;

use std::collections::HashMap;

use buffer_pool::BufferPool;
use catalog::{CATALOG_OBJECT_ID, FIRST_USER_OBJECT_ID, TableDef};

/// In-memory relational state owned by the storage file.
pub(crate) struct RelState {
    pub pool: BufferPool,
    /// Set when a commit or rollback could not be logged: the pool may hold
    /// state the WAL cannot explain, so all relational work — and above all
    /// checkpoints, which would make that state durable and unrecoverable —
    /// is refused until a restart replays the log.
    pub wedged: bool,
    /// Pages dirtied since the last checkpoint -> LSN of their first change.
    pub dpt: HashMap<u64, u64>,
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
    pub next_txn_id: u64,
    pub next_object_id: u32,
    /// Open explicit (multi-statement) transactions: txn id → its `BEGIN` LSN.
    /// A (fuzzy) checkpoint may run while these are open — it flushes their
    /// (uncommitted) pages under the steal policy — but must clamp the WAL head
    /// to the *oldest* begin LSN here so their undo records survive for crash
    /// rollback. Autocommit statements never leave one open across calls.
    pub active_txn_begins: std::collections::BTreeMap<u64, u64>,
}

impl RelState {
    pub fn new(pool_capacity_bytes: u64) -> Self {
        RelState {
            pool: BufferPool::new(pool_capacity_bytes),
            wedged: false,
            dpt: HashMap::new(),
            catalog_root: None,
            tables: HashMap::new(),
            databases: HashMap::new(),
            principals: HashMap::new(),
            database_principals: HashMap::new(),
            next_txn_id: 1,
            next_object_id: FIRST_USER_OBJECT_ID,
            active_txn_begins: std::collections::BTreeMap::new(),
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
