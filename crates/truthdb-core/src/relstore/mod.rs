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
    /// Catalog cache: table name -> definition.
    pub tables: HashMap<String, TableDef>,
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
        for def in self.tables.values() {
            if def.is_tree() {
                roots.insert(def.object_id, def.root_page);
            }
            for index in &def.indexes {
                roots.insert(index.object_id, index.root_page);
            }
        }
        roots
    }
}
