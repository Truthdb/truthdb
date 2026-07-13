//! Catalog storage: table definitions live as rows in a clustered B+ tree
//! (object id 1, keyed by object id, value = JSON-encoded [`TableDef`]).
//! The catalog root page is recorded in the superblock's `metadata_root`
//! (as an absolute file offset; 0 = none) and re-announced through
//! SET_CATALOG_ROOT WAL records so a pre-checkpoint crash can rediscover it.

use serde::{Deserialize, Serialize};

use crate::relstore::btree::{BTree, TreeInsert};
use crate::relstore::ctx::{OpMode, RelCtx};
use crate::relstore::key::encode_datum;
use crate::relstore::row::{Column, Schema};
use crate::relstore::types::{ColumnType, Datum};
use crate::storage::StorageError;
use crate::wal::records::RelRecord;

/// Object id of the catalog tree itself.
pub const CATALOG_OBJECT_ID: u32 = 1;
/// First object id handed to user tables.
pub const FIRST_USER_OBJECT_ID: u32 = 2;

/// An `IDENTITY(seed, increment)` column: which column it is (schema index),
/// its seed/increment, and the next value to hand out (persisted so identity
/// values continue across restarts and are never reused after DELETE).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct IdentitySpec {
    pub column: usize,
    pub seed: i64,
    pub increment: i64,
    pub next: i64,
}

/// A nonclustered (secondary) B+ index over a base table. Its own tree (keyed
/// by [`object_id`](IndexDef::object_id)) maps encoded index-key bytes to a
/// row locator (the base table's PK key for a clustered table, or the heap
/// RID). Indexes live embedded in their owning [`TableDef`] so they load and
/// mutate with the catalog row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub object_id: u32,
    pub name: String,
    /// (schema column index, ascending) for each index key column, in order.
    pub columns: Vec<(usize, bool)>,
    /// UNIQUE index: duplicate key values are rejected (error 2601).
    pub unique: bool,
    /// The index tree's root page.
    pub root_page: u64,
}

/// A `CHECK` constraint. The predicate is stored as source text (re-parsed and
/// evaluated per row at INSERT/UPDATE, like a column `DEFAULT`) so the catalog
/// row need not carry a serialized AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDef {
    pub name: String,
    pub predicate: String,
}

/// A `FOREIGN KEY` constraint (NO ACTION). Referenced columns are always the
/// parent's primary key, so only the child columns are stored — ordered to
/// match the parent's primary-key column order, so a child row's key can be
/// probed against the parent directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyDef {
    pub name: String,
    /// Child column indices, in parent primary-key order.
    pub columns: Vec<usize>,
    /// Referenced parent table (bare name).
    pub parent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub object_id: u32,
    pub name: String,
    /// (column name, parseable type spec, nullable).
    pub columns: Vec<(String, String, bool)>,
    /// Schema indices of the primary-key columns; empty = heap table.
    pub key_columns: Vec<usize>,
    /// Tree root page or heap first page.
    pub root_page: u64,
    /// Per-column `DEFAULT` source text (parallel to `columns`; empty = none).
    #[serde(default)]
    pub defaults: Vec<Option<String>>,
    /// Per-column collation name (parallel to `columns`; empty = default).
    #[serde(default)]
    pub collations: Vec<Option<String>>,
    /// The single IDENTITY column, if any.
    #[serde(default)]
    pub identity: Option<IdentitySpec>,
    /// Secondary indexes over this table.
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    /// `CHECK` constraints enforced on INSERT/UPDATE.
    #[serde(default)]
    pub check_constraints: Vec<CheckDef>,
    /// `FOREIGN KEY` constraints (this table is the referencing child).
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyDef>,
    /// For a VIEW: the source text of its `SELECT`, re-parsed and inlined (as a
    /// derived table) wherever the view is referenced. `None` for a base table.
    /// A view carries no data pages, columns, or key.
    #[serde(default)]
    pub view_query: Option<String>,
}

impl TableDef {
    /// True if this catalog entry is a view rather than a base table.
    pub fn is_view(&self) -> bool {
        self.view_query.is_some()
    }
}

impl TableDef {
    pub fn schema(&self) -> Result<Schema, StorageError> {
        let columns = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, spec, nullable))| {
                Ok(Column {
                    name: name.clone(),
                    column_type: ColumnType::parse(spec)
                        .map_err(|err| StorageError::InvalidFile(err.to_string()))?,
                    nullable: *nullable,
                    collation: self.collations.get(i).cloned().flatten(),
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Schema { columns })
    }

    pub fn is_tree(&self) -> bool {
        !self.key_columns.is_empty()
    }

    /// The DEFAULT source text for column `index`, if any.
    pub fn default_for(&self, index: usize) -> Option<&str> {
        self.defaults.get(index).and_then(|d| d.as_deref())
    }
}

fn catalog_tree(root: u64) -> BTree {
    BTree {
        object_id: CATALOG_OBJECT_ID,
        root,
    }
}

fn catalog_key(object_id: u32) -> Vec<u8> {
    let mut key = Vec::new();
    encode_datum(&Datum::Int(object_id as i32), &mut key).expect("int key encodes");
    key
}

/// Creates the catalog tree (system records) and announces its root.
pub(crate) fn create_catalog(ctx: &mut RelCtx<'_>) -> Result<u64, StorageError> {
    let tree = BTree::create(ctx, CATALOG_OBJECT_ID)?;
    ctx.append(&RelRecord::set_catalog_root(tree.root), false)?;
    Ok(tree.root)
}

/// Loads every table definition from the catalog tree.
pub(crate) fn load_tables(
    ctx: &mut RelCtx<'_>,
    catalog_root: u64,
) -> Result<Vec<TableDef>, StorageError> {
    let tree = catalog_tree(catalog_root);
    tree.scan(ctx)?
        .into_iter()
        .map(|(_, row)| {
            serde_json::from_slice(&row)
                .map_err(|err| StorageError::InvalidFile(format!("corrupt catalog row: {err}")))
        })
        .collect()
}

/// Inserts a table definition (undoable: part of the creating statement's
/// transaction).
pub(crate) fn insert_table(
    ctx: &mut RelCtx<'_>,
    mode: &mut OpMode<'_>,
    catalog_root: u64,
    def: &TableDef,
) -> Result<(), StorageError> {
    let row = serde_json::to_vec(def)
        .map_err(|err| StorageError::InvalidFile(format!("encode catalog row: {err}")))?;
    let tree = catalog_tree(catalog_root);
    match tree.insert_unique(ctx, mode, &catalog_key(def.object_id), &row)? {
        TreeInsert::Inserted => Ok(()),
        TreeInsert::DuplicateKey => Err(StorageError::InvalidFile(format!(
            "duplicate object id {} in catalog",
            def.object_id
        ))),
    }
}

/// Overwrites an existing table's catalog row in place (same object id/key),
/// used to persist a mutated IDENTITY counter. Undoable within the statement.
pub(crate) fn update_table(
    ctx: &mut RelCtx<'_>,
    mode: &mut OpMode<'_>,
    catalog_root: u64,
    def: &TableDef,
) -> Result<(), StorageError> {
    let row = serde_json::to_vec(def)
        .map_err(|err| StorageError::InvalidFile(format!("encode catalog row: {err}")))?;
    let tree = catalog_tree(catalog_root);
    tree.update(ctx, mode, &catalog_key(def.object_id), &row)?;
    Ok(())
}

/// Removes a table's catalog row. Stage 3 does a *logical* drop: the row is
/// deleted (undoable, part of the DROP statement) and the table's data pages
/// are left allocated (leaked until a future page-reclamation stage) — same
/// accepted trade-off as rolled-back allocations.
pub(crate) fn delete_table(
    ctx: &mut RelCtx<'_>,
    mode: &mut OpMode<'_>,
    catalog_root: u64,
    object_id: u32,
) -> Result<(), StorageError> {
    let tree = catalog_tree(catalog_root);
    tree.delete(ctx, mode, &catalog_key(object_id))?;
    Ok(())
}
