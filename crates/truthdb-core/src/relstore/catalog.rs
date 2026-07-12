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
}

impl TableDef {
    pub fn schema(&self) -> Result<Schema, StorageError> {
        let columns = self
            .columns
            .iter()
            .map(|(name, spec, nullable)| {
                Ok(Column {
                    name: name.clone(),
                    column_type: ColumnType::parse(spec)
                        .map_err(|err| StorageError::InvalidFile(err.to_string()))?,
                    nullable: *nullable,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok(Schema { columns })
    }

    pub fn is_tree(&self) -> bool {
        !self.key_columns.is_empty()
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
