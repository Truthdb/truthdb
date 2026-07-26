/// Decodes a row, honouring a caller's projection: `None` means every column.
///
/// The read paths take `Option<&[usize]>` rather than always being handed a
/// full list, so a caller that wants the whole row neither builds one nor pays
/// to walk it.
fn decode_projected(
    schema: &Schema,
    row: &[u8],
    projection: Option<&[usize]>,
) -> Result<Vec<Datum>, crate::relstore::types::TypeError> {
    match projection {
        Some(projection) => decode_row_projected(schema, row, projection),
        None => decode_row(schema, row),
    }
}

/// Inserts one row's entries into every secondary index. A duplicate on a
/// UNIQUE index surfaces as a constraint error the SQL layer maps to 2601.
fn index_insert_row(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    schema: &Schema,
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let include = if index.include.is_empty() {
            None
        } else {
            Some(
                index::encode_include(schema, &index.include, values)
                    .map_err(|err| StorageError::InvalidConfig(err.0))?,
            )
        };
        let (key, value) = index::leaf_entry(&index_key, locator, index.unique, include.as_deref());
        let tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &key, &value)? {
            TreeInsert::Inserted => {}
            TreeInsert::DuplicateKey => {
                return Err(StorageError::Constraint(format!(
                    "duplicate unique index '{}'",
                    index.name
                )));
            }
        }
    }
    Ok(())
}

/// Reindexes a set of updated rows: deletes every old entry first, then
/// inserts every new one, so a UNIQUE index tolerates value swaps within one
/// statement.
fn apply_index_updates(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    schema: &Schema,
    collations: &[Option<String>],
    ops: &[(Vec<Datum>, Locator, Vec<Datum>, Locator)],
) -> Result<(), StorageError> {
    if indexes.is_empty() {
        return Ok(());
    }
    for (old_values, old_locator, _, _) in ops {
        index_delete_row(ctx, txn, indexes, collations, old_values, old_locator)?;
    }
    for (_, _, new_values, new_locator) in ops {
        index_insert_row(
            ctx,
            txn,
            indexes,
            schema,
            collations,
            new_values,
            new_locator,
        )?;
    }
    Ok(())
}

/// Removes one row's entries from every secondary index.
fn index_delete_row(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let (key, _) = index::leaf_entry(&index_key, locator, index.unique, None);
        let tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        tree.delete(ctx, &mut OpMode::Txn(txn), &key)?;
    }
    Ok(())
}

fn column_index(schema: &Schema, name: &str) -> Result<usize, StorageError> {
    column_index_by(schema, name)
}

fn column_index_by(schema: &Schema, name: &str) -> Result<usize, StorageError> {
    schema
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| StorageError::InvalidConfig(format!("unknown column '{name}'")))
}

fn validate_not_null(schema: &Schema, values: &[Datum]) -> Result<(), StorageError> {
    for (column, value) in schema.columns.iter().zip(values) {
        if !column.nullable && value.is_null() {
            return Err(StorageError::Constraint(format!(
                "column '{}' does not allow NULL",
                column.name
            )));
        }
    }
    Ok(())
}

/// A row staged for insert: its clustered key (trees) and its encoding —
/// `None` when the table has (MAX) columns, whose oversize values must spill
/// inside the statement before the row can encode.
type StagedInsert = (Option<Vec<u8>>, Option<Vec<u8>>);
/// An in-place tree update: key, pre-encoded row or the values to encode
/// in-statement ((MAX) tables).
type StagedInPlace = (Vec<u8>, Option<Vec<u8>>, Option<Vec<Datum>>);
/// A re-keying tree update: old key, new key, then as [`StagedInPlace`].
type StagedRekey = (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<Vec<Datum>>);
/// A heap update: RID, then as [`StagedInPlace`]'s tail.
type StagedHeapUpdate = (Rid, Option<Vec<u8>>, Option<Vec<Datum>>);

use super::super::*;

mod index_access;
mod insert;
mod mutation;
mod overflow_values;
mod scan;
