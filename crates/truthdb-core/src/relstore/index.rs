//! Secondary (nonclustered) B+ index encoding.
//!
//! An index is its own [`BTree`](crate::relstore::btree::BTree) mapping encoded
//! index-key bytes to a row *locator* (how to fetch the full row from the base
//! table). Key layout:
//! - key = the index columns encoded order-preserving (a DESC column has its
//!   bytes bit-inverted so it sorts in reverse, which also puts its NULLs
//!   last — matching SQL Server), then — for a NON-unique index only — the
//!   locator bytes appended so otherwise-equal index values get distinct keys.
//! - value = the locator bytes (always), so a key lookup never has to decode
//!   the locator out of the key.
//!
//! A UNIQUE index keeps the locator out of the key, so two rows with equal
//! index values collide on insert (`DuplicateKey` → error 2601); a single NULL
//! is likewise unique (NULL encodes to one byte), as in SQL Server.

use crate::relstore::heap::Rid;
use crate::relstore::key::encode_datum_collated;
use crate::relstore::row::{Schema, decode_row, encode_row};
use crate::relstore::types::{Datum, TypeError};

/// The collation of the schema column at `index` (empty/short slice → the
/// case-insensitive database default), for folding character index keys.
fn column_collation(collations: &[Option<String>], index: usize) -> Option<&str> {
    collations.get(index).and_then(|c| c.as_deref())
}

/// How to fetch a base-table row from an index entry: by clustered PK key
/// bytes, or by heap home RID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locator {
    Key(Vec<u8>),
    Rid(Rid),
}

const LOCATOR_KEY: u8 = 0;
const LOCATOR_RID: u8 = 1;

/// Encodes an index's key columns from a row's values, applying per-column
/// ASC/DESC direction. `columns` is `(schema index, ascending)` in key order;
/// `collations` is the table's per-column collation (by schema index), so a
/// case-insensitive character index key folds the same way as the clustered key.
pub fn encode_index_columns(
    values: &[Datum],
    columns: &[(usize, bool)],
    collations: &[Option<String>],
) -> Result<Vec<u8>, TypeError> {
    let mut out = Vec::new();
    for &(index, ascending) in columns {
        let value = values
            .get(index)
            .ok_or_else(|| TypeError(format!("index column {index} out of range")))?;
        let collation = column_collation(collations, index);
        if ascending {
            encode_datum_collated(value, collation, &mut out)?;
        } else {
            let start = out.len();
            encode_datum_collated(value, collation, &mut out)?;
            // DESC: invert this column's bytes so ordering reverses.
            for b in &mut out[start..] {
                *b = !*b;
            }
        }
    }
    Ok(out)
}

/// Encodes a leading prefix of an index's columns directly from seek values
/// (`values[i]` is the value for `columns[i]`), applying ASC/DESC. Used to
/// build index-seek bounds. Encodes the same way as [`encode_index_columns`], so
/// a seek literal matches the stored key under the column's collation.
pub fn encode_index_prefix(
    values: &[Datum],
    columns: &[(usize, bool)],
    collations: &[Option<String>],
) -> Result<Vec<u8>, TypeError> {
    let mut out = Vec::new();
    for (i, value) in values.iter().enumerate() {
        let (index, ascending) = columns[i];
        let collation = column_collation(collations, index);
        if ascending {
            encode_datum_collated(value, collation, &mut out)?;
        } else {
            let start = out.len();
            encode_datum_collated(value, collation, &mut out)?;
            for b in &mut out[start..] {
                *b = !*b;
            }
        }
    }
    Ok(out)
}

/// The smallest byte string greater than every string with `prefix` as a
/// prefix — an inclusive upper bound covering the whole prefix block. `None`
/// means unbounded (the prefix is all `0xFF`).
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while let Some(last) = bound.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(bound);
        }
        bound.pop();
    }
    None
}

/// Serializes a locator (tag byte + payload) for storage in an index leaf.
pub fn encode_locator(locator: &Locator) -> Vec<u8> {
    let mut out = Vec::new();
    match locator {
        Locator::Key(key) => {
            out.push(LOCATOR_KEY);
            out.extend_from_slice(key);
        }
        Locator::Rid(rid) => {
            out.push(LOCATOR_RID);
            out.extend_from_slice(&rid.page.to_le_bytes());
            out.extend_from_slice(&rid.slot.to_le_bytes());
        }
    }
    out
}

/// Inverse of [`encode_locator`].
pub fn decode_locator(bytes: &[u8]) -> Locator {
    match bytes[0] {
        LOCATOR_KEY => Locator::Key(bytes[1..].to_vec()),
        LOCATOR_RID => Locator::Rid(Rid {
            page: u64::from_le_bytes(bytes[1..9].try_into().unwrap()),
            slot: u16::from_le_bytes(bytes[9..11].try_into().unwrap()),
        }),
        other => unreachable!("unknown locator tag {other}"),
    }
}

/// The `(leaf key, leaf value)` an index entry stores for one row, given the
/// row's index-column encoding, its locator, and — for an `INCLUDE` index —
/// the encoded included-column values. Non-unique indexes append the locator
/// to the key for uniqueness; unique indexes do not. The key never carries
/// include bytes (they would break both dedup semantics and seek bounds).
///
/// Value format: a bare locator for an index without `INCLUDE` (the
/// pre-INCLUDE format, unchanged for every existing index), or
/// `locator_len u16 LE | locator | include row bytes` when include values are
/// present. The reader picks the format from the catalog's
/// [`IndexDef::include`](crate::relstore::catalog::IndexDef::include), so the
/// two never mix within one index — a `Locator::Key` payload otherwise
/// consumes the rest of the value, which is why the length prefix exists.
pub fn leaf_entry(
    index_key: &[u8],
    locator: &Locator,
    unique: bool,
    include: Option<&[u8]>,
) -> (Vec<u8>, Vec<u8>) {
    let locator_bytes = encode_locator(locator);
    let key = if unique {
        index_key.to_vec()
    } else {
        let mut key = Vec::with_capacity(index_key.len() + locator_bytes.len());
        key.extend_from_slice(index_key);
        key.extend_from_slice(&locator_bytes);
        key
    };
    let value = match include {
        None => locator_bytes,
        Some(bytes) => {
            let mut value = Vec::with_capacity(2 + locator_bytes.len() + bytes.len());
            value.extend_from_slice(&(locator_bytes.len() as u16).to_le_bytes());
            value.extend_from_slice(&locator_bytes);
            value.extend_from_slice(bytes);
            value
        }
    };
    (key, value)
}

/// Splits an `INCLUDE` index's leaf value into its locator and the included
/// columns' row bytes (inverse of the `Some` arm of [`leaf_entry`]).
pub fn decode_leaf_value_with_include(bytes: &[u8]) -> (Locator, &[u8]) {
    let locator_len = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
    let locator = decode_locator(&bytes[2..2 + locator_len]);
    (locator, &bytes[2 + locator_len..])
}

/// The included columns' sub-schema, in `include` order. Their values are
/// stored through the ordinary row codec over this schema, so NULLs and
/// variable-length values need no special casing.
pub fn include_schema(schema: &Schema, include: &[usize]) -> Schema {
    Schema {
        columns: include.iter().map(|&i| schema.columns[i].clone()).collect(),
    }
}

/// Encodes a row's included-column values (original values, not key-folded —
/// recoverability is the whole point of `INCLUDE`).
pub fn encode_include(
    schema: &Schema,
    include: &[usize],
    values: &[Datum],
) -> Result<Vec<u8>, TypeError> {
    let sub: Vec<Datum> = include.iter().map(|&i| values[i].clone()).collect();
    encode_row(&include_schema(schema, include), &sub)
}

/// Decodes an `INCLUDE` leaf's stored values, in `include` order.
pub fn decode_include(
    schema: &Schema,
    include: &[usize],
    bytes: &[u8],
) -> Result<Vec<Datum>, TypeError> {
    decode_row(&include_schema(schema, include), bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn key(values: &[Datum], columns: &[(usize, bool)]) -> Vec<u8> {
        // Case-sensitive collation everywhere, so these order/DESC tests exercise
        // the raw byte layout directly (no case-folding of string keys).
        let cs = vec![Some("Latin1_General_CS_AS".to_string()); 8];
        encode_index_columns(values, columns, &cs).expect("encode")
    }

    #[test]
    fn ascending_column_preserves_value_order() {
        let asc = &[(0usize, true)];
        assert_eq!(
            key(&[Datum::Int(1)], asc).cmp(&key(&[Datum::Int(2)], asc)),
            Ordering::Less
        );
        assert_eq!(
            key(&[Datum::Int(-5)], asc).cmp(&key(&[Datum::Int(3)], asc)),
            Ordering::Less
        );
    }

    #[test]
    fn descending_column_reverses_value_order_and_sorts_nulls_last() {
        let desc = &[(0usize, false)];
        // 2 sorts before 1 under DESC.
        assert_eq!(
            key(&[Datum::Int(2)], desc).cmp(&key(&[Datum::Int(1)], desc)),
            Ordering::Less
        );
        // NULL sorts AFTER any value under DESC (SQL Server semantics).
        assert_eq!(
            key(&[Datum::Int(1)], desc).cmp(&key(&[Datum::Null], desc)),
            Ordering::Less
        );
    }

    #[test]
    fn composite_key_is_column_major() {
        let cols = &[(0usize, true), (1usize, true)];
        let a = key(&[Datum::Int(1), Datum::Int(9)], cols);
        let b = key(&[Datum::Int(2), Datum::Int(0)], cols);
        assert_eq!(a.cmp(&b), Ordering::Less, "first column dominates");
    }

    #[test]
    fn prefix_upper_bound_covers_the_whole_prefix_block() {
        // Any key starting with the prefix is < the bound; the bound is
        // minimal.
        let prefix = vec![0x01, 0x05];
        let bound = prefix_upper_bound(&prefix).expect("bound");
        assert!(prefix.as_slice() < bound.as_slice());
        let mut longer = prefix.clone();
        longer.extend_from_slice(&[0xFF, 0xFF]);
        assert!(longer.as_slice() < bound.as_slice());
        // Trailing 0xFF is carried.
        assert_eq!(prefix_upper_bound(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(prefix_upper_bound(&[0xFF, 0xFF]), None);
    }

    #[test]
    fn unique_key_omits_locator_but_non_unique_appends_it() {
        let index_key = vec![0x01, 0x2a];
        let locator = Locator::Rid(Rid { page: 7, slot: 3 });
        let (uk, uv) = leaf_entry(&index_key, &locator, true, None);
        assert_eq!(uk, index_key, "unique index key is the columns alone");
        assert_eq!(decode_locator(&uv), locator);
        let (nk, _) = leaf_entry(&index_key, &locator, false, None);
        assert!(nk.starts_with(&index_key) && nk.len() > index_key.len());
    }

    #[test]
    fn include_value_round_trips_and_key_stays_locator_only() {
        use crate::relstore::row::Column;
        use crate::relstore::types::ColumnType;

        let schema = Schema {
            columns: vec![
                Column {
                    name: "id".into(),
                    column_type: ColumnType::Int,
                    nullable: false,
                    collation: None,
                },
                Column {
                    name: "name".into(),
                    column_type: ColumnType::VarChar { max_len: 20 },
                    nullable: true,
                    collation: None,
                },
                Column {
                    name: "v".into(),
                    column_type: ColumnType::Int,
                    nullable: true,
                    collation: None,
                },
            ],
        };
        let include = vec![2usize, 1usize];
        let row = vec![Datum::Int(7), Datum::VarChar("MiXeD".into()), Datum::Null];
        let bytes = encode_include(&schema, &include, &row).expect("encode");
        // Both locator kinds round-trip through the length-prefixed value —
        // a Key locator's payload would otherwise swallow the include bytes.
        for locator in [
            Locator::Rid(Rid { page: 7, slot: 3 }),
            Locator::Key(vec![9, 9, 9, 9]),
        ] {
            let index_key = vec![0x01, 0x2a];
            let (key, value) = leaf_entry(&index_key, &locator, false, Some(&bytes));
            let bare = leaf_entry(&index_key, &locator, false, None).0;
            assert_eq!(key, bare, "include bytes never reach the key");
            let (got_locator, got_bytes) = decode_leaf_value_with_include(&value);
            assert_eq!(got_locator, locator);
            let decoded = decode_include(&schema, &include, got_bytes).expect("decode");
            // Original values, in include order, case preserved.
            assert_eq!(decoded, vec![Datum::Null, Datum::VarChar("MiXeD".into())]);
        }
    }
}
