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
use crate::relstore::key::{encode_datum, fold_key_datum};
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
        let folded = fold_key_datum(value, column_collation(collations, index));
        if ascending {
            encode_datum(&folded, &mut out)?;
        } else {
            let start = out.len();
            encode_datum(&folded, &mut out)?;
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
/// build index-seek bounds. Folds the same way as [`encode_index_columns`], so a
/// seek literal matches the stored (folded) key under a case-insensitive
/// collation.
pub fn encode_index_prefix(
    values: &[Datum],
    columns: &[(usize, bool)],
    collations: &[Option<String>],
) -> Result<Vec<u8>, TypeError> {
    let mut out = Vec::new();
    for (i, value) in values.iter().enumerate() {
        let (index, ascending) = columns[i];
        let folded = fold_key_datum(value, column_collation(collations, index));
        if ascending {
            encode_datum(&folded, &mut out)?;
        } else {
            let start = out.len();
            encode_datum(&folded, &mut out)?;
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
/// row's index-column encoding and its locator. Non-unique indexes append the
/// locator to the key for uniqueness; unique indexes do not.
pub fn leaf_entry(index_key: &[u8], locator: &Locator, unique: bool) -> (Vec<u8>, Vec<u8>) {
    let value = encode_locator(locator);
    let key = if unique {
        index_key.to_vec()
    } else {
        let mut key = Vec::with_capacity(index_key.len() + value.len());
        key.extend_from_slice(index_key);
        key.extend_from_slice(&value);
        key
    };
    (key, value)
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
        let (uk, uv) = leaf_entry(&index_key, &locator, true);
        assert_eq!(uk, index_key, "unique index key is the columns alone");
        assert_eq!(decode_locator(&uv), locator);
        let (nk, _) = leaf_entry(&index_key, &locator, false);
        assert!(nk.starts_with(&index_key) && nk.len() > index_key.len());
    }
}
