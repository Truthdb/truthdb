//! Hash keys for the in-memory hash operators (Stage 8): hash aggregate, hash
//! DISTINCT, and hash join.
//!
//! A [`HashKey`] wraps a tuple of [`SqlValue`]s and gives it a `Hash`/`Eq` pair
//! whose equality matches `order_key_cmp` (the same equality the old linear
//! grouping used) — so replacing the O(n²) linear-probe grouping with a
//! `HashMap<HashKey, _>` produces identical groups.
//!
//! The one subtlety is `SqlValue`'s cross-type numeric equality
//! (`Int(2) == Decimal(2.0) == Float(2.0) == Bool(true)`). The `Hash`/`Eq`
//! contract only requires `a == b ⇒ hash(a) == hash(b)`; collisions are resolved
//! by `eq`. So every numeric value is hashed by its canonical `f64` image, which
//! collapses all numerically-equal numerics to the same bucket, and `eq`
//! confirms with `order_key_cmp`. Strings/dates/binaries hash by their bytes.
//!
//! NULL hashes to a single sentinel bucket and `order_key_cmp` treats
//! `NULL == NULL` — so, for grouping, NULLs group together (SQL Server GROUP BY
//! semantics). Callers that must *not* match NULLs (equijoins, where
//! `NULL = NULL` is UNKNOWN) filter NULL keys out before building/probing.
//!
//! Consistency holds for keys whose columns are type-homogeneous (the real-world
//! case — a table column has one type, so a group/join key column is one variant
//! modulo NULL). A pathological heterogeneous key (e.g. `GROUP BY CASE WHEN …
//! THEN 'x' ELSE 1 END`, mixing string and numeric in one key position) is the
//! only case where a string that `order_key_cmp`-compares equal to a number
//! could land in a different bucket; that comparison is already ill-defined in
//! the engine (a type-clash compare error is folded to `Equal`), so it is not a
//! supported grouping and is documented as out of scope here.

use std::hash::{Hash, Hasher};

use truthdb_sql::value::{SqlValue, order_key_cmp};

use crate::relstore::types::ColumnType;

/// The hashing family of a column type. Two columns can be joined by the hash
/// path only if their classes match, which guarantees their key values hash the
/// same way (all numerics share one class via the canonical f64 image; VARCHAR
/// and NVARCHAR share the `Str` class because both decode to `SqlValue::Str`).
/// A mismatched pair (e.g. `int = varchar`, which `compare` resolves by string→
/// number coercion) would risk a false separation, so the caller falls back to
/// the nested-loop join for it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HashClass {
    Numeric,
    Str,
    Date,
    Time,
    DateTime2,
    Guid,
    Binary,
}

/// The hash class of a column type (see [`HashClass`]).
pub fn hash_class(ty: ColumnType) -> HashClass {
    match ty {
        ColumnType::TinyInt
        | ColumnType::SmallInt
        | ColumnType::Int
        | ColumnType::BigInt
        | ColumnType::Bit
        | ColumnType::Real
        | ColumnType::Float
        | ColumnType::Decimal { .. } => HashClass::Numeric,
        ColumnType::VarChar { .. } | ColumnType::NVarChar { .. } => HashClass::Str,
        ColumnType::Date => HashClass::Date,
        ColumnType::Time => HashClass::Time,
        ColumnType::DateTime2 => HashClass::DateTime2,
        ColumnType::UniqueIdentifier => HashClass::Guid,
        ColumnType::VarBinary { .. } => HashClass::Binary,
    }
}

/// A group/join key: a tuple of [`SqlValue`]s hashed and compared so that values
/// which `order_key_cmp`-compare `Equal` are the same key.
#[derive(Clone, Debug)]
pub struct HashKey(pub Vec<SqlValue>);

impl PartialEq for HashKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(a, b)| order_key_cmp(a, b) == std::cmp::Ordering::Equal)
    }
}

impl Eq for HashKey {}

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in &self.0 {
            hash_value(value, state);
        }
    }
}

/// True if any component is NULL. Equijoins skip NULL keys (a NULL never matches
/// in `a = b`), while grouping keeps them (NULLs group together).
pub fn key_has_null(values: &[SqlValue]) -> bool {
    values.iter().any(SqlValue::is_null)
}

/// Hashes one value with a per-family discriminant so different families never
/// need equality checks, and with a canonical numeric image so numerically-equal
/// numerics share a bucket (see the module docs).
fn hash_value<H: Hasher>(value: &SqlValue, state: &mut H) {
    match value {
        SqlValue::Null => 0u8.hash(state),
        // Whole numeric family (incl. bit) → canonical f64 bits. `as_numeric`
        // returns `Some` for exactly {Int, Float, Decimal, Bool}.
        SqlValue::Int(_) | SqlValue::Float(_) | SqlValue::Decimal(_) | SqlValue::Bool(_) => {
            1u8.hash(state);
            canonical_f64_bits(value).hash(state);
        }
        SqlValue::Str(s) => {
            2u8.hash(state);
            s.as_bytes().hash(state);
        }
        SqlValue::Date(d) => {
            3u8.hash(state);
            d.hash(state);
        }
        SqlValue::Time(t) => {
            4u8.hash(state);
            t.hash(state);
        }
        SqlValue::DateTime2(d, t) => {
            5u8.hash(state);
            d.hash(state);
            t.hash(state);
        }
        SqlValue::Guid(g) => {
            6u8.hash(state);
            g.hash(state);
        }
        SqlValue::Binary(b) => {
            7u8.hash(state);
            b.hash(state);
        }
    }
}

/// The f64 image of a numeric value, normalized so equal numbers hash equal:
/// `-0.0` folds to `0.0` and any NaN folds to one canonical NaN. Numerically
/// equal values (`Int(2)`, `Decimal(2.0)`, `Float(2.0)`) map to the same bits
/// because `compare_numeric` promotes them through this same `f64` for the
/// float-involved cases, and exact integer/decimal equality implies an equal
/// real value that rounds to one `f64`. Large magnitudes that lose precision
/// only *collide* (resolved by `eq`), never falsely separate.
fn canonical_f64_bits(value: &SqlValue) -> u64 {
    let raw = match value.as_numeric() {
        Some(n) => match n {
            truthdb_sql::value::Numeric::Int(v) => v as f64,
            truthdb_sql::value::Numeric::Float(v) => v,
            truthdb_sql::value::Numeric::Decimal(d) => d.to_f64(),
        },
        // Not reached: callers only pass numeric-family values here.
        None => 0.0,
    };
    let normalized = if raw == 0.0 {
        0.0 // collapses +0.0 and -0.0
    } else if raw.is_nan() {
        f64::NAN
    } else {
        raw
    };
    normalized.to_bits()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    use truthdb_sql::decimal::Decimal;

    fn h(values: Vec<SqlValue>) -> u64 {
        let mut hasher = DefaultHasher::new();
        HashKey(values).hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn numeric_family_hashes_and_compares_equal() {
        let int = SqlValue::Int(2);
        let dec = SqlValue::Decimal(Box::new(Decimal::parse("2.0").unwrap()));
        let flt = SqlValue::Float(2.0);
        let bit = SqlValue::Bool(true); // 1, distinct value
        assert_eq!(h(vec![int.clone()]), h(vec![dec.clone()]));
        assert_eq!(h(vec![int.clone()]), h(vec![flt.clone()]));
        assert_eq!(HashKey(vec![int.clone()]), HashKey(vec![dec]));
        assert_eq!(HashKey(vec![int]), HashKey(vec![flt]));
        assert_ne!(HashKey(vec![SqlValue::Int(2)]), HashKey(vec![bit]));
    }

    #[test]
    fn negative_zero_hashes_with_positive_zero() {
        assert_eq!(h(vec![SqlValue::Float(-0.0)]), h(vec![SqlValue::Float(0.0)]));
        assert_eq!(
            HashKey(vec![SqlValue::Float(-0.0)]),
            HashKey(vec![SqlValue::Int(0)])
        );
    }

    #[test]
    fn nulls_group_together() {
        assert_eq!(h(vec![SqlValue::Null]), h(vec![SqlValue::Null]));
        assert_eq!(HashKey(vec![SqlValue::Null]), HashKey(vec![SqlValue::Null]));
        assert!(key_has_null(&[SqlValue::Int(1), SqlValue::Null]));
        assert!(!key_has_null(&[SqlValue::Int(1)]));
    }

    #[test]
    fn distinct_strings_differ() {
        assert_ne!(
            HashKey(vec![SqlValue::Str("a".into())]),
            HashKey(vec![SqlValue::Str("b".into())])
        );
        assert_eq!(
            HashKey(vec![SqlValue::Str("a".into())]),
            HashKey(vec![SqlValue::Str("a".into())])
        );
    }

    #[test]
    fn multi_column_keys() {
        let a = HashKey(vec![SqlValue::Int(1), SqlValue::Str("x".into())]);
        let b = HashKey(vec![SqlValue::Int(1), SqlValue::Str("x".into())]);
        let c = HashKey(vec![SqlValue::Int(1), SqlValue::Str("y".into())]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
