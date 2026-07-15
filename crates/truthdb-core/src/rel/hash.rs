//! Hash keys for the in-memory hash operators (Stage 8): hash aggregate, hash
//! DISTINCT, and hash join.
//!
//! A [`HashKey`] wraps a tuple of [`SqlValue`]s and gives it a `Hash`/`Eq` pair
//! whose equality matches `order_key_cmp` (the same equality the old linear
//! grouping used) — so replacing the O(n²) linear-probe grouping with a
//! `HashMap<HashKey, _>` produces identical groups.
//!
//! The one subtlety is `SqlValue`'s cross-type numeric equality. `Int`/`Bit`/
//! `Decimal` inter-compare **exactly** (i128 rescale), while any comparison
//! involving a `Float` promotes both sides to `f64`. These are two different
//! equivalence regimes, and for large magnitudes they even disagree (exact
//! `Int == Decimal` need not hold once rounded to `f64` — `to_f64` on a Decimal
//! double-rounds), so **no single hash can be consistent with all of them**.
//! We therefore hash each regime by its own exact/canonical form and keep them
//! apart:
//! - `Int`/`Bit`/`Decimal` hash by their exact canonical rational
//!   (`m·10^e` with `m` not divisible by 10) — so `Int(9e18)` and the equal
//!   `Decimal` land in the same bucket, and cross-scale decimals (`2.50` vs
//!   `2.5`) canonicalize identically. No float rounding is involved.
//! - `Float` hashes by its `f64` bits.
//!
//! `hash_class` (used by the hash-join guard) puts the exact family
//! (`ExactNumeric`) and the float family (`ApproxNumeric`) in **different**
//! classes, so a `float = int`/`float = decimal` join — whose equality regime is
//! `f64` and cannot share a hash with the exact family — falls back to the
//! nested loop rather than risk a false separation. Strings/dates/binaries hash
//! by their bytes.
//!
//! The `Hash`/`Eq` contract only requires `a == b ⇒ hash(a) == hash(b)`;
//! collisions are resolved by `eq` (which uses `order_key_cmp`). Consistency
//! holds whenever a key column is type-homogeneous (the real-world case — a
//! table column is one type, and the join guard confines each hash join to one
//! numeric regime). Mixing `Float` with `Int`/`Decimal` in one key position
//! (only reachable via a pathological heterogeneous key expression) is the sole
//! unsupported case, as it already is for `order_key_cmp`.
//!
//! NULL hashes to a single sentinel bucket and `order_key_cmp` treats
//! `NULL == NULL` — so, for grouping, NULLs group together (SQL Server GROUP BY
//! semantics). Callers that must *not* match NULLs (equijoins, where
//! `NULL = NULL` is UNKNOWN) filter NULL keys out before building/probing. A
//! string that `order_key_cmp`-compares equal to a number (via the engine's
//! already-ill-defined string↔number coercion) is the same unsupported
//! heterogeneous-key case as the `Float`/exact mix above.

use std::hash::{Hash, Hasher};

use truthdb_sql::collation::CollationSensitivity;
use truthdb_sql::value::{SqlValue, order_key_cmp};

use crate::relstore::types::ColumnType;

/// Folds the string components of a group/DISTINCT key to their collation-
/// canonical form (`sensitivities[i]` governs `key[i]`; a missing entry defaults
/// to the case-insensitive database default), so case-insensitive-equal keys
/// share a bucket and compare `Eq`. The caller keeps the original key for output
/// — this produces only the bucketing/equality key. (Non-string values and
/// case-sensitive columns pass through unchanged.)
pub fn fold_hash_key(key: &[SqlValue], sensitivities: &[CollationSensitivity]) -> Vec<SqlValue> {
    key.iter()
        .enumerate()
        .map(|(index, value)| {
            let sens = sensitivities
                .get(index)
                .copied()
                .unwrap_or(CollationSensitivity::default_collation());
            sens.fold_value(value.clone())
        })
        .collect()
}

/// The hashing family of a column type. Two columns can be joined by the hash
/// path only if their classes match, which guarantees their key values hash the
/// same way. `ExactNumeric` (int family + decimal) uses exact canonical-rational
/// hashing; `ApproxNumeric` (real/float) uses `f64` bits — they are kept
/// separate because their equality regimes (exact i128 vs `f64` promotion)
/// disagree at large magnitudes and cannot share one hash (see the module
/// docs). VARCHAR and NVARCHAR share the `Str` class because both decode to
/// `SqlValue::Str`. A mismatched pair (e.g. `int = varchar`, or `float = int`)
/// would risk a false separation, so the caller keeps the nested-loop join.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HashClass {
    ExactNumeric,
    ApproxNumeric,
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
        | ColumnType::Decimal { .. } => HashClass::ExactNumeric,
        ColumnType::Real | ColumnType::Float => HashClass::ApproxNumeric,
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
/// need equality checks. `Int`/`Bit`/`Decimal` hash by their exact canonical
/// rational; `Float` by its `f64` bits (see the module docs).
fn hash_value<H: Hasher>(value: &SqlValue, state: &mut H) {
    match value {
        SqlValue::Null => 0u8.hash(state),
        // Exact numeric family: canonical rational `m·10^e`. Bit is 0/1.
        SqlValue::Int(v) => {
            1u8.hash(state);
            canonical_rational(*v as i128, 0).hash(state);
        }
        SqlValue::Bool(b) => {
            1u8.hash(state);
            canonical_rational(*b as i128, 0).hash(state);
        }
        SqlValue::Decimal(d) => {
            1u8.hash(state);
            canonical_rational(d.value, d.scale).hash(state);
        }
        // Approximate numeric family: f64 bits, hashed under a distinct
        // discriminant so it never shares a bucket with the exact family (the
        // join guard also keeps the two apart).
        SqlValue::Float(f) => {
            8u8.hash(state);
            float_bits(*f).hash(state);
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

/// The exact canonical rational `(m, e)` for `value·10^(-scale)`, with `m` not
/// divisible by 10 (`0` normalizes to `(0, 0)`). Two exact numerics compare
/// `Equal` iff they have the same canonical rational, so this — unlike an `f64`
/// image — never falsely separates equal `Int`/`Decimal` keys, at any
/// magnitude, and collapses cross-scale decimals (`2.50` and `2.5` → `(25, -1)`).
fn canonical_rational(value: i128, scale: u8) -> (i128, i32) {
    if value == 0 {
        return (0, 0);
    }
    let mut m = value;
    let mut e: i32 = -(i32::from(scale));
    while m % 10 == 0 {
        m /= 10;
        e += 1;
    }
    (m, e)
}

/// `f64` bits normalized so equal floats hash equal: `-0.0` folds to `0.0` and
/// any NaN folds to one canonical NaN.
fn float_bits(f: f64) -> u64 {
    let normalized = if f == 0.0 {
        0.0
    } else if f.is_nan() {
        f64::NAN
    } else {
        f
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
    fn int_and_decimal_hash_and_compare_equal() {
        let int = SqlValue::Int(2);
        let dec = SqlValue::Decimal(Box::new(Decimal::parse("2.0").unwrap()));
        let bit = SqlValue::Bool(true); // 1, distinct value
        assert_eq!(h(vec![int.clone()]), h(vec![dec.clone()]));
        assert_eq!(HashKey(vec![int.clone()]), HashKey(vec![dec]));
        assert_ne!(HashKey(vec![int]), HashKey(vec![bit]));
    }

    #[test]
    fn int_decimal_exact_at_large_magnitude() {
        // Regression: > 2^53, where an f64 image would double-round the Decimal
        // and split the bucket. 9007199254740993 == 9007199254740993.0 exactly.
        let big = 9_007_199_254_740_993_i64;
        let int = SqlValue::Int(big);
        let dec = SqlValue::Decimal(Box::new(Decimal::new(big as i128 * 10, 20, 1)));
        assert_eq!(h(vec![int.clone()]), h(vec![dec.clone()]));
        assert_eq!(HashKey(vec![int]), HashKey(vec![dec]));
    }

    #[test]
    fn cross_scale_decimals_hash_equal() {
        // 2.50 (value 250, scale 2) and 2.5 (value 25, scale 1) are equal.
        let a = SqlValue::Decimal(Box::new(Decimal::new(250, 5, 2)));
        let b = SqlValue::Decimal(Box::new(Decimal::new(25, 5, 1)));
        assert_eq!(h(vec![a.clone()]), h(vec![b.clone()]));
        assert_eq!(HashKey(vec![a]), HashKey(vec![b]));
    }

    #[test]
    fn negative_zero_float_hashes_with_positive_zero() {
        assert_eq!(
            h(vec![SqlValue::Float(-0.0)]),
            h(vec![SqlValue::Float(0.0)])
        );
        assert_eq!(
            HashKey(vec![SqlValue::Float(-0.0)]),
            HashKey(vec![SqlValue::Float(0.0)])
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
