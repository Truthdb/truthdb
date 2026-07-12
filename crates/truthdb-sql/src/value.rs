//! SQL values and three-valued logic.
//!
//! Comparisons and boolean connectives follow ANSI/T-SQL semantics: any
//! comparison involving NULL yields UNKNOWN (represented as `Bool(None)`),
//! `AND`/`OR` propagate UNKNOWN per their truth tables, and a WHERE clause
//! keeps a row only when the predicate is definitely TRUE.

use crate::error::{SqlError, SqlResult};

/// A runtime SQL value. The storage layer maps its richer typed values into
/// this smaller set for expression evaluation (integers collapse to `Int`,
/// decimal/float to `Float`, temporal to their tick counts as `Int`, binary
/// to hex `Str`); rendering of result columns uses the storage types
/// directly, so this lossy view is confined to expression evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

/// The truth value of a predicate: TRUE, FALSE, or UNKNOWN (None).
pub type ThreeValued = Option<bool>;

impl SqlValue {
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// Interprets the value as a predicate result for WHERE/HAVING: NULL and
    /// UNKNOWN are not TRUE.
    pub fn as_predicate(&self) -> ThreeValued {
        match self {
            SqlValue::Null => None,
            SqlValue::Bool(b) => Some(*b),
            // Non-boolean in a boolean position is a bind-time error, so this
            // is only reached for already-checked predicates.
            SqlValue::Int(v) => Some(*v != 0),
            SqlValue::Float(v) => Some(*v != 0.0),
            SqlValue::Str(_) => Some(true),
        }
    }

    /// Numeric coercion for arithmetic/comparison; None if not numeric. BIT
    /// is implicitly convertible to a number in T-SQL, so `Bool` participates
    /// (`false`->0, `true`->1).
    fn as_f64(&self) -> Option<f64> {
        match self {
            SqlValue::Int(v) => Some(*v as f64),
            SqlValue::Float(v) => Some(*v),
            SqlValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// Three-valued equality-family comparison. Returns UNKNOWN if either
    /// side is NULL.
    pub fn compare(&self, other: &SqlValue) -> SqlResult<Option<std::cmp::Ordering>> {
        use std::cmp::Ordering;
        if self.is_null() || other.is_null() {
            return Ok(None);
        }
        match (self, other) {
            (SqlValue::Bool(a), SqlValue::Bool(b)) => Ok(Some(a.cmp(b))),
            (SqlValue::Str(a), SqlValue::Str(b)) => Ok(Some(a.cmp(b))),
            // A string on only one side does not implicitly convert to a
            // number here (Stage 3 keeps the implicit-conversion ladder
            // minimal; string vs number is a clash).
            (SqlValue::Str(_), _) | (_, SqlValue::Str(_)) => Err(SqlError::conversion(format!(
                "Operand type clash: {} is incompatible with {}",
                self.type_name(),
                other.type_name()
            ))),
            _ => {
                let (Some(a), Some(b)) = (self.as_f64(), other.as_f64()) else {
                    return Err(SqlError::conversion(format!(
                        "Operand type clash: {} is incompatible with {}",
                        self.type_name(),
                        other.type_name()
                    )));
                };
                Ok(a.partial_cmp(&b).or(Some(Ordering::Equal)))
            }
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            SqlValue::Null => "null",
            SqlValue::Bool(_) => "bit",
            SqlValue::Int(_) => "int",
            SqlValue::Float(_) => "float",
            SqlValue::Str(_) => "varchar",
        }
    }
}

/// Total order for ORDER BY: NULL sorts first (SQL Server default for ASC),
/// then by value. Non-comparable types fall back to a stable type ordering.
pub fn order_key_cmp(a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => Ordering::Less,
        (_, SqlValue::Null) => Ordering::Greater,
        _ => a.compare(b).ok().flatten().unwrap_or(Ordering::Equal),
    }
}

/// `AND` truth table over three-valued logic.
pub fn and(a: ThreeValued, b: ThreeValued) -> ThreeValued {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

/// `OR` truth table over three-valued logic.
pub fn or(a: ThreeValued, b: ThreeValued) -> ThreeValued {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

/// `NOT` over three-valued logic (NOT UNKNOWN = UNKNOWN).
pub fn not(a: ThreeValued) -> ThreeValued {
    a.map(|v| !v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_valued_and_or_not_truth_tables() {
        let t = Some(true);
        let f = Some(false);
        let u: ThreeValued = None;

        // AND
        assert_eq!(and(t, t), t);
        assert_eq!(and(t, f), f);
        assert_eq!(and(f, u), f);
        assert_eq!(and(t, u), u);
        assert_eq!(and(u, u), u);

        // OR
        assert_eq!(or(f, f), f);
        assert_eq!(or(t, u), t);
        assert_eq!(or(f, u), u);
        assert_eq!(or(u, u), u);

        // NOT
        assert_eq!(not(t), f);
        assert_eq!(not(f), t);
        assert_eq!(not(u), u);
    }

    #[test]
    fn comparisons_with_null_are_unknown() {
        assert_eq!(SqlValue::Int(1).compare(&SqlValue::Null).unwrap(), None);
        assert_eq!(SqlValue::Null.compare(&SqlValue::Null).unwrap(), None);
        assert_eq!(
            SqlValue::Int(1).compare(&SqlValue::Int(2)).unwrap(),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn order_puts_nulls_first() {
        let mut values = vec![SqlValue::Int(2), SqlValue::Null, SqlValue::Int(1)];
        values.sort_by(order_key_cmp);
        assert_eq!(
            values,
            vec![SqlValue::Null, SqlValue::Int(1), SqlValue::Int(2)]
        );
    }
}
