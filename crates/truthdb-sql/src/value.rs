//! SQL values and three-valued logic.
//!
//! Comparisons and boolean connectives follow ANSI/T-SQL semantics: any
//! comparison involving NULL yields UNKNOWN (represented as `Bool(None)`),
//! `AND`/`OR` propagate UNKNOWN per their truth tables, and a WHERE clause
//! keeps a row only when the predicate is definitely TRUE. Numeric operands
//! (bit/int/decimal/float) inter-compare with SQL Server precedence; a
//! character operand implicitly converts to the other side's type.

use std::cmp::Ordering;

use crate::collation::CollationSensitivity;
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::temporal;

/// A runtime SQL value. Richer than the storage `Datum` set only in that
/// integers of every width collapse to `Int` and real/float to `Float`; the
/// exact types (decimal, temporal, guid, binary) are preserved so evaluation
/// stays faithful.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// Boxed so the 16-byte-aligned `i128` inside `Decimal` does not enlarge
    /// every `SqlValue` (which is held in the recursive evaluator's frames).
    Decimal(Box<Decimal>),
    Str(String),
    /// Days since 0001-01-01.
    Date(u32),
    /// 100ns ticks since midnight.
    Time(u64),
    /// (days since 0001-01-01, 100ns ticks since midnight).
    DateTime2(u32, u64),
    Guid([u8; 16]),
    Binary(Vec<u8>),
}

/// The truth value of a predicate: TRUE, FALSE, or UNKNOWN (None).
pub type ThreeValued = Option<bool>;

/// A value viewed as a number for arithmetic/comparison (bit → 0/1).
#[derive(Debug, Clone, Copy)]
pub enum Numeric {
    Int(i64),
    Decimal(Decimal),
    Float(f64),
}

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
            // A non-boolean in a boolean position is a bind-time error, so this
            // is only reached for already-checked predicates.
            SqlValue::Int(v) => Some(*v != 0),
            SqlValue::Float(v) => Some(*v != 0.0),
            SqlValue::Decimal(d) => Some(!d.is_zero()),
            _ => Some(true),
        }
    }

    /// Numeric view for arithmetic/comparison, or None if not numeric.
    pub fn as_numeric(&self) -> Option<Numeric> {
        match self {
            SqlValue::Int(v) => Some(Numeric::Int(*v)),
            SqlValue::Decimal(d) => Some(Numeric::Decimal(**d)),
            SqlValue::Float(v) => Some(Numeric::Float(*v)),
            SqlValue::Bool(b) => Some(Numeric::Int(*b as i64)),
            _ => None,
        }
    }

    /// Three-valued comparison. UNKNOWN if either side is NULL; otherwise the
    /// operands are brought to a common type (numeric precedence, or a
    /// character operand converted to the other's type).
    pub fn compare(&self, other: &SqlValue) -> SqlResult<Option<Ordering>> {
        if self.is_null() || other.is_null() {
            return Ok(None);
        }
        self.compare_non_null(other).map(Some)
    }

    /// Three-valued comparison under a collation's case sensitivity: like
    /// [`SqlValue::compare`], but two `Str` operands are compared case-folded
    /// when `sensitivity` is case-insensitive (the database default). Every
    /// non-string comparison is identical to `compare`. This is the entry point
    /// for SQL-visible string equality (`WHERE =`, joins, `IN`, `BETWEEN`);
    /// binary/internal comparisons keep using `compare`.
    pub fn compare_collated(
        &self,
        other: &SqlValue,
        sensitivity: CollationSensitivity,
    ) -> SqlResult<Option<Ordering>> {
        if self.is_null() || other.is_null() {
            return Ok(None);
        }
        if let (SqlValue::Str(a), SqlValue::Str(b)) = (self, other) {
            return Ok(Some(sensitivity.compare_str(a, b)));
        }
        self.compare_non_null(other).map(Some)
    }

    fn compare_non_null(&self, other: &SqlValue) -> SqlResult<Ordering> {
        use SqlValue::*;
        // Numeric family (incl. bit) inter-compares.
        if let (Some(a), Some(b)) = (self.as_numeric(), other.as_numeric()) {
            return Ok(compare_numeric(a, b));
        }
        match (self, other) {
            (Str(a), Str(b)) => Ok(a.cmp(b)),
            (Date(a), Date(b)) => Ok(a.cmp(b)),
            (Time(a), Time(b)) => Ok(a.cmp(b)),
            (DateTime2(d1, t1), DateTime2(d2, t2)) => Ok((d1, t1).cmp(&(d2, t2))),
            (Guid(a), Guid(b)) => Ok(a.cmp(b)),
            (Binary(a), Binary(b)) => Ok(a.cmp(b)),
            // Character operand vs a typed operand: convert the string.
            (Str(s), other) | (other, Str(s)) => {
                let converted = convert_string_like(s, other)?;
                // Preserve argument order for the result ordering.
                if matches!(self, Str(_)) {
                    converted.compare_non_null(other)
                } else {
                    other.compare_non_null(&converted)
                }
            }
            _ => Err(clash(self, other)),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            SqlValue::Null => "null",
            SqlValue::Bool(_) => "bit",
            SqlValue::Int(_) => "int",
            SqlValue::Float(_) => "float",
            SqlValue::Decimal(_) => "decimal",
            SqlValue::Str(_) => "varchar",
            SqlValue::Date(_) => "date",
            SqlValue::Time(_) => "time",
            SqlValue::DateTime2(..) => "datetime2",
            SqlValue::Guid(_) => "uniqueidentifier",
            SqlValue::Binary(_) => "varbinary",
        }
    }
}

/// Converts a character operand to the type of `like` for a mixed comparison,
/// following the common SQL Server implicit conversions (char → number/date).
fn convert_string_like(s: &str, like: &SqlValue) -> SqlResult<SqlValue> {
    let fail = || {
        SqlError::conversion(format!(
            "Conversion failed when converting the varchar value '{s}' to data type {}.",
            like.type_name()
        ))
    };
    match like {
        SqlValue::Int(_) | SqlValue::Bool(_) => s
            .trim()
            .parse::<i64>()
            .map(SqlValue::Int)
            .map_err(|_| fail()),
        SqlValue::Float(_) => s
            .trim()
            .parse::<f64>()
            .map(SqlValue::Float)
            .map_err(|_| fail()),
        SqlValue::Decimal(_) => Decimal::parse(s)
            .map(|d| SqlValue::Decimal(Box::new(d)))
            .ok_or_else(fail),
        SqlValue::Date(_) => temporal::parse_date(s).map(SqlValue::Date).ok_or_else(fail),
        SqlValue::Time(_) => temporal::parse_time(s).map(SqlValue::Time).ok_or_else(fail),
        SqlValue::DateTime2(..) => temporal::parse_datetime2(s)
            .map(|(d, t)| SqlValue::DateTime2(d, t))
            .ok_or_else(fail),
        _ => Err(fail()),
    }
}

fn clash(a: &SqlValue, b: &SqlValue) -> SqlError {
    SqlError::conversion(format!(
        "Operand type clash: {} is incompatible with {}",
        a.type_name(),
        b.type_name()
    ))
}

/// Promotes two numerics to a common type and compares (float wins over
/// decimal wins over int).
fn compare_numeric(a: Numeric, b: Numeric) -> Ordering {
    use Numeric::*;
    match (a, b) {
        (Float(_), _) | (_, Float(_)) => {
            let (x, y) = (to_f64(a), to_f64(b));
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
        (Decimal(x), Decimal(y)) => x.cmp(y),
        (Decimal(x), Int(y)) => x.cmp(crate::decimal::Decimal::from_i64(y)),
        (Int(x), Decimal(y)) => crate::decimal::Decimal::from_i64(x).cmp(y),
        (Int(x), Int(y)) => x.cmp(&y),
    }
}

fn to_f64(n: Numeric) -> f64 {
    match n {
        Numeric::Int(v) => v as f64,
        Numeric::Float(v) => v,
        Numeric::Decimal(d) => d.to_f64(),
    }
}

/// Total order for ORDER BY: NULL sorts first (SQL Server default for ASC),
/// then by value. Non-comparable types fall back to a stable type ordering.
pub fn order_key_cmp(a: &SqlValue, b: &SqlValue) -> Ordering {
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

        assert_eq!(and(t, t), t);
        assert_eq!(and(t, f), f);
        assert_eq!(and(f, u), f);
        assert_eq!(and(t, u), u);
        assert_eq!(and(u, u), u);

        assert_eq!(or(f, f), f);
        assert_eq!(or(t, u), t);
        assert_eq!(or(f, u), u);
        assert_eq!(or(u, u), u);

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
            Some(Ordering::Less)
        );
    }

    #[test]
    fn numeric_family_inter_compares() {
        let dec = SqlValue::Decimal(Box::new(Decimal::parse("2.5").unwrap()));
        assert_eq!(
            SqlValue::Int(2).compare(&dec).unwrap(),
            Some(Ordering::Less)
        );
        assert_eq!(
            SqlValue::Float(2.5).compare(&dec).unwrap(),
            Some(Ordering::Equal)
        );
        assert_eq!(
            SqlValue::Bool(true).compare(&SqlValue::Int(1)).unwrap(),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn string_converts_to_number_for_comparison() {
        assert_eq!(
            SqlValue::Str("10".into())
                .compare(&SqlValue::Int(9))
                .unwrap(),
            Some(Ordering::Greater)
        );
        assert!(
            SqlValue::Str("abc".into())
                .compare(&SqlValue::Int(1))
                .is_err()
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
