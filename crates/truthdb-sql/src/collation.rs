//! Case/accent sensitivity of a collation, for *equality* comparisons.
//!
//! Full linguistic ordering (locale-specific sort, accent-insensitivity) lives
//! behind icu4x in the storage crate and drives `ORDER BY` and index keys. This
//! module carries only the piece that equality operators (`WHERE =`, joins,
//! `GROUP BY`, `DISTINCT`, `MIN`/`MAX`) need: whether two strings that differ
//! only in *case* are equal. The database default collation is case-insensitive
//! (`..._CI_AS`), so string equality is case-insensitive unless a column is
//! explicitly declared `_CS`/`_BIN`.
//!
//! Accent-insensitivity (`_AI`) is **not** modelled here — the default is
//! accent-sensitive, and icu4x 1.5 exposes no public sort-key/fold API to derive
//! a canonical accent-stripped form consistently with the `ORDER BY` collator.
//! An `_AI` collation therefore behaves accent-sensitively for equality (a
//! documented limitation, mirroring `collation.rs`'s note that icu cannot express
//! case-sensitive + accent-insensitive either).

use std::borrow::Cow;
use std::cmp::Ordering;

use crate::value::SqlValue;

/// Whether string equality folds case. Derived from a SQL Server collation name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollationSensitivity {
    /// Case-insensitive (`_CI`) — the database default. `'abc' == 'ABC'`.
    CaseInsensitive,
    /// Case-sensitive (`_CS`) or binary (`_BIN`/`_BIN2`). Compares exactly.
    CaseSensitive,
}

impl CollationSensitivity {
    /// The database default: case-insensitive (matches `DEFAULT_COLLATION`,
    /// `SQL_Latin1_General_CP1_CI_AS`).
    pub const DEFAULT: CollationSensitivity = CollationSensitivity::CaseInsensitive;

    /// Parses the case sensitivity out of a SQL Server collation name. A `_BIN`
    /// or explicit `_CS` name is case-sensitive; everything else (incl. the
    /// default `_CI`) is case-insensitive.
    pub fn from_name(name: &str) -> CollationSensitivity {
        let lower = name.to_ascii_lowercase();
        if lower.contains("_bin") || lower.contains("_cs") {
            CollationSensitivity::CaseSensitive
        } else {
            CollationSensitivity::CaseInsensitive
        }
    }

    /// Parses an optional collation name, defaulting to the database default
    /// (case-insensitive) when absent — a column with no explicit `COLLATE`
    /// inherits the case-insensitive database default.
    pub fn from_optional(name: Option<&str>) -> CollationSensitivity {
        name.map_or(
            CollationSensitivity::DEFAULT,
            CollationSensitivity::from_name,
        )
    }

    /// Combines the sensitivities of the operands of one comparison. A single
    /// explicitly case-sensitive operand forces an exact comparison (this
    /// resolves a mixed `CI`/`CS` comparison conservatively — toward *not*
    /// over-matching — rather than raising SQL Server's collation-conflict
    /// error, which this engine does not model).
    pub fn combine(self, other: CollationSensitivity) -> CollationSensitivity {
        match (self, other) {
            (CollationSensitivity::CaseSensitive, _) | (_, CollationSensitivity::CaseSensitive) => {
                CollationSensitivity::CaseSensitive
            }
            _ => CollationSensitivity::CaseInsensitive,
        }
    }

    /// The canonical form of `s` for this sensitivity: lower-cased under a
    /// case-insensitive collation (so case-variants share a bucket and compare
    /// equal), unchanged under a case-sensitive one. Case-insensitive folding
    /// uses Unicode simple lowercasing, which matches the icu secondary-strength
    /// collator's case equality for realistic (case-only-differing) text; exotic
    /// Unicode case equivalences the collator recognises but lowercasing does not
    /// are a documented edge (they only ever *split* a group, never merge
    /// unequal values).
    pub fn fold<'a>(self, s: &'a str) -> Cow<'a, str> {
        match self {
            CollationSensitivity::CaseSensitive => Cow::Borrowed(s),
            CollationSensitivity::CaseInsensitive => {
                if s.is_ascii() {
                    Cow::Owned(s.to_ascii_lowercase())
                } else {
                    Cow::Owned(s.to_lowercase())
                }
            }
        }
    }

    /// Compares two strings under this sensitivity (case-folded for `_CI`).
    pub fn compare_str(self, a: &str, b: &str) -> Ordering {
        match self {
            CollationSensitivity::CaseSensitive => a.cmp(b),
            CollationSensitivity::CaseInsensitive => self.fold(a).cmp(&self.fold(b)),
        }
    }

    /// Folds a value used as a hash/comparison key: a string is canonicalised for
    /// this sensitivity (so case-insensitive-equal strings share a bucket and
    /// compare equal); every other value is returned unchanged. Only the key is
    /// folded — callers keep the original value for output.
    pub fn fold_value(self, value: SqlValue) -> SqlValue {
        match value {
            SqlValue::Str(s) => SqlValue::Str(self.fold(&s).into_owned()),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_case_insensitive() {
        assert_eq!(
            CollationSensitivity::DEFAULT,
            CollationSensitivity::CaseInsensitive
        );
        assert_eq!(
            CollationSensitivity::from_name("SQL_Latin1_General_CP1_CI_AS"),
            CollationSensitivity::CaseInsensitive
        );
        assert_eq!(
            CollationSensitivity::from_optional(None),
            CollationSensitivity::CaseInsensitive
        );
    }

    #[test]
    fn cs_and_bin_are_case_sensitive() {
        assert_eq!(
            CollationSensitivity::from_name("Latin1_General_CS_AS"),
            CollationSensitivity::CaseSensitive
        );
        assert_eq!(
            CollationSensitivity::from_name("Latin1_General_BIN2"),
            CollationSensitivity::CaseSensitive
        );
    }

    #[test]
    fn ci_folds_case_cs_does_not() {
        assert_eq!(
            CollationSensitivity::CaseInsensitive.compare_str("abc", "ABC"),
            Ordering::Equal
        );
        assert_ne!(
            CollationSensitivity::CaseSensitive.compare_str("abc", "ABC"),
            Ordering::Equal
        );
    }

    #[test]
    fn ci_folds_non_ascii() {
        // 'É' (U+00C9) lowercases to 'é' (U+00E9).
        assert_eq!(
            CollationSensitivity::CaseInsensitive.compare_str("café É", "café é"),
            Ordering::Equal
        );
        // Accent-sensitive: 'é' != 'e' even under CI.
        assert_ne!(
            CollationSensitivity::CaseInsensitive.compare_str("é", "e"),
            Ordering::Equal
        );
    }

    #[test]
    fn combine_prefers_case_sensitive() {
        use CollationSensitivity::*;
        assert_eq!(CaseInsensitive.combine(CaseInsensitive), CaseInsensitive);
        assert_eq!(CaseInsensitive.combine(CaseSensitive), CaseSensitive);
        assert_eq!(CaseSensitive.combine(CaseInsensitive), CaseSensitive);
    }
}
