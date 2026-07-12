//! Collation: maps SQL Server collation names to icu4x collators for
//! ORDER BY (and, later, index sort keys). Comparison strength encodes the
//! case/accent sensitivity suffixes (`_CI`/`_CS`, `_AI`/`_AS`); the locale
//! encodes the linguistic family (e.g. `Finnish_Swedish` -> Swedish, where
//! å/ä/ö sort after z).

use std::cmp::Ordering;

use icu_collator::{Collator, CollatorOptions, Strength};
use icu_locid::Locale;

/// The default database collation when none is configured or named.
pub const DEFAULT_COLLATION: &str = "SQL_Latin1_General_CP1_CI_AS";

pub struct Collation {
    collator: Collator,
    /// A binary (`*_BIN`/`*_BIN2`) collation orders by code point, bypassing
    /// the linguistic collator.
    binary: bool,
    name: String,
}

impl Collation {
    /// Builds a collation from a SQL Server collation name, falling back to a
    /// root collator if the name/locale is unrecognized.
    pub fn from_name(name: &str) -> Collation {
        let lower = name.to_ascii_lowercase();
        let binary = lower.contains("_bin");
        let (locale, strength) = parse_sql_collation(&lower);
        let mut options = CollatorOptions::new();
        options.strength = Some(strength);
        let collator = Collator::try_new(&locale.into(), options).unwrap_or_else(|_| {
            Collator::try_new(&Locale::UND.into(), CollatorOptions::new()).expect("root collator")
        });
        Collation {
            collator,
            binary,
            name: name.to_string(),
        }
    }

    pub fn compare(&self, a: &str, b: &str) -> Ordering {
        if self.binary {
            // Rust's str ordering is UTF-8 byte order, which equals code-point
            // order — SQL Server's binary collation semantics.
            a.cmp(b)
        } else {
            self.collator.compare(a, b)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Derives an icu locale and comparison strength from a lowercased SQL Server
/// collation name.
fn parse_sql_collation(lower: &str) -> (Locale, Strength) {
    let locale: Locale = if lower.contains("finnish_swedish") || lower.contains("swedish") {
        "sv".parse().unwrap()
    } else if lower.contains("danish") || lower.contains("norwegian") {
        "da".parse().unwrap()
    } else if lower.contains("german") {
        "de".parse().unwrap()
    } else if lower.contains("french") {
        "fr".parse().unwrap()
    } else if lower.contains("icelandic") {
        "is".parse().unwrap()
    } else {
        // Latin1_General / SQL_Latin1_General and unknowns use the root locale.
        Locale::UND
    };
    // Case-sensitive (`_CS`) keeps case at tertiary strength; `_CI` with `_AI`
    // ignores both (primary); `_CI` alone keeps accents but ignores case
    // (secondary). (icu cannot express case-sensitive+accent-insensitive, so a
    // `_CS_AI` collation stays accent-sensitive, preserving the case order.)
    let strength = if lower.contains("_cs") {
        Strength::Tertiary
    } else if lower.contains("_ai") {
        Strength::Primary
    } else if lower.contains("_ci") {
        Strength::Secondary
    } else {
        Strength::Tertiary
    };
    (locale, strength)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn swedish_sorts_a_ring_after_z() {
        let coll = Collation::from_name("Finnish_Swedish_CI_AS");
        // In Swedish, 'å'/'ä'/'ö' collate after 'z'.
        assert_eq!(coll.compare("z", "å"), Ordering::Less);
        assert_eq!(coll.compare("ä", "ö"), Ordering::Less);
        assert_eq!(coll.compare("a", "b"), Ordering::Less);
    }

    #[test]
    fn latin1_default_sorts_accents_near_base() {
        let coll = Collation::from_name(DEFAULT_COLLATION);
        // Under the root/Latin1 collation, 'å' sorts near 'a', before 'z'.
        assert_eq!(coll.compare("å", "z"), Ordering::Less);
    }

    #[test]
    fn case_insensitive_default() {
        let coll = Collation::from_name("SQL_Latin1_General_CP1_CI_AS");
        assert_eq!(coll.compare("abc", "ABC"), Ordering::Equal);
    }

    #[test]
    fn case_sensitive_variant() {
        let coll = Collation::from_name("Latin1_General_CS_AS");
        assert_ne!(coll.compare("abc", "ABC"), Ordering::Equal);
    }
}
