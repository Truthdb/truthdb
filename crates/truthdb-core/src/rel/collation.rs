//! Collation: maps SQL Server collation names to icu4x collators, for ORDER BY
//! comparison and for index sort keys. Strength encodes the case/accent
//! sensitivity suffixes (`_CI`/`_CS`, `_AI`/`_AS`); the locale encodes the
//! linguistic family (e.g. `Finnish_Swedish` -> Swedish, where å/ä/ö sort after
//! z).
//!
//! A [`Collation::sort_key`] is what carries linguistic order into the storage
//! layer: two sort keys built at the same strength compare bytewise exactly as
//! the collator compares the strings they came from, so a B+ tree ordered by
//! sort-key bytes is ordered linguistically. Case- and accent-insensitivity
//! fall out of the same mechanism — at primary strength `é` and `e` produce
//! identical keys — so an index over them matches insensitively with no
//! separate folding step.

use std::cmp::Ordering;

use icu_collator::options::{CollatorOptions, Strength};
use icu_collator::{CollationKeySink, CollatorBorrowed, CollatorPreferences};
use icu_locale_core::Locale;

/// The default database collation when none is configured or named.
pub const DEFAULT_COLLATION: &str = "SQL_Latin1_General_CP1_CI_AS";

pub struct Collation {
    collator: CollatorBorrowed<'static>,
    /// A binary (`*_BIN`/`*_BIN2`) collation orders by code point, bypassing
    /// the linguistic collator.
    binary: bool,
    name: String,
}

/// Sink state icu4x threads through a sort-key write. We need none of our own.
#[derive(Default)]
struct KeyState;

/// Collects sort-key bytes: icu4x writes a key through a sink rather than
/// returning a buffer.
struct KeyBuf(Vec<u8>);

impl CollationKeySink for KeyBuf {
    type Error = core::convert::Infallible;
    type State = KeyState;
    type Output = ();

    fn write(&mut self, _state: &mut KeyState, buf: &[u8]) -> Result<(), Self::Error> {
        self.0.extend_from_slice(buf);
        Ok(())
    }

    fn finish(&mut self, _state: KeyState) -> Result<Self::Output, Self::Error> {
        Ok(())
    }
}

impl Collation {
    /// Builds a collation from a SQL Server collation name, falling back to a
    /// root collator if the name/locale is unrecognized.
    pub fn from_name(name: &str) -> Collation {
        let lower = name.to_ascii_lowercase();
        let binary = lower.contains("_bin");
        let (locale, strength) = parse_sql_collation(&lower);
        let mut options = CollatorOptions::default();
        options.strength = Some(strength);
        let prefs: CollatorPreferences = locale.into();
        let collator = CollatorBorrowed::try_new(prefs, options).unwrap_or_else(|_| {
            CollatorBorrowed::try_new(Locale::UNKNOWN.into(), CollatorOptions::default())
                .expect("root collator")
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

    /// The collation's sort key for `s`: bytes that compare bytewise the way
    /// [`Self::compare`] compares strings.
    ///
    /// A binary collation has no linguistic key — its order *is* code-point
    /// order — so it yields the string's own UTF-8 bytes, which compare
    /// identically.
    pub fn sort_key(&self, s: &str) -> Vec<u8> {
        if self.binary {
            return s.as_bytes().to_vec();
        }
        let mut buf = KeyBuf(Vec::new());
        // The sink is infallible, so this cannot fail.
        let _ = self.collator.write_sort_key_to(s, &mut buf);
        buf.0
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
        Locale::UNKNOWN
    };
    // `_CS` keeps case at tertiary strength; `_AI` ignores accents (and, at
    // primary, case along with them); `_CI` alone keeps accents but ignores
    // case (secondary). icu strength is a single ladder, so it cannot express
    // case-sensitive-but-accent-insensitive: a `_CS_AI` collation stays
    // accent-sensitive, preserving the case distinction the name asks for.
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

    /// The property the storage layer rests on: ordering sort-key bytes is the
    /// same as asking the collator, so an index keyed on them is in linguistic
    /// order.
    #[test]
    fn sort_keys_order_bytewise_exactly_as_compare_does() {
        for name in [
            "Finnish_Swedish_CI_AS",
            "SQL_Latin1_General_CP1_CI_AS",
            "Latin1_General_CS_AS",
            "Latin1_General_CI_AI",
            "Latin1_General_BIN2",
        ] {
            let coll = Collation::from_name(name);
            let words = [
                "a", "A", "b", "z", "å", "ä", "ö", "é", "e", "Ee", "ee", "", "zz",
            ];
            for x in words {
                for y in words {
                    assert_eq!(
                        coll.sort_key(x).cmp(&coll.sort_key(y)),
                        coll.compare(x, y),
                        "{name}: sort keys must order {x:?} vs {y:?} as compare does"
                    );
                }
            }
        }
    }

    #[test]
    fn swedish_sort_keys_put_a_ring_after_z() {
        // The point of keying an index on sort keys: this ordering holds in raw
        // bytes, where a code-point comparison gets it wrong.
        let coll = Collation::from_name("Finnish_Swedish_CI_AS");
        assert!(coll.sort_key("z") < coll.sort_key("å"));
        // The root collation disagrees, which is what makes the key linguistic
        // rather than one fixed byte order.
        let root = Collation::from_name(DEFAULT_COLLATION);
        assert!(root.sort_key("å") < root.sort_key("z"));
    }

    #[test]
    fn insensitivity_falls_out_of_the_sort_key() {
        // Case-insensitive: 'a' and 'A' share a key, so an index over them
        // matches insensitively with no separate folding step.
        let ci = Collation::from_name("SQL_Latin1_General_CP1_CI_AS");
        assert_eq!(ci.sort_key("abc"), ci.sort_key("ABC"));
        // Accent-insensitive: 'é' and 'e' share a key.
        let ai = Collation::from_name("Latin1_General_CI_AI");
        assert_eq!(ai.sort_key("é"), ai.sort_key("e"));
        assert_eq!(ai.sort_key("Résumé"), ai.sort_key("resume"));
        // Case-sensitive keeps them apart...
        let cs = Collation::from_name("Latin1_General_CS_AS");
        assert_ne!(cs.sort_key("abc"), cs.sort_key("ABC"));
        // ...and accent-sensitive keeps accents apart.
        assert_ne!(ci.sort_key("é"), ci.sort_key("e"));
    }

    #[test]
    fn binary_sort_key_is_the_string_itself() {
        let bin = Collation::from_name("Latin1_General_BIN2");
        assert_eq!(bin.sort_key("abc"), b"abc".to_vec());
        assert!(bin.sort_key("A") < bin.sort_key("a"), "code-point order");
    }
}
