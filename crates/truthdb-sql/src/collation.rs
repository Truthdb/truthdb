//! Collation: SQL Server collation names mapped to icu4x collators, and the
//! equality/ordering rules the SQL layer applies with them.
//!
//! [`Collation::sort_key`] is what keeps the query layer and the storage layer
//! agreeing on what "equal" means. Two sort keys built at the same strength
//! compare bytewise exactly as the collator compares the strings, so the bytes
//! an index is keyed on and the bytes an equality test compares are the same
//! bytes. Case- and accent-insensitivity fall out of that: at the strength a
//! `_CI`/`_AI` collation implies, `'ABC'`/`'abc'` and `'é'`/`'e'` produce one
//! identical key.
//!
//! This lives in the SQL crate, not the storage crate, precisely so both layers
//! share one definition. When only the storage layer had it, index keys
//! case-folded while `WHERE =` did too and the two happened to agree — but
//! neither could express `_AI`, and any attempt to give the index real
//! linguistic keys immediately disagreed with the filter.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

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

/// Returns the shared [`Collation`] for a collation name (`None` = the database
/// default).
///
/// Building one loads icu data, and key encoding runs on every insert, seek and
/// probe, so each name is built once and shared. Lookups go through a
/// thread-local cache so the hot path takes no lock; the first use of a name on
/// any thread interns it under a global lock. Interned collations are leaked:
/// the set of names a database uses is small and fixed, and a `&'static` keeps
/// the hot path free of reference counting.
pub fn cached(name: Option<&str>) -> &'static Collation {
    thread_local! {
        static LOCAL: RefCell<HashMap<String, &'static Collation>> = RefCell::new(HashMap::new());
    }
    let name = name.unwrap_or(DEFAULT_COLLATION);
    LOCAL.with(|local| {
        if let Some(found) = local.borrow().get(name) {
            return *found;
        }
        let interned = intern(name);
        local.borrow_mut().insert(name.to_string(), interned);
        interned
    })
}

fn intern(name: &str) -> &'static Collation {
    static GLOBAL: OnceLock<Mutex<HashMap<String, &'static Collation>>> = OnceLock::new();
    let mut map = GLOBAL
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("collation cache poisoned");
    if let Some(found) = map.get(name) {
        return found;
    }
    let leaked: &'static Collation = Box::leak(Box::new(Collation::from_name(name)));
    map.insert(name.to_string(), leaked);
    leaked
}

impl std::fmt::Debug for Collation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Collation({})", self.name)
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

    /// Whether this collation compares exactly: `_CS` (case-sensitive) or a
    /// binary collation. Used to resolve a mixed comparison conservatively.
    pub fn is_exact(&self) -> bool {
        let lower = self.name.to_ascii_lowercase();
        self.binary || lower.contains("_cs")
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

use std::borrow::Cow;

use crate::value::SqlValue;

/// The collation a comparison runs under: the rules for what counts as equal
/// and how values order. Copy, and cheap to pass around — it is a handle on an
/// interned [`Collation`].
#[derive(Debug, Clone, Copy)]
pub struct CollationSensitivity(&'static Collation);

impl PartialEq for CollationSensitivity {
    fn eq(&self, other: &Self) -> bool {
        // Interned, so identity is name equality.
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for CollationSensitivity {}

impl CollationSensitivity {
    /// The database default (`SQL_Latin1_General_CP1_CI_AS`): case-insensitive,
    /// accent-sensitive.
    pub fn default_collation() -> CollationSensitivity {
        CollationSensitivity(cached(None))
    }

    /// The collation named by a SQL Server collation name.
    pub fn from_name(name: &str) -> CollationSensitivity {
        CollationSensitivity(cached(Some(name)))
    }

    /// The collation of an optional name, defaulting to the database default —
    /// a column with no explicit `COLLATE` inherits it.
    pub fn from_optional(name: Option<&str>) -> CollationSensitivity {
        CollationSensitivity(cached(name))
    }

    /// The collation to compare two operands under. A single explicitly
    /// case-sensitive or binary operand forces its collation on the comparison,
    /// resolving a mixed comparison conservatively — toward *not* over-matching
    /// — rather than raising SQL Server's collation-conflict error, which this
    /// engine does not model.
    pub fn combine(self, other: CollationSensitivity) -> CollationSensitivity {
        if self.0.is_exact() {
            self
        } else if other.0.is_exact() {
            other
        } else {
            self
        }
    }

    /// The bytes this collation compares `s` by: its sort key. Two strings are
    /// equal under the collation exactly when these are equal, and order the
    /// same way, which is what lets an index keyed on these bytes agree with
    /// every equality test and every `ORDER BY`.
    pub fn key(self, s: &str) -> Vec<u8> {
        self.0.sort_key(s)
    }

    /// A case-folded form of `s`, for `LIKE`.
    ///
    /// `LIKE` matches patterns rather than whole values, so it cannot use a sort
    /// key: a key is only comparable as a whole. It therefore folds case, which
    /// is exact for `_CI`, and leaves accents alone — so `LIKE` under an `_AI`
    /// collation stays accent-sensitive where `=` does not. That is a real and
    /// narrow divergence, and the only place the old case-folding approximation
    /// survives.
    pub fn fold<'a>(self, s: &'a str) -> Cow<'a, str> {
        if self.0.is_exact() {
            Cow::Borrowed(s)
        } else if s.is_ascii() {
            Cow::Owned(s.to_ascii_lowercase())
        } else {
            Cow::Owned(s.to_lowercase())
        }
    }

    /// Compares two strings under this collation: linguistic order, so a
    /// Swedish collation puts `å` after `z`.
    pub fn compare_str(self, a: &str, b: &str) -> Ordering {
        self.0.compare(a, b)
    }

    /// Canonicalises a value used as a hash/comparison key, so values this
    /// collation calls equal share a bucket and compare equal. A string becomes
    /// its sort key — the same bytes the index is keyed on — and every other
    /// value is unchanged. Only the key is canonicalised; callers keep the
    /// original value for output.
    pub fn fold_value(self, value: SqlValue) -> SqlValue {
        match value {
            SqlValue::Str(s) => SqlValue::Binary(self.0.sort_key(&s)),
            other => other,
        }
    }

    /// This collation's name.
    pub fn name(self) -> &'static str {
        self.0.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ci() -> CollationSensitivity {
        CollationSensitivity::from_name("SQL_Latin1_General_CP1_CI_AS")
    }
    fn cs() -> CollationSensitivity {
        CollationSensitivity::from_name("Latin1_General_CS_AS")
    }
    fn ai() -> CollationSensitivity {
        CollationSensitivity::from_name("Latin1_General_CI_AI")
    }
    fn bin() -> CollationSensitivity {
        CollationSensitivity::from_name("Latin1_General_BIN2")
    }

    #[test]
    fn default_is_case_insensitive_and_accent_sensitive() {
        let d = CollationSensitivity::default_collation();
        assert_eq!(d.key("abc"), d.key("ABC"), "case-insensitive");
        assert_ne!(d.key("é"), d.key("e"), "accent-sensitive");
        assert_eq!(d, ci(), "the default is the _CI_AS collation");
    }

    #[test]
    fn case_sensitive_and_binary_names_compare_exactly() {
        assert_ne!(cs().key("abc"), cs().key("ABC"));
        assert_ne!(bin().key("abc"), bin().key("ABC"));
    }

    #[test]
    fn accent_insensitive_ignores_accents_and_case() {
        assert_eq!(ai().key("Résumé"), ai().key("resume"));
        assert_eq!(ai().key("é"), ai().key("e"));
    }

    /// The property that keeps the index and the filter agreeing: equality is
    /// sort-key equality, and it is the same key the storage layer encodes.
    #[test]
    fn key_equality_is_the_collations_equality() {
        for (coll, a, b, equal) in [
            (ci(), "abc", "ABC", true),
            (ci(), "é", "e", false),
            (cs(), "abc", "ABC", false),
            (ai(), "Résumé", "resume", true),
            (ai(), "abc", "abd", false),
            (bin(), "a", "a", true),
            (bin(), "a", "A", false),
        ] {
            assert_eq!(
                coll.key(a) == coll.key(b),
                equal,
                "{}: {a:?} vs {b:?}",
                coll.name()
            );
        }
    }

    #[test]
    fn compare_str_is_linguistic() {
        // Swedish puts å after z; the root collation puts it near a.
        let sv = CollationSensitivity::from_name("Finnish_Swedish_CI_AS");
        assert_eq!(sv.compare_str("z", "å"), Ordering::Less);
        assert_eq!(ci().compare_str("å", "z"), Ordering::Less);
        assert_eq!(ci().compare_str("abc", "ABC"), Ordering::Equal);
        assert_ne!(cs().compare_str("abc", "ABC"), Ordering::Equal);
    }

    #[test]
    fn combine_lets_an_exact_operand_win() {
        assert_eq!(ci().combine(cs()), cs());
        assert_eq!(cs().combine(ci()), cs());
        assert_eq!(ci().combine(bin()), bin());
        assert_eq!(ci().combine(ci()), ci());
    }

    #[test]
    fn fold_value_canonicalises_only_strings() {
        assert_eq!(
            ci().fold_value(SqlValue::Str("ABC".into())),
            ci().fold_value(SqlValue::Str("abc".into())),
        );
        assert_eq!(
            ci().fold_value(SqlValue::Int(7)),
            SqlValue::Int(7),
            "non-strings pass through"
        );
    }

    /// LIKE cannot use a sort key (a key only compares whole), so it folds case
    /// and leaves accents alone. Pinned so the divergence stays deliberate.
    #[test]
    fn fold_is_case_only_for_like() {
        assert_eq!(ci().fold("ABC"), "abc");
        assert_eq!(cs().fold("ABC"), "ABC");
        assert_eq!(ai().fold("Résumé").to_lowercase(), "résumé");
    }

    #[test]
    fn interning_makes_the_same_name_the_same_collation() {
        assert_eq!(
            ci(),
            CollationSensitivity::from_name("SQL_Latin1_General_CP1_CI_AS")
        );
        assert_eq!(
            CollationSensitivity::from_optional(None),
            CollationSensitivity::default_collation()
        );
        assert_ne!(ci(), cs());
    }
}
