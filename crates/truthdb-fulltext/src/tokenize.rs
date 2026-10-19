//! Splitting text into terms.
//!
//! Words are found with the Unicode word boundaries of UAX #29, so a word
//! keeps its combining marks and Katakana runs stay whole, while Han
//! ideographs and Hiragana come out one character per term. Each word is
//! lowercased. There is no stemming, no stop list and no Unicode
//! normalization: a word written in decomposed form matches only the same
//! decomposed form.

use std::collections::BTreeMap;

use unicode_segmentation::UnicodeSegmentation;

/// Terms longer than this many bytes (after lowercasing) are not indexed.
pub const MAX_TERM_BYTES: usize = 64;

/// The terms of `text`, in order, repeats included.
pub fn terms(text: &str) -> impl Iterator<Item = String> + '_ {
    text.unicode_words()
        .map(str::to_lowercase)
        .filter(|term| term.len() <= MAX_TERM_BYTES)
}

/// What a document contributes to the index: how often each term occurs,
/// and its length in terms.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DocTerms {
    pub counts: BTreeMap<String, u32>,
    pub len: u32,
}

impl DocTerms {
    pub fn from_text(text: &str) -> DocTerms {
        let mut doc = DocTerms::default();
        for term in terms(text) {
            *doc.counts.entry(term).or_insert(0) += 1;
            doc.len = doc.len.saturating_add(1);
        }
        doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(text: &str) -> Vec<String> {
        terms(text).collect()
    }

    #[test]
    fn words_are_lowercased_and_split_on_punctuation() {
        assert_eq!(
            all("Hello, WORLD! It's a test-case: 3.14 x_y"),
            ["hello", "world", "it's", "a", "test", "case", "3.14", "x_y"]
        );
    }

    #[test]
    fn non_latin_words_keep_their_marks() {
        assert_eq!(all("Grüße ΣΟΦΙΑ"), ["grüße", "σοφια"]);
        // The virama (U+094D) is a mark, not a break.
        assert_eq!(all("हिन्दी भाषा"), ["हिन्दी", "भाषा"]);
        assert_eq!(all("Привет мир"), ["привет", "мир"]);
    }

    #[test]
    fn han_and_hiragana_are_one_term_per_character_and_katakana_stays_whole() {
        assert_eq!(all("東京タワーへ"), ["東", "京", "タワー", "へ"]);
    }

    #[test]
    fn overlong_terms_are_dropped() {
        let long = "a".repeat(MAX_TERM_BYTES + 1);
        let fits = "b".repeat(MAX_TERM_BYTES);
        assert_eq!(all(&format!("{long} {fits} c")), [fits.as_str(), "c"]);
    }

    #[test]
    fn a_document_counts_its_terms() {
        let doc = DocTerms::from_text("the cat and THE hat; the end");
        assert_eq!(doc.len, 7);
        assert_eq!(doc.counts["the"], 3);
        assert_eq!(doc.counts["cat"], 1);
        assert_eq!(doc.counts.len(), 5);
        assert_eq!(DocTerms::from_text("  ,.; "), DocTerms::default());
    }
}
