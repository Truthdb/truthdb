//! Names and limits, as S3 Vectors has them.

/// Dimensions a vector index may have.
pub const MAX_DIMENSION: u16 = 4096;
/// The longest vector key, in bytes.
pub const MAX_KEY_BYTES: usize = 1024;
/// The most metadata keys one vector may have.
pub const MAX_METADATA_KEYS: usize = 50;
/// The most bytes of metadata one vector may have (JSON), and of it the
/// filterable part.
pub const MAX_METADATA_BYTES: usize = 40 * 1024;
pub const MAX_FILTERABLE_BYTES: usize = 2 * 1024;
/// The most non-filterable metadata keys an index may name, and the
/// longest such name.
pub const MAX_NON_FILTERABLE_KEYS: usize = 10;
pub const MAX_METADATA_KEY_NAME: usize = 63;

/// Whether `name` is a valid vector bucket or vector index name: 3 to 63
/// characters of lowercase letters, digits, hyphens and dots, starting and
/// ending with a letter or digit.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (3..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'.')
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
}

/// Whether `key` is a valid vector key: 1 to [`MAX_KEY_BYTES`] bytes.
pub fn valid_key(key: &str) -> bool {
    (1..=MAX_KEY_BYTES).contains(&key.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_s3_vectors_rules() {
        for good in ["abc", "my-index.v2", "0aa", &"a".repeat(63)] {
            assert!(valid_name(good), "{good}");
        }
        for bad in ["ab", "Abc", "-ab", "ab-", "a_b", "a b", &"a".repeat(64)] {
            assert!(!valid_name(bad), "{bad}");
        }
        assert!(valid_key("k"));
        assert!(!valid_key(""));
        assert!(valid_key(&"k".repeat(MAX_KEY_BYTES)));
        assert!(!valid_key(&"k".repeat(MAX_KEY_BYTES + 1)));
    }
}
