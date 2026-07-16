//! Tripwire: memcmp over `Collation::sort_key` must agree with
//! `Collation::compare` for every pair — the property the NVARCHAR range-seek
//! gate rests on. `write_sort_key_to` lives behind icu_collator's `unstable`
//! feature — explicitly semver-exempt — so an icu update may change sort-key
//! bytes; this test is what turns that into a loud failure instead of range
//! seeks silently dropping rows. Includes supplementary-plane characters, combining
//! marks, contractions (Danish 'aa'), and boundary code points.

use truthdb_sql::collation::Collation;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn corpus() -> Vec<String> {
    // Alphabet stressing every divergence channel:
    // - ASCII upper/lower (case strength)
    // - Latin accents, precomposed AND decomposed (normalization)
    // - Nordic letters (locale tailorings), 'aa' contraction fodder
    // - BMP boundary chars: U+0000-adjacent, U+E000 (private use), U+FFFD,
    //   U+FFFF-1 region
    // - Supplementary plane: U+10000, U+1F600, U+20000, U+10FFFF
    let alphabet: Vec<&str> = vec![
        "a",
        "A",
        "b",
        "z",
        "Z",
        "å",
        "Å",
        "ä",
        "ö",
        "æ",
        "ø",
        "é",
        "e\u{0301}",
        "e",
        "ß",
        "ss",
        "aa",
        "th",
        "þ",
        "\u{E000}",
        "\u{FFFD}",
        "\u{FFFE}",
        "\u{10000}",
        "\u{1F600}",
        "\u{20000}",
        "\u{10FFFF}",
        "0",
        "9",
        " ",
        "-",
        "'",
    ];
    let mut rng = Rng(0x5EEDBEEF_CAFEF00D);
    let mut out: Vec<String> = Vec::new();
    // Fixed adversarial strings first.
    for s in [
        "",
        "a",
        "z",
        "å",
        "\u{1F600}",
        "z\u{1F600}",
        "a\u{10FFFF}",
        "\u{E000}",
        "\u{FFFD}z",
        "aa",
        "ab",
        "b",
        "resume",
        "résumé",
        "Résumé",
        "e\u{0301}",
        "é",
        "\u{10000}",
        "\u{FFFF}",
    ] {
        out.push(s.to_string());
    }
    // Random strings of length 0..=6 over the alphabet.
    for _ in 0..250 {
        let len = (rng.next() % 7) as usize;
        let mut s = String::new();
        for _ in 0..len {
            s.push_str(alphabet[(rng.next() as usize) % alphabet.len()]);
        }
        out.push(s);
    }
    out
}

#[test]
fn sort_key_memcmp_agrees_with_compare_everywhere() {
    let names = [
        "SQL_Latin1_General_CP1_CI_AS",
        "Latin1_General_CS_AS",
        "Latin1_General_CI_AI",
        "Latin1_General_BIN2",
        "Latin1_General_BIN",
        "Finnish_Swedish_CI_AS",
        "Danish_Norwegian_CI_AS",
        "French_CI_AS",
        "German_PhoneBook_CI_AS",
        "Icelandic_CS_AS",
    ];
    let strings = corpus();
    for name in names {
        let coll = Collation::from_name(name);
        let keys: Vec<Vec<u8>> = strings.iter().map(|s| coll.sort_key(s)).collect();
        let mut mismatches = 0;
        for i in 0..strings.len() {
            for j in 0..strings.len() {
                let by_key = keys[i].cmp(&keys[j]);
                let by_cmp = coll.compare(&strings[i], &strings[j]);
                if by_key != by_cmp {
                    mismatches += 1;
                    if mismatches <= 5 {
                        eprintln!(
                            "{name}: {:?} vs {:?}: key {:?} compare {:?}\n  k_i={:02X?}\n  k_j={:02X?}",
                            strings[i], strings[j], by_key, by_cmp, keys[i], keys[j]
                        );
                    }
                }
            }
        }
        assert_eq!(
            mismatches, 0,
            "{name}: sort_key order diverges from compare"
        );
    }
}
