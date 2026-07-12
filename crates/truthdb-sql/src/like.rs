//! T-SQL `LIKE` pattern matching: `%` (any run), `_` (any one char), `[...]`
//! character classes with ranges and `[^...]` negation, and an optional
//! ESCAPE character. The pattern is tokenized once, then matched with a
//! greedy two-pointer scan that backtracks only on `%`, so it is linear-ish
//! (O(n*m)) with no exponential blow-up.

enum Token {
    /// A run of any characters (`%`).
    AnyRun,
    /// Any single character (`_`).
    AnyOne,
    /// A literal character.
    Lit(char),
    /// A character class: `(negated, ranges)`. A single char c is in-class if
    /// some range lo..=hi contains it.
    Class(bool, Vec<(char, char)>),
}

fn tokenize(pattern: &str, escape: Option<char>) -> Vec<Token> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if Some(c) == escape {
            // The next character is a literal, whatever it is.
            if i + 1 < chars.len() {
                tokens.push(Token::Lit(chars[i + 1]));
                i += 2;
            } else {
                tokens.push(Token::Lit(c));
                i += 1;
            }
            continue;
        }
        match c {
            '%' => {
                tokens.push(Token::AnyRun);
                i += 1;
            }
            '_' => {
                tokens.push(Token::AnyOne);
                i += 1;
            }
            '[' => {
                if let Some((token, consumed)) = parse_class(&chars[i..]) {
                    tokens.push(token);
                    i += consumed;
                } else {
                    // An unterminated '[' is a literal bracket.
                    tokens.push(Token::Lit('['));
                    i += 1;
                }
            }
            _ => {
                tokens.push(Token::Lit(c));
                i += 1;
            }
        }
    }
    tokens
}

/// Parses `[...]` starting at `chars[0] == '['`. Returns (Class, chars consumed)
/// or None if there is no closing `]`.
fn parse_class(chars: &[char]) -> Option<(Token, usize)> {
    let mut i = 1;
    let negated = chars.get(i) == Some(&'^');
    if negated {
        i += 1;
    }
    let mut ranges = Vec::new();
    let start = i;
    while i < chars.len() {
        // A `]` as the first class member is a literal (SQL Server rule); a
        // later `]` closes the class.
        if chars[i] == ']' && i > start {
            return Some((Token::Class(negated, ranges), i + 1));
        }
        // Range `a-z` (the `-` must be between two chars, not last).
        if i + 2 < chars.len() && chars[i + 1] == '-' && chars[i + 2] != ']' {
            ranges.push((chars[i], chars[i + 2]));
            i += 3;
        } else {
            ranges.push((chars[i], chars[i]));
            i += 1;
        }
    }
    None
}

fn class_matches(negated: bool, ranges: &[(char, char)], c: char) -> bool {
    let hit = ranges.iter().any(|(lo, hi)| {
        let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
        c >= *lo && c <= *hi
    });
    hit != negated
}

fn token_matches(token: &Token, c: char) -> bool {
    match token {
        Token::AnyOne => true,
        Token::Lit(l) => *l == c,
        Token::Class(neg, ranges) => class_matches(*neg, ranges, c),
        Token::AnyRun => unreachable!("handled by the scan"),
    }
}

pub fn like_match(text: &str, pattern: &str, escape: Option<char>) -> bool {
    let tokens = tokenize(pattern, escape);
    let s: Vec<char> = text.chars().collect();
    let (mut si, mut ti) = (0usize, 0usize);
    // Backtrack point for the most recent `%`.
    let mut star_ti: Option<usize> = None;
    let mut star_si = 0usize;
    while si < s.len() {
        if ti < tokens.len() && matches!(tokens[ti], Token::AnyRun) {
            star_ti = Some(ti);
            star_si = si;
            ti += 1;
        } else if ti < tokens.len() && token_matches(&tokens[ti], s[si]) {
            si += 1;
            ti += 1;
        } else if let Some(st) = star_ti {
            // Backtrack: let the `%` swallow one more character.
            ti = st + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }
    // Trailing `%` tokens match the empty remainder.
    while ti < tokens.len() && matches!(tokens[ti], Token::AnyRun) {
        ti += 1;
    }
    ti == tokens.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(text: &str, pat: &str) -> bool {
        like_match(text, pat, None)
    }

    #[test]
    fn literals_and_wildcards() {
        assert!(m("abc", "abc"));
        assert!(!m("abc", "abd"));
        assert!(m("abc", "a%"));
        assert!(m("abc", "%c"));
        assert!(m("abc", "a_c"));
        assert!(m("abc", "%"));
        assert!(m("", "%"));
        assert!(!m("abc", "a_"));
        assert!(m("aXXXc", "a%c"));
    }

    #[test]
    fn character_classes() {
        assert!(m("b", "[abc]"));
        assert!(!m("d", "[abc]"));
        assert!(m("m", "[a-z]"));
        assert!(!m("M", "[a-z]"));
        assert!(m("M", "[^a-z]"));
        assert!(m("cat", "[bc]at"));
    }

    #[test]
    fn escape_character() {
        assert!(like_match("50%", "50!%", Some('!')));
        assert!(!like_match("500", "50!%", Some('!')));
        assert!(like_match("a_b", "a!_b", Some('!')));
    }
}
