//! Search conditions: the subset of T-SQL's `CONTAINS` syntax this index can
//! answer.
//!
//! ```text
//! condition := and { (OR | '|') and }
//! and       := primary { (AND | '&') [NOT | '!'] primary }
//! primary   := '(' condition ')' | word | '"' words ['*'] '"'
//! ```
//!
//! A quoted phrase matches documents holding all its words (positions are
//! not stored, so word order is not checked); a phrase ending in `*` makes
//! each of its words a prefix. `NOT` is allowed only after `AND`, as in SQL
//! Server. A bare word that splits into several terms (`e-mail`) is read as
//! a phrase.

use crate::tokenize::{MAX_TERM_BYTES, words};

/// The most operators, parentheses, words and phrases one condition holds.
/// Parsing and ranking recurse once per nesting level, so this bounds their
/// stack as well as the work.
pub const MAX_TOKENS: usize = 512;
/// The deepest parentheses may nest.
pub const MAX_NESTING: usize = 32;
/// The most distinct terms one condition looks up.
pub const MAX_TERMS: usize = 256;

/// One term the condition looks up: a whole term, or every term that starts
/// with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaf {
    pub term: String,
    pub prefix: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    /// An index into [`Query::leaves`].
    Leaf(usize),
    And(Vec<Node>),
    Or(Vec<Node>),
    AndNot(Box<Node>, Box<Node>),
}

/// A parsed search condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// The distinct terms the condition looks up.
    pub leaves: Vec<Leaf>,
    pub(crate) root: Node,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError(pub String);

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Open,
    Close,
    And,
    Or,
    Not,
    Phrase(String),
    Word(String),
}

impl Query {
    pub fn parse(condition: &str) -> Result<Query, QueryError> {
        let tokens = lex(condition)?;
        if tokens.len() > MAX_TOKENS {
            return Err(QueryError(format!(
                "The full-text search condition has more than {MAX_TOKENS} words and operators."
            )));
        }
        let mut parser = Parser {
            condition,
            tokens,
            at: 0,
            nesting: 0,
            leaves: Vec::new(),
        };
        let root = parser.or()?;
        if let Some(token) = parser.tokens.get(parser.at) {
            return Err(parser.error_near(token));
        }
        Ok(Query {
            leaves: parser.leaves,
            root,
        })
    }
}

fn lex(condition: &str) -> Result<Vec<Token>, QueryError> {
    let mut tokens = Vec::new();
    let mut chars = condition.char_indices().peekable();
    while let Some((start, c)) = chars.next() {
        match c {
            c if c.is_whitespace() => {}
            '(' => tokens.push(Token::Open),
            ')' => tokens.push(Token::Close),
            '|' => tokens.push(Token::Or),
            '&' => tokens.push(Token::And),
            '!' => tokens.push(Token::Not),
            '"' => {
                let Some(len) = condition[start + 1..].find('"') else {
                    return Err(QueryError(format!(
                        "Unterminated phrase in the full-text search condition '{condition}'."
                    )));
                };
                tokens.push(Token::Phrase(
                    condition[start + 1..start + 1 + len].to_string(),
                ));
                while chars.next_if(|&(at, _)| at <= start + 1 + len).is_some() {}
            }
            _ => {
                let mut end = start + c.len_utf8();
                while let Some((at, c)) =
                    chars.next_if(|&(_, c)| !c.is_whitespace() && !"()|&!\"".contains(c))
                {
                    end = at + c.len_utf8();
                }
                let word = &condition[start..end];
                tokens.push(match word.to_ascii_uppercase().as_str() {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    _ => Token::Word(word.to_string()),
                });
            }
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    condition: &'a str,
    tokens: Vec<Token>,
    at: usize,
    nesting: usize,
    leaves: Vec<Leaf>,
}

impl Parser<'_> {
    fn or(&mut self) -> Result<Node, QueryError> {
        let mut branches = vec![self.and()?];
        while self.eat(&Token::Or) {
            branches.push(self.and()?);
        }
        Ok(if branches.len() == 1 {
            branches.pop().expect("one branch")
        } else {
            Node::Or(branches)
        })
    }

    fn and(&mut self) -> Result<Node, QueryError> {
        let mut node = self.primary()?;
        while self.eat(&Token::And) {
            if self.eat(&Token::Not) {
                node = Node::AndNot(Box::new(node), Box::new(self.primary()?));
            } else {
                node = match (node, self.primary()?) {
                    (Node::And(mut all), next) => {
                        all.push(next);
                        Node::And(all)
                    }
                    (node, next) => Node::And(vec![node, next]),
                };
            }
        }
        Ok(node)
    }

    fn primary(&mut self) -> Result<Node, QueryError> {
        let Some(token) = self.tokens.get(self.at).cloned() else {
            return Err(QueryError(format!(
                "Syntax error: the full-text search condition '{}' ends early.",
                self.condition
            )));
        };
        self.at += 1;
        match token {
            Token::Open => {
                self.nesting += 1;
                if self.nesting > MAX_NESTING {
                    return Err(QueryError(format!(
                        "The full-text search condition nests parentheses more than {MAX_NESTING} deep."
                    )));
                }
                let node = self.or()?;
                self.nesting -= 1;
                if !self.eat(&Token::Close) {
                    return Err(match self.tokens.get(self.at) {
                        Some(token) => self.error_near(token),
                        None => QueryError(format!(
                            "Syntax error: missing ')' in the full-text search condition '{}'.",
                            self.condition
                        )),
                    });
                }
                Ok(node)
            }
            Token::Phrase(text) => {
                let trimmed = text.trim_end();
                let (words, prefix) = match trimmed.strip_suffix('*') {
                    Some(words) => (words, true),
                    None => (trimmed, false),
                };
                self.words(words, prefix, &text)
            }
            Token::Word(word) => self.words(&word, false, &word),
            token => Err(self.error_near(&token)),
        }
    }

    /// The node for the terms of `text`: all of them must match.
    fn words(&mut self, text: &str, prefix: bool, shown: &str) -> Result<Node, QueryError> {
        let mut nodes = Vec::new();
        for term in words(text) {
            if term.len() > MAX_TERM_BYTES {
                return Err(QueryError(format!(
                    "The word '{term}' in the full-text search condition is longer than {MAX_TERM_BYTES} bytes."
                )));
            }
            nodes.push(Node::Leaf(self.leaf(Leaf { term, prefix })?));
        }
        match nodes.len() {
            0 => Err(QueryError(format!(
                "The full-text search condition '{}' has no searchable word in '{shown}'.",
                self.condition
            ))),
            1 => Ok(nodes.pop().expect("one node")),
            _ => Ok(Node::And(nodes)),
        }
    }

    fn leaf(&mut self, leaf: Leaf) -> Result<usize, QueryError> {
        if let Some(index) = self.leaves.iter().position(|known| *known == leaf) {
            return Ok(index);
        }
        if self.leaves.len() == MAX_TERMS {
            return Err(QueryError(format!(
                "The full-text search condition has more than {MAX_TERMS} distinct terms."
            )));
        }
        self.leaves.push(leaf);
        Ok(self.leaves.len() - 1)
    }

    fn eat(&mut self, token: &Token) -> bool {
        let found = self.tokens.get(self.at) == Some(token);
        if found {
            self.at += 1;
        }
        found
    }

    fn error_near(&self, token: &Token) -> QueryError {
        let near = match token {
            Token::Open => "(".to_string(),
            Token::Close => ")".to_string(),
            Token::And => "AND".to_string(),
            Token::Or => "OR".to_string(),
            Token::Not => "NOT".to_string(),
            Token::Phrase(text) => format!("\"{text}\""),
            Token::Word(word) => word.clone(),
        };
        QueryError(format!(
            "Syntax error near '{near}' in the full-text search condition '{}'.",
            self.condition
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(term: &str, prefix: bool) -> Leaf {
        Leaf {
            term: term.to_string(),
            prefix,
        }
    }

    #[test]
    fn a_single_word_is_one_leaf() {
        let query = Query::parse("Cat").unwrap();
        assert_eq!(query.leaves, [leaf("cat", false)]);
        assert_eq!(query.root, Node::Leaf(0));
    }

    #[test]
    fn and_binds_tighter_than_or() {
        let query = Query::parse("a OR b AND c | d & e").unwrap();
        assert_eq!(
            query.root,
            Node::Or(vec![
                Node::Leaf(0),
                Node::And(vec![Node::Leaf(1), Node::Leaf(2)]),
                Node::And(vec![Node::Leaf(3), Node::Leaf(4)]),
            ])
        );
    }

    #[test]
    fn and_not_and_parentheses() {
        let query = Query::parse("(a or b) and not c & !d").unwrap();
        assert_eq!(query, Query::parse("(a or b) and not c &! d").unwrap());
        assert_eq!(
            query.root,
            Node::AndNot(
                Box::new(Node::AndNot(
                    Box::new(Node::Or(vec![Node::Leaf(0), Node::Leaf(1)])),
                    Box::new(Node::Leaf(2)),
                )),
                Box::new(Node::Leaf(3)),
            )
        );
    }

    #[test]
    fn phrases_need_every_word_and_a_star_makes_each_a_prefix() {
        let query = Query::parse("\"New York\" OR \"data bas*\"").unwrap();
        assert_eq!(
            query.leaves,
            [
                leaf("new", false),
                leaf("york", false),
                leaf("data", true),
                leaf("bas", true)
            ]
        );
        assert_eq!(
            query.root,
            Node::Or(vec![
                Node::And(vec![Node::Leaf(0), Node::Leaf(1)]),
                Node::And(vec![Node::Leaf(2), Node::Leaf(3)]),
            ])
        );
        // A bare word that splits is a phrase too; repeated terms share a leaf.
        let query = Query::parse("e-mail and mail").unwrap();
        assert_eq!(query.leaves, [leaf("e", false), leaf("mail", false)]);
    }

    #[test]
    fn malformed_conditions_are_rejected() {
        for bad in [
            "",
            "a b",
            "a and",
            "or a",
            "not a",
            "a or not b",
            "(a",
            "a)",
            "\"a",
            "\"!!\"",
            "a & ()",
            "a ! b",
            "wow!",
            "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa b\"",
        ] {
            assert!(Query::parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn conditions_past_the_limits_are_rejected_not_overflowed() {
        let nested = |depth| format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
        assert!(Query::parse(&nested(MAX_NESTING)).is_ok());
        assert!(Query::parse(&nested(MAX_NESTING + 1)).is_err());
        assert!(Query::parse(&nested(100_000)).is_err());
        let chain = |n: usize| {
            let mut condition = "a".to_string();
            for i in 0..n {
                condition.push_str(&format!(" &! w{i}"));
            }
            condition
        };
        // Each "&!" and its word are three tokens: "&", "!" and the word.
        assert!(Query::parse(&chain((MAX_TOKENS - 1) / 3)).is_ok());
        assert!(Query::parse(&chain((MAX_TOKENS - 1) / 3 + 1)).is_err());
        let phrase: String = (0..=MAX_TERMS).map(|n| format!("w{n} ")).collect();
        assert!(Query::parse(&format!("\"{phrase}\"")).is_err());
        let phrase: String = (0..MAX_TERMS).map(|n| format!("w{n} ")).collect();
        assert!(Query::parse(&format!("\"{phrase}\"")).is_ok());
    }
}
