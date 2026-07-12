//! T-SQL-flavored lexer. Produces tokens carrying byte-offset spans so the
//! parser and binder can point errors at the exact source text.
//!
//! Handles `--` line comments, `/* */` (nesting) block comments, `'...'`
//! string literals with `''` escaping, `[bracketed]` and `"quoted"`
//! identifiers, numeric literals (integer, decimal, float exponent), and the
//! operator/punctuation set the Stage 3 grammar needs.

use crate::error::{SqlError, SqlResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Span { start, end }
    }

    pub fn to(self, other: Span) -> Span {
        Span::new(self.start, other.end)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// Identifier or keyword (keywords are recognized in the parser by
    /// comparing the normalized text; delimited identifiers never match).
    Word {
        text: String,
        quoted: bool,
    },
    Int(i64),
    /// Numeric/decimal literal text (kept exact; typed at bind time).
    Number(String),
    String(String),
    // Operators / punctuation.
    Comma,
    Semicolon,
    LParen,
    RParen,
    Dot,
    Star,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

impl Token {
    /// The normalized keyword text for a bare (non-delimited) word, else
    /// None. Keyword matching is case-insensitive.
    pub fn keyword(&self) -> Option<String> {
        match &self.kind {
            TokenKind::Word {
                text,
                quoted: false,
            } => Some(text.to_ascii_uppercase()),
            _ => None,
        }
    }
}

pub struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Lexer {
            src: src.as_bytes(),
            pos: 0,
        }
    }

    pub fn tokenize(mut self) -> SqlResult<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            let is_eof = token.kind == TokenKind::Eof;
            tokens.push(token);
            if is_eof {
                return Ok(tokens);
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek2(&self) -> Option<u8> {
        self.src.get(self.pos + 1).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    fn skip_trivia(&mut self) -> SqlResult<()> {
        loop {
            match self.peek() {
                Some(b) if b.is_ascii_whitespace() => {
                    self.pos += 1;
                }
                Some(b'-') if self.peek2() == Some(b'-') => {
                    while let Some(b) = self.peek() {
                        self.pos += 1;
                        if b == b'\n' {
                            break;
                        }
                    }
                }
                Some(b'/') if self.peek2() == Some(b'*') => {
                    let start = self.pos;
                    self.pos += 2;
                    let mut depth = 1;
                    while depth > 0 {
                        match (self.peek(), self.peek2()) {
                            (Some(b'/'), Some(b'*')) => {
                                self.pos += 2;
                                depth += 1;
                            }
                            (Some(b'*'), Some(b'/')) => {
                                self.pos += 2;
                                depth -= 1;
                            }
                            (Some(_), _) => self.pos += 1,
                            (None, _) => {
                                // SQL Server error 113 for an unterminated
                                // block comment.
                                return Err(SqlError::message_only(
                                    113,
                                    "Missing end comment mark '*/'.",
                                )
                                .at(Span::new(start, self.pos)));
                            }
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn next_token(&mut self) -> SqlResult<Token> {
        self.skip_trivia()?;
        let start = self.pos;
        let Some(b) = self.peek() else {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span::new(start, start),
            });
        };

        let single = |kind: TokenKind, len: usize| Token {
            kind,
            span: Span::new(start, start + len),
        };

        match b {
            b',' => {
                self.pos += 1;
                Ok(single(TokenKind::Comma, 1))
            }
            b';' => {
                self.pos += 1;
                Ok(single(TokenKind::Semicolon, 1))
            }
            b'(' => {
                self.pos += 1;
                Ok(single(TokenKind::LParen, 1))
            }
            b')' => {
                self.pos += 1;
                Ok(single(TokenKind::RParen, 1))
            }
            b'.' if !self.peek2().is_some_and(|c| c.is_ascii_digit()) => {
                self.pos += 1;
                Ok(single(TokenKind::Dot, 1))
            }
            b'*' => {
                self.pos += 1;
                Ok(single(TokenKind::Star, 1))
            }
            b'+' => {
                self.pos += 1;
                Ok(single(TokenKind::Plus, 1))
            }
            b'-' => {
                self.pos += 1;
                Ok(single(TokenKind::Minus, 1))
            }
            b'/' => {
                self.pos += 1;
                Ok(single(TokenKind::Slash, 1))
            }
            b'%' => {
                self.pos += 1;
                Ok(single(TokenKind::Percent, 1))
            }
            b'=' => {
                self.pos += 1;
                Ok(single(TokenKind::Eq, 1))
            }
            b'<' => {
                self.pos += 1;
                match self.peek() {
                    Some(b'=') => {
                        self.pos += 1;
                        Ok(single(TokenKind::Le, 2))
                    }
                    Some(b'>') => {
                        self.pos += 1;
                        Ok(single(TokenKind::Ne, 2))
                    }
                    _ => Ok(single(TokenKind::Lt, 1)),
                }
            }
            b'>' => {
                self.pos += 1;
                if self.peek() == Some(b'=') {
                    self.pos += 1;
                    Ok(single(TokenKind::Ge, 2))
                } else {
                    Ok(single(TokenKind::Gt, 1))
                }
            }
            b'!' if self.peek2() == Some(b'=') => {
                self.pos += 2;
                Ok(single(TokenKind::Ne, 2))
            }
            b'\'' => self.lex_string(start),
            b'[' => self.lex_bracket_ident(start),
            b'"' => self.lex_quoted_ident(start),
            b'@' => Err(
                SqlError::message_only(102, "Variables are not supported yet.")
                    .at(Span::new(start, start + 1)),
            ),
            _ if b.is_ascii_digit() => self.lex_number(start),
            _ if is_ident_start(b) => self.lex_word(start),
            _ => {
                let ch = b as char;
                Err(SqlError::syntax(ch, Span::new(start, start + 1)))
            }
        }
    }

    fn lex_string(&mut self, start: usize) -> SqlResult<Token> {
        self.pos += 1; // opening quote
        let mut bytes = Vec::new();
        loop {
            match self.bump() {
                Some(b'\'') => {
                    if self.peek() == Some(b'\'') {
                        self.pos += 1;
                        bytes.push(b'\'');
                    } else {
                        return Ok(Token {
                            kind: TokenKind::String(decode_utf8(bytes)?),
                            span: Span::new(start, self.pos),
                        });
                    }
                }
                Some(b) => bytes.push(b),
                None => return Err(SqlError::unterminated_string(Span::new(start, self.pos))),
            }
        }
    }

    fn lex_bracket_ident(&mut self, start: usize) -> SqlResult<Token> {
        self.pos += 1; // [
        let mut bytes = Vec::new();
        loop {
            match self.bump() {
                Some(b']') => {
                    if self.peek() == Some(b']') {
                        self.pos += 1;
                        bytes.push(b']');
                    } else {
                        return Ok(Token {
                            kind: TokenKind::Word {
                                text: decode_utf8(bytes)?,
                                quoted: true,
                            },
                            span: Span::new(start, self.pos),
                        });
                    }
                }
                Some(b) => bytes.push(b),
                None => {
                    return Err(
                        SqlError::message_only(102, "Unclosed delimited identifier '['.")
                            .at(Span::new(start, self.pos)),
                    );
                }
            }
        }
    }

    fn lex_quoted_ident(&mut self, start: usize) -> SqlResult<Token> {
        self.pos += 1; // opening quote
        let mut bytes = Vec::new();
        loop {
            match self.bump() {
                Some(b'"') => {
                    if self.peek() == Some(b'"') {
                        self.pos += 1;
                        bytes.push(b'"');
                    } else {
                        return Ok(Token {
                            kind: TokenKind::Word {
                                text: decode_utf8(bytes)?,
                                quoted: true,
                            },
                            span: Span::new(start, self.pos),
                        });
                    }
                }
                Some(b) => bytes.push(b),
                None => {
                    return Err(SqlError::message_only(102, "Unclosed quoted identifier.")
                        .at(Span::new(start, self.pos)));
                }
            }
        }
    }

    fn lex_word(&mut self, start: usize) -> SqlResult<Token> {
        while self.peek().is_some_and(is_ident_cont) {
            self.pos += 1;
        }
        let text = std::str::from_utf8(&self.src[start..self.pos])
            .expect("ascii identifier")
            .to_string();
        Ok(Token {
            kind: TokenKind::Word {
                text,
                quoted: false,
            },
            span: Span::new(start, self.pos),
        })
    }

    fn lex_number(&mut self, start: usize) -> SqlResult<Token> {
        let mut is_float = false;
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            while self.peek().is_some_and(|b| b.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !self.peek().is_some_and(|b| b.is_ascii_digit()) {
                return Err(SqlError::syntax("e", Span::new(start, self.pos)));
            }
            while self.peek().is_some_and(|b| b.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        let text = std::str::from_utf8(&self.src[start..self.pos])
            .expect("ascii number")
            .to_string();
        let span = Span::new(start, self.pos);
        if is_float {
            Ok(Token {
                kind: TokenKind::Number(text),
                span,
            })
        } else {
            match text.parse::<i64>() {
                Ok(v) => Ok(Token {
                    kind: TokenKind::Int(v),
                    span,
                }),
                // Too large for i64: keep as an exact numeric literal.
                Err(_) => Ok(Token {
                    kind: TokenKind::Number(text),
                    span,
                }),
            }
        }
    }
}

/// Decodes accumulated literal bytes as UTF-8 (the source is valid UTF-8 and
/// only whole byte sequences are collected, so this succeeds for well-formed
/// input; a lone stray byte inside a multibyte sequence yields a clean error
/// instead of the old Latin-1 mojibake).
fn decode_utf8(bytes: Vec<u8>) -> SqlResult<String> {
    String::from_utf8(bytes)
        .map_err(|_| SqlError::message_only(102, "Invalid UTF-8 in a literal or identifier."))
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'#'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'#' || b == b'$'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(sql: &str) -> Vec<TokenKind> {
        Lexer::new(sql)
            .tokenize()
            .expect("tokenize")
            .into_iter()
            .map(|t| t.kind)
            .collect()
    }

    #[test]
    fn operators_and_punctuation() {
        assert_eq!(
            kinds("<= >= <> != < > = ( ) , ; . * + - / %"),
            vec![
                TokenKind::Le,
                TokenKind::Ge,
                TokenKind::Ne,
                TokenKind::Ne,
                TokenKind::Lt,
                TokenKind::Gt,
                TokenKind::Eq,
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::Comma,
                TokenKind::Semicolon,
                TokenKind::Dot,
                TokenKind::Star,
                TokenKind::Plus,
                TokenKind::Minus,
                TokenKind::Slash,
                TokenKind::Percent,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn strings_with_escapes() {
        assert_eq!(
            kinds("'it''s a test'"),
            vec![TokenKind::String("it's a test".to_string()), TokenKind::Eof]
        );
        assert!(Lexer::new("'unterminated").tokenize().is_err());
    }

    #[test]
    fn unicode_in_strings_and_idents_is_utf8() {
        // Latin-1-per-byte decoding would mangle these; UTF-8 keeps them.
        assert_eq!(
            kinds("'café åäö 😀'"),
            vec![TokenKind::String("café åäö 😀".to_string()), TokenKind::Eof]
        );
        assert_eq!(
            kinds("[café] \"naïve\""),
            vec![
                TokenKind::Word {
                    text: "café".to_string(),
                    quoted: true
                },
                TokenKind::Word {
                    text: "naïve".to_string(),
                    quoted: true
                },
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_block_comment_is_error_113() {
        let err = Lexer::new("SELECT 1 /* open").tokenize().unwrap_err();
        assert_eq!(err.number, 113);
    }

    #[test]
    fn identifiers_bracketed_and_quoted() {
        assert_eq!(
            kinds("[order] \"select\" plain"),
            vec![
                TokenKind::Word {
                    text: "order".to_string(),
                    quoted: true
                },
                TokenKind::Word {
                    text: "select".to_string(),
                    quoted: true
                },
                TokenKind::Word {
                    text: "plain".to_string(),
                    quoted: false
                },
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn numbers_int_float_decimal() {
        assert_eq!(
            kinds("42 3.14 1e10 2.5E-3"),
            vec![
                TokenKind::Int(42),
                TokenKind::Number("3.14".to_string()),
                TokenKind::Number("1e10".to_string()),
                TokenKind::Number("2.5E-3".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn comments_line_and_nested_block() {
        assert_eq!(
            kinds("1 -- comment\n /* a /* nested */ b */ 2"),
            vec![TokenKind::Int(1), TokenKind::Int(2), TokenKind::Eof]
        );
        assert!(Lexer::new("/* unterminated").tokenize().is_err());
    }

    #[test]
    fn spans_point_at_source() {
        let tokens = Lexer::new("select  x").tokenize().unwrap();
        assert_eq!(tokens[0].span, Span::new(0, 6));
        assert_eq!(tokens[1].span, Span::new(8, 9));
    }
}
