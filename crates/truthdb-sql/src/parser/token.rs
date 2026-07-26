use super::*;

impl Parser {
    // ---- helpers --------------------------------------------------------

    /// Parses a possibly schema-qualified name (`schema.name`), joining the
    /// parts with `.` into a single value (e.g. `sys.tables`). Stage 3 has
    /// one user schema (`dbo`) plus the `sys` catalog views; deeper
    /// qualification is left to later stages.
    /// Parses a name in TABLE position (an INSERT/UPDATE/DELETE target or a FROM
    /// primary): a `@t` local variable there names a table variable, kept with
    /// its leading `@` in `Name.value` as the marker the executor detects (the
    /// catalog resolver never matches a name containing `@`). Any other name is
    /// an ordinary (possibly schema-qualified) table name.
    pub(super) fn parse_table_name(&mut self) -> SqlResult<Name> {
        if let TokenKind::LocalVar(name) = &self.peek().kind {
            let name = name.clone();
            let span = self.bump().span;
            return Ok(Name {
                value: format!("@{name}"),
                quoted: false,
                span,
            });
        }
        self.parse_name()
    }

    pub(super) fn parse_name(&mut self) -> SqlResult<Name> {
        let first = self.parse_ident()?;
        let mut value = first.value;
        let mut span = first.span;
        while self.check(&TokenKind::Dot) {
            self.bump();
            let part = self.parse_ident()?;
            value.push('.');
            value.push_str(&part.value);
            span = span.to(part.span);
        }
        Ok(Name {
            value,
            quoted: first.quoted,
            span,
        })
    }

    pub(super) fn parse_ident(&mut self) -> SqlResult<Name> {
        let token = self.peek().clone();
        match &token.kind {
            TokenKind::Word { text, quoted } => {
                if !quoted && is_reserved(&text.to_ascii_uppercase()) {
                    return Err(SqlError::syntax(text, token.span));
                }
                self.bump();
                Ok(Name {
                    value: text.clone(),
                    quoted: *quoted,
                    span: token.span,
                })
            }
            _ => Err(SqlError::syntax(self.token_text(&token), token.span)),
        }
    }

    pub(super) fn parse_u32_literal(&mut self) -> SqlResult<u32> {
        let value = self.parse_u64_literal()?;
        u32::try_from(value)
            .map_err(|_| SqlError::message_only(1073, "Length value is out of range."))
    }

    /// Parses a signed integer literal (optional leading `-`), for IDENTITY
    /// seed/increment.
    pub(super) fn parse_i64_literal(&mut self) -> SqlResult<i64> {
        let negative = self.eat(&TokenKind::Minus);
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Int(v) => {
                self.bump();
                Ok(if negative { -v } else { v })
            }
            _ => Err(SqlError::syntax(self.token_text(&token), token.span)),
        }
    }

    pub(super) fn parse_u64_literal(&mut self) -> SqlResult<u64> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Int(v) if v >= 0 => {
                self.bump();
                Ok(v as u64)
            }
            _ => Err(SqlError::syntax(self.token_text(&token), token.span)),
        }
    }

    pub(super) fn peek(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    pub(super) fn peek_keyword(&self) -> Option<String> {
        self.peek().keyword()
    }

    /// The keyword `offset` tokens ahead of the cursor (for two-token lookahead
    /// like `NOT LIKE`).
    pub(super) fn peek_keyword_at(&self, offset: usize) -> Option<String> {
        self.tokens
            .get((self.pos + offset).min(self.tokens.len() - 1))
            .and_then(|t| t.keyword())
    }

    pub(super) fn bump(&mut self) -> Token {
        let token = self.peek().clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        token
    }

    pub(super) fn prev_span(&self) -> Span {
        let index = self.pos.saturating_sub(1).min(self.tokens.len() - 1);
        self.tokens[index].span
    }

    pub(super) fn check(&self, kind: &TokenKind) -> bool {
        &self.peek().kind == kind
    }

    pub(super) fn eat(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) {
            self.bump();
            true
        } else {
            false
        }
    }

    pub(super) fn expect(&mut self, kind: &TokenKind) -> SqlResult<Span> {
        if self.check(kind) {
            Ok(self.bump().span)
        } else {
            let token = self.peek().clone();
            Err(SqlError::syntax(self.token_text(&token), token.span))
        }
    }

    pub(super) fn expect_keyword(&mut self, keyword: &str) -> SqlResult<Span> {
        if self.peek_keyword().as_deref() == Some(keyword) {
            Ok(self.bump().span)
        } else {
            let token = self.peek().clone();
            Err(SqlError::syntax(self.token_text(&token), token.span))
        }
    }

    pub(super) fn at_eof(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    pub(super) fn token_text(&self, token: &Token) -> String {
        match &token.kind {
            TokenKind::Eof => "<eof>".to_string(),
            TokenKind::Word { text, .. } => text.clone(),
            TokenKind::Int(v) => v.to_string(),
            TokenKind::Number(t) => t.clone(),
            TokenKind::String(s) => format!("'{s}'"),
            other => format!("{other:?}"),
        }
    }
}
