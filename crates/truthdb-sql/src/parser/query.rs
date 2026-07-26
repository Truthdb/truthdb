use super::*;

impl Parser {
    // ---- SELECT ---------------------------------------------------------

    /// `WITH name AS (SELECT ...), ... ` — a common-table-expression prefix.
    fn parse_ctes(&mut self) -> SqlResult<Vec<Cte>> {
        // Bound WITH-in-WITH nesting like other recursive parse paths.
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(Self::too_deep());
        }
        self.expect_keyword("WITH")?;
        let mut ctes: Vec<Cte> = Vec::new();
        loop {
            let name = self.parse_name()?;
            if self.check(&TokenKind::LParen) {
                return Err(SqlError::message_only(
                    102,
                    "A column list on a common table expression is not supported yet.",
                ));
            }
            if ctes
                .iter()
                .any(|c| c.name.value.eq_ignore_ascii_case(&name.value))
            {
                return Err(SqlError::new(
                    460,
                    15,
                    1,
                    format!(
                        "Duplicate common table expression name '{}' was specified.",
                        name.value
                    ),
                )
                .at(name.span));
            }
            self.expect_keyword("AS")?;
            self.expect(&TokenKind::LParen)?;
            let query = self.parse_select()?;
            self.expect(&TokenKind::RParen)?;
            ctes.push(Cte {
                name,
                query: Box::new(query),
            });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.depth -= 1;
        Ok(ctes)
    }

    pub(super) fn parse_select(&mut self) -> SqlResult<Select> {
        let ctes = if self.peek_keyword().as_deref() == Some("WITH") {
            self.parse_ctes()?
        } else {
            Vec::new()
        };
        let start = self.expect_keyword("SELECT")?;
        // Optional set quantifier: `SELECT [ALL | DISTINCT]`.
        let distinct = match self.peek_keyword().as_deref() {
            Some("DISTINCT") => {
                self.bump();
                true
            }
            Some("ALL") => {
                self.bump();
                false
            }
            _ => false,
        };
        let top = if self.peek_keyword().as_deref() == Some("TOP") {
            self.bump();
            Some(self.parse_u64_literal()?)
        } else {
            None
        };

        let mut items = Vec::new();
        loop {
            if self.check(&TokenKind::Star) {
                self.bump();
                items.push(SelectItem::Wildcard);
            } else if self.is_qualified_wildcard() {
                // `table.*`
                let name = self.parse_ident()?;
                self.expect(&TokenKind::Dot)?;
                self.expect(&TokenKind::Star)?;
                items.push(SelectItem::QualifiedWildcard(name));
            } else if let Some(target) = self.assignment_target() {
                // `@var = expr` — an assignment SELECT (not the `@var = expr`
                // comparison a WHERE clause would parse).
                self.bump(); // @var
                self.bump(); // =
                let value = self.parse_expr()?;
                items.push(SelectItem::Assign { target, value });
            } else {
                let expr = self.parse_expr()?;
                let alias = self.parse_optional_alias()?;
                items.push(SelectItem::Expr { expr, alias });
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }

        // A SELECT cannot mix variable assignments with result columns (141).
        let assigns = items
            .iter()
            .filter(|i| matches!(i, SelectItem::Assign { .. }))
            .count();
        if assigns != 0 && assigns != items.len() {
            return Err(SqlError::message_only(
                141,
                "A SELECT statement that assigns a value to a variable must not be combined with data-retrieval operations.",
            ));
        }

        let from = if self.peek_keyword().as_deref() == Some("FROM") {
            self.bump();
            Some(self.parse_from()?)
        } else {
            None
        };

        let where_clause = if self.peek_keyword().as_deref() == Some("WHERE") {
            self.bump();
            Some(self.parse_expr()?)
        } else {
            None
        };

        // GROUP BY <expr>, ...
        let mut group_by = Vec::new();
        if self.peek_keyword().as_deref() == Some("GROUP") {
            self.bump();
            self.expect_keyword("BY")?;
            loop {
                group_by.push(self.parse_expr()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }

        // HAVING <predicate>
        let having = if self.peek_keyword().as_deref() == Some("HAVING") {
            self.bump();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let mut order_by = Vec::new();
        if self.peek_keyword().as_deref() == Some("ORDER") {
            self.bump();
            self.expect_keyword("BY")?;
            loop {
                let expr = self.parse_expr()?;
                let descending = match self.peek_keyword().as_deref() {
                    Some("ASC") => {
                        self.bump();
                        false
                    }
                    Some("DESC") => {
                        self.bump();
                        true
                    }
                    _ => false,
                };
                order_by.push(OrderItem { expr, descending });
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }

        Ok(Select {
            ctes,
            top,
            distinct,
            items,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            span: start.to(self.prev_span()),
        })
    }

    fn parse_optional_alias(&mut self) -> SqlResult<Option<Name>> {
        if self.peek_keyword().as_deref() == Some("AS") {
            self.bump();
            return Ok(Some(self.parse_name()?));
        }
        // A bare identifier that is not a clause keyword is an implicit alias.
        if let Some(keyword) = self.peek_keyword() {
            if is_clause_keyword(&keyword) {
                return Ok(None);
            }
            return Ok(Some(self.parse_name()?));
        }
        if matches!(self.peek().kind, TokenKind::Word { quoted: true, .. }) {
            return Ok(Some(self.parse_name()?));
        }
        Ok(None)
    }

    /// If the next two tokens are `@var =`, returns the variable name (the
    /// start of an assignment SELECT item). Peeks only — does not consume.
    fn assignment_target(&self) -> Option<String> {
        let name = match self.tokens.get(self.pos).map(|t| &t.kind) {
            Some(TokenKind::LocalVar(name)) => name.clone(),
            _ => return None,
        };
        match self.tokens.get(self.pos + 1).map(|t| &t.kind) {
            Some(TokenKind::Eq) => Some(name),
            _ => None,
        }
    }

    /// True if the next three tokens are `<word> . *` (a qualified wildcard).
    fn is_qualified_wildcard(&self) -> bool {
        let is_word = matches!(
            self.tokens.get(self.pos).map(|t| &t.kind),
            Some(TokenKind::Word { .. })
        );
        let is_dot = matches!(
            self.tokens.get(self.pos + 1).map(|t| &t.kind),
            Some(TokenKind::Dot)
        );
        let is_star = matches!(
            self.tokens.get(self.pos + 2).map(|t| &t.kind),
            Some(TokenKind::Star)
        );
        is_word && is_dot && is_star
    }

    // ---- FROM / joins ---------------------------------------------------

    /// Parses a FROM clause: a table primary followed by zero or more joins
    /// (comma = CROSS JOIN). Joins are left-associative.
    /// Parses a FROM clause. Comma has the LOWEST precedence (each operand is
    /// a full joined table), so `a, b RIGHT JOIN c` is `a CROSS JOIN (b RIGHT
    /// JOIN c)`, matching SQL Server.
    fn parse_from(&mut self) -> SqlResult<TableRef> {
        let mut left = self.parse_joined_table()?;
        while self.eat(&TokenKind::Comma) {
            let right = self.parse_joined_table()?;
            left = TableRef::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind: JoinKind::Cross,
                on: None,
            };
        }
        Ok(left)
    }

    /// Parses one table reference followed by its JOIN operators (no comma).
    fn parse_joined_table(&mut self) -> SqlResult<TableRef> {
        let mut left = self.parse_table_primary()?;
        loop {
            let kind = match self.peek_keyword().as_deref() {
                Some("INNER") => {
                    self.bump();
                    self.expect_keyword("JOIN")?;
                    JoinKind::Inner
                }
                Some("JOIN") => {
                    self.bump();
                    JoinKind::Inner
                }
                Some("LEFT") => {
                    self.bump();
                    let _ = self.eat_keyword("OUTER");
                    self.expect_keyword("JOIN")?;
                    JoinKind::Left
                }
                Some("RIGHT") => {
                    self.bump();
                    let _ = self.eat_keyword("OUTER");
                    self.expect_keyword("JOIN")?;
                    JoinKind::Right
                }
                Some("FULL") => {
                    self.bump();
                    let _ = self.eat_keyword("OUTER");
                    self.expect_keyword("JOIN")?;
                    JoinKind::Full
                }
                Some("CROSS") => {
                    self.bump();
                    if self.eat_keyword("APPLY") {
                        JoinKind::CrossApply
                    } else {
                        self.expect_keyword("JOIN")?;
                        JoinKind::Cross
                    }
                }
                // `OUTER APPLY` — a standalone OUTER (the LEFT/RIGHT/FULL forms
                // eat their optional OUTER above).
                Some("OUTER") => {
                    self.bump();
                    self.expect_keyword("APPLY")?;
                    JoinKind::OuterApply
                }
                _ => break,
            };
            let right = self.parse_table_primary()?;
            let on = if matches!(
                kind,
                JoinKind::Cross | JoinKind::CrossApply | JoinKind::OuterApply
            ) {
                None
            } else {
                self.expect_keyword("ON")?;
                Some(self.parse_expr()?)
            };
            left = TableRef::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                on,
            };
        }
        Ok(left)
    }

    fn parse_table_primary(&mut self) -> SqlResult<TableRef> {
        // A derived table or a parenthesized group re-enters parse_select /
        // parse_from, so bound the FROM nesting the same way expressions are
        // bounded — otherwise a deeply nested `((( ... )))` overflows the stack
        // and aborts the process. Shares the expression depth budget.
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(Self::too_deep());
        }
        let tref = self.parse_table_primary_body()?;
        self.depth -= 1;
        Ok(tref)
    }

    fn parse_table_primary_body(&mut self) -> SqlResult<TableRef> {
        if self.check(&TokenKind::LParen) {
            // `(SELECT ...)` is a derived table (required alias); any other
            // parenthesized form is a grouped table reference / join.
            if self.peek_keyword_at(1).as_deref() == Some("SELECT") {
                self.bump(); // (
                let subquery = self.parse_select()?;
                self.expect(&TokenKind::RParen)?;
                let alias = self.parse_optional_table_alias()?.ok_or_else(|| {
                    SqlError::message_only(
                        102,
                        "Incorrect syntax: a derived table must have an alias.",
                    )
                })?;
                return Ok(TableRef::Derived {
                    subquery: Box::new(subquery),
                    alias,
                });
            }
            self.bump(); // (
            let inner = self.parse_from()?;
            self.expect(&TokenKind::RParen)?;
            return Ok(inner);
        }
        let name = self.parse_table_name()?;
        // `name ( args )` in table position is a table-valued function call — but
        // a `@t` table variable is never a function call.
        if !name.value.starts_with('@') && self.check(&TokenKind::LParen) {
            self.bump(); // (
            let mut args = Vec::new();
            if !self.check(&TokenKind::RParen) {
                loop {
                    args.push(self.parse_expr()?);
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
            }
            self.expect(&TokenKind::RParen)?;
            let alias = self.parse_optional_table_alias()?;
            return Ok(TableRef::Function { name, args, alias });
        }
        let alias = self.parse_optional_table_alias()?;
        Ok(TableRef::Table { name, alias })
    }

    fn parse_optional_table_alias(&mut self) -> SqlResult<Option<Name>> {
        if self.peek_keyword().as_deref() == Some("AS") {
            self.bump();
            return Ok(Some(self.parse_name()?));
        }
        if let Some(keyword) = self.peek_keyword() {
            if is_clause_keyword(&keyword) || is_join_keyword(&keyword) {
                return Ok(None);
            }
            return Ok(Some(self.parse_name()?));
        }
        if matches!(self.peek().kind, TokenKind::Word { quoted: true, .. }) {
            return Ok(Some(self.parse_name()?));
        }
        Ok(None)
    }
}

/// Keywords that end the SELECT-list / cannot be an implicit alias.
fn is_clause_keyword(keyword: &str) -> bool {
    // `END` closes a block and `ELSE` continues an `IF` — both reserved in
    // T-SQL, so neither is ever an implicit alias. Without this,
    // `SELECT 1 END TRY` would read `END` as the alias for `1`, and
    // `IF c SELECT 1 ELSE SELECT 2` would alias `1` as `ELSE` and silently
    // detach the ELSE branch. (An explicit `AS end` or a delimited `[end]`
    // still aliases, as before.)
    matches!(
        keyword,
        "FROM" | "WHERE" | "ORDER" | "GROUP" | "HAVING" | "AS" | "END" | "ELSE"
    )
}

/// Keywords that introduce a join (so they are not read as a table alias).
fn is_join_keyword(keyword: &str) -> bool {
    matches!(
        keyword,
        "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "ON" | "OUTER"
    )
}
