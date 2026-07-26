use super::*;

impl Parser {
    /// `EXEC[UTE] <proc> [[@name =] <expr> [, ...]]` — the T-SQL text path to
    /// the system procedures. Arguments end at the statement boundary.
    pub(super) fn parse_exec(&mut self) -> SqlResult<Statement> {
        let start = self.bump().span; // EXEC or EXECUTE
        // `EXEC @rc = proc ...` captures the RETURN status.
        let next = &self.tokens[(self.pos + 1).min(self.tokens.len() - 1)];
        let return_var =
            if matches!(self.peek().kind, TokenKind::LocalVar(_)) && next.kind == TokenKind::Eq {
                let token = self.bump();
                let TokenKind::LocalVar(var) = &token.kind else {
                    unreachable!("matched above");
                };
                let var = var.clone();
                self.bump(); // `=`
                Some(var)
            } else {
                None
            };
        let proc = self.parse_name()?;
        let mut args = Vec::new();
        let mut end = proc.span;
        if !self.at_eof() && !self.check(&TokenKind::Semicolon) {
            loop {
                // `@name = expr` names the argument (the `=` after a local
                // variable disambiguates from a positional `@var` argument);
                // a bare expr is positional.
                let next = &self.tokens[(self.pos + 1).min(self.tokens.len() - 1)];
                let name = if matches!(self.peek().kind, TokenKind::LocalVar(_))
                    && next.kind == TokenKind::Eq
                {
                    let token = self.bump();
                    let TokenKind::LocalVar(var) = &token.kind else {
                        unreachable!("matched above");
                    };
                    let named = Name {
                        value: var.clone(),
                        quoted: false,
                        span: token.span,
                    };
                    self.bump(); // `=`
                    Some(named)
                } else {
                    None
                };
                let value = self.parse_expr()?;
                end = value.span;
                let output =
                    if matches!(self.peek_keyword().as_deref(), Some("OUTPUT") | Some("OUT")) {
                        end = self.bump().span;
                        true
                    } else {
                        false
                    };
                args.push(ExecArg {
                    name,
                    value,
                    output,
                });
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        Ok(Statement::Exec(ExecStatement {
            proc,
            return_var,
            args,
            span: start.to(end),
        }))
    }

    /// `IF <condition> <statement> [ELSE <statement>]`. A semicolon after the
    /// THEN statement ends the IF — `; ELSE` is a syntax error, as in T-SQL.
    pub(super) fn parse_if(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("IF")?;
        let condition = self.parse_expr()?;
        self.sub_depth += 1;
        let then_branch = self.parse_statement();
        let else_branch = if then_branch.is_ok() && self.peek_keyword().as_deref() == Some("ELSE") {
            self.bump();
            Some(self.parse_statement())
        } else {
            None
        };
        self.sub_depth -= 1;
        let then_branch = Box::new(then_branch?);
        let else_branch = match else_branch {
            Some(branch) => Some(Box::new(branch?)),
            None => None,
        };
        let end = self.prev_span();
        Ok(Statement::If {
            condition,
            then_branch,
            else_branch,
            span: start.to(end),
        })
    }

    /// `WHILE <condition> <statement>`.
    pub(super) fn parse_while(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("WHILE")?;
        let condition = self.parse_expr()?;
        self.while_depth += 1;
        self.sub_depth += 1;
        let body = self.parse_statement();
        self.sub_depth -= 1;
        self.while_depth -= 1;
        let body = Box::new(body?);
        let end = self.prev_span();
        Ok(Statement::While {
            condition,
            body,
            span: start.to(end),
        })
    }

    pub(super) fn parse_break(&mut self) -> SqlResult<Statement> {
        let span = self.expect_keyword("BREAK")?;
        if self.while_depth == 0 {
            return Err(SqlError::new(
                135,
                15,
                1,
                "Cannot use the BREAK statement outside the scope of a WHILE statement.",
            )
            .at(span));
        }
        Ok(Statement::Break { span })
    }

    pub(super) fn parse_continue(&mut self) -> SqlResult<Statement> {
        let span = self.expect_keyword("CONTINUE")?;
        if self.while_depth == 0 {
            return Err(SqlError::new(
                136,
                15,
                1,
                "Cannot use the CONTINUE statement outside the scope of a WHILE statement.",
            )
            .at(span));
        }
        Ok(Statement::Continue { span })
    }

    /// `RETURN [expr]` — the expression is parsed when the next token can
    /// start one (T-SQL RETURN takes an integer expression).
    pub(super) fn parse_return(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("RETURN")?;
        // A multi-statement TVF's RETURN carries no value — it returns the
        // accumulated result table variable — so never parse an expression.
        if self.in_table_function {
            return Ok(Statement::Return {
                span: start.to(self.prev_span()),
                value: None,
            });
        }
        // A scalar function's RETURN yields its mandatory typed result, so the
        // expression is always parsed (it commonly begins with a word — a
        // column, CASE, CAST, NULL, or a nested call — which the batch/procedure
        // whitelist below deliberately excludes).
        if self.in_function {
            let value = self.parse_expr()?;
            let end = self.prev_span();
            return Ok(Statement::Return {
                span: start.to(end),
                value: Some(value),
            });
        }
        let has_value = matches!(
            self.peek().kind,
            TokenKind::Int(_)
                | TokenKind::Number(_)
                | TokenKind::String(_)
                | TokenKind::LocalVar(_)
                | TokenKind::GlobalVar(_)
                | TokenKind::LParen
                | TokenKind::Minus
                | TokenKind::Plus
        );
        let value = if has_value {
            Some(self.parse_expr()?)
        } else {
            None
        };
        // Batches cannot return a value — SQL Server's compile-time 178.
        // Procedure bodies can (the RETURN status).
        if let Some(value) = &value
            && !self.in_procedure
        {
            return Err(SqlError::new(
                178,
                15,
                1,
                "A RETURN statement with a return value cannot be used in this context.",
            )
            .at(value.span));
        }
        let end = self.prev_span();
        Ok(Statement::Return {
            value,
            span: start.to(end),
        })
    }

    pub(super) fn parse_begin(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("BEGIN")?;
        // `BEGIN` opens a transaction, a `TRY` block, or a plain statement
        // block. (A bare `BEGIN CATCH` is invalid — it is only reachable
        // inside `parse_try_catch`, after `END TRY`.)
        if matches!(self.peek_keyword().as_deref(), Some("TRY")) {
            return self.parse_try_catch(start);
        }
        let mut end = match self.peek_keyword().as_deref() {
            Some("TRAN") | Some("TRANSACTION") => self.bump().span,
            // Anything else: a plain `BEGIN <statements> END` block. An
            // EMPTY block is a syntax error, as in SQL Server.
            _ => {
                let body = self.parse_block()?;
                if body.is_empty() {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
                let end = self.expect_keyword("END")?;
                return Ok(Statement::Block {
                    body,
                    span: start.to(end),
                });
            }
        };
        let name = self.parse_optional_txn_name();
        if let Some(n) = &name {
            end = n.span;
        }
        Ok(Statement::BeginTransaction {
            name,
            span: start.to(end),
        })
    }

    /// `BEGIN TRY <block> END TRY BEGIN CATCH <block> END CATCH` (the opening
    /// `BEGIN` is already consumed).
    fn parse_try_catch(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("TRY")?;
        let try_block = self.parse_block()?;
        self.expect_keyword("END")?;
        self.expect_keyword("TRY")?;
        self.expect_keyword("BEGIN")?;
        self.expect_keyword("CATCH")?;
        let catch_block = self.parse_block()?;
        self.expect_keyword("END")?;
        let end = self.expect_keyword("CATCH")?;
        Ok(Statement::TryCatch {
            try_block,
            catch_block,
            span: start.to(end),
        })
    }

    /// Parses statements up to (but not consuming) the closing `END` of a
    /// `TRY`/`CATCH` block. A nested `BEGIN TRY` is consumed whole by
    /// `parse_statement`, so a top-level `END` here always closes this block.
    fn parse_block(&mut self) -> SqlResult<Vec<Statement>> {
        self.sub_depth += 1;
        let block = self.parse_block_inner();
        self.sub_depth -= 1;
        block
    }

    fn parse_block_inner(&mut self) -> SqlResult<Vec<Statement>> {
        let mut statements = Vec::new();
        loop {
            while self.eat(&TokenKind::Semicolon) {}
            if matches!(self.peek_keyword().as_deref(), Some("END")) || self.at_eof() {
                break;
            }
            statements.push(self.parse_statement()?);
            // A label prefixes the next statement, so it needs no separator after
            // it (matching `parse_statements`).
            let after_label = matches!(statements.last(), Some(Statement::Label { .. }));
            if !after_label
                && !self.at_eof()
                && !matches!(self.peek_keyword().as_deref(), Some("END"))
                && !self.check(&TokenKind::Semicolon)
            {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        Ok(statements)
    }

    /// `<label>:` — a GOTO target.
    pub(super) fn parse_label(&mut self) -> SqlResult<Statement> {
        let start = self.peek().span;
        let name = self.parse_ident()?.value;
        self.expect(&TokenKind::Colon)?;
        let span = start.to(self.prev_span());
        Ok(Statement::Label { name, span })
    }

    /// `GOTO <label>`.
    pub(super) fn parse_goto(&mut self) -> SqlResult<Statement> {
        let start = self.bump().span; // GOTO
        let label = self.parse_ident()?.value;
        let span = start.to(self.prev_span());
        Ok(Statement::Goto { label, span })
    }
}
