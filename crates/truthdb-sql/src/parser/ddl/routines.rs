use super::*;

impl Parser {
    /// `CREATE|ALTER PROC[EDURE] <name> [params] AS <body-to-end-of-batch>`.
    /// The body is validated by parsing (with `RETURN <value>` legal) and
    /// stored as its source text.
    pub(super) fn parse_create_procedure(
        &mut self,
        start: Span,
        alter: bool,
    ) -> SqlResult<Statement> {
        self.bump(); // PROCEDURE | PROC
        if self.in_procedure {
            // No nested CREATE/ALTER PROCEDURE inside a body (SQL Server's
            // 156 class) — without this the inner body-capture would swallow
            // the rest of the outer body.
            return Err(SqlError::new(
                156,
                15,
                1,
                "Incorrect syntax near the keyword 'PROCEDURE'.",
            )
            .at(start));
        }
        if self.statement_index > 0 || self.sub_depth > 0 {
            return Err(SqlError::new(
                111,
                15,
                1,
                "'CREATE/ALTER PROCEDURE' must be the first statement in a query batch.",
            )
            .at(start));
        }
        let name = self.parse_name()?;
        // Parameters: bare or parenthesized, comma-separated.
        let parens = self.eat(&TokenKind::LParen);
        let mut params = Vec::new();
        while matches!(self.peek().kind, TokenKind::LocalVar(_)) {
            let token = self.bump();
            let TokenKind::LocalVar(param_name) = &token.kind else {
                unreachable!("matched above");
            };
            let param_name = param_name.clone();
            let param_start = token.span;
            let (data_type, mut end) = self.parse_data_type()?;
            let default_text = if self.eat(&TokenKind::Eq) {
                let expr_start = self.peek().span.start;
                let expr = self.parse_expr()?;
                end = expr.span;
                Some(
                    self.slice(Span::new(expr_start, expr.span.end))
                        .trim()
                        .to_string(),
                )
            } else {
                None
            };
            let output = if matches!(self.peek_keyword().as_deref(), Some("OUTPUT") | Some("OUT")) {
                end = self.bump().span;
                true
            } else {
                false
            };
            params.push(ProcParam {
                name: param_name,
                data_type,
                default_text,
                output,
                span: param_start.to(end),
            });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        if parens {
            self.expect(&TokenKind::RParen)?;
        }
        self.expect_keyword("AS")?;
        // The body is everything to the end of the batch, stored verbatim;
        // parse it now for validation (SQL Server checks syntax at CREATE).
        let body_start = self.peek().span.start;
        let body = self.src[body_start..].trim().to_string();
        if body.is_empty() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        self.in_procedure = true;
        let validated = (|| -> SqlResult<()> {
            loop {
                while self.eat(&TokenKind::Semicolon) {}
                if self.at_eof() {
                    return Ok(());
                }
                self.nodes = 0;
                let after_label = matches!(self.parse_statement()?, Statement::Label { .. });
                if !after_label && !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            }
        })();
        self.in_procedure = false;
        validated?;
        let span = start.to(self.prev_span());
        Ok(Statement::CreateProcedure(CreateProcedure {
            name,
            params,
            body,
            alter,
            span,
        }))
    }

    pub(super) fn parse_drop_procedure(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // PROCEDURE | PROC
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropProcedure {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// `CREATE|ALTER TRIGGER <name> ON <table> {AFTER|FOR} {INSERT|UPDATE|DELETE}
    /// [,...] AS <body-to-end-of-batch>`. Only AFTER (its `FOR` synonym)
    /// DML triggers are supported; `INSTEAD OF` is rejected as a syntax error.
    pub(super) fn parse_create_trigger(
        &mut self,
        start: Span,
        alter: bool,
    ) -> SqlResult<Statement> {
        self.bump(); // TRIGGER
        if self.in_procedure || self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'TRIGGER'.").at(start),
            );
        }
        if self.statement_index > 0 || self.sub_depth > 0 {
            return Err(SqlError::new(
                111,
                15,
                1,
                "'CREATE/ALTER TRIGGER' must be the first statement in a query batch.",
            )
            .at(start));
        }
        let name = self.parse_name()?;
        self.expect_keyword("ON")?;
        let target = self.parse_name()?;
        // Timing: AFTER (its FOR synonym) or INSTEAD OF.
        let instead_of = match self.peek_keyword().as_deref() {
            Some("AFTER") | Some("FOR") => {
                self.bump();
                false
            }
            Some("INSTEAD") => {
                self.bump();
                self.expect_keyword("OF")?;
                true
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        // The event list: INSERT/UPDATE/DELETE, comma-separated, at least one,
        // deduplicated.
        let mut events = Vec::new();
        loop {
            let event = match self.peek_keyword().as_deref() {
                Some("INSERT") => TriggerEvent::Insert,
                Some("UPDATE") => TriggerEvent::Update,
                Some("DELETE") => TriggerEvent::Delete,
                _ => {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            };
            self.bump();
            if !events.contains(&event) {
                events.push(event);
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.expect_keyword("AS")?;
        // The body is everything to the end of the batch, stored verbatim and
        // re-parsed per firing; parse it now (in the procedure grammar) to
        // validate its syntax at CREATE.
        let body_start = self.peek().span.start;
        let body = self.src[body_start..].trim().to_string();
        if body.is_empty() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        self.in_procedure = true;
        let validated = (|| -> SqlResult<()> {
            loop {
                while self.eat(&TokenKind::Semicolon) {}
                if self.at_eof() {
                    return Ok(());
                }
                self.nodes = 0;
                let after_label = matches!(self.parse_statement()?, Statement::Label { .. });
                if !after_label && !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            }
        })();
        self.in_procedure = false;
        validated?;
        let span = start.to(self.prev_span());
        Ok(Statement::CreateTrigger(CreateTrigger {
            name,
            target,
            events,
            instead_of,
            body,
            alter,
            span,
        }))
    }

    pub(super) fn parse_drop_trigger(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // TRIGGER
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropTrigger {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// `{ENABLE | DISABLE} TRIGGER {<name> | ALL} ON <table>`.
    pub(in crate::parser) fn parse_trigger_state(&mut self) -> SqlResult<Statement> {
        let start = self.peek().span;
        let enable = self.peek_keyword().as_deref() == Some("ENABLE");
        self.bump(); // ENABLE | DISABLE
        self.expect_keyword("TRIGGER")?;
        let trigger = if self.eat_keyword("ALL") {
            None
        } else {
            Some(self.parse_name()?)
        };
        self.expect_keyword("ON")?;
        let table = self.parse_name()?;
        let span = start.to(self.prev_span());
        Ok(Statement::SetTriggerState {
            trigger,
            table,
            enable,
            span,
        })
    }

    /// `CREATE|ALTER FUNCTION <name> ( [params] ) RETURNS <type> AS <body>`.
    /// Only the scalar form is parsed here; the body is validated by parsing it
    /// (with `RETURN <expr>` mandatory) and stored as source text.
    pub(super) fn parse_create_function(
        &mut self,
        start: Span,
        alter: bool,
    ) -> SqlResult<Statement> {
        self.bump(); // FUNCTION
        if self.in_procedure || self.in_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'FUNCTION'.")
                    .at(start),
            );
        }
        if self.statement_index > 0 || self.sub_depth > 0 {
            return Err(SqlError::new(
                111,
                15,
                1,
                "'CREATE/ALTER FUNCTION' must be the first statement in a query batch.",
            )
            .at(start));
        }
        let name = self.parse_name()?;
        // Function parameter lists are always parenthesized (SQL Server requires
        // the parentheses even for a zero-parameter function).
        self.expect(&TokenKind::LParen)?;
        let mut params = Vec::new();
        while matches!(self.peek().kind, TokenKind::LocalVar(_)) {
            let token = self.bump();
            let TokenKind::LocalVar(param_name) = &token.kind else {
                unreachable!("matched above");
            };
            let param_name = param_name.clone();
            let param_start = token.span;
            let (data_type, mut end) = self.parse_data_type()?;
            let default_text = if self.eat(&TokenKind::Eq) {
                let expr_start = self.peek().span.start;
                let expr = self.parse_expr()?;
                end = expr.span;
                Some(
                    self.slice(Span::new(expr_start, expr.span.end))
                        .trim()
                        .to_string(),
                )
            } else {
                None
            };
            // A function parameter cannot be OUTPUT (SQL Server 156-class); the
            // executor re-checks, but reject the keyword early for a clear error.
            if matches!(self.peek_keyword().as_deref(), Some("OUTPUT") | Some("OUT")) {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
            params.push(ProcParam {
                name: param_name,
                data_type,
                default_text,
                output: false,
                span: param_start.to(end),
            });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.expect(&TokenKind::RParen)?;
        self.expect_keyword("RETURNS")?;
        // `RETURNS @t TABLE ( <cols> ) AS BEGIN … RETURN END` is a
        // multi-statement table-valued function: the named table variable is
        // declared here and populated by the body (captured verbatim like a
        // scalar body, re-parsed under the function grammar per call).
        if let TokenKind::LocalVar(var_name) = &self.peek().kind {
            let var_name = var_name.clone();
            self.bump(); // @t
            self.expect_keyword("TABLE")?;
            // Capture the `( <cols> )` source verbatim (re-parsed per call, like
            // the scalar/inline bodies); parse it now to validate it and to find
            // where the column list ends.
            let cols_start = self.peek().span.start;
            self.parse_table_var_columns()?;
            let columns_text = self
                .slice(Span::new(cols_start, self.prev_span().end))
                .trim()
                .to_string();
            self.expect_keyword("AS")?;
            let body_start = self.peek().span.start;
            let body = self.src[body_start..].trim().to_string();
            if body.is_empty() {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
            // Validate the body parses under the multi-statement-TVF grammar
            // (RETURN takes no value here — it returns the accumulated table).
            self.in_table_function = true;
            let validated = (|| -> SqlResult<()> {
                loop {
                    while self.eat(&TokenKind::Semicolon) {}
                    if self.at_eof() {
                        return Ok(());
                    }
                    self.nodes = 0;
                    let after_label = matches!(self.parse_statement()?, Statement::Label { .. });
                    if !after_label && !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                        let token = self.peek().clone();
                        return Err(SqlError::syntax(self.token_text(&token), token.span));
                    }
                }
            })();
            self.in_table_function = false;
            validated?;
            let span = start.to(self.prev_span());
            return Ok(Statement::CreateFunction(CreateFunction {
                name,
                params,
                returns: ReturnsClause::MultiTable {
                    var_name,
                    columns_text,
                },
                body,
                alter,
                span,
            }));
        }
        // `RETURNS TABLE` is an inline table-valued function: its body is a
        // single `AS RETURN ( <select> )` captured as source text and expanded
        // like a parameterized view. Anything else is a scalar return type.
        if self.peek_keyword().as_deref() == Some("TABLE") {
            self.bump(); // TABLE
            self.expect_keyword("AS")?;
            self.expect_keyword("RETURN")?;
            let parens = self.eat(&TokenKind::LParen);
            let select_start = self.peek().span.start;
            let select = self.parse_select()?;
            let select_text = self
                .slice(Span::new(select_start, select.span.end))
                .trim()
                .to_string();
            if parens {
                self.expect(&TokenKind::RParen)?;
            }
            let span = start.to(self.prev_span());
            return Ok(Statement::CreateFunction(CreateFunction {
                name,
                params,
                returns: ReturnsClause::InlineTable,
                body: select_text,
                alter,
                span,
            }));
        }
        let (return_type, _) = self.parse_data_type()?;
        let returns = ReturnsClause::Scalar(return_type);
        self.expect_keyword("AS")?;
        let body_start = self.peek().span.start;
        let body = self.src[body_start..].trim().to_string();
        if body.is_empty() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        self.in_function = true;
        let validated = (|| -> SqlResult<()> {
            loop {
                while self.eat(&TokenKind::Semicolon) {}
                if self.at_eof() {
                    return Ok(());
                }
                self.nodes = 0;
                let after_label = matches!(self.parse_statement()?, Statement::Label { .. });
                if !after_label && !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            }
        })();
        self.in_function = false;
        validated?;
        let span = start.to(self.prev_span());
        Ok(Statement::CreateFunction(CreateFunction {
            name,
            params,
            returns,
            body,
            alter,
            span,
        }))
    }

    pub(super) fn parse_drop_function(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // FUNCTION
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropFunction {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }
}
