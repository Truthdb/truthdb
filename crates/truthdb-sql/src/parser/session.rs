use super::token::is_reserved;
use super::*;

impl Parser {
    // ---- transaction control --------------------------------------------

    pub(super) fn parse_commit(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("COMMIT")?;
        let end = self.eat_optional_tran_and_name(start);
        Ok(Statement::Commit {
            span: start.to(end),
        })
    }

    pub(super) fn parse_rollback(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("ROLLBACK")?;
        let mut end = start;
        if matches!(
            self.peek_keyword().as_deref(),
            Some("TRAN") | Some("TRANSACTION") | Some("WORK")
        ) {
            end = self.bump().span;
        }
        // A name after ROLLBACK [TRAN] targets a savepoint (partial rollback).
        let name = self.parse_optional_txn_name();
        if let Some(n) = &name {
            end = n.span;
        }
        Ok(Statement::Rollback {
            name,
            span: start.to(end),
        })
    }

    pub(super) fn parse_save(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("SAVE")?;
        // SAVE TRAN[SACTION] <name> — both the keyword and the name are required.
        match self.peek_keyword().as_deref() {
            Some("TRAN") | Some("TRANSACTION") => {
                self.bump();
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        let name = self.parse_name()?;
        let span = start.to(name.span);
        Ok(Statement::SaveTransaction { name, span })
    }

    /// Consumes an optional `TRAN`/`TRANSACTION`/`WORK` keyword and transaction
    /// name after COMMIT/ROLLBACK; returns the end span.
    fn eat_optional_tran_and_name(&mut self, start: Span) -> Span {
        let mut end = start;
        if matches!(
            self.peek_keyword().as_deref(),
            Some("TRAN") | Some("TRANSACTION") | Some("WORK")
        ) {
            end = self.bump().span;
        }
        if let Some(n) = self.parse_optional_txn_name() {
            end = n.span;
        }
        end
    }

    pub(super) fn parse_optional_txn_name(&mut self) -> Option<Name> {
        // A bare (non-clause) identifier following is the transaction name.
        if matches!(self.peek().kind, TokenKind::Word { quoted: true, .. }) {
            return self.parse_name().ok();
        }
        if let Some(kw) = self.peek_keyword() {
            if is_reserved(&kw) {
                return None;
            }
            return self.parse_name().ok();
        }
        None
    }

    /// `DECLARE @a TYPE [= expr], @b TYPE ...`.
    pub(super) fn parse_declare(&mut self) -> SqlResult<Statement> {
        let declare = self.expect_keyword("DECLARE")?;
        // `DECLARE <name> [options] CURSOR FOR <select>` — a cursor is named
        // without `@`, so a plain identifier here (rather than a `@var`) is one.
        if matches!(self.peek().kind, TokenKind::Word { .. }) {
            return self.parse_declare_cursor(declare);
        }
        let mut decls = Vec::new();
        loop {
            let token = self.peek().clone();
            let TokenKind::LocalVar(name) = &token.kind else {
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            };
            let name = name.clone();
            self.bump();
            let _ = self.eat_keyword("AS"); // `DECLARE @v AS INT` — AS optional
            // `DECLARE @t TABLE ( ... )` — an in-memory table variable, which
            // must be a declaration on its own (SQL Server forbids mixing it).
            if self.peek_keyword().as_deref() == Some("TABLE") {
                if !decls.is_empty() {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
                self.bump(); // TABLE
                let (columns, primary_key) = self.parse_table_var_columns()?;
                return Ok(Statement::DeclareTableVar {
                    name,
                    columns,
                    primary_key,
                    span: token.span.to(self.prev_span()),
                });
            }
            let (data_type, type_span) = self.parse_data_type()?;
            let (initializer, end) = if self.eat(&TokenKind::Eq) {
                let expr = self.parse_expr()?;
                let end = expr.span;
                (Some(expr), end)
            } else {
                (None, type_span)
            };
            decls.push(Declaration {
                name,
                data_type,
                initializer,
                span: token.span.to(end),
            });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        Ok(Statement::Declare(decls))
    }

    /// `<name> [INSENSITIVE|STATIC|SCROLL|FORWARD_ONLY|READ_ONLY|LOCAL|GLOBAL|...]
    /// CURSOR FOR <select> [FOR {READ ONLY | UPDATE [OF ...]}]`. The cursor is a
    /// static snapshot; the type/concurrency options are accepted and ignored,
    /// except SCROLL (which allows non-NEXT fetches).
    pub(super) fn parse_declare_cursor(&mut self, start: Span) -> SqlResult<Statement> {
        let name = self.parse_name()?;
        let mut scroll = false;
        loop {
            match self.peek_keyword().as_deref() {
                Some("SCROLL") => {
                    self.bump();
                    scroll = true;
                }
                Some("INSENSITIVE") | Some("STATIC") | Some("FORWARD_ONLY") | Some("KEYSET")
                | Some("DYNAMIC") | Some("FAST_FORWARD") | Some("READ_ONLY")
                | Some("SCROLL_LOCKS") | Some("OPTIMISTIC") | Some("TYPE_WARNING")
                | Some("LOCAL") | Some("GLOBAL") => {
                    self.bump();
                }
                Some("CURSOR") => {
                    self.bump();
                    break;
                }
                _ => {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            }
        }
        self.expect_keyword("FOR")?;
        let select = self.parse_select()?;
        // Optional `FOR READ ONLY` / `FOR UPDATE [OF col, ...]` — accepted and
        // ignored (the cursor is read-only; positioned updates are not supported).
        if self.eat_keyword("FOR") {
            if self.eat_keyword("READ") {
                self.expect_keyword("ONLY")?;
            } else if self.eat_keyword("UPDATE") {
                if self.eat_keyword("OF") {
                    loop {
                        let _ = self.parse_name()?;
                        if !self.eat(&TokenKind::Comma) {
                            break;
                        }
                    }
                }
            } else {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        let span = start.to(self.prev_span());
        Ok(Statement::DeclareCursor {
            name,
            select: Box::new(select),
            scroll,
            span,
        })
    }

    /// `FETCH [NEXT|PRIOR|FIRST|LAST|ABSOLUTE <n>|RELATIVE <n>] [FROM] <name>
    /// [INTO @v[, ...]]`.
    pub(super) fn parse_fetch(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("FETCH")?;
        let direction = match self.peek_keyword().as_deref() {
            Some("NEXT") => {
                self.bump();
                FetchDirection::Next
            }
            Some("PRIOR") => {
                self.bump();
                FetchDirection::Prior
            }
            Some("FIRST") => {
                self.bump();
                FetchDirection::First
            }
            Some("LAST") => {
                self.bump();
                FetchDirection::Last
            }
            Some("ABSOLUTE") => {
                self.bump();
                FetchDirection::Absolute(self.parse_expr()?)
            }
            Some("RELATIVE") => {
                self.bump();
                FetchDirection::Relative(self.parse_expr()?)
            }
            // No direction keyword: NEXT (and `FROM` was optional).
            _ => FetchDirection::Next,
        };
        let _ = self.eat_keyword("FROM");
        let name = self.parse_name()?;
        let mut into = Vec::new();
        if self.eat_keyword("INTO") {
            loop {
                let token = self.peek().clone();
                let TokenKind::LocalVar(var) = &token.kind else {
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                };
                into.push(var.clone());
                self.bump();
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        let span = start.to(self.prev_span());
        Ok(Statement::FetchCursor {
            name,
            direction,
            into,
            span,
        })
    }

    /// `parse_name` after an OPEN/CLOSE/DEALLOCATE keyword builds the statement.
    pub(super) fn parse_cursor_verb(
        &mut self,
        keyword: &str,
        make: impl FnOnce(Name, Span) -> Statement,
    ) -> SqlResult<Statement> {
        let start = self.expect_keyword(keyword)?;
        let name = self.parse_name()?;
        Ok(make(name, start.to(self.prev_span())))
    }

    /// Parses a table variable's `( <column-defs> )` body: column definitions
    /// (with inline `NULL`/`NOT NULL`, `PRIMARY KEY`, and `DEFAULT`) plus a
    /// table-level `PRIMARY KEY (cols)`. IDENTITY, UNIQUE, CHECK, and FOREIGN KEY
    /// — supported on a base table — are rejected here rather than silently
    /// ignored, since the table-variable executor does not enforce them.
    pub(super) fn parse_table_var_columns(&mut self) -> SqlResult<(Vec<ColumnDef>, Vec<Name>)> {
        self.expect(&TokenKind::LParen)?;
        let mut columns = Vec::new();
        let mut primary_key: Vec<Name> = Vec::new();
        loop {
            if self.peek_keyword().as_deref() == Some("PRIMARY") {
                if !primary_key.is_empty() {
                    return Err(SqlError::message_only(
                        8110,
                        "Cannot add multiple PRIMARY KEY constraints to a table.",
                    ));
                }
                self.bump();
                self.expect_keyword("KEY")?;
                self.expect(&TokenKind::LParen)?;
                loop {
                    primary_key.push(self.parse_name()?);
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
                self.expect(&TokenKind::RParen)?;
            } else {
                let column = self.parse_column_def()?;
                // The table-variable executor honors columns, NULL/NOT NULL,
                // PRIMARY KEY, and DEFAULT; the rest of parse_column_def's
                // grammar it would ignore, so reject those constructs here.
                let unsupported = if column.identity.is_some() {
                    Some("IDENTITY")
                } else if column.unique {
                    Some("UNIQUE")
                } else if !column.checks.is_empty() {
                    Some("CHECK")
                } else if !column.foreign_keys.is_empty() {
                    Some("REFERENCES")
                } else {
                    None
                };
                if let Some(feature) = unsupported {
                    return Err(SqlError::syntax(feature, column.span));
                }
                if column.primary_key {
                    if !primary_key.is_empty() {
                        return Err(SqlError::message_only(
                            8110,
                            "Cannot add multiple PRIMARY KEY constraints to a table.",
                        ));
                    }
                    primary_key.push(column.name.clone());
                }
                columns.push(column);
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.expect(&TokenKind::RParen)?;
        Ok((columns, primary_key))
    }

    /// `USE <database>`.
    pub(super) fn parse_use(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("USE")?;
        if self.in_procedure {
            // SQL Server 154: a USE inside a stored body would change the
            // caller's database context out from under the lock analysis that
            // resolved the body's names — refused at CREATE, like SQL Server.
            return Err(SqlError::new(
                154,
                15,
                1,
                "a USE database statement is not allowed in a procedure, function or trigger."
                    .to_string(),
            )
            .at(start));
        }
        let database = self.parse_name()?;
        let span = start.to(database.span);
        Ok(Statement::Use { database, span })
    }

    /// `THROW [number, message, state]`. The arguments are constants or
    /// variables (SQL Server's rule); a bare `THROW` — the re-throw form — is
    /// recognized by the next token NOT starting one.
    pub(super) fn parse_throw(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("THROW")?;
        let has_args = matches!(
            self.peek().kind,
            TokenKind::Int(_) | TokenKind::Number(_) | TokenKind::LocalVar(_)
        );
        if !has_args {
            return Ok(Statement::Throw(ThrowStatement {
                args: None,
                span: start,
            }));
        }
        let number = self.parse_expr()?;
        self.expect(&TokenKind::Comma)?;
        let message = self.parse_expr()?;
        self.expect(&TokenKind::Comma)?;
        let state = self.parse_expr()?;
        let span = start.to(state.span);
        Ok(Statement::Throw(ThrowStatement {
            args: Some(ThrowArgs {
                number,
                message,
                state,
            }),
            span,
        }))
    }

    /// `RAISERROR(msg, severity, state [, args...]) [WITH LOG|NOWAIT|SETERROR]`.
    pub(super) fn parse_raiserror(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("RAISERROR")?;
        self.expect(&TokenKind::LParen)?;
        let message = self.parse_expr()?;
        self.expect(&TokenKind::Comma)?;
        let severity = self.parse_expr()?;
        self.expect(&TokenKind::Comma)?;
        let state = self.parse_expr()?;
        let mut args = Vec::new();
        while self.eat(&TokenKind::Comma) {
            args.push(self.parse_expr()?);
        }
        let mut end = self.expect(&TokenKind::RParen)?;
        let (mut log, mut nowait, mut seterror) = (false, false, false);
        if matches!(self.peek_keyword().as_deref(), Some("WITH")) {
            self.bump();
            loop {
                match self.peek_keyword().as_deref() {
                    Some("LOG") => log = true,
                    Some("NOWAIT") => nowait = true,
                    Some("SETERROR") => seterror = true,
                    _ => {
                        let token = self.peek().clone();
                        return Err(SqlError::syntax(self.token_text(&token), token.span));
                    }
                }
                end = self.bump().span;
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        Ok(Statement::RaiseError(RaiseError {
            message,
            severity,
            state,
            args,
            log,
            nowait,
            seterror,
            span: start.to(end),
        }))
    }

    pub(super) fn parse_set(&mut self) -> SqlResult<Statement> {
        self.expect_keyword("SET")?;
        // `SET @v = expr` — a variable assignment.
        if let TokenKind::LocalVar(name) = &self.peek().kind {
            let name = name.clone();
            self.bump();
            self.expect(&TokenKind::Eq)?;
            let value = self.parse_expr()?;
            return Ok(Statement::Set(SetStatement::Variable { name, value }));
        }
        match self.peek_keyword().as_deref() {
            Some("XACT_ABORT") => {
                self.bump();
                let on = self.parse_on_off()?;
                Ok(Statement::Set(SetStatement::XactAbort(on)))
            }
            Some("TRANSACTION") => {
                self.bump();
                self.expect_keyword("ISOLATION")?;
                self.expect_keyword("LEVEL")?;
                let level = self.parse_isolation_level()?;
                Ok(Statement::Set(SetStatement::IsolationLevel(level)))
            }
            Some("SHOWPLAN_TEXT") => {
                self.bump();
                let on = self.parse_on_off()?;
                Ok(Statement::Set(SetStatement::ShowplanText(on)))
            }
            Some("NOCOUNT") => {
                self.bump();
                let on = self.parse_on_off()?;
                Ok(Statement::Set(SetStatement::NoCount(on)))
            }
            Some(kw) if Self::set_option_requires_on(kw) => {
                // The SQL Server default for these is ON, and TruthDB's engine
                // is hardwired to that ON behaviour. Accept ON as a no-op, but
                // reject OFF: silently ignoring it would return results that
                // differ from what the client asked for (e.g. `ANSI_NULLS OFF`
                // making `col = NULL` match NULL rows).
                self.bump();
                if self.parse_on_off()? {
                    Ok(Statement::Set(SetStatement::Ignored))
                } else {
                    Err(SqlError::message_only(
                        102,
                        format!("SET {kw} OFF is not supported."),
                    ))
                }
            }
            Some(kw) if Self::set_option_ignorable(kw) => {
                // Cosmetic or advisory options that do not change query results
                // at TruthDB's feature level. Accept any argument as a no-op.
                // Each takes a single argument (`ON`/`OFF`, a bare word, or a
                // number that may carry a leading sign), so consume the option
                // name, an optional sign, and one argument token.
                self.bump();
                let _ = self.eat(&TokenKind::Minus) || self.eat(&TokenKind::Plus);
                if !matches!(self.peek().kind, TokenKind::Semicolon | TokenKind::Eof) {
                    self.bump();
                }
                Ok(Statement::Set(SetStatement::Ignored))
            }
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    /// Options whose SQL Server ON default matches TruthDB's fixed behaviour.
    /// Accepting ON is a faithful no-op; OFF must be rejected because TruthDB
    /// cannot honour it and silently ignoring it would corrupt results.
    fn set_option_requires_on(kw: &str) -> bool {
        matches!(
            kw,
            "QUOTED_IDENTIFIER" | "ANSI_NULLS" | "CONCAT_NULL_YIELDS_NULL" | "ANSI_DEFAULTS"
        )
    }

    /// Cosmetic or advisory session options that clients (SSMS, sqlcmd,
    /// drivers) set at connection time. TruthDB does not model these, but
    /// ignoring them does not change query results, so accepting them as
    /// no-ops keeps those clients working.
    ///
    /// Options that change *what* or *how much* runs — `ROWCOUNT`, `NOEXEC`,
    /// `PARSEONLY`, `FMTONLY`, `IMPLICIT_TRANSACTIONS` — are deliberately absent:
    /// silently ignoring them would run statements the client meant to limit or
    /// skip. They stay hard errors until implemented.
    fn set_option_ignorable(kw: &str) -> bool {
        matches!(
            kw,
            "ANSI_PADDING"
                | "ANSI_WARNINGS"
                | "ANSI_NULL_DFLT_ON"
                | "ANSI_NULL_DFLT_OFF"
                | "ARITHABORT"
                | "ARITHIGNORE"
                | "NUMERIC_ROUNDABORT"
                | "CURSOR_CLOSE_ON_COMMIT"
                | "FORCEPLAN"
                | "TEXTSIZE"
                | "LOCK_TIMEOUT"
                | "DEADLOCK_PRIORITY"
                | "DATEFIRST"
                | "DATEFORMAT"
                | "LANGUAGE"
        )
    }

    pub(super) fn parse_on_off(&mut self) -> SqlResult<bool> {
        match self.peek_keyword().as_deref() {
            Some("ON") => {
                self.bump();
                Ok(true)
            }
            Some("OFF") => {
                self.bump();
                Ok(false)
            }
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    fn parse_isolation_level(&mut self) -> SqlResult<IsolationLevel> {
        match self.peek_keyword().as_deref() {
            Some("READ") => {
                self.bump();
                match self.peek_keyword().as_deref() {
                    Some("UNCOMMITTED") => {
                        self.bump();
                        Ok(IsolationLevel::ReadUncommitted)
                    }
                    Some("COMMITTED") => {
                        self.bump();
                        Ok(IsolationLevel::ReadCommitted)
                    }
                    _ => {
                        let token = self.peek().clone();
                        Err(SqlError::syntax(self.token_text(&token), token.span))
                    }
                }
            }
            Some("REPEATABLE") => {
                self.bump();
                self.expect_keyword("READ")?;
                Ok(IsolationLevel::RepeatableRead)
            }
            Some("SERIALIZABLE") => {
                self.bump();
                Ok(IsolationLevel::Serializable)
            }
            Some("SNAPSHOT") => {
                self.bump();
                Ok(IsolationLevel::Snapshot)
            }
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }
}
