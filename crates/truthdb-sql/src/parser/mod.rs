//! Hand-rolled recursive-descent parser for the Stage 3 grammar (no
//! sqlparser-rs, per the plan). Expression precedence, low to high:
//! `OR` < `AND` < `NOT` < comparison/`IS NULL` < `+ -` < `* / %` < unary `-`
//! < primary.

use crate::ast::*;
use crate::error::{SqlError, SqlResult};
use crate::lexer::{Span, Token, TokenKind};

mod control;
mod ddl;
mod dml;
mod expression;
mod query;
mod session;
mod token;

/// Maximum expression nesting depth (parens / NOT / unary). Bounds parser
/// recursion — each nesting level costs ~9 stack frames down the precedence
/// chain — so a crafted `((((...))))` errors cleanly instead of overflowing
/// even a 2 MiB thread stack. Real SQL never nests remotely this deep.
const MAX_EXPR_DEPTH: usize = 64;

/// Maximum number of expression nodes per TOP-LEVEL expression. Bounds each
/// expression's size so a long operator chain (`1 OR 1 OR 1 ...`), which
/// parses iteratively but evaluates recursively down its spine, cannot
/// overflow the stack during evaluation. Per expression, not per batch: a
/// 1000-tuple `INSERT ... VALUES` is thousands of tiny FLAT expressions —
/// none deepens any evaluation spine — and a per-batch count made row-lock
/// escalation unreachable (its threshold sat above the whole-batch budget).
const MAX_EXPR_NODES: usize = 2000;

pub struct Parser {
    /// The original SQL source, for slicing sub-expression text (e.g. a
    /// column DEFAULT) by span.
    src: String,
    tokens: Vec<Token>,
    pos: usize,
    /// Current expression recursion depth.
    depth: usize,
    /// Lexical `WHILE` nesting depth: `BREAK`/`CONTINUE` outside a loop are
    /// compile-time errors (SQL Server 135/136), so the parser tracks it.
    while_depth: usize,
    /// Parsing a stored-procedure body: `RETURN <value>` is then legal.
    in_procedure: bool,
    /// Parsing a scalar-function body: `RETURN <expr>` is mandatory and yields
    /// the function's typed result, so the value is always parsed (not gated on
    /// a leading-token whitelist) and the 178 batch-return check does not apply.
    in_function: bool,
    /// Parsing a multi-statement table-valued function body: `RETURN` takes NO
    /// value (it returns the accumulated result table variable), unlike the
    /// scalar-function body where a value is mandatory.
    in_table_function: bool,
    /// Nesting depth inside blocks / IF branches / WHILE bodies — procedure
    /// DDL must be top-level (SQL Server 156/111 classes).
    sub_depth: usize,
    /// Index of the top-level statement being parsed (CREATE/ALTER PROCEDURE
    /// must be the batch's first statement — SQL Server 111).
    statement_index: usize,
    /// Expression nodes built so far.
    nodes: usize,
}

impl Parser {
    /// Builds a parser over an already-tokenized batch (the token stream
    /// always ends with an `Eof` token). `src` is the original SQL the tokens
    /// were produced from, used to recover sub-expression source text.
    pub fn from_tokens(src: &str, tokens: Vec<Token>) -> Self {
        debug_assert!(tokens.last().map(|t| &t.kind) == Some(&TokenKind::Eof));
        Parser {
            src: src.to_string(),
            tokens,
            pos: 0,
            depth: 0,
            while_depth: 0,
            in_procedure: false,
            in_function: false,
            in_table_function: false,
            sub_depth: 0,
            statement_index: 0,
            nodes: 0,
        }
    }

    /// The source text covered by `span`.
    fn slice(&self, span: Span) -> String {
        self.src
            .get(span.start..span.end)
            .unwrap_or_default()
            .to_string()
    }

    fn too_deep() -> SqlError {
        SqlError::message_only(
            191,
            "Some part of your SQL statement is nested too deeply. Rewrite the query or break it into smaller queries.",
        )
    }

    /// Counts one expression node against the batch budget.
    fn node(&mut self) -> SqlResult<()> {
        self.nodes += 1;
        if self.nodes > MAX_EXPR_NODES {
            return Err(Self::too_deep());
        }
        Ok(())
    }

    /// Convenience for tests: tokenize then parse.
    #[cfg(test)]
    pub fn parse_str(sql: &str) -> SqlResult<Vec<Statement>> {
        Parser::from_tokens(sql, crate::lexer::Lexer::new(sql).tokenize()?).parse_statements()
    }

    /// Switches to the in-procedure grammar (`RETURN <value>` legal): the
    /// entry for parsing a stored procedure's body text.
    pub fn set_in_procedure(&mut self) {
        self.in_procedure = true;
    }

    /// Switches to the in-function grammar (`RETURN <expr>` mandatory): the
    /// entry for parsing a scalar function's body text.
    pub fn set_in_function(&mut self) {
        self.in_function = true;
    }

    pub fn set_in_table_function(&mut self) {
        self.in_table_function = true;
    }

    /// Parses a standalone table-variable column list `( <column-defs> )` and
    /// rejects trailing tokens — the entry used to re-parse a multi-statement
    /// TVF's stored RETURNS column text per call.
    pub fn parse_table_var_columns_entry(&mut self) -> SqlResult<(Vec<ColumnDef>, Vec<Name>)> {
        let result = self.parse_table_var_columns()?;
        if !self.at_eof() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        Ok(result)
    }

    /// Parses exactly one expression followed by EOF (for a re-parsed DEFAULT).
    pub fn parse_single_expr(mut self) -> SqlResult<Expr> {
        let expr = self.parse_expr()?;
        if !self.at_eof() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        Ok(expr)
    }

    /// Parses a whole batch of `;`-separated statements.
    pub fn parse_statements(mut self) -> SqlResult<Vec<Statement>> {
        let mut statements = Vec::new();
        loop {
            while self.eat(&TokenKind::Semicolon) {}
            if self.at_eof() {
                break;
            }
            // The expression-node budget also resets per statement: CTE and
            // derived-table bodies parse under depth >= 1, so their
            // expressions never reach parse_expr's depth-0 reset and would
            // otherwise inherit the previous statement's count.
            self.nodes = 0;
            self.statement_index = statements.len();
            statements.push(self.parse_statement()?);
            // A label (`<name>:`) prefixes the statement that follows it, so no
            // separator is required after it; every other statement needs `;` or
            // end-of-batch before the next one.
            let after_label = matches!(statements.last(), Some(Statement::Label { .. }));
            if !after_label && !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        Ok(statements)
    }

    fn parse_statement(&mut self) -> SqlResult<Statement> {
        // A label `<identifier>:` at statement start (a GOTO target). At the
        // start of a statement, an identifier immediately followed by `:` can
        // only be a label — no statement begins that way.
        if matches!(self.peek().kind, TokenKind::Word { .. })
            && matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::Colon)
            )
        {
            return self.parse_label();
        }
        match self.peek_keyword().as_deref() {
            Some("CREATE") => self.parse_create(),
            Some("ALTER") => self.parse_alter(),
            Some("DROP") => self.parse_drop(),
            Some("INSERT") => self.parse_insert(),
            Some("UPDATE") => self.parse_update(),
            Some("DELETE") => self.parse_delete(),
            Some("SELECT") | Some("WITH") => Ok(Statement::Select(self.parse_select()?)),
            Some("BEGIN") => self.parse_begin(),
            Some("COMMIT") => self.parse_commit(),
            Some("ROLLBACK") => self.parse_rollback(),
            Some("SAVE") => self.parse_save(),
            Some("SET") => self.parse_set(),
            Some("DECLARE") => self.parse_declare(),
            Some("EXEC") | Some("EXECUTE") => self.parse_exec(),
            Some("USE") => self.parse_use(),
            Some("THROW") => self.parse_throw(),
            Some("RAISERROR") => self.parse_raiserror(),
            Some("IF") => self.parse_if(),
            Some("WHILE") => self.parse_while(),
            Some("BREAK") => self.parse_break(),
            Some("CONTINUE") => self.parse_continue(),
            Some("RETURN") => self.parse_return(),
            Some("GRANT") => self.parse_permission(PermissionKind::Grant),
            Some("DENY") => self.parse_permission(PermissionKind::Deny),
            Some("REVOKE") => self.parse_permission(PermissionKind::Revoke),
            Some("BACKUP") => self.parse_backup(),
            Some("RESTORE") => self.parse_restore(),
            Some("ENABLE") | Some("DISABLE") => self.parse_trigger_state(),
            Some("GOTO") => self.parse_goto(),
            Some("OPEN") => {
                self.parse_cursor_verb("OPEN", |name, span| Statement::OpenCursor { name, span })
            }
            Some("FETCH") => self.parse_fetch(),
            Some("CLOSE") => {
                self.parse_cursor_verb("CLOSE", |name, span| Statement::CloseCursor { name, span })
            }
            Some("DEALLOCATE") => self.parse_cursor_verb("DEALLOCATE", |name, span| {
                Statement::DeallocateCursor { name, span }
            }),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    /// `CREATE|ALTER PROC[EDURE] <name> [params] AS <body-to-end-of-batch>`.
    /// The body is validated by parsing (with `RETURN <value>` legal) and
    /// stored as its source text.
    fn parse_create_procedure(&mut self, start: Span, alter: bool) -> SqlResult<Statement> {
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

    fn parse_drop_procedure(&mut self, start: Span) -> SqlResult<Statement> {
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
    fn parse_create_trigger(&mut self, start: Span, alter: bool) -> SqlResult<Statement> {
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

    fn parse_drop_trigger(&mut self, start: Span) -> SqlResult<Statement> {
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

    /// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` or `ALTER LOGIN <name>
    /// {ENABLE | DISABLE}`.
    fn parse_backup(&mut self) -> SqlResult<Statement> {
        let start = self.peek().span;
        self.bump(); // BACKUP
        // BACKUP is a side-effecting operation: it is illegal inside a function
        // or table-valued-function body (a function must be side-effect-free —
        // otherwise `SELECT dbo.f(x) FROM t` would run a full backup per row).
        // It is permitted inside a stored procedure, matching SQL Server.
        if self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'BACKUP'.").at(start),
            );
        }
        let is_log = match self.peek_keyword().as_deref() {
            Some("DATABASE") => false,
            Some("LOG") => true,
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        self.bump(); // DATABASE | LOG
        let database = self.parse_name()?;
        self.expect_keyword("TO")?;
        self.expect_keyword("DISK")?;
        self.expect(&TokenKind::Eq)?;
        let token = self.peek().clone();
        let TokenKind::String(path) = &token.kind else {
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        };
        let path = path.clone();
        self.bump();

        // Optional `WITH` options. CHECKSUM (default on) / NO_CHECKSUM,
        // COPY_ONLY, and INIT / NOINIT (accepted but inert — a TDBBAK1 file
        // holds one backup, so the destination is always overwritten).
        let mut checksum = true;
        let mut copy_only = false;
        if self.eat_keyword("WITH") {
            loop {
                match self.peek_keyword().as_deref() {
                    Some("CHECKSUM") => {
                        self.bump();
                        checksum = true;
                    }
                    Some("NO_CHECKSUM") => {
                        self.bump();
                        checksum = false;
                    }
                    Some("COPY_ONLY") => {
                        self.bump();
                        copy_only = true;
                    }
                    Some("INIT") | Some("NOINIT") => {
                        self.bump();
                    }
                    _ => {
                        let token = self.peek().clone();
                        return Err(SqlError::syntax(self.token_text(&token), token.span));
                    }
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        let span = start.to(self.prev_span());
        if is_log {
            Ok(Statement::BackupLog {
                database,
                path,
                checksum,
                copy_only,
                span,
            })
        } else {
            Ok(Statement::BackupDatabase {
                database,
                path,
                checksum,
                copy_only,
                span,
            })
        }
    }

    /// `RESTORE {VERIFYONLY|HEADERONLY|FILELISTONLY|DATABASE <name>|LOG <name>}
    /// FROM DISK = '<path>'`. The three inspect verbs run online; DATABASE/LOG
    /// parse (so the error is semantic, not a syntax error) but restore offline.
    fn parse_restore(&mut self) -> SqlResult<Statement> {
        let start = self.peek().span;
        self.bump(); // RESTORE
        // Like BACKUP, RESTORE is side-effecting/privileged and illegal inside a
        // function or table-valued-function body.
        if self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'RESTORE'.").at(start),
            );
        }
        let mode = match self.peek_keyword().as_deref() {
            Some("VERIFYONLY") => {
                self.bump();
                RestoreMode::VerifyOnly
            }
            Some("HEADERONLY") => {
                self.bump();
                RestoreMode::HeaderOnly
            }
            Some("FILELISTONLY") => {
                self.bump();
                RestoreMode::FileListOnly
            }
            Some("DATABASE") => {
                self.bump();
                let _ = self.parse_name()?;
                RestoreMode::Database
            }
            Some("LOG") => {
                self.bump();
                let _ = self.parse_name()?;
                RestoreMode::Log
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        self.expect_keyword("FROM")?;
        self.expect_keyword("DISK")?;
        self.expect(&TokenKind::Eq)?;
        let token = self.peek().clone();
        let TokenKind::String(path) = &token.kind else {
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        };
        let path = path.clone();
        self.bump();
        let span = start.to(self.prev_span());
        Ok(Statement::Restore { mode, path, span })
    }

    /// `{ENABLE | DISABLE} TRIGGER {<name> | ALL} ON <table>`.
    fn parse_trigger_state(&mut self) -> SqlResult<Statement> {
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

    fn parse_create_login(&mut self, start: Span, alter: bool) -> SqlResult<Statement> {
        self.bump(); // LOGIN
        if self.in_procedure || self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'LOGIN'.").at(start),
            );
        }
        if self.statement_index > 0 || self.sub_depth > 0 {
            return Err(SqlError::new(
                111,
                15,
                1,
                "'CREATE/ALTER LOGIN' must be the first statement in a query batch.",
            )
            .at(start));
        }
        let name = self.parse_name()?;
        let mut password = None;
        let mut disable = None;
        if alter
            && matches!(
                self.peek_keyword().as_deref(),
                Some("ENABLE") | Some("DISABLE")
            )
        {
            disable = Some(self.peek_keyword().as_deref() == Some("DISABLE"));
            self.bump();
        } else {
            self.expect_keyword("WITH")?;
            self.expect_keyword("PASSWORD")?;
            self.expect(&TokenKind::Eq)?;
            let token = self.peek().clone();
            let TokenKind::String(pw) = &token.kind else {
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            };
            password = Some(pw.clone());
            self.bump();
        }
        let span = start.to(self.prev_span());
        Ok(Statement::CreateLogin(CreateLogin {
            name,
            password,
            disable,
            alter,
            span,
        }))
    }

    /// `CREATE DATABASE <name>` — a bare (single-part) name; SQL Server's
    /// storage clauses (`ON`, `LOG ON`, ...) do not apply to a shared-file
    /// instance and are not accepted.
    fn parse_create_database(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // DATABASE
        let name = self.parse_single_part_name()?;
        Ok(Statement::CreateDatabase {
            span: start.to(name.span),
            name,
        })
    }

    /// `DROP DATABASE [IF EXISTS] <name>`.
    fn parse_drop_database(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // DATABASE
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_single_part_name()?;
        Ok(Statement::DropDatabase {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// A name that must be a single identifier — a database name is never
    /// dotted (error 170-class syntax rejection on a qualifier).
    fn parse_single_part_name(&mut self) -> SqlResult<Name> {
        let name = self.parse_name()?;
        if name.value.contains('.') {
            return Err(SqlError::syntax(&name.value, name.span));
        }
        Ok(name)
    }

    fn parse_drop_login(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // LOGIN
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropLogin {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// `CREATE USER <name> [FOR LOGIN <login> | WITHOUT LOGIN]`.
    fn parse_create_user(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // USER
        let name = self.parse_name()?;
        let mut for_login = None;
        match self.peek_keyword().as_deref() {
            Some("FOR") => {
                self.bump();
                self.expect_keyword("LOGIN")?;
                for_login = Some(self.parse_name()?);
            }
            // `WITHOUT LOGIN` — a user with no mapped login (accepted, no map).
            Some("WITHOUT") => {
                self.bump();
                self.expect_keyword("LOGIN")?;
            }
            _ => {}
        }
        let span = start.to(self.prev_span());
        Ok(Statement::CreateUser(CreateUser {
            name,
            for_login,
            span,
        }))
    }

    /// `CREATE ROLE <name> [AUTHORIZATION <owner>]` (AUTHORIZATION accepted, ignored).
    fn parse_create_role(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // ROLE
        let name = self.parse_name()?;
        if self.peek_keyword().as_deref() == Some("AUTHORIZATION") {
            self.bump();
            let _ = self.parse_name()?;
        }
        let span = start.to(self.prev_span());
        Ok(Statement::CreateRole { name, span })
    }

    fn parse_drop_user(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // USER
        let if_exists = self.parse_optional_if_exists()?;
        let name = self.parse_name()?;
        Ok(Statement::DropUser {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    fn parse_drop_role(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // ROLE
        let if_exists = self.parse_optional_if_exists()?;
        let name = self.parse_name()?;
        Ok(Statement::DropRole {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
    fn parse_alter_role(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // ROLE
        let name = self.parse_name()?;
        let action = match self.peek_keyword().as_deref() {
            Some("ADD") => {
                self.bump();
                RoleMemberAction::Add
            }
            Some("DROP") => {
                self.bump();
                RoleMemberAction::Drop
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        self.expect_keyword("MEMBER")?;
        let member = self.parse_name()?;
        let span = start.to(member.span);
        Ok(Statement::AlterRole {
            name,
            action,
            member,
            span,
        })
    }

    /// `GRANT|DENY|REVOKE <action>[, …] ON <object> TO|FROM <grantee>[, …]`.
    /// (Column-level, `WITH GRANT OPTION`, and database-scoped grants are not
    /// parsed here.)
    fn parse_permission(&mut self, kind: PermissionKind) -> SqlResult<Statement> {
        let start = self.expect_keyword(match kind {
            PermissionKind::Grant => "GRANT",
            PermissionKind::Deny => "DENY",
            PermissionKind::Revoke => "REVOKE",
        })?;
        let mut actions = vec![self.parse_permission_action()?];
        while self.peek().kind == TokenKind::Comma {
            self.bump();
            actions.push(self.parse_permission_action()?);
        }
        self.expect_keyword("ON")?;
        let object = self.parse_name()?;
        // GRANT/DENY use TO; REVOKE uses FROM.
        match kind {
            PermissionKind::Revoke => self.expect_keyword("FROM")?,
            _ => self.expect_keyword("TO")?,
        };
        let mut grantees = vec![self.parse_name()?];
        while self.peek().kind == TokenKind::Comma {
            self.bump();
            grantees.push(self.parse_name()?);
        }
        let span = start.to(self.prev_span());
        Ok(Statement::Permission(PermissionStatement {
            kind,
            actions,
            object,
            grantees,
            span,
        }))
    }

    fn parse_permission_action(&mut self) -> SqlResult<PermissionAction> {
        let token = self.peek().clone();
        let action = match self.peek_keyword().as_deref() {
            Some("SELECT") => PermissionAction::Select,
            Some("INSERT") => PermissionAction::Insert,
            Some("UPDATE") => PermissionAction::Update,
            Some("DELETE") => PermissionAction::Delete,
            Some("EXECUTE") | Some("EXEC") => PermissionAction::Execute,
            Some("REFERENCES") => PermissionAction::References,
            Some("ALTER") => PermissionAction::Alter,
            _ => return Err(SqlError::syntax(self.token_text(&token), token.span)),
        };
        self.bump();
        Ok(action)
    }

    /// Parses an optional leading `IF EXISTS`.
    fn parse_optional_if_exists(&mut self) -> SqlResult<bool> {
        if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// `CREATE|ALTER FUNCTION <name> ( [params] ) RETURNS <type> AS <body>`.
    /// Only the scalar form is parsed here; the body is validated by parsing it
    /// (with `RETURN <expr>` mandatory) and stored as source text.
    fn parse_create_function(&mut self, start: Span, alter: bool) -> SqlResult<Statement> {
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

    fn parse_drop_function(&mut self, start: Span) -> SqlResult<Statement> {
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

/// Reserved words that may not be used as bare identifiers.
fn is_reserved(keyword: &str) -> bool {
    matches!(
        keyword,
        "SELECT"
            | "FROM"
            | "WHERE"
            | "INSERT"
            | "INTO"
            | "VALUES"
            | "CREATE"
            | "TABLE"
            | "DROP"
            | "PRIMARY"
            | "KEY"
            | "AND"
            | "OR"
            | "NOT"
            | "NULL"
            | "IS"
            | "ORDER"
            | "BY"
            | "TOP"
            | "GROUP"
            | "HAVING"
    )
}

#[cfg(test)]
mod tests;
