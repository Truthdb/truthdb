//! Hand-rolled recursive-descent parser for the Stage 3 grammar (no
//! sqlparser-rs, per the plan). Expression precedence, low to high:
//! `OR` < `AND` < `NOT` < comparison/`IS NULL` < `+ -` < `* / %` < unary `-`
//! < primary.

use crate::ast::*;
use crate::error::{SqlError, SqlResult};
use crate::lexer::{Span, Token, TokenKind};

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
            if !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        Ok(statements)
    }

    fn parse_statement(&mut self) -> SqlResult<Statement> {
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
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    /// `EXEC[UTE] <proc> [[@name =] <expr> [, ...]]` — the T-SQL text path to
    /// the system procedures. Arguments end at the statement boundary.
    fn parse_exec(&mut self) -> SqlResult<Statement> {
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
    fn parse_if(&mut self) -> SqlResult<Statement> {
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
    fn parse_while(&mut self) -> SqlResult<Statement> {
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

    fn parse_break(&mut self) -> SqlResult<Statement> {
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

    fn parse_continue(&mut self) -> SqlResult<Statement> {
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
    fn parse_return(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("RETURN")?;
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

    // ---- transaction control --------------------------------------------

    fn parse_begin(&mut self) -> SqlResult<Statement> {
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
            if !self.at_eof()
                && !matches!(self.peek_keyword().as_deref(), Some("END"))
                && !self.check(&TokenKind::Semicolon)
            {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        Ok(statements)
    }

    fn parse_commit(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("COMMIT")?;
        let end = self.eat_optional_tran_and_name(start);
        Ok(Statement::Commit {
            span: start.to(end),
        })
    }

    fn parse_rollback(&mut self) -> SqlResult<Statement> {
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

    fn parse_save(&mut self) -> SqlResult<Statement> {
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

    fn parse_optional_txn_name(&mut self) -> Option<Name> {
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
    fn parse_declare(&mut self) -> SqlResult<Statement> {
        self.expect_keyword("DECLARE")?;
        let mut decls = Vec::new();
        loop {
            let token = self.peek().clone();
            let TokenKind::LocalVar(name) = &token.kind else {
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            };
            let name = name.clone();
            self.bump();
            let _ = self.eat_keyword("AS"); // `DECLARE @v AS INT` — AS optional
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

    /// `USE <database>`.
    fn parse_use(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("USE")?;
        let database = self.parse_name()?;
        let span = start.to(database.span);
        Ok(Statement::Use { database, span })
    }

    /// `THROW [number, message, state]`. The arguments are constants or
    /// variables (SQL Server's rule); a bare `THROW` — the re-throw form — is
    /// recognized by the next token NOT starting one.
    fn parse_throw(&mut self) -> SqlResult<Statement> {
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
    fn parse_raiserror(&mut self) -> SqlResult<Statement> {
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

    fn parse_set(&mut self) -> SqlResult<Statement> {
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

    fn parse_on_off(&mut self) -> SqlResult<bool> {
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

    // ---- CREATE TABLE ---------------------------------------------------

    /// Dispatches `CREATE TABLE` vs `CREATE [UNIQUE] INDEX`.
    fn parse_create(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("CREATE")?;
        let unique = self.peek_keyword().as_deref() == Some("UNIQUE");
        if unique {
            self.bump();
        }
        match self.peek_keyword().as_deref() {
            Some("INDEX") => self.parse_create_index(start, unique),
            Some("TABLE") if !unique => self.parse_create_table(start),
            Some("VIEW") if !unique => self.parse_create_view(start),
            Some("PROCEDURE") | Some("PROC") if !unique => {
                self.parse_create_procedure(start, false)
            }
            Some("FUNCTION") if !unique => self.parse_create_function(start, false),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    fn parse_create_view(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("VIEW")?;
        let name = self.parse_name()?;
        // A view column list (`CREATE VIEW v (a, b) AS ...`) renames the output
        // columns; not supported yet.
        if self.check(&TokenKind::LParen) {
            let token = self.peek().clone();
            return Err(SqlError::message_only(
                102,
                format!(
                    "A column list on CREATE VIEW is not supported yet, near '{}'.",
                    self.token_text(&token)
                ),
            ));
        }
        self.expect_keyword("AS")?;
        // Capture text from the current token so a leading `WITH` (whose CTEs
        // precede the SELECT keyword the query span starts at) is included.
        let query_start = self.peek().span.start;
        let query = self.parse_select()?;
        let query_text = self
            .slice(Span::new(query_start, query.span.end))
            .trim()
            .to_string();
        Ok(Statement::CreateView(CreateView {
            span: start.to(query.span),
            name,
            query_text,
        }))
    }

    fn parse_create_index(&mut self, start: Span, unique: bool) -> SqlResult<Statement> {
        self.expect_keyword("INDEX")?;
        let name = self.parse_name()?;
        self.expect_keyword("ON")?;
        let table = self.parse_name()?;
        self.expect(&TokenKind::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.parse_name()?;
            let ascending = match self.peek_keyword().as_deref() {
                Some("ASC") => {
                    self.bump();
                    true
                }
                Some("DESC") => {
                    self.bump();
                    false
                }
                _ => true,
            };
            columns.push(IndexColumn {
                name: col_name,
                ascending,
            });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let mut end = self.expect(&TokenKind::RParen)?;
        // Optional INCLUDE (col [, ...]): non-key columns stored in the leaf.
        let mut include = Vec::new();
        if self.peek_keyword().as_deref() == Some("INCLUDE") {
            self.bump();
            self.expect(&TokenKind::LParen)?;
            loop {
                include.push(self.parse_name()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            end = self.expect(&TokenKind::RParen)?;
        }
        Ok(Statement::CreateIndex(CreateIndex {
            name,
            table,
            unique,
            columns,
            include,
            span: start.to(end),
        }))
    }

    fn parse_create_table(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("TABLE")?;
        let table = self.parse_name()?;
        self.expect(&TokenKind::LParen)?;

        let mut columns = Vec::new();
        let mut primary_key: Vec<Name> = Vec::new();
        let mut check_constraints: Vec<CheckConstraint> = Vec::new();
        let mut foreign_keys: Vec<ForeignKey> = Vec::new();
        let mut unique_constraints: Vec<UniqueConstraint> = Vec::new();
        loop {
            // A leading `CONSTRAINT name` introduces a named table constraint.
            let constraint_name = self.parse_optional_constraint_name()?;
            match self.peek_keyword().as_deref() {
                Some("UNIQUE") => {
                    let start = self.bump().span;
                    self.expect(&TokenKind::LParen)?;
                    let mut cols = Vec::new();
                    loop {
                        cols.push(self.parse_name()?);
                        if !self.eat(&TokenKind::Comma) {
                            break;
                        }
                    }
                    let end = self.expect(&TokenKind::RParen)?;
                    unique_constraints.push(UniqueConstraint {
                        name: constraint_name,
                        columns: cols,
                        span: start.to(end),
                    });
                }
                Some("PRIMARY") => {
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
                }
                Some("CHECK") => {
                    check_constraints.push(self.parse_check_constraint(constraint_name)?);
                }
                Some("FOREIGN") => {
                    foreign_keys.push(self.parse_foreign_key(constraint_name)?);
                }
                _ if constraint_name.is_some() => {
                    // `CONSTRAINT name` must be followed by a table constraint.
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
                _ => {
                    let column = self.parse_column_def()?;
                    if column.primary_key {
                        if !primary_key.is_empty() {
                            return Err(SqlError::message_only(
                                8110,
                                "Cannot add multiple PRIMARY KEY constraints to a table.",
                            ));
                        }
                        primary_key.push(column.name.clone());
                    }
                    // A column-level `UNIQUE` is a single-column unique constraint.
                    if column.unique {
                        unique_constraints.push(UniqueConstraint {
                            name: None,
                            columns: vec![column.name.clone()],
                            span: column.span,
                        });
                    }
                    columns.push(column);
                }
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let end = self.expect(&TokenKind::RParen)?;
        Ok(Statement::CreateTable(CreateTable {
            table,
            columns,
            primary_key,
            check_constraints,
            foreign_keys,
            unique_constraints,
            span: start.to(end),
        }))
    }

    /// Consumes an optional `CONSTRAINT name` prefix, returning the name.
    fn parse_optional_constraint_name(&mut self) -> SqlResult<Option<Name>> {
        if self.peek_keyword().as_deref() == Some("CONSTRAINT") {
            self.bump();
            Ok(Some(self.parse_name()?))
        } else {
            Ok(None)
        }
    }

    /// Parses `CHECK (predicate)` (the `CONSTRAINT name` prefix, if any, is
    /// already consumed). The predicate is kept as source text.
    fn parse_check_constraint(&mut self, name: Option<Name>) -> SqlResult<CheckConstraint> {
        let start = self.expect_keyword("CHECK")?;
        let lparen = self.expect(&TokenKind::LParen)?;
        self.parse_expr()?;
        let end = self.expect(&TokenKind::RParen)?;
        // Slice the exact source between the CHECK's own parentheses. An
        // expression node's span drops any outer parentheses of a boundary
        // subexpression, so `self.slice(expr.span)` would capture unbalanced
        // parens (e.g. `(a + b) > 0` -> `a + b) > 0`); slicing between our own
        // parens keeps nested parentheses balanced.
        Ok(CheckConstraint {
            name,
            predicate: self
                .slice(Span::new(lparen.end, end.start))
                .trim()
                .to_string(),
            span: start.to(end),
        })
    }

    /// Parses `FOREIGN KEY (cols) REFERENCES parent [(pcols)]` (the
    /// `CONSTRAINT name` prefix, if any, is already consumed).
    fn parse_foreign_key(&mut self, name: Option<Name>) -> SqlResult<ForeignKey> {
        let start = self.expect_keyword("FOREIGN")?;
        self.expect_keyword("KEY")?;
        let columns = self.parse_name_list()?;
        self.expect_keyword("REFERENCES")?;
        let parent = self.parse_name()?;
        let (parent_columns, end) = self.parse_optional_reference_columns(parent.span)?;
        Ok(ForeignKey {
            name,
            columns,
            parent,
            parent_columns,
            span: start.to(end),
        })
    }

    /// Parses a column-level `REFERENCES parent [(pcol)]` into a single-column
    /// foreign key over `column`.
    fn parse_column_reference(
        &mut self,
        name: Option<Name>,
        column: &Name,
    ) -> SqlResult<ForeignKey> {
        let start = self.expect_keyword("REFERENCES")?;
        let parent = self.parse_name()?;
        let (parent_columns, end) = self.parse_optional_reference_columns(parent.span)?;
        Ok(ForeignKey {
            name,
            columns: vec![column.clone()],
            parent,
            parent_columns,
            span: start.to(end),
        })
    }

    /// Parses a parenthesized comma-separated name list.
    fn parse_name_list(&mut self) -> SqlResult<Vec<Name>> {
        self.expect(&TokenKind::LParen)?;
        let mut names = Vec::new();
        loop {
            names.push(self.parse_name()?);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.expect(&TokenKind::RParen)?;
        Ok(names)
    }

    /// Parses an optional `(cols)` after `REFERENCES parent`; absent means the
    /// parent's primary key. Returns the columns and the end span.
    fn parse_optional_reference_columns(&mut self, fallback: Span) -> SqlResult<(Vec<Name>, Span)> {
        if self.check(&TokenKind::LParen) {
            let cols = self.parse_name_list()?;
            let end = cols.last().map(|n| n.span).unwrap_or(fallback);
            Ok((cols, end))
        } else {
            Ok((Vec::new(), fallback))
        }
    }

    fn parse_column_def(&mut self) -> SqlResult<ColumnDef> {
        let name = self.parse_name()?;
        let (data_type, type_span) = self.parse_data_type()?;
        let mut nullable = None;
        let mut primary_key = false;
        let mut default = None;
        let mut identity = None;
        let mut collation = None;
        let mut checks = Vec::new();
        let mut foreign_keys = Vec::new();
        let mut unique = false;
        let mut end = type_span;
        loop {
            match self.peek_keyword().as_deref() {
                Some("UNIQUE") => {
                    end = self.bump().span;
                    unique = true;
                }
                Some("CHECK") => {
                    let check = self.parse_check_constraint(None)?;
                    end = check.span;
                    checks.push(check);
                }
                Some("REFERENCES") => {
                    let fk = self.parse_column_reference(None, &name)?;
                    end = fk.span;
                    foreign_keys.push(fk);
                }
                Some("CONSTRAINT") => {
                    let constraint_name = self.parse_optional_constraint_name()?;
                    // A named column constraint is CHECK or REFERENCES.
                    if self.peek_keyword().as_deref() == Some("REFERENCES") {
                        let fk = self.parse_column_reference(constraint_name, &name)?;
                        end = fk.span;
                        foreign_keys.push(fk);
                    } else {
                        let check = self.parse_check_constraint(constraint_name)?;
                        end = check.span;
                        checks.push(check);
                    }
                }
                Some("NOT") => {
                    self.bump();
                    end = self.expect_keyword("NULL")?;
                    nullable = Some(false);
                }
                Some("NULL") => {
                    end = self.bump().span;
                    nullable = Some(true);
                }
                Some("PRIMARY") => {
                    self.bump();
                    end = self.expect_keyword("KEY")?;
                    primary_key = true;
                    // A PK column is implicitly NOT NULL.
                    if nullable != Some(false) {
                        nullable = Some(false);
                    }
                }
                Some("DEFAULT") => {
                    self.bump();
                    let expr = self.parse_expr()?;
                    end = expr.span;
                    default = Some(self.slice(expr.span));
                }
                Some("IDENTITY") => {
                    self.bump();
                    let (id, id_end) = self.parse_identity(type_span)?;
                    end = id_end;
                    identity = Some(id);
                }
                Some("COLLATE") => {
                    self.bump();
                    let coll = self.parse_ident()?;
                    end = coll.span;
                    collation = Some(coll.value);
                }
                _ => break,
            }
        }
        Ok(ColumnDef {
            span: name.span.to(end),
            name,
            data_type,
            nullable,
            primary_key,
            unique,
            default,
            identity,
            collation,
            checks,
            foreign_keys,
        })
    }

    /// Parses an optional `(seed, increment)` after the IDENTITY keyword.
    /// Bare `IDENTITY` defaults to `(1, 1)`, as in SQL Server.
    fn parse_identity(&mut self, fallback: Span) -> SqlResult<(Identity, Span)> {
        let mut seed = 1i64;
        let mut increment = 1i64;
        let mut end = fallback;
        if self.eat(&TokenKind::LParen) {
            seed = self.parse_i64_literal()?;
            self.expect(&TokenKind::Comma)?;
            increment = self.parse_i64_literal()?;
            end = self.expect(&TokenKind::RParen)?;
        }
        Ok((Identity { seed, increment }, end))
    }

    fn parse_data_type(&mut self) -> SqlResult<(DataType, Span)> {
        let token = self.bump();
        let span = token.span;
        let keyword = token
            .keyword()
            .ok_or_else(|| SqlError::syntax(self.token_text(&token), span))?;
        // `None` length = `(MAX)`.
        let with_len = |parser: &mut Self, default: u32| -> SqlResult<(Option<u32>, Span)> {
            if parser.eat(&TokenKind::LParen) {
                if parser.peek_keyword().as_deref() == Some("MAX") {
                    parser.bump();
                    let end = parser.expect(&TokenKind::RParen)?;
                    return Ok((None, end));
                }
                let n = parser.parse_u32_literal()?;
                let end = parser.expect(&TokenKind::RParen)?;
                Ok((Some(n), end))
            } else {
                Ok((Some(default), span))
            }
        };
        let data_type = match keyword.as_str() {
            "TINYINT" => DataType::TinyInt,
            "SMALLINT" => DataType::SmallInt,
            "INT" | "INTEGER" => DataType::Int,
            "BIGINT" => DataType::BigInt,
            "BIT" => DataType::Bit,
            "REAL" => DataType::Real,
            "FLOAT" => DataType::Float,
            "DATE" => DataType::Date,
            "TIME" => DataType::Time,
            "DATETIME2" => DataType::DateTime2,
            "UNIQUEIDENTIFIER" => DataType::UniqueIdentifier,
            "DECIMAL" | "NUMERIC" => {
                let (precision, scale, end) = self.parse_decimal_args(span)?;
                return Ok((DataType::Decimal { precision, scale }, span.to(end)));
            }
            "VARCHAR" | "CHAR" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((
                    match n {
                        Some(n) => DataType::VarChar(n),
                        None => DataType::VarCharMax,
                    },
                    span.to(end),
                ));
            }
            "NVARCHAR" | "NCHAR" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((
                    match n {
                        Some(n) => DataType::NVarChar(n),
                        None => DataType::NVarCharMax,
                    },
                    span.to(end),
                ));
            }
            "VARBINARY" | "BINARY" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((
                    match n {
                        Some(n) => DataType::VarBinary(n),
                        None => DataType::VarBinaryMax,
                    },
                    span.to(end),
                ));
            }
            other => {
                return Err(SqlError::message_only(
                    243,
                    format!("Type {other} is not a defined system type."),
                )
                .at(span));
            }
        };
        Ok((data_type, span))
    }

    /// Parses an optional `(precision[, scale])` for DECIMAL/NUMERIC. Defaults
    /// to `(18, 0)` (SQL Server's), validating p in 1..=38 and s <= p (error
    /// 2749/2750-style range messages folded into a 102 for simplicity).
    fn parse_decimal_args(&mut self, span: Span) -> SqlResult<(u8, u8, Span)> {
        let mut precision: u32 = 18;
        let mut scale: u32 = 0;
        let mut end = span;
        if self.eat(&TokenKind::LParen) {
            precision = self.parse_u32_literal()?;
            if self.eat(&TokenKind::Comma) {
                scale = self.parse_u32_literal()?;
            }
            end = self.expect(&TokenKind::RParen)?;
        }
        if precision == 0 || precision > 38 || scale > precision {
            return Err(SqlError::message_only(
                2749,
                format!(
                    "The precision {precision} and scale {scale} are invalid (precision 1..=38, scale <= precision)."
                ),
            )
            .at(span));
        }
        Ok((precision as u8, scale as u8, end))
    }

    // ---- ALTER TABLE ----------------------------------------------------

    fn parse_alter(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("ALTER")?;
        if self.peek_keyword().as_deref() == Some("DATABASE") {
            return self.parse_alter_database(start);
        }
        if matches!(
            self.peek_keyword().as_deref(),
            Some("PROCEDURE") | Some("PROC")
        ) {
            return self.parse_create_procedure(start, true);
        }
        if self.peek_keyword().as_deref() == Some("FUNCTION") {
            return self.parse_create_function(start, true);
        }
        self.expect_keyword("TABLE")?;
        let table = self.parse_name()?;
        let (action, end) = match self.peek_keyword().as_deref() {
            Some("ADD") => {
                self.bump();
                // `ADD [CONSTRAINT name] (CHECK | FOREIGN KEY ...)`, or
                // `ADD <column> <type> ...` — T-SQL has no COLUMN keyword
                // here, so anything but a constraint introducer is a column.
                match self.peek_keyword().as_deref() {
                    Some("CONSTRAINT") | Some("FOREIGN") | Some("CHECK") => {
                        let name = self.parse_optional_constraint_name()?;
                        if self.peek_keyword().as_deref() == Some("FOREIGN") {
                            let fk = self.parse_foreign_key(name)?;
                            let end = fk.span;
                            (AlterAction::AddForeignKey(fk), end)
                        } else {
                            let check = self.parse_check_constraint(name)?;
                            let end = check.span;
                            (AlterAction::AddCheck(check), end)
                        }
                    }
                    _ => {
                        let column = self.parse_column_def()?;
                        let end = column.span;
                        (AlterAction::AddColumn(column), end)
                    }
                }
            }
            Some("DROP") => {
                self.bump();
                self.expect_keyword("CONSTRAINT")?;
                let name = self.parse_name()?;
                let end = name.span;
                (AlterAction::DropConstraint(name), end)
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        Ok(Statement::AlterTable(AlterTable {
            table,
            action,
            span: start.to(end),
        }))
    }

    // ---- ALTER DATABASE -------------------------------------------------

    /// `ALTER DATABASE {name | CURRENT} SET <option> {ON|OFF} [, ...]`.
    /// Only the Stage 13 versioning options are recognized; anything else is
    /// a syntax error rather than a silent no-op (these options change what
    /// concurrent readers see).
    fn parse_alter_database(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("DATABASE")?;
        let name = if self.peek_keyword().as_deref() == Some("CURRENT") {
            self.bump();
            None
        } else {
            Some(self.parse_name()?)
        };
        self.expect_keyword("SET")?;
        let mut options = Vec::new();
        let mut end;
        loop {
            let option = match self.peek_keyword().as_deref() {
                Some("READ_COMMITTED_SNAPSHOT") => DatabaseOption::ReadCommittedSnapshot,
                Some("ALLOW_SNAPSHOT_ISOLATION") => DatabaseOption::AllowSnapshotIsolation,
                _ => {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            };
            end = self.bump().span;
            let on = self.parse_on_off()?;
            options.push((option, on));
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        Ok(Statement::AlterDatabase(AlterDatabase {
            name,
            options,
            span: start.to(end),
        }))
    }

    // ---- DROP TABLE -----------------------------------------------------

    /// Dispatches `DROP TABLE` vs `DROP INDEX`.
    fn parse_drop(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("DROP")?;
        match self.peek_keyword().as_deref() {
            Some("INDEX") => self.parse_drop_index(start),
            Some("TABLE") => self.parse_drop_table(start),
            Some("VIEW") => self.parse_drop_view(start),
            Some("PROCEDURE") | Some("PROC") => self.parse_drop_procedure(start),
            Some("FUNCTION") => self.parse_drop_function(start),
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
                self.parse_statement()?;
                if !self.at_eof() && !self.check(&TokenKind::Semicolon) {
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
        // `RETURNS TABLE` is an inline table-valued function: its body is a
        // single `AS RETURN ( <select> )` captured as source text and expanded
        // like a parameterized view. (Multi-statement `RETURNS @t TABLE(...)` is
        // added by later work.) Anything else is a scalar return type.
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
                self.parse_statement()?;
                if !self.at_eof() && !self.check(&TokenKind::Semicolon) {
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

    fn parse_drop_view(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("VIEW")?;
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropView(DropView {
            span: start.to(name.span),
            name,
            if_exists,
        }))
    }

    fn parse_drop_index(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("INDEX")?;
        let name = self.parse_name()?;
        self.expect_keyword("ON")?;
        let table = self.parse_name()?;
        Ok(Statement::DropIndex(DropIndex {
            span: start.to(table.span),
            name,
            table,
        }))
    }

    fn parse_drop_table(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("TABLE")?;
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let table = self.parse_name()?;
        Ok(Statement::DropTable(DropTable {
            span: start.to(table.span),
            table,
            if_exists,
        }))
    }

    // ---- INSERT ---------------------------------------------------------

    fn parse_insert(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("INSERT")?;
        // Optional INTO.
        if self.peek_keyword().as_deref() == Some("INTO") {
            self.bump();
        }
        let table = self.parse_name()?;
        let columns = if self.check(&TokenKind::LParen) {
            // Column list, unless this paren opens VALUES-less tuple (it
            // does not in our grammar), so it is always a column list.
            self.bump();
            let mut names = Vec::new();
            loop {
                names.push(self.parse_name()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            self.expect(&TokenKind::RParen)?;
            Some(names)
        } else {
            None
        };
        // The row source is either a SELECT or literal VALUES tuples.
        let source = if self.peek_keyword().as_deref() == Some("SELECT") {
            InsertSource::Select(Box::new(self.parse_select()?))
        } else {
            self.expect_keyword("VALUES")?;
            let mut rows = Vec::new();
            loop {
                self.expect(&TokenKind::LParen)?;
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_expr()?);
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
                self.expect(&TokenKind::RParen)?;
                rows.push(values);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            InsertSource::Values(rows)
        };
        let end = self.prev_span();
        Ok(Statement::Insert(Insert {
            span: start.to(end),
            table,
            columns,
            source,
        }))
    }

    // ---- UPDATE ---------------------------------------------------------

    fn parse_update(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("UPDATE")?;
        let table = self.parse_name()?;
        self.expect_keyword("SET")?;
        let mut assignments = Vec::new();
        loop {
            let column = self.parse_name()?;
            self.expect(&TokenKind::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let where_clause = self.parse_optional_where()?;
        let end = self.prev_span();
        Ok(Statement::Update(Update {
            span: start.to(end),
            table,
            assignments,
            where_clause,
        }))
    }

    // ---- DELETE ---------------------------------------------------------

    fn parse_delete(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("DELETE")?;
        // Optional FROM.
        if self.peek_keyword().as_deref() == Some("FROM") {
            self.bump();
        }
        let table = self.parse_name()?;
        let where_clause = self.parse_optional_where()?;
        let end = self.prev_span();
        Ok(Statement::Delete(Delete {
            span: start.to(end),
            table,
            where_clause,
        }))
    }

    fn parse_optional_where(&mut self) -> SqlResult<Option<Expr>> {
        if self.peek_keyword().as_deref() == Some("WHERE") {
            self.bump();
            Ok(Some(self.parse_expr()?))
        } else {
            Ok(None)
        }
    }

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

    fn parse_select(&mut self) -> SqlResult<Select> {
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
                    self.expect_keyword("JOIN")?;
                    JoinKind::Cross
                }
                _ => break,
            };
            let right = self.parse_table_primary()?;
            let on = if kind == JoinKind::Cross {
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
        let name = self.parse_name()?;
        // `name ( args )` in table position is a table-valued function call.
        if self.check(&TokenKind::LParen) {
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

    /// Consumes `keyword` if it is next; returns whether it did.
    fn eat_keyword(&mut self, keyword: &str) -> bool {
        if self.peek_keyword().as_deref() == Some(keyword) {
            self.bump();
            true
        } else {
            false
        }
    }

    // ---- expressions (precedence climbing) ------------------------------

    /// Expression entry point with a recursion-depth guard (parens, the only
    /// unbounded nesting other than NOT/unary, re-enter here).
    fn parse_expr(&mut self) -> SqlResult<Expr> {
        if self.depth == 0 {
            // The node budget is per top-level expression (see
            // MAX_EXPR_NODES): reset it here rather than accumulating across
            // the batch, or flat many-expression statements (a 1000-tuple
            // INSERT) exhaust a budget meant for one deep spine.
            self.nodes = 0;
        }
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(Self::too_deep());
        }
        let expr = self.parse_or()?;
        self.depth -= 1;
        Ok(expr)
    }

    fn parse_or(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_and()?;
        while self.peek_keyword().as_deref() == Some("OR") {
            self.bump();
            let right = self.parse_and()?;
            self.node()?;
            left = binary(BinaryOp::Or, left, right);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_not()?;
        while self.peek_keyword().as_deref() == Some("AND") {
            self.bump();
            let right = self.parse_not()?;
            self.node()?;
            left = binary(BinaryOp::And, left, right);
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> SqlResult<Expr> {
        if self.peek_keyword().as_deref() == Some("NOT") {
            self.depth += 1;
            if self.depth > MAX_EXPR_DEPTH {
                return Err(Self::too_deep());
            }
            let start = self.bump().span;
            let expr = self.parse_not()?;
            self.depth -= 1;
            self.node()?;
            return Ok(Expr {
                span: start.to(expr.span),
                kind: ExprKind::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                },
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> SqlResult<Expr> {
        let left = self.parse_additive()?;
        // IS [NOT] NULL
        if self.peek_keyword().as_deref() == Some("IS") {
            self.bump();
            let negated = if self.peek_keyword().as_deref() == Some("NOT") {
                self.bump();
                true
            } else {
                false
            };
            let end = self.expect_keyword("NULL")?;
            self.node()?;
            return Ok(Expr {
                span: left.span.to(end),
                kind: ExprKind::IsNull {
                    expr: Box::new(left),
                    negated,
                },
            });
        }
        // [NOT] LIKE / IN / BETWEEN (the trailing-NOT predicate form).
        let negated = self.peek_keyword().as_deref() == Some("NOT")
            && matches!(
                self.peek_keyword_at(1).as_deref(),
                Some("LIKE") | Some("IN") | Some("BETWEEN")
            );
        if negated {
            self.bump();
        }
        match self.peek_keyword().as_deref() {
            Some("LIKE") => return self.parse_like(left, negated),
            Some("IN") => return self.parse_in(left, negated),
            Some("BETWEEN") => return self.parse_between(left, negated),
            _ => {}
        }
        let op = match self.peek().kind {
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::Ne => BinaryOp::Ne,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::Le => BinaryOp::Le,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::Ge => BinaryOp::Ge,
            _ => return Ok(left),
        };
        self.bump();
        let right = self.parse_additive()?;
        self.node()?;
        Ok(binary(op, left, right))
    }

    fn parse_like(&mut self, left: Expr, negated: bool) -> SqlResult<Expr> {
        self.bump(); // LIKE
        let pattern = self.parse_additive()?;
        let mut end = pattern.span;
        let escape = if self.peek_keyword().as_deref() == Some("ESCAPE") {
            self.bump();
            let token = self.bump();
            end = token.span;
            match &token.kind {
                TokenKind::String(s) if s.chars().count() == 1 => s.chars().next(),
                _ => return Err(SqlError::syntax(self.token_text(&token), token.span)),
            }
        } else {
            None
        };
        self.node()?;
        Ok(Expr {
            span: left.span.to(end),
            kind: ExprKind::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                escape,
                negated,
            },
        })
    }

    fn parse_in(&mut self, left: Expr, negated: bool) -> SqlResult<Expr> {
        self.bump(); // IN
        self.expect(&TokenKind::LParen)?;
        // `expr IN (SELECT ...)` is a subquery; otherwise a value list.
        if self.peek_keyword().as_deref() == Some("SELECT") {
            let subquery = self.parse_select()?;
            let end = self.expect(&TokenKind::RParen)?;
            self.node()?;
            return Ok(Expr {
                span: left.span.to(end),
                kind: ExprKind::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                    negated,
                },
            });
        }
        let mut list = Vec::new();
        loop {
            list.push(self.parse_expr()?);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: left.span.to(end),
            kind: ExprKind::InList {
                expr: Box::new(left),
                list,
                negated,
            },
        })
    }

    fn parse_between(&mut self, left: Expr, negated: bool) -> SqlResult<Expr> {
        self.bump(); // BETWEEN
        // `low`/`high` parse at additive precedence so BETWEEN's `AND` is not
        // swallowed as a boolean connective.
        let low = self.parse_additive()?;
        self.expect_keyword("AND")?;
        let high = self.parse_additive()?;
        self.node()?;
        Ok(Expr {
            span: left.span.to(high.span),
            kind: ExprKind::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            },
        })
    }

    fn parse_function(&mut self, name: Name) -> SqlResult<Expr> {
        self.expect(&TokenKind::LParen)?;
        if let Some(func) = agg_func(&name.value) {
            return self.parse_aggregate(name, func);
        }
        let mut args = Vec::new();
        if !self.check(&TokenKind::RParen) {
            loop {
                args.push(self.parse_expr()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: name.span.to(end),
            kind: ExprKind::Function {
                name: name.value,
                args,
            },
        })
    }

    /// Parses an aggregate call body (the opening `(` is already consumed):
    /// `COUNT(*)`, `COUNT([DISTINCT|ALL] expr)`, `SUM/AVG/MIN/MAX(...)`.
    fn parse_aggregate(&mut self, name: Name, func: AggFunc) -> SqlResult<Expr> {
        // COUNT(*) — the only aggregate that takes a star.
        if func == AggFunc::Count && self.check(&TokenKind::Star) {
            self.bump();
            let end = self.expect(&TokenKind::RParen)?;
            self.node()?;
            return Ok(Expr {
                span: name.span.to(end),
                kind: ExprKind::Aggregate {
                    func,
                    distinct: false,
                    arg: None,
                },
            });
        }
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
        let arg = self.parse_expr()?;
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: name.span.to(end),
            kind: ExprKind::Aggregate {
                func,
                distinct,
                arg: Some(Box::new(arg)),
            },
        })
    }

    fn parse_case(&mut self) -> SqlResult<Expr> {
        let start = self.bump().span; // CASE
        // A simple CASE has an operand before the first WHEN.
        let operand = if self.peek_keyword().as_deref() == Some("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        let mut branches = Vec::new();
        while self.peek_keyword().as_deref() == Some("WHEN") {
            self.bump();
            let cond = self.parse_expr()?;
            self.expect_keyword("THEN")?;
            let result = self.parse_expr()?;
            self.node()?;
            branches.push((cond, result));
        }
        if branches.is_empty() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        let else_result = if self.peek_keyword().as_deref() == Some("ELSE") {
            self.bump();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let end = self.expect_keyword("END")?;
        self.node()?;
        Ok(Expr {
            span: start.to(end),
            kind: ExprKind::Case {
                operand,
                branches,
                else_result,
            },
        })
    }

    fn parse_cast(&mut self) -> SqlResult<Expr> {
        let start = self.bump().span; // CAST
        self.expect(&TokenKind::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword("AS")?;
        let (target, _) = self.parse_data_type()?;
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: start.to(end),
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target,
            },
        })
    }

    fn parse_convert(&mut self) -> SqlResult<Expr> {
        let start = self.bump().span; // CONVERT
        self.expect(&TokenKind::LParen)?;
        let (target, _) = self.parse_data_type()?;
        self.expect(&TokenKind::Comma)?;
        let expr = self.parse_expr()?;
        // An optional style argument is accepted and ignored for now.
        if self.eat(&TokenKind::Comma) {
            let _ = self.parse_expr()?;
        }
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: start.to(end),
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target,
            },
        })
    }

    fn parse_additive(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek().kind {
                TokenKind::Plus => BinaryOp::Add,
                TokenKind::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.bump();
            let right = self.parse_multiplicative()?;
            self.node()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek().kind {
                TokenKind::Star => BinaryOp::Mul,
                TokenKind::Slash => BinaryOp::Div,
                TokenKind::Percent => BinaryOp::Mod,
                _ => break,
            };
            self.bump();
            let right = self.parse_unary()?;
            self.node()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> SqlResult<Expr> {
        if self.check(&TokenKind::Minus) {
            self.depth += 1;
            if self.depth > MAX_EXPR_DEPTH {
                return Err(Self::too_deep());
            }
            let start = self.bump().span;
            let expr = self.parse_unary()?;
            self.depth -= 1;
            self.node()?;
            return Ok(Expr {
                span: start.to(expr.span),
                kind: ExprKind::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                },
            });
        }
        if self.check(&TokenKind::Plus) {
            self.depth += 1;
            if self.depth > MAX_EXPR_DEPTH {
                return Err(Self::too_deep());
            }
            self.bump();
            let expr = self.parse_unary()?;
            self.depth -= 1;
            return Ok(expr);
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> SqlResult<Expr> {
        self.node()?;
        let token = self.peek().clone();
        match &token.kind {
            TokenKind::Int(v) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::Int(*v),
                    span: token.span,
                })
            }
            TokenKind::Number(text) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::Number(text.clone()),
                    span: token.span,
                })
            }
            TokenKind::String(s) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::Str(s.clone()),
                    span: token.span,
                })
            }
            TokenKind::GlobalVar(name) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::GlobalVar(name.clone()),
                    span: token.span,
                })
            }
            TokenKind::LocalVar(name) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::LocalVar(name.clone()),
                    span: token.span,
                })
            }
            TokenKind::LParen => {
                // `(SELECT ...)` is a scalar subquery; otherwise a grouping paren.
                if self.peek_keyword_at(1).as_deref() == Some("SELECT") {
                    let start = self.bump().span; // (
                    let subquery = self.parse_select()?;
                    let end = self.expect(&TokenKind::RParen)?;
                    Ok(Expr {
                        kind: ExprKind::Subquery(Box::new(subquery)),
                        span: start.to(end),
                    })
                } else {
                    self.bump();
                    let inner = self.parse_expr()?;
                    self.expect(&TokenKind::RParen)?;
                    Ok(inner)
                }
            }
            TokenKind::Word { quoted, .. } => {
                let keyword = token.keyword();
                match keyword.as_deref() {
                    Some("NULL") if !quoted => {
                        self.bump();
                        Ok(Expr {
                            kind: ExprKind::Null,
                            span: token.span,
                        })
                    }
                    Some("TRUE") if !quoted => {
                        self.bump();
                        Ok(Expr {
                            kind: ExprKind::Bool(true),
                            span: token.span,
                        })
                    }
                    Some("FALSE") if !quoted => {
                        self.bump();
                        Ok(Expr {
                            kind: ExprKind::Bool(false),
                            span: token.span,
                        })
                    }
                    Some("CASE") if !quoted => self.parse_case(),
                    Some("CAST") if !quoted => self.parse_cast(),
                    Some("CONVERT") if !quoted => self.parse_convert(),
                    Some("EXISTS") if !quoted => {
                        // `EXISTS (SELECT ...)`; `NOT EXISTS` is parse_not over this.
                        let start = self.bump().span;
                        self.expect(&TokenKind::LParen)?;
                        let subquery = self.parse_select()?;
                        let end = self.expect(&TokenKind::RParen)?;
                        Ok(Expr {
                            kind: ExprKind::Exists(Box::new(subquery)),
                            span: start.to(end),
                        })
                    }
                    Some(kw) if !quoted && is_reserved(kw) => {
                        Err(SqlError::syntax(self.token_text(&token), token.span))
                    }
                    _ => {
                        let name = self.parse_name()?;
                        // A name followed by `(` is a function call — including a
                        // schema-qualified one (`dbo.f(@x)`), the canonical way to
                        // call a user-defined function. A column reference is
                        // never followed by `(`, so this never misreads one.
                        if self.check(&TokenKind::LParen) {
                            self.parse_function(name)
                        } else {
                            Ok(Expr {
                                span: name.span,
                                kind: ExprKind::Column(name),
                            })
                        }
                    }
                }
            }
            _ => Err(SqlError::syntax(self.token_text(&token), token.span)),
        }
    }

    // ---- helpers --------------------------------------------------------

    /// Parses a possibly schema-qualified name (`schema.name`), joining the
    /// parts with `.` into a single value (e.g. `sys.tables`). Stage 3 has
    /// one user schema (`dbo`) plus the `sys` catalog views; deeper
    /// qualification is left to later stages.
    fn parse_name(&mut self) -> SqlResult<Name> {
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

    fn parse_ident(&mut self) -> SqlResult<Name> {
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

    fn parse_u32_literal(&mut self) -> SqlResult<u32> {
        let value = self.parse_u64_literal()?;
        u32::try_from(value)
            .map_err(|_| SqlError::message_only(1073, "Length value is out of range."))
    }

    /// Parses a signed integer literal (optional leading `-`), for IDENTITY
    /// seed/increment.
    fn parse_i64_literal(&mut self) -> SqlResult<i64> {
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

    fn parse_u64_literal(&mut self) -> SqlResult<u64> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Int(v) if v >= 0 => {
                self.bump();
                Ok(v as u64)
            }
            _ => Err(SqlError::syntax(self.token_text(&token), token.span)),
        }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn peek_keyword(&self) -> Option<String> {
        self.peek().keyword()
    }

    /// The keyword `offset` tokens ahead of the cursor (for two-token lookahead
    /// like `NOT LIKE`).
    fn peek_keyword_at(&self, offset: usize) -> Option<String> {
        self.tokens
            .get((self.pos + offset).min(self.tokens.len() - 1))
            .and_then(|t| t.keyword())
    }

    fn bump(&mut self) -> Token {
        let token = self.peek().clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        token
    }

    fn prev_span(&self) -> Span {
        let index = self.pos.saturating_sub(1).min(self.tokens.len() - 1);
        self.tokens[index].span
    }

    fn check(&self, kind: &TokenKind) -> bool {
        &self.peek().kind == kind
    }

    fn eat(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: &TokenKind) -> SqlResult<Span> {
        if self.check(kind) {
            Ok(self.bump().span)
        } else {
            let token = self.peek().clone();
            Err(SqlError::syntax(self.token_text(&token), token.span))
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> SqlResult<Span> {
        if self.peek_keyword().as_deref() == Some(keyword) {
            Ok(self.bump().span)
        } else {
            let token = self.peek().clone();
            Err(SqlError::syntax(self.token_text(&token), token.span))
        }
    }

    fn at_eof(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    fn token_text(&self, token: &Token) -> String {
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

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr {
        span: left.span.to(right.span),
        kind: ExprKind::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
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

/// The aggregate function for a name, if it is one (case-insensitive).
fn agg_func(name: &str) -> Option<AggFunc> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
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
mod tests {
    use super::*;

    #[test]
    fn deeply_nested_parens_error_not_overflow() {
        let sql = format!("SELECT {}1{}", "(".repeat(5000), ")".repeat(5000));
        let err = Parser::parse_str(&sql).expect_err("must reject, not overflow");
        assert_eq!(err.number, 191);
    }

    #[test]
    fn deeply_nested_from_error_not_overflow() {
        // Nested parenthesized-group FROM: must reject cleanly, not overflow.
        let group = format!("SELECT 1 FROM {}t{}", "(".repeat(5000), ")".repeat(5000));
        assert_eq!(Parser::parse_str(&group).unwrap_err().number, 191);
        // Nested derived tables likewise.
        let derived = format!(
            "SELECT * FROM {}SELECT * FROM t{} x",
            "(SELECT * FROM ".repeat(2000),
            ") y".repeat(2000),
        );
        assert_eq!(Parser::parse_str(&derived).unwrap_err().number, 191);
    }

    #[test]
    fn deep_not_and_unary_chains_error_not_overflow() {
        let nots = format!("SELECT {}1", "NOT ".repeat(5000));
        assert_eq!(Parser::parse_str(&nots).unwrap_err().number, 191);
        // Spaced so `--` is not read as a comment.
        let neg = format!("SELECT {}1", "- ".repeat(5000));
        assert_eq!(Parser::parse_str(&neg).unwrap_err().number, 191);
    }

    #[test]
    fn long_operator_chain_errors_not_overflow() {
        // Parses iteratively but would overflow eval; the node budget caps it.
        let sql = format!("SELECT 1{}", " OR 1".repeat(20_000));
        assert_eq!(Parser::parse_str(&sql).unwrap_err().number, 191);
    }

    #[test]
    fn the_node_budget_is_per_expression_not_per_batch() {
        // Thousands of tiny FLAT expressions are fine — none deepens an
        // evaluation spine. A per-batch count once made a 1001-tuple INSERT
        // unparseable, which put row-lock escalation above the reachable
        // ceiling.
        let tuples: Vec<String> = (0..3000).map(|i| format!("({i}, {i})")).collect();
        let sql = format!("INSERT INTO t VALUES {}", tuples.join(", "));
        assert!(Parser::parse_str(&sql).is_ok());
        // Two statements each near the budget: legal — the budget resets.
        // Each `OR` costs ~2 nodes (the operator and its literal): 900 ORs
        // ≈ 1801 nodes, inside the 2000 budget — twice over in one batch.
        let chain = format!("SELECT 1{}", " OR 1".repeat(900));
        let batch = format!("{chain}; {chain}");
        assert!(Parser::parse_str(&batch).is_ok());
        // One expression over the budget still errors, even at the end of an
        // otherwise-light batch (1001 ORs ≈ 2003 nodes).
        let over = format!("SELECT 1; SELECT 1{}", " OR 1".repeat(1001));
        assert_eq!(Parser::parse_str(&over).unwrap_err().number, 191);
        // CTE and derived-table bodies parse under depth >= 1 (no depth-0
        // reset for their expressions): the per-statement reset must cover
        // them, or a big-but-legal statement poisons the next one's budget.
        let big = format!("SELECT 1{}", " OR 1".repeat(900));
        let derived = format!("{big}; SELECT * FROM (SELECT 1{}) d", " OR 1".repeat(200));
        assert!(Parser::parse_str(&derived).is_ok(), "derived after big");
        let cte = format!(
            "{big}; WITH c AS (SELECT 1{} AS x) SELECT x FROM c",
            " OR 1".repeat(200)
        );
        assert!(Parser::parse_str(&cte).is_ok(), "cte after big");
    }

    #[test]
    fn reasonable_depth_is_accepted() {
        let sql = format!("SELECT {}1{}", "(".repeat(50), ")".repeat(50));
        assert!(Parser::parse_str(&sql).is_ok());
        let chain = format!("SELECT 1{}", " + 1".repeat(100));
        assert!(Parser::parse_str(&chain).is_ok());
    }

    #[test]
    fn try_catch_parses_into_blocks() {
        let stmts = Parser::parse_str(
            "BEGIN TRY \
               INSERT INTO t VALUES (1); \
               SELECT 2; \
             END TRY \
             BEGIN CATCH \
               SELECT ERROR_NUMBER(); \
             END CATCH",
        )
        .expect("parse");
        let Statement::TryCatch {
            try_block,
            catch_block,
            ..
        } = &stmts[0]
        else {
            panic!("expected a TRY/CATCH, got {:?}", stmts[0]);
        };
        assert_eq!(try_block.len(), 2, "two statements in the TRY block");
        assert_eq!(catch_block.len(), 1, "one statement in the CATCH block");
        assert!(matches!(try_block[0], Statement::Insert(_)));

        // A nested TRY inside the TRY block is consumed whole (its END TRY / END
        // CATCH do not close the outer block).
        let stmts = Parser::parse_str(
            "BEGIN TRY \
               BEGIN TRY SELECT 1; END TRY BEGIN CATCH SELECT 2; END CATCH; \
               SELECT 3; \
             END TRY \
             BEGIN CATCH SELECT 4; END CATCH",
        )
        .expect("parse");
        let Statement::TryCatch { try_block, .. } = &stmts[0] else {
            panic!("expected a TRY/CATCH");
        };
        assert_eq!(try_block.len(), 2, "nested TRY + the following SELECT");
        assert!(matches!(try_block[0], Statement::TryCatch { .. }));

        // An unterminated TRY block is a syntax error, not a hang.
        assert_eq!(
            Parser::parse_str("BEGIN TRY SELECT 1;").unwrap_err().number,
            102,
        );
    }

    #[test]
    fn try_catch_parses_without_statement_terminators() {
        // The canonical T-SQL form omits the `;` before END TRY / END CATCH.
        // `END` must not be read as an implicit alias for the preceding select
        // item (or table), which would leave the cursor on `TRY`.
        for sql in [
            "BEGIN TRY SELECT 1 END TRY BEGIN CATCH SELECT 2 END CATCH",
            "BEGIN TRY SELECT * FROM t END TRY BEGIN CATCH SELECT 2 END CATCH",
            "BEGIN TRY SELECT a FROM t WHERE a = 1 END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() END CATCH",
        ] {
            let stmts = Parser::parse_str(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            let Statement::TryCatch {
                try_block,
                catch_block,
                ..
            } = &stmts[0]
            else {
                panic!("expected a TRY/CATCH for {sql}");
            };
            assert_eq!(try_block.len(), 1, "{sql}");
            assert_eq!(catch_block.len(), 1, "{sql}");
        }

        // An explicit `AS end` still aliases (only the *bare* END is declined),
        // and a delimited [end] is an identifier, not the block terminator.
        let stmts = Parser::parse_str("SELECT 1 AS end").expect("AS end still aliases");
        assert!(matches!(stmts[0], Statement::Select(_)));
        let stmts = Parser::parse_str("SELECT 1 [end]").expect("[end] still aliases");
        assert!(matches!(stmts[0], Statement::Select(_)));
    }

    #[test]
    fn assignment_select_parses_as_assign_item() {
        // `SELECT @v = expr` is an assignment item, not a boolean comparison.
        let stmts = Parser::parse_str("SELECT @v = 1 + 2").expect("parse");
        let Statement::Select(select) = &stmts[0] else {
            panic!("expected select")
        };
        assert!(
            matches!(&select.items[0], SelectItem::Assign { target, .. } if target == "v"),
            "expected an assignment item: {:?}",
            select.items[0]
        );

        // `@v = x` inside a WHERE stays a comparison (only the item list assigns).
        let stmts = Parser::parse_str("SELECT 1 WHERE @v = 5").expect("parse");
        let Statement::Select(select) = &stmts[0] else {
            panic!("expected select")
        };
        assert!(matches!(&select.items[0], SelectItem::Expr { .. }));
        assert!(select.where_clause.is_some());

        // Mixing an assignment with a result column is a syntax-level error 141.
        assert_eq!(
            Parser::parse_str("SELECT @v = 1, 2").unwrap_err().number,
            141,
        );
    }

    #[test]
    fn ignorable_set_options_parse_as_noops() {
        // Cosmetic/advisory options clients send at connection time: ON/OFF
        // flags, value forms, a signed value, and a required-ON option at ON.
        // (NOCOUNT graduated to a real option in Stage 14.)
        let sql = "SET QUOTED_IDENTIFIER ON; SET ANSI_WARNINGS OFF; \
                   SET TEXTSIZE 2147483647; SET DATEFORMAT mdy; SET LOCK_TIMEOUT -1";
        let stmts = Parser::parse_str(sql).expect("all recognized as no-ops");
        assert_eq!(stmts.len(), 5);
        assert!(
            stmts
                .iter()
                .all(|s| matches!(s, Statement::Set(SetStatement::Ignored))),
            "every option should parse to SetStatement::Ignored: {stmts:?}",
        );
        // NOCOUNT is a real session option now.
        let stmts = Parser::parse_str("SET NOCOUNT ON; SET NOCOUNT OFF").expect("parses");
        assert!(matches!(
            stmts.as_slice(),
            [
                Statement::Set(SetStatement::NoCount(true)),
                Statement::Set(SetStatement::NoCount(false))
            ]
        ));
        // An unknown option is still a syntax error, not silently ignored.
        assert_eq!(Parser::parse_str("SET WHATSIT ON").unwrap_err().number, 102);
    }

    #[test]
    fn result_changing_set_options_are_not_silently_ignored() {
        // OFF for an option TruthDB hardwires to ON must be rejected, never
        // silently accepted (it would change query results).
        assert_eq!(
            Parser::parse_str("SET ANSI_NULLS OFF").unwrap_err().number,
            102,
        );
        assert_eq!(
            Parser::parse_str("SET CONCAT_NULL_YIELDS_NULL OFF")
                .unwrap_err()
                .number,
            102,
        );
        // ...but the matching ON is a faithful no-op.
        assert!(matches!(
            Parser::parse_str("SET ANSI_NULLS ON").as_deref(),
            Ok([Statement::Set(SetStatement::Ignored)]),
        ));
        // Options that change what/how much runs stay hard errors, not no-ops,
        // so we never silently drop a client's row cap or skip flag.
        for sql in [
            "SET ROWCOUNT 100",
            "SET NOEXEC ON",
            "SET IMPLICIT_TRANSACTIONS ON",
        ] {
            assert_eq!(
                Parser::parse_str(sql).unwrap_err().number,
                102,
                "{sql} must not be a silent no-op",
            );
        }
    }

    #[test]
    fn table_level_primary_key_duplicate_rejected() {
        let sql = "CREATE TABLE t (a INT, PRIMARY KEY (a), PRIMARY KEY (a))";
        assert_eq!(Parser::parse_str(sql).unwrap_err().number, 8110);
        let sql2 = "CREATE TABLE t (id INT PRIMARY KEY, PRIMARY KEY (id))";
        assert_eq!(Parser::parse_str(sql2).unwrap_err().number, 8110);
    }
}
