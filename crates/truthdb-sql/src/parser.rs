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

/// Maximum number of expression nodes per batch. Bounds the AST size so a
/// long operator chain (`1 OR 1 OR 1 ...`), which parses iteratively but
/// evaluates recursively down its spine, cannot overflow the stack during
/// evaluation.
const MAX_EXPR_NODES: usize = 2000;

pub struct Parser {
    /// The original SQL source, for slicing sub-expression text (e.g. a
    /// column DEFAULT) by span.
    src: String,
    tokens: Vec<Token>,
    pos: usize,
    /// Current expression recursion depth.
    depth: usize,
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
            Some("SELECT") => Ok(Statement::Select(self.parse_select()?)),
            Some("BEGIN") => self.parse_begin(),
            Some("COMMIT") => self.parse_commit(),
            Some("ROLLBACK") => self.parse_rollback(),
            Some("SET") => self.parse_set(),
            Some("DECLARE") => self.parse_declare(),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    // ---- transaction control --------------------------------------------

    fn parse_begin(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("BEGIN")?;
        // Stage 6 has no BEGIN...END blocks; BEGIN must open a transaction.
        let mut end = match self.peek_keyword().as_deref() {
            Some("TRAN") | Some("TRANSACTION") => self.bump().span,
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
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

    fn parse_commit(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("COMMIT")?;
        let end = self.eat_optional_tran_and_name(start);
        Ok(Statement::Commit {
            span: start.to(end),
        })
    }

    fn parse_rollback(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("ROLLBACK")?;
        let end = self.eat_optional_tran_and_name(start);
        Ok(Statement::Rollback {
            span: start.to(end),
        })
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
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
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
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
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
        let end = self.expect(&TokenKind::RParen)?;
        Ok(Statement::CreateIndex(CreateIndex {
            name,
            table,
            unique,
            columns,
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
        loop {
            // A leading `CONSTRAINT name` introduces a named table constraint.
            let constraint_name = self.parse_optional_constraint_name()?;
            match self.peek_keyword().as_deref() {
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
        let mut end = type_span;
        loop {
            match self.peek_keyword().as_deref() {
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
        let with_len = |parser: &mut Self, default: u32| -> SqlResult<(u32, Span)> {
            if parser.eat(&TokenKind::LParen) {
                if parser.peek_keyword().as_deref() == Some("MAX") {
                    return Err(SqlError::message_only(
                        102,
                        "(MAX) length types are not supported until a later stage.",
                    ));
                }
                let n = parser.parse_u32_literal()?;
                let end = parser.expect(&TokenKind::RParen)?;
                Ok((n, end))
            } else {
                Ok((default, span))
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
                return Ok((DataType::VarChar(n), span.to(end)));
            }
            "NVARCHAR" | "NCHAR" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((DataType::NVarChar(n), span.to(end)));
            }
            "VARBINARY" | "BINARY" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((DataType::VarBinary(n), span.to(end)));
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
        self.expect_keyword("TABLE")?;
        let table = self.parse_name()?;
        let (action, end) = match self.peek_keyword().as_deref() {
            Some("ADD") => {
                self.bump();
                // `ADD [CONSTRAINT name] (CHECK | FOREIGN KEY ...)`. ADD column
                // arrives in a later part.
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

    // ---- DROP TABLE -----------------------------------------------------

    /// Dispatches `DROP TABLE` vs `DROP INDEX`.
    fn parse_drop(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("DROP")?;
        match self.peek_keyword().as_deref() {
            Some("INDEX") => self.parse_drop_index(start),
            Some("TABLE") => self.parse_drop_table(start),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
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

    fn parse_select(&mut self) -> SqlResult<Select> {
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
            } else {
                let expr = self.parse_expr()?;
                let alias = self.parse_optional_alias()?;
                items.push(SelectItem::Expr { expr, alias });
            }
            if !self.eat(&TokenKind::Comma) {
                break;
            }
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
                        // A single identifier followed by `(` is a function call.
                        if !name.value.contains('.') && self.check(&TokenKind::LParen) {
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
    matches!(
        keyword,
        "FROM" | "WHERE" | "ORDER" | "GROUP" | "HAVING" | "AS"
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
    fn reasonable_depth_is_accepted() {
        let sql = format!("SELECT {}1{}", "(".repeat(50), ")".repeat(50));
        assert!(Parser::parse_str(&sql).is_ok());
        let chain = format!("SELECT 1{}", " + 1".repeat(100));
        assert!(Parser::parse_str(&chain).is_ok());
    }

    #[test]
    fn table_level_primary_key_duplicate_rejected() {
        let sql = "CREATE TABLE t (a INT, PRIMARY KEY (a), PRIMARY KEY (a))";
        assert_eq!(Parser::parse_str(sql).unwrap_err().number, 8110);
        let sql2 = "CREATE TABLE t (id INT PRIMARY KEY, PRIMARY KEY (id))";
        assert_eq!(Parser::parse_str(sql2).unwrap_err().number, 8110);
    }
}
