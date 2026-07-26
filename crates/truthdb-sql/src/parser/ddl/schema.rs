use super::*;

impl Parser {
    pub(super) fn parse_create_view(&mut self, start: Span) -> SqlResult<Statement> {
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

    pub(super) fn parse_create_index(&mut self, start: Span, unique: bool) -> SqlResult<Statement> {
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

    pub(super) fn parse_create_table(&mut self, start: Span) -> SqlResult<Statement> {
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
    pub(super) fn parse_optional_constraint_name(&mut self) -> SqlResult<Option<Name>> {
        if self.peek_keyword().as_deref() == Some("CONSTRAINT") {
            self.bump();
            Ok(Some(self.parse_name()?))
        } else {
            Ok(None)
        }
    }

    /// Parses `CHECK (predicate)` (the `CONSTRAINT name` prefix, if any, is
    /// already consumed). The predicate is kept as source text.
    pub(super) fn parse_check_constraint(
        &mut self,
        name: Option<Name>,
    ) -> SqlResult<CheckConstraint> {
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
    pub(super) fn parse_foreign_key(&mut self, name: Option<Name>) -> SqlResult<ForeignKey> {
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
    pub(super) fn parse_column_reference(
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
    pub(super) fn parse_name_list(&mut self) -> SqlResult<Vec<Name>> {
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
    pub(super) fn parse_optional_reference_columns(
        &mut self,
        fallback: Span,
    ) -> SqlResult<(Vec<Name>, Span)> {
        if self.check(&TokenKind::LParen) {
            let cols = self.parse_name_list()?;
            let end = cols.last().map(|n| n.span).unwrap_or(fallback);
            Ok((cols, end))
        } else {
            Ok((Vec::new(), fallback))
        }
    }

    pub(in crate::parser) fn parse_column_def(&mut self) -> SqlResult<ColumnDef> {
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
    pub(super) fn parse_identity(&mut self, fallback: Span) -> SqlResult<(Identity, Span)> {
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

    pub(in crate::parser) fn parse_data_type(&mut self) -> SqlResult<(DataType, Span)> {
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
    pub(super) fn parse_decimal_args(&mut self, span: Span) -> SqlResult<(u8, u8, Span)> {
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

    // ---- ALTER DATABASE -------------------------------------------------

    /// `ALTER DATABASE {name | CURRENT} SET <option> {ON|OFF} [, ...]`.
    /// Only the Stage 13 versioning options are recognized; anything else is
    /// a syntax error rather than a silent no-op (these options change what
    /// concurrent readers see).
    pub(super) fn parse_alter_database(&mut self, start: Span) -> SqlResult<Statement> {
        self.expect_keyword("DATABASE")?;
        let name = if self.peek_keyword().as_deref() == Some("CURRENT") {
            self.bump();
            None
        } else {
            Some(self.parse_name()?)
        };
        // `ALTER DATABASE <name> FAILOVER` (no SET): standby promotion.
        if self.peek_keyword().as_deref() == Some("FAILOVER") {
            let end = self.bump().span;
            return Ok(Statement::AlterDatabase(AlterDatabase {
                name,
                options: vec![(DatabaseOption::Failover, true)],
                span: start.to(end),
            }));
        }
        self.expect_keyword("SET")?;
        let mut options = Vec::new();
        let mut end;
        loop {
            let (option, value) = match self.peek_keyword().as_deref() {
                Some("READ_COMMITTED_SNAPSHOT") => {
                    end = self.bump().span;
                    (DatabaseOption::ReadCommittedSnapshot, self.parse_on_off()?)
                }
                Some("ALLOW_SNAPSHOT_ISOLATION") => {
                    end = self.bump().span;
                    (DatabaseOption::AllowSnapshotIsolation, self.parse_on_off()?)
                }
                // `SET RECOVERY {FULL | SIMPLE}` — a mode keyword, not ON/OFF.
                Some("RECOVERY") => {
                    self.bump();
                    let is_full = match self.peek_keyword().as_deref() {
                        Some("FULL") => true,
                        Some("SIMPLE") => false,
                        _ => {
                            let token = self.peek().clone();
                            return Err(SqlError::syntax(self.token_text(&token), token.span));
                        }
                    };
                    end = self.bump().span;
                    (DatabaseOption::Recovery, is_full)
                }
                _ => {
                    let token = self.peek().clone();
                    return Err(SqlError::syntax(self.token_text(&token), token.span));
                }
            };
            options.push((option, value));
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

    pub(super) fn parse_drop_view(&mut self, start: Span) -> SqlResult<Statement> {
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

    pub(super) fn parse_drop_index(&mut self, start: Span) -> SqlResult<Statement> {
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

    pub(super) fn parse_drop_table(&mut self, start: Span) -> SqlResult<Statement> {
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
}
