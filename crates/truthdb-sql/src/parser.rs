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
    tokens: Vec<Token>,
    pos: usize,
    /// Current expression recursion depth.
    depth: usize,
    /// Expression nodes built so far.
    nodes: usize,
}

impl Parser {
    /// Builds a parser over an already-tokenized batch (the token stream
    /// always ends with an `Eof` token).
    pub fn from_tokens(tokens: Vec<Token>) -> Self {
        debug_assert!(tokens.last().map(|t| &t.kind) == Some(&TokenKind::Eof));
        Parser {
            tokens,
            pos: 0,
            depth: 0,
            nodes: 0,
        }
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
        Parser::from_tokens(crate::lexer::Lexer::new(sql).tokenize()?).parse_statements()
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
            Some("CREATE") => self.parse_create_table(),
            Some("DROP") => self.parse_drop_table(),
            Some("INSERT") => self.parse_insert(),
            Some("SELECT") => Ok(Statement::Select(self.parse_select()?)),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    // ---- CREATE TABLE ---------------------------------------------------

    fn parse_create_table(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("CREATE")?;
        self.expect_keyword("TABLE")?;
        let table = self.parse_name()?;
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
        let end = self.expect(&TokenKind::RParen)?;
        Ok(Statement::CreateTable(CreateTable {
            table,
            columns,
            primary_key,
            span: start.to(end),
        }))
    }

    fn parse_column_def(&mut self) -> SqlResult<ColumnDef> {
        let name = self.parse_name()?;
        let (data_type, type_span) = self.parse_data_type()?;
        let mut nullable = None;
        let mut primary_key = false;
        let mut end = type_span;
        loop {
            match self.peek_keyword().as_deref() {
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
                _ => break,
            }
        }
        Ok(ColumnDef {
            span: name.span.to(end),
            name,
            data_type,
            nullable,
            primary_key,
        })
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
            "FLOAT" | "REAL" => DataType::Float,
            "VARCHAR" | "CHAR" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((DataType::VarChar(n), span.to(end)));
            }
            "NVARCHAR" | "NCHAR" => {
                let (n, end) = with_len(self, 1)?;
                return Ok((DataType::NVarChar(n), span.to(end)));
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

    // ---- DROP TABLE -----------------------------------------------------

    fn parse_drop_table(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("DROP")?;
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
            let end = self.expect(&TokenKind::RParen)?;
            rows.push(values);
            let _ = end;
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let end = self.prev_span();
        Ok(Statement::Insert(Insert {
            span: start.to(end),
            table,
            columns,
            rows,
        }))
    }

    // ---- SELECT ---------------------------------------------------------

    fn parse_select(&mut self) -> SqlResult<Select> {
        let start = self.expect_keyword("SELECT")?;
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
            Some(self.parse_name()?)
        } else {
            None
        };

        let where_clause = if self.peek_keyword().as_deref() == Some("WHERE") {
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
            items,
            from,
            where_clause,
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
            TokenKind::LParen => {
                self.bump();
                let inner = self.parse_expr()?;
                self.expect(&TokenKind::RParen)?;
                Ok(inner)
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
                    Some(kw) if !quoted && is_reserved(kw) => {
                        Err(SqlError::syntax(self.token_text(&token), token.span))
                    }
                    _ => {
                        let name = self.parse_name()?;
                        Ok(Expr {
                            span: name.span,
                            kind: ExprKind::Column(name),
                        })
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
