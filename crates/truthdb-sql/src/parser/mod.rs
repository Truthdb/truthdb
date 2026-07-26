//! Hand-rolled recursive-descent parser for the Stage 3 grammar (no
//! sqlparser-rs, per the plan). Expression precedence, low to high:
//! `OR` < `AND` < `NOT` < comparison/`IS NULL` < `+ -` < `* / %` < unary `-`
//! < primary.

use crate::ast::*;
use crate::error::{SqlError, SqlResult};
use crate::lexer::{Span, Token, TokenKind};

mod backup_restore;
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
