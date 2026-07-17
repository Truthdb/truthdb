//! TruthDB SQL front end (pure: no I/O, no storage dependency).
//!
//! Pipeline: [`lexer`] tokenizes, [`parser`] builds an [`ast`] with source
//! spans, and callers bind/execute the AST against a catalog. [`value`]
//! carries SQL values with three-valued-logic semantics used by the
//! expression [`eval`]uator. Errors are [`SqlError`]s with SQL
//! Server-compatible numbers (see [`error`]).

pub mod ast;
pub mod collation;
pub mod decimal;
pub mod error;
pub mod eval;
pub mod functions;
pub mod guid;
pub mod lexer;
pub mod like;
pub mod parser;
pub mod temporal;
pub mod value;

pub use ast::{Expr, Statement};
pub use error::{SqlError, SqlResult};
pub use value::SqlValue;

/// Parses a single batch of `;`-separated statements.
pub fn parse(sql: &str) -> SqlResult<Vec<Statement>> {
    let tokens = lexer::Lexer::new(sql).tokenize()?;
    parser::Parser::from_tokens(sql, tokens).parse_statements()
}

/// Parses a stored-procedure body: the in-procedure grammar, where
/// `RETURN <value>` is legal (178 outside one).
pub fn parse_procedure_body(sql: &str) -> SqlResult<Vec<Statement>> {
    let tokens = lexer::Lexer::new(sql).tokenize()?;
    let mut parser = parser::Parser::from_tokens(sql, tokens);
    parser.set_in_procedure();
    parser.parse_statements()
}

/// Parses a scalar-function body: the in-function grammar, where `RETURN <expr>`
/// yields the function's mandatory typed result.
pub fn parse_function_body(sql: &str) -> SqlResult<Vec<Statement>> {
    let tokens = lexer::Lexer::new(sql).tokenize()?;
    let mut parser = parser::Parser::from_tokens(sql, tokens);
    parser.set_in_function();
    parser.parse_statements()
}

/// Parses a single standalone expression (e.g. a column DEFAULT re-parsed at
/// INSERT time). Rejects trailing tokens.
pub fn parse_expr(sql: &str) -> SqlResult<Expr> {
    let tokens = lexer::Lexer::new(sql).tokenize()?;
    parser::Parser::from_tokens(sql, tokens).parse_single_expr()
}
