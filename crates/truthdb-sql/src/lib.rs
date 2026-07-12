//! TruthDB SQL front end (pure: no I/O, no storage dependency).
//!
//! Pipeline: [`lexer`] tokenizes, [`parser`] builds an [`ast`] with source
//! spans, and callers bind/execute the AST against a catalog. [`value`]
//! carries SQL values with three-valued-logic semantics used by the
//! expression [`eval`]uator. Errors are [`SqlError`]s with SQL
//! Server-compatible numbers (see [`error`]).

pub mod ast;
pub mod error;
pub mod eval;
pub mod lexer;
pub mod parser;
pub mod value;

pub use ast::Statement;
pub use error::{SqlError, SqlResult};
pub use value::SqlValue;

/// Parses a single batch of `;`-separated statements.
pub fn parse(sql: &str) -> SqlResult<Vec<Statement>> {
    let tokens = lexer::Lexer::new(sql).tokenize()?;
    parser::Parser::from_tokens(tokens).parse_statements()
}
