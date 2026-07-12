//! SQL errors with SQL Server-compatible message numbers.
//!
//! The numbers matter: real drivers and tools key off them, so we mint the
//! same codes from day one (102 syntax, 208 invalid object, 207 invalid
//! column, 2627 PK/unique violation, 245 conversion, 515 NULL into NOT NULL,
//! 8152 string truncation, 8134 divide by zero, ...). `state` and `level`
//! default to SQL Server's common values for each class.

use crate::lexer::Span;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    pub number: i32,
    pub level: u8,
    pub state: u8,
    pub message: String,
    /// Source span, when the error is tied to a token.
    pub span: Option<Span>,
}

pub type SqlResult<T> = Result<T, SqlError>;

impl SqlError {
    pub fn new(number: i32, level: u8, state: u8, message: impl Into<String>) -> Self {
        SqlError {
            number,
            level,
            state,
            message: message.into(),
            span: None,
        }
    }

    pub fn at(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }

    /// 102: incorrect syntax near '<token>'.
    pub fn syntax(near: impl std::fmt::Display, span: Span) -> Self {
        SqlError::new(102, 15, 1, format!("Incorrect syntax near '{near}'.")).at(span)
    }

    /// 103/105-ish unterminated literal; SQL Server uses 105 for
    /// unterminated string constants.
    pub fn unterminated_string(span: Span) -> Self {
        SqlError::new(
            105,
            15,
            1,
            "Unclosed quotation mark after the character string.",
        )
        .at(span)
    }

    /// 208: invalid object (table) name.
    pub fn invalid_object(name: &str) -> Self {
        SqlError::new(208, 16, 1, format!("Invalid object name '{name}'."))
    }

    /// 207: invalid column name.
    pub fn invalid_column(name: &str) -> Self {
        SqlError::new(207, 16, 1, format!("Invalid column name '{name}'."))
    }

    /// 2627: primary key / unique constraint violation.
    pub fn pk_violation(table: &str) -> Self {
        SqlError::new(
            2627,
            14,
            1,
            format!(
                "Violation of PRIMARY KEY constraint. Cannot insert duplicate key in object 'dbo.{table}'."
            ),
        )
    }

    /// 515: cannot insert NULL into a NOT NULL column.
    pub fn null_into_not_null(column: &str, table: &str) -> Self {
        SqlError::new(
            515,
            16,
            2,
            format!(
                "Cannot insert the value NULL into column '{column}', table 'dbo.{table}'; column does not allow nulls."
            ),
        )
    }

    /// 245: conversion failed.
    pub fn conversion(message: impl Into<String>) -> Self {
        SqlError::new(245, 16, 1, message)
    }

    /// 8152: string or binary data would be truncated.
    pub fn string_truncation(column: &str) -> Self {
        SqlError::new(
            8152,
            16,
            2,
            format!("String or binary data would be truncated in column '{column}'."),
        )
    }

    /// 8134: divide by zero.
    pub fn divide_by_zero() -> Self {
        SqlError::new(8134, 16, 1, "Divide by zero error encountered.")
    }

    /// 156/parser: keyword used where an identifier is expected, etc.
    pub fn message_only(number: i32, message: impl Into<String>) -> Self {
        SqlError::new(number, 16, 1, message)
    }
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Msg {}, Level {}, State {}\n{}",
            self.number, self.level, self.state, self.message
        )
    }
}
