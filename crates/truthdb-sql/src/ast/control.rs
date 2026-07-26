use crate::lexer::Span;

use super::{DataType, Expr, Name};

/// `THROW [number, message, state]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrowStatement {
    /// `None` = the bare re-throw form.
    pub args: Option<ThrowArgs>,
    pub span: Span,
}

/// The three arguments of a parameterized `THROW`.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrowArgs {
    pub number: Expr,
    pub message: Expr,
    pub state: Expr,
}

/// `RAISERROR(msg, severity, state [, args...]) [WITH option, ...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct RaiseError {
    /// The message text (or a message id, which TruthDB rejects — there is no
    /// `sys.messages`).
    pub message: Expr,
    pub severity: Expr,
    pub state: Expr,
    /// printf-style substitution arguments.
    pub args: Vec<Expr>,
    pub log: bool,
    pub nowait: bool,
    pub seterror: bool,
    pub span: Span,
}

/// One `@name TYPE [= initializer]` in a `DECLARE`.
#[derive(Debug, Clone, PartialEq)]
pub struct Declaration {
    /// Variable name without the `@`, lowercased.
    pub name: String,
    pub data_type: DataType,
    pub initializer: Option<Expr>,
    pub span: Span,
}

/// `EXEC[UTE] [@rc =] <proc> [[@name =] <expr> [OUTPUT] [, ...]]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecStatement {
    pub proc: Name,
    /// `EXEC @rc = proc`: the variable receiving the RETURN status
    /// (lowercased, without the `@`).
    pub return_var: Option<String>,
    pub args: Vec<ExecArg>,
    pub span: Span,
}

/// One argument of an `EXEC`: optionally named (`@p = expr`), optionally
/// `OUTPUT` (the argument must then be a variable, which receives the
/// parameter's final value).
#[derive(Debug, Clone, PartialEq)]
pub struct ExecArg {
    pub name: Option<Name>,
    pub value: Expr,
    pub output: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub enum SetStatement {
    XactAbort(bool),
    /// `SET NOCOUNT ON|OFF` — when on, statement DONEs carry no row count
    /// (the "(n rows affected)" chatter SSMS scripts turn off).
    NoCount(bool),
    IsolationLevel(IsolationLevel),
    /// `SET SHOWPLAN_TEXT ON|OFF` — when on, statements return their plan text
    /// instead of executing.
    ShowplanText(bool),
    /// `SET @v = expr` — assigns a batch variable.
    Variable {
        name: String,
        value: Expr,
    },
    /// A recognized session option that TruthDB accepts but ignores (client
    /// compatibility: `SET QUOTED_IDENTIFIER ON`, `SET NOCOUNT ON`,
    /// `SET TEXTSIZE 2147483647`, ...).
    Ignored,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    /// `SNAPSHOT` — transaction-scoped versioned reads (Stage 13). Gated at
    /// data access on `ALLOW_SNAPSHOT_ISOLATION` (3952), not at SET.
    Snapshot,
}
