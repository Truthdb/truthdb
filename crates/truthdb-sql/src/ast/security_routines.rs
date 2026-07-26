use crate::lexer::Span;

use super::{DataType, Name};

/// `GRANT` / `DENY` / `REVOKE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionKind {
    Grant,
    Deny,
    Revoke,
}

/// An object privilege named in `GRANT`/`DENY`/`REVOKE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionAction {
    Select,
    Insert,
    Update,
    Delete,
    Execute,
    References,
    Alter,
}

/// `GRANT|DENY|REVOKE <actions> ON <object> TO|FROM <grantees>`.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionStatement {
    pub kind: PermissionKind,
    pub actions: Vec<PermissionAction>,
    pub object: Name,
    pub grantees: Vec<Name>,
    pub span: Span,
}

/// Whether `ALTER ROLE` adds or removes the named member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleMemberAction {
    Add,
    Drop,
}

/// `CREATE USER <name> [FOR LOGIN <login>]`. A user with no `FOR LOGIN` clause is
/// a user without a login (valid, but cannot authenticate through it).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUser {
    pub name: Name,
    pub for_login: Option<Name>,
    pub span: Span,
}

/// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` / `ALTER LOGIN <name>
/// {ENABLE | DISABLE}`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateLogin {
    pub name: Name,
    /// The plaintext password to hash. `Some` for CREATE and for ALTER ... WITH
    /// PASSWORD; `None` for an ALTER that only toggles enabled/disabled.
    pub password: Option<String>,
    /// `Some(true)` = DISABLE, `Some(false)` = ENABLE, `None` = unchanged.
    pub disable: Option<bool>,
    /// `ALTER LOGIN` replaces an existing login's payload.
    pub alter: bool,
    pub span: Span,
}

/// `CREATE|ALTER TRIGGER <name> ON <table> AFTER <events> AS <body>`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigger {
    pub name: Name,
    /// The table the trigger is attached to.
    pub target: Name,
    /// The DML events it fires on (at least one; deduplicated by the parser).
    pub events: Vec<TriggerEvent>,
    /// `INSTEAD OF` (fires in place of the DML) rather than `AFTER`/`FOR`.
    pub instead_of: bool,
    /// The body source text (everything after `AS`), re-parsed per firing.
    pub body: String,
    /// `ALTER TRIGGER` replaces an existing definition.
    pub alter: bool,
    pub span: Span,
}

/// A DML event a trigger fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

/// `CREATE|ALTER PROC[EDURE] <name> [@p TYPE [= default] [OUTPUT], ...] AS <body>`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateProcedure {
    pub name: Name,
    pub params: Vec<ProcParam>,
    /// The body's source text: everything after `AS`, verbatim.
    pub body: String,
    /// `ALTER PROCEDURE` replaces an existing definition.
    pub alter: bool,
    pub span: Span,
}

/// `CREATE|ALTER FUNCTION <name> ( [@p TYPE [= default], ...] ) RETURNS <ret> AS <body>`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunction {
    pub name: Name,
    pub params: Vec<ProcParam>,
    pub returns: ReturnsClause,
    /// The body's source text: everything after `AS`, verbatim (scalar form).
    pub body: String,
    /// `ALTER FUNCTION` replaces an existing definition.
    pub alter: bool,
    pub span: Span,
}

/// A function's declared return shape.
#[derive(Debug, Clone, PartialEq)]
pub enum ReturnsClause {
    /// `RETURNS <scalar type>`: a scalar UDF.
    Scalar(DataType),
    /// `RETURNS TABLE AS RETURN ( <select> )`: an inline table-valued function.
    /// The body is the SELECT (its source captured in `CreateFunction.body`).
    InlineTable,
    /// `RETURNS @t TABLE ( <column-defs> ) AS BEGIN … RETURN END`: a
    /// multi-statement table-valued function. The named table variable is
    /// declared here, populated by the body (captured in `CreateFunction.body`),
    /// and its final rows are the function's result. The column list is kept as
    /// source text and re-parsed per call, exactly like the scalar/inline body.
    MultiTable {
        /// The result table variable's name, without the leading `@`, lowercased.
        var_name: String,
        /// The `( <column-defs> )` source text (including the parentheses).
        columns_text: String,
    },
}

/// One declared procedure parameter.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcParam {
    /// Lowercased, without the `@`.
    pub name: String,
    pub data_type: DataType,
    /// Default value source text (the parameter is then optional at EXEC).
    pub default_text: Option<String>,
    /// `OUTPUT`/`OUT`.
    pub output: bool,
    pub span: Span,
}
