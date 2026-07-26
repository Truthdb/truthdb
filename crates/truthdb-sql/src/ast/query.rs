use crate::lexer::Span;

use super::{Expr, Name};

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: Name,
    /// Explicit column list, or None for "all columns in table order".
    pub columns: Option<Vec<Name>>,
    pub source: InsertSource,
    pub span: Span,
}

/// The rows an `INSERT` supplies: literal `VALUES` tuples or a `SELECT`.
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<Select>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table: Name,
    /// `SET col = expr` assignments, in source order.
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: Name,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: Name,
    pub where_clause: Option<Expr>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    /// `WITH name AS (SELECT ...), ...` common table expressions (empty = none).
    /// Non-recursive; expanded inline (as derived tables) before execution.
    pub ctes: Vec<Cte>,
    pub top: Option<u64>,
    /// `SELECT DISTINCT` — deduplicate the projected rows.
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    /// The FROM clause: a table or a join tree (absent for a constant SELECT).
    pub from: Option<TableRef>,
    pub where_clause: Option<Expr>,
    /// `GROUP BY <expr>, ...` (empty = no grouping).
    pub group_by: Vec<Expr>,
    /// `HAVING <predicate>` — filters groups after aggregation.
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub span: Span,
}

/// A `WITH` common table expression: `name AS (SELECT ...)`. The optional
/// column-rename list is not yet supported.
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: Name,
    pub query: Box<Select>,
}

/// A FROM clause: a base table (with optional alias) or a join of two table
/// references. Comma-separated tables desugar to `CROSS JOIN`.
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Table {
        name: Name,
        alias: Option<Name>,
    },
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        kind: JoinKind,
        /// The `ON` predicate (absent for `CROSS JOIN`).
        on: Option<Expr>,
    },
    /// A derived table: `(SELECT ...) AS alias`. The alias is required.
    Derived {
        subquery: Box<Select>,
        alias: Name,
    },
    /// A table-valued function call in FROM: `dbo.f(args) [AS alias]`. The alias
    /// is optional (an unaliased call exposes the bare function name).
    Function {
        name: Name,
        args: Vec<Expr>,
        alias: Option<Name>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// `CROSS APPLY`: the right side is re-evaluated per left row (correlated to
    /// it); a left row with no right rows is dropped (like an inner join).
    CrossApply,
    /// `OUTER APPLY`: like `CROSS APPLY`, but a left row with no right rows is
    /// kept with NULLs for the right columns (like a left join).
    OuterApply,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*`
    Wildcard,
    /// `table.*`
    QualifiedWildcard(Name),
    Expr {
        expr: Expr,
        alias: Option<Name>,
    },
    /// `@var = expr` — an assignment SELECT. All items must be assignments (a
    /// query cannot mix assignments with result columns). `target` is the
    /// variable name without its leading `@`, lowercased (as the lexer emits it).
    Assign {
        target: String,
        value: Expr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub descending: bool,
}
