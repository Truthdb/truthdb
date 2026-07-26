use crate::lexer::Span;

use super::{DataType, Select};

/// An identifier with its source span and whether it was delimited
/// (delimited identifiers are never treated as keywords).
#[derive(Debug, Clone, PartialEq)]
pub struct Name {
    pub value: String,
    pub quoted: bool,
    pub span: Span,
}

impl Name {
    /// Case-insensitive match against a plain identifier (delimited names
    /// compare case-sensitively in SQL Server under the default collation,
    /// but for object/column resolution we fold case for both — Stage 3
    /// keeps a single case-insensitive namespace).
    pub fn eq_ignore_case(&self, other: &str) -> bool {
        self.value.eq_ignore_ascii_case(other)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    pub kind: ExprKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    Null,
    Int(i64),
    /// Exact numeric/float literal text (typed at bind time).
    Number(String),
    Str(String),
    Bool(bool),
    Column(Name),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `expr [NOT] LIKE pattern [ESCAPE 'c']`.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<char>,
        negated: bool,
    },
    /// `expr [NOT] IN (v1, v2, ...)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// `CASE [operand] WHEN cond THEN result ... [ELSE result] END`. When
    /// `operand` is set it is a simple CASE (compared to each WHEN value).
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// `CAST(expr AS type)` / `CONVERT(type, expr)`.
    Cast {
        expr: Box<Expr>,
        target: DataType,
    },
    /// A scalar function call: `name(arg, ...)` (incl. ISNULL/COALESCE/IIF and
    /// niladic functions like GETDATE()).
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// An aggregate: `COUNT(*)` (arg `None`), `COUNT(x)`, `SUM(DISTINCT x)`,
    /// etc. Resolved by the grouping executor, never by scalar eval.
    Aggregate {
        func: AggFunc,
        distinct: bool,
        /// The argument expression; `None` only for `COUNT(*)`.
        arg: Option<Box<Expr>>,
    },
    /// A `@@`-prefixed global/session variable (e.g. `@@TRANCOUNT`), evaluated
    /// from the session's [`EvalContext`](crate::eval::EvalContext).
    GlobalVar(String),
    /// A `@`-prefixed local/batch variable (name without the `@`, lowercased),
    /// resolved from the batch's declared variables.
    LocalVar(String),
    /// A precomputed value. Not produced by the parser — the executor rewrites
    /// each evaluated subquery to a `Literal` so scalar evaluation stays free of
    /// storage access.
    Literal(crate::value::SqlValue),
    /// A scalar subquery `(SELECT ...)`. Rewritten to a [`Literal`] (its single
    /// value; 512 if it returns more than one row) before evaluation.
    Subquery(Box<Select>),
    /// `EXISTS (SELECT ...)`. Rewritten to a boolean before evaluation.
    Exists(Box<Select>),
    /// `expr [NOT] IN (SELECT ...)`. Rewritten to an [`InList`] of the
    /// subquery's values before evaluation.
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Select>,
        negated: bool,
    },
}

/// The five standard aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}
