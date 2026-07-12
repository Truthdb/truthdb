//! Abstract syntax tree for the Stage 3 grammar. Nodes carry [`Span`]s so
//! binding/semantic errors can point at the offending source text.

use crate::lexer::Span;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Select(Select),
    /// `BEGIN TRAN[SACTION] [name]`.
    BeginTransaction {
        name: Option<Name>,
        span: Span,
    },
    /// `COMMIT [TRAN[SACTION]] [name]`.
    Commit {
        span: Span,
    },
    /// `ROLLBACK [TRAN[SACTION]] [name]`.
    Rollback {
        span: Span,
    },
    /// `SET` session option (XACT_ABORT / TRANSACTION ISOLATION LEVEL).
    Set(SetStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetStatement {
    XactAbort(bool),
    IsolationLevel(IsolationLevel),
    /// `SET SHOWPLAN_TEXT ON|OFF` — when on, statements return their plan text
    /// instead of executing.
    ShowplanText(bool),
}

/// `CREATE [UNIQUE] INDEX <name> ON <table> (<col> [ASC|DESC], ...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: Name,
    pub table: Name,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub name: Name,
    /// Ascending (`ASC`, the default) or descending (`DESC`).
    pub ascending: bool,
}

/// `DROP INDEX <name> ON <table>`.
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndex {
    pub name: Name,
    pub table: Name,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub table: Name,
    pub columns: Vec<ColumnDef>,
    /// Column names named in a table-level `PRIMARY KEY (...)`, or the single
    /// column that carried an inline `PRIMARY KEY`.
    pub primary_key: Vec<Name>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: Name,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub primary_key: bool,
    /// `DEFAULT <expr>` source text — re-parsed and evaluated at INSERT so a
    /// non-constant default (e.g. a niladic function) is applied per row.
    pub default: Option<String>,
    /// `IDENTITY(seed, increment)` — server-generated values.
    pub identity: Option<Identity>,
    /// `COLLATE <name>` on a character column.
    pub collation: Option<String>,
    pub span: Span,
}

/// `IDENTITY(seed, increment)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identity {
    pub seed: i64,
    pub increment: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Bit,
    Real,
    Float,
    Decimal { precision: u8, scale: u8 },
    Date,
    Time,
    DateTime2,
    UniqueIdentifier,
    VarChar(u32),
    NVarChar(u32),
    VarBinary(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTable {
    pub table: Name,
    pub if_exists: bool,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: Name,
    /// Explicit column list, or None for "all columns in table order".
    pub columns: Option<Vec<Name>>,
    pub rows: Vec<Vec<Expr>>,
    pub span: Span,
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
    pub top: Option<u64>,
    /// `SELECT DISTINCT` — deduplicate the projected rows.
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub from: Option<Name>,
    pub where_clause: Option<Expr>,
    /// `GROUP BY <expr>, ...` (empty = no grouping).
    pub group_by: Vec<Expr>,
    /// `HAVING <predicate>` — filters groups after aggregation.
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*`
    Wildcard,
    Expr {
        expr: Expr,
        alias: Option<Name>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub descending: bool,
}

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
