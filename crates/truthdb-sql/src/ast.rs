//! Abstract syntax tree for the Stage 3 grammar. Nodes carry [`Span`]s so
//! binding/semantic errors can point at the offending source text.

use crate::lexer::Span;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateView(CreateView),
    DropView(DropView),
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
    /// `ROLLBACK [TRAN[SACTION]] [name]`. A `name` rolls back to that savepoint
    /// (the transaction stays open); no name rolls back the whole transaction.
    Rollback {
        name: Option<Name>,
        span: Span,
    },
    /// `SAVE TRAN[SACTION] name` — a named savepoint within a transaction.
    SaveTransaction {
        name: Name,
        span: Span,
    },
    /// `SET` session option (XACT_ABORT / TRANSACTION ISOLATION LEVEL) or a
    /// `SET @v = expr` variable assignment.
    Set(SetStatement),
    /// `ALTER TABLE ...`.
    AlterTable(AlterTable),
    /// `DECLARE @a TYPE [= expr], ...` — batch variable declarations.
    Declare(Vec<Declaration>),
    /// `EXEC[UTE] <proc> [args...]` — the T-SQL text path to the system
    /// procedures (`sp_executesql` is the supported one).
    Exec(ExecStatement),
    /// `BEGIN TRY <try_block> END TRY BEGIN CATCH <catch_block> END CATCH`. An
    /// error in the try block transfers control to the catch block.
    TryCatch {
        try_block: Vec<Statement>,
        catch_block: Vec<Statement>,
        span: Span,
    },
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

/// `EXEC[UTE] <proc> [@name =] <expr> [, ...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecStatement {
    pub proc: Name,
    pub args: Vec<ExecArg>,
    pub span: Span,
}

/// One argument of an `EXEC`: optionally named (`@p = expr`).
#[derive(Debug, Clone, PartialEq)]
pub struct ExecArg {
    pub name: Option<Name>,
    pub value: Expr,
}

/// `ALTER TABLE <table> <action>`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub table: Name,
    pub action: AlterAction,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    /// `ADD <column> <type> [NULL|NOT NULL] [DEFAULT expr] [COLLATE name]`.
    AddColumn(ColumnDef),
    /// `ADD [CONSTRAINT name] CHECK (expr)`.
    AddCheck(CheckConstraint),
    /// `ADD [CONSTRAINT name] FOREIGN KEY (...) REFERENCES ...`.
    AddForeignKey(ForeignKey),
    /// `DROP CONSTRAINT <name>`.
    DropConstraint(Name),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetStatement {
    XactAbort(bool),
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

/// `CREATE [UNIQUE] INDEX <name> ON <table> (<col> [ASC|DESC], ...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: Name,
    pub table: Name,
    pub unique: bool,
    pub columns: Vec<IndexColumn>,
    /// `INCLUDE (col, ...)`: non-key columns whose values are stored in the
    /// index leaves so a query over them is answered from the index alone.
    pub include: Vec<Name>,
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
    /// Table-level `[CONSTRAINT name] CHECK (expr)` constraints.
    pub check_constraints: Vec<CheckConstraint>,
    /// Table-level `[CONSTRAINT name] FOREIGN KEY (...) REFERENCES ...`.
    pub foreign_keys: Vec<ForeignKey>,
    /// `[CONSTRAINT name] UNIQUE (...)` constraints (table-level, or desugared
    /// from an inline column `UNIQUE`). Each becomes a unique index.
    pub unique_constraints: Vec<UniqueConstraint>,
    pub span: Span,
}

/// A `[CONSTRAINT name] UNIQUE (cols)` constraint. A column-level `col ... UNIQUE`
/// desugars to a single-column one.
#[derive(Debug, Clone, PartialEq)]
pub struct UniqueConstraint {
    pub name: Option<Name>,
    pub columns: Vec<Name>,
    pub span: Span,
}

/// A `[CONSTRAINT name] CHECK (predicate)` constraint (table- or column-level).
/// The predicate is kept as source text (re-parsed at bind/enforcement time,
/// like a column `DEFAULT`) so the catalog need not serialize an AST.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckConstraint {
    pub name: Option<Name>,
    pub predicate: String,
    pub span: Span,
}

/// A `[CONSTRAINT name] FOREIGN KEY (cols) REFERENCES parent [(pcols)]`
/// constraint. A column-level `col ... REFERENCES parent [(pcol)]` desugars to
/// a single-column foreign key. `parent_columns` empty means "the parent's
/// primary key".
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKey {
    pub name: Option<Name>,
    pub columns: Vec<Name>,
    pub parent: Name,
    pub parent_columns: Vec<Name>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: Name,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub primary_key: bool,
    /// Column-level `UNIQUE` — desugars to a single-column unique constraint.
    pub unique: bool,
    /// `DEFAULT <expr>` source text — re-parsed and evaluated at INSERT so a
    /// non-constant default (e.g. a niladic function) is applied per row.
    pub default: Option<String>,
    /// `IDENTITY(seed, increment)` — server-generated values.
    pub identity: Option<Identity>,
    /// `COLLATE <name>` on a character column.
    pub collation: Option<String>,
    /// Column-level `[CONSTRAINT name] CHECK (expr)` constraints.
    pub checks: Vec<CheckConstraint>,
    /// Column-level `[CONSTRAINT name] REFERENCES parent [(pcol)]` foreign keys.
    pub foreign_keys: Vec<ForeignKey>,
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

/// `CREATE VIEW name AS SELECT ...`. Only the source text of the query is kept;
/// it is re-parsed and inlined wherever the view is referenced.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateView {
    pub name: Name,
    pub query_text: String,
    pub span: Span,
}

/// `DROP VIEW [IF EXISTS] name`.
#[derive(Debug, Clone, PartialEq)]
pub struct DropView {
    pub name: Name,
    pub if_exists: bool,
    pub span: Span,
}

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
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
