use crate::lexer::Span;

use super::Name;

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

/// `ALTER DATABASE {name | CURRENT} SET <option> {ON|OFF} [, ...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabase {
    /// `None` = `CURRENT`.
    pub name: Option<Name>,
    pub options: Vec<(DatabaseOption, bool)>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseOption {
    ReadCommittedSnapshot,
    AllowSnapshotIsolation,
    /// `SET RECOVERY {FULL | SIMPLE}`. Unlike the ON/OFF options, the paired
    /// bool carries the mode: `true` = FULL, `false` = SIMPLE.
    Recovery,
    /// `ALTER DATABASE <name> FAILOVER` — promotion of a replication standby.
    /// Online it always errors pointing at the offline `truthdb-cli promote`
    /// (the paired bool is unused).
    Failover,
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
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    Time,
    DateTime2,
    UniqueIdentifier,
    VarChar(u32),
    NVarChar(u32),
    VarBinary(u32),
    /// `VARCHAR(MAX)` — no declared length cap (Stage 14).
    VarCharMax,
    NVarCharMax,
    VarBinaryMax,
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
