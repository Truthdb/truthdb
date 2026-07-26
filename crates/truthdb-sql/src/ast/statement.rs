use crate::lexer::Span;

use super::{
    AlterDatabase, AlterTable, ColumnDef, CreateFunction, CreateIndex, CreateLogin,
    CreateProcedure, CreateTable, CreateTrigger, CreateUser, CreateView, Declaration, Delete,
    DropIndex, DropTable, DropView, ExecStatement, Expr, Insert, Name, PermissionStatement,
    RaiseError, RoleMemberAction, Select, SetStatement, ThrowStatement, Update,
};

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
    /// `ALTER DATABASE {name | CURRENT} SET <option> {ON|OFF} [, ...]`.
    AlterDatabase(AlterDatabase),
    /// `DECLARE @a TYPE [= expr], ...` — batch variable declarations.
    Declare(Vec<Declaration>),
    /// `DECLARE @t TABLE ( <column-defs> )` — an in-memory table variable. It is
    /// a standalone declaration (SQL Server forbids mixing it with others).
    DeclareTableVar {
        /// The variable name, without the leading `@`, lowercased.
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<Name>,
        span: Span,
    },
    /// `EXEC[UTE] <proc> [args...]` — the T-SQL text path to the system
    /// procedures (`sp_executesql` is the supported one).
    Exec(ExecStatement),
    /// `USE <database>` — a database context switch. TruthDB is a single-
    /// database instance, so the only accepted target is the current
    /// database; the point is the ENVCHANGE clients (SSMS) expect back.
    Use {
        database: Name,
        span: Span,
    },
    /// `BEGIN TRY <try_block> END TRY BEGIN CATCH <catch_block> END CATCH`. An
    /// error in the try block transfers control to the catch block.
    TryCatch {
        try_block: Vec<Statement>,
        catch_block: Vec<Statement>,
        span: Span,
    },
    /// `THROW [number, message, state]` — raises a severity-16 error that
    /// terminates the batch; the bare form re-throws inside a `CATCH`.
    Throw(ThrowStatement),
    /// `RAISERROR(msg, severity, state [, args...]) [WITH LOG|NOWAIT|SETERROR]`.
    RaiseError(RaiseError),
    /// `BEGIN <statements> END` — a plain statement block (not TRY, not TRAN).
    Block {
        body: Vec<Statement>,
        span: Span,
    },
    /// `IF <condition> <statement> [ELSE <statement>]`. T-SQL three-valued:
    /// only TRUE runs the THEN branch; FALSE and NULL take the ELSE.
    If {
        condition: Expr,
        then_branch: Box<Statement>,
        else_branch: Option<Box<Statement>>,
        span: Span,
    },
    /// `WHILE <condition> <statement>`.
    While {
        condition: Expr,
        body: Box<Statement>,
        span: Span,
    },
    /// `BREAK` — terminates the innermost enclosing WHILE. The parser rejects
    /// it outside one (SQL Server's compile-time 135).
    Break {
        span: Span,
    },
    /// `CONTINUE` — restarts the innermost enclosing WHILE.
    Continue {
        span: Span,
    },
    /// `RETURN [expr]` — exits the batch (and, later, the procedure).
    Return {
        value: Option<Expr>,
        span: Span,
    },
    /// `GOTO <label>` — an unconditional jump to a label in the same statement
    /// list or an enclosing one.
    Goto {
        label: String,
        span: Span,
    },
    /// `<label>:` — a `GOTO` target. Executed in sequence it is a no-op.
    Label {
        name: String,
        span: Span,
    },
    /// `CREATE PROCEDURE` / `ALTER PROCEDURE` — the body is stored as source
    /// text (the view posture) and re-parsed at EXEC.
    CreateProcedure(CreateProcedure),
    /// `DROP PROCEDURE [IF EXISTS] <name>`.
    DropProcedure {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `CREATE FUNCTION` / `ALTER FUNCTION` — the body is stored as source text
    /// (the view posture) and re-parsed at each call.
    CreateFunction(CreateFunction),
    /// `DROP FUNCTION [IF EXISTS] <name>`.
    DropFunction {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `CREATE|ALTER TRIGGER <name> ON <table> AFTER {INSERT|UPDATE|DELETE}[,...]
    /// AS <body>` — an AFTER DML trigger. The body is stored as source text and
    /// re-parsed per firing (the procedure posture).
    CreateTrigger(CreateTrigger),
    /// `DROP TRIGGER [IF EXISTS] <name>`.
    DropTrigger {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `{ENABLE | DISABLE} TRIGGER {<name> | ALL} ON <table>` — flips a trigger's
    /// disabled flag (a disabled trigger does not fire). `trigger` is `None` for
    /// `ALL` (every trigger on the table).
    SetTriggerState {
        trigger: Option<Name>,
        table: Name,
        enable: bool,
        span: Span,
    },
    /// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` or `ALTER LOGIN <name>
    /// {ENABLE | DISABLE}` — a SQL-authentication server login.
    CreateLogin(CreateLogin),
    /// `DROP LOGIN [IF EXISTS] <name>`.
    DropLogin {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `CREATE DATABASE <name>` — a naming namespace over the shared log and
    /// data file (level 1 of the multiple-databases plan).
    CreateDatabase {
        name: Name,
        span: Span,
    },
    /// `DROP DATABASE [IF EXISTS] <name>` — drops the database and every
    /// object in it.
    DropDatabase {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `CREATE USER <name> [FOR LOGIN <login>]`.
    CreateUser(CreateUser),
    /// `DROP USER [IF EXISTS] <name>`.
    DropUser {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `CREATE ROLE <name>`.
    CreateRole {
        name: Name,
        span: Span,
    },
    /// `DROP ROLE [IF EXISTS] <name>`.
    DropRole {
        name: Name,
        if_exists: bool,
        span: Span,
    },
    /// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
    AlterRole {
        name: Name,
        action: RoleMemberAction,
        member: Name,
        span: Span,
    },
    /// `GRANT|DENY|REVOKE <actions> ON <object> TO|FROM <grantees>`.
    Permission(PermissionStatement),
    /// `BACKUP DATABASE <name> TO DISK = '<path>' [WITH <opt>[, ...]]` — an
    /// online full backup to a `TDBBAK1` file.
    BackupDatabase {
        database: Name,
        path: String,
        checksum: bool,
        copy_only: bool,
        span: Span,
    },
    /// `BACKUP LOG <name> TO DISK = '<path>' [WITH <opt>[, ...]]` — a
    /// transaction-log backup (FULL recovery model only).
    BackupLog {
        database: Name,
        path: String,
        checksum: bool,
        copy_only: bool,
        span: Span,
    },
    /// `RESTORE {VERIFYONLY|HEADERONLY|FILELISTONLY|DATABASE <name>|LOG <name>}
    /// FROM DISK = '<path>'` — the online, read-only restore verbs. Actual
    /// `RESTORE DATABASE`/`LOG` is offline (the CLI); online it errors.
    Restore {
        mode: RestoreMode,
        path: String,
        span: Span,
    },
    /// `DECLARE <name> [SCROLL] CURSOR FOR <select>` — a static cursor over a
    /// query's result (materialized when OPENed).
    DeclareCursor {
        name: Name,
        select: Box<Select>,
        scroll: bool,
        span: Span,
    },
    /// `OPEN <name>` — executes the cursor's query and positions before the first
    /// row.
    OpenCursor {
        name: Name,
        span: Span,
    },
    /// `FETCH [<direction>] [FROM] <name> [INTO @v[, ...]]`.
    FetchCursor {
        name: Name,
        direction: FetchDirection,
        into: Vec<String>,
        span: Span,
    },
    /// `CLOSE <name>` — releases the result set (the cursor can be re-OPENed).
    CloseCursor {
        name: Name,
        span: Span,
    },
    /// `DEALLOCATE <name>` — removes the cursor.
    DeallocateCursor {
        name: Name,
        span: Span,
    },
}

/// A `FETCH` direction. ABSOLUTE/RELATIVE carry a row-count expression.
#[derive(Debug, Clone, PartialEq)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(Expr),
    Relative(Expr),
}

/// The `RESTORE` verb.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreMode {
    /// `RESTORE VERIFYONLY` — validate the whole backup file.
    VerifyOnly,
    /// `RESTORE HEADERONLY` — one row of backup metadata.
    HeaderOnly,
    /// `RESTORE FILELISTONLY` — one row per file in the backup.
    FileListOnly,
    /// `RESTORE DATABASE` — offline only (errors online).
    Database,
    /// `RESTORE LOG` — offline only (errors online).
    Log,
}
