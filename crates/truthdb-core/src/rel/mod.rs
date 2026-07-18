//! SQL execution over the relational storage engine.
//!
//! Parses a batch with [`truthdb_sql`], then binds and runs each statement
//! against [`Storage`]'s `rel_*` API. SELECT uses a simple Volcano-style
//! pipeline materialized in memory: source scan -> WHERE filter -> ORDER BY
//! sort -> TOP limit -> projection. `sys.tables`/`sys.columns` are virtual
//! sources built from the catalog. Storage errors are mapped to SQL Server
//! error numbers.

mod aggregate;
pub mod collation;
mod hash;
mod plan;
mod value;

use truthdb_sql::ast::{
    AlterAction, AlterDatabase, AlterTable, CheckConstraint, ColumnDef, CreateFunction,
    CreateIndex, CreateLogin, CreateProcedure, CreateTable, CreateTrigger, CreateUser, CreateView,
    DataType, DatabaseOption, Declaration, Delete, DropIndex, DropTable, DropView, ExecStatement,
    Expr, ExprKind, ForeignKey, Insert, InsertSource, IsolationLevel, JoinKind, Name, OrderItem,
    PermissionAction, PermissionKind, PermissionStatement, RaiseError, ReturnsClause,
    RoleMemberAction, Select, SelectItem, SetStatement, Statement, TableRef, ThrowArgs,
    ThrowStatement, Update,
};
use truthdb_sql::collation::CollationSensitivity;
use truthdb_sql::error::SqlError;
use truthdb_sql::eval::{ColumnResolver, EvalContext, SecurityContext};
use truthdb_sql::lexer::Span;
use truthdb_sql::value::{SqlValue, order_key_cmp};
use truthdb_sql::{ast, eval};

use xxhash_rust::xxh64::xxh64;

use crate::lock::{LockMode, Resource};
use crate::relstore::btree::ScanCursor;
use crate::relstore::catalog::{
    self, FunctionDef, FunctionReturns, PermAction, PermissionEntry, PrincipalDef, ProcParamDef,
    ProcedureDef, TableDef, TriggerDef,
};
use crate::relstore::row::{Column, Schema};
use crate::relstore::types::{ColumnType, Datum};
use crate::relstore::version::ReadSnapshot;
use crate::storage::{RowLocator, Storage, StorageError, StorageTxn, TxnScope};

/// A declared table variable's in-memory contents: its column schema, the key
/// columns of its declared PRIMARY KEY (for uniqueness enforcement), the
/// per-column `DEFAULT` source text (parallel to the schema columns, re-parsed
/// and evaluated per INSERT), and its rows. A row is a `Vec<Datum>` in schema
/// order, exactly like a base-table row.
#[derive(Clone)]
struct TableVar {
    schema: Schema,
    key_columns: Vec<usize>,
    defaults: Vec<Option<String>>,
    rows: Vec<Vec<Datum>>,
}

/// Per-session transaction state carried across statements/batches. Lives in
/// the session (engine thread); autocommit statements use `Default`.
#[derive(Default)]
pub struct TxnContext {
    txn: Option<StorageTxn>,
    /// `@@TRANCOUNT` — nested BEGINs increment; only the outermost COMMIT
    /// actually commits.
    trancount: u32,
    /// Set when a statement failed inside the transaction (SQL Server
    /// XACT_ABORT-style): only ROLLBACK is then allowed.
    doomed: bool,
    xact_abort: bool,
    isolation: Isolation,
    /// `SET NOCOUNT ON` — statements report no row count to the client:
    /// TDS DONEs drop `DONE_COUNT`, and the native protocol's count envelope
    /// becomes a bare done (the CLI then prints no "(n rows affected)" line,
    /// exactly as sqlcmd goes quiet against SQL Server). Result rows and
    /// `@@ROWCOUNT` are untouched.
    nocount: bool,
    /// Rows affected/returned by the previous statement — `@@ROWCOUNT`.
    rowcount: i64,
    /// The previous statement's error number, 0 on success — `@@ERROR`.
    last_error: i32,
    /// Names of the stored procedures currently executing (innermost last),
    /// for `ERROR_PROCEDURE()`; empty in an ad-hoc batch.
    proc_stack: Vec<String>,
    /// The procedure executing when the LAST error was raised — captured at
    /// the raise site, because by CATCH entry the procedure's frame has
    /// already unwound off `proc_stack`.
    error_procedure: Option<String>,
    /// A procedure body's `RETURN [value]` status, stashed by the Return arm
    /// for `EXEC @rc = name` to read after the body unwinds; 0 when the body
    /// falls off the end.
    proc_return: Option<i64>,
    /// A scalar function body's `RETURN <expr>` value, coerced to the declared
    /// return type and stashed by the Return arm for the caller to read. Only
    /// set while a function body runs (see [`run_user_scalar_function`]).
    func_return: Option<SqlValue>,
    /// `SET SHOWPLAN_TEXT ON` — a SELECT returns its plan text, not results.
    showplan_text: bool,
    /// Declared batch variables (name without `@`, lowercased) to their type
    /// and current value. Cleared at the start of each batch.
    variables: std::collections::HashMap<String, (ColumnType, SqlValue)>,
    /// Declared table variables (name without `@`, lowercased): in-memory
    /// rowsets that live only on the session (never on Storage), so they survive
    /// ROLLBACK and are cleared at batch end — SQL Server table-variable
    /// semantics. Kept disjoint from `variables`; a name lives in exactly one.
    table_variables: std::collections::HashMap<String, TableVar>,
    /// Connection identity for session intrinsics (`DB_NAME()`,
    /// `SUSER_SNAME()`, `@@SPID`), set once when the session opens.
    database: String,
    login: String,
    spid: i32,
    /// The session's database user name (`USER_NAME()`), resolved from the login
    /// when the session opens (`dbo` for a sysadmin, else the mapped user, else
    /// the login name). Distinct from `login` (the server principal).
    user: String,
    /// The login's server principal_id and the user's database principal_id —
    /// the keys the membership intrinsics resolve against. Set at session open.
    login_sid: u32,
    user_sid: u32,
    /// The session's effective SERVER-role names (lowercased) — the transitive
    /// closure of the login's roles (today: `sysadmin`). Read by
    /// `IS_SRVROLEMEMBER`. Kept separate from database roles so the two
    /// namespaces do not cross-answer.
    session_server_roles: std::collections::HashSet<String>,
    /// The session's effective DATABASE-role names (lowercased) — the transitive
    /// closure of the database user's roles. Read by `IS_ROLEMEMBER`. Both sets
    /// are refreshed at batch start from the membership cache, so a security DDL
    /// is seen by the next batch (SQL Server's per-batch permission caching).
    session_db_roles: std::collections::HashSet<String>,
    /// Object-permission enforcement subject (bypass flag + the grant-matching
    /// principal id set), refreshed at batch start alongside the role sets.
    security: truthdb_sql::eval::SecurityContext,
    /// The last identity value inserted in this session (SQL Server scope),
    /// surfaced as `SCOPE_IDENTITY()`. Persists across statements until the next
    /// identity INSERT; unaffected by non-identity inserts.
    scope_identity: Option<i64>,
    /// Named savepoints in the current transaction (`SAVE TRANSACTION <name>`,
    /// lowercased) → the point to which `ROLLBACK TRANSACTION <name>` returns.
    /// Cleared when the transaction ends.
    savepoints: std::collections::HashMap<String, crate::relstore::ctx::Savepoint>,
    /// Errors caught by the currently-executing `CATCH` blocks (a stack, so
    /// nested `TRY`/`CATCH` restore the outer error on exit). `ERROR_*()` read
    /// the top; empty outside any `CATCH` block.
    error_stack: Vec<truthdb_sql::eval::ErrorInfo>,
    /// The SNAPSHOT-isolation transaction's read view, captured (and
    /// registered against pruning) at its first data access and released
    /// when the transaction ends — commit, rollback, reap, or disconnect.
    txn_snapshot: Option<ReadSnapshot>,
    /// Set when the idle reaper rolled this session's transaction back. The
    /// session's next batch fails with 1205 and clears it, so a client that
    /// comes back believing it is still in a transaction is told the
    /// transaction is gone — rather than silently autocommitting statements it
    /// means to be transactional, and only discovering it at a COMMIT that
    /// errors 3902 long after the writes became durable.
    reaped: bool,
}

/// Session isolation level (defaults to READ COMMITTED, like SQL Server).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Isolation {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
    /// Transaction-scoped versioned reads (Stage 13): one snapshot at the
    /// transaction's first data access, reused by every statement in it.
    Snapshot,
}

impl TxnContext {
    fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    fn eval_context(&self) -> EvalContext {
        EvalContext {
            trancount: self.trancount as i32,
            variables: self
                .variables
                .iter()
                .map(|(name, (_, value))| (name.clone(), value.clone()))
                .collect(),
            database: self.database.clone(),
            login: self.login.clone(),
            user: self.user.clone(),
            server_roles: self.session_server_roles.clone(),
            db_roles: self.session_db_roles.clone(),
            security: self.security.clone(),
            spid: self.spid,
            rowcount: self.rowcount,
            scope_identity: self.scope_identity,
            error: self.error_stack.last().cloned(),
            xact_state: self.xact_state(),
            last_error: self.last_error,
            nestlevel: EXEC_DEPTH.with(|d| d.get()) as i32,
        }
    }

    /// `XACT_STATE()`: 0 with no open transaction, -1 when the open transaction
    /// is doomed (uncommittable), else 1.
    fn xact_state(&self) -> i8 {
        if !self.in_txn() {
            0
        } else if self.doomed {
            -1
        } else {
            1
        }
    }

    /// Enters a `CATCH` block: records the caught error so `ERROR_*()` resolve
    /// to it (pushed, so nested `TRY`/`CATCH` restore the outer error on exit).
    /// Records the error context every failed statement leaves behind:
    /// `@@ERROR` and the raising procedure (for `ERROR_PROCEDURE()`). Called
    /// at the RAISE site — the only place the procedure frame is still live.
    fn record_error(&mut self, number: i32) {
        self.last_error = number;
        self.error_procedure = self.proc_stack.last().cloned();
    }

    fn push_error(&mut self, error: &SqlError) {
        self.error_stack.push(truthdb_sql::eval::ErrorInfo {
            number: error.number,
            message: error.message.clone(),
            severity: error.level,
            state: error.state,
            procedure: self.error_procedure.clone(),
        });
    }

    /// Leaves a `CATCH` block, restoring the enclosing error context (if any).
    fn pop_error(&mut self) {
        self.error_stack.pop();
    }

    /// Records the connection identity used by session intrinsics. Called once
    /// when the session opens. `session_roles` is filled separately, per batch,
    /// from the membership cache (see [`Self::refresh_session_roles`]).
    pub fn set_session_identity(
        &mut self,
        database: String,
        login: String,
        spid: i32,
        user: String,
        login_sid: u32,
        user_sid: u32,
    ) {
        self.database = database;
        self.login = login;
        self.spid = spid;
        self.user = user;
        self.login_sid = login_sid;
        self.user_sid = user_sid;
    }

    /// Refreshes the session's effective role NAMES from the membership cache.
    /// The login's roles are SERVER roles (`sysadmin`); the database user's roles
    /// are DATABASE roles — kept in separate sets so `IS_SRVROLEMEMBER` and
    /// `IS_ROLEMEMBER` never answer for the other's namespace. Called at batch
    /// start; a security DDL is therefore visible to the next batch.
    pub fn refresh_session_roles(&mut self, storage: &Storage) {
        let server_role_ids = storage.effective_roles(self.login_sid);
        let db_role_ids = storage.effective_roles(self.user_sid);
        let names = |ids: &std::collections::HashSet<u32>| -> std::collections::HashSet<String> {
            ids.iter()
                .filter_map(|&id| storage.principal_name(id))
                .map(|name| name.to_ascii_lowercase())
                .collect()
        };
        self.session_server_roles = names(&server_role_ids);
        self.session_db_roles = names(&db_role_ids);

        // The object-permission subject. A trusted/internal connection
        // (login_sid 0 — the native protocol and in-process tests), a sysadmin,
        // or dbo/db_owner bypasses every object-permission check (owns or
        // controls the database). Otherwise a GRANT/DENY matches the database
        // user, its effective roles, or `public`.
        use crate::storage::{DB_OWNER_ID, DBO_ID, PUBLIC_ID, SYSADMIN_ID};
        let bypass = self.login_sid == 0
            || server_role_ids.contains(&SYSADMIN_ID)
            || self.user_sid == DBO_ID
            || db_role_ids.contains(&DB_OWNER_ID);
        let mut principals = db_role_ids;
        principals.insert(self.user_sid);
        principals.insert(PUBLIC_ID);
        self.security = truthdb_sql::eval::SecurityContext { bypass, principals };
    }

    /// Clears batch-scoped variables (called at the start of each batch).
    pub fn clear_variables(&mut self) {
        self.variables.clear();
        self.table_variables.clear();
    }

    /// The final value and type of a batch variable, as a `Datum`, for the
    /// RPC-by-name response tail: after the synthesized `EXEC` batch completes
    /// the session reads the OUTPUT parameters (copied back into caller-scope
    /// variables) and the seeded return-status variable back off the context.
    /// `name` may carry a leading `@`; lookup is case-insensitive, matching how
    /// variables are keyed.
    pub fn variable_datum(&self, name: &str) -> Option<(ColumnType, Datum)> {
        let key = name.trim_start_matches('@').to_ascii_lowercase();
        let (column_type, value) = self.variables.get(&key)?;
        let datum = value::sql_to_datum(value, column_type, &key).ok()?;
        Some((*column_type, datum))
    }

    /// True if a transaction is open (used by the session to decide whether a
    /// disconnect must roll back).
    pub fn has_open_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// Whether an explicit transaction is open (`@@TRANCOUNT > 0`) — what a
    /// reply's DONE stamps as `DONE_INXACT`.
    pub fn in_transaction(&self) -> bool {
        self.trancount > 0
    }

    /// The session's current isolation level (drives which locks reads take).
    pub fn isolation(&self) -> Isolation {
        self.isolation
    }

    /// Rolls back and discards any open transaction, resetting every piece of
    /// transaction-scoped state.
    ///
    /// `savepoints` must be cleared here, not merely on the paths that discard
    /// the context afterwards: a savepoint holds the *undo-log offset* of the
    /// transaction that recorded it, so one surviving into the session's next
    /// transaction would let `ROLLBACK TRANSACTION <name>` find a stale entry
    /// instead of erroring 3908 — and hand a dead transaction's offset to
    /// `rel_rollback_to`, which either truncates the new transaction's undo log
    /// (silently discarding committed work) or panics on `split_off`.
    pub fn abort(&mut self, storage: &Storage) {
        if let Some(txn) = self.txn.take() {
            let _ = storage.rel_rollback(txn);
        }
        self.release_txn_snapshot(storage);
        self.trancount = 0;
        self.doomed = false;
        self.savepoints.clear();
    }

    /// Releases the SNAPSHOT transaction's registered read view, if any —
    /// called on every transaction-ending path (a leaked registration pins
    /// the version store's prune watermark forever).
    fn release_txn_snapshot(&mut self, storage: &Storage) {
        if let Some(snap) = self.txn_snapshot.take() {
            storage.release_read_snapshot(snap.seq);
        }
    }

    /// Rolls back this session's transaction because it sat idle too long, and
    /// records that it happened so the session's next batch can say so.
    pub fn abort_idle(&mut self, storage: &Storage) {
        self.abort(storage);
        self.reaped = true;
    }

    /// Takes the "your transaction was reaped" flag, if set.
    fn take_reaped(&mut self) -> bool {
        std::mem::take(&mut self.reaped)
    }
}

/// Result of one executed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementResult {
    Rows(RowSet),
    RowsAffected(u64),
    /// DDL and other statements with no rowset and no count.
    Done,
}

/// A result column: its name and resolved SQL type (drives TDS
/// COLMETADATA and display rendering alike).
#[derive(Debug, Clone, PartialEq)]
pub struct ResultColumn {
    pub name: String,
    pub column_type: ColumnType,
}

/// A typed result set: column metadata plus rows of typed [`Datum`]s.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSet {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Datum>>,
}

/// Error severity at or above which a statement failure dooms the whole
/// transaction even under `SET XACT_ABORT OFF` (SQL Server treats severity ≥ 17
/// as resource/batch-level, versus 11–16 statement-level). Constraint violations
/// (2627/2601/515/547, severity 14–16) stay below it, so they roll back only the
/// failing statement and the transaction survives.
const XACT_ABORT_SEVERITY: u8 = 17;

/// Error severity at or above which the error is fatal to the CONNECTION
/// (SQL Server severity >= 20): it bypasses every `TRY`, dooms the
/// transaction, and the protocol layers close the stream after delivering
/// it. Only RAISERROR ... WITH LOG can currently produce one.
pub const FATAL_SEVERITY: u8 = 20;

/// A batch's outcome: the results of the statements that ran, plus the error
/// that stopped the batch (if any). Statements before an error have already
/// committed (each statement is autocommit in Stage 3), so their results are
/// preserved rather than discarded.
pub struct BatchOutcome {
    pub results: Vec<StatementResult>,
    pub error: Option<SqlError>,
}

/// One `sp_executesql` parameter: its `@name` (as it appears in the RPC
/// stream), declared type, and decoded value. Passed by the TDS layer to
/// [`execute_batch_with_params`], which seeds them as batch variables the
/// statement text can read by name.
#[derive(Debug, Clone)]
pub struct RpcParam {
    pub name: String,
    pub column_type: ColumnType,
    pub value: Datum,
}

/// Parses and executes a SQL batch. A parse error yields an empty batch with
/// the error; a runtime error stops the batch but keeps earlier results.
pub fn execute_batch(storage: &Storage, sql: &str, txn_ctx: &mut TxnContext) -> BatchOutcome {
    execute_batch_with_params(storage, sql, txn_ctx, &[])
}

/// Like [`execute_batch`], but seeds `params` as batch variables before running
/// the statement text — the `sp_executesql` path. Parameters are injected as
/// already-typed values, never re-rendered into the SQL text, so a parameter
/// value can never alter the statement's structure (no injection surface).
pub fn execute_batch_with_params(
    storage: &Storage,
    sql: &str,
    txn_ctx: &mut TxnContext,
    params: &[RpcParam],
) -> BatchOutcome {
    let mut collector = Collector::default();
    let error = execute_batch_streamed(storage, sql, txn_ctx, params, &mut collector);
    collector.into_outcome(error)
}

/// Like [`execute_batch_with_params`], but each statement's result leaves
/// through `emitter` as the statement produces it instead of accumulating into
/// a [`BatchOutcome`]: a result set opens with its columns, its rows follow (in
/// chunks read straight off the scan for the streamed shape), and each
/// statement's DONE carries the transaction state *after that statement* —
/// which is what TDS `DONE_INXACT` means per statement. The returned error is
/// the batch's terminal error (what `BatchOutcome::error` carried), reported by
/// the caller after the statement events.
///
/// Durability keeps its ordering: nothing the client can rely on — a DONE
/// acknowledging a commit, or rows carrying commit-derived state such as a
/// reserved identity value — is emitted before the commit behind it is
/// fsync-durable. DONEs queue in the run and flush before the next result set
/// opens and at the end of the batch; both points fsync first when any
/// statement since the last one may have committed (the same kind-based test
/// as before), so a batch of writes with nothing to stream between them still
/// costs one fsync.
pub fn execute_batch_streamed(
    storage: &Storage,
    sql: &str,
    txn_ctx: &mut TxnContext,
    params: &[RpcParam],
    emitter: &mut dyn BatchEmitter,
) -> Option<SqlError> {
    // A transaction reaped for idleness is reported to the session's next batch
    // (once). 1205 is the code this engine already uses for a server-initiated
    // transaction abort (the parked deadlock victim), and every driver treats it
    // as "the transaction is gone, retry it" — which is exactly the right
    // recovery here.
    if txn_ctx.take_reaped() {
        return Some(SqlError::new(
            1205,
            13,
            51,
            "The transaction was rolled back because the session was idle for too long. Rerun the transaction.",
        ));
    }
    // Variables are batch-scoped: each batch starts with none.
    txn_ctx.clear_variables();
    // Refresh the session's effective role set from the membership cache, so a
    // security DDL committed since the last batch is reflected in this one's
    // IS_ROLEMEMBER/IS_SRVROLEMEMBER (SQL Server's per-batch permission caching).
    txn_ctx.refresh_session_roles(storage);
    for param in params {
        // The lexer keys `@p1` as `p1` (leading `@` stripped, lowercased); the
        // RPC name arrives as `@p1`, so normalise it the same way to match.
        let key = param.name.trim_start_matches('@').to_ascii_lowercase();
        let value = value::datum_to_sql(&param.value, &param.column_type);
        txn_ctx.variables.insert(key, (param.column_type, value));
    }
    let statements = match truthdb_sql::parse(sql) {
        Ok(statements) => statements,
        Err(error) => return Some(error),
    };
    let mut run = BatchRun {
        emitter,
        deferred: Vec::new(),
        rowset_open: false,
        durability_failed: false,
        committed: false,
        last_error: None,
        function_return_type: None,
    };
    // `run_block` returns Err only when the batch must terminate (a cancel, or a
    // dooming/uncaught error outside any TRY); a non-dooming error under
    // `XACT_ABORT OFF` is recorded in `run.last_error` and the batch continues.
    let terminating = run_block(storage, &statements, txn_ctx, &mut run, false).err();
    // The batch-end durability point, and the DONEs it was holding back. A
    // durability failure outranks any statement error: a lost commit is more
    // severe than an error the client asked about, and a benign continued error
    // must not mask one.
    let durability = run.finish(storage);
    durability.or(terminating).or(run.last_error)
}

/// Receives a batch's results as the executor produces them. The session layer
/// forwards each call as a `BatchEvent` onto the reply channel; buffered
/// callers (the native command path, the SLT runner, tests) use [`Collector`]
/// to reassemble a [`BatchOutcome`].
pub trait BatchEmitter {
    /// Opens a result set: its column metadata.
    fn columns(&mut self, columns: Vec<ResultColumn>);
    /// A chunk of rows for the open result set.
    fn rows(&mut self, rows: Vec<Vec<Datum>>);
    /// Ends one statement: its row count / rows-affected (`None` for DDL),
    /// the transaction state after it ran, and its command class (the DONE's
    /// `CurCmd` on the wire).
    fn statement_done(&mut self, count: Option<u64>, in_transaction: bool, command: DoneCommand);
    /// Ends a statement that failed after its result set had begun streaming:
    /// the set is closed so the stream stays framed for the statements that
    /// follow. The error itself is reported separately — at the end of the
    /// batch for a continued error, or not at all for one a `CATCH` handled.
    fn statement_aborted(&mut self, in_transaction: bool);

    /// The session's database context was (re-)established (`USE`): TDS
    /// renders the ENVCHANGE + 5701 INFO SSMS expects. Emitters that have no
    /// wire (the collecting native path, tests) ignore it.
    fn database_context(&mut self, _database: &str) {}

    /// An informational message (RAISERROR severity <= 10): TDS renders an
    /// INFO token in-stream, not an error. Emitters with no wire ignore it.
    fn info(&mut self, _error: &SqlError) {}
}

/// A [`BatchEmitter`] that drops everything: a scalar function body produces no
/// result sets (a data-returning SELECT is rejected at CREATE, 444), so its
/// per-statement DONE events have nowhere to go.
struct DiscardEmitter;

impl BatchEmitter for DiscardEmitter {
    fn columns(&mut self, _columns: Vec<ResultColumn>) {}
    fn rows(&mut self, _rows: Vec<Vec<Datum>>) {}
    fn statement_done(
        &mut self,
        _count: Option<u64>,
        _in_transaction: bool,
        _command: DoneCommand,
    ) {
    }
    fn statement_aborted(&mut self, _in_transaction: bool) {}
}

/// Reassembles emitted results into the whole-batch [`BatchOutcome`] for the
/// callers that want everything at once.
#[derive(Default)]
pub struct Collector {
    results: Vec<StatementResult>,
    /// The result set currently streaming, if a statement opened one.
    open: Option<RowSet>,
}

impl Collector {
    /// The collected outcome. A result set still open belongs to a statement
    /// that failed after its rows started streaming; a failed statement
    /// contributes no result, so it is dropped.
    pub fn into_outcome(self, error: Option<SqlError>) -> BatchOutcome {
        BatchOutcome {
            results: self.results,
            error,
        }
    }
}

impl BatchEmitter for Collector {
    fn columns(&mut self, columns: Vec<ResultColumn>) {
        self.open = Some(RowSet {
            columns,
            rows: Vec::new(),
        });
    }

    fn rows(&mut self, mut rows: Vec<Vec<Datum>>) {
        if let Some(rowset) = self.open.as_mut() {
            rowset.rows.append(&mut rows);
        }
    }

    fn statement_done(&mut self, count: Option<u64>, _in_transaction: bool, _command: DoneCommand) {
        self.results.push(match self.open.take() {
            Some(rowset) => StatementResult::Rows(rowset),
            None => match count {
                Some(n) => StatementResult::RowsAffected(n),
                None => StatementResult::Done,
            },
        });
    }

    fn statement_aborted(&mut self, _in_transaction: bool) {
        self.open = None;
    }
}

/// The mutable accumulator threaded through [`run_block`] across a batch and
/// its nested `TRY`/`CATCH` blocks: the emitter results leave through, the
/// DONEs held back for durability, and the batch's error state.
struct BatchRun<'a> {
    emitter: &'a mut dyn BatchEmitter,
    /// Finished statements' DONEs, held back until the next durability point
    /// (the next result set opening, or the end of the batch) so a DONE that
    /// acknowledges a commit is never emitted before that commit is durable.
    deferred: Vec<DeferredDone>,
    /// A result set's columns have been emitted but its statement has not
    /// finished — the state [`BatchRun::abort_open_rowset`] closes on failure.
    rowset_open: bool,
    /// A durability (fsync) failure wedged the store. The error terminates the
    /// batch and is never catchable: the old batch-end fsync ran past every
    /// TRY, and a CATCH must not be able to swallow a lost commit.
    durability_failed: bool,
    /// Whether any executed statement may have made a durable commit: group
    /// commit defers the WAL fsync, so the end of the batch fsyncs once if so.
    committed: bool,
    /// The last non-dooming statement error under `SET XACT_ABORT OFF` (outside
    /// any TRY) — the batch continues past it (SQL Server default) and it is
    /// reported alongside the results rather than terminating the batch.
    last_error: Option<SqlError>,
    /// Set while running a scalar function body: the declared return type. The
    /// `RETURN <expr>` arm then evaluates its value, coerces it to this type,
    /// and stashes it in `TxnContext::func_return` (rather than the procedure
    /// int-status path).
    function_return_type: Option<ColumnType>,
}

/// A statement's DONE, parked until the next durability point.
struct DeferredDone {
    count: Option<u64>,
    in_transaction: bool,
    command: DoneCommand,
}

/// The command class a statement's DONE reports in its `CurCmd` field.
/// mssql-jdbc discards a DONE's row count unless `CurCmd` names a DML
/// command, so `executeUpdate` returns -1 against a server that leaves it
/// zero (pytds and go-mssqldb ignore the field).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DoneCommand {
    Select,
    Insert,
    Update,
    Delete,
    /// DDL, SET, transaction control — anything whose count nobody reads.
    Other,
}

/// The `CurCmd` class of a statement.
fn done_command(statement: &Statement) -> DoneCommand {
    match statement {
        Statement::Select(_) => DoneCommand::Select,
        Statement::Insert(_) => DoneCommand::Insert,
        Statement::Update(_) => DoneCommand::Update,
        Statement::Delete(_) => DoneCommand::Delete,
        _ => DoneCommand::Other,
    }
}

impl BatchRun<'_> {
    /// Opens a result set. [`run_block`] flushed the deferred DONEs before any
    /// statement that can produce one, so statement order on the stream holds.
    fn open_rowset(&mut self, columns: Vec<ResultColumn>) {
        debug_assert!(
            self.deferred.is_empty(),
            "a result set opened over deferred DONEs"
        );
        self.rowset_open = true;
        self.emitter.columns(columns);
    }

    /// Emits a chunk of rows for the open result set.
    fn rows(&mut self, rows: Vec<Vec<Datum>>) {
        if !rows.is_empty() {
            self.emitter.rows(rows);
        }
    }

    /// Forwards a database-context change (`USE`) to the emitter, ahead of
    /// the statement's (deferred) DONE.
    fn database_context(&mut self, database: &str) {
        self.emitter.database_context(database);
    }

    /// Emits an informational message (RAISERROR severity <= 10). `run_block`
    /// flushed the deferred DONEs before the statement, so stream order holds.
    fn info(&mut self, error: SqlError) {
        debug_assert!(
            self.deferred.is_empty(),
            "an INFO message over deferred DONEs"
        );
        self.emitter.info(&error);
    }

    /// Ends a statement. Its DONE is deferred to the next durability point.
    fn done(&mut self, count: Option<u64>, in_transaction: bool, command: DoneCommand) {
        self.rowset_open = false;
        self.deferred.push(DeferredDone {
            count,
            in_transaction,
            command,
        });
    }

    /// Closes the open result set of a statement that failed after its columns
    /// (and possibly rows) were already emitted, so the stream stays framed for
    /// the statements that follow (a caught or continued error). No-op when
    /// nothing is open — a statement that failed before emitting anything
    /// leaves no trace, as before.
    fn abort_open_rowset(&mut self, in_transaction: bool) {
        if self.rowset_open {
            self.rowset_open = false;
            self.emitter.statement_aborted(in_transaction);
        }
    }

    /// Emits the deferred DONEs, fsyncing first when any statement since the
    /// last durability point may have committed. The gate is the same
    /// kind-based `committed` flag the batch-end fsync uses — not "does some
    /// DONE acknowledge a commit" — because commit-derived state escapes
    /// through the *rows* of whatever result set opens next, not only through
    /// DONEs: an identity value reserved by an in-transaction INSERT (a
    /// mini-commit) is readable one statement later via `SELECT
    /// SCOPE_IDENTITY()`, and a value the client has seen must never be
    /// reissued after a crash. On a durability failure the DONEs the batch
    /// can no longer stand behind are dropped and the batch terminates (see
    /// [`Self::make_durable`]).
    fn flush(&mut self, storage: &Storage) -> Result<(), SqlError> {
        if self.committed {
            self.committed = false;
            if let Some(error) = self.make_durable(storage) {
                return Err(error);
            }
        }
        for done in self.deferred.drain(..) {
            self.emitter
                .statement_done(done.count, done.in_transaction, done.command);
        }
        Ok(())
    }

    /// The end of the batch: one fsync if any statement may have committed
    /// since the last durability point — by kind, not transaction state, so a
    /// hidden mini-commit (an identity reservation, even inside an open
    /// transaction or under a statement that then failed) is never missed —
    /// then the remaining DONEs. Returns the durability error, if any.
    fn finish(&mut self, storage: &Storage) -> Option<SqlError> {
        if self.committed {
            self.committed = false;
            if let Some(error) = self.make_durable(storage) {
                return Some(error);
            }
        }
        for done in self.deferred.drain(..) {
            self.emitter
                .statement_done(done.count, done.in_transaction, done.command);
        }
        None
    }

    /// Blocks until the batch's commit records are fsync-durable (group
    /// commit). A durability failure wedges the store — the in-memory state is
    /// now ahead of the log, so no further op may serve it — and drops the
    /// deferred DONEs, which would otherwise acknowledge commits a restart is
    /// about to undo.
    fn make_durable(&mut self, storage: &Storage) -> Option<SqlError> {
        match storage.ensure_durable(storage.wal_tail()) {
            Ok(()) => None,
            Err(err) => {
                storage.wedge();
                self.deferred.clear();
                self.durability_failed = true;
                Some(map_storage_err(err, ""))
            }
        }
    }
}

/// Runs a statement list, recursing into `TRY`/`CATCH`. `in_try` is true while
/// executing inside a `TRY` block, where a statement error transfers control to
/// the matching `CATCH` (returned as `Err`) instead of applying the normal
/// batch policy. Returns `Err` when the enclosing context must stop: a cancel,
/// an error that propagates to a `CATCH`, or a dooming/terminating error at the
/// top level.
/// The inner SQL text of an `EXEC sp_executesql N'...'` whose statement
/// argument is a string LITERAL — the analyzable case. `None` for any other
/// procedure or a non-literal statement argument.
fn exec_literal_sql(exec: &ExecStatement) -> Option<String> {
    if !strip_schema(&exec.proc.value).eq_ignore_ascii_case("sp_executesql") {
        return None;
    }
    let stmt = exec
        .args
        .iter()
        .find(|a| {
            a.name.as_ref().is_some_and(|n| {
                n.value.eq_ignore_ascii_case("stmt") || n.value.eq_ignore_ascii_case("statement")
            })
        })
        .or_else(|| exec.args.first().filter(|a| a.name.is_none()))?;
    match &stmt.value.kind {
        ExprKind::Str(text) => Some(text.clone()),
        _ => None,
    }
}

thread_local! {
    /// Nesting depth of EXEC inner batches on this worker (SQL Server caps
    /// procedure nesting at 32, error 217).
    static EXEC_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Ownership-chaining depth for object-permission checks: how many OWNED
    /// stored-object bodies (procedure, scalar UDF, multi-statement TVF, trigger)
    /// enclose the current statement. Distinct from [`EXEC_DEPTH`] because
    /// `sp_executesql` bumps that but does NOT chain — dynamic SQL runs in the
    /// caller's own permission context. Permission checks fire only where this
    /// (and `VIEW_DEPTH`) is 0.
    static CHAIN_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard entered when running an OWNED stored-object body (procedure,
/// scalar UDF, multi-statement TVF, trigger): it raises the ownership-chaining
/// depth so the body's object reads are not re-permission-checked (the caller's
/// permission on the object suffices — single `dbo` owner).
struct ChainGuard;

impl ChainGuard {
    fn enter() -> Self {
        CHAIN_DEPTH.with(|d| d.set(d.get() + 1));
        ChainGuard
    }
}

impl Drop for ChainGuard {
    fn drop(&mut self) {
        CHAIN_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// RAII guard entered when running DYNAMIC SQL (`sp_executesql`): it RESETS the
/// ownership-chaining depth to 0 for the duration, then restores it — dynamic
/// SQL never chains, so its statements are permission-checked as the caller's
/// own, even when the `sp_executesql` call sits inside a procedure body.
struct DynamicScope(u32);

impl DynamicScope {
    fn enter() -> Self {
        let saved = CHAIN_DEPTH.with(|d| d.replace(0));
        DynamicScope(saved)
    }
}

impl Drop for DynamicScope {
    fn drop(&mut self) {
        CHAIN_DEPTH.with(|d| d.set(self.0));
    }
}

/// Runs `EXEC sp_executesql @stmt [, @params, values...]`: evaluates the
/// arguments against the CURRENT variables, then runs the inner text as its
/// own batch scope — fresh variables seeded from the declared parameters
/// (inner DECLAREs do not leak out; outer variables are not visible inside),
/// sharing the transaction context. Each inner statement emits its own
/// events, exactly like a top-level statement. Any other procedure answers
/// 2812, the same as the RPC path.
/// An EXEC failure, tagged by ORIGIN — the fact the EXEC arm needs and must
/// not guess: `run_exec`'s own validation/depth errors are statement-scope at
/// the EXEC site, while an error that crossed out of the inner batch already
/// terminated it (batch-abort scope is the whole nest).
enum ExecError {
    Own(SqlError),
    Inner(SqlError),
}

/// Applies the standard doom rule to an error raised outside any statement's
/// own execution — `run_exec`'s validation and depth errors, which no inner
/// `run_block` arm will see. The decision is made here, at the source, so the
/// TRY boundary never has to re-derive it (it cannot know the error's origin).
fn doom_per_rule(txn_ctx: &mut TxnContext, error: SqlError) -> SqlError {
    if txn_ctx.in_txn() && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY) {
        txn_ctx.doomed = true;
    }
    error
}

/// Executes a user stored procedure: binds arguments to declared parameters
/// (positional and named, defaults filling gaps, OUTPUT validated), runs the
/// stored body text under a fresh variable scope with SET options reverting
/// at exit (the sp_executesql posture), captures the RETURN status into
/// `EXEC @rc =`, and copies OUTPUT parameters back — both only when the body
/// completes (SQL Server skips them when execution aborts).
fn run_user_procedure(
    storage: &Storage,
    exec: &ExecStatement,
    def: &TableDef,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
    let procedure = def.procedure.as_ref().expect("checked by the caller");
    let own = |txn_ctx: &mut TxnContext, error: SqlError| -> ExecError {
        ExecError::Own(doom_per_rule(txn_ctx, error))
    };
    // Evaluate arguments in the CALLER's scope.
    let eval_ctx = txn_ctx.eval_context();
    let mut positional = Vec::new();
    let mut named: Vec<(String, SqlValue, bool, Option<String>)> = Vec::new();
    let mut positional_meta: Vec<(bool, Option<String>)> = Vec::new();
    for (arg_index, arg) in exec.args.iter().enumerate() {
        // Once an argument is named, the rest must be (SQL Server 119) —
        // silently continuing would bind positions past the named one.
        if arg.name.is_none() && !named.is_empty() {
            let error = SqlError::new(
                119,
                15,
                1,
                format!(
                    "Must pass parameter number {} and subsequent parameters as '@name = value'. \
                     After the form '@name = value' has been used, all subsequent parameters must \
                     be passed in the form '@name = value'.",
                    arg_index + 1
                ),
            );
            return Err(own(txn_ctx, error));
        }
        // An OUTPUT argument must be a bare variable (it receives a value).
        let arg_var = match &arg.value.kind {
            ExprKind::LocalVar(name) => Some(name.clone()),
            _ => None,
        };
        if arg.output && arg_var.is_none() {
            let error = SqlError::new(
                179,
                16,
                1,
                "Cannot use the OUTPUT option when passing a constant to a stored procedure.",
            );
            return Err(own(txn_ctx, error));
        }
        let value = eval_constant(&arg.value, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
        match &arg.name {
            Some(n) => {
                let key = n.value.trim_start_matches('@').to_ascii_lowercase();
                // A parameter supplied twice (named twice, or named on top
                // of a positional binding) is an error, not a silent pick.
                let position_of = |name: &str| procedure.params.iter().position(|p| p.name == name);
                let already_positional =
                    position_of(&key).is_some_and(|index| index < positional.len());
                if already_positional || named.iter().any(|(n, ..)| *n == key) {
                    let error = SqlError::new(
                        8143,
                        16,
                        1,
                        format!(
                            "Parameter '@{key}' was supplied multiple times for procedure {}.",
                            def.name
                        ),
                    );
                    return Err(own(txn_ctx, error));
                }
                named.push((key, value, arg.output, arg_var));
            }
            None => {
                positional.push(value);
                positional_meta.push((arg.output, arg_var));
            }
        }
    }
    // `EXEC @rc = p`: the status variable must already be declared (137).
    if let Some(rc) = &exec.return_var
        && !txn_ctx.variables.contains_key(rc)
    {
        let error = undeclared_variable_err(rc);
        return Err(own(txn_ctx, error));
    }
    if positional.len() > procedure.params.len() {
        let error = SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        );
        return Err(own(txn_ctx, error));
    }
    // Named arguments that match no declared parameter fail before any
    // binding (8145 precedes 201, as SQL Server orders it).
    for (name, ..) in &named {
        if !procedure.params.iter().any(|p| p.name == *name) {
            let error = SqlError::new(
                8145,
                16,
                2,
                format!("@{name} is not a parameter for procedure {}.", def.name),
            );
            return Err(own(txn_ctx, error));
        }
    }
    // Bind: positional in declaration order, then named by name, then
    // defaults; a missing non-default parameter is 201. OUTPUT copy-back
    // targets (param name -> caller variable) are collected as we bind.
    let mut bound: Vec<(String, ColumnType, SqlValue)> = Vec::new();
    let mut copy_back: Vec<(String, String)> = Vec::new();
    for (index, param) in procedure.params.iter().enumerate() {
        let column_type = ColumnType::parse(&param.type_spec).map_err(|e| {
            let error = SqlError::message_only(245, e.to_string());
            own(txn_ctx, error)
        })?;
        let supplied = if index < positional.len() {
            let (output, arg_var) = positional_meta[index].clone();
            Some((positional[index].clone(), output, arg_var))
        } else {
            named
                .iter()
                .find(|(n, ..)| *n == param.name)
                .map(|(_, v, output, arg_var)| (v.clone(), *output, arg_var.clone()))
        };
        let coerce = |value: SqlValue| -> Result<SqlValue, SqlError> {
            let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
            Ok(value::datum_to_sql(&datum, &column_type))
        };
        let value = match supplied {
            Some((value, output, arg_var)) => {
                if output {
                    if !param.output {
                        let error = SqlError::new(
                            8162,
                            16,
                            2,
                            format!(
                                "The formal parameter \"@{}\" was not declared as an OUTPUT \
                                 parameter, but the actual parameter passed in requested output.",
                                param.name
                            ),
                        );
                        return Err(own(txn_ctx, error));
                    }
                    copy_back.push((
                        param.name.clone(),
                        arg_var.expect("validated: OUTPUT arguments are variables"),
                    ));
                }
                // Bind-time conversion to the DECLARED type, as SQL Server
                // converts (or errors) at the call — without it a string
                // argument flows into an INT parameter mistagged.
                coerce(value).map_err(|e| own(txn_ctx, e))?
            }
            None => match &param.default {
                Some(text) => {
                    let expr = truthdb_sql::parse_expr(text).map_err(|e| own(txn_ctx, e))?;
                    let value = eval_constant(&expr, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
                    coerce(value).map_err(|e| own(txn_ctx, e))?
                }
                None => {
                    let error = SqlError::new(
                        201,
                        16,
                        4,
                        format!(
                            "Procedure or function '{}' expects parameter '@{}', which was not \
                             supplied.",
                            def.name, param.name
                        ),
                    );
                    return Err(own(txn_ctx, error));
                }
            },
        };
        bound.push((param.name.clone(), column_type, value));
    }
    // The stored body parses under the in-procedure grammar.
    let statements =
        truthdb_sql::parse_procedure_body(&procedure.body).map_err(|e| own(txn_ctx, e))?;

    // Fresh scope, SET options reverting at exit — the sp_executesql shape.
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    let outer_xact_abort = txn_ctx.xact_abort;
    let outer_nocount = txn_ctx.nocount;
    let outer_isolation = txn_ctx.isolation;
    let outer_showplan = txn_ctx.showplan_text;
    for (name, column_type, value) in bound {
        txn_ctx.variables.insert(name, (column_type, value));
    }
    txn_ctx.proc_stack.push(def.name.clone());
    txn_ctx.proc_return = None;
    // A procedure called from a trigger body does NOT see the trigger's
    // inserted/deleted (they are visible only in the trigger's own statements).
    let _trigger_shadow = TriggerScope::clear();
    // A procedure body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        let error = SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        );
        Err(ExecError::Own(doom_per_rule(txn_ctx, error)))
    } else {
        run_block(storage, &statements, txn_ctx, run, in_try)
            .map(|_| ())
            .map_err(ExecError::Inner)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.proc_stack.pop();
    // Capture OUTPUT values from the inner scope BEFORE restoring the outer.
    let output_values: Vec<(String, (ColumnType, SqlValue))> = copy_back
        .iter()
        .filter_map(|(param, var)| {
            txn_ctx
                .variables
                .get(param)
                .map(|slot| (var.clone(), slot.clone()))
        })
        .collect();
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    txn_ctx.xact_abort = outer_xact_abort;
    txn_ctx.nocount = outer_nocount;
    txn_ctx.isolation = outer_isolation;
    txn_ctx.showplan_text = outer_showplan;
    let return_status = txn_ctx.proc_return.take().unwrap_or(0);
    if result.is_ok() {
        // OUTPUT copy-back and the return status land only when the body
        // completed (SQL Server skips both when execution aborts).
        for (var, slot) in output_values {
            txn_ctx.variables.insert(var, slot);
        }
        if let Some(rc) = &exec.return_var {
            txn_ctx
                .variables
                .insert(rc.clone(), (ColumnType::Int, SqlValue::Int(return_status)));
        }
    }
    result
}

/// Runs a scalar user-defined function's body once with `arg_values` bound to
/// its parameters, returning the value its `RETURN` produced, coerced to the
/// declared return type.
///
/// The body runs in an isolated throwaway context — only the parameters are
/// visible (SQL Server functions do not see caller locals), no transaction is
/// open (functions are side-effect-free), and any table reads observe the
/// caller's ambient snapshot on this thread. Nesting shares the `EXEC_DEPTH`
/// budget (217 at depth 32). Because the context has no transaction, an error in
/// the body always terminates the function (there is no XACT_ABORT-OFF continue
/// path), which is exactly the SQL Server posture: a function error aborts the
/// statement that called it.
fn run_user_scalar_function(
    storage: &Storage,
    def: &TableDef,
    arg_values: &[SqlValue],
    caller: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let function = def.function.as_ref().expect("checked by the caller");
    // The caller (resolve_scalar_function) only routes scalar functions here.
    let FunctionReturns::Scalar { type_spec, body } = &function.returns else {
        return Err(function_not_a_table(&def.name));
    };
    // Invoking a scalar function needs EXECUTE permission.
    enforce_object_permission(def, &caller.security, PermAction::Execute)?;
    if arg_values.len() < function.params.len() {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {}.",
                def.name
            ),
        ));
    }
    if arg_values.len() > function.params.len() {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        ));
    }
    let return_type =
        ColumnType::parse(type_spec).map_err(|e| SqlError::message_only(245, e.to_string()))?;
    // Fresh scope with only the parameters; the caller's session identity is
    // carried so DB_NAME()/SUSER_SNAME()/USER_NAME()/@@SPID and role membership
    // resolve inside the body. The sids are left 0 (the body reuses the caller's
    // already-computed role set rather than re-resolving membership).
    let mut txn_ctx = TxnContext::default();
    txn_ctx.set_session_identity(
        caller.database.clone(),
        caller.login.clone(),
        caller.spid,
        caller.user.clone(),
        0,
        0,
    );
    txn_ctx.session_server_roles = caller.server_roles.clone();
    txn_ctx.session_db_roles = caller.db_roles.clone();
    txn_ctx.security = caller.security.clone();
    for (param, value) in function.params.iter().zip(arg_values) {
        let column_type = ColumnType::parse(&param.type_spec)
            .map_err(|e| SqlError::message_only(245, e.to_string()))?;
        let datum = value::sql_to_datum(value, &column_type, &param.name)?;
        let coerced = value::datum_to_sql(&datum, &column_type);
        txn_ctx
            .variables
            .insert(param.name.clone(), (column_type, coerced));
    }
    let statements = truthdb_sql::parse_function_body(body)?;
    // A scalar function called from a trigger body does not see inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // A function body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        Err(SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        ))
    } else {
        let mut emitter = DiscardEmitter;
        let mut run = BatchRun {
            emitter: &mut emitter,
            deferred: Vec::new(),
            rowset_open: false,
            durability_failed: false,
            committed: false,
            last_error: None,
            function_return_type: Some(return_type),
        };
        run_block(storage, &statements, &mut txn_ctx, &mut run, false).map(|_| ())
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    result?;
    // The body ends in `RETURN <expr>` (enforced at CREATE, 455), so a completed
    // body always set `func_return`.
    txn_ctx.func_return.take().ok_or_else(|| {
        SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )
    })
}

fn run_exec(
    storage: &Storage,
    exec: &ExecStatement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
    if !strip_schema(&exec.proc.value).eq_ignore_ascii_case("sp_executesql") {
        // A user procedure, if the catalog has one; 2812 otherwise.
        if let Some(def) = resolve_table(storage, &exec.proc.value)
            && def.is_procedure()
        {
            enforce_object_permission(&def, &txn_ctx.security, PermAction::Execute)
                .map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e.at(exec.proc.span))))?;
            return run_user_procedure(storage, exec, &def, txn_ctx, run, in_try);
        }
        let error = SqlError::new(
            2812,
            16,
            62,
            format!("Could not find stored procedure '{}'.", exec.proc.value),
        )
        .at(exec.proc.span);
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    }
    if exec.return_var.is_some() {
        let error = SqlError::new(
            179,
            16,
            1,
            "Cannot capture a return status from sp_executesql.",
        );
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    }
    let eval_ctx = txn_ctx.eval_context();
    let mut positional = Vec::new();
    let mut named: Vec<(String, SqlValue)> = Vec::new();
    for arg in &exec.args {
        let value = eval_constant(&arg.value, &eval_ctx)
            .map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e)))?;
        match &arg.name {
            Some(n) => named.push((n.value.clone(), value)),
            None => positional.push(value),
        }
    }
    let take_named = |named: &mut Vec<(String, SqlValue)>, keys: &[&str]| -> Option<SqlValue> {
        let index = named
            .iter()
            .position(|(n, _)| keys.iter().any(|k| n.eq_ignore_ascii_case(k)))?;
        Some(named.remove(index).1)
    };
    let mut positional = positional.into_iter();
    let stmt = match take_named(&mut named, &["stmt", "statement"]).or_else(|| positional.next()) {
        Some(value) => value,
        None => {
            let error = SqlError::new(
                214,
                16,
                2,
                "Procedure expects parameter '@statement' of type 'ntext/nchar/nvarchar'.",
            );
            return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
        }
    };
    let SqlValue::Str(sql) = stmt else {
        let error = SqlError::new(
            214,
            16,
            2,
            "Procedure expects parameter '@statement' of type 'ntext/nchar/nvarchar'.",
        );
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    };
    let decls =
        match take_named(&mut named, &["params", "parameters"]).or_else(|| positional.next()) {
            Some(SqlValue::Str(d)) => d,
            Some(SqlValue::Null) | None => String::new(),
            Some(_) => {
                let error = SqlError::new(
                    214,
                    16,
                    3,
                    "Procedure expects parameter '@params' of type 'ntext/nchar/nvarchar'.",
                );
                return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
            }
        };
    // Bind values: named ones by their own names, positional ones from the
    // declaration list, exactly as the RPC path binds unnamed wire values.
    let names = decl_names(&decls);
    let mut seeded: Vec<(String, SqlValue)> = named;
    for (i, value) in positional.enumerate() {
        let Some(name) = names.get(i) else {
            let error = SqlError::new(
                8144,
                16,
                2,
                "Procedure or function has too many arguments specified.",
            );
            return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
        };
        seeded.push((name.clone(), value));
    }
    let statements =
        truthdb_sql::parse(&sql).map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e)))?;

    // The inner batch is its own variable scope, on the shared transaction —
    // and SET options revert at scope exit, as SQL Server reverts them: an
    // inner SET (XACT_ABORT, ISOLATION LEVEL, SHOWPLAN) must not outlive the
    // EXEC, or a post-EXEC statement would run under an isolation the up-front
    // lock analysis never saw.
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    let outer_xact_abort = txn_ctx.xact_abort;
    let outer_nocount = txn_ctx.nocount;
    let outer_isolation = txn_ctx.isolation;
    let outer_showplan = txn_ctx.showplan_text;
    for (name, value) in seeded {
        let key = name.trim_start_matches('@').to_ascii_lowercase();
        let column_type = value::infer_type(std::slice::from_ref(&value));
        txn_ctx.variables.insert(key, (column_type, value));
    }
    // Dynamic SQL run from a trigger body does not see inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // Dynamic SQL does NOT ownership-chain: reset the chaining depth so its
    // statements are permission-checked as the caller's own, even when this
    // sp_executesql sits inside a procedure body.
    let _dynamic = DynamicScope::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        let error = SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        );
        Err(ExecError::Own(doom_per_rule(txn_ctx, error)))
    } else {
        // An inner RETURN exits the inner batch only (Break/Continue cannot
        // escape — the inner parse rejects them, its own 135/136 scope). An
        // error crossing out already carries every decision: dooming, and by
        // crossing at all, termination of the whole nest.
        run_block(storage, &statements, txn_ctx, run, in_try)
            .map(|_| ())
            .map_err(ExecError::Inner)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    txn_ctx.xact_abort = outer_xact_abort;
    txn_ctx.nocount = outer_nocount;
    txn_ctx.isolation = outer_isolation;
    txn_ctx.showplan_text = outer_showplan;
    result
}

/// The ONE place a failed statement's fate is decided — continue the batch
/// (`Ok(())`), or end it (`Err`, dooming already applied). The doom decision
/// needs the statement's KIND (RAISERROR is exempt from XACT_ABORT; THROW is
/// batch-terminating without dooming), so every decide-now error site funnels
/// here: the generic statement arm and IF/WHILE condition failures. (EXEC
/// boundary errors do NOT — theirs were decided at the source, in the inner
/// `run_block` or `doom_per_rule`.)
fn statement_error_ladder(
    statement: &Statement,
    error: SqlError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), SqlError> {
    // A cancelled statement aborts the batch immediately: key on the cancel
    // marker, not any flag, so an Attention landing concurrently with an
    // unrelated failure cannot suppress that failure's dooming. A cancel is
    // not a SQL error, so `@@ERROR` is untouched.
    if error.number == CANCEL_ERROR {
        return Err(error);
    }
    txn_ctx.record_error(error.number);
    // A durability failure wedged the store (a flush inside the statement,
    // e.g. before a snapshot capture): never continue past a lost commit.
    if run.durability_failed {
        return Err(error);
    }
    // Severity >= 20 is fatal to the connection: it bypasses TRY (the
    // TryCatch arm refuses it too), dooms the transaction, and the protocol
    // layer closes the stream after delivering it.
    if error.level >= FATAL_SEVERITY {
        if txn_ctx.in_txn() {
            txn_ctx.doomed = true;
        }
        return Err(error);
    }
    // The doom decision is made HERE, where the failing statement's kind is
    // known — never re-derived at the TRY boundary, which cannot see it.
    // `SET XACT_ABORT` (or severity >= 17) dooms; RAISERROR is exempt by
    // definition (SQL Server: "errors raised by RAISERROR are not affected
    // by SET XACT_ABORT") and never dooms.
    let dooms = !matches!(statement, Statement::RaiseError(_))
        && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY);
    if txn_ctx.in_txn() && dooms {
        txn_ctx.doomed = true;
    }
    // Inside a TRY, the error then transfers to the matching CATCH (which
    // sees XACT_STATE() = -1 when it doomed). The CATCH runs more statements,
    // so a result set this one already started streaming must be closed.
    if in_try {
        run.abort_open_rowset(txn_ctx.in_txn());
        return Err(error);
    }
    // RAISERROR is statement-scope: the batch always continues.
    if matches!(statement, Statement::RaiseError(_)) {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    // THROW always terminates the batch — even when it does not doom the
    // transaction (XACT_ABORT OFF leaves it open and committable later).
    if matches!(statement, Statement::Throw(_)) {
        return Err(error);
    }
    // Other statements: a non-dooming in-transaction error rolls back only
    // the statement and the batch continues; a dooming one ends the batch
    // (only ROLLBACK is then accepted, error 3930). This must stay keyed on the
    // ERROR (its severity / XACT_ABORT), NOT on whether the transaction is
    // already doomed: a doomed transaction still runs a CATCH's reads and
    // statement-terminating errors (division by zero, conversion) so the CATCH
    // can reach `IF XACT_STATE() <> 0 ROLLBACK` — terminating the batch on those
    // would leave the uncommittable transaction open holding its locks.
    if txn_ctx.in_txn() && !dooms {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    Err(error)
}

/// Enters the versioned-read scopes for an IF/WHILE condition that reads
/// tables — the SAME rules a SELECT gets in `exec_statement_streamed`: under
/// RCSI the condition reads its own statement snapshot; under SNAPSHOT
/// isolation it establishes/uses the transaction snapshot and enforces 3952.
/// Without this the condition read holds NEITHER lock nor snapshot (analysis
/// assumes versioned reads and drops Table S) — a live dirty read, the
/// Stage 13 seam class, caught by the control-flow review.
fn enter_condition_scopes<'a>(
    storage: &'a Storage,
    condition: &Expr,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<(Option<SnapshotScope<'a>>, Option<TxnSnapshotScope>), SqlError> {
    let mut tables = Vec::new();
    collect_expr_tables(condition, &mut tables);
    // A scalar function the condition calls may read tables through its body;
    // those reads must observe the same snapshot as a direct read (the lock
    // analysis already resolved them), so arm the scope when the condition
    // reaches any table directly OR through a called function.
    if tables.is_empty() && expr_function_read_ids(storage, condition).is_empty() {
        return Ok((None, None));
    }
    match txn_ctx.isolation() {
        Isolation::ReadCommitted if storage.rcsi_enabled() => {
            // The snapshot is the durable commit prefix: the session's own
            // just-committed statements must be durable before capture.
            run.flush(storage)?;
            Ok((
                Some(SnapshotScope::enter(
                    storage,
                    txn_ctx.txn.as_ref().map(StorageTxn::txn_id),
                )),
                None,
            ))
        }
        Isolation::Snapshot => {
            if !storage.snapshot_isolation_allowed() {
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return Err(snapshot_not_allowed_error(&txn_ctx.database));
            }
            if txn_ctx.in_txn() {
                if txn_ctx.txn_snapshot.is_none() {
                    // First data access establishes the transaction's view —
                    // a condition read counts.
                    run.flush(storage)?;
                    let own = txn_ctx.txn.as_ref().map(StorageTxn::txn_id);
                    txn_ctx.txn_snapshot = Some(storage.capture_read_snapshot(own));
                }
                Ok((None, txn_ctx.txn_snapshot.map(TxnSnapshotScope::enter)))
            } else {
                run.flush(storage)?;
                Ok((Some(SnapshotScope::enter(storage, None)), None))
            }
        }
        _ => Ok((None, None)),
    }
}

/// Evaluates an IF/WHILE condition: subqueries (EXISTS, scalar, IN) resolve
/// eagerly through the same machinery as WHERE-clause subqueries, then the
/// residual expression evaluates against the session context. T-SQL
/// three-valued: TRUE runs the branch; FALSE and NULL (UNKNOWN) do not.
fn eval_condition(
    storage: &Storage,
    condition: &Expr,
    txn_ctx: &TxnContext,
) -> Result<bool, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let no_outer = |_: &str| -> Option<usize> { None };
    let resolved = substitute_correlated_in_expr(storage, condition, &no_outer, &[], &eval_ctx)?;
    match eval_constant(&resolved, &eval_ctx)? {
        SqlValue::Bool(taken) => Ok(taken),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is              expected.",
        )),
    }
}

/// How a statement block ended: normally, or via a control-flow statement
/// that must propagate to the construct that absorbs it (`WHILE` for
/// Break/Continue, the batch — later the procedure — for Return). TRY/CATCH
/// and plain blocks pass every non-Normal flow straight through.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Flow {
    Normal,
    Break,
    Continue,
    Return,
}

fn run_block(
    storage: &Storage,
    statements: &[Statement],
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<Flow, SqlError> {
    for statement in statements {
        // A TDS Attention (cancel) aborts the batch before the next statement.
        // It is never catchable — it propagates straight out, past any TRY.
        check_cancelled()?;
        if let Statement::Exec(exec) = statement {
            // The inner statements flow through `run_block` recursion, whose
            // own loop applies the per-statement flush and commit flag — the
            // same shape as TRY/CATCH dispatch. Errors take the ordinary
            // statement path: cancels and durability failures propagate, a
            // TRY transfers to CATCH, XACT_ABORT OFF continues the batch.
            match run_exec(storage, exec, txn_ctx, run, in_try) {
                Ok(()) => {}
                Err(exec_error) => {
                    // A failed EXEC sets @@ROWCOUNT to 0 like any failed
                    // statement.
                    txn_ctx.rowcount = 0;
                    let (error, from_inner) = match exec_error {
                        ExecError::Own(error) => (error, false),
                        ExecError::Inner(error) => (error, true),
                    };
                    if error.number == CANCEL_ERROR {
                        return Err(error);
                    }
                    // Inner errors were recorded at their raise site (the
                    // inner ladder), where the procedure frame was still
                    // live; re-recording here would blank ERROR_PROCEDURE().
                    if !from_inner {
                        txn_ctx.record_error(error.number);
                    }
                    if run.durability_failed {
                        return Err(error);
                    }
                    // Transfer to CATCH: decisions (dooming included) were
                    // already made where the error arose — per-statement in
                    // the inner `run_block`, or `doom_per_rule` for
                    // `run_exec`'s own errors. A fatal (>= 20) error is
                    // refused by the TryCatch arm's own filter.
                    if in_try {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        return Err(error);
                    }
                    // An error crossing OUT of the inner batch already
                    // terminated it — and batch-abort scope is the whole
                    // nest, so the outer batch ends too (a THROW inside
                    // EXEC'd text ends the calling batch even when nothing
                    // doomed; non-dooming ordinary errors never cross — the
                    // inner run_block continued past them). Nothing is
                    // re-derived from severity here: the review showed that
                    // second derivation dropped THROW's termination.
                    if from_inner {
                        return Err(error);
                    }
                    // run_exec's OWN failure (unknown proc, 214, 8144, parse,
                    // depth): statement-scope at the EXEC site. Dooming was
                    // applied at the source; this decides only continuation.
                    let terminates = txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY;
                    if txn_ctx.in_txn() && !terminates {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        run.last_error = Some(error);
                        continue;
                    }
                    return Err(error);
                }
            }
            continue;
        }
        match statement {
            Statement::Block { body, .. } => {
                match run_block(storage, body, txn_ctx, run, in_try)? {
                    Flow::Normal => {}
                    flow => return Ok(flow),
                }
                continue;
            }
            Statement::If {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                // A successful condition evaluation resets `@@ERROR` (the IF
                // itself is a statement) — AFTER the condition read it, which
                // is what makes `IF @@ERROR <> 0` work.
                // A condition subquery reads table variables through the same
                // FROM path as a SELECT, so it needs the same read view armed —
                // the IF/WHILE arms bypass exec_statement_streamed, so arm here.
                let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                    .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                {
                    Ok(taken) => taken,
                    Err(error) => {
                        txn_ctx.rowcount = 0;
                        statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                        continue;
                    }
                };
                txn_ctx.last_error = 0;
                let branch = if taken {
                    Some(then_branch)
                } else {
                    else_branch.as_ref()
                };
                if let Some(branch) = branch {
                    match run_block(storage, std::slice::from_ref(branch), txn_ctx, run, in_try)? {
                        Flow::Normal => {}
                        flow => return Ok(flow),
                    }
                }
                continue;
            }
            Statement::While {
                condition, body, ..
            } => {
                loop {
                    // A TDS Attention lands between iterations too — an
                    // infinite `WHILE 1 = 1` must die on cancel even when its
                    // body runs no cancellable statement.
                    check_cancelled()?;
                    // Re-armed each iteration: the body may INSERT into @t, and
                    // the next condition read must see the updated rows.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                        .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                    {
                        Ok(taken) => taken,
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            break;
                        }
                    };
                    txn_ctx.last_error = 0;
                    if !taken {
                        break;
                    }
                    match run_block(storage, std::slice::from_ref(body), txn_ctx, run, in_try)? {
                        Flow::Normal | Flow::Continue => {}
                        Flow::Break => break,
                        Flow::Return => return Ok(Flow::Return),
                    }
                }
                continue;
            }
            // The parser rejects BREAK/CONTINUE outside a WHILE (135/136), so
            // these only ever propagate up to an enclosing loop.
            Statement::Break { .. } => return Ok(Flow::Break),
            Statement::Continue { .. } => return Ok(Flow::Continue),
            // The parser rejects `RETURN <value>` outside a procedure (178);
            // inside one the status is stashed for `EXEC @rc =` to read.
            Statement::Return { value, .. } => {
                // A scalar function body's RETURN: evaluate its (mandatory)
                // value, coerce it to the declared return type, and stash it for
                // the caller. Nested user functions and subqueries in the RETURN
                // expression are rewritten to literals first, exactly like an
                // IF/WHILE condition.
                if let Some(return_type) = run.function_return_type {
                    let value = value
                        .as_ref()
                        .expect("a scalar function RETURN carries a value (parser-enforced)");
                    // A RETURN subquery reads table variables through the FROM
                    // path; arm the body's own (empty) view so it shadows the
                    // caller's rather than reading caller locals.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let eval_ctx = txn_ctx.eval_context();
                    let no_outer = |_: &str| -> Option<usize> { None };
                    let coerced =
                        substitute_correlated_in_expr(storage, value, &no_outer, &[], &eval_ctx)
                            .and_then(|bound| eval_constant(&bound, &eval_ctx))
                            .and_then(|raw| {
                                let datum =
                                    value::sql_to_datum(&raw, &return_type, "return value")?;
                                Ok(value::datum_to_sql(&datum, &return_type))
                            });
                    match coerced {
                        Ok(coerced) => {
                            txn_ctx.func_return = Some(coerced);
                            return Ok(Flow::Return);
                        }
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                if let Some(value) = value {
                    let eval_ctx = txn_ctx.eval_context();
                    match eval_constant(value, &eval_ctx) {
                        Ok(SqlValue::Int(status))
                            if (i32::MIN as i64..=i32::MAX as i64).contains(&status) =>
                        {
                            txn_ctx.proc_return = Some(status)
                        }
                        // A RETURN value outside int range overflows, as SQL
                        // Server does (8115) — the status is an int. Without this
                        // the out-of-range value would be stashed and later fail
                        // to encode (and, on the RPC path, read back as NULL and
                        // be mistaken for a procedure that never completed).
                        Ok(SqlValue::Int(_)) => {
                            let error = SqlError::new(
                                8115,
                                16,
                                2,
                                "Arithmetic overflow error converting expression to data type int.",
                            );
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                        Ok(SqlValue::Null) => {
                            // SQL Server warns and returns 0; we return 0.
                            txn_ctx.proc_return = Some(0);
                        }
                        Ok(_) | Err(_) => {
                            let error =
                                eval_constant(value, &eval_ctx).err().unwrap_or_else(|| {
                                    SqlError::new(
                                        257,
                                        16,
                                        3,
                                        "The RETURN status must be an integer.",
                                    )
                                });
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                return Ok(Flow::Return);
            }
            _ => {}
        }
        if let Statement::TryCatch {
            try_block,
            catch_block,
            ..
        } = statement
        {
            match run_block(storage, try_block, txn_ctx, run, true) {
                Ok(Flow::Normal) => {}
                // BREAK/CONTINUE/RETURN cross a TRY without running its CATCH.
                Ok(flow) => return Ok(flow),
                // An Attention that landed inside the TRY block is not caught.
                Err(cancel) if cancel.number == CANCEL_ERROR => return Err(cancel),
                // A durability failure wedged the store: no CATCH swallows a
                // lost commit (the old batch-end fsync ran past every TRY).
                Err(error) if run.durability_failed => return Err(error),
                // Severity >= 20 is fatal to the connection: no CATCH sees
                // it. Already recorded (and doomed) at the raise site.
                Err(error) if error.level >= FATAL_SEVERITY => return Err(error),
                Err(error) => {
                    // The failed statement's own writes were already undone to
                    // its savepoint (`rel_statement_scoped`), and the doom
                    // decision was made where the statement failed — the inner
                    // `run_block` knows the statement's kind (RAISERROR is
                    // exempt from XACT_ABORT), this boundary does not. Control
                    // transfers to CATCH either way; a doomed transaction
                    // reports XACT_STATE() = -1 there.
                    txn_ctx.push_error(&error);
                    // The CATCH block runs in the *enclosing* try-context: its
                    // own errors are not caught here, so they propagate to an
                    // outer CATCH (or end the batch) per `in_try`.
                    let caught = run_block(storage, catch_block, txn_ctx, run, in_try);
                    txn_ctx.pop_error();
                    match caught? {
                        Flow::Normal => {}
                        flow => return Ok(flow),
                    }
                }
            }
            continue;
        }
        // A statement that can open a result set is a durability point: the
        // deferred DONEs must reach the stream before its columns do, and any
        // commit made so far must be fsync-durable before rows that can carry
        // its state (an identity value, via SCOPE_IDENTITY()) leave the server.
        if produces_rowset(statement) || matches!(statement, Statement::RaiseError(_)) {
            run.flush(storage)?;
        }
        // Flag durability by statement kind, before matching the result: a
        // write/DDL/COMMIT can commit even when it then errors — an autocommit
        // statement, an identity reservation (its own mini-commit, made even
        // inside an open transaction and even if the row insert later fails),
        // or the outermost COMMIT.
        run.committed |= statement_may_commit(statement);
        match exec_statement_streamed(storage, statement, txn_ctx, run) {
            Ok(outcome) => {
                // The statement succeeded: `@@ERROR` reads 0 — except after a
                // severity <= 10 RAISERROR, which set it itself (0, or 50000
                // under SETERROR).
                if !matches!(statement, Statement::RaiseError(_)) {
                    txn_ctx.last_error = 0;
                }
                let in_transaction = txn_ctx.in_txn();
                let command = done_command(statement);
                // `SET NOCOUNT ON` suppresses the DONE's count on the wire;
                // rows/results are untouched. `@@ROWCOUNT` records the true
                // count either way (NOCOUNT does not change it).
                let nocount = txn_ctx.nocount;
                let wire_count =
                    |count: u64| -> Option<u64> { if nocount { None } else { Some(count) } };
                // `USE` succeeded: earlier statements' deferred DONEs go out
                // first, then the database-context ENVCHANGE + 5701 INFO the
                // client (SSMS) expects, then the USE's own DONE below —
                // SQL Server's exact order.
                if let Statement::Use { database, .. } = statement {
                    run.flush(storage)?;
                    run.database_context(&database.value);
                }
                match outcome {
                    StatementOutcome::Streamed { rows } => {
                        txn_ctx.rowcount = rows as i64;
                        run.done(wire_count(rows), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Rows(rowset)) => {
                        let count = rowset.rows.len() as u64;
                        txn_ctx.rowcount = count as i64;
                        run.open_rowset(rowset.columns);
                        run.rows(rowset.rows);
                        run.done(wire_count(count), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::RowsAffected(n)) => {
                        txn_ctx.rowcount = n as i64;
                        run.done(wire_count(n), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Done) => {
                        // A simple variable assignment (`SET @x = ...`) sets
                        // @@ROWCOUNT to 1 — recorded by exec_set, preserved
                        // here; every other Done statement resets it to 0.
                        if !matches!(
                            statement,
                            Statement::Set(SetStatement::Variable { .. }) | Statement::Declare(_)
                        ) {
                            txn_ctx.rowcount = 0;
                        }
                        run.done(None, in_transaction, command);
                    }
                }
            }
            Err(error) => {
                // A failed statement sets @@ROWCOUNT to 0, as SQL Server does.
                txn_ctx.rowcount = 0;
                statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
            }
        }
    }
    Ok(Flow::Normal)
}

/// `sp_describe_first_result_set`: the column metadata of `tsql`'s first
/// statement's result set, without executing it. Covers exactly the shapes
/// whose output columns are statically derivable — the single-table SELECTs
/// [`scan_plan`] recognises (their COLMETADATA is derived before the first
/// row at execution too). A statement producing no result set describes as
/// zero rows, matching SQL Server. Everything else — joins, aggregates,
/// computed columns — has result types this engine only learns by executing
/// (`infer_type` over the values), so it answers 11514: honest, and the same
/// static-type-derivation gap Stage 8's output streaming tracks.
///
/// `@tsql`'s declared parameters need no binding here: the planner treats an
/// unresolvable variable as a non-seekable predicate and falls back to the
/// scan, and output columns come from the table schema either way — pinned by
/// the parameterized-describe test.
pub fn describe_first_result_set(storage: &Storage, tsql: &str) -> Result<RowSet, SqlError> {
    let undeterminable = || {
        SqlError::new(
            11514,
            16,
            1,
            "The metadata could not be determined because the statement's result-set \
             types are not statically derivable by this server (single-table SELECTs \
             are; joins, aggregates and computed columns are typed at execution).",
        )
    };
    let statements = truthdb_sql::parse(tsql)?;
    // The contract is the batch's first RESULT SET, not its first statement:
    // `INSERT ...; SELECT ...` describes the SELECT. TRY blocks are entered
    // (their statements run unless an error preempts them); a rowset only a
    // CATCH can produce stays undescribed — that path is conditional.
    let columns = match first_rowset_statement(&statements) {
        None => Vec::new(),
        Some(Statement::Select(select)) => {
            // TOP never changes the columns, and `scan_plan` rejects `TOP 0`
            // for an execution-path reason describe does not share.
            let mut select = select.clone();
            select.top = None;
            let mut ctx = TxnContext::default();
            // Describe derives column metadata without executing or reading data;
            // it does not enforce object permissions (and its throwaway context
            // carries no session identity), so the shared `scan_plan` must not
            // treat it as a denied read.
            ctx.security.bypass = true;
            match scan_plan(storage, &select, &ctx.eval_context()) {
                Some(plan) => plan.columns,
                None => return Err(undeterminable()),
            }
        }
        Some(_) => return Err(undeterminable()),
    };

    let mut rows = Vec::new();
    for (ordinal, column) in columns.iter().enumerate() {
        let (type_id, type_name) = describe_type(&column.column_type);
        rows.push(vec![
            Datum::Bit(false),                    // is_hidden
            Datum::Int(ordinal as i32 + 1),       // column_ordinal
            Datum::NVarChar(column.name.clone()), // name
            Datum::Bit(true), // is_nullable (result metadata is nullable, as at execution)
            Datum::Int(type_id), // system_type_id
            Datum::NVarChar(type_name), // system_type_name
        ]);
    }
    Ok(RowSet {
        columns: vec![
            ResultColumn {
                name: "is_hidden".into(),
                column_type: ColumnType::Bit,
            },
            ResultColumn {
                name: "column_ordinal".into(),
                column_type: ColumnType::Int,
            },
            ResultColumn {
                name: "name".into(),
                column_type: ColumnType::NVarChar { max_len: 128 },
            },
            ResultColumn {
                name: "is_nullable".into(),
                column_type: ColumnType::Bit,
            },
            ResultColumn {
                name: "system_type_id".into(),
                column_type: ColumnType::Int,
            },
            ResultColumn {
                name: "system_type_name".into(),
                column_type: ColumnType::NVarChar { max_len: 256 },
            },
        ],
        rows,
    })
}

/// The first statement in `statements` that opens a result set, descending
/// into TRY blocks (entered unless an error preempts them) but not CATCH
/// blocks (conditional).
fn first_rowset_statement(statements: &[Statement]) -> Option<&Statement> {
    statements.iter().find_map(|statement| match statement {
        Statement::TryCatch { try_block, .. } => first_rowset_statement(try_block),
        s if produces_rowset(s) => Some(s),
        _ => None,
    })
}

/// A column type's `sys.types` id and its `system_type_name` spelling, as
/// `sp_describe_first_result_set` reports them.
fn describe_type(column_type: &ColumnType) -> (i32, String) {
    match column_type {
        ColumnType::TinyInt => (48, "tinyint".into()),
        ColumnType::SmallInt => (52, "smallint".into()),
        ColumnType::Int => (56, "int".into()),
        ColumnType::BigInt => (127, "bigint".into()),
        ColumnType::Bit => (104, "bit".into()),
        ColumnType::Real => (59, "real".into()),
        ColumnType::Float => (62, "float".into()),
        ColumnType::Decimal { precision, scale } => (106, format!("decimal({precision},{scale})")),
        ColumnType::Date => (40, "date".into()),
        ColumnType::Time => (41, "time".into()),
        ColumnType::DateTime2 => (42, "datetime2".into()),
        ColumnType::UniqueIdentifier => (36, "uniqueidentifier".into()),
        ColumnType::VarChar { max_len } => (167, format!("varchar({max_len})")),
        ColumnType::NVarChar { max_len } => (231, format!("nvarchar({max_len})")),
        ColumnType::VarBinary { max_len } => (165, format!("varbinary({max_len})")),
        ColumnType::VarCharMax => (167, "varchar(max)".into()),
        ColumnType::NVarCharMax => (231, "nvarchar(max)".into()),
        ColumnType::VarBinaryMax => (165, "varbinary(max)".into()),
    }
}

/// Whether a statement can open a result set on the stream: a `SELECT` that
/// returns rows (an assignment `SELECT @v = ...` returns none). `SET
/// SHOWPLAN_TEXT`'s plan rows ride the same `SELECT` arm.
fn produces_rowset(statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => !select
            .items
            .iter()
            .any(|i| matches!(i, SelectItem::Assign { .. })),
        // EXEC's inner batch — and any control-flow body — may open result
        // sets; conservative.
        Statement::Exec(_) => true,
        Statement::Block { .. } | Statement::If { .. } | Statement::While { .. } => true,
        _ => false,
    }
}

/// One executed statement's outcome, from [`exec_statement_streamed`].
enum StatementOutcome {
    /// The statement's whole result, for the caller to emit.
    Result(StatementResult),
    /// A streamed `SELECT`: its columns and rows already left through the
    /// emitter as the scan produced them; only its DONE remains.
    Streamed { rows: u64 },
}

thread_local! {
    /// The running statement's read snapshot (Stage 13), when its isolation
    /// is versioned — RCSI's per-statement view. Thread-local rather than
    /// threaded through every read path: a batch executes synchronously on
    /// one worker thread, and every nested read of the statement (subqueries,
    /// views, derived tables, correlated re-evaluation) shares the statement
    /// snapshot by construction.
    static CURRENT_SNAPSHOT: std::cell::Cell<Option<ReadSnapshot>> =
        const { std::cell::Cell::new(None) };
}

/// The running statement's read snapshot, if it reads versioned.
fn current_snapshot() -> Option<ReadSnapshot> {
    CURRENT_SNAPSHOT.get()
}

thread_local! {
    /// The running statement's table variables (the session's, shared read-only
    /// for the statement). Thread-local for the same reason as CURRENT_SNAPSHOT:
    /// a batch runs on one worker thread, and the FROM-source builders carry
    /// only an EvalContext (a truthdb-sql type that cannot hold core `Datum`
    /// rows), so the store cannot ride through it.
    static CURRENT_TABLE_VARS: std::cell::RefCell<
        Option<std::rc::Rc<std::collections::HashMap<String, TableVar>>>,
    > = const { std::cell::RefCell::new(None) };
}

/// The table variable `@name` visible to the running statement, cloned out for a
/// FROM read (an in-memory rowset).
fn current_table_var(name: &str) -> Option<TableVar> {
    let key = name.trim_start_matches('@').to_ascii_lowercase();
    CURRENT_TABLE_VARS.with(|c| c.borrow().as_ref().and_then(|m| m.get(&key).cloned()))
}

/// Installs the statement's table variables for its execution, restoring the
/// prior installation on drop (scopes can nest — a subquery or TVF body reads
/// within the caller's — so restore rather than clear).
struct TableVarScope {
    prev: Option<std::rc::Rc<std::collections::HashMap<String, TableVar>>>,
}

impl TableVarScope {
    fn enter(vars: std::rc::Rc<std::collections::HashMap<String, TableVar>>) -> Self {
        let prev = CURRENT_TABLE_VARS.with(|c| c.borrow_mut().replace(vars));
        TableVarScope { prev }
    }
}

impl Drop for TableVarScope {
    fn drop(&mut self) {
        CURRENT_TABLE_VARS.with(|c| *c.borrow_mut() = self.prev.take());
    }
}

/// Installs `vars` as the table-variable read view for the returned guard's
/// lifetime — the SINGLE arming rule shared by every path that can read a table
/// variable: ordinary statements, IF/WHILE conditions, scalar-function RETURN
/// expressions, and TVF bodies. Armed when `vars` is non-empty OR an outer scope
/// is already armed. The second clause is the correctness hinge: a function,
/// procedure, or TVF body runs with a fresh (empty) table-variable set, and it
/// must SHADOW the caller's view — not inherit it — so its `FROM @t` resolves
/// against its own (empty) locals and errors 1087, never the caller's rows.
/// When neither holds (the common no-table-variable batch) it arms nothing, so
/// the hot path pays only a thread-local read.
fn arm_table_var_view(vars: &std::collections::HashMap<String, TableVar>) -> Option<TableVarScope> {
    let outer_armed = CURRENT_TABLE_VARS.with(|c| c.borrow().is_some());
    (!vars.is_empty() || outer_armed).then(|| TableVarScope::enter(std::rc::Rc::new(vars.clone())))
}

/// The `inserted`/`deleted` pseudo-tables a firing trigger body reads: the new
/// and old row images of the statement that fired it, with the parent table's
/// schema. Rows are in schema order, exactly like a base-table row.
struct TriggerTables {
    schema: Schema,
    inserted: Vec<Vec<Datum>>,
    deleted: Vec<Vec<Datum>>,
}

thread_local! {
    /// The `inserted`/`deleted` view visible to the running trigger body (like
    /// CURRENT_TABLE_VARS for table variables — a batch runs on one thread and
    /// the FROM-source builders carry only an EvalContext).
    static CURRENT_TRIGGER_TABLES: std::cell::RefCell<Option<std::rc::Rc<TriggerTables>>> =
        const { std::cell::RefCell::new(None) };
}

/// The `inserted` or `deleted` pseudo-table rows visible to the running trigger,
/// as a materialized source, if a trigger scope is armed and `name` is one of
/// them. Returns `None` for any other name (falls through to catalog resolution).
fn current_trigger_source(name: &str, qualifier: &str) -> Option<Source> {
    let which = name.to_ascii_lowercase();
    if which != "inserted" && which != "deleted" {
        return None;
    }
    CURRENT_TRIGGER_TABLES.with(|c| {
        let borrow = c.borrow();
        let tables = borrow.as_ref()?;
        let rows = if which == "inserted" {
            tables.inserted.clone()
        } else {
            tables.deleted.clone()
        };
        let count = tables.schema.columns.len();
        let columns = tables
            .schema
            .columns
            .iter()
            .map(|col| ResultColumn {
                name: col.name.clone(),
                column_type: col.column_type,
            })
            .collect();
        let collations = tables
            .schema
            .columns
            .iter()
            .map(|col| col.collation.clone())
            .collect();
        Some(Source {
            columns,
            qualifiers: vec![Some(qualifier.to_string()); count],
            collations,
            rows: SourceRows::Materialized(rows),
        })
    })
}

/// Installs the `inserted`/`deleted` view for a trigger body's execution,
/// restoring the prior installation on drop (a nested trigger's body shadows the
/// outer's — restore rather than clear).
struct TriggerScope {
    prev: Option<std::rc::Rc<TriggerTables>>,
}

impl TriggerScope {
    fn enter(tables: std::rc::Rc<TriggerTables>) -> Self {
        let prev = CURRENT_TRIGGER_TABLES.with(|c| c.borrow_mut().replace(tables));
        TriggerScope { prev }
    }

    /// Clears the `inserted`/`deleted` view for a stored-object body (a
    /// procedure, function, TVF, or view called from within a trigger body):
    /// those pseudo-tables are visible only in the trigger's OWN statements, not
    /// in objects it calls. Restores the prior view on drop. A no-op (cheap) when
    /// no trigger scope is armed.
    fn clear() -> Self {
        let prev = CURRENT_TRIGGER_TABLES.with(|c| c.borrow_mut().take());
        TriggerScope { prev }
    }
}

impl Drop for TriggerScope {
    fn drop(&mut self) {
        CURRENT_TRIGGER_TABLES.with(|c| *c.borrow_mut() = self.prev.take());
    }
}

thread_local! {
    /// The row images captured by the DML that is currently firing triggers, so
    /// exec_insert/update/delete can populate `inserted`/`deleted` without a
    /// signature change. Armed by the firing wrapper ONLY when the target table
    /// has triggers — the common no-trigger path leaves this `None` (no clone).
    static TRIGGER_CAPTURE: std::cell::RefCell<Option<CapturedImages>> =
        const { std::cell::RefCell::new(None) };
}

/// New (`inserted`) and old (`deleted`) row images collected during a DML that
/// has AFTER triggers to fire.
#[derive(Default)]
struct CapturedImages {
    inserted: Vec<Vec<Datum>>,
    deleted: Vec<Vec<Datum>>,
}

/// Records row images into the active capture, if one is armed. `f` builds the
/// (inserted, deleted) images for a statement; it runs only when capture is on,
/// so the no-trigger path pays nothing.
fn capture_trigger_images(f: impl FnOnce() -> (Vec<Vec<Datum>>, Vec<Vec<Datum>>)) {
    TRIGGER_CAPTURE.with(|c| {
        let mut borrow = c.borrow_mut();
        if let Some(images) = borrow.as_mut() {
            let (ins, del) = f();
            images.inserted.extend(ins);
            images.deleted.extend(del);
        }
    });
}

thread_local! {
    /// The object_ids of triggers whose bodies are currently on the stack. With
    /// recursive triggers OFF (the default), a trigger must not re-fire itself
    /// (direct recursion) — a trigger on T whose body DMLs T is suppressed for
    /// that same trigger. Nested triggers on OTHER tables are not affected.
    static FIRING_TRIGGERS: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Statement-scoped snapshot registration: capture on entry, and release —
/// pruning must not wait on a statement that errored — on every exit path.
struct SnapshotScope<'a> {
    storage: &'a Storage,
    seq: u64,
    /// The snapshot that was current when this scope was entered, restored on
    /// exit. Scopes can nest — a scalar function's body statement runs under the
    /// caller's active statement/transaction snapshot — so a nested scope must
    /// restore the caller's snapshot on drop, not erase it.
    prev: Option<ReadSnapshot>,
}

impl<'a> SnapshotScope<'a> {
    fn enter(storage: &'a Storage, own_txn: Option<u64>) -> Self {
        let prev = CURRENT_SNAPSHOT.get();
        let snap = storage.capture_read_snapshot(own_txn);
        CURRENT_SNAPSHOT.set(Some(snap));
        SnapshotScope {
            storage,
            seq: snap.seq,
            prev,
        }
    }
}

impl Drop for SnapshotScope<'_> {
    fn drop(&mut self) {
        CURRENT_SNAPSHOT.set(self.prev);
        self.storage.release_read_snapshot(self.seq);
    }
}

/// Statement-scoped view of a TRANSACTION's snapshot (SNAPSHOT isolation):
/// sets the thread-local for this statement and restores the prior one on exit
/// (see [`SnapshotScope::prev`]), but the registration lives with the
/// transaction, not the statement.
struct TxnSnapshotScope {
    prev: Option<ReadSnapshot>,
}

impl TxnSnapshotScope {
    fn enter(snap: ReadSnapshot) -> Self {
        let prev = CURRENT_SNAPSHOT.get();
        CURRENT_SNAPSHOT.set(Some(snap));
        TxnSnapshotScope { prev }
    }
}

impl Drop for TxnSnapshotScope {
    fn drop(&mut self) {
        CURRENT_SNAPSHOT.set(self.prev);
    }
}

/// Whether a statement touches any base table: DML always does; a SELECT
/// only when its FROM/subqueries name one. `SELECT 1` under SNAPSHOT must
/// neither raise 3952 nor establish the transaction's snapshot — SQL Server
/// defers both to the first read of an actual object.
fn statement_reads_tables(storage: &Storage, statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => select_reads_tables(storage, select),
        // An INSERT whose TARGET is a table variable writes only session memory,
        // so — unlike a base-table INSERT — it is not itself a data access; but a
        // `SELECT` source still reads real tables and must arm the snapshot.
        Statement::Insert(insert) if insert.table.value.starts_with('@') => match &insert.source {
            InsertSource::Select(select) => select_reads_tables(storage, select),
            _ => false,
        },
        _ => true,
    }
}

/// Whether a SELECT reads any real table — directly (FROM/subqueries) or through
/// a scalar function it calls. A `@t` table-variable source is session-local and
/// is not counted (it neither locks nor snapshots).
fn select_reads_tables(storage: &Storage, select: &Select) -> bool {
    let expanded = expand_ctes(select);
    let mut tables = Vec::new();
    collect_locked_tables(&expanded, &mut tables);
    !tables.is_empty() || !select_function_read_ids(storage, &expanded).is_empty()
}

/// SQL Server 3952: SNAPSHOT isolation used while the database does not
/// allow it — raised at data access, not at SET, exactly as SQL Server does.
fn snapshot_not_allowed_error(database: &str) -> SqlError {
    SqlError::new(
        3952,
        16,
        1,
        format!(
            "Snapshot isolation transaction failed accessing database '{database}' because \
             snapshot isolation is not allowed in this database. Use ALTER DATABASE to allow \
             snapshot isolation."
        ),
    )
}

/// Runs one statement. A plain `SELECT` the scan planner accepts streams its
/// rows through `run` as the scan reads them — the whole point of the event
/// stream: the client sees rows while the scan runs, and the statement's peak
/// memory is one chunk, not the result. Everything else executes exactly as
/// before and returns its materialized result.
fn exec_statement_streamed(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    // Versioned reads (Stage 13). RCSI: a SELECT under READ COMMITTED with
    // the option on reads a per-statement snapshot instead of blocking on
    // writers' locks (DML and the reads inside it stay lock-based —
    // conservative versus SQL Server; the write locks subsume what
    // versioning would relax). SNAPSHOT isolation: every data-access
    // statement shares the transaction's snapshot, captured at its first
    // data access; outside a transaction each statement is its own.
    let data_access = matches!(
        statement,
        Statement::Select(_) | Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    );
    let mut _stmt_scope = None;
    let mut _txn_scope = None;
    // Make the running context's table variables visible to this statement's
    // FROM reads. The clone is the statement's read view; INSERT/UPDATE write
    // the real store on TxnContext. Inside a function/procedure body (fresh,
    // empty table variables) this shadows the caller's view with an empty one,
    // so the body cannot read the caller's @t — see arm_table_var_view.
    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
    match txn_ctx.isolation() {
        Isolation::ReadCommitted
            if matches!(statement, Statement::Select(_)) && storage.rcsi_enabled() =>
        {
            // The snapshot is the durable commit prefix, so the session's
            // own just-committed statements must be fsync-durable before
            // capture or the statement would not see them. Rowset-producing
            // SELECTs already flushed in `run_block`; this covers assignment
            // SELECTs (and then no-ops when nothing committed since the
            // last durability point).
            run.flush(storage)?;
            _stmt_scope = Some(SnapshotScope::enter(
                storage,
                txn_ctx.txn.as_ref().map(StorageTxn::txn_id),
            ));
        }
        Isolation::Snapshot if data_access && statement_reads_tables(storage, statement) => {
            if !storage.snapshot_isolation_allowed() {
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return Err(snapshot_not_allowed_error(&txn_ctx.database));
            }
            if txn_ctx.in_txn() {
                if txn_ctx.txn_snapshot.is_none() {
                    // First data access establishes the transaction's view.
                    run.flush(storage)?;
                    let own = txn_ctx.txn.as_ref().map(StorageTxn::txn_id);
                    txn_ctx.txn_snapshot = Some(storage.capture_read_snapshot(own));
                }
                _txn_scope = txn_ctx.txn_snapshot.map(TxnSnapshotScope::enter);
            } else {
                // Autocommit: the statement is its own transaction, so its
                // snapshot is statement-scoped, like RCSI's.
                run.flush(storage)?;
                _stmt_scope = Some(SnapshotScope::enter(storage, None));
            }
        }
        _ => {}
    }
    exec_statement_streamed_inner(storage, statement, txn_ctx, run)
}

fn exec_statement_streamed_inner(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    if let Statement::RaiseError(raise) = statement {
        return exec_raiserror(raise, txn_ctx, run);
    }
    // The streamed shape: a plain SELECT — no SHOWPLAN (its rows are the plan's,
    // not the table's), no assignment (routed to exec_select_assign) — that
    // `scan_plan` accepts. A doomed transaction still allows reads, so the gate
    // needs no doomed check for a SELECT.
    if let Statement::Select(select) = statement
        && !txn_ctx.showplan_text
        && !select
            .items
            .iter()
            .any(|i| matches!(i, SelectItem::Assign { .. }))
    {
        let eval_ctx = txn_ctx.eval_context();
        if let Some(plan) = scan_plan(storage, select, &eval_ctx) {
            let rows = scan_select_streamed(storage, &plan, select, &eval_ctx, run)?;
            return Ok(StatementOutcome::Streamed { rows });
        }
    }
    exec_statement(storage, statement, txn_ctx).map(StatementOutcome::Result)
}

/// Whether a statement can make a durable commit that the batch must fsync: any
/// write/DDL (its own autocommit, or an identity reservation's mini-commit even
/// inside a transaction) or a `COMMIT`. Conservative by design — it flags by
/// kind, not by transaction state, so a hidden mini-commit (e.g. identity) is
/// never missed. Reads, `BEGIN`, `ROLLBACK`, `SET` and `DECLARE` never commit.
fn statement_may_commit(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::Exec(_)
            | Statement::Block { .. }
            | Statement::If { .. }
            | Statement::While { .. }
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::Commit { .. }
    )
}

/// The table/database locks a batch needs, from its statements and the
/// session isolation level, deduped to the strongest mode per resource. The
/// engine acquires these up front (before running any statement) so a
/// conflicting batch can be parked and restarted cleanly.
///
/// A parse error yields no locks — execution re-parses and surfaces it.
/// `sys.*` views and unresolved tables take no lock (catalog reads are
/// unlocked; missing tables error at execution).
/// Object ids of the parent tables a table's foreign keys reference.
fn fk_parent_object_ids(storage: &Storage, def: &TableDef) -> Vec<u32> {
    def.foreign_keys
        .iter()
        .filter_map(|fk| resolve_table(storage, &fk.parent).map(|p| p.object_id))
        .collect()
}

/// Object ids of the tables whose foreign keys reference `parent_name`.
fn fk_child_object_ids(storage: &Storage, parent_name: &str) -> Vec<u32> {
    storage
        .rel_tables()
        .into_iter()
        .filter(|t| {
            t.foreign_keys
                .iter()
                .any(|fk| fk.parent.eq_ignore_ascii_case(parent_name))
        })
        .map(|t| t.object_id)
        .collect()
}

/// True if any table has a foreign key referencing `name` — i.e. `name` is an
/// FK parent. Such a table keeps table-granular write locks so an FK
/// existence-read (Table IS on the parent) still serializes against a
/// concurrent change to the referenced row.
fn is_fk_parent(storage: &Storage, name: &str) -> bool {
    !fk_child_object_ids(storage, name).is_empty()
}

/// Above this many row-lock keys for one statement, `analyze_locks` escalates to
/// a single table lock (SQL Server-style lock escalation) rather than flooding
/// the lock table.
const ROW_LOCK_ESCALATION_THRESHOLD: usize = 1000;

/// A key hash for the [`Resource::Row`] lock, from the row's clustered-key bytes.
fn row_key_hash(schema: &Schema, key_columns: &[usize], key_values: &[Datum]) -> Option<u64> {
    let bytes = crate::relstore::key::encode_key(schema, key_columns, key_values).ok()?;
    Some(xxh64(&bytes, 0))
}

/// True if the clustered key can be safely hashed for a row lock: no key column
/// is a floating type. REAL/FLOAT keys are excluded because `-0.0 == 0.0` (and
/// NaN) compare equal in evaluation but encode to distinct key bytes, so two
/// writers to one physical row could get distinct hashes and not serialize.
///
/// Character keys are safe even under a case-insensitive collation: the row-lock
/// hash is taken over the *folded* key (`encode_key` folds character keys by
/// collation, Stage 5), so `WHERE key = 'ABC'` and a concurrent write of `'abc'`
/// hash to the same row resource and serialize.
fn key_columns_row_lockable(schema: &Schema, key_columns: &[usize]) -> bool {
    key_columns.iter().all(|&i| {
        !matches!(
            schema.columns[i].column_type,
            ColumnType::Real | ColumnType::Float
        )
    })
}

/// True if a literal may pin a key column for a row lock: the executor's
/// equality must be a direct same-domain match so the lock key equals the stored
/// row's key. The hazard is a **character** key compared to a non-string literal:
/// the executor coerces the stored string to the literal's number (many strings
/// → one number: `'05' = 5`), while the lock key coerces the number to one
/// canonical string — opposite directions that disagree. So a character key
/// column requires a string literal; other domains coerce unambiguously (or
/// `sql_to_datum` errors and the caller falls back).
fn literal_pins_key(value: &SqlValue, column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::VarChar { .. } | ColumnType::NVarChar { .. } => {
            matches!(value, SqlValue::Str(_))
        }
        _ => true,
    }
}

/// True if the table has a secondary UNIQUE index. Such a table keeps
/// table-granular locks for INSERT/UPDATE: a Row X on the clustered key alone
/// would not serialize two writers colliding on the *unique index* key.
fn has_secondary_unique_index(def: &TableDef) -> bool {
    def.indexes.iter().any(|ix| ix.unique)
}

/// Evaluates a constant literal expression (`5`, `'x'`, `-3`, NULL, …) to a
/// value. Returns `None` for anything that is not a self-contained literal —
/// a column reference, variable, function call, or subquery — so the caller
/// falls back to a coarser (table) lock rather than a wrong row key.
fn eval_literal_const(expr: &Expr) -> Option<SqlValue> {
    if !is_literal_const(expr) {
        return None;
    }
    let empty: Vec<String> = Vec::new();
    eval::eval(expr, &[], &empty, &EvalContext::default()).ok()
}

/// True if `expr` is a self-contained literal (no columns/vars/functions/
/// subqueries): a literal, or a unary +/- over one.
fn is_literal_const(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Null
        | ExprKind::Literal(_) => true,
        ExprKind::Unary { expr: inner, .. } => is_literal_const(inner),
        _ => false,
    }
}

/// True if `expr` contains any subquery node (scalar, EXISTS, or IN (SELECT)).
fn expr_has_subquery(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => true,
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => false,
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => expr_has_subquery(e),
        ExprKind::Binary { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        ExprKind::Like {
            expr: e, pattern, ..
        } => expr_has_subquery(e) || expr_has_subquery(pattern),
        ExprKind::InList { expr: e, list, .. } => {
            expr_has_subquery(e) || list.iter().any(expr_has_subquery)
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => expr_has_subquery(e) || expr_has_subquery(low) || expr_has_subquery(high),
        ExprKind::Function { args, .. } => args.iter().any(expr_has_subquery),
        ExprKind::Aggregate { arg, .. } => arg.as_deref().is_some_and(expr_has_subquery),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(expr_has_subquery)
                || branches
                    .iter()
                    .any(|(w, r)| expr_has_subquery(w) || expr_has_subquery(r))
                || else_result.as_deref().is_some_and(expr_has_subquery)
        }
    }
}

/// The row-lock keys for an INSERT: `Some(hashes)` when the target is a
/// clustered table and every row supplies all key columns as constant literals
/// (so two concurrent inserters of *different* keys need not serialize).
/// `None` — fall back to a table lock — for a heap, an IDENTITY/defaulted key
/// (value is server-generated, unknown here), `INSERT ... SELECT`, a
/// non-constant key expression, or more keys than the escalation threshold.
fn insert_row_key_hashes(def: &TableDef, insert: &Insert) -> Option<Vec<u64>> {
    if def.key_columns.is_empty() {
        return None;
    }
    let InsertSource::Values(value_rows) = &insert.source else {
        return None;
    };
    let schema = def.schema().ok()?;
    if !key_columns_row_lockable(&schema, &def.key_columns) {
        return None;
    }
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    // Column index for each value position (explicit list, else all non-identity
    // columns in order — matching `exec_insert`).
    let target: Vec<usize> = match &insert.columns {
        Some(names) => names
            .iter()
            .map(|n| column_index(&schema, &n.value))
            .collect::<Option<Vec<_>>>()?,
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };
    let mut hashes = Vec::with_capacity(value_rows.len());
    for row in value_rows {
        if row.len() != target.len() {
            return None; // arity mismatch — executor will error; table-lock it
        }
        let mut key_values = vec![Datum::Null; ncols];
        for &kc in &def.key_columns {
            if Some(kc) == identity_col {
                return None; // server-generated key value
            }
            let pos = target.iter().position(|&t| t == kc)?; // key not supplied
            let value = eval_literal_const(&row[pos])?;
            let column = &schema.columns[kc];
            if !literal_pins_key(&value, &column.column_type) {
                return None;
            }
            key_values[kc] = value::sql_to_datum(&value, &column.column_type, &column.name).ok()?;
        }
        hashes.push(row_key_hash(&schema, &def.key_columns, &key_values)?);
        if hashes.len() > ROW_LOCK_ESCALATION_THRESHOLD {
            return None;
        }
    }
    Some(hashes)
}

/// The single row-lock key for a point UPDATE/DELETE: `Some(hash)` when the
/// WHERE clause is a subquery-free conjunction that pins *every* clustered-key
/// column to a constant literal. `None` — fall back to a table lock — otherwise
/// (heap, partial/absent key predicate, range/OR/subquery predicate).
fn where_point_key_hash(def: &TableDef, where_clause: &Option<Expr>) -> Option<u64> {
    if def.key_columns.is_empty() {
        return None;
    }
    let where_clause = where_clause.as_ref()?;
    if expr_has_subquery(where_clause) {
        return None;
    }
    let schema = def.schema().ok()?;
    if !key_columns_row_lockable(&schema, &def.key_columns) {
        return None;
    }
    let mut conjuncts = Vec::new();
    flatten_and(where_clause, &mut conjuncts);
    let mut key_values = vec![Datum::Null; schema.columns.len()];
    let mut bound: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for conjunct in conjuncts {
        let ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left,
            right,
        } = &conjunct.kind
        else {
            continue;
        };
        let (name, value_expr) = match (&left.kind, &right.kind) {
            (ExprKind::Column(n), _) => (n, right.as_ref()),
            (_, ExprKind::Column(n)) => (n, left.as_ref()),
            _ => continue,
        };
        let Some(ci) = column_index(&schema, &name.value) else {
            continue;
        };
        if !def.key_columns.contains(&ci) {
            continue;
        }
        let Some(value) = eval_literal_const(value_expr) else {
            continue;
        };
        let column = &schema.columns[ci];
        if !literal_pins_key(&value, &column.column_type) {
            continue;
        }
        if let Ok(datum) = value::sql_to_datum(&value, &column.column_type, &column.name) {
            key_values[ci] = datum;
            bound.insert(ci);
        }
    }
    if def.key_columns.iter().any(|kc| !bound.contains(kc)) {
        return None; // not every key column pinned
    }
    row_key_hash(&schema, &def.key_columns, &key_values)
}

/// The row-lock key for a point UPDATE: as [`where_point_key_hash`], but only
/// when no assignment targets a key column (a key change moves the row, touching
/// two keys) and no assignment value contains a subquery (which would read rows
/// the single row lock does not cover).
fn update_row_key_hash(def: &TableDef, update: &Update) -> Option<u64> {
    let schema = def.schema().ok()?;
    for assignment in &update.assignments {
        let ci = column_index(&schema, &assignment.column.value)?;
        if def.key_columns.contains(&ci) || expr_has_subquery(&assignment.value) {
            return None;
        }
    }
    where_point_key_hash(def, &update.where_clause)
}

pub fn analyze_locks(
    storage: &Storage,
    sql: &str,
    isolation: Isolation,
) -> Vec<(Resource, LockMode)> {
    let Ok(parsed) = truthdb_sql::parse(sql) else {
        return Vec::new();
    };
    // The visited set terminates recursive procedures. Keyed on (procedure,
    // effective analysis regime), NOT the name alone: a body's lock
    // contribution is ISOLATION-DEPENDENT (versioned RC contributes Database
    // IS; an escalated re-entry needs Table S), so a body re-entered under a
    // different regime must re-analyze — the review's HIGH showed a shared
    // body analyzed versioned first and then skipped under SERIALIZABLE,
    // executing with no Table S. The regime lattice is finite, so
    // termination survives.
    let mut visited = std::collections::HashSet::new();
    let mut trigger_visited = std::collections::HashSet::new();
    analyze_statements_locks(
        storage,
        &parsed,
        isolation,
        &mut visited,
        &mut trigger_visited,
    )
}

fn analyze_statements_locks(
    storage: &Storage,
    parsed: &[Statement],
    isolation: Isolation,
    visited: &mut std::collections::HashSet<(String, Isolation)>,
    trigger_visited: &mut std::collections::HashSet<(u32, Isolation)>,
) -> Vec<(Resource, LockMode)> {
    // Flatten TRY/CATCH so the locks a batch needs are pre-acquired for the
    // statements inside its try/catch blocks too, not just the top level.
    let mut statements: Vec<&Statement> = Vec::new();
    flatten_statements(parsed, &mut statements);
    // Reads take shared locks except under READ UNCOMMITTED, which takes none.
    // A batch that raises the isolation level (e.g. `SET ISOLATION LEVEL
    // SERIALIZABLE; SELECT ...`) must lock its reads even if the session was
    // READ UNCOMMITTED on entry — otherwise the post-SET read would run
    // unlocked. We therefore take read locks unless the whole batch is READ
    // UNCOMMITTED: the session is RU and no SET raises it above RU.
    // SNAPSHOT is a versioned level, not a raise: a SET to it must not force
    // lock-based analysis (its whole point is to read without Table S).
    let escalates_reads = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(level))
                if !matches!(level, IsolationLevel::ReadUncommitted | IsolationLevel::Snapshot)
        )
    });
    // A batch that SETs SNAPSHOT mid-stream still read-locks (statements
    // before the SET run at the session level, and batch analysis cannot see
    // the boundary) — but it must at least hold the Database IS fence, so an
    // RU session's `SET SNAPSHOT; SELECT` is not entirely lock-free.
    let sets_snapshot = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(IsolationLevel::Snapshot))
        )
    });
    let reads_lock =
        !matches!(isolation, Isolation::ReadUncommitted) || escalates_reads || sets_snapshot;
    // Versioned reads take Database IS only — the DDL fence for the batch's
    // duration — and no Table S: READ COMMITTED under RCSI (per-statement
    // snapshots) and SNAPSHOT isolation (the transaction's snapshot). A batch
    // whose SET raises the level is analyzed lock-based (conservative: the
    // raise is seen here, the exact statement boundary is not).
    let versioned_reads = !escalates_reads
        && (matches!(isolation, Isolation::Snapshot)
            || (matches!(isolation, Isolation::ReadCommitted) && storage.rcsi_enabled()));
    // The isolation a fired trigger body (and any EXEC it makes) must be analyzed
    // under: an in-line SET that raises the level locks the body's reads too, so
    // forward a lock-based level whenever this batch locks reads — the SAME
    // correction the EXEC path applies. Without it a trigger body under a
    // versioned session (Snapshot / RCSI) would recompute versioned_reads=true
    // and drop the Table S it actually reads lock-based at runtime (a dirty read,
    // the Stage-13 seam class).
    let nested_isolation = if reads_lock {
        if matches!(isolation, Isolation::ReadCommitted | Isolation::Snapshot) && !escalates_reads {
            isolation
        } else {
            Isolation::RepeatableRead
        }
    } else {
        isolation
    };
    let mut needs: std::collections::HashMap<Resource, LockMode> = std::collections::HashMap::new();
    let mut add = |resource: Resource, mode: LockMode| {
        needs
            .entry(resource)
            .and_modify(|m| *m = m.combine(mode))
            .or_insert(mode);
    };
    for statement in statements.iter().copied() {
        match statement {
            Statement::Select(select) => {
                if !reads_lock {
                    continue;
                }
                // Lock every base table the query reads — the FROM clause AND
                // any subqueries in its expressions (WHERE/SELECT/HAVING/...).
                // CTEs are inlined first so their base tables are counted.
                let expanded = expand_ctes(select);
                let mut tables = Vec::new();
                collect_locked_tables(&expanded, &mut tables);
                for name in tables {
                    for oid in read_lock_object_ids(storage, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        if !versioned_reads {
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                }
                // A scalar function the query calls reads tables through its
                // body; lock those up front too (2PL), or the body would read
                // with no lock held. read_lock_object_ids recurses the body.
                for oid in select_function_read_ids(storage, &expanded) {
                    add(Resource::Database, LockMode::IntentShared);
                    if !versioned_reads {
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            Statement::Insert(insert) => {
                if let Some(def) = resolve_table(storage, &insert.table.value) {
                    // Row X locks on each inserted key (two inserters of
                    // different keys then run concurrently under Table IX); a
                    // heap / IDENTITY / non-literal key falls back to Table X.
                    // A table referenced as an FK parent keeps Table X so an FK
                    // existence-read (Table IS) still serializes against it; a
                    // secondary UNIQUE index likewise needs table-granular
                    // serialization (the PK Row X does not cover its key).
                    let hashes =
                        if is_fk_parent(storage, &def.name) || has_secondary_unique_index(&def) {
                            None
                        } else {
                            insert_row_key_hashes(&def, insert)
                        };
                    match hashes {
                        Some(hashes) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            for hash in hashes {
                                add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                            }
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // A child INSERT reads its FK parents (integrity read).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    // A firing AFTER-INSERT trigger's body reads/writes further
                    // tables; hold those locks up front too (strict 2PL).
                    add_trigger_locks(
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Insert,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
                // INSERT ... SELECT also reads its source tables (and any
                // subqueries in the SELECT); lock them like a SELECT so it
                // cannot read another txn's uncommitted rows (they combine to
                // SIX on the target if it is a source).
                if reads_lock && let InsertSource::Select(select) = &insert.source {
                    let expanded = expand_ctes(select);
                    let mut tables = Vec::new();
                    collect_locked_tables(&expanded, &mut tables);
                    for name in tables {
                        for oid in read_lock_object_ids(storage, &name.value) {
                            add(Resource::Database, LockMode::IntentShared);
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                    for oid in select_function_read_ids(storage, &expanded) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            Statement::Update(update) => {
                if let Some(def) = resolve_table(storage, &update.table.value) {
                    // A point UPDATE (WHERE pins the whole key, no key-column
                    // write, no subquery) takes Table IX + a single Row X. An FK
                    // parent or a secondary UNIQUE index keeps Table X (see INSERT).
                    let hash =
                        if is_fk_parent(storage, &def.name) || has_secondary_unique_index(&def) {
                            None
                        } else {
                            update_row_key_hash(&def, update)
                        };
                    match hash {
                        Some(hash) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // UPDATE reads FK parents (new values) and referencing
                    // children (a changed PK must not orphan them).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    for oid in fk_child_object_ids(storage, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    add_trigger_locks(
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Update,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
            }
            Statement::Delete(delete) => {
                if let Some(def) = resolve_table(storage, &delete.table.value) {
                    // A point DELETE (WHERE pins the whole key, no subquery)
                    // takes Table IX + a single Row X. A table referenced as an
                    // FK parent keeps Table X (see INSERT).
                    let hash = if is_fk_parent(storage, &def.name) {
                        None
                    } else {
                        where_point_key_hash(&def, &delete.where_clause)
                    };
                    match hash {
                        Some(hash) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // DELETE reads referencing children (NO ACTION check).
                    for oid in fk_child_object_ids(storage, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    add_trigger_locks(
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Delete,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
            }
            // DDL serializes against every active transaction via a
            // database-exclusive lock (it is disallowed inside a txn anyway).
            Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            // ALTER DATABASE quiesces the database: no snapshot may be live
            // and no writer mid-transaction while the options flip.
            | Statement::AlterDatabase(_) => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // EXEC sp_executesql with a LITERAL statement is analyzable up
            // front: recurse into the inner text. Anything else (a variable
            // statement, an unknown procedure) cannot be analyzed before it
            // runs — lock the database exclusively rather than under-lock
            // (2PL acquires the full set up front).
            Statement::Exec(exec) => {
                // A user procedure: its stored body analyzes like literal
                // inner text, parsed with the IN-PROCEDURE grammar — a plain
                // parse would reject `RETURN <value>` (178), yield no locks,
                // and the body would run UNLOCKED (the 2PL hole class).
                if let Some(def) = resolve_table(storage, &exec.proc.value)
                    && let Some(procedure) = &def.procedure
                {
                    let inner_isolation = if reads_lock {
                        if matches!(isolation, Isolation::ReadCommitted | Isolation::Snapshot)
                            && !escalates_reads
                        {
                            isolation
                        } else {
                            Isolation::RepeatableRead
                        }
                    } else {
                        isolation
                    };
                    if visited.insert((def.name.clone(), inner_isolation))
                        && let Ok(body) = truthdb_sql::parse_procedure_body(&procedure.body)
                    {
                        for (resource, mode) in analyze_statements_locks(
                            storage,
                            &body,
                            inner_isolation,
                            visited,
                            trigger_visited,
                        ) {
                            add(resource, mode);
                        }
                    }
                    continue;
                }
                match exec_literal_sql(exec) {
                Some(inner) => {
                    // The inner text runs under the batch's EFFECTIVE
                    // isolation: a `SET ... SERIALIZABLE` before the EXEC
                    // must lock the inner reads too, so the recursion gets a
                    // read-locking level whenever this batch locks reads.
                    // (An inner SET raising isolation is seen by the
                    // recursion's own scan; it cannot outlive the EXEC — SET
                    // options revert at scope exit.)
                    //
                    // That level must be one the versioned-read path can
                    // never claim: under RCSI the recursion's own
                    // `versioned_reads` would drop Table S for a plain
                    // READ COMMITTED, while at runtime the inner statement
                    // executes under the OUTER effective level and reads
                    // lock-based — a reachable dirty read at SERIALIZABLE
                    // (caught by the adversarial review). READ COMMITTED is
                    // passed only when it truly is the effective level.
                    let inner_isolation = if reads_lock {
                        if matches!(
                            isolation,
                            Isolation::ReadCommitted | Isolation::Snapshot
                        ) && !escalates_reads
                        {
                            // Both survive the recursion faithfully: the
                            // inner analysis reaches the same versioned/
                            // lock-based decision execution will.
                            isolation
                        } else {
                            Isolation::RepeatableRead
                        }
                    } else {
                        isolation
                    };
                    if let Ok(parsed) = truthdb_sql::parse(&inner) {
                        for (resource, mode) in analyze_statements_locks(
                            storage,
                            &parsed,
                            inner_isolation,
                            visited,
                            trigger_visited,
                        ) {
                            add(resource, mode);
                        }
                    }
                }
                None => add(Resource::Database, LockMode::Exclusive),
                }
            }
            // Procedure DDL rewrites the catalog: Database X, like other DDL.
            Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_) => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // IF/WHILE conditions read tables through their subqueries —
            // locked exactly like a SELECT's tables (their bodies were
            // flattened into this list and analyze as themselves).
            Statement::If { condition, .. } | Statement::While { condition, .. } => {
                if !reads_lock {
                    continue;
                }
                let mut tables = Vec::new();
                collect_expr_tables(condition, &mut tables);
                for name in tables {
                    for oid in read_lock_object_ids(storage, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        if !versioned_reads {
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                }
                for oid in expr_function_read_ids(storage, condition) {
                    add(Resource::Database, LockMode::IntentShared);
                    if !versioned_reads {
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            // Transaction control, SET, and DECLARE take no data locks.
            // TRY/CATCH and plain blocks were flattened away by
            // `flatten_statements`, so their contained statements appear here
            // directly; BREAK/CONTINUE/RETURN touch nothing.
            Statement::Block { .. }
            | Statement::Break { .. }
            | Statement::Continue { .. }
            | Statement::Return { .. }
            | Statement::BeginTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::SaveTransaction { .. }
            | Statement::Set(_)
            | Statement::Declare(_)
            | Statement::DeclareTableVar { .. }
            | Statement::Use { .. }
            | Statement::Throw(_)
            | Statement::RaiseError(_)
            | Statement::TryCatch { .. }
            // BACKUP takes no batch lock: it is online and manages its own
            // per-chunk storage locking. A Database X here would serialize it
            // against every writer and defeat the fuzzy design.
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. } => {}
        }
    }
    // Batch-level lock escalation: if a table accumulated more than the
    // threshold of row locks across the whole batch (many literal-key INSERTs,
    // a loop, or several point statements), replace them all with one Table X.
    // Bounds the lock set a batch can request regardless of per-statement caps.
    let mut row_counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for resource in needs.keys() {
        if let Resource::Row(oid, _) = resource {
            *row_counts.entry(*oid).or_default() += 1;
        }
    }
    let escalate: std::collections::HashSet<u32> = row_counts
        .into_iter()
        .filter(|(_, count)| *count > ROW_LOCK_ESCALATION_THRESHOLD)
        .map(|(oid, _)| oid)
        .collect();
    if !escalate.is_empty() {
        needs.retain(
            |resource, _| !matches!(resource, Resource::Row(oid, _) if escalate.contains(oid)),
        );
        for oid in escalate {
            needs
                .entry(Resource::Table(oid))
                .and_modify(|m| *m = m.combine(LockMode::Exclusive))
                .or_insert(LockMode::Exclusive);
            needs
                .entry(Resource::Database)
                .and_modify(|m| *m = m.combine(LockMode::IntentExclusive))
                .or_insert(LockMode::IntentExclusive);
        }
    }
    needs.into_iter().collect()
}

/// Parses and executes a SQL batch, returning one result per statement, or
/// the first error (discarding earlier results). Kept for tests; the server
/// uses [`execute_batch`].
#[cfg(test)]
pub fn execute(storage: &Storage, sql: &str) -> Result<Vec<StatementResult>, SqlError> {
    let mut txn_ctx = TxnContext::default();
    let outcome = execute_batch(storage, sql, &mut txn_ctx);
    match outcome.error {
        Some(error) => Err(error),
        None => Ok(outcome.results),
    }
}

impl TxnContext {
    fn scope(&mut self) -> TxnScope<'_> {
        match &mut self.txn {
            Some(txn) => TxnScope::Explicit(txn),
            None => TxnScope::Auto,
        }
    }
}

fn exec_statement(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // A doomed (uncommittable) transaction rejects log writes with 3930, but —
    // like SQL Server — still allows reads (`SELECT`), `SET`, `DECLARE`, and a
    // full `ROLLBACK`, so a CATCH block can inspect `XACT_STATE()`/`ERROR_*()`
    // and then roll back. A partial rollback to a savepoint and `SAVE` stay
    // rejected (an uncommittable transaction can only be fully rolled back).
    if txn_ctx.doomed && !doomed_allows(statement) {
        return Err(SqlError::new(
            3930,
            16,
            1,
            "The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.",
        ));
    }
    let result = exec_statement_dispatch(storage, statement, txn_ctx);
    // SQL Server rolls a SNAPSHOT transaction back entirely on an update
    // conflict — "transaction aborted", not statement-failed-transaction-
    // doomed. @@TRANCOUNT drops to zero and the session continues.
    if let Err(error) = &result
        && error.number == 3960
        && txn_ctx.in_txn()
    {
        txn_ctx.abort(storage);
    }
    result
}

fn exec_statement_dispatch(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // DDL (schema + security) requires a privileged principal (sysadmin / dbo /
    // db_owner / the internal channel). A restricted database user is refused
    // before any change is made.
    if !txn_ctx.security.bypass && is_privileged_ddl(statement) {
        return Err(SqlError::new(
            15247,
            16,
            1,
            "User does not have permission to perform this action.".to_string(),
        ));
    }
    match statement {
        Statement::BeginTransaction { .. } => exec_begin(storage, txn_ctx),
        Statement::Use { database, .. } => exec_use(database, txn_ctx),
        Statement::Throw(throw) => Err(exec_throw(throw, txn_ctx)),
        Statement::CreateProcedure(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_procedure(storage, create)
        }
        Statement::DropProcedure {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_procedure(storage, name, *if_exists)
        }
        Statement::CreateFunction(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_function(storage, create)
        }
        Statement::DropFunction {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_function(storage, name, *if_exists)
        }
        Statement::CreateTrigger(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_trigger(storage, create)
        }
        Statement::DropTrigger {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_trigger(storage, name, *if_exists)
        }
        Statement::CreateLogin(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_login(storage, create)
        }
        Statement::DropLogin {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_login(storage, name, *if_exists)
        }
        Statement::CreateUser(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_user(storage, create)
        }
        Statement::DropUser {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_database_principal(storage, name, *if_exists, false)
        }
        Statement::CreateRole { name, .. } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_role(storage, name)
        }
        Statement::DropRole {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_database_principal(storage, name, *if_exists, true)
        }
        Statement::AlterRole {
            name,
            action,
            member,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_alter_role_member(storage, name, *action, member)
        }
        Statement::Permission(stmt) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_permission(storage, stmt, &txn_ctx.security)
        }
        Statement::BackupDatabase {
            path,
            checksum,
            copy_only,
            ..
        } => {
            // BACKUP manages its own (per-chunk) locking, so it cannot run
            // inside a transaction that holds locks, and it is a privileged
            // operation (gated by is_privileged_ddl above).
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    3021,
                    16,
                    1,
                    "Cannot perform a backup or restore operation within a transaction."
                        .to_string(),
                ));
            }
            storage
                .backup_full_with(std::path::Path::new(path), *checksum, *copy_only)
                .map_err(|e| {
                    SqlError::new(
                        3013,
                        16,
                        1,
                        format!("BACKUP DATABASE is terminating abnormally. {e}"),
                    )
                })?;
            Ok(StatementResult::Done)
        }
        Statement::BackupLog {
            path,
            checksum,
            copy_only,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    3021,
                    16,
                    1,
                    "Cannot perform a backup or restore operation within a transaction."
                        .to_string(),
                ));
            }
            if !storage.recovery_model_full() {
                return Err(SqlError::new(
                    4208,
                    16,
                    1,
                    "The statement BACKUP LOG is not allowed while the recovery model is SIMPLE. \
                     Use BACKUP DATABASE or change the recovery model to FULL with ALTER DATABASE."
                        .to_string(),
                ));
            }
            storage
                .backup_log(std::path::Path::new(path), *checksum, *copy_only)
                .map_err(|e| {
                    SqlError::new(
                        3013,
                        16,
                        1,
                        format!("BACKUP LOG is terminating abnormally. {e}"),
                    )
                })?;
            Ok(StatementResult::Done)
        }
        // Executed by `run_block`'s own arms; nothing routes them here.
        Statement::Block { .. }
        | Statement::If { .. }
        | Statement::While { .. }
        | Statement::Break { .. }
        | Statement::Continue { .. }
        | Statement::Return { .. } => {
            unreachable!("control flow is executed by run_block")
        }
        // Handled in `exec_statement_streamed_inner` (severity <= 10 emits an
        // INFO event, which needs the emitter); nothing else routes it here.
        Statement::RaiseError(_) => unreachable!("RAISERROR reaches only the streaming executor"),
        Statement::Commit { .. } => exec_commit(storage, txn_ctx),
        Statement::Rollback { name, .. } => exec_rollback(storage, txn_ctx, name.as_ref()),
        Statement::SaveTransaction { name, .. } => exec_save(storage, txn_ctx, name),
        Statement::Set(set) => exec_set(txn_ctx, set),
        Statement::Declare(decls) => exec_declare(txn_ctx, decls),
        Statement::DeclareTableVar {
            name,
            columns,
            primary_key,
            ..
        } => exec_declare_table_var(txn_ctx, name, columns, primary_key),
        Statement::CreateTable(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_table(storage, create)
        }
        Statement::DropTable(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_table(storage, drop)
        }
        Statement::CreateView(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_view(storage, create)
        }
        Statement::DropView(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_view(storage, drop)
        }
        Statement::CreateIndex(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_index(storage, create)
        }
        Statement::DropIndex(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_index(storage, drop)
        }
        Statement::AlterTable(alter) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            let eval_ctx = txn_ctx.eval_context();
            exec_alter_table(storage, alter, &eval_ctx)
        }
        Statement::AlterDatabase(alter) => {
            if txn_ctx.in_txn() {
                // SQL Server 226: ALTER DATABASE is not allowed inside a
                // multi-statement transaction.
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "ALTER DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_alter_database(storage, alter, txn_ctx)
        }
        Statement::Insert(insert) => {
            // INSERT into a `@t` table variable is pure session memory (no
            // Storage, no lock, no WAL) — handled here where `&mut TxnContext`
            // is in hand, before the storage scope is taken.
            if insert.table.value.starts_with('@') {
                let eval_ctx = txn_ctx.eval_context();
                return exec_insert_table_var(storage, insert, txn_ctx, &eval_ctx);
            }
            let (target, triggers) =
                after_triggers_for(storage, &insert.table.value, catalog::TriggerEvent::Insert);
            let run_insert = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let (result, identity) = {
                    let mut scope = txn_ctx.scope();
                    exec_insert(storage, insert, &mut scope, &eval_ctx)?
                };
                // An identity INSERT updates SCOPE_IDENTITY(); a non-identity one
                // (identity == None) leaves it unchanged.
                if let Some(value) = identity {
                    txn_ctx.scope_identity = Some(value);
                }
                Ok(result)
            };
            match target {
                Some(target) if !triggers.is_empty() => {
                    run_dml_with_triggers(storage, txn_ctx, &target, triggers, run_insert)
                }
                _ => run_insert(txn_ctx),
            }
        }
        Statement::Update(update) => {
            let (target, triggers) =
                after_triggers_for(storage, &update.table.value, catalog::TriggerEvent::Update);
            let run_update = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let mut scope = txn_ctx.scope();
                exec_update(storage, update, &mut scope, &eval_ctx)
            };
            match target {
                Some(target) if !triggers.is_empty() => {
                    run_dml_with_triggers(storage, txn_ctx, &target, triggers, run_update)
                }
                _ => run_update(txn_ctx),
            }
        }
        Statement::Delete(delete) => {
            let (target, triggers) =
                after_triggers_for(storage, &delete.table.value, catalog::TriggerEvent::Delete);
            let run_delete = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let mut scope = txn_ctx.scope();
                exec_delete(storage, delete, &mut scope, &eval_ctx)
            };
            match target {
                Some(target) if !triggers.is_empty() => {
                    run_dml_with_triggers(storage, txn_ctx, &target, triggers, run_delete)
                }
                _ => run_delete(txn_ctx),
            }
        }
        Statement::Select(select) => {
            if select
                .items
                .iter()
                .any(|i| matches!(i, SelectItem::Assign { .. }))
            {
                return exec_select_assign(storage, select, txn_ctx);
            }
            let eval_ctx = txn_ctx.eval_context();
            if txn_ctx.showplan_text {
                Ok(StatementResult::Rows(showplan_rows(
                    storage, select, &eval_ctx,
                )?))
            } else {
                Ok(StatementResult::Rows(exec_select(
                    storage, select, &eval_ctx,
                )?))
            }
        }
        // TRY/CATCH is control flow, handled by `run_block`, which never routes
        // it here.
        Statement::TryCatch { .. } => Err(SqlError::message_only(
            0,
            "internal error: TRY/CATCH must be executed by run_block",
        )),
        // EXEC recurses into its inner batch, handled by `run_block` too.
        Statement::Exec(_) => Err(SqlError::message_only(
            0,
            "internal error: EXEC must be executed by run_block",
        )),
    }
}

/// Statements a doomed (uncommittable) transaction still permits: reads
/// (`SELECT`, including `SELECT @v = ...`), session-state changes (`SET`,
/// `DECLARE`), and a full `ROLLBACK`. Everything else (DML/DDL, `COMMIT`,
/// `SAVE`, a partial `ROLLBACK` to a savepoint) writes to the log and is
/// rejected with 3930.
fn doomed_allows(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Select(_)
            | Statement::Set(_)
            | Statement::Declare(_)
            | Statement::Use { .. }
            | Statement::Throw(_)
            | Statement::RaiseError(_)
            | Statement::Rollback { name: None, .. }
    )
}

/// Flattens `TRY`/`CATCH` blocks into the leaf statements they contain, so lock
/// analysis (which pre-acquires every table lock a batch needs) sees the
/// statements nested inside try/catch blocks too.
fn flatten_statements<'a>(statements: &'a [Statement], out: &mut Vec<&'a Statement>) {
    for statement in statements {
        match statement {
            Statement::TryCatch {
                try_block,
                catch_block,
                ..
            } => {
                flatten_statements(try_block, out);
                flatten_statements(catch_block, out);
            }
            Statement::Block { body, .. } => flatten_statements(body, out),
            // IF/WHILE stay in the list (their CONDITIONS take read locks);
            // their bodies flatten so the leaf statements analyze as
            // themselves — a WHILE body's INSERT needs its lock up front like
            // any other, and both IF branches are analyzed (conservative:
            // which one runs is a runtime fact).
            Statement::If {
                then_branch,
                else_branch,
                ..
            } => {
                out.push(statement);
                flatten_statements(std::slice::from_ref(then_branch), out);
                if let Some(else_branch) = else_branch {
                    flatten_statements(std::slice::from_ref(else_branch), out);
                }
            }
            Statement::While { body, .. } => {
                out.push(statement);
                flatten_statements(std::slice::from_ref(body), out);
            }
            other => out.push(other),
        }
    }
}

/// Builds a one-column `SHOWPLAN_TEXT` rowset describing a SELECT's access
/// path, without executing it.
fn showplan_rows(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let lines = match select.from.as_ref() {
        None => vec!["Constant Scan".to_string()],
        Some(TableRef::Table { name, .. })
            if !name.value.to_ascii_lowercase().starts_with("sys.") =>
        {
            match resolve_table(storage, &name.value) {
                Some(def) => {
                    // The scan shape carries the covering decision (it knows
                    // which columns the query reads); other shapes never
                    // cover, so the plain choose() answer is exact for them.
                    if let Some(plan) = scan_plan(storage, select, eval_ctx) {
                        plan::plan_text(&plan.access, &def.name, plan.covering)
                    } else {
                        let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
                        // Fetched only when choose() can use it (it returns a
                        // scan outright without a predicate or indexes).
                        let row_count = if def.indexes.is_empty() || select.where_clause.is_none() {
                            None
                        } else {
                            storage.rel_row_count(&def.name)
                        };
                        let path = plan::choose(
                            &def,
                            &schema,
                            &select.where_clause,
                            eval_ctx,
                            None,
                            row_count,
                        );
                        plan::plan_text(&path, &def.name, false)
                    }
                }
                None => vec![format!("Table Scan({})", name.value)],
            }
        }
        Some(TableRef::Table { name, .. }) => vec![format!("Table Scan({})", name.value)],
        // A lone table-valued function call: name it honestly rather than
        // letting it fall into the join catch-all (which would invent a
        // "Nested Loops" over a phantom base table named after the function).
        Some(TableRef::Function { name, .. }) => {
            vec![format!("Table-valued Function({})", name.value)]
        }
        Some(join) => {
            // Multi-table: a nested-loop join over full scans (Stage 8).
            let mut tables = Vec::new();
            collect_table_names(join, &mut tables);
            let mut lines = vec!["Nested Loops (join)".to_string()];
            for table in tables {
                lines.push(format!("  Table Scan({})", strip_schema(&table.value)));
            }
            lines
        }
    };
    Ok(RowSet {
        columns: vec![ResultColumn {
            name: "StmtText".to_string(),
            column_type: ColumnType::NVarChar { max_len: 4000 },
        }],
        rows: lines
            .into_iter()
            .map(|line| vec![Datum::NVarChar(line)])
            .collect(),
    })
}

fn ddl_in_txn_err() -> SqlError {
    SqlError::new(
        226,
        16,
        1,
        "DDL statements are not allowed inside an explicit transaction in this version.",
    )
}

// ---- transaction control -----------------------------------------------

/// `USE <database>`: a single-database instance, so the only accepted target
/// is the session's current database — the statement exists for the
/// database-context ENVCHANGE clients (SSMS) expect back (emitted by
/// `run_block` on success).
fn exec_use(database: &Name, ctx: &TxnContext) -> Result<StatementResult, SqlError> {
    if !database.value.eq_ignore_ascii_case(&ctx.database) {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                database.value
            ),
        )
        .at(database.span));
    }
    Ok(StatementResult::Done)
}

/// `THROW`: builds the error to raise (the caller returns it — `run_block`
/// then applies THROW's batch-terminating rule). The bare form re-throws the
/// innermost `CATCH`'s error verbatim, severity included; the argument form
/// is always severity 16 with a user error number (>= 50000).
fn exec_throw(throw: &ThrowStatement, ctx: &TxnContext) -> SqlError {
    let Some(args) = &throw.args else {
        return match ctx.error_stack.last() {
            Some(info) => {
                SqlError::new(info.number, info.severity, info.state, info.message.clone())
            }
            None => SqlError::new(
                10704,
                16,
                1,
                "To rethrow an error, a THROW statement must be used inside a CATCH block.",
            ),
        };
    };
    let eval_ctx = ctx.eval_context();
    match exec_throw_args(args, &eval_ctx) {
        // Both sides raise: the built error, or the argument evaluation's own.
        Ok(error) | Err(error) => error,
    }
}

fn exec_throw_args(args: &ThrowArgs, eval_ctx: &EvalContext) -> Result<SqlError, SqlError> {
    let number = int_argument(&args.number, eval_ctx, "THROW", "error number")?;
    if !(50_000..=i64::from(i32::MAX)).contains(&number) {
        return Err(SqlError::new(
            35100,
            16,
            1,
            format!(
                "Error number {number} in the THROW statement is outside the valid range. \
                 Specify an error number in the valid range of 50000 to 2147483647."
            ),
        ));
    }
    let message = match eval_constant(&args.message, eval_ctx)? {
        SqlValue::Str(text) => text,
        other => {
            return Err(SqlError::new(
                102,
                15,
                1,
                format!(
                    "The THROW message must be a string, not {}.",
                    other.type_name()
                ),
            ));
        }
    };
    let state = int_argument(&args.state, eval_ctx, "THROW", "state")?;
    if !(0..=255).contains(&state) {
        return Err(SqlError::new(
            102,
            15,
            1,
            format!("The THROW state must be between 0 and 255, not {state}."),
        ));
    }
    Ok(SqlError::new(number as i32, 16, state as u8, message))
}

/// `RAISERROR(msg, severity, state, args...)`. Severity decides the shape:
/// <= 10 emits an informational message (a TDS INFO token, not an error) and
/// the statement SUCCEEDS; 11..=18 raises an ordinary error (statement-scope
/// — `run_block` exempts it from XACT_ABORT and never dooms for it);
/// 19..=25 additionally require `WITH LOG`, and >= 20 is fatal to the
/// connection. The error number is always 50000 (message-id RAISERROR needs
/// `sys.messages`, which TruthDB does not have — 18054 like an unknown id).
fn exec_raiserror(
    raise: &RaiseError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let severity = int_argument(&raise.severity, &eval_ctx, "RAISERROR", "severity")?;
    if !(0..=25).contains(&severity) {
        return Err(SqlError::new(
            2754,
            16,
            1,
            format!("Error severity {severity} is out of the range 0 through 25."),
        ));
    }
    if severity > 18 && !raise.log {
        return Err(SqlError::new(
            2754,
            16,
            1,
            "Error severity levels greater than 18 can only be specified by members of the \
             sysadmin role, using the WITH LOG option.",
        ));
    }
    // State 0 is reported as 1, as SQL Server does.
    let state = int_argument(&raise.state, &eval_ctx, "RAISERROR", "state")?;
    if !(0..=255).contains(&state) {
        return Err(SqlError::new(
            2753,
            16,
            1,
            format!("The RAISERROR state must be between 0 and 255, not {state}."),
        ));
    }
    let state = (state as u8).max(1);
    let message = match eval_constant(&raise.message, &eval_ctx)? {
        SqlValue::Str(format) => {
            let mut args = Vec::with_capacity(raise.args.len());
            for arg in &raise.args {
                args.push(eval_constant(arg, &eval_ctx)?);
            }
            format_raiserror(&format, &args)?
        }
        // A message id: there is no `sys.messages`, so no id resolves.
        SqlValue::Int(id) => {
            return Err(SqlError::new(
                18054,
                16,
                1,
                format!(
                    "Error {id}, severity {severity}, state {state} was raised, but no message \
                     with that error number was found in sys.messages."
                ),
            ));
        }
        other => {
            return Err(SqlError::new(
                102,
                15,
                1,
                format!(
                    "The RAISERROR message must be a string or a message id, not {}.",
                    other.type_name()
                ),
            ));
        }
    };
    const AD_HOC_MESSAGE_NUMBER: i32 = 50000;
    if severity <= 10 {
        // Informational: `@@ERROR` reads 0 (or 50000 under SETERROR) — set
        // here because `run_block`'s success path leaves RAISERROR's value.
        txn_ctx.last_error = if raise.seterror {
            AD_HOC_MESSAGE_NUMBER
        } else {
            0
        };
        run.info(SqlError::new(
            AD_HOC_MESSAGE_NUMBER,
            severity as u8,
            state,
            message,
        ));
        return Ok(StatementOutcome::Result(StatementResult::Done));
    }
    Err(SqlError::new(
        AD_HOC_MESSAGE_NUMBER,
        severity as u8,
        state,
        message,
    ))
}

/// An integer statement argument (THROW/RAISERROR take constants or
/// variables).
fn int_argument(
    expr: &Expr,
    eval_ctx: &EvalContext,
    statement: &str,
    what: &str,
) -> Result<i64, SqlError> {
    match eval_constant(expr, eval_ctx)? {
        SqlValue::Int(value) => Ok(value),
        other => Err(SqlError::new(
            102,
            15,
            1,
            format!(
                "The {statement} {what} must be an integer, not {}.",
                other.type_name()
            ),
        )),
    }
}

/// RAISERROR's printf subset: `%d`/`%i` (also `%u`, `%x`/`%X`, `%o`) for
/// integer arguments, `%s` for strings, `%%` for a literal percent. Anything
/// else is refused (2787), as is an argument of the wrong type or a missing
/// one (2786). Surplus arguments are ignored, as SQL Server does.
fn format_raiserror(format: &str, args: &[SqlValue]) -> Result<String, SqlError> {
    let mut out = String::with_capacity(format.len());
    let mut next_arg = 0usize;
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(directive) = chars.next() else {
            return Err(SqlError::new(
                2787,
                16,
                1,
                "Invalid format specification: '%' at the end of the message.",
            ));
        };
        if directive == '%' {
            out.push('%');
            continue;
        }
        let argument = args.get(next_arg).ok_or_else(|| {
            SqlError::new(
                2786,
                16,
                1,
                format!(
                    "The data type of substitution parameter {} does not match the expected \
                     type of the format specification (missing argument).",
                    next_arg + 1
                ),
            )
        })?;
        let mismatch = || {
            SqlError::new(
                2786,
                16,
                1,
                format!(
                    "The data type of substitution parameter {} does not match the expected \
                     type of the format specification.",
                    next_arg + 1
                ),
            )
        };
        // A NULL argument prints "(null)" under every directive, as SQL
        // Server does. Integer arguments are int-typed (32-bit) there, so
        // the unsigned/hex forms wrap at 32 bits (-1 -> ffffffff) and a
        // value outside int range is a type mismatch (2786, the bigint
        // refusal).
        if matches!(argument, SqlValue::Null) {
            out.push_str("(null)");
            next_arg += 1;
            continue;
        }
        let int_arg = || -> Result<i32, SqlError> {
            match argument {
                SqlValue::Int(value) => i32::try_from(*value).map_err(|_| mismatch()),
                _ => Err(mismatch()),
            }
        };
        match directive {
            'd' | 'i' => out.push_str(&int_arg()?.to_string()),
            'u' => out.push_str(&(int_arg()? as u32).to_string()),
            'x' => out.push_str(&format!("{:x}", int_arg()? as u32)),
            'X' => out.push_str(&format!("{:X}", int_arg()? as u32)),
            'o' => out.push_str(&format!("{:o}", int_arg()? as u32)),
            's' => match argument {
                SqlValue::Str(value) => out.push_str(value),
                _ => return Err(mismatch()),
            },
            other => {
                return Err(SqlError::new(
                    2787,
                    16,
                    1,
                    format!("Invalid format specification: '%{other}'."),
                ));
            }
        }
        next_arg += 1;
    }
    Ok(out)
}

fn exec_begin(storage: &Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
    if ctx.txn.is_none() {
        ctx.txn = Some(storage.rel_begin().map_err(|e| map_storage_err(e, ""))?);
    }
    // Nested BEGIN only bumps the count (SQL Server semantics).
    ctx.trancount += 1;
    Ok(StatementResult::Done)
}

fn exec_commit(storage: &Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3902,
            16,
            1,
            "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    ctx.trancount -= 1;
    // Only the outermost COMMIT actually commits.
    if ctx.trancount == 0
        && let Some(txn) = ctx.txn.take()
    {
        ctx.savepoints.clear();
        // The transaction is over either way the commit goes.
        ctx.release_txn_snapshot(storage);
        storage
            .rel_commit(txn)
            .map_err(|e| map_storage_err(e, ""))?;
    }
    Ok(StatementResult::Done)
}

fn exec_rollback(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: Option<&Name>,
) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3903,
            16,
            1,
            "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    // ROLLBACK <savepoint>: partial rollback — the transaction stays open and
    // @@TRANCOUNT is unchanged; only the work done since the savepoint is undone.
    if let Some(name) = name {
        let Some(savepoint) = ctx
            .savepoints
            .get(&name.value.to_ascii_lowercase())
            .copied()
        else {
            return Err(SqlError::new(
                3908,
                16,
                1,
                format!(
                    "Cannot roll back {}. No transaction or savepoint of that name was found.",
                    name.value
                ),
            ));
        };
        if let Some(txn) = ctx.txn.as_mut() {
            storage
                .rel_rollback_to(txn, savepoint)
                .map_err(|e| map_storage_err(e, ""))?;
        }
        // Savepoints taken after this one are invalidated — their undo-log suffix
        // was just discarded (the target savepoint itself remains re-usable).
        ctx.savepoints
            .retain(|_, sp| sp.undo_len <= savepoint.undo_len);
        return Ok(StatementResult::Done);
    }
    // ROLLBACK (whole transaction), regardless of nesting. Reset the session's
    // transaction counters even if the storage rollback fails (which wedges the
    // store): the transaction is over either way, so leaving @@TRANCOUNT /
    // doomed set would desync the session.
    let result = match ctx.txn.take() {
        Some(txn) => storage
            .rel_rollback(txn)
            .map_err(|e| map_storage_err(e, "")),
        None => Ok(()),
    };
    ctx.release_txn_snapshot(storage);
    ctx.trancount = 0;
    ctx.doomed = false;
    ctx.savepoints.clear();
    result.map(|()| StatementResult::Done)
}

/// `SAVE TRANSACTION <name>`: record a savepoint the transaction can later roll
/// back to. Requires an active transaction (in autocommit there is nothing to
/// save, so it is a no-op). Re-saving an existing name overwrites it, as in
/// SQL Server.
fn exec_save(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    if let Some(txn) = ctx.txn.as_ref() {
        let savepoint = storage.rel_savepoint(txn);
        ctx.savepoints
            .insert(name.value.to_ascii_lowercase(), savepoint);
    }
    Ok(StatementResult::Done)
}

fn exec_set(ctx: &mut TxnContext, set: &SetStatement) -> Result<StatementResult, SqlError> {
    match set {
        SetStatement::XactAbort(on) => ctx.xact_abort = *on,
        SetStatement::IsolationLevel(level) => {
            ctx.isolation = match level {
                IsolationLevel::ReadUncommitted => Isolation::ReadUncommitted,
                IsolationLevel::ReadCommitted => Isolation::ReadCommitted,
                IsolationLevel::RepeatableRead => Isolation::RepeatableRead,
                IsolationLevel::Serializable => Isolation::Serializable,
                IsolationLevel::Snapshot => Isolation::Snapshot,
            }
        }
        SetStatement::ShowplanText(on) => ctx.showplan_text = *on,
        SetStatement::NoCount(on) => ctx.nocount = *on,
        SetStatement::Variable { name, value } => {
            // "Statements that make a simple assignment always set the
            // @@ROWCOUNT value to 1" — the Done result would reset it to 0,
            // so the assignment records its own count here.
            ctx.rowcount = 1;
            let column_type = ctx
                .variables
                .get(name)
                .map(|(t, _)| *t)
                .ok_or_else(|| undeclared_variable_err(name))?;
            let eval_ctx = ctx.eval_context();
            let coerced = coerce_variable(value, &column_type, name, &eval_ctx)?;
            ctx.variables.insert(name.clone(), (column_type, coerced));
        }
        SetStatement::Ignored => {}
    }
    Ok(StatementResult::Done)
}

/// `DECLARE @a TYPE [= expr], ...`. Each variable is added to the batch (error
/// 134 if already declared); an initializer (which may reference an earlier
/// variable) is coerced to the declared type, else the value starts NULL.
fn exec_declare(ctx: &mut TxnContext, decls: &[Declaration]) -> Result<StatementResult, SqlError> {
    for decl in decls {
        // A name occupies the scalar and table-variable stores jointly, so a
        // scalar DECLARE after a `DECLARE @t TABLE` of the same name is 134 too.
        if ctx.variables.contains_key(&decl.name) || ctx.table_variables.contains_key(&decl.name) {
            return Err(SqlError::new(
                134,
                15,
                2,
                format!(
                    "The variable name '@{}' has already been declared. Variable names must be unique within a query batch.",
                    decl.name
                ),
            ));
        }
        let column_type = data_type_to_column_type(&decl.data_type, &decl.name)?;
        let value = match &decl.initializer {
            Some(expr) => {
                let eval_ctx = ctx.eval_context();
                coerce_variable(expr, &column_type, &decl.name, &eval_ctx)?
            }
            None => SqlValue::Null,
        };
        ctx.variables
            .insert(decl.name.clone(), (column_type, value));
    }
    Ok(StatementResult::Done)
}

/// `DECLARE @t TABLE ( ... )`: registers an empty in-memory table variable. Its
/// schema is bound like a base table's columns; its declared PRIMARY KEY becomes
/// the key columns used for uniqueness at INSERT time.
fn exec_declare_table_var(
    ctx: &mut TxnContext,
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[Name],
) -> Result<StatementResult, SqlError> {
    // A name occupies the scalar and table-variable stores jointly.
    if ctx.variables.contains_key(name) || ctx.table_variables.contains_key(name) {
        return Err(SqlError::new(
            134,
            15,
            2,
            format!(
                "The variable name '@{name}' has already been declared. Variable names must be \
                 unique within a query batch."
            ),
        ));
    }
    let (schema, key_columns, defaults) = build_table_var_definition(name, columns, primary_key)?;
    ctx.table_variables.insert(
        name.to_string(),
        TableVar {
            schema,
            key_columns,
            defaults,
            rows: Vec::new(),
        },
    );
    Ok(StatementResult::Done)
}

/// A table variable's built definition: its column schema, the schema indices of
/// its PRIMARY KEY columns, and the per-column DEFAULT source text (parallel to
/// the schema columns).
type TableVarDefinition = (Schema, Vec<usize>, Vec<Option<String>>);

/// Builds the schema, key-column indices, and per-column DEFAULT source text for
/// a table-variable declaration (`DECLARE @name TABLE(cols)` and the RETURNS
/// clause of a multi-statement TVF share this): unique column names (2705), PK
/// columns forced NOT NULL (8111 on explicit-NULL, MAX-key rejected), and the
/// DEFAULT texts applied per INSERT. `name` (without `@`) names the table in the
/// error messages.
fn build_table_var_definition(
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[Name],
) -> Result<TableVarDefinition, SqlError> {
    // Column names within the table variable must be unique (2705), the same
    // rule a base table enforces in exec_create_table.
    let mut seen: Vec<&str> = Vec::new();
    for column in columns {
        if seen
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&column.name.value))
        {
            return Err(SqlError::new(
                2705,
                16,
                3,
                format!(
                    "Column names in each table must be unique. Column name '{}' is specified more than once.",
                    column.name.value
                ),
            )
            .at(column.name.span));
        }
        seen.push(&column.name.value);
    }
    let bound = columns
        .iter()
        .map(bind_column)
        .collect::<Result<Vec<_>, _>>()?;
    let mut schema = Schema { columns: bound };
    let mut key_columns = Vec::new();
    for pk in primary_key {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&pk.value))
            .ok_or_else(|| {
                SqlError::new(
                    1911,
                    16,
                    1,
                    format!(
                        "Column name '{}' does not exist in the target table or view.",
                        pk.value
                    ),
                )
            })?;
        // A PRIMARY KEY column is implicitly NOT NULL; declaring it NULL is
        // 8111, and a MAX-typed column cannot be a key — the same rules a base
        // table enforces in exec_create_table.
        let declared_null = columns
            .iter()
            .find(|c| c.name.eq_ignore_case(&pk.value))
            .and_then(|c| c.nullable)
            == Some(true);
        if declared_null {
            return Err(SqlError::new(
                8111,
                16,
                1,
                format!(
                    "Cannot define PRIMARY KEY constraint on nullable column in table '@{name}'."
                ),
            ));
        }
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&pk.value, &format!("@{name}")).at(pk.span));
        }
        schema.columns[index].nullable = false;
        key_columns.push(index);
    }
    // Per-column DEFAULT source text (parallel to the schema columns), applied
    // at INSERT to columns left unspecified — same as a base table.
    let defaults: Vec<Option<String>> = columns.iter().map(|c| c.default.clone()).collect();
    Ok((schema, key_columns, defaults))
}

fn undeclared_variable_err(name: &str) -> SqlError {
    SqlError::new(
        137,
        15,
        2,
        format!("Must declare the scalar variable \"@{name}\"."),
    )
}

/// Evaluates a variable initializer/assignment (a constant expression that may
/// reference already-declared variables) and coerces it to the declared type.
fn coerce_variable(
    expr: &Expr,
    column_type: &ColumnType,
    name: &str,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let sql_value = eval_constant(expr, eval_ctx)?;
    let datum = value::sql_to_datum(&sql_value, column_type, name)?;
    Ok(value::datum_to_sql(&datum, column_type))
}

// ---- CREATE TABLE -------------------------------------------------------

fn exec_create_table(storage: &Storage, create: &CreateTable) -> Result<StatementResult, SqlError> {
    // Strip an optional `dbo.` schema prefix so the table is stored (and
    // later resolved) under its bare name.
    let table_name = strip_schema(&create.table.value);
    if resolve_table(storage, table_name).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{table_name}' in the database."),
        ));
    }

    let mut seen = Vec::new();
    let mut columns = Vec::with_capacity(create.columns.len());
    for column in &create.columns {
        if seen
            .iter()
            .any(|n: &String| n.eq_ignore_ascii_case(&column.name.value))
        {
            return Err(SqlError::new(
                2705,
                16,
                3,
                format!(
                    "Column names in each table must be unique. Column name '{}' is specified more than once.",
                    column.name.value
                ),
            ));
        }
        seen.push(column.name.value.clone());
        columns.push(bind_column(column)?);
    }

    // Primary key columns must exist and are implicitly NOT NULL (declaring
    // one explicitly NULL is an error, matching SQL Server 8111).
    let mut key_names = Vec::new();
    for key in &create.primary_key {
        let Some(index) = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&key.value))
        else {
            return Err(SqlError::new(
                1750,
                16,
                0,
                format!(
                    "Column '{}' in the PRIMARY KEY is not a column of the table.",
                    key.value
                ),
            )
            .at(key.span));
        };
        let declared_null = create
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_case(&key.value))
            .and_then(|c| c.nullable)
            == Some(true);
        if declared_null {
            return Err(SqlError::new(
                8111,
                16,
                1,
                format!(
                    "Cannot define PRIMARY KEY constraint on nullable column in table '{table_name}'."
                ),
            ));
        }
        if columns[index].column_type.is_max() {
            return Err(max_key_column_error(&key.value, table_name).at(key.span));
        }
        columns[index].nullable = false;
        key_names.push(columns[index].name.clone());
    }

    // Per-column DEFAULT source text (parallel to columns).
    let defaults: Vec<Option<String>> = create.columns.iter().map(|c| c.default.clone()).collect();

    // At most one IDENTITY column, on an integer type.
    let mut identity: Option<catalog::IdentitySpec> = None;
    for (index, column) in create.columns.iter().enumerate() {
        let Some(id) = column.identity else { continue };
        if identity.is_some() {
            return Err(SqlError::new(
                2744,
                16,
                2,
                format!(
                    "Multiple identity columns specified for table '{table_name}'. Only one identity column per table is allowed."
                ),
            ));
        }
        if !matches!(
            columns[index].column_type,
            ColumnType::TinyInt | ColumnType::SmallInt | ColumnType::Int | ColumnType::BigInt
        ) {
            return Err(SqlError::new(
                2749,
                16,
                2,
                format!(
                    "Identity column '{}' must be of a data type that is an integer.",
                    column.name.value
                ),
            )
            .at(column.span));
        }
        if column.default.is_some() {
            return Err(SqlError::new(
                1754,
                16,
                1,
                "Defaults cannot be created on columns with an IDENTITY attribute.".to_string(),
            )
            .at(column.span));
        }
        identity = Some(catalog::IdentitySpec {
            column: index,
            seed: id.seed,
            increment: id.increment,
            next: id.seed,
        });
    }

    // CHECK constraints (column-level + table-level): validate, name, and
    // fold into the catalog. Validation needs the bound columns.
    let check_constraints = build_check_defs(create, &columns, table_name)?;
    // FOREIGN KEY constraints: validate against the (possibly self-)referenced
    // table's primary key and order each child column to the parent's PK.
    // Constraint names are unique across kinds, so seed with the check names.
    let check_names: Vec<String> = check_constraints.iter().map(|c| c.name.clone()).collect();
    let foreign_keys = build_foreign_key_defs(storage, create, &columns, table_name, &check_names)?;

    // UNIQUE constraints become unique indexes. Resolve their columns now (while
    // `columns` is in hand) so an invalid column errors before the table exists.
    let mut unique_indexes: Vec<(String, Vec<(usize, bool)>)> = Vec::new();
    for (i, uc) in create.unique_constraints.iter().enumerate() {
        let mut cols = Vec::with_capacity(uc.columns.len());
        for col in &uc.columns {
            let index = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col.value))
                .ok_or_else(|| SqlError::invalid_column(&col.value).at(col.span))?;
            cols.push((index, true));
        }
        let name = uc
            .name
            .as_ref()
            .map(|n| n.value.clone())
            .unwrap_or_else(|| format!("UQ_{table_name}_{}", i + 1));
        unique_indexes.push((name, cols));
    }

    storage
        .rel_create_table(
            table_name,
            columns,
            &key_names,
            defaults,
            identity,
            check_constraints,
            foreign_keys,
        )
        .map_err(|err| map_storage_err(err, table_name))?;
    for (name, cols) in unique_indexes {
        storage
            .rel_create_index(table_name, name, cols, true, Vec::new())
            .map_err(|err| map_storage_err(err, table_name))?;
    }
    Ok(StatementResult::Done)
}

/// Collects and validates a table's FOREIGN KEY constraints (column-level, then
/// table-level), assigning a name to unnamed ones. `check_names` are the names
/// already taken by the table's CHECK constraints so a FK cannot reuse one
/// (constraint names are unique across kinds).
fn build_foreign_key_defs(
    storage: &Storage,
    create: &CreateTable,
    columns: &[Column],
    table_name: &str,
    check_names: &[String],
) -> Result<Vec<catalog::ForeignKeyDef>, SqlError> {
    let raw = create
        .columns
        .iter()
        .flat_map(|c| c.foreign_keys.iter())
        .chain(create.foreign_keys.iter());

    // The parent's primary key (name, type) per PK column, in PK order. A
    // self-reference reads it from this CREATE; otherwise from the catalog.
    let self_pk = || -> Result<Vec<(String, ColumnType)>, SqlError> {
        create
            .primary_key
            .iter()
            .map(|k| {
                let col = columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&k.value))
                    .expect("primary key column bound");
                Ok((col.name.clone(), col.column_type))
            })
            .collect()
    };

    let mut names: Vec<String> = check_names.to_vec();
    let mut defs = Vec::new();
    for fk in raw {
        let parent_bare = strip_schema(&fk.parent.value);
        let is_self = parent_bare.eq_ignore_ascii_case(table_name);
        // Parent primary key: (column name, type) in PK order.
        let parent_pk: Vec<(String, ColumnType)> = if is_self {
            self_pk()?
        } else {
            let parent = resolve_table(storage, &fk.parent.value)
                .ok_or_else(|| SqlError::invalid_object(&fk.parent.value).at(fk.parent.span))?;
            let schema = parent
                .schema()
                .map_err(|e| map_storage_err(e, &parent.name))?;
            parent
                .key_columns
                .iter()
                .map(|&i| {
                    (
                        schema.columns[i].name.clone(),
                        schema.columns[i].column_type,
                    )
                })
                .collect()
        };
        let def = bind_foreign_key(fk, columns, table_name, &parent_pk, parent_bare, &names)?;
        names.push(def.name.clone());
        defs.push(def);
    }
    Ok(defs)
}

/// Validates one FOREIGN KEY against the parent's primary key and produces a
/// [`catalog::ForeignKeyDef`] whose child column indices are ordered to match
/// the parent's PK. Referenced columns must be exactly the parent PK (SQL
/// Server requires a unique/PK target); child and parent column types and
/// counts must match.
fn bind_foreign_key(
    fk: &ForeignKey,
    columns: &[Column],
    table_name: &str,
    parent_pk: &[(String, ColumnType)],
    parent_bare: &str,
    existing_names: &[String],
) -> Result<catalog::ForeignKeyDef, SqlError> {
    let no_key = || {
        SqlError::new(
            1776,
            16,
            0,
            format!(
                "There are no primary or candidate keys in the referenced table '{parent_bare}' that match the referencing column list in the foreign key."
            ),
        )
        .at(fk.parent.span)
    };
    if parent_pk.is_empty() {
        return Err(no_key());
    }
    // Referenced parent columns (defaulting to the whole PK) paired with the
    // child columns positionally.
    let parent_cols: Vec<String> = if fk.parent_columns.is_empty() {
        parent_pk.iter().map(|(n, _)| n.clone()).collect()
    } else {
        fk.parent_columns.iter().map(|n| n.value.clone()).collect()
    };
    if fk.columns.len() != parent_cols.len() {
        return Err(SqlError::new(
            1776,
            16,
            0,
            "The number of referencing columns differs from the number of referenced columns.",
        )
        .at(fk.span));
    }
    // The referenced set must be exactly the parent PK (order-independent).
    if parent_cols.len() != parent_pk.len()
        || !parent_pk
            .iter()
            .all(|(pk, _)| parent_cols.iter().any(|c| c.eq_ignore_ascii_case(pk)))
    {
        return Err(no_key());
    }

    // Resolve child column indices and check each child/parent type matches.
    let child_index = |name: &Name| -> Result<usize, SqlError> {
        columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&name.value))
            .ok_or_else(|| SqlError::invalid_column(&name.value).at(name.span))
    };
    // For each parent PK column (in PK order), find the child column mapped to
    // it and record its index — so the stored order matches the parent PK.
    let mut ordered = Vec::with_capacity(parent_pk.len());
    for (pk_name, pk_type) in parent_pk {
        // Which referenced position names this PK column?
        let pos = parent_cols
            .iter()
            .position(|c| c.eq_ignore_ascii_case(pk_name))
            .ok_or_else(no_key)?;
        let child_col = &fk.columns[pos];
        let idx = child_index(child_col)?;
        if columns[idx].column_type != *pk_type {
            return Err(SqlError::new(
                1778,
                16,
                0,
                format!(
                    "Column '{table_name}.{}' is not the same data type as referencing column '{parent_bare}.{pk_name}' in the foreign key.",
                    columns[idx].name
                ),
            )
            .at(child_col.span));
        }
        ordered.push(idx);
    }

    let name = match &fk.name {
        Some(n) => {
            if existing_names
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&n.value))
            {
                return Err(SqlError::new(
                    2714,
                    16,
                    5,
                    format!(
                        "There is already an object named '{}' in the database.",
                        n.value
                    ),
                )
                .at(n.span));
            }
            n.value.clone()
        }
        None => {
            let mut seq = 0u32;
            loop {
                seq += 1;
                let candidate = format!("FK__{table_name}__{parent_bare}__{seq}");
                if !existing_names
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(&candidate))
                {
                    break candidate;
                }
            }
        }
    };
    Ok(catalog::ForeignKeyDef {
        name,
        columns: ordered,
        parent: parent_bare.to_string(),
    })
}

/// Collects a table's CHECK constraints (column-level, then table-level) and
/// binds each ([`bind_check`]), threading the running name list so unnamed
/// constraints get unique auto names and duplicate explicit names are caught.
fn build_check_defs(
    create: &CreateTable,
    columns: &[Column],
    table_name: &str,
) -> Result<Vec<catalog::CheckDef>, SqlError> {
    let raw = create
        .columns
        .iter()
        .flat_map(|c| c.checks.iter())
        .chain(create.check_constraints.iter());

    let mut names: Vec<String> = Vec::new();
    let mut defs = Vec::new();
    for check in raw {
        let def = bind_check(check, columns, table_name, &names)?;
        names.push(def.name.clone());
        defs.push(def);
    }
    Ok(defs)
}

/// Validates one CHECK constraint against a table's columns and its existing
/// constraint names: the predicate must parse and reference only real columns
/// (207/4104); an explicit name must not collide (2714); an unnamed check is
/// assigned the first free `CK__<table>__<n>`.
fn bind_check(
    check: &CheckConstraint,
    columns: &[Column],
    table_name: &str,
    existing_names: &[String],
) -> Result<catalog::CheckDef, SqlError> {
    let expr = truthdb_sql::parse_expr(&check.predicate)?;
    validate_check_columns(&expr, columns)?;
    let name = match &check.name {
        Some(n) => {
            if existing_names
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&n.value))
            {
                return Err(SqlError::new(
                    2714,
                    16,
                    5,
                    format!(
                        "There is already an object named '{}' in the database.",
                        n.value
                    ),
                )
                .at(n.span));
            }
            n.value.clone()
        }
        None => {
            let mut seq = 0u32;
            loop {
                seq += 1;
                let candidate = format!("CK__{table_name}__{seq}");
                if !existing_names
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(&candidate))
                {
                    break candidate;
                }
            }
        }
    };
    Ok(catalog::CheckDef {
        name,
        predicate: check.predicate.clone(),
    })
}

/// Rejects a CHECK predicate that references a column the table does not have
/// (error 207). Only column existence is checked here; type/boolean validity
/// is left to per-row evaluation.
fn validate_check_columns(expr: &Expr, columns: &[Column]) -> Result<(), SqlError> {
    match &expr.kind {
        ExprKind::Column(name) => {
            // A CHECK may only reference columns of its own table by their bare
            // name. A multi-part identifier (`t.col`) can't be resolved by the
            // bare-name enforcement resolver, so reject it here (4104) rather
            // than accept a table that then rejects every INSERT with 207.
            if name.value.contains('.') {
                return Err(SqlError::new(
                    4104,
                    16,
                    1,
                    format!(
                        "The multi-part identifier \"{}\" could not be bound.",
                        name.value
                    ),
                )
                .at(name.span));
            }
            if columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&name.value))
            {
                Ok(())
            } else {
                Err(SqlError::invalid_column(&name.value).at(name.span))
            }
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. } => validate_check_columns(expr, columns),
        ExprKind::Binary { left, right, .. } => {
            validate_check_columns(left, columns)?;
            validate_check_columns(right, columns)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_check_columns(expr, columns)?;
            validate_check_columns(pattern, columns)
        }
        ExprKind::InList { expr, list, .. } => {
            validate_check_columns(expr, columns)?;
            list.iter()
                .try_for_each(|e| validate_check_columns(e, columns))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_check_columns(expr, columns)?;
            validate_check_columns(low, columns)?;
            validate_check_columns(high, columns)
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(op) = operand {
                validate_check_columns(op, columns)?;
            }
            for (when, then) in branches {
                validate_check_columns(when, columns)?;
                validate_check_columns(then, columns)?;
            }
            if let Some(e) = else_result {
                validate_check_columns(e, columns)?;
            }
            Ok(())
        }
        ExprKind::Function { args, .. } => args
            .iter()
            .try_for_each(|a| validate_check_columns(a, columns)),
        ExprKind::Aggregate { arg, .. } => arg
            .as_ref()
            .map_or(Ok(()), |a| validate_check_columns(a, columns)),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => Ok(()),
        // Subqueries are not allowed in a CHECK constraint (SQL Server 1046).
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {
            Err(SqlError::new(
                1046,
                15,
                1,
                "Subqueries are not allowed in this context. Only scalar expressions are allowed.",
            ))
        }
    }
}

/// Parses a table's stored CHECK predicates once (per statement) for row
/// enforcement, pairing each with its constraint name.
fn parse_checks(def: &TableDef) -> Result<Vec<(String, Expr)>, SqlError> {
    def.check_constraints
        .iter()
        .map(|c| Ok((c.name.clone(), truthdb_sql::parse_expr(&c.predicate)?)))
        .collect()
}

/// Enforces CHECK constraints against a fully-built row (schema order). A
/// constraint passes on TRUE or UNKNOWN (NULL); FALSE is error 547.
fn enforce_checks(
    checks: &[(String, Expr)],
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
    verb: &str,
    table: &str,
) -> Result<(), SqlError> {
    for (name, expr) in checks {
        match eval::eval(expr, row, resolver, eval_ctx)? {
            SqlValue::Bool(false) => {
                return Err(SqlError::new(
                    547,
                    16,
                    0,
                    format!(
                        "The {verb} statement conflicted with the CHECK constraint \"{name}\". The conflict occurred in database \"truthdb\", table \"dbo.{table}\".",
                    ),
                ));
            }
            SqlValue::Bool(true) | SqlValue::Null => {}
            _ => {
                return Err(SqlError::new(
                    4145,
                    15,
                    1,
                    format!(
                        "An expression of non-boolean type specified in a context where a condition is expected, near the CHECK constraint \"{name}\"."
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// A child row's referencing key for one foreign key (the FK columns in parent
/// primary-key order). `None` if any FK column is NULL — MATCH SIMPLE, which
/// skips enforcement (the NULL-FK trap).
fn fk_key(fk: &catalog::ForeignKeyDef, row: &[Datum]) -> Option<Vec<Datum>> {
    let key: Vec<Datum> = fk.columns.iter().map(|&i| row[i].clone()).collect();
    if key.iter().any(|d| matches!(d, Datum::Null)) {
        None
    } else {
        Some(key)
    }
}

/// Whether a referencing `key` (parent PK order) exists in the parent — either
/// a committed parent row, or, for a self-reference, a sibling row in `batch`
/// (whose PK columns are `child.key_columns`).
fn fk_parent_exists(
    storage: &Storage,
    fk: &catalog::ForeignKeyDef,
    key: &[Datum],
    child: &TableDef,
    batch: &[Vec<Datum>],
) -> Result<bool, SqlError> {
    if storage
        .rel_get(&fk.parent, key)
        .map_err(|e| map_storage_err(e, &fk.parent))?
        .is_some()
    {
        return Ok(true);
    }
    if fk.parent.eq_ignore_ascii_case(&child.name) && child.key_columns.len() == key.len() {
        // Fold both the referencing key and each sibling's PK by the parent PK
        // collation, so a case-insensitive self-reference matches a case-variant
        // sibling in the same statement — consistent with the folded `rel_get`
        // above (which handles the committed-row case).
        let key_coll: Vec<Option<String>> = child
            .key_columns
            .iter()
            .map(|&i| child.collations.get(i).cloned().flatten())
            .collect();
        let folded_key = collated_key(key, &key_coll);
        return Ok(batch.iter().any(|r| {
            let sibling: Vec<Datum> = child.key_columns.iter().map(|&i| r[i].clone()).collect();
            collated_key(&sibling, &key_coll) == folded_key
        }));
    }
    Ok(false)
}

fn fk_child_violation(name: &str, verb: &str, parent: &str) -> SqlError {
    SqlError::new(
        547,
        16,
        0,
        format!(
            "The {verb} statement conflicted with the FOREIGN KEY constraint \"{name}\". The conflict occurred in database \"truthdb\", table \"dbo.{parent}\".",
        ),
    )
}

/// Enforces this table's FOREIGN KEY constraints against a built child row:
/// each non-NULL referencing key must exist in the parent's primary key. For a
/// self-reference, a sibling row in the same statement (`batch`) also satisfies
/// it. A missing parent is error 547. `check_self_ref` skips self-referencing
/// foreign keys (an UPDATE validates those against its post-update snapshot,
/// since a pre-mutation probe would see stale rows).
fn enforce_child_fks(
    storage: &Storage,
    def: &TableDef,
    row: &[Datum],
    batch: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    for fk in &def.foreign_keys {
        if !check_self_ref && fk.parent.eq_ignore_ascii_case(&def.name) {
            continue;
        }
        let Some(key) = fk_key(fk, row) else {
            continue; // NULL referencing column: not enforced
        };
        if !fk_parent_exists(storage, fk, &key, def, batch)? {
            return Err(fk_child_violation(&fk.name, verb, &fk.parent));
        }
    }
    Ok(())
}

/// A child index whose leading key columns are exactly the FK's child columns,
/// usable to probe for referencing rows by seeking the removed parent key
/// instead of scanning the whole child.
fn fk_probe_index<'a>(
    child: &'a TableDef,
    fk: &catalog::ForeignKeyDef,
) -> Option<&'a catalog::IndexDef> {
    child.indexes.iter().find(|index| {
        index.columns.len() >= fk.columns.len()
            && index
                .columns
                .iter()
                .zip(&fk.columns)
                .all(|((col, _asc), &fk_col)| *col == fk_col)
    })
}

/// Whether the child FK columns and the referenced parent PK columns have the
/// same case sensitivity. The FK index fast path folds the probe key by the
/// *child* column collation (to match the child index's folded keys), while the
/// insert-time check (`rel_get`) and the scan fallback fold by the *parent* PK
/// collation; when they disagree (a mixed-collation FK) the fast path can miss a
/// reference, so it is only used when the collations match — otherwise the scan
/// fallback (parent collation, consistent with insert) handles it.
fn fk_collations_match(child: &TableDef, fk: &catalog::ForeignKeyDef, parent: &TableDef) -> bool {
    fk.columns.len() == parent.key_columns.len()
        && fk.columns.iter().zip(&parent.key_columns).all(|(&c, &p)| {
            CollationSensitivity::from_optional(child.collations.get(c).and_then(|x| x.as_deref()))
                == CollationSensitivity::from_optional(
                    parent.collations.get(p).and_then(|x| x.as_deref()),
                )
        })
}

/// The error raised when a surviving child row references a removed parent key.
fn reference_conflict(verb: &str, fk_name: &str, child_name: &str) -> SqlError {
    SqlError::new(
        547,
        16,
        0,
        format!(
            "The {verb} statement conflicted with the REFERENCE constraint \"{fk_name}\". The conflict occurred in database \"truthdb\", table \"dbo.{child_name}\"."
        ),
    )
}

/// Enforces NO ACTION on the parent side: no surviving child row may reference
/// any of `removed_keys` (parent primary-key values being deleted or vacated by
/// an UPDATE). A referencing child is error 547. When the child has an index on
/// the FK columns, each removed key is probed by an index seek; otherwise the
/// child is scanned.
fn enforce_parent_fks(
    storage: &Storage,
    parent: &TableDef,
    removed_keys: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    if removed_keys.is_empty() {
        return Ok(());
    }
    // Fold the removed parent keys by the parent PK collation so the scan
    // fallback matches child references case-insensitively — the same folding the
    // index fast path gets from the child index's key encoding.
    let parent_key_coll: Vec<Option<String>> = parent
        .key_columns
        .iter()
        .map(|&i| parent.collations.get(i).cloned().flatten())
        .collect();
    let removed_folded: Vec<Vec<u8>> = removed_keys
        .iter()
        .map(|k| collated_key(k, &parent_key_coll))
        .collect();
    let children: Vec<TableDef> = storage
        .rel_tables()
        .into_iter()
        .filter(|t| {
            t.foreign_keys
                .iter()
                .any(|fk| fk.parent.eq_ignore_ascii_case(&parent.name))
        })
        .collect();
    for child in &children {
        let self_ref = child.name.eq_ignore_ascii_case(&parent.name);
        // A self-referencing table's own FKs are validated against the
        // post-update snapshot, not the pre-mutation child scan.
        if self_ref && !check_self_ref {
            continue;
        }
        for fk in &child.foreign_keys {
            if !fk.parent.eq_ignore_ascii_case(&parent.name) {
                continue;
            }
            // Fast path: an index on the FK columns lets us seek each removed
            // parent key instead of scanning the child. Not used for a
            // self-reference (whose own being-removed rows must be excluded). If
            // a key fails to encode (unexpected type mismatch), fall back to the
            // scan rather than risk missing a reference.
            if !self_ref
                && fk_collations_match(child, fk, parent)
                && let Some(index) = fk_probe_index(child, fk)
            {
                let mut handled = true;
                for key in removed_keys {
                    match crate::relstore::index::encode_index_prefix(
                        key,
                        &index.columns,
                        &child.collations,
                    ) {
                        Ok(lower) => {
                            let upper = crate::relstore::index::prefix_upper_bound(&lower);
                            let matches = storage
                                .rel_index_scan(
                                    &child.name,
                                    index.object_id,
                                    Some(lower),
                                    upper,
                                    None,
                                    false,
                                    // Integrity probe: must see the current
                                    // state, never a snapshot.
                                    None,
                                )
                                .map_err(|e| map_storage_err(e, &child.name))?;
                            if !matches.is_empty() {
                                return Err(reference_conflict(verb, &fk.name, &child.name));
                            }
                        }
                        Err(_) => {
                            handled = false;
                            break;
                        }
                    }
                }
                if handled {
                    continue;
                }
            }
            // Fallback: scan the child and compare each row's FK key.
            let child_rows = storage
                .rel_scan(&child.name)
                .map_err(|e| map_storage_err(e, &child.name))?;
            for row in &child_rows {
                // A self-referencing row that is itself being removed does not
                // count as a surviving reference.
                if self_ref {
                    let pk: Vec<Datum> =
                        parent.key_columns.iter().map(|&i| row[i].clone()).collect();
                    if removed_folded.contains(&collated_key(&pk, &parent_key_coll)) {
                        continue;
                    }
                }
                let Some(key) = fk_key(fk, row) else {
                    continue;
                };
                if removed_folded.contains(&collated_key(&key, &parent_key_coll)) {
                    return Err(reference_conflict(verb, &fk.name, &child.name));
                }
            }
        }
    }
    Ok(())
}

/// The primary-key values of a row (in key-column order).
fn pk_of(def: &TableDef, row: &[Datum]) -> Vec<Datum> {
    def.key_columns.iter().map(|&i| row[i].clone()).collect()
}

/// A key's collation-canonical bytes (`collations` parallel to `values`), for
/// comparing keys by value — the FK scan fallback and the self-reference checks.
///
/// This encodes exactly as the index key does, so "equal" here means what it
/// means to a seek: two keys match when the collation says they do, including
/// case- and accent-insensitively. Comparing the encoded bytes rather than the
/// values is what keeps the two definitions from drifting apart.
fn collated_key(values: &[Datum], collations: &[Option<String>]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, value) in values.iter().enumerate() {
        // A key column always encodes; a type error here would mean the row did
        // not come from this table.
        let _ = crate::relstore::key::encode_datum_collated(
            value,
            collations.get(i).and_then(|c| c.as_deref()),
            &mut out,
        );
    }
    out
}

/// Maps a parsed [`DataType`] to a storage [`ColumnType`], validating length
/// bounds. `name` is only used for the length-overflow error message.
fn data_type_to_column_type(data_type: &DataType, name: &str) -> Result<ColumnType, SqlError> {
    Ok(match data_type {
        DataType::TinyInt => ColumnType::TinyInt,
        DataType::SmallInt => ColumnType::SmallInt,
        DataType::Int => ColumnType::Int,
        DataType::BigInt => ColumnType::BigInt,
        DataType::Bit => ColumnType::Bit,
        DataType::Real => ColumnType::Real,
        DataType::Float => ColumnType::Float,
        DataType::Decimal { precision, scale } => ColumnType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Date => ColumnType::Date,
        DataType::Time => ColumnType::Time,
        DataType::DateTime2 => ColumnType::DateTime2,
        DataType::UniqueIdentifier => ColumnType::UniqueIdentifier,
        DataType::VarChar(n) => ColumnType::VarChar {
            max_len: length(*n, name)?,
        },
        DataType::NVarChar(n) => ColumnType::NVarChar {
            max_len: length(*n, name)?,
        },
        DataType::VarBinary(n) => ColumnType::VarBinary {
            max_len: length(*n, name)?,
        },
        DataType::VarCharMax => ColumnType::VarCharMax,
        DataType::NVarCharMax => ColumnType::NVarCharMax,
        DataType::VarBinaryMax => ColumnType::VarBinaryMax,
    })
}

/// Binds a declared column. A character column left without an explicit
/// `COLLATE` keeps `None` here and is resolved to the database default by
/// `rel_create_table`, the one point every CREATE TABLE passes through.
fn bind_column(column: &ColumnDef) -> Result<Column, SqlError> {
    let column_type = data_type_to_column_type(&column.data_type, &column.name.value)?;
    // A COLLATE clause is only meaningful on character columns.
    if column.collation.is_some()
        && !matches!(
            column_type,
            ColumnType::VarChar { .. } | ColumnType::NVarChar { .. }
        )
    {
        return Err(SqlError::new(
            4536,
            16,
            1,
            format!(
                "COLLATE clause cannot be used on column '{}' because its data type is not character based.",
                column.name.value
            ),
        )
        .at(column.span));
    }
    // Columns are nullable by default (SQL Server ANSI default), PK columns
    // and explicit NOT NULL are not.
    let nullable = column.nullable.unwrap_or(!column.primary_key);
    Ok(Column {
        name: column.name.value.clone(),
        column_type,
        nullable,
        collation: column.collation.clone(),
    })
}

fn length(n: u32, name: &str) -> Result<u16, SqlError> {
    u16::try_from(n).map_err(|_| {
        SqlError::new(
            131,
            15,
            2,
            format!("The size for column '{name}' exceeds the maximum."),
        )
    })
}

// ---- DROP TABLE ---------------------------------------------------------

fn exec_drop_table(storage: &Storage, drop: &DropTable) -> Result<StatementResult, SqlError> {
    // DROP TABLE does not drop a view or a procedure (use the matching DROP).
    // The object exists but is the wrong type, so error even under IF EXISTS
    // rather than silently no-op — the review showed DROP TABLE silently
    // DESTROYING a procedure through the shared catalog path.
    if resolve_table(storage, &drop.table.value)
        .is_some_and(|d| d.is_view() || d.is_procedure() || d.is_function() || d.is_trigger())
    {
        return Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the table '{}', because it does not exist or you do not have permission.",
                drop.table.value
            ),
        ));
    }
    let name = resolve_table(storage, &drop.table.value).map(|d| d.name);
    match name {
        Some(name) => {
            // A table still referenced by another table's foreign key cannot be
            // dropped (SQL Server 3726) — it would leave a dangling reference.
            if let Some(child) = storage.rel_tables().into_iter().find(|t| {
                !t.name.eq_ignore_ascii_case(&name)
                    && t.foreign_keys
                        .iter()
                        .any(|fk| fk.parent.eq_ignore_ascii_case(&name))
            }) {
                let referencing = child
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.parent.eq_ignore_ascii_case(&name))
                    .map(|fk| fk.name.clone())
                    .unwrap_or_default();
                return Err(SqlError::new(
                    3726,
                    16,
                    1,
                    format!(
                        "Could not drop object '{name}' because it is referenced by a FOREIGN KEY constraint '{referencing}'."
                    ),
                ));
            }
            // Cascade-drop the table's triggers — a trigger outlives its parent
            // table nowhere in SQL Server, and an orphan would permanently block
            // its own name (and dangle in sys.triggers).
            if let Some(parent_oid) = resolve_table(storage, &name).map(|d| d.object_id) {
                let orphan_triggers: Vec<String> = storage
                    .rel_tables()
                    .into_iter()
                    .filter(|d| {
                        d.trigger
                            .as_ref()
                            .is_some_and(|t| t.parent_object_id == parent_oid)
                    })
                    .map(|d| d.name)
                    .collect();
                for trigger_name in orphan_triggers {
                    storage
                        .rel_drop_table(&trigger_name)
                        .map_err(|err| map_storage_err(err, &trigger_name))?;
                }
            }
            storage
                .rel_drop_table(&name)
                .map_err(|err| map_storage_err(err, &drop.table.value))?;
            Ok(StatementResult::Done)
        }
        None if drop.if_exists => Ok(StatementResult::Done),
        None => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the table '{}', because it does not exist or you do not have permission.",
                drop.table.value
            ),
        )),
    }
}

// ---- CREATE / DROP VIEW -------------------------------------------------

/// Parses a stored view definition back into its `SELECT`. The text was
/// validated at CREATE, so this only fails on catalog corruption.
fn parse_view_query(text: &str, view_name: &str) -> Result<Select, SqlError> {
    match truthdb_sql::parse(text)?.into_iter().next() {
        Some(Statement::Select(select)) => Ok(select),
        _ => Err(SqlError::message_only(
            208,
            format!("The definition of view '{view_name}' is not a SELECT."),
        )),
    }
}

fn exec_create_view(storage: &Storage, create: &CreateView) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if resolve_table(storage, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    // Validate the definition parses as a SELECT now; base-table and column
    // resolution is deferred to query time (SQL Server-style deferred name
    // resolution — a view over a not-yet-created table is allowed).
    parse_view_query(&create.query_text, bare)?;
    storage
        .rel_create_view(bare, &create.query_text)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// A parameter default must be a CONSTANT (SQL Server rejects at CREATE):
/// literals, NULL, and a signed literal — never variables or functions,
/// which would otherwise evaluate against each CALLER's scope and drift.
fn constant_default(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_) => true,
        ExprKind::Unary { expr, .. } => constant_default(expr),
        _ => false,
    }
}

fn exec_create_procedure(
    storage: &Storage,
    create: &CreateProcedure,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    // The builtin dispatcher checks `sp_executesql` BEFORE the catalog, so a
    // user procedure with that name would execute as the builtin while lock
    // ANALYSIS resolved the catalog first — an unanalyzed inner batch (the
    // review's shadow finding). Refuse the shadow outright.
    if bare.eq_ignore_ascii_case("sp_executesql") {
        return Err(SqlError::new(
            2714,
            16,
            6,
            "The name 'sp_executesql' is reserved for the system procedure.",
        ));
    }
    let params = create
        .params
        .iter()
        .map(|p| -> Result<ProcParamDef, SqlError> {
            // The declared type round-trips through the column-type spec
            // parser, exactly like table columns.
            let column_type = data_type_to_column_type(&p.data_type, &p.name)?;
            if let Some(text) = &p.default_text {
                let expr = truthdb_sql::parse_expr(text)?;
                if !constant_default(&expr) {
                    return Err(SqlError::new(
                        102,
                        15,
                        1,
                        format!(
                            "The default for parameter '@{}' must be a constant.",
                            p.name
                        ),
                    )
                    .at(p.span));
                }
            }
            Ok(ProcParamDef {
                name: p.name.clone(),
                type_spec: column_type.name(),
                default: p.default_text.clone(),
                output: p.output,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let procedure = ProcedureDef {
        params,
        body: create.body.clone(),
    };
    if create.alter {
        match resolve_table(storage, &create.name.value) {
            Some(def) if def.is_procedure() => {
                storage
                    .rel_alter_procedure(&def.name, procedure)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_procedure(bare, procedure)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

fn exec_drop_procedure(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, &name.value) {
        Some(def) if def.is_procedure() => {
            storage
                .rel_drop_table(&def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        Some(_) | None if if_exists => Ok(StatementResult::Done),
        _ => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the procedure '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

fn exec_create_function(
    storage: &Storage,
    create: &CreateFunction,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    let params = create
        .params
        .iter()
        .map(|p| -> Result<ProcParamDef, SqlError> {
            let column_type = data_type_to_column_type(&p.data_type, &p.name)?;
            if let Some(text) = &p.default_text {
                let expr = truthdb_sql::parse_expr(text)?;
                if !constant_default(&expr) {
                    return Err(SqlError::new(
                        102,
                        15,
                        1,
                        format!(
                            "The default for parameter '@{}' must be a constant.",
                            p.name
                        ),
                    )
                    .at(p.span));
                }
            }
            Ok(ProcParamDef {
                name: p.name.clone(),
                type_spec: column_type.name(),
                default: p.default_text.clone(),
                output: false,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let returns = match &create.returns {
        ReturnsClause::Scalar(return_type) => {
            let return_type = data_type_to_column_type(return_type, bare)?;
            // Validate the body: side-effect-free, ending in RETURN <expr> (SQL
            // Server's function-body rules). Re-parse under the function grammar.
            let body = truthdb_sql::parse_function_body(&create.body)?;
            validate_scalar_function_body(&body)?;
            FunctionReturns::Scalar {
                type_spec: return_type.name(),
                body: create.body.clone(),
            }
        }
        ReturnsClause::InlineTable => {
            // The body is a single SELECT expanded like a parameterized view —
            // validate it parses (no side-effect body check: it is a query).
            parse_view_query(&create.body, bare)?;
            FunctionReturns::InlineTable {
                select_text: create.body.clone(),
            }
        }
        ReturnsClause::MultiTable {
            var_name,
            columns_text,
        } => {
            // Validate the RETURNS table declaration builds (mirrors DECLARE @t
            // TABLE) and the body parses under the multi-statement TVF rules (may
            // populate the result / local table variables but not touch real
            // tables; must end in RETURN). Both are stored as text, re-parsed and
            // re-built per call.
            let (columns, primary_key) = truthdb_sql::parse_table_var_columns(columns_text)?;
            build_table_var_definition(var_name, &columns, &primary_key)?;
            let body = truthdb_sql::parse_table_function_body(&create.body)?;
            validate_multi_tvf_body(&body)?;
            FunctionReturns::MultiStatementTable {
                returns_var: var_name.clone(),
                columns_text: columns_text.clone(),
                body: create.body.clone(),
            }
        }
    };
    let function = FunctionDef { params, returns };
    if create.alter {
        match resolve_table(storage, &create.name.value) {
            Some(def) if def.is_function() => {
                storage
                    .rel_alter_function(&def.name, function)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_function(bare, function)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// Validates a scalar function's body against SQL Server's rules: every
/// statement must be side-effect-free (443 otherwise; a data-returning SELECT is
/// 444), and the last statement must be a `RETURN <expr>` (455).
fn validate_scalar_function_body(statements: &[Statement]) -> Result<(), SqlError> {
    for statement in statements {
        check_function_statement(statement)?;
    }
    match last_effective_statement(statements) {
        Some(Statement::Return { value: Some(_), .. }) => Ok(()),
        _ => Err(SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )),
    }
}

/// The body's terminal statement, unwrapping a trailing `BEGIN...END` block —
/// SQL Server's 455 check looks at the last statement of the body block.
fn last_effective_statement(statements: &[Statement]) -> Option<&Statement> {
    match statements.last() {
        Some(Statement::Block { body, .. }) => last_effective_statement(body),
        other => other,
    }
}

/// Rejects a statement a function body may not contain. Side-effecting
/// statements (DML, DDL, EXEC, transaction control, THROW/RAISERROR) are 443; a
/// data-returning SELECT is 444; control flow recurses.
fn check_function_statement(statement: &Statement) -> Result<(), SqlError> {
    match statement {
        Statement::Declare(_)
        | Statement::Set(_)
        | Statement::Return { .. }
        | Statement::Break { .. }
        | Statement::Continue { .. } => Ok(()),
        Statement::Block { body, .. } => {
            for inner in body {
                check_function_statement(inner)?;
            }
            Ok(())
        }
        Statement::If {
            then_branch,
            else_branch,
            ..
        } => {
            check_function_statement(then_branch)?;
            if let Some(else_branch) = else_branch {
                check_function_statement(else_branch)?;
            }
            Ok(())
        }
        Statement::While { body, .. } => check_function_statement(body),
        // An assignment SELECT (`SELECT @x = …`) is allowed — it returns no
        // rows. A SELECT that produces a result set cannot (444).
        Statement::Select(select)
            if select
                .items
                .iter()
                .all(|i| matches!(i, SelectItem::Assign { .. })) =>
        {
            Ok(())
        }
        Statement::Select(_) => Err(SqlError::new(
            444,
            16,
            2,
            "Select statements included within a function cannot return data to a client.",
        )),
        _ => Err(SqlError::new(
            443,
            16,
            1,
            "Invalid use of a side-effecting operator within a function.",
        )),
    }
}

/// Validates a multi-statement TVF body: like a scalar function it is
/// side-effect-free against the database, but it MAY populate table variables
/// (its result and any locals it declares), and its last statement must be a
/// (valueless) RETURN.
fn validate_multi_tvf_body(statements: &[Statement]) -> Result<(), SqlError> {
    for statement in statements {
        check_multi_tvf_statement(statement)?;
    }
    match last_effective_statement(statements) {
        Some(Statement::Return { .. }) => Ok(()),
        _ => Err(SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )),
    }
}

/// Rejects a statement a multi-statement TVF body may not contain. The only
/// difference from a scalar body (`check_function_statement`) is that DML into a
/// table variable (an `@`-target) is allowed — that is how the result is built.
fn check_multi_tvf_statement(statement: &Statement) -> Result<(), SqlError> {
    match statement {
        // INSERT into a table variable (the result or a local) is how a
        // multi-statement TVF produces rows.
        Statement::Insert(insert) if insert.table.value.starts_with('@') => Ok(()),
        Statement::DeclareTableVar { .. } => Ok(()),
        Statement::Block { body, .. } => {
            for inner in body {
                check_multi_tvf_statement(inner)?;
            }
            Ok(())
        }
        Statement::If {
            then_branch,
            else_branch,
            ..
        } => {
            check_multi_tvf_statement(then_branch)?;
            if let Some(else_branch) = else_branch {
                check_multi_tvf_statement(else_branch)?;
            }
            Ok(())
        }
        Statement::While { body, .. } => check_multi_tvf_statement(body),
        // Everything else defers to the scalar-body rules (DECLARE/SET/RETURN/
        // assignment-SELECT allowed; real-table DML/EXEC/DDL 443; data SELECT
        // 444).
        other => check_function_statement(other),
    }
}

fn exec_drop_function(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, &name.value) {
        Some(def) if def.is_function() => {
            storage
                .rel_drop_table(&def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        Some(_) | None if if_exists => Ok(StatementResult::Done),
        _ => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the function '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

/// `CREATE|ALTER TRIGGER <name> ON <table> AFTER <events> AS <body>`: registers
/// an AFTER DML trigger as a catalog object attached to its target table.
fn exec_create_trigger(
    storage: &Storage,
    create: &CreateTrigger,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    // The target must be an existing base table (not a view/procedure/function/
    // trigger). SQL Server 4929-class.
    let target = resolve_table(storage, &create.target.value)
        .ok_or_else(|| SqlError::invalid_object(&create.target.value).at(create.target.span))?;
    if target.is_view() || target.is_procedure() || target.is_function() || target.is_trigger() {
        return Err(SqlError::new(
            4929,
            16,
            1,
            format!(
                "Cannot create trigger '{bare}' because its target '{}' is not a base table.",
                target.name
            ),
        )
        .at(create.target.span));
    }
    // Validate the body parses under the in-procedure grammar (re-parsed per
    // firing). inserted/deleted resolve at firing time, not here.
    truthdb_sql::parse_procedure_body(&create.body)?;
    let events: Vec<catalog::TriggerEvent> = create
        .events
        .iter()
        .map(|e| match e {
            ast::TriggerEvent::Insert => catalog::TriggerEvent::Insert,
            ast::TriggerEvent::Update => catalog::TriggerEvent::Update,
            ast::TriggerEvent::Delete => catalog::TriggerEvent::Delete,
        })
        .collect();
    let trigger = TriggerDef {
        parent_object_id: target.object_id,
        events,
        body: create.body.clone(),
        is_disabled: false,
    };
    if create.alter {
        match resolve_table(storage, &create.name.value) {
            Some(def) if def.is_trigger() => {
                storage
                    .rel_alter_trigger(&def.name, trigger)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_trigger(bare, trigger)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

fn exec_drop_trigger(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, &name.value) {
        Some(def) if def.is_trigger() => {
            storage
                .rel_drop_table(&def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        Some(_) | None if if_exists => Ok(StatementResult::Done),
        _ => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the trigger '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

/// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` / `ALTER LOGIN <name>
/// {ENABLE | DISABLE}`. Logins are server principals in their own namespace
/// (disjoint from schema objects); the password is hashed here (on the worker —
/// CREATE/ALTER LOGIN is rare admin DDL, unlike verification which runs off the
/// worker per connection).
fn exec_create_login(storage: &Storage, create: &CreateLogin) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if create.alter {
        let Some(existing) = storage.rel_login(bare) else {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot alter the login '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(create.name.span));
        };
        let mut principal = existing
            .principal
            .clone()
            .expect("rel_login returns a login");
        if let Some(password) = &create.password {
            principal.password_blob = crate::auth::hash_password(password);
        }
        if let Some(disable) = create.disable {
            principal.is_disabled = disable;
        }
        storage
            .rel_alter_login(bare, principal)
            .map_err(|e| map_storage_err(e, &create.name.value))?;
        return Ok(StatementResult::Done);
    }
    if storage.rel_login(bare).is_some() {
        return Err(SqlError::new(
            15025,
            16,
            1,
            format!("The server principal '{bare}' already exists."),
        )
        .at(create.name.span));
    }
    let password = create
        .password
        .as_ref()
        .expect("CREATE LOGIN carries a password (parser-enforced)");
    let principal = PrincipalDef::login(
        crate::auth::hash_password(password),
        create.disable.unwrap_or(false),
    );
    storage
        .rel_create_login(bare, principal)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

fn exec_drop_login(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let dropped = storage
        .rel_drop_login(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    if !dropped && !if_exists {
        return Err(SqlError::new(
            15151,
            16,
            1,
            format!(
                "Cannot drop the login '{bare}', because it does not exist or you do not have permission."
            ),
        )
        .at(name.span));
    }
    Ok(StatementResult::Done)
}

/// `CREATE USER <name> [FOR LOGIN <login>]`. A database principal in its own
/// namespace (out of the object namespace), optionally mapped to a login.
fn exec_create_user(storage: &Storage, create: &CreateUser) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(create.name.span));
    }
    let login_sid = match &create.for_login {
        Some(login) => {
            let login_bare = strip_schema(&login.value);
            let Some(def) = storage.rel_login(login_bare) else {
                return Err(SqlError::new(
                    15007,
                    16,
                    1,
                    format!("'{login_bare}' is not a valid login or you do not have permission."),
                )
                .at(login.span));
            };
            Some(def.object_id)
        }
        None => None,
    };
    storage
        .rel_create_database_principal(bare, PrincipalDef::user(login_sid))
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// `CREATE ROLE <name>`.
fn exec_create_role(storage: &Storage, name: &Name) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(name.span));
    }
    storage
        .rel_create_database_principal(bare, PrincipalDef::role())
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `DROP USER`/`DROP ROLE`. `expect_role` selects which kind is being dropped;
/// a mismatch (DROP USER on a role, or vice versa) reports not-found for the
/// requested kind, as SQL Server does.
fn exec_drop_database_principal(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
    expect_role: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let kind = if expect_role { "role" } else { "user" };
    match storage.rel_database_principal(bare) {
        Some(def) if def.is_role() == expect_role => {}
        _ if if_exists => return Ok(StatementResult::Done),
        _ => {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot drop the {kind} '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(name.span));
        }
    }
    storage
        .rel_drop_database_principal(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
fn exec_alter_role_member(
    storage: &Storage,
    role: &Name,
    action: RoleMemberAction,
    member: &Name,
) -> Result<StatementResult, SqlError> {
    let role_bare = strip_schema(&role.value);
    let member_bare = strip_schema(&member.value);
    match action {
        RoleMemberAction::Add => storage.rel_add_role_member(role_bare, member_bare),
        RoleMemberAction::Drop => storage.rel_drop_role_member(role_bare, member_bare),
    }
    .map_err(|e| map_storage_err(e, &role.value))?;
    Ok(StatementResult::Done)
}

/// Maps a parsed permission action to its catalog form.
fn map_perm_action(action: PermissionAction) -> PermAction {
    match action {
        PermissionAction::Select => PermAction::Select,
        PermissionAction::Insert => PermAction::Insert,
        PermissionAction::Update => PermAction::Update,
        PermissionAction::Delete => PermAction::Delete,
        PermissionAction::Execute => PermAction::Execute,
        PermissionAction::References => PermAction::References,
        PermissionAction::Alter => PermAction::Alter,
    }
}

/// `GRANT|DENY|REVOKE <actions> ON <object> TO|FROM <grantees>`. The authority to
/// manage permissions is enforced by the DDL privilege gate in the dispatcher
/// (a bypassing principal — sysadmin / dbo / db_owner / internal). Here we just
/// resolve the securable and apply each (grantee, action).
fn exec_permission(
    storage: &Storage,
    stmt: &PermissionStatement,
    _sec: &SecurityContext,
) -> Result<StatementResult, SqlError> {
    // The securable must be a schema object (table, view, procedure, function).
    let Some(def) = resolve_table(storage, &stmt.object.value) else {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    };
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    }
    let object = def.name.clone(); // the canonical name = the rel.tables key
    for grantee in &stmt.grantees {
        let grantee_bare = strip_schema(&grantee.value);
        for action in &stmt.actions {
            let catalog_action = map_perm_action(*action);
            match stmt.kind {
                PermissionKind::Grant => {
                    storage.rel_grant_object(&object, grantee_bare, catalog_action, false)
                }
                PermissionKind::Deny => {
                    storage.rel_grant_object(&object, grantee_bare, catalog_action, true)
                }
                PermissionKind::Revoke => {
                    storage.rel_revoke_object(&object, grantee_bare, catalog_action)
                }
            }
            .map_err(|e| map_storage_err(e, &grantee.value).at(grantee.span))?;
        }
    }
    Ok(StatementResult::Done)
}

/// Schema and security DDL a non-privileged principal may not run. (GRANT/DENY/
/// REVOKE — `Permission` — is included: only a privileged principal manages
/// permissions.) Fine-grained database-scoped CREATE grants and the db_ddladmin
/// role are deferred: today any DDL requires bypass privilege.
fn is_privileged_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. }
    )
}

/// Resolves the AFTER triggers to fire for a DML on `target_name` for `event`,
/// plus the target's definition (for the pseudo-table schema). Empty when no
/// trigger exists anywhere (the cheap `rel_has_triggers` gate keeps the common
/// path free) or the target is not a base table.
fn after_triggers_for(
    storage: &Storage,
    target_name: &str,
    event: catalog::TriggerEvent,
) -> (Option<TableDef>, Vec<TableDef>) {
    if !storage.rel_has_triggers() {
        return (None, Vec::new());
    }
    match resolve_table(storage, target_name) {
        Some(def)
            if def.trigger.is_none()
                && def.procedure.is_none()
                && def.function.is_none()
                && def.view_query.is_none() =>
        {
            let triggers = storage.rel_triggers_for(def.object_id, event);
            (Some(def), triggers)
        }
        _ => (None, Vec::new()),
    }
}

/// Runs a DML statement (via `dml`) and fires its AFTER triggers atomically.
/// Under autocommit an implicit transaction is opened so the DML stages rather
/// than commits, so DML + triggers share one transaction (a trigger ROLLBACK
/// undoes the DML) and a trigger that ends the transaction raises 3609.
fn run_dml_with_triggers(
    storage: &Storage,
    txn_ctx: &mut TxnContext,
    target_def: &TableDef,
    triggers: Vec<TableDef>,
    dml: impl FnOnce(&mut TxnContext) -> Result<StatementResult, SqlError>,
) -> Result<StatementResult, SqlError> {
    let schema = target_def
        .schema()
        .map_err(|e| map_storage_err(e, &target_def.name))?;
    let implicit = !txn_ctx.in_txn();
    if implicit {
        exec_begin(storage, txn_ctx)?;
    }
    let tc_before = txn_ctx.trancount;
    // Arm the row-image capture, run the DML (staged on the transaction), then
    // take the captured images for the trigger bodies.
    TRIGGER_CAPTURE.with(|c| *c.borrow_mut() = Some(CapturedImages::default()));
    let dml_result = dml(txn_ctx);
    let images = TRIGGER_CAPTURE
        .with(|c| c.borrow_mut().take())
        .unwrap_or_default();
    let result = match dml_result {
        Ok(r) => r,
        Err(e) => {
            if implicit {
                txn_ctx.abort(storage);
            }
            return Err(e);
        }
    };
    let tables = std::rc::Rc::new(TriggerTables {
        schema,
        inserted: images.inserted,
        deleted: images.deleted,
    });
    // Fire each trigger once, in creation order, even for an empty image set.
    for trig_def in &triggers {
        let fired = fire_one_trigger(storage, txn_ctx, trig_def, &tables);
        // A trigger body that changed @@TRANCOUNT — a ROLLBACK/COMMIT that
        // reduced it or an unbalanced BEGIN that raised it — ENDED the
        // transaction (3609). This is checked BEFORE the error branch so the
        // idiomatic `ROLLBACK; RAISERROR` abort pattern does not doom a
        // transaction the trigger already tore down (which would wedge the
        // session doomed with no open transaction). `abort` normalizes the
        // state; surface the trigger's own error if it raised one, else 3609.
        if txn_ctx.trancount != tc_before {
            txn_ctx.abort(storage);
            return Err(fired.err().unwrap_or_else(|| {
                SqlError::new(
                    3609,
                    16,
                    1,
                    "The transaction ended in the trigger. The batch has been aborted.",
                )
            }));
        }
        // A trigger error with the transaction still open makes it
        // uncommittable. Roll back the IMPLICIT (autocommit) transaction opened
        // here; DOOM the caller's EXPLICIT one (leave it open, @@TRANCOUNT
        // intact, XACT_STATE() = -1) — SQL Server's uncommittable-transaction
        // semantics, so a TRY/CATCH sees the doomed state and must ROLLBACK
        // (its writes hit the 3930 guard), and an uncaught error terminates the
        // batch (statement_error_ladder does not continue past a doomed txn).
        // The doomed transaction's staged rows can never commit.
        if let Err(e) = fired {
            if implicit {
                txn_ctx.abort(storage);
            } else {
                txn_ctx.doomed = true;
            }
            return Err(e);
        }
    }
    if implicit {
        exec_commit(storage, txn_ctx)?;
    }
    Ok(result)
}

/// Fires one trigger body: parses it, runs it in the firing statement's
/// transaction (procedure posture — shared txn, fresh variable scope) with the
/// `inserted`/`deleted` view armed, bounded by the nesting cap. Direct
/// self-recursion is suppressed (recursive triggers OFF).
fn fire_one_trigger(
    storage: &Storage,
    txn_ctx: &mut TxnContext,
    trig_def: &TableDef,
    tables: &std::rc::Rc<TriggerTables>,
) -> Result<(), SqlError> {
    let trigger = trig_def.trigger.as_ref().expect("caller passes a trigger");
    // Recursive triggers OFF (the default) suppresses only DIRECT recursion: a
    // trigger whose own body re-fires itself (it is the currently-executing
    // trigger — top of the firing stack). Indirect recursion (a fires b fires a,
    // where a is deeper in the stack, not the top) stays enabled and is bounded
    // by the nesting cap, matching "nested triggers ON".
    if FIRING_TRIGGERS.with(|f| f.borrow().last() == Some(&trig_def.object_id)) {
        return Ok(());
    }
    let statements = truthdb_sql::parse_procedure_body(&trigger.body)?;
    // A trigger body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    if depth > 32 {
        EXEC_DEPTH.with(|d| d.set(d.get() - 1));
        return Err(SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        ));
    }
    // Procedure posture: fresh variable/table-variable scope, shared transaction.
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    FIRING_TRIGGERS.with(|f| f.borrow_mut().push(trig_def.object_id));
    let result = {
        let _trigger_scope = TriggerScope::enter(std::rc::Rc::clone(tables));
        let mut emitter = DiscardEmitter;
        let mut run = BatchRun {
            emitter: &mut emitter,
            deferred: Vec::new(),
            rowset_open: false,
            durability_failed: false,
            committed: false,
            last_error: None,
            function_return_type: None,
        };
        let flow = run_block(storage, &statements, txn_ctx, &mut run, false);
        // An error raised in the trigger body — a terminating one (Err), or a
        // non-terminating RAISERROR/THROW/failed-statement (severity >= 11) that
        // run_block records in last_error and NOT caught by an inner TRY/CATCH —
        // aborts the firing statement: SQL Server rolls back the DML and returns
        // the error. (A successful CATCH clears last_error, so a trigger that
        // handles its own error still succeeds.)
        flow.map(|_| ()).and_then(|()| match run.last_error.take() {
            Some(err) => Err(err),
            None => Ok(()),
        })
    };
    FIRING_TRIGGERS.with(|f| {
        f.borrow_mut().pop();
    });
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    result
}

fn exec_drop_view(storage: &Storage, drop: &DropView) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, &drop.name.value) {
        Some(def) if def.is_view() => {
            storage
                .rel_drop_table(&def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        // The object exists but is a base table, not a view.
        Some(_) => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the view '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        )),
        None if drop.if_exists => Ok(StatementResult::Done),
        None => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the view '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        )),
    }
}

// ---- CREATE / DROP INDEX ------------------------------------------------

/// SQL Server 1919: a (MAX)-class column cannot be an index/key column.
fn max_key_column_error(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1919,
        16,
        1,
        format!(
            "Column '{column}' in table '{table}' is of a type that is invalid for use as a \
             key column in an index."
        ),
    )
}

fn exec_create_index(storage: &Storage, create: &CreateIndex) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &create.table.value)
        .ok_or_else(|| SqlError::invalid_object(&create.table.value).at(create.table.span))?;
    reject_view_as_table(&def)?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let mut columns = Vec::with_capacity(create.columns.len());
    for col in &create.columns {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.name.value))
            .ok_or_else(|| index_column_missing(&col.name.value, &def.name).at(col.name.span))?;
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&col.name.value, &def.name).at(col.name.span));
        }
        columns.push((index, col.ascending));
    }
    // INCLUDE columns: resolved against the schema, no duplicates (1909, as
    // SQL Server). A *key* column may be INCLUDEd — a deliberate divergence
    // from SQL Server, which rejects that: our index keys are one-way
    // collation sort keys, so a query reading the key column itself can only
    // be covered by also storing its original value.
    let mut include = Vec::with_capacity(create.include.len());
    for col in &create.include {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.value))
            .ok_or_else(|| index_column_missing(&col.value, &def.name).at(col.span))?;
        // (MAX) columns cannot be INCLUDEd either — a divergence from SQL
        // Server (whose row-overflow indexes can carry them): our include
        // payloads live in ordinary index leaf cells under the tree cell cap.
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&col.value, &def.name).at(col.span));
        }
        if include.contains(&index) {
            return Err(SqlError::new(
                1909,
                16,
                1,
                format!(
                    "Cannot use duplicate column names in index. Column name '{}' listed more than once.",
                    col.value
                ),
            )
            .at(col.span));
        }
        include.push(index);
    }
    storage
        .rel_create_index(
            &def.name,
            create.name.value.clone(),
            columns,
            create.unique,
            include,
        )
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// SQL Server's 1911 for a `CREATE INDEX` column (key or `INCLUDE`) that does
/// not exist on the target table — where most statements answer 207.
fn index_column_missing(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1911,
        16,
        1,
        format!("Column name '{column}' does not exist in the target table or view '{table}'."),
    )
}

fn exec_drop_index(storage: &Storage, drop: &DropIndex) -> Result<StatementResult, SqlError> {
    // Resolve the table so the index lookup is scoped to it (index names are
    // per-table; two tables may share an index name).
    let table = resolve_table(storage, &drop.table.value)
        .ok_or_else(|| SqlError::invalid_object(&drop.table.value).at(drop.table.span))?;
    let existed = storage
        .rel_drop_index(&table.name, &drop.name.value)
        .map_err(|e| map_storage_err(e, &drop.name.value))?;
    if !existed {
        return Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the index '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        ));
    }
    Ok(StatementResult::Done)
}

// ---- ALTER TABLE --------------------------------------------------------

/// `ALTER DATABASE {name | CURRENT} SET READ_COMMITTED_SNAPSHOT /
/// ALLOW_SNAPSHOT_ISOLATION {ON|OFF}`. The batch's Database X lock has
/// quiesced the store: no snapshot is live, no writer is mid-transaction.
fn exec_alter_database(
    storage: &Storage,
    alter: &AlterDatabase,
    txn_ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if let Some(name) = &alter.name
        && !name.value.eq_ignore_ascii_case(&txn_ctx.database)
    {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                name.value
            ),
        )
        .at(name.span));
    }
    // A SNAPSHOT transaction idle between batches holds no locks, so the
    // batch's Database X does not prove no snapshot is live. Flipping the
    // options under one would reset (or stop publishing to) the store its
    // reads depend on; SQL Server waits the transactions out, TruthDB
    // refuses and lets the operator retry.
    if storage.has_registered_snapshots() {
        return Err(SqlError::new(
            5061,
            16,
            1,
            format!(
                "ALTER DATABASE failed because a lock could not be placed on database '{}'. \
                 Try again later.",
                txn_ctx.database
            ),
        ));
    }
    let mut rcsi = None;
    let mut allow_snapshot = None;
    let mut recovery_full = None;
    for (option, on) in &alter.options {
        match option {
            DatabaseOption::ReadCommittedSnapshot => rcsi = Some(*on),
            DatabaseOption::AllowSnapshotIsolation => allow_snapshot = Some(*on),
            // For Recovery the bool is the mode: true = FULL, false = SIMPLE.
            DatabaseOption::Recovery => recovery_full = Some(*on),
        }
    }
    storage
        .rel_set_db_options(rcsi, allow_snapshot, recovery_full)
        .map_err(|err| map_storage_err(err, &txn_ctx.database))?;
    Ok(StatementResult::Done)
}

fn exec_alter_table(
    storage: &Storage,
    alter: &AlterTable,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &alter.table.value)
        .ok_or_else(|| SqlError::invalid_object(&alter.table.value).at(alter.table.span))?;
    reject_view_as_table(&def)?;
    match &alter.action {
        AlterAction::AddColumn(column) => alter_add_column(storage, &def, column, eval_ctx),
        AlterAction::AddCheck(check) => alter_add_check(storage, &def, check, eval_ctx),
        AlterAction::AddForeignKey(fk) => alter_add_foreign_key(storage, &def, fk),
        AlterAction::DropConstraint(name) => alter_drop_constraint(storage, &def, name),
    }
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY (...) REFERENCES ...`.
/// Validates the constraint and every existing row (WITH CHECK): a child row
/// referencing a missing parent is 547 and the constraint is not added.
fn alter_add_foreign_key(
    storage: &Storage,
    def: &TableDef,
    fk: &ForeignKey,
) -> Result<StatementResult, SqlError> {
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let parent_bare = strip_schema(&fk.parent.value);
    let parent_pk: Vec<(String, ColumnType)> = if parent_bare.eq_ignore_ascii_case(&def.name) {
        def.key_columns
            .iter()
            .map(|&i| {
                (
                    schema.columns[i].name.clone(),
                    schema.columns[i].column_type,
                )
            })
            .collect()
    } else {
        let parent = resolve_table(storage, &fk.parent.value)
            .ok_or_else(|| SqlError::invalid_object(&fk.parent.value).at(fk.parent.span))?;
        let pschema = parent
            .schema()
            .map_err(|e| map_storage_err(e, &parent.name))?;
        parent
            .key_columns
            .iter()
            .map(|&i| {
                (
                    pschema.columns[i].name.clone(),
                    pschema.columns[i].column_type,
                )
            })
            .collect()
    };
    let existing_names: Vec<String> = def
        .check_constraints
        .iter()
        .map(|c| c.name.clone())
        .chain(def.foreign_keys.iter().map(|f| f.name.clone()))
        .collect();
    let new_def = bind_foreign_key(
        fk,
        &schema.columns,
        &def.name,
        &parent_pk,
        parent_bare,
        &existing_names,
    )?;

    // WITH CHECK: every existing child row must satisfy the new foreign key
    // (its sibling rows count for a self-reference).
    let rows = storage
        .rel_scan(&def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        if let Some(key) = fk_key(&new_def, row)
            && !fk_parent_exists(storage, &new_def, &key, def, &rows)?
        {
            return Err(fk_child_violation(
                &new_def.name,
                "ALTER TABLE",
                &new_def.parent,
            ));
        }
    }

    let mut fks = def.foreign_keys.clone();
    fks.push(new_def);
    storage
        .rel_set_foreign_keys(&def.name, fks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ADD <column>`: appends the column to the catalog and
/// rewrites every existing row under the new schema. The row codec is
/// positional (every offset derives from the schema, with no per-row version
/// stamp), so a metadata-only ADD cannot exist — the rewrite is the honest
/// implementation, one transactional statement under the ALTER's exclusive
/// lock. Existing rows take a FROZEN fill: NULL, or the DEFAULT evaluated
/// once now (SQL Server freezes it the same way); later INSERTs evaluate the
/// live default text per row like any other column.
fn alter_add_column(
    storage: &Storage,
    def: &catalog::TableDef,
    column: &ColumnDef,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    if def
        .columns
        .iter()
        .any(|(name, _, _)| name.eq_ignore_ascii_case(&column.name.value))
    {
        return Err(SqlError::new(
            2705,
            16,
            4,
            format!(
                "Column names in each table must be unique. Column name '{}' is specified more than once.",
                column.name.value
            ),
        )
        .at(column.name.span));
    }
    // The plan's scope: a plain column with nullability, DEFAULT and COLLATE.
    // Constraint-carrying additions are their own statements in T-SQL anyway.
    if column.primary_key
        || column.unique
        || column.identity.is_some()
        || !column.checks.is_empty()
        || !column.foreign_keys.is_empty()
    {
        return Err(SqlError::new(
            40510,
            16,
            1,
            "ALTER TABLE ADD supports a plain column (with NULL/NOT NULL, DEFAULT and COLLATE); add constraints with their own ALTER TABLE ADD CONSTRAINT statements.",
        )
        .at(column.span));
    }
    let bound = bind_column(column)?;
    // An authoritative emptiness probe (one-row scan under the ALTER's
    // exclusive lock) — the row counter is a statistic and must not become
    // load-bearing here: an under-count would let NULL fills into a NOT NULL
    // column, and a pre-upgrade table without a counter would 4901 even when
    // empty.
    let has_rows = {
        let mut probe = Vec::new();
        storage
            .rel_scan_slice(&def.name, ScanCursor::start(), 1, None, &mut probe)
            .map_err(|err| map_storage_err(err, &def.name))?;
        !probe.is_empty()
    };
    // The frozen fill existing rows take.
    let fill = match &column.default {
        Some(text) => {
            let sql_value = eval_default(text, eval_ctx)?;
            value::sql_to_datum(&sql_value, &bound.column_type, &bound.name)?
        }
        None => Datum::Null,
    };
    if !bound.nullable && fill.is_null() && has_rows {
        return Err(SqlError::new(
            4901,
            16,
            1,
            format!(
                "ALTER TABLE only allows columns to be added that can contain nulls, or have a DEFAULT definition specified, or the column being added is an identity or timestamp column, or alternatively if none of the previous conditions are satisfied the table must be empty to allow addition of this column. Column '{}' cannot be added to non-empty table '{}' because it does not satisfy these conditions.",
                bound.name, def.name
            ),
        )
        .at(column.span));
    }
    storage
        .rel_alter_add_column(&def.name, bound, column.default.clone(), fill)
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] CHECK (expr)`. Validates the new
/// constraint against every existing row (SQL Server's default WITH CHECK); a
/// violating row is error 547 and the constraint is not added.
fn alter_add_check(
    storage: &Storage,
    def: &TableDef,
    check: &CheckConstraint,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    // Constraint names are unique across kinds (CHECK and FOREIGN KEY).
    let existing: Vec<String> = def
        .check_constraints
        .iter()
        .map(|c| c.name.clone())
        .chain(def.foreign_keys.iter().map(|f| f.name.clone()))
        .collect();
    let new_def = bind_check(check, &schema.columns, &def.name, &existing)?;

    // WITH CHECK: no existing row may violate the new constraint.
    let compiled = vec![(
        new_def.name.clone(),
        truthdb_sql::parse_expr(&new_def.predicate)?,
    )];
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let rows = storage
        .rel_scan(&def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        let scope = row_values(row, &types);
        enforce_checks(
            &compiled,
            &scope,
            &resolver,
            eval_ctx,
            "ALTER TABLE",
            &def.name,
        )?;
    }

    let mut checks = def.check_constraints.clone();
    checks.push(new_def);
    storage
        .rel_set_check_constraints(&def.name, checks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... DROP CONSTRAINT name`. Removes a CHECK or FOREIGN KEY
/// constraint by name (case-insensitive); an unknown name is error 3728.
fn alter_drop_constraint(
    storage: &Storage,
    def: &TableDef,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    if def
        .check_constraints
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&name.value))
    {
        let checks: Vec<catalog::CheckDef> = def
            .check_constraints
            .iter()
            .filter(|c| !c.name.eq_ignore_ascii_case(&name.value))
            .cloned()
            .collect();
        storage
            .rel_set_check_constraints(&def.name, checks)
            .map_err(|e| map_storage_err(e, &def.name))?;
        return Ok(StatementResult::Done);
    }
    if def
        .foreign_keys
        .iter()
        .any(|f| f.name.eq_ignore_ascii_case(&name.value))
    {
        let fks: Vec<catalog::ForeignKeyDef> = def
            .foreign_keys
            .iter()
            .filter(|f| !f.name.eq_ignore_ascii_case(&name.value))
            .cloned()
            .collect();
        storage
            .rel_set_foreign_keys(&def.name, fks)
            .map_err(|e| map_storage_err(e, &def.name))?;
        return Ok(StatementResult::Done);
    }
    Err(SqlError::new(
        3728,
        16,
        1,
        format!("'{}' is not a constraint.", name.value),
    )
    .at(name.span))
}

// ---- INSERT -------------------------------------------------------------

fn exec_insert(
    storage: &Storage,
    insert: &Insert,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<(StatementResult, Option<i64>), SqlError> {
    let def = resolve_table(storage, &insert.table.value)
        .ok_or_else(|| SqlError::invalid_object(&insert.table.value).at(insert.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(&def, &eval_ctx.security, PermAction::Insert)
        .map_err(|e| e.at(insert.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    let increment = def.identity.map(|s| s.increment).unwrap_or(0);

    // CHECK constraints are parsed once and evaluated against each built row.
    let checks = parse_checks(&def)?;
    let check_resolver = SchemaScope { schema: &schema };
    let check_types = schema_types(&schema);

    // Target column indices. An explicit list may not name the identity column
    // (8101) or repeat a column (264); an omitted list targets every
    // non-identity column in order (identity is server-generated).
    let target: Vec<usize> = match &insert.columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for n in names {
                let index = column_index(&schema, &n.value)
                    .ok_or_else(|| SqlError::invalid_column(&n.value).at(n.span))?;
                if Some(index) == identity_col {
                    return Err(SqlError::new(
                        8101,
                        16,
                        1,
                        format!(
                            "An explicit value for the identity column in table '{}' can only be specified when a column list is used and IDENTITY_INSERT is ON.",
                            def.name
                        ),
                    )
                    .at(n.span));
                }
                if indices.contains(&index) {
                    return Err(SqlError::new(
                        264,
                        16,
                        1,
                        format!(
                            "The column name '{}' is specified more than once in the SET clause or column list of an INSERT.",
                            n.value
                        ),
                    )
                    .at(n.span));
                }
                indices.push(index);
            }
            indices
        }
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };

    // Gather the input rows (each of length `target.len()`) from either the
    // VALUES tuples or a SELECT. A SELECT is fully materialized before any
    // insert, so `INSERT INTO t SELECT ... FROM t` is Halloween-safe.
    let input_rows = insert_input_rows(storage, &insert.source, target.len(), eval_ctx)?;

    // Reserve identity values for the whole batch up front. A failed insert
    // consumes them (a gap), but a value is never reused (SQL Server-faithful).
    let identity_first = if identity_col.is_some() {
        storage
            .rel_reserve_identity(&def.name, input_rows.len())
            .map_err(|e| map_storage_err(e, &def.name))?
    } else {
        None
    };

    // Build every row up front; insert them as one atomic statement.
    let mut rows = Vec::with_capacity(input_rows.len());
    for (row_no, input) in input_rows.iter().enumerate() {
        check_cancelled()?;
        // Full row in schema order: unspecified columns start NULL.
        let mut values = vec![Datum::Null; ncols];
        for (position, sql_value) in target.iter().zip(input) {
            let column = &schema.columns[*position];
            if sql_value.is_null() && !column.nullable {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
            values[*position] = value::sql_to_datum(sql_value, &column.column_type, &column.name)?;
        }
        // Server-generated identity value for this row.
        if let (Some(col), Some(first)) = (identity_col, identity_first) {
            let v = first.saturating_add((row_no as i64).saturating_mul(increment));
            values[col] = identity_datum(&schema.columns[col].column_type, v)?;
        }
        // DEFAULTs for columns that were neither targeted nor identity.
        for (index, column) in schema.columns.iter().enumerate() {
            if !values[index].is_null() || target.contains(&index) || Some(index) == identity_col {
                continue;
            }
            if let Some(text) = def.default_for(index) {
                let sql_value = eval_default(text, eval_ctx)?;
                values[index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
            }
        }
        // NOT NULL enforcement after defaults/identity are applied.
        for (index, column) in schema.columns.iter().enumerate() {
            if !column.nullable && values[index].is_null() {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
        }
        if !checks.is_empty() {
            let scope = row_values(&values, &check_types);
            enforce_checks(
                &checks,
                &scope,
                &check_resolver,
                eval_ctx,
                "INSERT",
                &def.name,
            )?;
        }
        rows.push(values);
    }

    // FOREIGN KEY (child side): each new row must reference an existing parent
    // (a sibling row in this batch counts for a self-reference).
    if !def.foreign_keys.is_empty() {
        for row in &rows {
            enforce_child_fks(storage, &def, row, &rows, "INSERT", true)?;
        }
    }

    // Capture the new row images for an AFTER trigger's `inserted` table (only
    // when a capture is armed — the no-trigger path clones nothing).
    capture_trigger_images(|| (rows.clone(), Vec::new()));
    let inserted = rows.len() as u64;
    storage
        .rel_insert_many(&def.name, rows, scope)
        .map_err(|err| map_storage_err(err, &def.name))?;
    // The last identity value generated (for SCOPE_IDENTITY()): the reserved
    // first value plus the increment for each subsequent row. `None` when the
    // table has no identity column or no rows were inserted.
    let last_identity = match (identity_col, identity_first) {
        (Some(_), Some(first)) if inserted > 0 => {
            Some(first.saturating_add((inserted as i64 - 1).saturating_mul(increment)))
        }
        _ => None,
    };
    Ok((StatementResult::RowsAffected(inserted), last_identity))
}

/// `INSERT [INTO] @t ...`: appends rows to an in-memory table variable. No
/// Storage, no lock, no WAL, no identity/default/CHECK/FK (deferred) — just the
/// declared column coercion, NOT NULL, and PRIMARY KEY uniqueness, all in memory
/// so a ROLLBACK leaves the rows intact (SQL Server table-variable semantics).
fn exec_insert_table_var(
    storage: &Storage,
    insert: &Insert,
    ctx: &mut TxnContext,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let key = insert
        .table
        .value
        .trim_start_matches('@')
        .to_ascii_lowercase();
    let (schema, key_columns, defaults) = {
        let tv = ctx
            .table_variables
            .get(&key)
            .ok_or_else(|| must_declare_table_var(&insert.table.value).at(insert.table.span))?;
        (
            tv.schema.clone(),
            tv.key_columns.clone(),
            tv.defaults.clone(),
        )
    };
    let ncols = schema.columns.len();
    // Target columns: an explicit list resolves against the declared schema (264
    // for a repeat); an omitted list targets every column in order.
    let target: Vec<usize> = match &insert.columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for n in names {
                let index = column_index(&schema, &n.value)
                    .ok_or_else(|| SqlError::invalid_column(&n.value).at(n.span))?;
                if indices.contains(&index) {
                    return Err(SqlError::new(
                        264,
                        16,
                        1,
                        format!(
                            "The column name '{}' is specified more than once in the SET clause or column list of an INSERT.",
                            n.value
                        ),
                    )
                    .at(n.span));
                }
                indices.push(index);
            }
            indices
        }
        None => (0..ncols).collect(),
    };
    // A SELECT source is fully materialized here before any append, so
    // `INSERT @t SELECT ... FROM @t` reads @t's pre-insert rows (Halloween-safe).
    let input_rows = insert_input_rows(storage, &insert.source, target.len(), eval_ctx)?;
    let mut new_rows = Vec::with_capacity(input_rows.len());
    for input in &input_rows {
        check_cancelled()?;
        let mut values = vec![Datum::Null; ncols];
        for (position, sql_value) in target.iter().zip(input) {
            let column = &schema.columns[*position];
            values[*position] = value::sql_to_datum(sql_value, &column.column_type, &column.name)?;
        }
        // DEFAULTs fill columns that were not targeted and are still NULL,
        // before the NOT NULL check — so `c INT NOT NULL DEFAULT 5` inserts 5,
        // not a spurious 515.
        for (index, column) in schema.columns.iter().enumerate() {
            if !values[index].is_null() || target.contains(&index) {
                continue;
            }
            if let Some(text) = &defaults[index] {
                let sql_value = eval_default(text, eval_ctx)?;
                values[index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
            }
        }
        // NOT NULL after defaults applied; unspecified columns without a
        // default remain NULL.
        for (index, column) in schema.columns.iter().enumerate() {
            if !column.nullable && values[index].is_null() {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
        }
        new_rows.push(values);
    }
    let tv = ctx.table_variables.get_mut(&key).expect("checked above");
    // PRIMARY KEY uniqueness (collation-aware, against existing and same-batch
    // rows). Checked before any append, so a violation appends nothing.
    if !key_columns.is_empty() {
        let mut seen: std::collections::HashSet<Vec<u8>> = tv
            .rows
            .iter()
            .filter_map(|r| crate::relstore::key::encode_key(&schema, &key_columns, r).ok())
            .collect();
        for row in &new_rows {
            let encoded = crate::relstore::key::encode_key(&schema, &key_columns, row)
                .map_err(|e| SqlError::message_only(245, e.to_string()))?;
            if !seen.insert(encoded) {
                return Err(SqlError::new(
                    2627,
                    14,
                    1,
                    "Violation of PRIMARY KEY constraint. Cannot insert duplicate key in a table variable.",
                ));
            }
        }
    }
    let inserted = new_rows.len() as u64;
    tv.rows.extend(new_rows);
    Ok(StatementResult::RowsAffected(inserted))
}

/// SQL Server 1087: a `@t` table variable used before it was declared.
fn must_declare_table_var(name: &str) -> SqlError {
    SqlError::new(
        1087,
        15,
        2,
        format!("Must declare the table variable \"{name}\"."),
    )
}

/// Produces the input rows an INSERT supplies, each already in target-column
/// order and as [`SqlValue`]s: `VALUES` tuples are evaluated as constants; a
/// `SELECT` is executed and its rows converted. Rejects an arity mismatch
/// against the target column count (110 for VALUES, 120/121 for SELECT).
fn insert_input_rows(
    storage: &Storage,
    source: &InsertSource,
    target_len: usize,
    eval_ctx: &EvalContext,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    match source {
        InsertSource::Values(rows) => {
            let mut out = Vec::with_capacity(rows.len());
            for exprs in rows {
                if exprs.len() != target_len {
                    return Err(SqlError::new(
                        110,
                        15,
                        1,
                        "There are fewer or more columns in the INSERT statement than values specified in the VALUES clause.",
                    ));
                }
                let mut vals = Vec::with_capacity(target_len);
                for expr in exprs {
                    vals.push(eval_constant(expr, eval_ctx)?);
                }
                out.push(vals);
            }
            Ok(out)
        }
        InsertSource::Select(select) => {
            let rowset = exec_select(storage, select, eval_ctx)?;
            if rowset.columns.len() != target_len {
                let (number, more_or_fewer) = if rowset.columns.len() < target_len {
                    (120, "fewer")
                } else {
                    (121, "more")
                };
                return Err(SqlError::new(
                    number,
                    15,
                    1,
                    format!(
                        "The select list for the INSERT statement contains {more_or_fewer} items than the insert list. The number of SELECT values must match the number of INSERT columns."
                    ),
                ));
            }
            let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
            Ok(rowset
                .rows
                .iter()
                .map(|row| row_values(row, &types))
                .collect())
        }
    }
}

/// Evaluates a column DEFAULT (re-parsed from its stored source text).
fn eval_default(text: &str, eval_ctx: &EvalContext) -> Result<SqlValue, SqlError> {
    let expr = truthdb_sql::parse_expr(text)?;
    eval_constant(&expr, eval_ctx)
}

/// Coerces a generated identity value to its column's integer type, erroring
/// on overflow.
fn identity_datum(column_type: &ColumnType, v: i64) -> Result<Datum, SqlError> {
    let overflow = || {
        SqlError::new(
            8115,
            16,
            1,
            format!(
                "Arithmetic overflow error converting IDENTITY to data type {}.",
                column_type.name()
            ),
        )
    };
    match column_type {
        ColumnType::TinyInt => u8::try_from(v).map(Datum::TinyInt).map_err(|_| overflow()),
        ColumnType::SmallInt => i16::try_from(v)
            .map(Datum::SmallInt)
            .map_err(|_| overflow()),
        ColumnType::Int => i32::try_from(v).map(Datum::Int).map_err(|_| overflow()),
        ColumnType::BigInt => Ok(Datum::BigInt(v)),
        // Non-integer identity columns are rejected at CREATE TABLE.
        _ => Ok(Datum::Null),
    }
}

// ---- UPDATE / DELETE ----------------------------------------------------

/// The DML target scan: current rows under lock-based isolation; under
/// SNAPSHOT isolation (the statement's thread-local snapshot is set), the
/// transaction-snapshot rows instead, each carrying a conflict mark when its
/// current state was changed or deleted by a writer the snapshot cannot see.
/// Targeting a marked row is SQL Server's 3960 update conflict.
fn scan_located_for_dml(
    storage: &Storage,
    def: &TableDef,
) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, SqlError> {
    match current_snapshot() {
        Some(snap) => storage
            .rel_scan_located_snapshot(&def.name, snap)
            .map_err(|e| map_storage_err(e, &def.name)),
        None => Ok(storage
            .rel_scan_located(&def.name)
            .map_err(|e| map_storage_err(e, &def.name))?
            .into_iter()
            .map(|(locator, row)| (locator, row, false))
            .collect()),
    }
}

/// SQL Server 3960: a SNAPSHOT transaction tried to write a row a later
/// committed transaction already changed. The whole transaction is rolled
/// back (see `exec_statement`'s 3960 handling), as SQL Server does.
fn update_conflict_error(table: &str, database: &str) -> SqlError {
    SqlError::new(
        3960,
        16,
        1,
        format!(
            "Snapshot isolation transaction aborted due to update conflict. You cannot use \
             snapshot isolation to access table '{table}' directly or indirectly in database \
             '{database}' to update, delete, or insert the row that has been modified or \
             deleted by another transaction. Retry the transaction or change the isolation \
             level for the update/delete statement."
        ),
    )
}

fn exec_update(
    storage: &Storage,
    update: &Update,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &update.table.value)
        .ok_or_else(|| SqlError::invalid_object(&update.table.value).at(update.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(&def, &eval_ctx.security, PermAction::Update)
        .map_err(|e| e.at(update.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };
    let identity_col = def.identity.map(|s| s.column);
    let checks = parse_checks(&def)?;

    // Resolve each SET target once; an IDENTITY column cannot be updated.
    let mut assignments: Vec<(usize, &Expr)> = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let index = column_index(&schema, &assignment.column.value).ok_or_else(|| {
            SqlError::invalid_column(&assignment.column.value).at(assignment.column.span)
        })?;
        if Some(index) == identity_col {
            return Err(SqlError::new(
                8102,
                16,
                1,
                format!(
                    "Cannot update identity column '{}'.",
                    assignment.column.value
                ),
            )
            .at(assignment.column.span));
        }
        if assignments.iter().any(|(i, _)| *i == index) {
            return Err(SqlError::new(
                264,
                16,
                1,
                format!(
                    "The column name '{}' is specified more than once in the SET clause or column list of an INSERT. A column cannot be assigned more than one value in the same clause.",
                    assignment.column.value
                ),
            )
            .at(assignment.column.span));
        }
        assignments.push((index, &assignment.value));
    }

    // Materialize the whole table (Halloween-safe), filter, and compute new
    // rows before any mutation.
    let located = scan_located_for_dml(storage, &def)?;
    let types = schema_types(&schema);
    let mut updates = Vec::new();
    for (locator, row, conflict) in located {
        check_cancelled()?;
        if !predicate_true(&update.where_clause, &row, &types, &resolver, eval_ctx)? {
            continue;
        }
        if conflict {
            return Err(update_conflict_error(&def.name, &eval_ctx.database));
        }
        // Every SET expression sees the pre-update row; keep the old values
        // for secondary-index maintenance.
        let old_values = row.clone();
        let old_scope = row_values(&row, &types);
        let mut new_row = row;
        for (index, expr) in &assignments {
            let column = &schema.columns[*index];
            let sql_value = eval::eval(expr, &old_scope, &resolver, eval_ctx)?;
            if sql_value.is_null() && !column.nullable {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &update.table.value,
                ));
            }
            new_row[*index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
        }
        if !checks.is_empty() {
            let scope = row_values(&new_row, &types);
            enforce_checks(&checks, &scope, &resolver, eval_ctx, "UPDATE", &def.name)?;
        }
        updates.push((locator, old_values, new_row));
    }

    // FOREIGN KEY (child side): each updated row must still reference a valid
    // parent. Self-referencing FKs are validated separately below.
    if !def.foreign_keys.is_empty() {
        for (_, _, new_row) in &updates {
            enforce_child_fks(storage, &def, new_row, &[], "UPDATE", false)?;
        }
    }
    // FOREIGN KEY (parent side, other tables): a row whose primary key changes
    // vacates its old key; no surviving child in ANOTHER table may still
    // reference it (NO ACTION). Self-references are handled by the snapshot.
    if def.is_tree() {
        let removed: Vec<Vec<Datum>> = updates
            .iter()
            .filter_map(|(_, old, new)| {
                let old_pk = pk_of(&def, old);
                (old_pk != pk_of(&def, new)).then_some(old_pk)
            })
            .collect();
        enforce_parent_fks(storage, &def, &removed, "UPDATE", false)?;
    }
    // FOREIGN KEY (self-reference): a self-referencing table's own foreign keys
    // must hold against the state the UPDATE produces — a pre-mutation probe
    // sees stale rows. Every surviving row's non-NULL self-FK key must match a
    // surviving primary key.
    if def.is_tree()
        && def
            .foreign_keys
            .iter()
            .any(|fk| fk.parent.eq_ignore_ascii_case(&def.name))
    {
        let old_pks: Vec<Vec<Datum>> = updates.iter().map(|(_, old, _)| pk_of(&def, old)).collect();
        let mut post_rows: Vec<Vec<Datum>> = storage
            .rel_scan(&def.name)
            .map_err(|e| map_storage_err(e, &def.name))?
            .into_iter()
            .filter(|r| !old_pks.contains(&pk_of(&def, r)))
            .collect();
        post_rows.extend(updates.iter().map(|(_, _, new)| new.clone()));
        // Fold the surviving PKs and each FK reference by the (self-referenced)
        // PK collation, so a case-insensitive self-reference matches a case-
        // variant sibling — consistent with the INSERT batch path
        // (`fk_parent_exists`) and the DELETE path (`enforce_parent_fks`).
        let key_coll: Vec<Option<String>> = def
            .key_columns
            .iter()
            .map(|&i| def.collations.get(i).cloned().flatten())
            .collect();
        let post_pks: Vec<Vec<u8>> = post_rows
            .iter()
            .map(|r| collated_key(&pk_of(&def, r), &key_coll))
            .collect();
        for r in &post_rows {
            for fk in def
                .foreign_keys
                .iter()
                .filter(|fk| fk.parent.eq_ignore_ascii_case(&def.name))
            {
                if let Some(key) = fk_key(fk, r)
                    && !post_pks.contains(&collated_key(&key, &key_coll))
                {
                    return Err(fk_child_violation(&fk.name, "UPDATE", &fk.parent));
                }
            }
        }
    }

    // Capture the old/new images for an AFTER trigger's `deleted`/`inserted`
    // tables (a row that did not change still appears in both, as SQL Server
    // does — every matched row is in `updates`).
    capture_trigger_images(|| {
        (
            updates.iter().map(|(_, _, new)| new.clone()).collect(),
            updates.iter().map(|(_, old, _)| old.clone()).collect(),
        )
    });
    let count = storage
        .rel_update_located(&def.name, updates, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

fn exec_delete(
    storage: &Storage,
    delete: &Delete,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &delete.table.value)
        .ok_or_else(|| SqlError::invalid_object(&delete.table.value).at(delete.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(&def, &eval_ctx.security, PermAction::Delete)
        .map_err(|e| e.at(delete.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };

    let types = schema_types(&schema);
    let located = scan_located_for_dml(storage, &def)?;
    let mut targets = Vec::new();
    for (locator, row, conflict) in located {
        check_cancelled()?;
        if predicate_true(&delete.where_clause, &row, &types, &resolver, eval_ctx)? {
            if conflict {
                return Err(update_conflict_error(&def.name, &eval_ctx.database));
            }
            // Keep the row values for secondary-index maintenance.
            targets.push((locator, row));
        }
    }

    // FOREIGN KEY (parent side): no surviving child may reference a deleted row
    // (a self-referencing row that is itself deleted does not count).
    if def.is_tree() {
        let removed: Vec<Vec<Datum>> = targets.iter().map(|(_, row)| pk_of(&def, row)).collect();
        enforce_parent_fks(storage, &def, &removed, "DELETE", true)?;
    }

    // Capture the deleted images for an AFTER trigger's `deleted` table.
    capture_trigger_images(|| {
        (
            Vec::new(),
            targets.iter().map(|(_, row)| row.clone()).collect(),
        )
    });
    let count = storage
        .rel_delete_located(&def.name, targets, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

/// Resolver over a single table's schema columns, carrying per-column collation.
/// UPDATE/DELETE/CHECK predicate evaluation must go through this (not a bare
/// `Vec<String>`, whose `ColumnResolver::collation` reports the case-insensitive
/// default for *every* column) so an explicit `_CS`/`_BIN` column compares
/// case-sensitively — otherwise a `DELETE ... WHERE cs_col = 'abc'` would fold
/// case and remove case-variant rows it must keep.
struct SchemaScope<'a> {
    schema: &'a Schema,
}

impl truthdb_sql::eval::ColumnResolver for SchemaScope<'_> {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    fn collation(&self, index: usize) -> CollationSensitivity {
        CollationSensitivity::from_optional(
            self.schema
                .columns
                .get(index)
                .and_then(|c| c.collation.as_deref()),
        )
    }
}

fn schema_types(schema: &Schema) -> Vec<ColumnType> {
    schema.columns.iter().map(|c| c.column_type).collect()
}

/// Evaluates an optional WHERE predicate against a row. Absent WHERE matches
/// all rows; a NULL/UNKNOWN result does not match; a non-boolean predicate is
/// error 4145 (same rule as SELECT).
fn predicate_true(
    where_clause: &Option<Expr>,
    row: &[Datum],
    types: &[ColumnType],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
) -> Result<bool, SqlError> {
    let Some(predicate) = where_clause else {
        return Ok(true);
    };
    match eval::eval(predicate, &row_values(row, types), resolver, eval_ctx)? {
        SqlValue::Bool(b) => Ok(b),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is expected, near 'WHERE'.",
        )
        .at(predicate.span)),
    }
}

// ---- SELECT -------------------------------------------------------------

/// Rows a table scan reads per slice before dropping the storage lock and
/// letting another session in. Large enough that the per-slice overhead (a lock
/// acquisition and a catalog lookup) is noise against decoding the rows, small
/// enough that a big scan yields often.
const SCAN_SLICE_ROWS: usize = 1024;

struct Source {
    columns: Vec<ResultColumn>,
    /// Per-column table qualifier (alias or table name; `None` = virtual/
    /// constant source), parallel to `columns`. Drives multi-table resolution.
    qualifiers: Vec<Option<String>>,
    /// Per-column collation names (parallel to `columns`; `None` = database
    /// default). Used by ORDER BY on character columns.
    collations: Vec<Option<String>>,
    /// Rows of typed values (real-table Datums; virtual sources build them).
    rows: SourceRows,
}

/// A source's rows: already whole, or pulled slice-by-slice from a base-table
/// scan as the consumer iterates (Stage 8 streaming scans, the input side). A
/// consumer that filters or folds row-at-a-time holds one slice, not the
/// table; one that needs the whole input calls [`SourceRows::materialize`].
enum SourceRows {
    Materialized(Vec<Vec<Datum>>),
    Scan(ScanStream),
}

/// A base-table scan not yet read: full-width rows, [`SCAN_SLICE_ROWS`] at a
/// time on the resumable cursor. The storage lock is taken per slice, as
/// everywhere since #96; under every isolation level that takes read locks
/// the table's S lock spans the whole batch, so lazy pulling changes no
/// isolation semantics — and READ UNCOMMITTED took no read lock before
/// either (the cursor's per-page object check safe-stops on a recycled
/// page, as the B+ tree layer documents).
struct ScanStream {
    table: String,
    cursor: ScanCursor,
}

impl ScanStream {
    fn next_slice(&mut self, storage: &Storage) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        let mut slice = Vec::new();
        while !self.cursor.done() && slice.is_empty() {
            check_cancelled()?;
            self.cursor = storage
                .rel_scan_slice(&self.table, self.cursor, SCAN_SLICE_ROWS, None, &mut slice)
                .map_err(|err| map_storage_err(err, &self.table))?;
        }
        Ok(if slice.is_empty() { None } else { Some(slice) })
    }
}

/// A [`Source`] with its rows fully in hand — the join operators' BUILD side,
/// which is walked repeatedly (nested loop) or hashed whole (the grace-hash
/// spill bounds it past the memory budget). The probe side never takes this
/// form: it streams via [`SourceRows::next_slice`].
struct MaterializedSource {
    columns: Vec<ResultColumn>,
    collations: Vec<Option<String>>,
    rows: Vec<Vec<Datum>>,
}

impl MaterializedSource {
    fn from(source: Source, storage: &Storage) -> Result<Self, SqlError> {
        let Source {
            columns,
            collations,
            rows,
            ..
        } = source;
        Ok(MaterializedSource {
            columns,
            collations,
            rows: rows.materialize(storage)?,
        })
    }
}

impl SourceRows {
    /// Pulls the next batch of rows, for a consumer that walks the source
    /// exactly once (a join's probe side). A scan hands over its next slice; a
    /// materialized source hands everything in one batch.
    fn next_slice(&mut self, storage: &Storage) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        match self {
            SourceRows::Materialized(rows) => {
                if rows.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(rows)))
                }
            }
            SourceRows::Scan(stream) => stream.next_slice(storage),
        }
    }

    /// The whole input, for consumers that need it at once. A scan drains its
    /// remaining slices; a materialized source hands its rows over as-is.
    fn materialize(self, storage: &Storage) -> Result<Vec<Vec<Datum>>, SqlError> {
        match self {
            SourceRows::Materialized(rows) => Ok(rows),
            SourceRows::Scan(mut stream) => {
                #[cfg(test)]
                storage.count_scan_materialization();
                let mut rows = Vec::new();
                while let Some(mut slice) = stream.next_slice(storage)? {
                    rows.append(&mut slice);
                }
                Ok(rows)
            }
        }
    }
}

impl Source {
    fn types(&self) -> Vec<ColumnType> {
        self.columns.iter().map(|c| c.column_type).collect()
    }

    fn scope(&self) -> JoinScope {
        JoinScope {
            columns: self
                .qualifiers
                .iter()
                .zip(&self.columns)
                .map(|(qualifier, column)| (qualifier.clone(), column.name.clone()))
                .collect(),
            collations: self.collations.clone(),
        }
    }
}

/// Resolves column references against a (possibly multi-table) row source. A
/// dotted `t.col` matches by qualifier + name; a bare `col` matches a unique
/// column (ambiguous or unknown → `None`, surfaced by eval as an invalid-
/// column error).
pub(super) struct JoinScope {
    /// (qualifier, bare column name) per source column.
    columns: Vec<(Option<String>, String)>,
    /// Per-column collation names, parallel to `columns` (`None` = database
    /// default). Empty for correlation-only scopes that never drive comparison.
    collations: Vec<Option<String>>,
}

/// Resolver over an output RowSet's columns. Output columns are unqualified,
/// so a qualified `t.col` reference (e.g. in a grouped query's ORDER BY)
/// resolves by its bare name.
///
/// It does not carry per-column collation, so an *embedded equality* in an
/// ORDER BY expression over a `_CS`/`_BIN` output column (e.g.
/// `ORDER BY CASE WHEN code = 'ABC' THEN 0 ELSE 1 END`) folds case
/// (case-insensitive default). The sort key itself is collation-correct — the
/// non-aggregated path orders via `sort_collators` (real per-column collation)
/// and the aggregated/DISTINCT path via `order_key_cmp` — so this only affects a
/// nested `=` inside an ORDER BY expression on a case-sensitive column: a narrow,
/// documented limitation.
struct OutputScope {
    names: Vec<String>,
}

impl truthdb_sql::eval::ColumnResolver for OutputScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        let bare = name.rsplit('.').next().unwrap_or(name);
        self.names.iter().position(|n| n.eq_ignore_ascii_case(bare))
    }
}

impl JoinScope {
    /// True if any column matches `name` — even ambiguously (>1 match), where
    /// [`ColumnResolver::resolve`] returns `None`. Correlation analysis uses this
    /// to tell "the inner scope has this name (bind/error here)" from "the name
    /// is absent (it is an outer reference)": an ambiguous inner column must NOT
    /// be rebound to a same-named outer column.
    fn matches_any(&self, name: &str) -> bool {
        self.columns.iter().any(|(qualifier, column)| {
            if let Some((q, c)) = name.rsplit_once('.') {
                qualifier
                    .as_deref()
                    .is_some_and(|qq| qq.eq_ignore_ascii_case(q))
                    && column.eq_ignore_ascii_case(c)
            } else {
                column.eq_ignore_ascii_case(name)
            }
        })
    }

    /// Source-column indices belonging to a table qualifier (for `t.*`).
    fn indices_for_qualifier(&self, qualifier: &str) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, (q, _))| {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
            })
            .map(|(index, _)| index)
            .collect()
    }
}

impl truthdb_sql::eval::ColumnResolver for JoinScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        match self.resolve_detail(name) {
            truthdb_sql::eval::Resolution::Found(index) => Some(index),
            // Ambiguous and not-found both fail to bind a single column.
            _ => None,
        }
    }

    fn resolve_detail(&self, name: &str) -> truthdb_sql::eval::Resolution {
        use truthdb_sql::eval::Resolution;
        let matches = |q: &Option<String>, c: &str| -> bool {
            if let Some((qualifier, column)) = name.rsplit_once('.') {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
                    && c.eq_ignore_ascii_case(column)
            } else {
                c.eq_ignore_ascii_case(name)
            }
        };
        let mut found = None;
        for (index, (qualifier, column)) in self.columns.iter().enumerate() {
            if matches(qualifier, column) {
                if found.is_some() {
                    return Resolution::Ambiguous; // more than one match
                }
                found = Some(index);
            }
        }
        match found {
            Some(index) => Resolution::Found(index),
            None => Resolution::NotFound,
        }
    }

    fn collation(&self, index: usize) -> truthdb_sql::collation::CollationSensitivity {
        truthdb_sql::collation::CollationSensitivity::from_optional(
            self.collations.get(index).and_then(|c| c.as_deref()),
        )
    }
}

/// SqlValues of a row, for expression evaluation. `types` (parallel to `row`)
/// restores each value's exact type (e.g. a DECIMAL's scale).
fn row_values(row: &[Datum], types: &[ColumnType]) -> Vec<SqlValue> {
    row.iter()
        .zip(types)
        .map(|(d, t)| value::datum_to_sql(d, t))
        .collect()
}

// ---- common table expressions -------------------------------------------

/// Inlines a SELECT's `WITH` common table expressions: each FROM reference to a
/// CTE name becomes a derived table over the CTE's query. CTEs are expanded in
/// order, so a later CTE may reference an earlier one; non-recursive (a self- or
/// forward-reference is left as a base-table name and errors at bind). Returns a
/// CTE-free SELECT.
type CteMap = std::collections::HashMap<String, Select>;

fn expand_ctes(select: &Select) -> Select {
    expand_select_ctes(select, &CteMap::new())
}

/// A copy of `select` with every CTE reference — at this level and nested inside
/// its subqueries — replaced by a derived table. `outer` is the enclosing CTE
/// scope; this select's own `WITH` layers on top of it (so a nested `WITH` sees
/// enclosing CTEs and is itself inlined). The result carries no `ctes` at any
/// level, so lock analysis, which walks the expanded tree without re-expanding,
/// still sees every base table the executor reads.
fn expand_select_ctes(select: &Select, outer: &CteMap) -> Select {
    let mut resolved = outer.clone();
    for cte in &select.ctes {
        let body = expand_select_ctes(&cte.query, &resolved);
        resolved.insert(cte.name.value.to_ascii_lowercase(), body);
    }
    let resolved = &resolved;
    let mut out = select.clone();
    out.ctes = Vec::new();
    out.from = out
        .from
        .as_ref()
        .map(|from| expand_from_ctes(from, resolved));
    out.items = out
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => SelectItem::Expr {
                expr: expand_expr_ctes(expr, resolved),
                alias: alias.clone(),
            },
            // Inline CTE references inside an assignment value too, so lock
            // analysis (which expands the original assignment SELECT) sees the
            // real base tables behind a CTE used only in the value expression.
            SelectItem::Assign { target, value } => SelectItem::Assign {
                target: target.clone(),
                value: expand_expr_ctes(value, resolved),
            },
            other => other.clone(),
        })
        .collect();
    out.where_clause = out
        .where_clause
        .as_ref()
        .map(|e| expand_expr_ctes(e, resolved));
    out.having = out.having.as_ref().map(|e| expand_expr_ctes(e, resolved));
    out.group_by = out
        .group_by
        .iter()
        .map(|e| expand_expr_ctes(e, resolved))
        .collect();
    out.order_by = out
        .order_by
        .iter()
        .map(|o| OrderItem {
            expr: expand_expr_ctes(&o.expr, resolved),
            descending: o.descending,
        })
        .collect();
    out
}

/// Replaces CTE references in a FROM tree with derived tables (recursing into
/// joins — including the `ON` predicate's subqueries — and nested derived
/// tables, which may also reference the CTEs).
fn expand_from_ctes(tref: &TableRef, resolved: &CteMap) -> TableRef {
    match tref {
        TableRef::Table { name, alias } => {
            // Only an unqualified reference can name a CTE (CTE names are not
            // schema-qualified); `dbo.s` must resolve to a base table.
            let cte = (!name.value.contains('.'))
                .then(|| resolved.get(&name.value.to_ascii_lowercase()))
                .flatten();
            match cte {
                Some(body) => TableRef::Derived {
                    subquery: Box::new(body.clone()),
                    // The exposed name is the alias, else the CTE reference name.
                    alias: alias.clone().unwrap_or_else(|| name.clone()),
                },
                None => tref.clone(),
            }
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => TableRef::Join {
            left: Box::new(expand_from_ctes(left, resolved)),
            right: Box::new(expand_from_ctes(right, resolved)),
            kind: *kind,
            on: on.as_ref().map(|e| expand_expr_ctes(e, resolved)),
        },
        TableRef::Derived { subquery, alias } => TableRef::Derived {
            subquery: Box::new(expand_select_ctes(subquery, resolved)),
            alias: alias.clone(),
        },
        // A TVF name is never a CTE (CTE names are unqualified); only its
        // arguments may reference one.
        TableRef::Function { name, args, alias } => TableRef::Function {
            name: name.clone(),
            args: args.iter().map(|a| expand_expr_ctes(a, resolved)).collect(),
            alias: alias.clone(),
        },
    }
}

/// Replaces CTE references inside a subquery embedded in an expression (so a CTE
/// is visible to WHERE/SELECT/HAVING subqueries, not only the FROM clause).
fn expand_expr_ctes(expr: &Expr, resolved: &CteMap) -> Expr {
    let recur = |e: &Expr| Box::new(expand_expr_ctes(e, resolved));
    let recur_opt = |e: &Option<Box<Expr>>| e.as_ref().map(|e| recur(e));
    let kind = match &expr.kind {
        ExprKind::Subquery(s) => ExprKind::Subquery(Box::new(expand_select_ctes(s, resolved))),
        ExprKind::Exists(s) => ExprKind::Exists(Box::new(expand_select_ctes(s, resolved))),
        ExprKind::InSubquery {
            expr: e,
            subquery,
            negated,
        } => ExprKind::InSubquery {
            expr: recur(e),
            subquery: Box::new(expand_select_ctes(subquery, resolved)),
            negated: *negated,
        },
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: recur(e),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: recur(left),
            right: recur(right),
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: recur(e),
            negated: *negated,
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: recur(e),
            pattern: recur(pattern),
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: recur(e),
            list: list.iter().map(|x| expand_expr_ctes(x, resolved)).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: recur(e),
            low: recur(low),
            high: recur(high),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: recur_opt(operand),
            branches: branches
                .iter()
                .map(|(w, r)| (expand_expr_ctes(w, resolved), expand_expr_ctes(r, resolved)))
                .collect(),
            else_result: recur_opt(else_result),
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: recur(e),
            target: target.clone(),
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args.iter().map(|a| expand_expr_ctes(a, resolved)).collect(),
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: recur_opt(arg),
        },
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => expr.kind.clone(),
    };
    Expr {
        kind,
        span: expr.span,
    }
}

// ---- subquery resolution ------------------------------------------------

/// Returns a copy of a SELECT with every subquery in its expressions
/// (WHERE/HAVING/SELECT list/GROUP BY/ORDER BY) evaluated and replaced by a
/// precomputed literal. Subqueries in a FROM-clause join `ON` are not rewritten
/// here (they are rare and error at evaluation). Only uncorrelated subqueries
/// are supported; a correlated one references an outer column and fails to
/// resolve when executed independently.
fn rewrite_select_subqueries(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<Select, SqlError> {
    // The columns this query exposes to a correlated subquery. A correlated
    // subquery in the WHERE or the SELECT list is left un-evaluated here (the
    // per-row loops bind the outer row), and one in HAVING likewise (the
    // per-group loop binds the group row). GROUP BY and ORDER BY do not
    // support correlation and evaluate as before.
    let self_scope = select
        .from
        .as_ref()
        .and_then(|from| from_column_names(storage, from))
        .map(|columns| JoinScope {
            collations: Vec::new(),
            columns,
        });
    let items = select
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => Ok(SelectItem::Expr {
                expr: rewrite_subqueries(storage, expr, eval_ctx, self_scope.as_ref())?,
                alias: alias.clone(),
            }),
            other => Ok(other.clone()),
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    let where_clause = select
        .where_clause
        .as_ref()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx, self_scope.as_ref()))
        .transpose()?;
    let having = select
        .having
        .as_ref()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx, self_scope.as_ref()))
        .transpose()?;
    let group_by = select
        .group_by
        .iter()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx, None))
        .collect::<Result<Vec<_>, SqlError>>()?;
    let order_by = select
        .order_by
        .iter()
        .map(|o| {
            Ok(OrderItem {
                expr: rewrite_subqueries(storage, &o.expr, eval_ctx, None)?,
                descending: o.descending,
            })
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    Ok(Select {
        ctes: select.ctes.clone(),
        top: select.top,
        distinct: select.distinct,
        items,
        from: select.from.clone(),
        where_clause,
        group_by,
        having,
        order_by,
        span: select.span,
    })
}

/// Recursively replaces each subquery node in an expression with its evaluated
/// result: a scalar `(SELECT ...)` -> a literal, `EXISTS (...)` -> a boolean,
/// `expr IN (SELECT ...)` -> an `InList` of the subquery's values.
fn rewrite_subqueries(
    storage: &Storage,
    expr: &Expr,
    eval_ctx: &EvalContext,
    correlated_scope: Option<&JoinScope>,
) -> Result<Expr, SqlError> {
    let recur =
        |storage: &Storage, e: &Expr| rewrite_subqueries(storage, e, eval_ctx, correlated_scope);
    let recur_box = |storage: &Storage, e: &Expr| -> Result<Box<Expr>, SqlError> {
        Ok(Box::new(recur(storage, e)?))
    };
    let recur_opt =
        |storage: &Storage, e: &Option<Box<Expr>>| -> Result<Option<Box<Expr>>, SqlError> {
            e.as_ref().map(|e| recur_box(storage, e)).transpose()
        };
    // A subquery correlated with the enclosing query (`correlated_scope`) is left
    // in place — the per-row WHERE loop substitutes its outer references and runs
    // it once per outer row (`substitute_correlated_in_expr`).
    let leave_correlated = |select: &Select| {
        correlated_scope.is_some_and(|scope| is_correlated(storage, select, scope))
    };
    let kind = match &expr.kind {
        ExprKind::Subquery(select) if leave_correlated(select) => expr.kind.clone(),
        ExprKind::Exists(select) if leave_correlated(select) => expr.kind.clone(),
        ExprKind::InSubquery { subquery, .. } if leave_correlated(subquery) => expr.kind.clone(),
        ExprKind::Subquery(select) => {
            ExprKind::Literal(eval_scalar_subquery(storage, select, eval_ctx)?)
        }
        ExprKind::Exists(select) => {
            let rowset = exec_select(storage, select, eval_ctx)?;
            ExprKind::Bool(!rowset.rows.is_empty())
        }
        ExprKind::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            let lhs = recur_box(storage, lhs)?;
            let list = eval_in_subquery(storage, subquery, eval_ctx)?
                .into_iter()
                .map(|v| Expr {
                    kind: ExprKind::Literal(v),
                    span: expr.span,
                })
                .collect();
            ExprKind::InList {
                expr: lhs,
                list,
                negated: *negated,
            }
        }
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: recur_box(storage, e)?,
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: recur_box(storage, left)?,
            right: recur_box(storage, right)?,
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: recur_box(storage, e)?,
            negated: *negated,
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: recur_box(storage, e)?,
            pattern: recur_box(storage, pattern)?,
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: recur_box(storage, e)?,
            list: list
                .iter()
                .map(|x| recur(storage, x))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: recur_box(storage, e)?,
            low: recur_box(storage, low)?,
            high: recur_box(storage, high)?,
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: recur_opt(storage, operand)?,
            branches: branches
                .iter()
                .map(|(w, r)| Ok((recur(storage, w)?, recur(storage, r)?)))
                .collect::<Result<_, SqlError>>()?,
            else_result: recur_opt(storage, else_result)?,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: recur_box(storage, e)?,
            target: target.clone(),
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| recur(storage, a))
                .collect::<Result<_, _>>()?,
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: recur_opt(storage, arg)?,
        },
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => expr.kind.clone(),
    };
    Ok(Expr {
        kind,
        span: expr.span,
    })
}

/// Evaluates a scalar subquery to a single value: NULL for 0 rows, the value
/// for 1 row, error 512 for more than 1 row; error 116 if it is not exactly one
/// column wide.
fn eval_scalar_subquery(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let rowset = exec_select(storage, select, eval_ctx)?;
    if rowset.columns.len() != 1 {
        return Err(scalar_subquery_shape_err());
    }
    match rowset.rows.len() {
        0 => Ok(SqlValue::Null),
        1 => Ok(value::datum_to_sql(
            &rowset.rows[0][0],
            &rowset.columns[0].column_type,
        )),
        _ => Err(SqlError::new(
            512,
            16,
            1,
            "Subquery returned more than 1 value. This is not permitted when the subquery follows =, !=, <, <=, >, >= or when the subquery is used as an expression.",
        )),
    }
}

/// Evaluates an `IN (SELECT ...)` subquery to its list of values (one column,
/// else error 116).
fn eval_in_subquery(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<Vec<SqlValue>, SqlError> {
    let rowset = exec_select(storage, select, eval_ctx)?;
    if rowset.columns.len() != 1 {
        return Err(scalar_subquery_shape_err());
    }
    let column_type = rowset.columns[0].column_type;
    Ok(rowset
        .rows
        .iter()
        .map(|r| value::datum_to_sql(&r[0], &column_type))
        .collect())
}

fn scalar_subquery_shape_err() -> SqlError {
    SqlError::new(
        116,
        16,
        1,
        "Only one expression can be specified in the select list when the subquery is not introduced with EXISTS.",
    )
}

// ---- correlated subquery support ----------------------------------------
//
// A subquery that references an enclosing query's column is *correlated*. It is
// left un-evaluated by `rewrite_select_subqueries` (which only folds away
// uncorrelated subqueries) and instead re-run once per outer row: the per-row
// WHERE loop calls `substitute_correlated_in_expr`, which binds the outer row's
// values into the subquery (`substitute_subquery_outer_refs`) and evaluates it.
// This is the "correct, slow, honest" per-row apply. Supported for correlated
// subqueries in the WHERE clause over base-table / join subqueries; a correlated
// reference inside a derived-table / view subquery (whose inner scope cannot be
// read from the catalog) falls back to the prior behavior (invalid-column 207).

/// The `(qualifier, bare column name)` columns a FROM clause exposes, read from
/// the catalog WITHOUT materializing rows. `None` if the FROM has a derived
/// table or a view, whose output columns need binding to determine.
fn from_column_names(storage: &Storage, from: &TableRef) -> Option<Vec<(Option<String>, String)>> {
    match from {
        TableRef::Table { name, alias } => {
            let def = resolve_table(storage, &name.value)?;
            // A view defers to its expansion; a PROCEDURE/FUNCTION/TRIGGER must
            // not read as a zero-column table — bailing here routes to the
            // collecting path, which errors 2809/208.
            if def.is_view() || def.is_procedure() || def.is_function() || def.is_trigger() {
                return None;
            }
            let qualifier = alias
                .as_ref()
                .map(|a| a.value.clone())
                .unwrap_or_else(|| strip_schema(&name.value).to_string());
            Some(
                def.columns
                    .iter()
                    .map(|(cname, _, _)| (Some(qualifier.clone()), cname.clone()))
                    .collect(),
            )
        }
        TableRef::Join { left, right, .. } => {
            let mut cols = from_column_names(storage, left)?;
            cols.extend(from_column_names(storage, right)?);
            Some(cols)
        }
        TableRef::Derived { subquery, alias } => {
            let mut cols = Vec::new();
            for item in &subquery.items {
                match item {
                    SelectItem::Expr { expr, alias: a } => {
                        let name = a
                            .as_ref()
                            .map(|n| n.value.clone())
                            .or_else(|| bare_column_name(expr))?;
                        cols.push((Some(alias.value.clone()), name));
                    }
                    SelectItem::Wildcard => {
                        let inner = from_column_names(storage, subquery.from.as_ref()?)?;
                        cols.extend(
                            inner
                                .into_iter()
                                .map(|(_, n)| (Some(alias.value.clone()), n)),
                        );
                    }
                    SelectItem::QualifiedWildcard(q) => {
                        let inner = from_column_names(storage, subquery.from.as_ref()?)?;
                        cols.extend(
                            inner
                                .into_iter()
                                .filter(|(qu, _)| {
                                    qu.as_deref()
                                        .is_some_and(|x| x.eq_ignore_ascii_case(&q.value))
                                })
                                .map(|(_, n)| (Some(alias.value.clone()), n)),
                        );
                    }
                    SelectItem::Assign { .. } => return None,
                }
            }
            Some(cols)
        }
        // A TVF's output columns are its body SELECT's projection — only known
        // after the body is parsed and bound, like a view. Defer to expansion.
        TableRef::Function { .. } => None,
    }
}

/// The inner scope of a subquery (its own FROM columns), or `None` if it cannot
/// be determined from the catalog alone.
fn subquery_inner_scope(storage: &Storage, subquery: &Select) -> Option<JoinScope> {
    let columns = match &subquery.from {
        Some(from) => from_column_names(storage, from)?,
        None => Vec::new(),
    };
    Some(JoinScope {
        collations: Vec::new(),
        columns,
    })
}

/// True if `subquery` references a column that resolves in the enclosing `outer`
/// scope but not in its own FROM — i.e. it is correlated.
fn is_correlated(storage: &Storage, subquery: &Select, outer: &JoinScope) -> bool {
    let Some(inner) = subquery_inner_scope(storage, subquery) else {
        return false;
    };
    let mut correlated = false;
    select_column_refs(subquery, &mut |name| {
        // `matches_any` (not `resolve`) so an *ambiguous* inner column is treated
        // as inner (it errors in the subquery) rather than rebound to the outer.
        if !inner.matches_any(&name.value) && outer.resolve(&name.value).is_some() {
            correlated = true;
        }
    });
    // A correlated reference may live inside a derived table's body — its own
    // clauses resolve in the derived scope, so the walk above never sees it.
    correlated || from_has_correlated_derived(storage, subquery.from.as_ref(), outer)
}

/// Calls `f` on every column reference inside an AGGREGATE argument anywhere
/// in the select's own clauses (not descending into nested subqueries).
fn select_aggregate_arg_refs(select: &Select, f: &mut impl FnMut(&Name)) {
    fn walk(expr: &Expr, f: &mut impl FnMut(&Name)) {
        match &expr.kind {
            ExprKind::Aggregate { arg: Some(a), .. } => expr_column_refs(a, f),
            ExprKind::Aggregate { arg: None, .. } => {}
            ExprKind::Unary { expr: e, .. }
            | ExprKind::IsNull { expr: e, .. }
            | ExprKind::Cast { expr: e, .. } => walk(e, f),
            ExprKind::Binary { left, right, .. } => {
                walk(left, f);
                walk(right, f);
            }
            ExprKind::Like {
                expr: e, pattern, ..
            } => {
                walk(e, f);
                walk(pattern, f);
            }
            ExprKind::InList { expr: e, list, .. } => {
                walk(e, f);
                list.iter().for_each(|x| walk(x, f));
            }
            ExprKind::Between {
                expr: e, low, high, ..
            } => {
                walk(e, f);
                walk(low, f);
                walk(high, f);
            }
            ExprKind::Function { args, .. } => args.iter().for_each(|x| walk(x, f)),
            ExprKind::Case {
                operand,
                branches,
                else_result,
            } => {
                if let Some(o) = operand {
                    walk(o, f);
                }
                for (w, r) in branches {
                    walk(w, f);
                    walk(r, f);
                }
                if let Some(e) = else_result {
                    walk(e, f);
                }
            }
            _ => {}
        }
    }
    if let Some(w) = &select.where_clause {
        walk(w, f);
    }
    for item in &select.items {
        if let SelectItem::Expr { expr, .. } = item {
            walk(expr, f);
        }
    }
    if let Some(h) = &select.having {
        walk(h, f);
    }
}

/// Whether any derived-table body in a FROM tree is correlated to `outer`.
fn from_has_correlated_derived(
    storage: &Storage,
    from: Option<&TableRef>,
    outer: &JoinScope,
) -> bool {
    match from {
        None | Some(TableRef::Table { .. }) => false,
        Some(TableRef::Join { left, right, .. }) => {
            from_has_correlated_derived(storage, Some(left), outer)
                || from_has_correlated_derived(storage, Some(right), outer)
        }
        Some(TableRef::Derived { subquery, .. }) => is_correlated(storage, subquery, outer),
        // A TVF's literal/non-outer arguments do not correlate it to the outer
        // FROM (APPLY, with outer-referencing args, is out of scope).
        Some(TableRef::Function { .. }) => false,
    }
}

/// Calls `f` on every column reference in a select's own clauses (WHERE, SELECT
/// items, HAVING, GROUP BY, ORDER BY), not descending into nested subqueries
/// (which resolve in their own scope).
fn select_column_refs(select: &Select, f: &mut impl FnMut(&Name)) {
    if let Some(w) = &select.where_clause {
        expr_column_refs(w, f);
    }
    for item in &select.items {
        match item {
            SelectItem::Expr { expr, .. } => expr_column_refs(expr, f),
            SelectItem::Assign { value, .. } => expr_column_refs(value, f),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
        }
    }
    if let Some(h) = &select.having {
        expr_column_refs(h, f);
    }
    for e in &select.group_by {
        expr_column_refs(e, f);
    }
    for o in &select.order_by {
        expr_column_refs(&o.expr, f);
    }
}

/// Calls `f` on every column reference in an expression, not descending into
/// nested subquery bodies.
fn expr_column_refs(expr: &Expr, f: &mut impl FnMut(&Name)) {
    match &expr.kind {
        ExprKind::Column(name) => f(name),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_) => {}
        // The IN operand is at this scope; the subquery body is not.
        ExprKind::InSubquery { expr: e, .. } => expr_column_refs(e, f),
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => expr_column_refs(e, f),
        ExprKind::Binary { left, right, .. } => {
            expr_column_refs(left, f);
            expr_column_refs(right, f);
        }
        ExprKind::Like {
            expr: e, pattern, ..
        } => {
            expr_column_refs(e, f);
            expr_column_refs(pattern, f);
        }
        ExprKind::InList { expr: e, list, .. } => {
            expr_column_refs(e, f);
            list.iter().for_each(|x| expr_column_refs(x, f));
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => {
            expr_column_refs(e, f);
            expr_column_refs(low, f);
            expr_column_refs(high, f);
        }
        ExprKind::Function { args, .. } => args.iter().for_each(|a| expr_column_refs(a, f)),
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                expr_column_refs(a, f);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                expr_column_refs(o, f);
            }
            for (w, r) in branches {
                expr_column_refs(w, f);
                expr_column_refs(r, f);
            }
            if let Some(e) = else_result {
                expr_column_refs(e, f);
            }
        }
    }
}

/// Rewrites ORDER BY so a SELECT-list alias resolves on the path that sorts the
/// *source* rows.
///
/// ORDER BY is the one clause where a SELECT alias is in scope, but this path
/// sorts base rows against a [`JoinScope`], which knows only base columns — so
/// `SELECT v AS vv FROM a ORDER BY vv` would not resolve, and an alias over a
/// computed expression (`SELECT v * 2 AS dbl ... ORDER BY dbl`) has no base
/// column to fall back on at all. Each unqualified name matching an alias is
/// replaced by that alias's expression, which the existing sort machinery then
/// evaluates against the base row. (The grouped/DISTINCT path projects first and
/// resolves ORDER BY against the output names, so it already sees aliases.)
///
/// An alias shadows a base column of the same name, as in SQL Server. A
/// qualified name (`a.v`) always means that table's column and is never
/// rewritten. `map_expr_columns` does not rescan what it substitutes, so a
/// self-referential alias (`SELECT v + 1 AS v ... ORDER BY v`) substitutes once
/// instead of recursing forever. Ordinals (`ORDER BY 1`) are integers, not
/// column references, so they are untouched.
fn order_by_with_aliases(
    order_by: &[OrderItem],
    items: &[SelectItem],
    scope: &JoinScope,
) -> Result<Vec<OrderItem>, SqlError> {
    let aliases: Vec<(&str, &Expr)> = items
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr {
                expr,
                alias: Some(name),
            } => Some((name.value.as_str(), expr)),
            _ => None,
        })
        .collect();
    let outputs = output_exprs(items, scope);
    order_by
        .iter()
        .map(|item| {
            // A bare integer is a 1-based output-column ordinal, not a value.
            let expr = if let ExprKind::Int(n) = &item.expr.kind {
                usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|i| outputs.get(i).cloned())
                    .ok_or_else(|| {
                        SqlError::new(
                            108,
                            16,
                            1,
                            format!("The ORDER BY position number {n} is out of range."),
                        )
                    })?
            } else {
                map_expr_columns(&item.expr, &|name: &Name| {
                    if name.value.contains('.') {
                        return None;
                    }
                    aliases
                        .iter()
                        .find(|(alias, _)| name.eq_ignore_case(alias))
                        .map(|(_, expr)| (*expr).clone())
                })
            };
            Ok(OrderItem {
                expr,
                descending: item.descending,
            })
        })
        .collect()
}

/// The select list as one source-evaluable expression per *output* column, so a
/// positional `ORDER BY <n>` can name what it points at. A wildcard expands to
/// its source columns, each referenced by qualifier where it has one — `a.v`
/// rather than `v` — so a join with a repeated column name stays unambiguous.
fn output_exprs(items: &[SelectItem], scope: &JoinScope) -> Vec<Expr> {
    // Synthetic: built from the scope, so it resolves by construction and its
    // span is never surfaced in an error.
    let synthetic = Span::new(0, 0);
    let column_expr = |index: usize| {
        let (qualifier, column) = &scope.columns[index];
        let value = match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        };
        Expr {
            span: synthetic,
            kind: ExprKind::Column(Name {
                value,
                quoted: false,
                span: synthetic,
            }),
        }
    };
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => out.extend((0..scope.columns.len()).map(column_expr)),
            SelectItem::QualifiedWildcard(qualifier) => out.extend(
                scope
                    .indices_for_qualifier(&qualifier.value)
                    .into_iter()
                    .map(column_expr),
            ),
            SelectItem::Expr { expr, .. } => out.push(expr.clone()),
            // An assignment SELECT produces no result columns to order by.
            SelectItem::Assign { .. } => {}
        }
    }
    out
}

/// Replaces every column reference in an expression via `f` (a replacement, or
/// `None` to keep), not descending into nested subquery bodies (but mapping an
/// `IN (SELECT)` operand, which is at this scope).
fn map_expr_columns(expr: &Expr, f: &impl Fn(&Name) -> Option<Expr>) -> Expr {
    let map = |e: &Expr| map_expr_columns(e, f);
    let map_box = |e: &Expr| Box::new(map_expr_columns(e, f));
    let kind = match &expr.kind {
        ExprKind::Column(name) => match f(name) {
            Some(replacement) => return replacement,
            None => expr.kind.clone(),
        },
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_) => expr.kind.clone(),
        ExprKind::InSubquery {
            expr: e,
            subquery,
            negated,
        } => ExprKind::InSubquery {
            expr: map_box(e),
            subquery: subquery.clone(),
            negated: *negated,
        },
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: map_box(e),
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: map_box(e),
            negated: *negated,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: map_box(e),
            target: target.clone(),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: map_box(left),
            right: map_box(right),
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: map_box(e),
            pattern: map_box(pattern),
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: map_box(e),
            list: list.iter().map(map).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: map_box(e),
            low: map_box(low),
            high: map_box(high),
            negated: *negated,
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args.iter().map(map).collect(),
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: arg.as_ref().map(|a| map_box(a)),
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: operand.as_ref().map(|o| map_box(o)),
            branches: branches.iter().map(|(w, r)| (map(w), map(r))).collect(),
            else_result: else_result.as_ref().map(|e| map_box(e)),
        },
    };
    Expr {
        kind,
        span: expr.span,
    }
}

/// A copy of `subquery` with references to the enclosing query's columns (per
/// `outer`) replaced by the current outer row's literal values — making a
/// correlated subquery uncorrelated for that row. `None` if the inner scope
/// cannot be determined; the caller then runs the subquery unchanged.
fn substitute_subquery_outer_refs(
    storage: &Storage,
    subquery: &Select,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
) -> Option<Select> {
    let inner = subquery_inner_scope(storage, subquery)?;
    let substitute = |name: &Name| -> Option<Expr> {
        if inner.matches_any(&name.value) {
            return None; // the subquery's own column wins (even if ambiguous)
        }
        let index = outer(&name.value)?;
        Some(Expr {
            kind: ExprKind::Literal(outer_row.get(index)?.clone()),
            span: name.span,
        })
    };
    // An outer reference INSIDE an aggregate argument has outer-aggregate
    // semantics in SQL Server (the aggregate computes over the OUTER group);
    // substituting a per-row literal would silently compute something else.
    // Bail — the subquery runs unchanged and errors cleanly.
    let mut outer_in_agg = false;
    select_aggregate_arg_refs(subquery, &mut |name| {
        if !inner.matches_any(&name.value) && outer(&name.value).is_some() {
            outer_in_agg = true;
        }
    });
    if outer_in_agg {
        return None;
    }
    let mut out = subquery.clone();
    out.where_clause = out
        .where_clause
        .as_ref()
        .map(|e| map_expr_columns(e, &substitute));
    out.items = out
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => SelectItem::Expr {
                expr: map_expr_columns(expr, &substitute),
                alias: alias.clone(),
            },
            other => other.clone(),
        })
        .collect();
    out.having = out
        .having
        .as_ref()
        .map(|e| map_expr_columns(e, &substitute));
    out.group_by = out
        .group_by
        .iter()
        .map(|e| map_expr_columns(e, &substitute))
        .collect();
    out.order_by = out
        .order_by
        .iter()
        .map(|o| OrderItem {
            expr: map_expr_columns(&o.expr, &substitute),
            descending: o.descending,
        })
        .collect();
    // A correlated reference INSIDE a derived table's body lives in `from`,
    // not in any expression above — descend and substitute there too. The
    // recursive call's own inner-scope check handles shadowing.
    if let Some(from) = out.from.as_mut() {
        substitute_from_outer_refs(storage, from, outer, outer_row)?;
    }
    Some(out)
}

/// Substitutes outer references inside every derived-table body of a FROM
/// tree. `None` when any derived body's scope cannot be determined.
fn substitute_from_outer_refs(
    storage: &Storage,
    from: &mut TableRef,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
) -> Option<()> {
    match from {
        TableRef::Table { .. } => Some(()),
        TableRef::Join { left, right, .. } => {
            substitute_from_outer_refs(storage, left, outer, outer_row)?;
            substitute_from_outer_refs(storage, right, outer, outer_row)
        }
        TableRef::Derived { subquery, .. } => {
            **subquery = substitute_subquery_outer_refs(storage, subquery, outer, outer_row)?;
            Some(())
        }
        // A TVF's body lives in the catalog (bound at expansion, not here) and
        // its literal arguments carry no outer references.
        TableRef::Function { .. } => Some(()),
    }
}

/// Evaluates each correlated subquery in `expr` against `outer_row` (binding the
/// enclosing query's columns per `outer`) and replaces it with a literal —
/// producing a subquery-free predicate for that outer row.
/// A [`ColumnResolver`] over a bare name→index closure (the `outer` resolver the
/// correlated-substitution pass carries), so a user scalar function's arguments
/// can be evaluated against the current row.
struct FnResolver<'a>(&'a dyn Fn(&str) -> Option<usize>);

impl truthdb_sql::eval::ColumnResolver for FnResolver<'_> {
    fn resolve(&self, name: &str) -> Option<usize> {
        (self.0)(name)
    }
}

/// Resolves a function-call name to a user-defined SCALAR function, or `None`
/// (an unknown name, a built-in, or a table-valued function). Schema-qualified
/// (`dbo.f`) and bare names both resolve; a bare name that shadows a built-in
/// takes the user function (a documented minor divergence from SQL Server, which
/// requires schema-qualified UDF calls).
fn resolve_scalar_function(storage: &Storage, name: &str) -> Option<TableDef> {
    // A bare (unqualified) name that matches a built-in always binds to the
    // built-in — a same-named UDF is reached only by its schema-qualified name
    // (`dbo.abs`), as SQL Server requires. Without this a UDF named like a
    // built-in would silently hijack every unqualified call to that name.
    if !name.contains('.') && truthdb_sql::functions::is_builtin_function(name) {
        return None;
    }
    let def = resolve_table(storage, name)?;
    match def.function.as_ref()?.returns {
        FunctionReturns::Scalar { .. } => Some(def),
        // A table-valued function is not a scalar call (it resolves in FROM).
        FunctionReturns::InlineTable { .. } | FunctionReturns::MultiStatementTable { .. } => None,
    }
}

/// True if an expression contains a call to a user-defined scalar function —
/// which, like a subquery, cannot be evaluated by the pure eval crate and must
/// be rewritten to a literal per row first.
fn expr_has_user_function(storage: &Storage, expr: &Expr) -> bool {
    let has = |e: &Expr| expr_has_user_function(storage, e);
    match &expr.kind {
        ExprKind::Function { name, args } => {
            resolve_scalar_function(storage, name).is_some() || args.iter().any(has)
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_)
        | ExprKind::InSubquery { .. } => false,
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => has(e),
        ExprKind::Binary { left, right, .. } => has(left) || has(right),
        ExprKind::Like {
            expr: e, pattern, ..
        } => has(e) || has(pattern),
        ExprKind::InList { expr: e, list, .. } => has(e) || list.iter().any(has),
        ExprKind::Between {
            expr: e, low, high, ..
        } => has(e) || has(low) || has(high),
        ExprKind::Aggregate { arg, .. } => arg.as_deref().is_some_and(has),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(has)
                || branches.iter().any(|(w, r)| has(w) || has(r))
                || else_result.as_deref().is_some_and(has)
        }
    }
}

/// True if an expression needs the per-row storage-aware rewrite (a subquery or
/// a user scalar function) before the pure evaluator can run on it.
fn expr_needs_binding(storage: &Storage, expr: &Expr) -> bool {
    expr_has_subquery(expr) || expr_has_user_function(storage, expr)
}

fn substitute_correlated_in_expr(
    storage: &Storage,
    expr: &Expr,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
    eval_ctx: &EvalContext,
) -> Result<Expr, SqlError> {
    let recur = |e: &Expr| substitute_correlated_in_expr(storage, e, outer, outer_row, eval_ctx);
    let recur_box = |e: &Expr| -> Result<Box<Expr>, SqlError> { Ok(Box::new(recur(e)?)) };
    let bind = |sq: &Select| -> Select {
        substitute_subquery_outer_refs(storage, sq, outer, outer_row).unwrap_or_else(|| sq.clone())
    };
    let kind = match &expr.kind {
        ExprKind::Subquery(sq) => {
            ExprKind::Literal(eval_scalar_subquery(storage, &bind(sq), eval_ctx)?)
        }
        ExprKind::Exists(sq) => {
            let rowset = exec_select(storage, &bind(sq), eval_ctx)?;
            ExprKind::Bool(!rowset.rows.is_empty())
        }
        ExprKind::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            let lhs = recur_box(lhs)?;
            let list = eval_in_subquery(storage, &bind(subquery), eval_ctx)?
                .into_iter()
                .map(|v| Expr {
                    kind: ExprKind::Literal(v),
                    span: expr.span,
                })
                .collect();
            ExprKind::InList {
                expr: lhs,
                list,
                negated: *negated,
            }
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => expr.kind.clone(),
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: recur_box(e)?,
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: recur_box(e)?,
            negated: *negated,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: recur_box(e)?,
            target: target.clone(),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: recur_box(left)?,
            right: recur_box(right)?,
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: recur_box(e)?,
            pattern: recur_box(pattern)?,
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: recur_box(e)?,
            list: list.iter().map(&recur).collect::<Result<_, _>>()?,
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: recur_box(e)?,
            low: recur_box(low)?,
            high: recur_box(high)?,
            negated: *negated,
        },
        ExprKind::Function { name, args } => {
            let args = args.iter().map(&recur).collect::<Result<Vec<_>, _>>()?;
            // A call that resolves to a user scalar function runs its body once
            // for this row (its arguments evaluated against the row) and folds to
            // the returned value — the same rewrite-to-literal discipline
            // subqueries use, keeping scalar evaluation free of storage access.
            if let Some(def) = resolve_scalar_function(storage, name) {
                let resolver = FnResolver(outer);
                let values = args
                    .iter()
                    .map(|a| eval::eval(a, outer_row, &resolver, eval_ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                ExprKind::Literal(run_user_scalar_function(storage, &def, &values, eval_ctx)?)
            } else {
                ExprKind::Function {
                    name: name.clone(),
                    args,
                }
            }
        }
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: match arg {
                Some(a) => Some(recur_box(a)?),
                None => None,
            },
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: match operand {
                Some(o) => Some(recur_box(o)?),
                None => None,
            },
            branches: branches
                .iter()
                .map(|(w, r)| Ok((recur(w)?, recur(r)?)))
                .collect::<Result<_, SqlError>>()?,
            else_result: match else_result {
                Some(e) => Some(recur_box(e)?),
                None => None,
            },
        },
    };
    Ok(Expr {
        kind,
        span: expr.span,
    })
}

/// Whether a WHERE/ON predicate keeps a row. The predicate must be
/// boolean-typed (SQL Server 4145): a bare numeric/string expression is
/// rejected rather than silently coerced, and UNKNOWN drops the row (3VL).
fn where_keeps(
    predicate: &Expr,
    row: &[SqlValue],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<bool, SqlError> {
    match eval::eval(predicate, row, resolver, eval_ctx)? {
        SqlValue::Bool(b) => Ok(b),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is expected, near 'WHERE'.",
        )
        .at(predicate.span)),
    }
}

fn exec_select(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // A top-level assignment SELECT is routed to exec_select_assign; one reaching
    // here has been nested in a subquery / derived table / CTE, which is invalid.
    if select
        .items
        .iter()
        .any(|i| matches!(i, SelectItem::Assign { .. }))
    {
        return Err(SqlError::message_only(
            141,
            "A SELECT that assigns to a variable cannot be used inside a query expression.",
        ));
    }
    // A single-table scan whose every output column comes from the schema needs
    // no stage that waits for the whole input, so it runs row by row instead.
    // The gate goes before the CTE expansion and the subquery rewrite because it
    // excludes both — and the rewrite would otherwise clone the whole statement
    // and run any subquery eagerly.
    if let Some(plan) = scan_plan(storage, select, eval_ctx) {
        return scan_select(storage, &plan, select, eval_ctx);
    }

    // Inline any WITH common table expressions (as derived tables) first.
    let expanded;
    let select = if select.ctes.is_empty() {
        select
    } else {
        expanded = expand_ctes(select);
        &expanded
    };
    // Resolve each (uncorrelated) subquery once, up front, replacing it with a
    // literal / boolean / value-list so the rest of execution is subquery-free.
    let rewritten = rewrite_select_subqueries(storage, select, eval_ctx)?;
    let select = &rewritten;

    let source = build_source(
        storage,
        select.from.as_ref(),
        &select.where_clause,
        eval_ctx,
    )?;
    let resolver = source.scope();
    let types = source.types();

    // WHERE. The predicate must be boolean-typed (SQL Server 4145): a bare
    // numeric/string expression is rejected rather than silently coerced. Any
    // subquery left in the (already-rewritten) predicate is correlated: bind the
    // outer row into it and evaluate per row.
    let where_correlated = select
        .where_clause
        .as_ref()
        .is_some_and(|w| expr_needs_binding(storage, w));
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // One row: filter it into `rows` or drop it. Shared by both input shapes.
    let take = |row: Vec<Datum>, rows: &mut Vec<Vec<Datum>>| -> Result<(), SqlError> {
        check_cancelled()?;
        let sql_row = row_values(&row, &types);
        let keep = match &select.where_clause {
            None => true,
            Some(predicate) => {
                let bound;
                let predicate = if where_correlated {
                    bound = substitute_correlated_in_expr(
                        storage,
                        predicate,
                        &|name| resolver.resolve(name),
                        &sql_row,
                        eval_ctx,
                    )?;
                    &bound
                } else {
                    predicate
                };
                where_keeps(predicate, &sql_row, &resolver, eval_ctx)?
            }
        };
        if keep {
            rows.push(row);
        }
        Ok(())
    };
    // A scanned base table streams in slices: peak input memory is one slice
    // plus the survivors, not the table (Stage 8 streaming scans). Everything
    // downstream (aggregate/sort/join operators) bounds or spills its own
    // working set, so a filtered pipeline is bounded end to end.
    match source.rows {
        SourceRows::Materialized(input) => {
            for row in input {
                take(row, &mut rows)?;
            }
        }
        SourceRows::Scan(mut stream) => {
            while let Some(slice) = stream.next_slice(storage)? {
                for row in slice {
                    take(row, &mut rows)?;
                }
            }
        }
    }

    // A grouped/aggregated or DISTINCT query projects first (its ORDER BY
    // references the output), while a plain query orders the source rows so it
    // can order by columns that are not in the SELECT list.
    if aggregate::is_aggregated(select) || select.distinct {
        let mut out = if aggregate::is_aggregated(select) {
            aggregate::execute(storage, select, &rows, &types, &resolver, eval_ctx)?
        } else {
            project(
                storage,
                &select.items,
                &source.columns,
                &rows,
                &types,
                &resolver,
                eval_ctx,
            )?
        };
        if select.distinct {
            // Each output column's collation (resolved back to its source column;
            // a computed/aliased column has no source column → the case-
            // insensitive default), so DISTINCT honours an explicit `_CS`/`_BIN`
            // column exactly like GROUP BY / COUNT(DISTINCT) do.
            let out_sens: Vec<CollationSensitivity> = out
                .columns
                .iter()
                .map(|c| {
                    resolver
                        .resolve(&c.name)
                        .map(|i| resolver.collation(i))
                        .unwrap_or(CollationSensitivity::default_collation())
                })
                .collect();
            dedup_rows(storage, &mut out, &out_sens)?;
        }
        order_output(&mut out, &select.order_by, eval_ctx)?;
        if let Some(top) = select.top {
            out.rows.truncate(top as usize);
        }
        return Ok(out);
    }

    // ORDER BY (evaluated against the source row; stable so equal keys keep
    // input order). Spills to temp extents when the input exceeds the budget.
    if !select.order_by.is_empty() {
        let order_by = order_by_with_aliases(&select.order_by, &select.items, &resolver)?;
        rows = order_rows(
            storage,
            rows,
            &order_by,
            &types,
            &source.collations,
            &resolver,
            eval_ctx,
        )?;
    }

    // TOP.
    if let Some(top) = select.top {
        rows.truncate(top as usize);
    }

    project(
        storage,
        &select.items,
        &source.columns,
        &rows,
        &types,
        &resolver,
        eval_ctx,
    )
}

#[cfg(test)]
thread_local! {
    /// Test hook: makes [`scan_plan`] decline everything, so a test can run one
    /// query down both paths and compare. Thread-local, not a `static`: a batch
    /// runs on one thread, and the suite runs its tests in parallel in a single
    /// binary — a global would force every other test's queries onto the
    /// collecting path for as long as this one was set.
    static FORCE_COLLECTING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn force_collecting() -> bool {
    FORCE_COLLECTING.with(|c| c.get())
}

/// Runs `f` with [`scan_select`]'s path disabled, so `f`'s queries take the
/// collecting path. Restores on drop, so a panicking `f` cannot leave the flag
/// set for the next test to run on this thread.
#[cfg(test)]
pub(crate) fn without_scan_path<R>(f: impl FnOnce() -> R) -> R {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            FORCE_COLLECTING.with(|c| c.set(false));
        }
    }
    FORCE_COLLECTING.with(|c| c.set(true));
    let _restore = Restore;
    f()
}

/// A `SELECT` that can be answered a row at a time: one base table, scanned,
/// filtered and projected without any stage that must see the whole input
/// first. Produced by [`scan_plan`], consumed by [`scan_select`].
struct ScanPlan {
    /// The base table's catalog name — what the scan reads.
    table: String,
    /// How to read it: the planner's choice, made once (see [`scan_plan`]).
    access: plan::AccessPath,
    /// The schema columns this query reads at all — its projection plus the
    /// WHERE clause's — ascending and distinct. Everything below is expressed
    /// in *these* coordinates, not the table's: the storage layer decodes only
    /// these, so a scanned row has exactly this width.
    needed: Vec<usize>,
    /// Type of each needed column, parallel to `needed`.
    types: Vec<ColumnType>,
    /// Resolves the WHERE clause's column references against a scanned row.
    resolver: JoinScope,
    /// Output columns. Every type here is the schema's, which is what makes the
    /// shape work: a computed column's type comes from `infer_type` over every
    /// value in it, so it cannot be known until the last row has been seen.
    columns: Vec<ResultColumn>,
    /// The scanned-row position each output column reads (an index into
    /// `needed`, not into the table's columns).
    picks: Vec<usize>,
    /// An index seek that is *covering*: every needed column's original value
    /// is stored in the index leaves (`INCLUDE`), so the scan answers from the
    /// index alone — no per-row base-table lookup. Never true for a table
    /// scan.
    covering: bool,
}

/// The parameter names of a declaration list (`@p1 int, @p2 nvarchar(10)`),
/// in order: the first token of each top-level comma-separated entry.
/// `sp_execute` values arrive unnamed on the wire; these names bind them.
pub(crate) fn decl_names(decls: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut in_quote = false;
    let mut entry = String::new();
    for ch in decls.chars().chain(std::iter::once(',')) {
        match ch {
            // A quoted default value (`@p varchar(10) = 'a,b'`) may contain
            // commas and parens; none of them separate declarations. A
            // doubled '' escape toggles twice, landing back where it was.
            '\'' => {
                in_quote = !in_quote;
                entry.push(ch);
            }
            '(' if !in_quote => {
                depth += 1;
                entry.push(ch);
            }
            ')' if !in_quote => {
                depth = depth.saturating_sub(1);
                entry.push(ch);
            }
            ',' if !in_quote && depth == 0 => {
                if let Some(name) = entry.split_whitespace().next() {
                    names.push(name.to_string());
                }
                entry.clear();
            }
            _ => entry.push(ch),
        }
    }
    names
}

/// Recognises the shape [`scan_select`] can run, or `None` for everything else
/// — which then takes the collecting path unchanged.
///
/// Every rejection here is a stage that cannot answer until it has the whole
/// input: `ORDER BY` sorts it, `DISTINCT` dedups it, an aggregate folds it, a
/// computed column types it, and a join/derived table/CTE/view is another query
/// underneath. An `IndexSeek` is excluded for the opposite reason — it reads
/// *less* than a scan, and reading the whole table to filter it down would
/// trade the planner's work for this one's.
fn scan_plan(storage: &Storage, select: &Select, eval_ctx: &EvalContext) -> Option<ScanPlan> {
    #[cfg(test)]
    if force_collecting() {
        return None;
    }
    if !select.ctes.is_empty()
        || select.distinct
        || !select.order_by.is_empty()
        || aggregate::is_aggregated(select)
    {
        return None;
    }
    // `TOP 0` wants no rows, so this path would never evaluate the WHERE — and
    // the engine reports an unresolvable column (207) or a non-boolean predicate
    // (4145) from that evaluation, having no separate binding pass. Reading a
    // table to discard all of it is not worth answering an invalid query with an
    // empty result set, so the degenerate case stays on the collecting path.
    if select.top == Some(0) {
        return None;
    }
    // An uncorrelated subquery is executed by the rewrite this path skips; a
    // correlated one runs a query per row. (A subquery in the SELECT list is
    // already excluded: it is not a bare column.) A user scalar function in the
    // WHERE needs the same rewrite, so decline it here too.
    if select
        .where_clause
        .as_ref()
        .is_some_and(|w| expr_needs_binding(storage, w))
    {
        return None;
    }
    // Whether every output column *could* be a source column, which is a
    // property of the syntax alone. Deciding it before the catalog is read keeps
    // `SELECT id + 1 FROM t` from paying for a table definition, a schema and a
    // resolver it is only going to discard.
    if !select.items.iter().all(|item| {
        matches!(
            item,
            SelectItem::Wildcard
                | SelectItem::QualifiedWildcard(_)
                | SelectItem::Expr {
                    expr: Expr {
                        kind: ExprKind::Column(_),
                        ..
                    },
                    ..
                }
        )
    }) {
        return None;
    }
    let Some(TableRef::Table { name, alias }) = select.from.as_ref() else {
        return None;
    };
    // The `sys.*` virtual tables build their rows in Rust and have no cursor to
    // scan. They are matched by their full name *before* catalog resolution, so
    // this check has to come first for the same reason: `resolve_table` strips a
    // schema prefix, so it would answer `sys.tables` with a user table called
    // `tables`.
    if is_sys_view(&name.value) {
        return None;
    }
    let def = resolve_table(storage, &name.value)?;
    // A view is its own SELECT, expanded as a derived table — and its TableDef
    // has no columns and a `root_page` of 0, so a wildcard over it would project
    // nothing and the scan would read the catalog root instead of the view. A
    // PROCEDURE/FUNCTION/TRIGGER has the same empty shape and must not stream as
    // an empty table: the collecting path rejects it (2809/208).
    if def.view_query.is_some() || def.is_procedure() || def.is_function() || def.is_trigger() {
        return None;
    }
    // If SELECT is denied, fall back to the collecting path, which resolves the
    // same table through `build_table_source` and raises the 229 there — keeping
    // the check on the one path the executor uses to touch the object.
    enforce_object_permission(&def, &eval_ctx.security, PermAction::Select).ok()?;
    let schema = def.schema().ok()?;

    let qualifier = alias
        .as_ref()
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let source: Vec<ResultColumn> = schema
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            column_type: c.column_type,
        })
        .collect();
    let collations: Vec<Option<String>> =
        schema.columns.iter().map(|c| c.collation.clone()).collect();
    // The full-width scope, used to plan the projection and resolve the WHERE's
    // references. What the scan actually runs on is the pruned scope built
    // below, once the needed columns are known.
    let resolver = JoinScope {
        columns: source
            .iter()
            .map(|c| (Some(qualifier.clone()), c.name.clone()))
            .collect(),
        collations: collations.clone(),
    };

    // The projection plan, mirroring `project`'s: every item must resolve to a
    // source column, so the output's types are the schema's.
    let mut columns = Vec::new();
    let mut picks = Vec::new();
    for item in &select.items {
        match item {
            SelectItem::Wildcard => {
                for (index, column) in source.iter().enumerate() {
                    picks.push(index);
                    columns.push(column.clone());
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let indices = resolver.indices_for_qualifier(&q.value);
                // Unbound (4104): leave the error to the collecting path.
                if indices.is_empty() {
                    return None;
                }
                for index in indices {
                    picks.push(index);
                    columns.push(source[index].clone());
                }
            }
            SelectItem::Expr { expr, alias } => {
                let index = bare_column_index(expr, &resolver)?;
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                picks.push(index);
                columns.push(ResultColumn {
                    name,
                    column_type: source[index].column_type,
                });
            }
            // Rejected before projection on both paths.
            SelectItem::Assign { .. } => return None,
        }
    }

    // Projection pruning. The query reads the columns it projects plus the ones
    // its WHERE names, and nothing else — so those are the only ones the storage
    // layer decodes, and a scanned row is exactly that wide. A character column
    // costs a `String` allocation to decode, so on a wide table this is most of
    // the per-row work.
    //
    // A WHERE reference that does not resolve is simply not collected: it is not
    // a column of this table, so there is nothing to decode for it, and `eval`
    // still reports it (207) against the same resolver as before.
    let mut needed = picks.clone();
    let mut where_columns = Vec::new();
    if let Some(predicate) = &select.where_clause {
        collect_column_refs(predicate, &mut where_columns);
    }
    needed.extend(
        where_columns
            .iter()
            .filter_map(|name| resolver.resolve(name)),
    );
    needed.sort_unstable();
    needed.dedup();

    // The same access path `build_table_source` would take (it passes no
    // `needed`, so its choice can differ only toward a covering index — and it
    // never reaches this shape). Choosing here, rather than declining a seek,
    // is what keeps this gate free for the queries it rejects: a decline would
    // have thrown away the definition, the schema and this choice, and
    // `build_table_source` would compute all three again. Chosen after
    // `needed` is known so a covering index can win its tie (see
    // [`plan::choose`]).
    // The row count is a statistic (one buffer-pool-cached page read),
    // fetched only when choose() can use it (it returns a scan outright
    // without a predicate or indexes).
    let row_count = if def.indexes.is_empty() || select.where_clause.is_none() {
        None
    } else {
        storage.rel_row_count(&def.name)
    };
    let access = plan::choose(
        &def,
        &schema,
        &select.where_clause,
        eval_ctx,
        Some(&needed),
        row_count,
    );

    // Everything downstream now speaks in the scanned row's coordinates.
    let position = |index: usize| {
        needed
            .binary_search(&index)
            .expect("a needed column is in `needed`")
    };
    let picks = picks.into_iter().map(position).collect();
    let types = needed.iter().map(|&i| source[i].column_type).collect();
    let resolver = JoinScope {
        columns: needed
            .iter()
            .map(|&i| (Some(qualifier.clone()), source[i].name.clone()))
            .collect(),
        collations: needed.iter().map(|&i| collations[i].clone()).collect(),
    };

    // A seek covers when every column the query reads is INCLUDEd in the
    // index — original values in the leaf, since the key bytes are one-way
    // collation sort keys and cannot serve.
    let covering = match &access {
        plan::AccessPath::IndexSeek {
            index_object_id, ..
        } => def
            .indexes
            .iter()
            .find(|i| i.object_id == *index_object_id)
            .is_some_and(|i| needed.iter().all(|c| i.include.contains(c))),
        plan::AccessPath::TableScan => false,
    };

    Some(ScanPlan {
        table: def.name,
        access,
        needed,
        types,
        resolver,
        columns,
        picks,
        covering,
    })
}

/// Every column name a predicate references, in no particular order (duplicates
/// included — the caller dedups after resolving).
///
/// Exhaustive by construction: no wildcard arm, so a new [`ExprKind`] is a
/// compile error here rather than a column silently left undecoded.
fn collect_column_refs(expr: &Expr, out: &mut Vec<String>) {
    match &expr.kind {
        ExprKind::Column(name) => out.push(name.value.clone()),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => {}
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => collect_column_refs(e, out),
        ExprKind::Binary { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        ExprKind::Like {
            expr: e, pattern, ..
        } => {
            collect_column_refs(e, out);
            collect_column_refs(pattern, out);
        }
        ExprKind::InList { expr: e, list, .. } => {
            collect_column_refs(e, out);
            for item in list {
                collect_column_refs(item, out);
            }
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => {
            collect_column_refs(e, out);
            collect_column_refs(low, out);
            collect_column_refs(high, out);
        }
        ExprKind::Function { args, .. } => {
            for arg in args {
                collect_column_refs(arg, out);
            }
        }
        ExprKind::Aggregate { arg, .. } => {
            if let Some(arg) = arg {
                collect_column_refs(arg, out);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_column_refs(operand, out);
            }
            for (when, then) in branches {
                collect_column_refs(when, out);
                collect_column_refs(then, out);
            }
            if let Some(else_result) = else_result {
                collect_column_refs(else_result, out);
            }
        }
        // The gate rejects a subquery before this runs.
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {}
    }
}

/// The `sys.*` catalog views, which [`build_table_source`] answers by name
/// ahead of any catalog lookup.
fn is_sys_view(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "sys.tables"
            | "sys.views"
            | "sys.sql_modules"
            | "sys.columns"
            | "sys.indexes"
            | "sys.check_constraints"
            | "sys.foreign_keys"
            | "sys.default_constraints"
            | "sys.databases"
            | "sys.configurations"
            | "sys.database_principals"
            | "sys.database_role_members"
            | "sys.database_permissions"
    )
}

/// Scans, filters and projects one base table a row at a time.
///
/// The collecting path builds the whole table into `Source.rows`, filters that
/// into a second vector, converts *every* row to `SqlValue` in `project`, and
/// projects into a third — four copies of the input alive at once, before TOP
/// has discarded any of them. Here a slice is the only input in hand, each row
/// is projected or dropped as it is read, and `TOP n` stops the scan rather
/// than truncating afterwards.
///
/// The result is still collected; what this drops is the *input's*
/// materialization, which is the part that has no upper bound. An index seek
/// keeps its input materialized — `rel_index_scan` has no cursor — so it gains
/// the per-row savings but not that one; a seek's candidate set is bounded by
/// the seek.
///
/// `TOP n` therefore stops the scan without evaluating the predicate on rows
/// past the nth kept one, where the collecting path evaluated every source row
/// before truncating. A predicate that errors on one of those rows (a divide by
/// zero, an overflow) now goes unraised — which is SQL Server's behaviour, whose
/// Top operator likewise stops asking its child for rows.
fn scan_select(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let mut out = RowSet {
        columns: plan.columns.clone(),
        rows: Vec::new(),
    };
    scan_select_rows(storage, plan, select, eval_ctx, &mut |row| {
        out.rows.push(row);
    })?;
    Ok(out)
}

/// Rows per [`BatchEmitter::rows`] chunk on the streamed scan path: enough to
/// amortize the per-event cost, small enough that the statement's peak memory
/// is a chunk, not the result.
const STREAM_CHUNK_ROWS: usize = 256;

/// The streamed shape of [`scan_select`]: opens the result set, then emits
/// kept rows in [`STREAM_CHUNK_ROWS`] chunks as the scan produces them, so the
/// client sees rows while the scan is still running. On a mid-scan error the
/// full chunks already emitted stand — the caller closes the set (see
/// [`BatchRun::abort_open_rowset`]) — and the partial chunk is dropped.
fn scan_select_streamed(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
    run: &mut BatchRun<'_>,
) -> Result<u64, SqlError> {
    run.open_rowset(plan.columns.clone());
    let mut chunk: Vec<Vec<Datum>> = Vec::new();
    let kept = scan_select_rows(storage, plan, select, eval_ctx, &mut |row| {
        chunk.push(row);
        if chunk.len() >= STREAM_CHUNK_ROWS {
            run.rows(std::mem::take(&mut chunk));
        }
    })?;
    run.rows(chunk);
    Ok(kept)
}

/// Walks the plan's access path, filters, projects, and hands each kept row to
/// `sink`, stopping once `TOP` is satisfied. Returns the number of rows kept
/// (which `TOP` counts, matching the collecting path's truncation of the
/// filtered rows — `TOP 0` never reaches here, the gate declines it). Both
/// executions of the scan shape ride this walk: [`scan_select`] collects into
/// a `RowSet`, [`scan_select_streamed`] emits chunks as slices are read.
fn scan_select_rows(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
    sink: &mut dyn FnMut(Vec<Datum>),
) -> Result<u64, SqlError> {
    #[cfg(test)]
    storage.count_scan_select();
    let types = &plan.types;
    let mut kept: u64 = 0;
    let enough = |kept: u64| select.top.is_some_and(|top| kept >= top);

    // One row: filter it, and project it or drop it. `Ok(false)` once TOP is
    // satisfied and the caller should stop reading.
    let mut take = |row: Vec<Datum>| -> Result<bool, SqlError> {
        check_cancelled()?;
        if let Some(predicate) = &select.where_clause {
            let sql_row = row_values(&row, types);
            if !where_keeps(predicate, &sql_row, &plan.resolver, eval_ctx)? {
                return Ok(true);
            }
        }
        sink(plan.picks.iter().map(|i| row[*i].clone()).collect());
        kept += 1;
        Ok(!enough(kept))
    };

    match &plan.access {
        plan::AccessPath::TableScan => {
            if let Some(snapshot) = current_snapshot() {
                // A versioned reader holds no table lock, so the sliced
                // cursor's between-slice contract does not hold for it; the
                // snapshot scan reads the table atomically and merges the
                // version store.
                let rows = storage
                    .rel_scan_snapshot(&plan.table, Some(&plan.needed), snapshot)
                    .map_err(|err| map_storage_err(err, &plan.table))?;
                for row in rows {
                    if !take(row)? {
                        break;
                    }
                }
            } else {
                let mut cursor = ScanCursor::start();
                let mut slice: Vec<Vec<Datum>> = Vec::new();
                'scan: while !cursor.done() {
                    cursor = storage
                        .rel_scan_slice(
                            &plan.table,
                            cursor,
                            SCAN_SLICE_ROWS,
                            Some(&plan.needed),
                            &mut slice,
                        )
                        .map_err(|err| map_storage_err(err, &plan.table))?;
                    for row in slice.drain(..) {
                        if !take(row)? {
                            break 'scan;
                        }
                    }
                }
            }
        }
        plan::AccessPath::IndexSeek {
            index_object_id,
            lower,
            upper,
            ..
        } => {
            // The seek narrows the candidates; the predicate still re-checks
            // each one, so the result matches a full scan.
            let rows = storage
                .rel_index_scan(
                    &plan.table,
                    *index_object_id,
                    lower.clone(),
                    upper.clone(),
                    Some(&plan.needed),
                    plan.covering,
                    current_snapshot(),
                )
                .map_err(|err| map_storage_err(err, &plan.table))?;
            for row in rows {
                if !take(row)? {
                    break;
                }
            }
        }
    }
    Ok(kept)
}

/// `SELECT @a = expr, @b = expr2 [FROM ...]` — an assignment SELECT. The value
/// expressions are projected as an ordinary result set; each variable then
/// takes the value from the *last* row the query produces (SQL Server's
/// documented behaviour for the final value). Zero rows leave the variables
/// unchanged. A value that reads a variable being assigned in the same
/// statement (running aggregation, cross-referencing targets) is rejected
/// rather than evaluated against the pre-statement snapshot, which would give a
/// result that silently differs from SQL Server's per-row assignment.
fn exec_select_assign(
    storage: &Storage,
    select: &Select,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // Every target must be a declared variable; capture their declared types.
    let mut targets: Vec<(String, ColumnType)> = Vec::with_capacity(select.items.len());
    for item in &select.items {
        let SelectItem::Assign { target, .. } = item else {
            // The dispatcher only routes here when every item is an assignment.
            unreachable!("assignment SELECT has a non-assignment item");
        };
        let column_type = txn_ctx
            .variables
            .get(target)
            .map(|(t, _)| *t)
            .ok_or_else(|| undeclared_variable_err(target))?;
        targets.push((target.clone(), column_type));
    }

    // Every value is evaluated against the variables' pre-statement values, so a
    // value that references a variable being assigned here would silently
    // diverge from SQL Server's per-row / left-to-right assignment (running
    // aggregation, cross-referencing targets). Reject those rather than compute
    // a wrong result; the caller can use SET or a set-based aggregate instead.
    let target_names: std::collections::HashSet<&str> =
        targets.iter().map(|(name, _)| name.as_str()).collect();
    for item in &select.items {
        let SelectItem::Assign { value, .. } = item else {
            unreachable!()
        };
        if expr_uses_local_var(value, &target_names) {
            return Err(SqlError::message_only(
                141,
                "An assignment SELECT cannot reference a variable it is assigning in the same statement; use SET or a set-based aggregate.",
            ));
        }
    }

    // Project the value expressions as an ordinary result set.
    let projected = Select {
        items: select
            .items
            .iter()
            .map(|item| {
                let SelectItem::Assign { value, .. } = item else {
                    unreachable!()
                };
                SelectItem::Expr {
                    expr: value.clone(),
                    alias: None,
                }
            })
            .collect(),
        ..select.clone()
    };
    let rowset = exec_select(storage, &projected, &txn_ctx.eval_context())?;

    // Assign the last row's values (SQL Server: the variable holds the value
    // from the final row). No rows -> variables keep their current values.
    if let Some(last) = rowset.rows.last() {
        for (index, (name, column_type)) in targets.iter().enumerate() {
            let produced = value::datum_to_sql(&last[index], &rowset.columns[index].column_type);
            let datum = value::sql_to_datum(&produced, column_type, name)?;
            let coerced = value::datum_to_sql(&datum, column_type);
            txn_ctx
                .variables
                .insert(name.clone(), (*column_type, coerced));
        }
    }
    // SQL Server counts the rows an assignment SELECT processed: the DONE
    // carries it and `@@ROWCOUNT` reports it.
    Ok(StatementResult::RowsAffected(rowset.rows.len() as u64))
}

/// Removes duplicate output rows (SELECT DISTINCT), keeping first occurrence.
/// NULLs are equal to each other (`Datum` equality), matching SQL Server.
fn dedup_rows(
    storage: &Storage,
    rowset: &mut RowSet,
    sensitivities: &[CollationSensitivity],
) -> Result<(), SqlError> {
    // Hash-based DISTINCT — O(n) instead of the old O(n²) linear scan. Each
    // output column is single-typed (projection coerced it), so `HashKey`'s
    // `order_key_cmp` equality agrees with the former `Vec<Datum>` equality for
    // every realistic input. (Edge: two `float` NaN rows now collapse to one —
    // `order_key_cmp` treats NaN as equal, like GROUP BY already did — where the
    // old raw `Datum` `==` kept them distinct.)
    let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
    // DISTINCT folds string columns by each output column's collation
    // (`sensitivities`, parallel to the columns), so a `_CI` column folds case
    // and a `_CS`/`_BIN` column stays exact — consistent with GROUP BY and
    // COUNT(DISTINCT). `dedup_key` keeps the original row for output.
    let dedup_key = |row: &[Datum]| hash::fold_hash_key(&row_values(row, &types), sensitivities);
    let approx: usize = rowset.rows.iter().map(|r| approx_row_bytes(r)).sum();
    if approx <= sort_budget() {
        // In-memory: keep first-appearance order (DISTINCT without ORDER BY has
        // no guaranteed order, but this is the least-surprising small-set result).
        let mut seen: std::collections::HashSet<hash::HashKey> = std::collections::HashSet::new();
        rowset
            .rows
            .retain(|row| seen.insert(hash::HashKey(dedup_key(row))));
        return Ok(());
    }

    // Grace-hash DISTINCT: partition rows by key hash into temp extents (equal
    // rows share a partition), then dedup each partition in memory. The per-
    // partition dedup set is bounded to ~one partition instead of the whole
    // input. Output is by partition (immaterial — a spilling DISTINCT is not
    // order-sensitive; any ORDER BY runs afterward).
    let partitions = (approx / sort_budget() + 1).max(2);
    let partition_of = |key: &[SqlValue]| -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hash::HashKey(key.to_vec()).hash(&mut hasher);
        (hasher.finish() % partitions as u64) as usize
    };
    let spill_err = |e| map_storage_err(e, "<distinct spill>");
    let mut parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    for row in &rowset.rows {
        // Partition by the *folded* key so case-insensitive-equal rows land in
        // the same partition (else a cross-partition duplicate is missed); the
        // stored row stays original for output.
        let key = dedup_key(row);
        parts[partition_of(&key)]
            .write_row(row)
            .map_err(spill_err)?;
    }
    for part in parts.iter_mut() {
        part.finish_writing().map_err(spill_err)?;
    }
    let mut out: Vec<Vec<Datum>> = Vec::new();
    for part in parts.iter_mut() {
        let mut seen: std::collections::HashSet<hash::HashKey> = std::collections::HashSet::new();
        let mut reader = part.reader();
        while let Some(row) = reader.next_row().map_err(spill_err)? {
            if seen.insert(hash::HashKey(dedup_key(&row))) {
                out.push(row);
            }
        }
    }
    rowset.rows = out;
    Ok(())
}

/// Orders an output RowSet by ORDER BY items referencing the output: a bare
/// integer is a 1-based output-column ordinal; any other expression is
/// evaluated against the output row (its columns are the resolver). Uses
/// code-point ordering (NULLs first), stable.
fn order_output(
    rowset: &mut RowSet,
    order_by: &[OrderItem],
    eval_ctx: &EvalContext,
) -> Result<(), SqlError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let names: Vec<String> = rowset.columns.iter().map(|c| c.name.clone()).collect();
    let scope = OutputScope { names };
    let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
    let mut keyed: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(rowset.rows.len());
    for (index, row) in rowset.rows.iter().enumerate() {
        let sql_row = row_values(row, &types);
        let mut key = Vec::with_capacity(order_by.len());
        for item in order_by {
            let value = if let ExprKind::Int(n) = &item.expr.kind {
                let ordinal = usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .filter(|&i| i < sql_row.len())
                    .ok_or_else(|| {
                        SqlError::new(
                            108,
                            16,
                            1,
                            format!("The ORDER BY position number {n} is out of range."),
                        )
                    })?;
                sql_row[ordinal].clone()
            } else {
                eval::eval(&item.expr, &sql_row, &scope, eval_ctx)?
            };
            key.push(value);
        }
        keyed.push((key, index));
    }
    keyed.sort_by(|(ka, ia), (kb, ib)| {
        for (index, item) in order_by.iter().enumerate() {
            let mut ord = order_key_cmp(&ka[index], &kb[index]);
            if item.descending {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        ia.cmp(ib)
    });
    rowset.rows = keyed.iter().map(|(_, i)| rowset.rows[*i].clone()).collect();
    Ok(())
}

/// Per-query sort memory budget: a sort whose rows exceed this spills to temp
/// extents (external merge sort) rather than growing without bound.
const SORT_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

/// A row paired with its evaluated ORDER BY key, as carried through the sort.
type KeyedRow = (Vec<SqlValue>, Vec<Datum>);

#[cfg(test)]
thread_local! {
    /// Test-only override that forces the external-sort spill path on small
    /// inputs (execution runs on the calling thread in tests).
    static TEST_SORT_BUDGET: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// The active sort memory budget (overridable in tests).
fn sort_budget() -> usize {
    #[cfg(test)]
    if let Some(budget) = TEST_SORT_BUDGET.with(std::cell::Cell::get) {
        return budget;
    }
    SORT_MEMORY_BUDGET
}

/// Forces (or clears) the sort spill budget for the current test thread.
#[cfg(test)]
pub(crate) fn set_test_sort_budget(budget: Option<usize>) {
    TEST_SORT_BUDGET.with(|cell| cell.set(budget));
}

thread_local! {
    /// The cancellation flag for the batch running on this worker thread — set by
    /// the connection task when a TDS Attention (cancel) arrives. Executor loops
    /// poll it via [`check_cancelled`] so a running statement can be aborted.
    static CANCEL_FLAG: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// Binds a cancellation flag to the current thread for one batch, clearing it on
/// drop so a later batch on the same pooled worker never sees a stale flag.
pub struct CancelScope;

impl CancelScope {
    pub fn enter(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) -> CancelScope {
        CANCEL_FLAG.with(|c| *c.borrow_mut() = Some(flag));
        CancelScope
    }
}

impl Drop for CancelScope {
    fn drop(&mut self) {
        CANCEL_FLAG.with(|c| *c.borrow_mut() = None);
    }
}

/// True if the batch on this thread has been asked to cancel (Attention).
fn is_cancelled() -> bool {
    CANCEL_FLAG.with(|c| {
        c.borrow()
            .as_ref()
            .is_some_and(|f| f.load(std::sync::atomic::Ordering::Relaxed))
    })
}

/// Errors if the current batch has been cancelled (TDS Attention). Executor
/// loops call this periodically so a long statement aborts mid-flight. The
/// client is answered with a `DONE(attention)`, not this error — it is an
/// internal marker the batch driver recognises to stop without dooming the txn.
pub fn check_cancelled() -> Result<(), SqlError> {
    if is_cancelled() {
        Err(SqlError::message_only(
            CANCEL_ERROR,
            "The query was canceled.",
        ))
    } else {
        Ok(())
    }
}

/// The error number [`check_cancelled`] raises. The batch driver keys on this
/// (not the raw cancel flag) so a concurrent Attention can't suppress the
/// `XACT_ABORT`/severity dooming of an *unrelated* statement failure.
const CANCEL_ERROR: i32 = 3617;

/// Sets the current thread's cancel flag (test helper — execution runs on the
/// calling thread in tests, so this simulates an Attention).
#[cfg(test)]
pub(crate) fn set_test_cancel(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    CANCEL_FLAG.with(|c| *c.borrow_mut() = Some(flag));
}

/// Clears the current thread's cancel flag (test helper — reset before other
/// tests reuse the thread).
#[cfg(test)]
pub(crate) fn clear_test_cancel() {
    CANCEL_FLAG.with(|c| *c.borrow_mut() = None);
}

/// The ORDER BY comparator for one pair of pre-evaluated key tuples: per item,
/// collation-aware for a character column, else value order (NULLs first);
/// `descending` reverses. No tie-break here — the caller adds stability.
fn compare_sort_keys(
    a: &[SqlValue],
    b: &[SqlValue],
    order_by: &[OrderItem],
    collators: &[Option<collation::Collation>],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (col, item) in order_by.iter().enumerate() {
        let ord = match (&collators[col], &a[col], &b[col]) {
            (Some(coll), SqlValue::Str(x), SqlValue::Str(y)) => coll.compare(x, y),
            (Some(_), SqlValue::Null, SqlValue::Null) => Ordering::Equal,
            (Some(_), SqlValue::Null, _) => Ordering::Less,
            (Some(_), _, SqlValue::Null) => Ordering::Greater,
            _ => order_key_cmp(&a[col], &b[col]),
        };
        let ord = if item.descending { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Builds the per-item collators (only a bare character column is collation-
/// ordered; everything else uses value order).
fn sort_collators(
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
) -> Vec<Option<collation::Collation>> {
    order_by
        .iter()
        .map(|item| {
            let index = bare_column_index(&item.expr, resolver)?;
            let is_char = matches!(
                types.get(index),
                Some(ColumnType::VarChar { .. }) | Some(ColumnType::NVarChar { .. })
            );
            if !is_char {
                return None;
            }
            let name = collations
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| collation::DEFAULT_COLLATION.to_string());
            Some(collation::Collation::from_name(&name))
        })
        .collect()
}

/// The ORDER BY key of one row.
fn sort_key(
    row: &[Datum],
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<SqlValue>, SqlError> {
    let values = row_values(row, types);
    order_by
        .iter()
        .map(|item| eval::eval(&item.expr, &values, resolver, eval_ctx))
        .collect()
}

/// A rough in-memory byte estimate for a row, for the sort budget.
fn approx_row_bytes(row: &[Datum]) -> usize {
    let payload: usize = row
        .iter()
        .map(|d| match d {
            Datum::VarChar(s) | Datum::NVarChar(s) => s.len() + 16,
            Datum::VarBinary(b) => b.len() + 16,
            _ => 16,
        })
        .sum();
    payload + 24
}

/// Sorts the (already WHERE-filtered) source rows by ORDER BY. Fits-in-budget
/// inputs sort in memory (Rust's stable `sort_by`); a larger input spills
/// sorted runs to temp extents and k-way merges them (external merge sort),
/// bounding the sort's working memory instead of erroring or doubling memory.
fn order_rows(
    storage: &Storage,
    rows: Vec<Vec<Datum>>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<Vec<Datum>>, SqlError> {
    order_rows_budgeted(
        storage,
        rows,
        order_by,
        types,
        collations,
        resolver,
        eval_ctx,
        sort_budget(),
    )
}

#[allow(clippy::too_many_arguments)]
fn order_rows_budgeted<'a>(
    storage: &'a Storage,
    rows: Vec<Vec<Datum>>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    budget: usize,
) -> Result<Vec<Vec<Datum>>, SqlError> {
    let collators = sort_collators(order_by, types, collations, resolver);
    let cmp = |a: &Vec<SqlValue>, b: &Vec<SqlValue>| compare_sort_keys(a, b, order_by, &collators);

    // Generate sorted runs, spilling a run to a `RowSpool` each time the
    // accumulated rows reach the budget. The final (in-memory) run is kept.
    let mut runs: Vec<crate::relstore::spill::RowSpool<'a>> = Vec::new();
    let mut current: Vec<KeyedRow> = Vec::new();
    let mut current_bytes = 0usize;
    for row in rows {
        check_cancelled()?;
        let key = sort_key(&row, order_by, types, resolver, eval_ctx)?;
        current_bytes += approx_row_bytes(&row);
        current.push((key, row));
        if current_bytes >= budget {
            runs.push(sort_and_spill(storage, &mut current, &cmp)?);
            current_bytes = 0;
        }
    }
    // No spill: a plain stable in-memory sort.
    if runs.is_empty() {
        current.sort_by(|(a, _), (b, _)| cmp(a, b));
        return Ok(current.into_iter().map(|(_, row)| row).collect());
    }
    // Sort the final partial run and merge every run.
    current.sort_by(|(a, _), (b, _)| cmp(a, b));
    merge_runs(
        &runs, current, order_by, types, resolver, eval_ctx, &collators,
    )
}

/// Stably sorts `run` in place and writes its rows (in sorted order) to a fresh
/// `RowSpool`, clearing `run`.
fn sort_and_spill<'a>(
    storage: &'a Storage,
    run: &mut Vec<KeyedRow>,
    cmp: &impl Fn(&Vec<SqlValue>, &Vec<SqlValue>) -> std::cmp::Ordering,
) -> Result<crate::relstore::spill::RowSpool<'a>, SqlError> {
    run.sort_by(|(a, _), (b, _)| cmp(a, b));
    let mut spool = crate::relstore::spill::RowSpool::new(storage);
    for (_, row) in run.drain(..) {
        spool
            .write_row(&row)
            .map_err(|e| map_storage_err(e, "<sort spill>"))?;
    }
    spool
        .finish_writing()
        .map_err(|e| map_storage_err(e, "<sort spill>"))?;
    Ok(spool)
}

/// K-way merges the sorted spilled `runs` and the sorted in-memory `tail` run
/// into one sorted row vector. Keys are recomputed per row on read (cheap for
/// column refs); ties prefer the earlier run so the merge is globally stable
/// (spilled runs hold earlier input rows than the in-memory tail).
#[allow(clippy::too_many_arguments)]
fn merge_runs(
    runs: &[crate::relstore::spill::RowSpool<'_>],
    tail: Vec<KeyedRow>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    collators: &[Option<collation::Collation>],
) -> Result<Vec<Vec<Datum>>, SqlError> {
    // One cursor per source: spilled-run readers first, then the in-memory tail.
    let mut readers: Vec<_> = runs.iter().map(|r| r.reader()).collect();
    let mut tail_iter = tail.into_iter();

    // Current head (key + row) of each source, in the same order.
    let source_count = readers.len() + 1;
    let mut heads: Vec<Option<(Vec<SqlValue>, Vec<Datum>)>> = Vec::with_capacity(source_count);
    for reader in &mut readers {
        heads.push(read_head(reader, order_by, types, resolver, eval_ctx)?);
    }
    heads.push(tail_iter.next());

    let total: usize = runs.iter().map(|r| r.row_count() as usize).sum::<usize>() + heads.len();
    let mut out: Vec<Vec<Datum>> = Vec::with_capacity(total);
    loop {
        // Pick the smallest head; on a key tie, the earliest source (lowest
        // index) wins, which preserves input order across runs.
        let mut best: Option<usize> = None;
        for (i, head) in heads.iter().enumerate() {
            let Some((key, _)) = head else { continue };
            match best {
                None => best = Some(i),
                Some(b) => {
                    let (bkey, _) = heads[b].as_ref().unwrap();
                    if compare_sort_keys(key, bkey, order_by, collators) == std::cmp::Ordering::Less
                    {
                        best = Some(i);
                    }
                }
            }
        }
        let Some(i) = best else { break };
        let (_, row) = heads[i].take().unwrap();
        out.push(row);
        // Advance the chosen source.
        heads[i] = if i < readers.len() {
            read_head(&mut readers[i], order_by, types, resolver, eval_ctx)?
        } else {
            tail_iter.next()
        };
    }
    Ok(out)
}

/// Reads the next row from a spool reader and pairs it with its ORDER BY key.
fn read_head(
    reader: &mut crate::relstore::spill::RowSpoolReader,
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Option<KeyedRow>, SqlError> {
    match reader
        .next_row()
        .map_err(|e| map_storage_err(e, "<sort spill>"))?
    {
        Some(row) => {
            let key = sort_key(&row, order_by, types, resolver, eval_ctx)?;
            Ok(Some((key, row)))
        }
        None => Ok(None),
    }
}

fn project(
    storage: &Storage,
    items: &[SelectItem],
    source_columns: &[ResultColumn],
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // Output column plan: a source column (typed, pass-through) or a
    // computed expression (evaluated then typed by inference).
    enum Proj<'a> {
        SourceColumn { index: usize, name: String },
        Expr { name: String, expr: &'a Expr },
    }
    let mut projs = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                for (index, column) in source_columns.iter().enumerate() {
                    projs.push(Proj::SourceColumn {
                        index,
                        name: column.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                let indices = resolver.indices_for_qualifier(&qualifier.value);
                if indices.is_empty() {
                    return Err(SqlError::new(
                        4104,
                        16,
                        1,
                        format!(
                            "The multi-part identifier \"{}.*\" could not be bound.",
                            qualifier.value
                        ),
                    )
                    .at(qualifier.span));
                }
                for index in indices {
                    projs.push(Proj::SourceColumn {
                        index,
                        name: source_columns[index].name.clone(),
                    });
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                match bare_column_index(expr, resolver) {
                    // A bare column still carries its resolved output name so an
                    // `AS alias` (or the referenced name's casing) is preserved.
                    Some(index) => projs.push(Proj::SourceColumn { index, name }),
                    None => projs.push(Proj::Expr { name, expr }),
                }
            }
            // Assignment SELECTs are rewritten to Expr items before projection.
            SelectItem::Assign { .. } => {
                unreachable!("assignment SELECT handled before projection")
            }
        }
    }

    // Precompute all row values once for expression evaluation.
    let row_sql: Vec<Vec<SqlValue>> = rows.iter().map(|r| row_values(r, types)).collect();

    let mut columns = Vec::with_capacity(projs.len());
    let mut out_rows: Vec<Vec<Datum>> = vec![Vec::with_capacity(projs.len()); rows.len()];
    for proj in &projs {
        match proj {
            Proj::SourceColumn { index, name } => {
                columns.push(ResultColumn {
                    name: name.clone(),
                    column_type: source_columns[*index].column_type,
                });
                for (out, row) in out_rows.iter_mut().zip(rows) {
                    out.push(row[*index].clone());
                }
            }
            Proj::Expr { name, expr } => {
                // Evaluate the column for every row, then infer one type. A
                // subquery still present here is correlated (the rewrite pass
                // left it for the per-row bind): substitute the outer row's
                // values in, making it uncorrelated for that row.
                let correlated = expr_needs_binding(storage, expr);
                let mut values = Vec::with_capacity(rows.len());
                for row in &row_sql {
                    let bound;
                    let expr = if correlated {
                        bound = substitute_correlated_in_expr(
                            storage,
                            expr,
                            &|name| resolver.resolve(name),
                            row,
                            eval_ctx,
                        )?;
                        &bound
                    } else {
                        expr
                    };
                    values.push(eval::eval(expr, row, resolver, eval_ctx)?);
                }
                let column_type = value::infer_type(&values);
                for (out, value) in out_rows.iter_mut().zip(&values) {
                    // Coerce each value to the inferred column type (e.g. all
                    // decimals to the widest scale) so the column is uniform.
                    out.push(value::sql_to_datum(value, &column_type, name)?);
                }
                columns.push(ResultColumn {
                    name: name.clone(),
                    column_type,
                });
            }
        }
    }
    Ok(RowSet {
        columns,
        rows: out_rows,
    })
}

fn bare_column_name(expr: &Expr) -> Option<String> {
    match &expr.kind {
        // A qualified `t.col` reference outputs the bare column name.
        ExprKind::Column(name) => Some(name.value.rsplit('.').next().unwrap_or("").to_string()),
        _ => None,
    }
}

fn bare_column_index(expr: &Expr, scope: &JoinScope) -> Option<usize> {
    match &expr.kind {
        ExprKind::Column(name) => scope.resolve(&name.value),
        _ => None,
    }
}

/// Collects every base-table name referenced in a FROM join tree, recursing
/// into derived-table subqueries so their tables are locked too. (Used for the
/// SHOWPLAN table list; [`collect_locked_tables`] is the lock-set collector.)
fn collect_table_names<'a>(tref: &'a TableRef, out: &mut Vec<&'a Name>) {
    match tref {
        TableRef::Table { name, .. } => out.push(name),
        TableRef::Join { left, right, .. } => {
            collect_table_names(left, out);
            collect_table_names(right, out);
        }
        TableRef::Derived { subquery, .. } => {
            if let Some(from) = &subquery.from {
                collect_table_names(from, out);
            }
        }
        TableRef::Function { name, .. } => out.push(name),
    }
}

/// Collects every base table a SELECT reads for the lock set: its FROM tree
/// (including derived-table subqueries and join `ON` clauses) plus every
/// subquery embedded in its expressions (WHERE/SELECT list/HAVING/GROUP BY/
/// ORDER BY). Recurses through nested subqueries.
fn collect_locked_tables<'a>(select: &'a Select, out: &mut Vec<&'a Name>) {
    // CTE bodies read their base tables like any derived table — collected
    // HERE, not left to callers' inlining passes: a condition subquery's
    // `WITH` has no expansion pass before lock analysis, and a missed table
    // is a read with no lock under up-front 2PL (the review's finding). A
    // CTE's own name may land in `out` via FROM references; it resolves to
    // no object and locks nothing, which is correct.
    for cte in &select.ctes {
        collect_locked_tables(&cte.query, out);
    }
    if let Some(from) = &select.from {
        collect_from_tables(from, out);
    }
    for item in &select.items {
        match item {
            SelectItem::Expr { expr, .. } | SelectItem::Assign { value: expr, .. } => {
                collect_expr_tables(expr, out)
            }
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
        }
    }
    for expr in select.where_clause.iter().chain(select.having.iter()) {
        collect_expr_tables(expr, out);
    }
    for expr in &select.group_by {
        collect_expr_tables(expr, out);
    }
    for item in &select.order_by {
        collect_expr_tables(&item.expr, out);
    }
}

/// Collects base tables from a FROM tree, recursing into derived subqueries and
/// join `ON` predicates (which may contain their own subqueries).
fn collect_from_tables<'a>(tref: &'a TableRef, out: &mut Vec<&'a Name>) {
    match tref {
        // A `@t` table variable is session-local: it takes no lock and — via
        // statement_reads_tables — must not arm a snapshot, so it is never
        // collected as a locked/snapshotted table.
        TableRef::Table { name, .. } if name.value.starts_with('@') => {}
        TableRef::Table { name, .. } => out.push(name),
        TableRef::Join {
            left, right, on, ..
        } => {
            collect_from_tables(left, out);
            collect_from_tables(right, out);
            if let Some(on) = on {
                collect_expr_tables(on, out);
            }
        }
        TableRef::Derived { subquery, .. } => collect_locked_tables(subquery, out),
        // A TVF in FROM: its name resolves (via read_lock_object_ids) to the
        // tables its body reads, and its arguments may embed subqueries.
        TableRef::Function { name, args, .. } => {
            out.push(name);
            for arg in args {
                collect_expr_tables(arg, out);
            }
        }
    }
}

/// Collects base tables from every subquery embedded in an expression.
/// True if `expr` references any of the named local variables (`@name`, given
/// without the leading `@`), descending into subqueries. Used to reject an
/// assignment SELECT whose value reads a variable it is assigning.
fn expr_uses_local_var(expr: &Expr, names: &std::collections::HashSet<&str>) -> bool {
    match &expr.kind {
        ExprKind::LocalVar(name) => names.contains(name.as_str()),
        ExprKind::Subquery(select) | ExprKind::Exists(select) => {
            select_uses_local_var(select, names)
        }
        ExprKind::InSubquery { expr, subquery, .. } => {
            expr_uses_local_var(expr, names) || select_uses_local_var(subquery, names)
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => expr_uses_local_var(expr, names),
        ExprKind::Binary { left, right, .. } => {
            expr_uses_local_var(left, names) || expr_uses_local_var(right, names)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expr_uses_local_var(expr, names) || expr_uses_local_var(pattern, names)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_uses_local_var(expr, names) || list.iter().any(|e| expr_uses_local_var(e, names))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_uses_local_var(expr, names)
                || expr_uses_local_var(low, names)
                || expr_uses_local_var(high, names)
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| expr_uses_local_var(o, names))
                || branches
                    .iter()
                    .any(|(w, t)| expr_uses_local_var(w, names) || expr_uses_local_var(t, names))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_uses_local_var(e, names))
        }
        ExprKind::Function { args, .. } => args.iter().any(|a| expr_uses_local_var(a, names)),
        ExprKind::Aggregate { arg, .. } => {
            arg.as_ref().is_some_and(|a| expr_uses_local_var(a, names))
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_) => false,
    }
}

/// True if any expression in `select` references one of the named local
/// variables (descends the SELECT list, WHERE/HAVING, GROUP BY, and ORDER BY).
fn select_uses_local_var(select: &Select, names: &std::collections::HashSet<&str>) -> bool {
    let item_uses = select.items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } | SelectItem::Assign { value: expr, .. } => {
            expr_uses_local_var(expr, names)
        }
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    });
    item_uses
        || select
            .where_clause
            .iter()
            .chain(select.having.iter())
            .chain(select.group_by.iter())
            .any(|e| expr_uses_local_var(e, names))
        || select
            .order_by
            .iter()
            .any(|o| expr_uses_local_var(&o.expr, names))
}

fn collect_expr_tables<'a>(expr: &'a Expr, out: &mut Vec<&'a Name>) {
    match &expr.kind {
        ExprKind::Subquery(select) | ExprKind::Exists(select) => collect_locked_tables(select, out),
        ExprKind::InSubquery { expr, subquery, .. } => {
            collect_expr_tables(expr, out);
            collect_locked_tables(subquery, out);
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => collect_expr_tables(expr, out),
        ExprKind::Binary { left, right, .. } => {
            collect_expr_tables(left, out);
            collect_expr_tables(right, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_expr_tables(expr, out);
            collect_expr_tables(pattern, out);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_expr_tables(expr, out);
            list.iter().for_each(|e| collect_expr_tables(e, out));
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_expr_tables(expr, out);
            collect_expr_tables(low, out);
            collect_expr_tables(high, out);
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_expr_tables(o, out);
            }
            for (when, then) in branches {
                collect_expr_tables(when, out);
                collect_expr_tables(then, out);
            }
            if let Some(e) = else_result {
                collect_expr_tables(e, out);
            }
        }
        ExprKind::Function { args, .. } => args.iter().for_each(|a| collect_expr_tables(a, out)),
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                collect_expr_tables(a, out);
            }
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => {}
    }
}

/// Collects, as OWNED names, the read-lock targets an expression reaches: the
/// tables its subqueries reference and the (user or built-in) functions it
/// calls. Unlike [`collect_expr_tables`], the results do not borrow the input,
/// so they can be gathered from a separately-parsed scalar-function body — the
/// key to locking a table-reading function's inner reads up front under 2PL.
/// Built-in function names collected here resolve to nothing and are harmless.
fn collect_expr_read_names(expr: &Expr, tables: &mut Vec<String>, funcs: &mut Vec<String>) {
    match &expr.kind {
        ExprKind::Function { name, args } => {
            funcs.push(name.clone());
            args.iter()
                .for_each(|a| collect_expr_read_names(a, tables, funcs));
        }
        ExprKind::Subquery(select) | ExprKind::Exists(select) => {
            collect_select_read_names(select, tables, funcs)
        }
        ExprKind::InSubquery { expr, subquery, .. } => {
            collect_expr_read_names(expr, tables, funcs);
            collect_select_read_names(subquery, tables, funcs);
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => collect_expr_read_names(expr, tables, funcs),
        ExprKind::Binary { left, right, .. } => {
            collect_expr_read_names(left, tables, funcs);
            collect_expr_read_names(right, tables, funcs);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_expr_read_names(expr, tables, funcs);
            collect_expr_read_names(pattern, tables, funcs);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_expr_read_names(expr, tables, funcs);
            list.iter()
                .for_each(|e| collect_expr_read_names(e, tables, funcs));
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_expr_read_names(expr, tables, funcs);
            collect_expr_read_names(low, tables, funcs);
            collect_expr_read_names(high, tables, funcs);
        }
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                collect_expr_read_names(a, tables, funcs);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_expr_read_names(o, tables, funcs);
            }
            for (w, r) in branches {
                collect_expr_read_names(w, tables, funcs);
                collect_expr_read_names(r, tables, funcs);
            }
            if let Some(e) = else_result {
                collect_expr_read_names(e, tables, funcs);
            }
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => {}
    }
}

/// Owned read-name collection over a SELECT (see [`collect_expr_read_names`]).
fn collect_select_read_names(select: &Select, tables: &mut Vec<String>, funcs: &mut Vec<String>) {
    for cte in &select.ctes {
        collect_select_read_names(&cte.query, tables, funcs);
    }
    if let Some(from) = &select.from {
        collect_from_read_names(from, tables, funcs);
    }
    for item in &select.items {
        if let SelectItem::Expr { expr, .. } | SelectItem::Assign { value: expr, .. } = item {
            collect_expr_read_names(expr, tables, funcs);
        }
    }
    for expr in select
        .where_clause
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter())
    {
        collect_expr_read_names(expr, tables, funcs);
    }
    for item in &select.order_by {
        collect_expr_read_names(&item.expr, tables, funcs);
    }
}

fn collect_from_read_names(tref: &TableRef, tables: &mut Vec<String>, funcs: &mut Vec<String>) {
    match tref {
        // A `@t` table variable is session-local — no lock, no snapshot.
        TableRef::Table { name, .. } if name.value.starts_with('@') => {}
        TableRef::Table { name, .. } => tables.push(name.value.clone()),
        TableRef::Join {
            left, right, on, ..
        } => {
            collect_from_read_names(left, tables, funcs);
            collect_from_read_names(right, tables, funcs);
            if let Some(on) = on {
                collect_expr_read_names(on, tables, funcs);
            }
        }
        TableRef::Derived { subquery, .. } => collect_select_read_names(subquery, tables, funcs),
        // A TVF in FROM: push the name into `funcs` so select_function_read_ids
        // recurses its body (the owned-collector twin of collect_from_tables).
        TableRef::Function { name, args, .. } => {
            funcs.push(name.value.clone());
            for arg in args {
                collect_expr_read_names(arg, tables, funcs);
            }
        }
    }
}

/// Owned read-name collection over a scalar function body's statement (its
/// reads come only from expressions — a data-returning statement is rejected at
/// CREATE, 444).
fn collect_statement_read_names(
    statement: &Statement,
    tables: &mut Vec<String>,
    funcs: &mut Vec<String>,
) {
    match statement {
        Statement::Return {
            value: Some(expr), ..
        } => collect_expr_read_names(expr, tables, funcs),
        Statement::Set(SetStatement::Variable { value, .. }) => {
            collect_expr_read_names(value, tables, funcs)
        }
        // An assignment SELECT in a function body reads its FROM tables.
        Statement::Select(select) => collect_select_read_names(select, tables, funcs),
        // A multi-statement TVF body's `INSERT @t SELECT …` / `INSERT @t VALUES
        // (subquery)` reads real tables through its source, which must be locked
        // up front. The @t target itself is session-local (no lock).
        Statement::Insert(insert) => match &insert.source {
            InsertSource::Select(select) => collect_select_read_names(select, tables, funcs),
            InsertSource::Values(rows) => {
                for row in rows {
                    for expr in row {
                        collect_expr_read_names(expr, tables, funcs);
                    }
                }
            }
        },
        Statement::Declare(declarations) => {
            for decl in declarations {
                if let Some(init) = &decl.initializer {
                    collect_expr_read_names(init, tables, funcs);
                }
            }
        }
        Statement::If {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_expr_read_names(condition, tables, funcs);
            collect_statement_read_names(then_branch, tables, funcs);
            if let Some(e) = else_branch {
                collect_statement_read_names(e, tables, funcs);
            }
        }
        Statement::While {
            condition, body, ..
        } => {
            collect_expr_read_names(condition, tables, funcs);
            collect_statement_read_names(body, tables, funcs);
        }
        Statement::Block { body, .. } => {
            for inner in body {
                collect_statement_read_names(inner, tables, funcs);
            }
        }
        _ => {}
    }
}

/// A table's exposed name: its alias, else its (schema-stripped) name.
fn exposed_name(name: &Name, alias: Option<&Name>) -> String {
    alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string())
}

/// Collects the exposed names of every table in a FROM tree. A derived table's
/// exposed name is its alias (its inner tables are not exposed to the outer
/// query).
fn collect_exposed_names(tref: &TableRef, out: &mut Vec<String>) {
    match tref {
        TableRef::Table { name, alias } => out.push(exposed_name(name, alias.as_ref())),
        TableRef::Join { left, right, .. } => {
            collect_exposed_names(left, out);
            collect_exposed_names(right, out);
        }
        TableRef::Derived { alias, .. } => out.push(alias.value.clone()),
        TableRef::Function { name, alias, .. } => out.push(exposed_name(name, alias.as_ref())),
    }
}

/// Rejects a FROM clause with duplicate exposed table names / correlation
/// names (SQL Server 1013), which would otherwise bind ambiguously.
fn check_exposed_names(from: &TableRef) -> Result<(), SqlError> {
    let mut names = Vec::new();
    collect_exposed_names(from, &mut names);
    for i in 0..names.len() {
        for j in (i + 1)..names.len() {
            if names[i].eq_ignore_ascii_case(&names[j]) {
                return Err(SqlError::new(
                    1013,
                    16,
                    1,
                    format!(
                        "The objects \"{}\" and \"{}\" in the FROM clause have the same exposed names. Use correlation names to distinguish them.",
                        names[i], names[j]
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn build_source(
    storage: &Storage,
    from: Option<&TableRef>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    if let Some(from) = from {
        check_exposed_names(from)?;
    }
    build_source_inner(storage, from, where_clause, eval_ctx)
}

fn build_source_inner(
    storage: &Storage,
    from: Option<&TableRef>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    match from {
        None => Ok(Source {
            // No FROM: one row, no columns (constant SELECT).
            columns: Vec::new(),
            qualifiers: Vec::new(),
            collations: Vec::new(),
            rows: SourceRows::Materialized(vec![Vec::new()]),
        }),
        // A single top-level table may use the WHERE for an index seek; base
        // tables inside a join scan fully (join-order planning is later).
        Some(TableRef::Table { name, alias }) => {
            build_table_source(storage, name, alias.as_ref(), where_clause, eval_ctx)
        }
        Some(join) => build_join(storage, join, eval_ctx),
    }
}

/// SQL Server caps view/function nesting at 32 levels; a deeper chain (or a view
/// cycle) errors here rather than overflowing the stack.
const MAX_VIEW_NESTING: u32 = 32;

thread_local! {
    /// Current view-expansion depth on this worker thread (each batch runs on
    /// one thread, so a thread-local is per-request).
    static VIEW_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard that increments the view-nesting depth on `enter` and restores it
/// on drop (including the error/`?` paths), erroring past [`MAX_VIEW_NESTING`].
struct ViewDepthGuard;

impl ViewDepthGuard {
    fn enter(view_name: &str) -> Result<Self, SqlError> {
        let depth = VIEW_DEPTH.with(|d| d.get());
        if depth >= MAX_VIEW_NESTING {
            return Err(SqlError::message_only(
                436,
                format!(
                    "View '{view_name}' exceeds the maximum view nesting level of {MAX_VIEW_NESTING} (possibly a view cycle)."
                ),
            ));
        }
        VIEW_DEPTH.with(|d| d.set(depth + 1));
        Ok(ViewDepthGuard)
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// True where object-permission checks apply — not inside an OWNED stored-object
/// body (procedure, function, TVF, view, or trigger), whose reads are covered by
/// ownership chaining (all objects share the single `dbo` owner today), so the
/// caller's permission on the body suffices (the grant-EXECUTE-only pattern).
/// Dynamic SQL (`sp_executesql`) resets `CHAIN_DEPTH`, so it is checked here even
/// when nested in a procedure — matching SQL Server, which does not chain
/// through dynamic SQL.
fn at_top_level() -> bool {
    CHAIN_DEPTH.with(|d| d.get()) == 0 && VIEW_DEPTH.with(|d| d.get()) == 0
}

/// Whether `sec` permits `action` on an object with these permission entries.
/// A matching DENY for any of the session's principals wins (DENY beats GRANT);
/// otherwise a matching GRANT permits; otherwise denied (no implicit grant).
fn permits(perms: &[PermissionEntry], sec: &SecurityContext, action: PermAction) -> bool {
    let mut granted = false;
    for entry in perms {
        if entry.action == action && sec.principals.contains(&entry.grantee) {
            if entry.deny {
                return false;
            }
            granted = true;
        }
    }
    granted
}

/// Enforces `action` on the resolved object `def`, erroring 229 if the session
/// lacks the permission. A no-op for a bypassing session (sysadmin / dbo /
/// internal) and inside any stored-object body (ownership chaining).
fn enforce_object_permission(
    def: &TableDef,
    sec: &SecurityContext,
    action: PermAction,
) -> Result<(), SqlError> {
    if sec.bypass || !at_top_level() || permits(&def.permissions, sec, action) {
        return Ok(());
    }
    Err(SqlError::new(
        229,
        14,
        5,
        format!(
            "The {} permission was denied on the object '{}', database 'truthdb', schema 'dbo'.",
            action.name(),
            def.name
        ),
    ))
}

/// Builds the row source for one base table (or `sys.*` view), stamping every
/// column with the table's qualifier (its alias, else its name).
fn build_table_source(
    storage: &Storage,
    name: &Name,
    alias: Option<&Name>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    // A `@t` table variable: serve its in-memory rows as a materialized source.
    // (The catalog resolver never matches an `@`-name, so this is the only path
    // that handles it — and it never touches Storage.)
    if name.value.starts_with('@') {
        let tv = current_table_var(&name.value)
            .ok_or_else(|| must_declare_table_var(&name.value).at(name.span))?;
        let count = tv.schema.columns.len();
        let columns = tv
            .schema
            .columns
            .iter()
            .map(|c| ResultColumn {
                name: c.name.clone(),
                column_type: c.column_type,
            })
            .collect();
        let collations = tv
            .schema
            .columns
            .iter()
            .map(|c| c.collation.clone())
            .collect();
        return Ok(Source {
            columns,
            qualifiers: vec![Some(qualifier); count],
            collations,
            rows: SourceRows::Materialized(tv.rows),
        });
    }
    // `inserted`/`deleted`: the firing trigger's pseudo-tables. Resolved before
    // the catalog so a real table named `inserted` cannot be reached from inside
    // a trigger body (SQL Server reserves them there too). Only matches when a
    // trigger scope is armed; otherwise falls through to catalog resolution.
    if let Some(source) = current_trigger_source(&name.value, &qualifier) {
        return Ok(source);
    }
    let base = match name.value.to_ascii_lowercase().as_str() {
        "sys.tables" => sys_tables(storage),
        "sys.databases" => sys_databases(storage, eval_ctx),
        "sys.configurations" => sys_configurations(),
        "sys.views" => sys_views(storage),
        "sys.procedures" => sys_procedures(storage),
        "sys.triggers" => sys_triggers(storage),
        "sys.trigger_events" => sys_trigger_events(storage),
        "sys.server_principals" => sys_server_principals(storage),
        "sys.sql_logins" => sys_sql_logins(storage),
        "sys.database_principals" => sys_database_principals(storage),
        "sys.database_role_members" => sys_database_role_members(storage),
        "sys.database_permissions" => sys_database_permissions(storage),
        "sys.parameters" => sys_parameters(storage),
        "sys.objects" => sys_objects(storage),
        "sys.sql_modules" => sys_sql_modules(storage),
        "sys.columns" => sys_columns(storage),
        "sys.indexes" => sys_indexes(storage),
        "sys.check_constraints" => sys_check_constraints(storage),
        "sys.foreign_keys" => sys_foreign_keys(storage),
        "sys.default_constraints" => sys_default_constraints(storage),
        _ => {
            let def = resolve_table(storage, &name.value)
                .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
            // A procedure is not a queryable object (SQL Server 2809).
            if def.is_procedure() {
                return Err(procedure_not_a_table(&def.name).at(name.span));
            }
            // A trigger is not a queryable object either — resolving it as a base
            // table would heap-scan its (empty) root page 0. 208 invalid object.
            if def.is_trigger() {
                return Err(SqlError::invalid_object(&name.value).at(name.span));
            }
            // A scalar function is not a rowset — it cannot appear in FROM.
            // (Table-valued functions, added later, expand here instead.)
            if def.is_function() {
                return Err(function_not_a_table(&def.name).at(name.span));
            }
            // SELECT permission on the base table or view (checked here, at the
            // top level, before a view body expands — the body's own reads are
            // covered by ownership chaining and not re-checked).
            enforce_object_permission(&def, &eval_ctx.security, PermAction::Select)
                .map_err(|e| e.at(name.span))?;
            // A view: run its stored SELECT as a derived table under the view's
            // qualifier. A view over another view expands recursively — building
            // the derived source re-enters `build_table_source` for the inner
            // view — bounded by a nesting-depth guard that turns a view cycle
            // (self- or mutually-referential views) into a clean error instead
            // of a stack overflow.
            if let Some(query_text) = &def.view_query {
                let _guard = ViewDepthGuard::enter(&def.name)?;
                let body = parse_view_query(query_text, &def.name)?;
                let qual = Name {
                    value: qualifier,
                    quoted: false,
                    span: name.span,
                };
                // A view body is a stored-object scope, like a function/TVF
                // body: it must not read the CALLER's table variables. Shadow
                // the read view with an empty one so `SELECT ... FROM @t` inside
                // a view errors 1087 rather than returning caller rows. (An
                // in-statement derived table or CTE is NOT a separate scope and
                // keeps the statement's view — only stored bodies shadow.)
                let _table_var_scope = arm_table_var_view(&std::collections::HashMap::new());
                let _trigger_shadow = TriggerScope::clear();
                return build_derived_source(storage, &body, &qual, eval_ctx);
            }
            let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
            // An index seek narrows the candidate set; the WHERE filter later
            // re-checks, so results match a full scan.
            // Fetched only when choose() can use it (it returns a scan
            // outright without a predicate or indexes).
            let row_count = if def.indexes.is_empty() || where_clause.is_none() {
                None
            } else {
                storage.rel_row_count(&def.name)
            };
            let rows = match plan::choose(&def, &schema, where_clause, eval_ctx, None, row_count) {
                // A scan is handed out LAZY: the consumer pulls slices, so a
                // filtering/folding reader holds one slice, not the table
                // (and the storage lock is still taken per slice, as before).
                // Under a read snapshot the scan materializes atomically
                // instead: a versioned reader holds no table lock, so the
                // sliced cursor's contract does not hold for it.
                plan::AccessPath::TableScan => match current_snapshot() {
                    Some(snapshot) => SourceRows::Materialized(
                        storage
                            .rel_scan_snapshot(&def.name, None, snapshot)
                            .map_err(|err| map_storage_err(err, &def.name))?,
                    ),
                    None => SourceRows::Scan(ScanStream {
                        table: def.name.clone(),
                        cursor: ScanCursor::start(),
                    }),
                },
                plan::AccessPath::IndexSeek {
                    index_object_id,
                    lower,
                    upper,
                    ..
                } => SourceRows::Materialized(
                    storage
                        .rel_index_scan(
                            &def.name,
                            index_object_id,
                            lower,
                            upper,
                            None,
                            false,
                            current_snapshot(),
                        )
                        .map_err(|err| map_storage_err(err, &def.name))?,
                ),
            };
            let columns = schema
                .columns
                .iter()
                .map(|c| ResultColumn {
                    name: c.name.clone(),
                    column_type: c.column_type,
                })
                .collect();
            let collations = schema.columns.iter().map(|c| c.collation.clone()).collect();
            Source {
                columns,
                qualifiers: Vec::new(),
                collations,
                rows,
            }
        }
    };
    let count = base.columns.len();
    Ok(Source {
        qualifiers: vec![Some(qualifier); count],
        ..base
    })
}

/// Expands an inline table-valued function call `dbo.f(args) [AS alias]` in a
/// FROM clause: binds the call's argument values to the function's `@params`,
/// then runs its stored body SELECT as a derived table under the call's
/// qualifier — a parameterized view. The body's table reads are locked and
/// snapshotted up front by the lock analysis and the snapshot-scope arming,
/// which both resolve the function name into its body (see collect_read_lock_ids
/// and statement_reads_tables); the body reads under the caller's ambient
/// snapshot on this thread. Recursion is bounded by the shared view-depth guard.
fn build_function_source(
    storage: &Storage,
    name: &Name,
    args: &[Expr],
    alias: Option<&Name>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let def = resolve_table(storage, &name.value)
        .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
    let function = def
        .function
        .as_ref()
        .ok_or_else(|| function_not_a_table(&def.name).at(name.span))?;
    // A table-valued function in FROM is read like a table: SELECT permission.
    enforce_object_permission(&def, &eval_ctx.security, PermAction::Select)
        .map_err(|e| e.at(name.span))?;
    if args.len() < function.params.len() {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {}.",
                def.name
            ),
        )
        .at(name.span));
    }
    if args.len() > function.params.len() {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        )
        .at(name.span));
    }
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let qual = Name {
        value: qualifier,
        quoted: false,
        span: name.span,
    };
    match &function.returns {
        FunctionReturns::InlineTable { select_text } => {
            // Bind the arguments to the parameters, coercing to the declared
            // types, in a FRESH variable scope (a TVF body sees only its
            // parameters, not caller locals). Arguments may themselves contain
            // subqueries or scalar UDFs.
            let no_outer = |_: &str| -> Option<usize> { None };
            let mut variables = std::collections::HashMap::new();
            for (param, arg) in function.params.iter().zip(args) {
                let column_type = ColumnType::parse(&param.type_spec)
                    .map_err(|e| SqlError::message_only(245, e.to_string()))?;
                let value = substitute_correlated_in_expr(storage, arg, &no_outer, &[], eval_ctx)
                    .and_then(|bound| eval_constant(&bound, eval_ctx))?;
                let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
                variables.insert(
                    param.name.clone(),
                    value::datum_to_sql(&datum, &column_type),
                );
            }
            let mut fn_ctx = eval_ctx.clone();
            fn_ctx.variables = variables;
            // Expand the body like a view (bounded by the shared nesting guard).
            let _guard = ViewDepthGuard::enter(&def.name)?;
            let body = parse_view_query(select_text, &def.name)?;
            // A TVF body sees only its parameters, not caller locals — the scalar
            // side is isolated above (fresh `variables`); do the same for the
            // table-variable read view. Without this the body's `FROM @t` would
            // resolve against the CALLER's table variable, since build_derived_
            // source runs under whatever scope the calling statement armed. An
            // empty view makes such a body error 1087, as SQL Server rejects it.
            let _table_var_scope = arm_table_var_view(&std::collections::HashMap::new());
            let _trigger_shadow = TriggerScope::clear();
            build_derived_source(storage, &body, &qual, &fn_ctx)
        }
        FunctionReturns::MultiStatementTable {
            returns_var,
            columns_text,
            body,
        } => run_multi_statement_tvf(
            storage,
            function,
            returns_var,
            columns_text,
            body,
            args,
            &qual,
            eval_ctx,
        ),
        // A scalar function called in table position is not a rowset.
        FunctionReturns::Scalar { .. } => Err(function_not_a_table(&def.name).at(name.span)),
    }
}

/// Runs a multi-statement TVF and returns its result table variable's rows as a
/// materialized source. The body runs in an isolated context (a fresh
/// `TxnContext`, like a scalar UDF: parameters only, no transaction, ambient
/// snapshot for its reads) seeded with the empty result table variable, which
/// its statements populate; the accumulated rows are the function's result.
#[allow(clippy::too_many_arguments)]
fn run_multi_statement_tvf(
    storage: &Storage,
    function: &FunctionDef,
    returns_var: &str,
    columns_text: &str,
    body_text: &str,
    args: &[Expr],
    qual: &Name,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    // Rebuild the result table variable's schema (re-parsed per call, like the
    // body — the CREATE-time validation guarantees this succeeds).
    let (columns, primary_key) = truthdb_sql::parse_table_var_columns(columns_text)?;
    let (schema, key_columns, defaults) =
        build_table_var_definition(returns_var, &columns, &primary_key)?;
    // Fresh isolated scope: parameters only, caller session identity carried for
    // DB_NAME()/SUSER_SNAME()/USER_NAME()/@@SPID and role membership. Arguments
    // evaluate in the CALLER's context. The sids are left 0 (the body does not
    // re-resolve membership — it reuses the caller's already-computed role set).
    let mut txn_ctx = TxnContext::default();
    txn_ctx.set_session_identity(
        eval_ctx.database.clone(),
        eval_ctx.login.clone(),
        eval_ctx.spid,
        eval_ctx.user.clone(),
        0,
        0,
    );
    txn_ctx.session_server_roles = eval_ctx.server_roles.clone();
    txn_ctx.session_db_roles = eval_ctx.db_roles.clone();
    txn_ctx.security = eval_ctx.security.clone();
    let no_outer = |_: &str| -> Option<usize> { None };
    for (param, arg) in function.params.iter().zip(args) {
        let column_type = ColumnType::parse(&param.type_spec)
            .map_err(|e| SqlError::message_only(245, e.to_string()))?;
        let value = substitute_correlated_in_expr(storage, arg, &no_outer, &[], eval_ctx)
            .and_then(|bound| eval_constant(&bound, eval_ctx))?;
        let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
        txn_ctx.variables.insert(
            param.name.clone(),
            (column_type, value::datum_to_sql(&datum, &column_type)),
        );
    }
    // Seed the empty result table variable; the body populates it.
    txn_ctx.table_variables.insert(
        returns_var.to_string(),
        TableVar {
            schema,
            key_columns,
            defaults,
            rows: Vec::new(),
        },
    );
    let statements = truthdb_sql::parse_table_function_body(body_text)?;
    // A multi-statement TVF called from a trigger body does not see
    // inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // A multi-statement TVF body ownership-chains: reads are not re-checked.
    let _chain = ChainGuard::enter();
    // Same nesting cap as a scalar UDF (217), decremented on every exit path.
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        Err(SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        ))
    } else {
        let mut emitter = DiscardEmitter;
        let mut run = BatchRun {
            emitter: &mut emitter,
            deferred: Vec::new(),
            rowset_open: false,
            durability_failed: false,
            committed: false,
            last_error: None,
            // A multi-statement TVF's RETURN carries no value.
            function_return_type: None,
        };
        run_block(storage, &statements, &mut txn_ctx, &mut run, false).map(|_| ())
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    result?;
    // The accumulated rows are the result. Serve them as a materialized source
    // stamped with the call's qualifier (identical shape to the @t FROM branch).
    let tv = txn_ctx
        .table_variables
        .get(returns_var)
        .expect("seeded above");
    let count = tv.schema.columns.len();
    let columns_out = tv
        .schema
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            column_type: c.column_type,
        })
        .collect();
    let collations = tv
        .schema
        .columns
        .iter()
        .map(|c| c.collation.clone())
        .collect();
    Ok(Source {
        columns: columns_out,
        qualifiers: vec![Some(qual.value.clone()); count],
        collations,
        rows: SourceRows::Materialized(tv.rows.clone()),
    })
}

/// Recursively builds a join tree's combined row source (base tables scan
/// fully).
fn build_join(
    storage: &Storage,
    tref: &TableRef,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    match tref {
        TableRef::Table { name, alias } => {
            build_table_source(storage, name, alias.as_ref(), &None, eval_ctx)
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => {
            let left = build_join(storage, left, eval_ctx)?;
            let right = build_join(storage, right, eval_ctx)?;
            join_sources(storage, left, right, *kind, on.as_ref(), eval_ctx)
        }
        TableRef::Derived { subquery, alias } => {
            build_derived_source(storage, subquery, alias, eval_ctx)
        }
        TableRef::Function { name, args, alias } => {
            build_function_source(storage, name, args, alias.as_ref(), eval_ctx)
        }
    }
}

/// Builds a derived table's row source by executing its subquery and stamping
/// every output column with the derived-table alias. Every column must be named
/// (8155) and names must be unique within the derived table (8156).
fn build_derived_source(
    storage: &Storage,
    subquery: &Select,
    alias: &Name,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let rowset = exec_select(storage, subquery, eval_ctx)?;
    for (index, column) in rowset.columns.iter().enumerate() {
        if column.name.is_empty() {
            return Err(SqlError::new(
                8155,
                16,
                2,
                format!(
                    "No column name was specified for column {} of '{}'.",
                    index + 1,
                    alias.value
                ),
            ));
        }
        if rowset.columns[..index]
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&column.name))
        {
            return Err(SqlError::new(
                8156,
                16,
                1,
                format!(
                    "The column '{}' was specified multiple times for '{}'.",
                    column.name, alias.value
                ),
            ));
        }
    }
    let count = rowset.columns.len();
    Ok(Source {
        columns: rowset.columns,
        qualifiers: vec![Some(alias.value.clone()); count],
        // KNOWN LIMITATION: a RowSet carries no per-column collation, so a
        // derived character column loses its source collation and an outer
        // ORDER BY sorts it under the database default rather than the base
        // column's COLLATE. Fixing this needs collation threaded through the
        // project/RowSet boundary; deferred (narrow, non-default-collation only).
        collations: vec![None; count],
        rows: SourceRows::Materialized(rowset.rows),
    })
}

/// Joins two sources. The PROBE side — the side driving output, walked exactly
/// once: left, or right for a RIGHT join — streams slice-by-slice; only the
/// BUILD side is materialized here, and the hash join grace-spills it past the
/// memory budget. The ON predicate (absent for CROSS) is evaluated against the
/// concatenated row; outer joins emit NULL-extended rows for unmatched sides.
fn join_sources(
    storage: &Storage,
    left: Source,
    right: Source,
    kind: JoinKind,
    on: Option<&Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut qualifiers = left.qualifiers.clone();
    qualifiers.extend(right.qualifiers.clone());
    let mut collations = left.collations.clone();
    collations.extend(right.collations.clone());
    let types: Vec<ColumnType> = columns.iter().map(|c| c.column_type).collect();
    let scope = JoinScope {
        columns: qualifiers
            .iter()
            .zip(&columns)
            .map(|(q, c)| (q.clone(), c.name.clone()))
            .collect(),
        collations: collations.clone(),
    };
    let left_nulls = vec![Datum::Null; left.columns.len()];
    let right_nulls = vec![Datum::Null; right.columns.len()];

    let concat = |l: &[Datum], r: &[Datum]| -> Vec<Datum> { l.iter().chain(r).cloned().collect() };
    let matches = |l: &[Datum], r: &[Datum]| -> Result<bool, SqlError> {
        match on {
            None => Ok(true),
            Some(pred) => {
                let row = concat(l, r);
                match eval::eval(pred, &row_values(&row, &types), &scope, eval_ctx)? {
                    SqlValue::Bool(b) => Ok(b),
                    SqlValue::Null => Ok(false),
                    _ => Err(SqlError::new(
                        4145,
                        15,
                        1,
                        "An expression of non-boolean type specified in a context where a condition is expected, near 'ON'.",
                    )
                    .at(pred.span)),
                }
            }
        }
    };

    // Equijoin key columns (bare `left_col = right_col` conjuncts of a
    // hash-compatible type). When present on an INNER/LEFT/RIGHT/FULL join, a
    // hash join replaces the O(n·m) nested loop; the full ON predicate is still
    // re-checked on each hash candidate, so the result set and its order are
    // identical to the nested loop. (Like a real optimizer, the hash join
    // evaluates the ON predicate only on candidate pairs sharing a key, so a
    // side-effecting error in a residual conjunct — e.g. `1/b.z` — may be raised
    // on fewer rows than the loop would; the SQL result set is unaffected.)
    // CROSS and equi-key-less joins keep the loop.
    let equi = match on {
        Some(pred) => extract_equi_keys(pred, &left, &right),
        None => Vec::new(),
    };

    // The build side is the one NOT driving output: left for RIGHT, else
    // right. It is walked repeatedly (nested loop) or hashed whole, so it
    // materializes here (bounded by the grace-hash spill past the budget);
    // the probe side stays a stream.
    let build_left = matches!(kind, JoinKind::Right);
    let (probe, build) = if build_left {
        (right, left)
    } else {
        (left, right)
    };
    let build = MaterializedSource::from(build, storage)?;
    // LEFT/RIGHT/FULL null-extend unmatched probe rows; FULL also null-extends
    // unmatched build rows. Emission is oriented so output is always
    // [left columns .. right columns].
    let preserve_probe = matches!(kind, JoinKind::Left | JoinKind::Right | JoinKind::Full);
    let preserve_build = matches!(kind, JoinKind::Full);
    let emit_match = |p: &[Datum], b: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(b, p)
        } else {
            concat(p, b)
        }
    };
    let emit_probe_only = |p: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(&left_nulls, p)
        } else {
            concat(p, &right_nulls)
        }
    };
    let emit_build_only = |b: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(b, &right_nulls)
        } else {
            concat(&left_nulls, b)
        }
    };

    let mut rows = Vec::new();
    if !equi.is_empty() && !matches!(kind, JoinKind::Cross) {
        hash_join(
            storage,
            probe,
            &build,
            build_left,
            &equi,
            preserve_probe,
            preserve_build,
            &matches,
            &emit_match,
            &emit_probe_only,
            &emit_build_only,
            &mut rows,
        )?;
    } else {
        // Nested loop: stream the probe side, walking the whole build side
        // per probe row.
        let mut build_matched = vec![false; build.rows.len()];
        let mut probe_rows = probe.rows;
        while let Some(slice) = probe_rows.next_slice(storage)? {
            for p in &slice {
                check_cancelled()?;
                let mut matched = false;
                for (bi, b) in build.rows.iter().enumerate() {
                    if matches_oriented(p, b, build_left, &matches)? {
                        rows.push(emit_match(p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
                if preserve_probe && !matched {
                    rows.push(emit_probe_only(p));
                }
            }
        }
        if preserve_build {
            for (bi, b) in build.rows.iter().enumerate() {
                if !build_matched[bi] {
                    rows.push(emit_build_only(b));
                }
            }
        }
    }
    Ok(Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    })
}

/// An equijoin key pair: `(left column index, right column index)` for a
/// `left_col = right_col` conjunct of the ON predicate.
type EquiKey = (usize, usize);

/// Extracts the equijoin key pairs usable for a hash join from an ON predicate:
/// the top-level `AND` conjuncts that are `col = col` with one bare column
/// resolving uniquely to the left source, the other uniquely to the right, and
/// matching hash classes. A predicate with no such conjunct (a range/disjunction
/// join, an expression key, or a type-mismatched equality) yields an empty list
/// and the caller keeps the nested-loop join. Non-equi conjuncts are left for
/// the full-ON re-check on each hash candidate, so results are unchanged.
fn extract_equi_keys(pred: &Expr, left: &Source, right: &Source) -> Vec<EquiKey> {
    let left_scope = left.scope();
    let right_scope = right.scope();
    // `Some(true)` = resolves uniquely to the left source, `Some(false)` = right,
    // `None` = neither, both, or not a bare column.
    let side_of = |expr: &Expr| -> Option<(bool, usize)> {
        let ExprKind::Column(name) = &expr.kind else {
            return None;
        };
        match (
            left_scope.resolve(&name.value),
            right_scope.resolve(&name.value),
        ) {
            (Some(i), None) => Some((true, i)),
            (None, Some(j)) => Some((false, j)),
            _ => None,
        }
    };
    let mut conjuncts = Vec::new();
    flatten_and(pred, &mut conjuncts);
    let mut keys = Vec::new();
    for conjunct in conjuncts {
        let ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left: le,
            right: re,
        } = &conjunct.kind
        else {
            continue;
        };
        let pair = match (side_of(le), side_of(re)) {
            (Some((true, i)), Some((false, j))) => (i, j),
            (Some((false, j)), Some((true, i))) => (i, j),
            _ => continue,
        };
        if hash::hash_class(left.columns[pair.0].column_type)
            == hash::hash_class(right.columns[pair.1].column_type)
        {
            keys.push(pair);
        }
    }
    keys
}

/// Collects the top-level `AND` conjuncts of an expression (flattening nested
/// `AND`s); any other expression is one conjunct.
fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let ExprKind::Binary {
        op: ast::BinaryOp::And,
        left,
        right,
    } = &expr.kind
    {
        flatten_and(left, out);
        flatten_and(right, out);
    } else {
        out.push(expr);
    }
}

/// Grace-hash join for any kind: partition both inputs by join-key hash into
/// temp extents (matching rows share a partition, since equal keys hash equally,
/// so per-partition matched/unmatched equals globally matched/unmatched), then
/// join each partition pair in memory — the build hash table is bounded to one
/// partition. The probe input streams straight into its partitions; each
/// partition then materializes only its build rows and streams its probe rows
/// back. NULL-keyed rows never match: the outer side's are null-extended
/// directly, the inner side's are dropped. Output order is by partition
/// (immaterial — a spilling join is not order-sensitive).
#[allow(clippy::too_many_arguments)]
fn grace_hash_join(
    storage: &Storage,
    mut probe_rows: SourceRows,
    build: &MaterializedSource,
    build_left: bool,
    preserve_probe: bool,
    preserve_build: bool,
    probe_key: &impl Fn(&[Datum]) -> Vec<SqlValue>,
    build_key: &impl Fn(&[Datum]) -> Vec<SqlValue>,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
    emit_match: &impl Fn(&[Datum], &[Datum]) -> Vec<Datum>,
    emit_probe_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    emit_build_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    rows: &mut Vec<Vec<Datum>>,
) -> Result<(), SqlError> {
    use hash::{HashKey, key_has_null};
    use std::collections::HashMap;

    let build_bytes: usize = build.rows.iter().map(|r| approx_row_bytes(r)).sum();
    let partitions = (build_bytes / sort_budget() + 1).max(2);
    let partition_of = |key: &[SqlValue]| -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        HashKey(key.to_vec()).hash(&mut hasher);
        (hasher.finish() % partitions as u64) as usize
    };
    let spill_err = |e| map_storage_err(e, "<join spill>");

    let mut probe_parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    let mut build_parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    // Partition non-null-key rows; null-key rows can't match, so emit the outer
    // side's now and drop the rest. The probe side streams slice-by-slice.
    while let Some(slice) = probe_rows.next_slice(storage)? {
        for p in &slice {
            check_cancelled()?;
            let key = probe_key(p);
            if key_has_null(&key) {
                if preserve_probe {
                    rows.push(emit_probe_only(p));
                }
                continue;
            }
            probe_parts[partition_of(&key)]
                .write_row(p)
                .map_err(spill_err)?;
        }
    }
    for b in &build.rows {
        check_cancelled()?;
        let key = build_key(b);
        if key_has_null(&key) {
            if preserve_build {
                rows.push(emit_build_only(b));
            }
            continue;
        }
        build_parts[partition_of(&key)]
            .write_row(b)
            .map_err(spill_err)?;
    }
    for part in probe_parts.iter_mut().chain(build_parts.iter_mut()) {
        part.finish_writing().map_err(spill_err)?;
    }

    for part in 0..partitions {
        let mut b_rows: Vec<Vec<Datum>> =
            Vec::with_capacity(build_parts[part].row_count() as usize);
        let mut b_reader = build_parts[part].reader();
        while let Some(row) = b_reader.next_row().map_err(spill_err)? {
            b_rows.push(row);
        }
        let mut table: HashMap<HashKey, Vec<usize>> = HashMap::new();
        for (bi, b) in b_rows.iter().enumerate() {
            table.entry(HashKey(build_key(b))).or_default().push(bi);
        }
        let mut build_matched = vec![false; b_rows.len()];
        let mut p_reader = probe_parts[part].reader();
        while let Some(p) = p_reader.next_row().map_err(spill_err)? {
            let mut matched = false;
            if let Some(cands) = table.get(&HashKey(probe_key(&p))) {
                for &bi in cands {
                    let b = &b_rows[bi];
                    if matches_oriented(&p, b, build_left, matches)? {
                        rows.push(emit_match(&p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
            }
            if preserve_probe && !matched {
                rows.push(emit_probe_only(&p));
            }
        }
        if preserve_build {
            for (bi, b) in b_rows.iter().enumerate() {
                if !build_matched[bi] {
                    rows.push(emit_build_only(b));
                }
            }
        }
    }
    Ok(())
}

/// Evaluates the ON predicate for a probe/build pair in the caller's left/right
/// orientation (`matches` always takes `(left, right)`).
fn matches_oriented(
    probe: &[Datum],
    build: &[Datum],
    build_left: bool,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
) -> Result<bool, SqlError> {
    if build_left {
        matches(build, probe)
    } else {
        matches(probe, build)
    }
}

/// Hash join on the given equi-key columns. The build side is hashed by its
/// key tuple; the probe side streams and drives output, so row order matches
/// the nested loop exactly (unmatched build rows for FULL are null-extended at
/// the end, as the loop does). NULL key components never match (`x = NULL` is
/// UNKNOWN), so NULL-keyed rows are excluded from the table and treated as
/// unmatched. The full ON predicate is re-evaluated on every candidate, so
/// residual (non-equi) conjuncts and the 3VL of the equality are honored
/// identically to the nested loop. A build side past the memory budget spills
/// via [`grace_hash_join`].
#[allow(clippy::too_many_arguments)]
fn hash_join(
    storage: &Storage,
    probe: Source,
    build: &MaterializedSource,
    build_left: bool,
    equi: &[EquiKey],
    preserve_probe: bool,
    preserve_build: bool,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
    emit_match: &impl Fn(&[Datum], &[Datum]) -> Vec<Datum>,
    emit_probe_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    emit_build_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    rows: &mut Vec<Vec<Datum>>,
) -> Result<(), SqlError> {
    use hash::{HashKey, key_has_null};
    use std::collections::HashMap;

    let Source {
        columns: probe_columns,
        collations: probe_collations,
        rows: mut probe_rows,
        ..
    } = probe;

    // The case sensitivity governing each equi-key pair — the combined collation
    // of its two columns, combined in left/right order (`combine` favors its
    // first operand when both sides are exact, and `matches` combines in that
    // same order). The hash key is only a *pre-filter*: the full ON predicate
    // (collation-aware `matches`) re-checks each candidate, so the buckets must
    // be a superset of true matches. Folding both sides' key strings by this
    // sensitivity ensures case-insensitive-equal keys share a bucket (an
    // unfolded, case-sensitive hash would put `'abc'` and `'ABC'` in different
    // buckets, and the CI `matches` would never be consulted → a lost match).
    let (left_collations, right_collations) = if build_left {
        (&build.collations, &probe_collations)
    } else {
        (&probe_collations, &build.collations)
    };
    let key_sens: Vec<CollationSensitivity> = equi
        .iter()
        .map(|&(i, j)| {
            CollationSensitivity::from_optional(left_collations.get(i).and_then(|c| c.as_deref()))
                .combine(CollationSensitivity::from_optional(
                    right_collations.get(j).and_then(|c| c.as_deref()),
                ))
        })
        .collect();
    // Each equi pair reoriented as (probe column, build column): the pairs are
    // (left, right), and the build side is left exactly for a RIGHT join.
    let key_cols: Vec<(usize, usize)> = equi
        .iter()
        .map(|&(i, j)| if build_left { (j, i) } else { (i, j) })
        .collect();
    let probe_key = |p: &[Datum]| -> Vec<SqlValue> {
        key_cols
            .iter()
            .zip(&key_sens)
            .map(|(&(pc, _), &sens)| {
                sens.fold_value(value::datum_to_sql(&p[pc], &probe_columns[pc].column_type))
            })
            .collect()
    };
    let build_key = |b: &[Datum]| -> Vec<SqlValue> {
        key_cols
            .iter()
            .zip(&key_sens)
            .map(|(&(_, bc), &sens)| {
                sens.fold_value(value::datum_to_sql(&b[bc], &build.columns[bc].column_type))
            })
            .collect()
    };

    // Grace-hash spill for a large build side (any kind): partition both sides
    // by join-key hash so each partition's build table fits the memory budget.
    let build_bytes: usize = build.rows.iter().map(|r| approx_row_bytes(r)).sum();
    if build_bytes > sort_budget() {
        return grace_hash_join(
            storage,
            probe_rows,
            build,
            build_left,
            preserve_probe,
            preserve_build,
            &probe_key,
            &build_key,
            matches,
            emit_match,
            emit_probe_only,
            emit_build_only,
            rows,
        );
    }

    let mut table: HashMap<HashKey, Vec<usize>> = HashMap::new();
    for (index, row) in build.rows.iter().enumerate() {
        check_cancelled()?;
        let key = build_key(row);
        if key_has_null(&key) {
            continue;
        }
        table.entry(HashKey(key)).or_default().push(index);
    }

    let mut build_matched = vec![false; build.rows.len()];
    while let Some(slice) = probe_rows.next_slice(storage)? {
        for p in &slice {
            check_cancelled()?;
            let key = probe_key(p);
            let mut matched = false;
            if !key_has_null(&key)
                && let Some(cands) = table.get(&HashKey(key))
            {
                for &bi in cands {
                    let b = &build.rows[bi];
                    if matches_oriented(p, b, build_left, matches)? {
                        rows.push(emit_match(p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
            }
            if preserve_probe && !matched {
                rows.push(emit_probe_only(p));
            }
        }
    }
    if preserve_build {
        for (bi, b) in build.rows.iter().enumerate() {
            if !build_matched[bi] {
                rows.push(emit_build_only(b));
            }
        }
    }
    Ok(())
}

// ---- sys.* virtual sources ---------------------------------------------

fn nvarchar(name: &str, max_len: u16) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::NVarChar { max_len },
    }
}

fn int_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::Int,
    }
}

fn sys_tables(storage: &Storage) -> Source {
    let columns = vec![int_col("object_id"), nvarchar("name", 128)];
    let rows = storage
        .rel_tables()
        .into_iter()
        // Only base tables. (The `!is_view()` filter alone let procedures leak
        // in — a pre-existing gap — so exclude every non-table object kind.)
        .filter(|def| {
            !def.is_view() && !def.is_procedure() && !def.is_function() && !def.is_trigger()
        })
        .map(|def| vec![Datum::Int(def.object_id as i32), Datum::NVarChar(def.name)])
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

/// `sys.views` — one row per view, with its stored definition text.
/// `sys.databases` (Stage 14, SSMS query-window probes): the one database
/// this instance serves, with the columns tools actually read. The
/// versioning flags report the live `ALTER DATABASE` options.
fn sys_databases(storage: &Storage, eval_ctx: &EvalContext) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("database_id"),
        int_col("compatibility_level"),
        nvarchar("collation_name", 128),
        nvarchar("user_access_desc", 60),
        nvarchar("state_desc", 60),
        nvarchar("recovery_model_desc", 60),
        int_col("snapshot_isolation_state"),
        ResultColumn {
            name: "is_read_committed_snapshot_on".into(),
            column_type: ColumnType::Bit,
        },
        ResultColumn {
            name: "is_read_only".into(),
            column_type: ColumnType::Bit,
        },
    ];
    let rows = vec![vec![
        Datum::NVarChar(eval_ctx.database.clone()),
        Datum::Int(1),
        Datum::Int(160),
        Datum::NVarChar("SQL_Latin1_General_CP1_CI_AS".into()),
        Datum::NVarChar("MULTI_USER".into()),
        Datum::NVarChar("ONLINE".into()),
        Datum::NVarChar(
            if storage.recovery_model_full() {
                "FULL"
            } else {
                "SIMPLE"
            }
            .into(),
        ),
        Datum::Int(storage.snapshot_isolation_allowed() as i32),
        Datum::Bit(storage.rcsi_enabled()),
        Datum::Bit(false),
    ]];
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

/// `sys.configurations` (Stage 14): the handful of static rows connection
/// tools probe. Values are INT here (SQL Server uses sql_variant) — a
/// documented simplification.
fn sys_configurations() -> Source {
    let columns = vec![
        int_col("configuration_id"),
        nvarchar("name", 35),
        int_col("value"),
        int_col("minimum"),
        int_col("maximum"),
        int_col("value_in_use"),
        nvarchar("description", 255),
        ResultColumn {
            name: "is_dynamic".into(),
            column_type: ColumnType::Bit,
        },
        ResultColumn {
            name: "is_advanced".into(),
            column_type: ColumnType::Bit,
        },
    ];
    let entry =
        |id: i32, name: &str, value: i32, min: i32, max: i32, dynamic: bool, advanced: bool| {
            vec![
                Datum::Int(id),
                Datum::NVarChar(name.into()),
                Datum::Int(value),
                Datum::Int(min),
                Datum::Int(max),
                Datum::Int(value),
                Datum::NVarChar(name.into()),
                Datum::Bit(dynamic),
                Datum::Bit(advanced),
            ]
        };
    let rows = vec![
        entry(16384, "show advanced options", 0, 0, 1, true, false),
        entry(1539, "user options", 0, 0, 32767, true, false),
        entry(
            1544,
            "max server memory (MB)",
            i32::MAX,
            16,
            i32::MAX,
            true,
            true,
        ),
    ];
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_views(storage: &Storage) -> Source {
    let columns = vec![
        int_col("object_id"),
        nvarchar("name", 128),
        nvarchar("definition", 4000),
    ];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter_map(|def| {
            def.view_query.map(|q| {
                vec![
                    Datum::Int(def.object_id as i32),
                    Datum::NVarChar(def.name),
                    Datum::NVarChar(q),
                ]
            })
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

/// `sys.sql_modules`: the SQL definition of each module (currently views), keyed
/// by `object_id`. SQL Server surfaces view/procedure/trigger text here; today
/// only views carry a definition.
fn sys_sql_modules(storage: &Storage) -> Source {
    let columns = vec![int_col("object_id"), nvarchar("definition", 4000)];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter_map(|def| {
            // Views store their SELECT; procedures and functions their body.
            let definition = def
                .view_query
                .clone()
                .or_else(|| def.procedure.as_ref().map(|p| p.body.clone()))
                .or_else(|| {
                    def.function.as_ref().map(|f| match &f.returns {
                        FunctionReturns::Scalar { body, .. } => body.clone(),
                        FunctionReturns::InlineTable { select_text } => select_text.clone(),
                        FunctionReturns::MultiStatementTable { body, .. } => body.clone(),
                    })
                })
                .or_else(|| def.trigger.as_ref().map(|t| t.body.clone()))?;
            Some(vec![
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(definition),
            ])
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_procedures(storage: &Storage) -> Source {
    let columns = vec![nvarchar("name", 128), int_col("object_id")];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.is_procedure())
        .map(|def| {
            vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
            ]
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_triggers(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("object_id"),
        int_col("parent_id"),
        nvarchar("type", 2),
        int_col("is_disabled"),
        int_col("is_instead_of_trigger"),
    ];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter_map(|def| {
            let trigger = def.trigger.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::Int(trigger.parent_object_id as i32),
                Datum::NVarChar("TR".to_string()),
                Datum::Int(i32::from(trigger.is_disabled)),
                // Only AFTER triggers exist here (INSTEAD OF is not supported).
                Datum::Int(0),
            ])
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_trigger_events(storage: &Storage) -> Source {
    let columns = vec![
        int_col("object_id"),
        int_col("type"),
        nvarchar("type_desc", 128),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        let Some(trigger) = def.trigger.as_ref() else {
            continue;
        };
        for event in &trigger.events {
            let (code, desc) = match event {
                catalog::TriggerEvent::Insert => (1, "INSERT"),
                catalog::TriggerEvent::Update => (2, "UPDATE"),
                catalog::TriggerEvent::Delete => (3, "DELETE"),
            };
            rows.push(vec![
                Datum::Int(def.object_id as i32),
                Datum::Int(code),
                Datum::NVarChar(desc.to_string()),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_server_principals(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        nvarchar("type", 1),
        nvarchar("type_desc", 60),
        int_col("is_disabled"),
    ];
    let mut rows: Vec<Vec<Datum>> = storage
        .rel_logins()
        .into_iter()
        .filter_map(|def| {
            let principal = def.principal.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                // SQL logins: type 'S' / SQL_LOGIN.
                Datum::NVarChar("S".to_string()),
                Datum::NVarChar("SQL_LOGIN".to_string()),
                Datum::Int(i32::from(principal.is_disabled)),
            ])
        })
        .collect();
    // The fixed server roles (today: sysadmin) — type 'R' / SERVER_ROLE.
    for fixed in crate::storage::FIXED_PRINCIPALS
        .iter()
        .filter(|p| p.is_server)
    {
        rows.push(vec![
            Datum::NVarChar(fixed.name.to_string()),
            Datum::Int(fixed.id as i32),
            Datum::NVarChar("R".to_string()),
            Datum::NVarChar("SERVER_ROLE".to_string()),
            Datum::Int(0),
        ]);
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_sql_logins(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        int_col("is_disabled"),
    ];
    let rows = storage
        .rel_logins()
        .into_iter()
        .filter_map(|def| {
            let principal = def.principal.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::Int(i32::from(principal.is_disabled)),
            ])
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_database_principals(storage: &Storage) -> Source {
    use crate::relstore::catalog::PrincipalKind;
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        nvarchar("type", 1),
        nvarchar("type_desc", 60),
        nvarchar("default_schema_name", 128),
        int_col("owning_principal_id"),
    ];
    // A user (SQL_USER 'S') defaults to the dbo schema; a role (DATABASE_ROLE
    // 'R') has no default schema.
    let row = |name: String, id: u32, kind: PrincipalKind| {
        let (type_code, type_desc) = match kind {
            PrincipalKind::Role => ("R", "DATABASE_ROLE"),
            _ => ("S", "SQL_USER"),
        };
        let default_schema = if matches!(kind, PrincipalKind::User) {
            Datum::NVarChar("dbo".to_string())
        } else {
            Datum::Null
        };
        vec![
            Datum::NVarChar(name),
            Datum::Int(id as i32),
            Datum::NVarChar(type_code.to_string()),
            Datum::NVarChar(type_desc.to_string()),
            default_schema,
            Datum::Null, // owning_principal_id
        ]
    };
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // Fixed database principals (dbo + the fixed database roles + public); the
    // server-scoped sysadmin role belongs to sys.server_principals instead.
    for fixed in crate::storage::FIXED_PRINCIPALS
        .iter()
        .filter(|p| !p.is_server)
    {
        rows.push(row(fixed.name.to_string(), fixed.id, fixed.kind));
    }
    for def in storage.rel_database_principals() {
        if let Some(principal) = def.principal.as_ref() {
            rows.push(row(def.name.clone(), def.object_id, principal.kind));
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_database_role_members(storage: &Storage) -> Source {
    let columns = vec![int_col("role_principal_id"), int_col("member_principal_id")];
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // Synthesized: the dbo user is a member of db_owner.
    rows.push(vec![
        Datum::Int(crate::storage::DB_OWNER_ID as i32),
        Datum::Int(crate::storage::DBO_ID as i32),
    ]);
    // Stored database membership edges (member -> role, from each member's row).
    for def in storage.rel_database_principals() {
        if let Some(principal) = def.principal.as_ref() {
            for &role_id in &principal.member_of {
                rows.push(vec![
                    Datum::Int(role_id as i32),
                    Datum::Int(def.object_id as i32),
                ]);
            }
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_database_permissions(storage: &Storage) -> Source {
    let columns = vec![
        int_col("class"),
        nvarchar("class_desc", 60),
        int_col("major_id"),
        int_col("minor_id"),
        int_col("grantee_principal_id"),
        nvarchar("permission_name", 128),
        nvarchar("state", 1),
        nvarchar("state_desc", 60),
    ];
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    for def in storage.rel_tables() {
        for perm in &def.permissions {
            let (state, state_desc) = if perm.deny {
                ("D", "DENY")
            } else {
                ("G", "GRANT")
            };
            rows.push(vec![
                Datum::Int(1), // class 1 = OBJECT_OR_COLUMN
                Datum::NVarChar("OBJECT_OR_COLUMN".to_string()),
                Datum::Int(def.object_id as i32),
                Datum::Int(0), // minor_id 0 = the whole object (no column-level)
                Datum::Int(perm.grantee as i32),
                Datum::NVarChar(perm.action.name().to_string()),
                Datum::NVarChar(state.to_string()),
                Datum::NVarChar(state_desc.to_string()),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_parameters(storage: &Storage) -> Source {
    let columns = vec![
        int_col("object_id"),
        nvarchar("name", 128),
        int_col("parameter_id"),
        nvarchar("system_type_name", 128),
        int_col("is_output"),
        int_col("has_default_value"),
    ];
    let mut rows = Vec::new();
    let mut push_param = |object_id: u32,
                          name: String,
                          id: i32,
                          type_spec: String,
                          output: bool,
                          has_default: bool| {
        rows.push(vec![
            Datum::Int(object_id as i32),
            Datum::NVarChar(name),
            Datum::Int(id),
            Datum::NVarChar(type_spec),
            Datum::Int(i32::from(output)),
            Datum::Int(i32::from(has_default)),
        ]);
    };
    for def in storage.rel_tables() {
        if let Some(procedure) = &def.procedure {
            for (index, param) in procedure.params.iter().enumerate() {
                push_param(
                    def.object_id,
                    format!("@{}", param.name),
                    index as i32 + 1,
                    param.type_spec.clone(),
                    param.output,
                    param.default.is_some(),
                );
            }
        } else if let Some(function) = &def.function {
            // A SCALAR function's return value is parameter_id 0 (empty name,
            // is_output set — SQL Server's convention). A table-valued function
            // returns a table, so it has no scalar return parameter.
            if let FunctionReturns::Scalar { type_spec, .. } = &function.returns {
                push_param(
                    def.object_id,
                    String::new(),
                    0,
                    type_spec.clone(),
                    true,
                    false,
                );
            }
            for (index, param) in function.params.iter().enumerate() {
                push_param(
                    def.object_id,
                    format!("@{}", param.name),
                    index as i32 + 1,
                    param.type_spec.clone(),
                    false,
                    param.default.is_some(),
                );
            }
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_objects(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("object_id"),
        nvarchar("type", 2),
        nvarchar("type_desc", 60),
    ];
    let rows = storage
        .rel_tables()
        .into_iter()
        .map(|def| {
            // SQL Server's single-letter codes carry a trailing space; the
            // two-letter function codes fill CHAR(2) exactly.
            let (code, desc) = if let Some(function) = &def.function {
                match function.returns {
                    FunctionReturns::Scalar { .. } => ("FN", "SQL_SCALAR_FUNCTION"),
                    FunctionReturns::InlineTable { .. } => {
                        ("IF", "SQL_INLINE_TABLE_VALUED_FUNCTION")
                    }
                    FunctionReturns::MultiStatementTable { .. } => {
                        ("TF", "SQL_TABLE_VALUED_FUNCTION")
                    }
                }
            } else if def.is_procedure() {
                ("P ", "SQL_STORED_PROCEDURE")
            } else if def.is_trigger() {
                ("TR", "SQL_TRIGGER")
            } else if def.is_view() {
                ("V ", "VIEW")
            } else {
                ("U ", "USER_TABLE")
            };
            vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(code.to_string()),
                Datum::NVarChar(desc.to_string()),
            ]
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_columns(storage: &Storage) -> Source {
    let columns = vec![
        int_col("object_id"),
        nvarchar("name", 128),
        int_col("column_id"),
        nvarchar("type", 128),
        ResultColumn {
            name: "is_nullable".to_string(),
            column_type: ColumnType::Bit,
        },
        nvarchar("collation_name", 128),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        for (index, (name, type_spec, nullable)) in def.columns.iter().enumerate() {
            let collation = def
                .collations
                .get(index)
                .cloned()
                .flatten()
                .map(Datum::NVarChar)
                .unwrap_or(Datum::Null);
            rows.push(vec![
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(name.clone()),
                Datum::Int(index as i32 + 1),
                Datum::NVarChar(type_spec.clone()),
                Datum::Bit(*nullable),
                collation,
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_indexes(storage: &Storage) -> Source {
    let columns = vec![
        int_col("object_id"),
        int_col("index_id"),
        nvarchar("name", 128),
        ResultColumn {
            name: "is_unique".to_string(),
            column_type: ColumnType::Bit,
        },
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        for index in &def.indexes {
            rows.push(vec![
                Datum::Int(def.object_id as i32),
                Datum::Int(index.object_id as i32),
                Datum::NVarChar(index.name.clone()),
                Datum::Bit(index.unique),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_check_constraints(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("parent_object_id"),
        nvarchar("definition", 4000),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        for check in &def.check_constraints {
            rows.push(vec![
                Datum::NVarChar(check.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(format!("({})", check.predicate)),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_foreign_keys(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("parent_object_id"),
        int_col("referenced_object_id"),
    ];
    // Resolve parent (referenced) table names to object ids.
    let tables = storage.rel_tables();
    let oid_of = |name: &str| {
        tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .map(|t| t.object_id)
    };
    let mut rows = Vec::new();
    for def in &tables {
        for fk in &def.foreign_keys {
            rows.push(vec![
                Datum::NVarChar(fk.name.clone()),
                Datum::Int(def.object_id as i32),
                oid_of(&fk.parent)
                    .map(|o| Datum::Int(o as i32))
                    .unwrap_or(Datum::Null),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

fn sys_default_constraints(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("parent_object_id"),
        int_col("parent_column_id"),
        nvarchar("definition", 4000),
    ];
    // Inline column DEFAULTs are unnamed; SQL Server auto-names them
    // `DF__<table>__<column>__...`. We synthesize a stable `DF__<table>__<col>`.
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        for (index, text) in def.defaults.iter().enumerate() {
            let Some(text) = text else { continue };
            let column = &def.columns[index].0;
            rows.push(vec![
                Datum::NVarChar(format!("DF__{}__{}", def.name, column)),
                Datum::Int(def.object_id as i32),
                Datum::Int(index as i32 + 1),
                Datum::NVarChar(format!("({text})")),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

// ---- helpers ------------------------------------------------------------

/// Evaluates a constant expression (INSERT VALUES): no columns in scope.
fn eval_constant(expr: &Expr, eval_ctx: &EvalContext) -> Result<SqlValue, SqlError> {
    let empty: Vec<String> = Vec::new();
    eval::eval(expr, &[], &empty, eval_ctx)
}

fn column_index(schema: &Schema, name: &str) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

/// Strips an optional `dbo.` schema prefix (Stage 3 has a single user
/// schema); `sys.` names are handled separately as catalog views.
fn strip_schema(name: &str) -> &str {
    name.split_once('.')
        .filter(|(schema, _)| schema.eq_ignore_ascii_case("dbo"))
        .map(|(_, rest)| rest)
        .unwrap_or(name)
}

/// Case-insensitive table resolution (single `dbo` schema in Stage 3). An
/// optional `dbo.` schema prefix is accepted and stripped.
/// The base-table object ids that a read of `name` must Shared-lock: the table
/// itself, or — for a view — the base tables its definition reads. `sys.*`
/// views take no lock. Nested views (a view over a view) are not expanded here
/// (they error at query time), so they contribute no locks; view expansion is
/// one level deep, matching the executor.
fn read_lock_object_ids(storage: &Storage, name: &str) -> Vec<u32> {
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    collect_read_lock_ids(storage, name, 0, &mut out, &mut visited);
    out
}

/// Adds the locks the AFTER-`event` trigger bodies of `parent_object_id` take,
/// so a DML that fires them holds every lock its bodies acquire UP FRONT (strict
/// 2PL — a trigger body reading/writing another table with no pre-acquired lock
/// is the recurring seam-defect class). Each body is analyzed by the SAME
/// machinery the batch uses (`analyze_statements_locks`), so its EXEC, TRY/CATCH,
/// FK-integrity reads, subquery reads, and its own nested triggers are ALL
/// covered — not a hand-rolled subset. Recursion (a trigger whose body DMLs a
/// table with its own triggers) is bounded by `trigger_visited` (trigger
/// object_ids), so a trigger cycle terminates cleanly rather than hanging
/// analysis under the scheduler mutex.
fn add_trigger_locks(
    storage: &Storage,
    parent_object_id: u32,
    event: catalog::TriggerEvent,
    isolation: Isolation,
    visited: &mut std::collections::HashSet<(String, Isolation)>,
    trigger_visited: &mut std::collections::HashSet<(u32, Isolation)>,
    add: &mut impl FnMut(Resource, LockMode),
) {
    for trig in storage.rel_triggers_for(parent_object_id, event) {
        if !trigger_visited.insert((trig.object_id, isolation)) {
            continue;
        }
        let Some(t) = &trig.trigger else { continue };
        if let Ok(statements) = truthdb_sql::parse_procedure_body(&t.body) {
            for (resource, mode) in
                analyze_statements_locks(storage, &statements, isolation, visited, trigger_visited)
            {
                add(resource, mode);
            }
        }
    }
}

/// The base-table object ids the scalar functions called in a SELECT read
/// through their bodies (deduped). Non-function names collected along the way
/// resolve to nothing.
fn select_function_read_ids(storage: &Storage, select: &Select) -> Vec<u32> {
    let mut tables = Vec::new();
    let mut funcs = Vec::new();
    collect_select_read_names(select, &mut tables, &mut funcs);
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    for func in &funcs {
        collect_read_lock_ids(storage, func, 0, &mut out, &mut visited);
    }
    out
}

/// Like [`select_function_read_ids`] but for a bare expression (an IF/WHILE
/// condition).
fn expr_function_read_ids(storage: &Storage, expr: &Expr) -> Vec<u32> {
    let mut tables = Vec::new();
    let mut funcs = Vec::new();
    collect_expr_read_names(expr, &mut tables, &mut funcs);
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    for func in &funcs {
        collect_read_lock_ids(storage, func, 0, &mut out, &mut visited);
    }
    out
}

/// Resolves `name` to the base-table object ids the executor will read,
/// recursing through nested views (so a view over a view locks the inner view's
/// base tables). Bounded by [`MAX_VIEW_NESTING`] so a view cycle terminates.
fn collect_read_lock_ids(
    storage: &Storage,
    name: &str,
    depth: u32,
    out: &mut Vec<u32>,
    visited: &mut std::collections::HashSet<u32>,
) {
    if depth > MAX_VIEW_NESTING || name.to_ascii_lowercase().starts_with("sys.") {
        return;
    }
    let Some(def) = resolve_table(storage, name) else {
        return;
    };
    // Expand each function/view body at most once per analysis. The depth guard
    // bounds recursion depth but NOT fan-out: a self- or mutually-referential
    // body that references itself twice would otherwise recurse exponentially
    // (2^depth), hanging analysis — and, because analyze_locks runs under the
    // scheduler mutex, freezing every session.
    if (def.is_function() || def.view_query.is_some()) && !visited.insert(def.object_id) {
        return;
    }
    // A function: its inner reads (subqueries in a scalar body, or an inline
    // TVF's body SELECT, plus nested function calls) must be locked up front, or
    // the body would read tables with no lock held under 2PL — the seam-defect
    // class. Recurse into the body's read targets, bounded by the same guard.
    if let Some(function) = &def.function {
        let mut tables = Vec::new();
        let mut funcs = Vec::new();
        match &function.returns {
            FunctionReturns::Scalar { body, .. } => {
                if let Ok(statements) = truthdb_sql::parse_function_body(body) {
                    for statement in &statements {
                        collect_statement_read_names(statement, &mut tables, &mut funcs);
                    }
                }
            }
            FunctionReturns::InlineTable { select_text } => {
                if let Ok(body) = parse_view_query(select_text, &def.name) {
                    let expanded = expand_ctes(&body);
                    collect_select_read_names(&expanded, &mut tables, &mut funcs);
                }
            }
            // A multi-statement TVF body may read real tables (e.g. INSERT @t
            // SELECT FROM base): those reads must be locked up front, exactly
            // like a scalar body. (@-targets are session-local and are skipped
            // by the read-name collectors, so they add no lock.)
            FunctionReturns::MultiStatementTable { body, .. } => {
                if let Ok(statements) = truthdb_sql::parse_table_function_body(body) {
                    for statement in &statements {
                        collect_statement_read_names(statement, &mut tables, &mut funcs);
                    }
                }
            }
        }
        for referenced in tables.iter().chain(funcs.iter()) {
            collect_read_lock_ids(storage, referenced, depth + 1, out, visited);
        }
        return;
    }
    let Some(text) = &def.view_query else {
        // A base table.
        if !out.contains(&def.object_id) {
            out.push(def.object_id);
        }
        return;
    };
    // A view: recurse into every table its body references — and every scalar
    // function it calls, whose body may read further tables (else a UDF reached
    // through a view would read unlocked). Inline the body's own CTEs so a base
    // table reached only through a CTE is still locked.
    let Ok(body) = parse_view_query(text, &def.name) else {
        return;
    };
    let expanded = expand_ctes(&body);
    let mut tables = Vec::new();
    let mut funcs = Vec::new();
    collect_select_read_names(&expanded, &mut tables, &mut funcs);
    for referenced in tables.iter().chain(funcs.iter()) {
        collect_read_lock_ids(storage, referenced, depth + 1, out, visited);
    }
}

/// Views are read-only here; INSERT/UPDATE/DELETE against one is rejected —
/// and a PROCEDURE is not a data object at all (SQL Server 2809).
fn reject_dml_on_view(def: &TableDef) -> Result<(), SqlError> {
    if def.is_procedure() {
        return Err(procedure_not_a_table(&def.name));
    }
    if def.is_function() {
        return Err(function_not_a_table(&def.name));
    }
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&def.name));
    }
    if def.is_view() {
        return Err(SqlError::new(
            4406,
            16,
            1,
            format!(
                "Update or insert of view '{}' is not supported (the view is read-only).",
                def.name
            ),
        ));
    }
    Ok(())
}

/// SQL Server 2809: a procedure referenced where a table/view is required.
fn procedure_not_a_table(name: &str) -> SqlError {
    SqlError::new(
        2809,
        16,
        1,
        format!(
            "The request for procedure '{name}' failed because '{name}' is a procedure object."
        ),
    )
}

/// A scalar function used where a table is required (`FROM`, DML target,
/// table-only DDL). A scalar function is not a rowset; SQL Server 4121-class.
fn function_not_a_table(name: &str) -> SqlError {
    SqlError::new(
        4121,
        16,
        1,
        format!(
            "Cannot find the user-defined function '{name}', or the name refers to a scalar \
             function that cannot be used where a table is expected."
        ),
    )
}

/// Table-only DDL (ALTER TABLE, CREATE INDEX) rejects a view. Without this a
/// view's `root_page = 0` would be heap-scanned — and page 0 is the catalog
/// root, so a bare `ALTER TABLE view ADD CHECK (1=1)` could corrupt the catalog.
fn reject_view_as_table(def: &TableDef) -> Result<(), SqlError> {
    if def.is_procedure() {
        return Err(procedure_not_a_table(&def.name));
    }
    if def.is_function() {
        return Err(function_not_a_table(&def.name));
    }
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&def.name));
    }
    if def.is_view() {
        return Err(SqlError::new(
            4928,
            16,
            1,
            format!(
                "Cannot perform this operation on '{}' because it is a view, not a table.",
                def.name
            ),
        ));
    }
    Ok(())
}

fn resolve_table(storage: &Storage, name: &str) -> Option<TableDef> {
    let bare = strip_schema(name);
    if let Some(def) = storage.rel_table(bare) {
        return Some(def);
    }
    storage
        .rel_tables()
        .into_iter()
        .find(|d| d.name.eq_ignore_ascii_case(bare))
}

/// Maps a storage error to a SQL Server-numbered error. PK and NULL
/// violations are recognized by their storage messages.
fn map_storage_err(err: StorageError, table: &str) -> SqlError {
    match err {
        StorageError::Constraint(msg) if msg.contains("duplicate primary key") => {
            SqlError::pk_violation(table)
        }
        StorageError::Constraint(msg) if msg.contains("duplicate unique index") => {
            // 2601: cannot insert a duplicate key row in a unique index.
            SqlError::new(2601, 14, 1, msg)
        }
        StorageError::Constraint(msg) if msg.contains("already exists") => {
            // 1913: an index with that name already exists on the table.
            SqlError::new(1913, 16, 1, msg)
        }
        StorageError::Constraint(msg) if msg.contains("does not allow NULL") => {
            SqlError::new(515, 16, 2, msg)
        }
        StorageError::Constraint(msg) => SqlError::new(547, 16, 0, msg),
        StorageError::SnapshotSchemaChange(name) => SqlError::new(
            3961,
            16,
            1,
            format!(
                "Snapshot isolation transaction failed in database because the object accessed \
                 by the statement has been modified by a DDL statement in another concurrent \
                 transaction since the start of this transaction. It is disallowed because the \
                 metadata is not versioned. Object: '{name}'."
            ),
        ),
        StorageError::InvalidConfig(msg) => SqlError::new(1701, 16, 1, msg),
        // The WAL ring is full — under FULL recovery this is typically because
        // un-backed-up log pins truncation (run BACKUP LOG); under SIMPLE it is
        // an oversized active transaction. SQL Server reports 9002 either way.
        StorageError::WalFull(msg) => SqlError::new(
            9002,
            17,
            2,
            format!("The transaction log for the database is full. {msg}"),
        ),
        other => SqlError::new(
            3621,
            16,
            1,
            format!("The statement has been terminated. {other}"),
        ),
    }
}

pub use ast::Statement as SqlStatement;

/// Renders a result cell to its display string (`None` = NULL). Shared by
/// the JSON envelope and any text renderer.
pub fn render_cell(datum: &Datum, column_type: &ColumnType) -> Option<String> {
    value::display(datum, column_type)
}
