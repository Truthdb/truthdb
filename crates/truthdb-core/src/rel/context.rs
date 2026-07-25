use super::prelude::*;

/// A declared table variable's in-memory contents: its column schema, the key
/// columns of its declared PRIMARY KEY (for uniqueness enforcement), the
/// per-column `DEFAULT` source text (parallel to the schema columns, re-parsed
/// and evaluated per INSERT), and its rows. A row is a `Vec<Datum>` in schema
/// order, exactly like a base-table row.
#[derive(Clone)]
pub(super) struct TableVar {
    pub(super) schema: Schema,
    pub(super) key_columns: Vec<usize>,
    pub(super) defaults: Vec<Option<String>>,
    pub(super) rows: Vec<Vec<Datum>>,
}

/// truthdb-sql cannot depend on this crate, so it mirrors the default
/// database id; the two constants must never drift.
const _: () = assert!(
    truthdb_sql::eval::DEFAULT_DATABASE_ID == crate::relstore::catalog::DEFAULT_DATABASE_ID
);

/// A session's current database id. A wrapper so `TxnContext::default()`
/// lands in the DEFAULT database (id 1), never a nonexistent id 0 — one
/// representation of "the default database", not two.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct CurrentDb(pub(super) u32);

impl Default for CurrentDb {
    fn default() -> Self {
        CurrentDb(crate::relstore::catalog::DEFAULT_DATABASE_ID)
    }
}

/// Per-session transaction state carried across statements/batches. Lives in
/// the session (engine thread); autocommit statements use `Default`.
#[derive(Default)]
pub struct TxnContext {
    pub(super) txn: Option<StorageTxn>,
    /// `@@TRANCOUNT` — nested BEGINs increment; only the outermost COMMIT
    /// actually commits.
    pub(super) trancount: u32,
    /// Set when a statement failed inside the transaction (SQL Server
    /// XACT_ABORT-style): only ROLLBACK is then allowed.
    pub(super) doomed: bool,
    pub(super) xact_abort: bool,
    pub(super) isolation: Isolation,
    /// `SET NOCOUNT ON` — statements report no row count to the client:
    /// TDS DONEs drop `DONE_COUNT`, and the native protocol's count envelope
    /// becomes a bare done (the CLI then prints no "(n rows affected)" line,
    /// exactly as sqlcmd goes quiet against SQL Server). Result rows and
    /// `@@ROWCOUNT` are untouched.
    pub(super) nocount: bool,
    /// Rows affected/returned by the previous statement — `@@ROWCOUNT`.
    pub(super) rowcount: i64,
    /// The previous statement's error number, 0 on success — `@@ERROR`.
    pub(super) last_error: i32,
    /// Names of the stored procedures currently executing (innermost last),
    /// for `ERROR_PROCEDURE()`; empty in an ad-hoc batch.
    pub(super) proc_stack: Vec<String>,
    /// The procedure executing when the LAST error was raised — captured at
    /// the raise site, because by CATCH entry the procedure's frame has
    /// already unwound off `proc_stack`.
    pub(super) error_procedure: Option<String>,
    /// A procedure body's `RETURN [value]` status, stashed by the Return arm
    /// for `EXEC @rc = name` to read after the body unwinds; 0 when the body
    /// falls off the end.
    pub(super) proc_return: Option<i64>,
    /// A scalar function body's `RETURN <expr>` value, coerced to the declared
    /// return type and stashed by the Return arm for the caller to read. Only
    /// set while a function body runs (see [`run_user_scalar_function`]).
    pub(super) func_return: Option<SqlValue>,
    /// `SET SHOWPLAN_TEXT ON` — a SELECT returns its plan text, not results.
    pub(super) showplan_text: bool,
    /// Declared batch variables (name without `@`, lowercased) to their type
    /// and current value. Cleared at the start of each batch.
    pub(super) variables: std::collections::HashMap<String, (ColumnType, SqlValue)>,
    /// Declared table variables (name without `@`, lowercased): in-memory
    /// rowsets that live only on the session (never on Storage), so they survive
    /// ROLLBACK and are cleared at batch end — SQL Server table-variable
    /// semantics. Kept disjoint from `variables`; a name lives in exactly one.
    pub(super) table_variables: std::collections::HashMap<String, TableVar>,
    /// Declared cursors (name lowercased). Batch-scoped (cleared at batch end).
    pub(super) cursors: std::collections::HashMap<String, CursorState>,
    /// `@@FETCH_STATUS`: the result of the last cursor FETCH (0 / -1 / -2).
    pub(super) fetch_status: i32,
    /// Connection identity for session intrinsics (`DB_NAME()`,
    /// `SUSER_SNAME()`, `@@SPID`), set once when the session opens.
    pub(super) database: String,
    /// Per-batch snapshot of every database `(id, canonical name)` — read by
    /// `DB_ID(name)`/`DB_NAME(id)`. Refreshed with the security context at
    /// batch start (a same-batch CREATE DATABASE is visible next batch, a
    /// documented staleness matching the per-batch security refresh).
    pub(super) databases_snapshot: Vec<(u32, String)>,
    /// The session's current database id — the namespace every unqualified
    /// object name resolves in. Sessions run in the default database until
    /// `USE` learns to switch it (the multi-database plan's A2 slice).
    pub(super) database_id: CurrentDb,
    pub(super) login: String,
    pub(super) spid: i32,
    /// The session's database user name (`USER_NAME()`), resolved from the login
    /// when the session opens (`dbo` for a sysadmin, else the mapped user, else
    /// the login name). Distinct from `login` (the server principal).
    pub(super) user: String,
    /// The login's server principal_id and the user's database principal_id —
    /// the keys the membership intrinsics resolve against. Set at session open.
    pub(super) login_sid: u32,
    pub(super) user_sid: u32,
    /// The session's effective SERVER-role names (lowercased) — the transitive
    /// closure of the login's roles (today: `sysadmin`). Read by
    /// `IS_SRVROLEMEMBER`. Kept separate from database roles so the two
    /// namespaces do not cross-answer.
    pub(super) session_server_roles: std::collections::HashSet<String>,
    /// The session's effective DATABASE-role names (lowercased) — the transitive
    /// closure of the database user's roles. Read by `IS_ROLEMEMBER`. Both sets
    /// are refreshed at batch start from the membership cache, so a security DDL
    /// is seen by the next batch (SQL Server's per-batch permission caching).
    pub(super) session_db_roles: std::collections::HashSet<String>,
    /// Object-permission enforcement subject (bypass flag + the grant-matching
    /// principal id set), refreshed at batch start alongside the role sets.
    pub(super) security: truthdb_sql::eval::SecurityContext,
    /// The last identity value inserted in this session (SQL Server scope),
    /// surfaced as `SCOPE_IDENTITY()`. Persists across statements until the next
    /// identity INSERT; unaffected by non-identity inserts.
    pub(super) scope_identity: Option<i64>,
    /// Named savepoints in the current transaction (`SAVE TRANSACTION <name>`,
    /// lowercased) → the point to which `ROLLBACK TRANSACTION <name>` returns.
    /// Cleared when the transaction ends.
    pub(super) savepoints: std::collections::HashMap<String, crate::relstore::ctx::Savepoint>,
    /// Errors caught by the currently-executing `CATCH` blocks (a stack, so
    /// nested `TRY`/`CATCH` restore the outer error on exit). `ERROR_*()` read
    /// the top; empty outside any `CATCH` block.
    pub(super) error_stack: Vec<truthdb_sql::eval::ErrorInfo>,
    /// The SNAPSHOT-isolation transaction's read view, captured (and
    /// registered against pruning) at its first data access and released
    /// when the transaction ends — commit, rollback, reap, or disconnect.
    pub(super) txn_snapshot: Option<ReadSnapshot>,
    /// Set when the idle reaper rolled this session's transaction back. The
    /// session's next batch fails with 1205 and clears it, so a client that
    /// comes back believing it is still in a transaction is told the
    /// transaction is gone — rather than silently autocommitting statements it
    /// means to be transactional, and only discovering it at a COMMIT that
    /// errors 3902 long after the writes became durable.
    pub(super) reaped: bool,
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
    pub(super) fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    pub(super) fn eval_context(&self) -> EvalContext {
        EvalContext {
            database_id: self.database_id.0,
            databases: self.databases_snapshot.clone(),
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
            updated_columns: current_trigger_updated_columns(),
            fetch_status: self.fetch_status,
        }
    }

    /// `XACT_STATE()`: 0 with no open transaction, -1 when the open transaction
    /// is doomed (uncommittable), else 1.
    pub(super) fn xact_state(&self) -> i8 {
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
    pub(super) fn record_error(&mut self, number: i32) {
        self.last_error = number;
        self.error_procedure = self.proc_stack.last().cloned();
    }

    pub(super) fn push_error(&mut self, error: &SqlError) {
        self.error_stack.push(truthdb_sql::eval::ErrorInfo {
            number: error.number,
            message: error.message.clone(),
            severity: error.level,
            state: error.state,
            procedure: self.error_procedure.clone(),
        });
    }

    /// Leaves a `CATCH` block, restoring the enclosing error context (if any).
    pub(super) fn pop_error(&mut self) {
        self.error_stack.pop();
    }

    /// Records the connection identity used by session intrinsics. Called once
    /// when the session opens. `session_roles` is filled separately, per batch,
    /// from the membership cache (see [`Self::refresh_session_roles`]).
    #[allow(clippy::too_many_arguments)]
    pub fn set_session_identity(
        &mut self,
        database: String,
        database_id: u32,
        login: String,
        spid: i32,
        user: String,
        login_sid: u32,
        user_sid: u32,
    ) {
        self.database = database;
        self.database_id = CurrentDb(database_id);
        self.login = login;
        self.spid = spid;
        self.user = user;
        self.login_sid = login_sid;
        self.user_sid = user_sid;
    }

    /// The session's current database id — the namespace unqualified names
    /// resolve in.
    pub(crate) fn database_id(&self) -> u32 {
        self.database_id.0
    }

    /// Switches the session's current database (`USE`): the canonical name
    /// and its id move together — they are one decision, never two.
    pub(crate) fn set_current_database(&mut self, name: String, db_id: u32) {
        self.database = name;
        self.database_id = CurrentDb(db_id);
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
        self.databases_snapshot = storage.rel_databases();

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
        self.cursors.clear();
        self.fetch_status = 0;
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
    pub(super) fn release_txn_snapshot(&mut self, storage: &Storage) {
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
    pub(super) fn take_reaped(&mut self) -> bool {
        std::mem::take(&mut self.reaped)
    }
}

/// One executed statement's outcome, from [`exec_statement_streamed`].
pub(super) enum StatementOutcome {
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
    pub(super) static CURRENT_SNAPSHOT: std::cell::Cell<Option<ReadSnapshot>> =
        const { std::cell::Cell::new(None) };
}

/// The running statement's read snapshot, if it reads versioned.
pub(super) fn current_snapshot() -> Option<ReadSnapshot> {
    CURRENT_SNAPSHOT.get()
}

thread_local! {
    /// The running statement's table variables (the session's, shared read-only
    /// for the statement). Thread-local for the same reason as CURRENT_SNAPSHOT:
    /// a batch runs on one worker thread, and the FROM-source builders carry
    /// only an EvalContext (a truthdb-sql type that cannot hold core `Datum`
    /// rows), so the store cannot ride through it.
    pub(super) static CURRENT_TABLE_VARS: std::cell::RefCell<
        Option<std::rc::Rc<std::collections::HashMap<String, TableVar>>>,
    > = const { std::cell::RefCell::new(None) };
}

/// The table variable `@name` visible to the running statement, cloned out for a
/// FROM read (an in-memory rowset).
pub(super) fn current_table_var(name: &str) -> Option<TableVar> {
    let key = name.trim_start_matches('@').to_ascii_lowercase();
    CURRENT_TABLE_VARS.with(|c| c.borrow().as_ref().and_then(|m| m.get(&key).cloned()))
}

/// Installs the statement's table variables for its execution, restoring the
/// prior installation on drop (scopes can nest — a subquery or TVF body reads
/// within the caller's — so restore rather than clear).
pub(super) struct TableVarScope {
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
pub(super) fn arm_table_var_view(
    vars: &std::collections::HashMap<String, TableVar>,
) -> Option<TableVarScope> {
    let outer_armed = CURRENT_TABLE_VARS.with(|c| c.borrow().is_some());
    (!vars.is_empty() || outer_armed).then(|| TableVarScope::enter(std::rc::Rc::new(vars.clone())))
}

/// The `inserted`/`deleted` pseudo-tables a firing trigger body reads: the new
/// and old row images of the statement that fired it, with the parent table's
/// schema. Rows are in schema order, exactly like a base-table row.
pub(super) struct TriggerTables {
    pub(super) schema: Schema,
    pub(super) inserted: Vec<Vec<Datum>>,
    pub(super) deleted: Vec<Vec<Datum>>,
    /// The 0-based indices of the columns the firing statement touched.
    pub(super) updated: Vec<usize>,
}

thread_local! {
    /// The `inserted`/`deleted` view visible to the running trigger body (like
    /// CURRENT_TABLE_VARS for table variables — a batch runs on one thread and
    /// the FROM-source builders carry only an EvalContext).
    pub(super) static CURRENT_TRIGGER_TABLES: std::cell::RefCell<Option<std::rc::Rc<TriggerTables>>> =
        const { std::cell::RefCell::new(None) };
}

/// The `inserted` or `deleted` pseudo-table rows visible to the running trigger,
/// as a materialized source, if a trigger scope is armed and `name` is one of
/// them. Returns `None` for any other name (falls through to catalog resolution).
pub(super) fn current_trigger_source(name: &str, qualifier: &str) -> Option<Source> {
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
pub(super) struct TriggerScope {
    prev: Option<std::rc::Rc<TriggerTables>>,
}

impl TriggerScope {
    pub(super) fn enter(tables: std::rc::Rc<TriggerTables>) -> Self {
        let prev = CURRENT_TRIGGER_TABLES.with(|c| c.borrow_mut().replace(tables));
        TriggerScope { prev }
    }

    /// Clears the `inserted`/`deleted` view for a stored-object body (a
    /// procedure, function, TVF, or view called from within a trigger body):
    /// those pseudo-tables are visible only in the trigger's OWN statements, not
    /// in objects it calls. Restores the prior view on drop. A no-op (cheap) when
    /// no trigger scope is armed.
    pub(super) fn clear() -> Self {
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
    pub(super) static TRIGGER_CAPTURE: std::cell::RefCell<Option<CapturedImages>> =
        const { std::cell::RefCell::new(None) };
}

/// New (`inserted`) and old (`deleted`) row images collected during a DML that
/// has triggers to fire, plus the indices of the columns the statement touched
/// (its SET list, or every inserted column) for `UPDATE()`/`COLUMNS_UPDATED()`.
#[derive(Default)]
pub(super) struct CapturedImages {
    pub(super) inserted: Vec<Vec<Datum>>,
    pub(super) deleted: Vec<Vec<Datum>>,
    pub(super) updated: Vec<usize>,
}

/// Records row images into the active capture, if one is armed. `f` builds the
/// (inserted, deleted) images for a statement; it runs only when capture is on,
/// so the no-trigger path pays nothing.
pub(super) fn capture_trigger_images(f: impl FnOnce() -> (Vec<Vec<Datum>>, Vec<Vec<Datum>>)) {
    TRIGGER_CAPTURE.with(|c| {
        let mut borrow = c.borrow_mut();
        if let Some(images) = borrow.as_mut() {
            let (ins, del) = f();
            images.inserted.extend(ins);
            images.deleted.extend(del);
        }
    });
}

/// Records the indices of the columns a firing UPDATE's SET list (or an INSERT's
/// target columns) touched, for `UPDATE()`/`COLUMNS_UPDATED()`, if capture is on.
pub(super) fn capture_trigger_updated(indices: Vec<usize>) {
    TRIGGER_CAPTURE.with(|c| {
        if let Some(images) = c.borrow_mut().as_mut() {
            images.updated = indices;
        }
    });
}

/// The columns the firing trigger's statement touched, resolved against the
/// parent table's schema — the value behind `UPDATE()`/`COLUMNS_UPDATED()` in a
/// trigger body. `None` outside a trigger.
pub(super) fn current_trigger_updated_columns() -> Option<truthdb_sql::eval::UpdatedColumns> {
    CURRENT_TRIGGER_TABLES.with(|c| {
        let borrow = c.borrow();
        let tables = borrow.as_ref()?;
        Some(truthdb_sql::eval::UpdatedColumns {
            columns: tables
                .schema
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect(),
            touched: tables.updated.iter().copied().collect(),
        })
    })
}

thread_local! {
    /// The object_ids of triggers whose bodies are currently on the stack. With
    /// recursive triggers OFF (the default), a trigger must not re-fire itself
    /// (direct recursion) — a trigger on T whose body DMLs T is suppressed for
    /// that same trigger. Nested triggers on OTHER tables are not affected.
    pub(super) static FIRING_TRIGGERS: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Statement-scoped snapshot registration: capture on entry, and release —
/// pruning must not wait on a statement that errored — on every exit path.
pub(super) struct SnapshotScope<'a> {
    storage: &'a Storage,
    seq: u64,
    /// The snapshot that was current when this scope was entered, restored on
    /// exit. Scopes can nest — a scalar function's body statement runs under the
    /// caller's active statement/transaction snapshot — so a nested scope must
    /// restore the caller's snapshot on drop, not erase it.
    prev: Option<ReadSnapshot>,
}

impl<'a> SnapshotScope<'a> {
    pub(super) fn enter(storage: &'a Storage, own_txn: Option<u64>) -> Self {
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
pub(super) struct TxnSnapshotScope {
    prev: Option<ReadSnapshot>,
}

impl TxnSnapshotScope {
    pub(super) fn enter(snap: ReadSnapshot) -> Self {
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
pub(super) fn statement_reads_tables(storage: &Storage, db_id: u32, statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => select_reads_tables(storage, db_id, select),
        // An INSERT whose TARGET is a table variable writes only session memory,
        // so — unlike a base-table INSERT — it is not itself a data access; but a
        // `SELECT` source still reads real tables and must arm the snapshot.
        Statement::Insert(insert) if insert.table.value.starts_with('@') => match &insert.source {
            InsertSource::Select(select) => select_reads_tables(storage, db_id, select),
            _ => false,
        },
        _ => true,
    }
}

/// Whether a SELECT reads any real table — directly (FROM/subqueries) or through
/// a scalar function it calls. A `@t` table-variable source is session-local and
/// is not counted (it neither locks nor snapshots).
pub(super) fn select_reads_tables(storage: &Storage, db_id: u32, select: &Select) -> bool {
    let expanded = expand_ctes(select);
    let mut tables = Vec::new();
    collect_locked_tables(&expanded, &mut tables);
    !tables.is_empty() || !select_function_read_ids(storage, db_id, &expanded).is_empty()
}

/// SQL Server 3952: SNAPSHOT isolation used while the database does not
/// allow it — raised at data access, not at SET, exactly as SQL Server does.
pub(super) fn snapshot_not_allowed_error(database: &str) -> SqlError {
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
