use super::*;

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
