use crate::collation::CollationSensitivity;
use crate::value::SqlValue;

/// The outcome of resolving a column name, distinguishing a missing column from
/// one that is ambiguous (matches more than one source column) so each maps to
/// the correct SQL Server error (208-family 207 vs 209).
pub enum Resolution {
    Found(usize),
    NotFound,
    Ambiguous,
}

/// Resolves a column name to its position in the row, case-insensitively.
pub trait ColumnResolver {
    fn resolve(&self, name: &str) -> Option<usize>;

    /// Like [`ColumnResolver::resolve`] but distinguishes not-found from
    /// ambiguous. The default cannot detect ambiguity (a `None` from `resolve`
    /// is reported as not-found); a multi-source resolver overrides it.
    fn resolve_detail(&self, name: &str) -> Resolution {
        match self.resolve(name) {
            Some(index) => Resolution::Found(index),
            None => Resolution::NotFound,
        }
    }

    /// The case sensitivity of the column at `index` — its collation. Drives
    /// case-insensitive string equality. The default (case-insensitive, the
    /// database default) is used by resolvers with no per-column collation; a
    /// resolver over base-table columns overrides it to honour explicit
    /// `_CS`/`_BIN` columns.
    fn collation(&self, _index: usize) -> CollationSensitivity {
        CollationSensitivity::default_collation()
    }
}

impl ColumnResolver for [String] {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.iter().position(|c| c.eq_ignore_ascii_case(name))
    }
}

impl ColumnResolver for Vec<String> {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.as_slice().resolve(name)
    }
}
/// The default database's id. Mirrors `truthdb-core`'s catalog constant
/// (this crate is a dependency of truthdb-core and cannot import it); the
/// mirror is asserted equal at truthdb-core's build.
pub const DEFAULT_DATABASE_ID: u32 = 1;

/// Session context available to expression evaluation: `@@`-variables, the
/// batch's `@`-variables, and (in later stages) the current time /
/// SCOPE_IDENTITY. `Default` is a no-transaction, no-variable context, used
/// where no session is in scope.
#[derive(Debug, Clone)]
pub struct EvalContext {
    pub trancount: i32,
    /// Declared batch variables (name without `@`, lowercased) to their current
    /// value. Present but NULL for a declared-but-unset variable; absent means
    /// undeclared.
    pub variables: std::collections::HashMap<String, SqlValue>,
    /// The connection's current database name — `DB_NAME()`.
    pub database: String,
    /// The connection's current database id — the namespace unqualified
    /// object names resolve in. Never 0: the manual `Default` lands in the
    /// default database.
    pub database_id: u32,
    /// Every database as `(id, canonical name)` — read by `DB_ID(name)` and
    /// `DB_NAME(id)`. Snapshotted per statement by the exec layer; empty in
    /// contexts with no storage in scope (the argument forms then answer
    /// NULL, like USER_NAME's by-id form).
    pub databases: Vec<(u32, String)>,
    /// The authenticated login name — `SUSER_SNAME()`.
    pub login: String,
    /// The session's database user name — `USER_NAME()` (with no argument).
    pub user: String,
    /// The session's effective SERVER-role names (lowercased) — read by
    /// `IS_SRVROLEMEMBER`.
    pub server_roles: std::collections::HashSet<String>,
    /// The session's effective DATABASE-role names (lowercased) — read by
    /// `IS_ROLEMEMBER`. Separate from `server_roles` so the two role namespaces
    /// do not cross-answer.
    pub db_roles: std::collections::HashSet<String>,
    /// Object-permission enforcement subject for this session (bypass flag +
    /// the principal_ids a grant may match). Computed once per batch.
    pub security: SecurityContext,
    /// The session process id — `@@SPID`.
    pub spid: i32,
    /// Rows affected/returned by the session's previous statement —
    /// `@@ROWCOUNT`.
    pub rowcount: i64,
    /// The last identity value inserted in this scope — `SCOPE_IDENTITY()`.
    /// `None` until an identity INSERT runs.
    pub scope_identity: Option<i64>,
    /// The error that transferred control to the innermost active `CATCH`
    /// block, read by `ERROR_NUMBER()`/`ERROR_MESSAGE()`/etc. `None` outside any
    /// `CATCH` block (where those functions return NULL).
    pub error: Option<ErrorInfo>,
    /// `XACT_STATE()`: 1 = an active, committable transaction; -1 = an active
    /// but uncommittable (doomed) transaction; 0 = no transaction.
    pub xact_state: i8,
    /// `@@ERROR` — the previous statement's error number, 0 on success.
    pub last_error: i32,
    /// `@@NESTLEVEL` — the current procedure nesting depth (0 in a batch).
    pub nestlevel: i32,
    /// Inside a trigger body: which columns the firing UPDATE/INSERT touched,
    /// for `UPDATE(<col>)` and `COLUMNS_UPDATED()`. `None` outside a trigger.
    pub updated_columns: Option<UpdatedColumns>,
    /// `@@FETCH_STATUS` — the result of the last cursor FETCH: 0 success, -1 past
    /// the end / no more rows, -2 the fetched row is missing.
    pub fetch_status: i32,
}

impl Default for EvalContext {
    fn default() -> Self {
        EvalContext {
            trancount: 0,
            variables: Default::default(),
            database: String::new(),
            database_id: DEFAULT_DATABASE_ID,
            databases: Vec::new(),
            login: String::new(),
            user: String::new(),
            server_roles: Default::default(),
            db_roles: Default::default(),
            security: Default::default(),
            spid: 0,
            rowcount: 0,
            scope_identity: None,
            error: None,
            xact_state: 0,
            last_error: 0,
            nestlevel: 0,
            updated_columns: None,
            fetch_status: 0,
        }
    }
}

/// The columns a trigger's firing statement touched: the parent table's column
/// names and the 0-based indices that were set (the UPDATE `SET` list, or every
/// inserted column for an INSERT; empty for a DELETE).
#[derive(Clone, Debug)]
pub struct UpdatedColumns {
    pub columns: Vec<String>,
    pub touched: std::collections::HashSet<usize>,
}

/// The object-permission enforcement subject for a session: whether it bypasses
/// checks (a trusted/internal connection, a sysadmin, or dbo/db_owner), and the
/// set of principal_ids a `GRANT`/`DENY` may match (the database user, its
/// effective roles, and `public`). Computed once per batch from the session
/// identity; read at the read/DML/EXECUTE choke points.
#[derive(Debug, Clone, Default)]
pub struct SecurityContext {
    pub bypass: bool,
    pub principals: std::collections::HashSet<u32>,
}

/// The error captured by a `CATCH` block, surfaced by the `ERROR_*()`
/// functions. Lines are not tracked (no statement-line map), so
/// `ERROR_LINE()` reports 0.
#[derive(Debug, Clone, Default)]
pub struct ErrorInfo {
    pub number: i32,
    pub message: String,
    pub severity: u8,
    pub state: u8,
    /// The stored procedure executing when the error was raised, for
    /// `ERROR_PROCEDURE()` — `None` in ad-hoc batches.
    pub procedure: Option<String>,
}
