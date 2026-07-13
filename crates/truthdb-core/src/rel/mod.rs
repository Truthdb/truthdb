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
mod plan;
mod value;

use truthdb_sql::ast::{
    AlterAction, AlterTable, CheckConstraint, ColumnDef, CreateIndex, CreateTable, DataType,
    Declaration, Delete, DropIndex, DropTable, Expr, ExprKind, ForeignKey, Insert, InsertSource,
    IsolationLevel, JoinKind, Name, OrderItem, Select, SelectItem, SetStatement, Statement,
    TableRef, Update,
};
use truthdb_sql::error::SqlError;
use truthdb_sql::eval::EvalContext;
use truthdb_sql::value::{SqlValue, order_key_cmp};
use truthdb_sql::{ast, eval};

use crate::lock::{LockMode, Resource};
use crate::relstore::catalog::{self, TableDef};
use crate::relstore::row::{Column, Schema};
use crate::relstore::types::{ColumnType, Datum};
use crate::storage::{Storage, StorageError, StorageTxn, TxnScope};

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
    /// `SET SHOWPLAN_TEXT ON` — a SELECT returns its plan text, not results.
    showplan_text: bool,
    /// Declared batch variables (name without `@`, lowercased) to their type
    /// and current value. Cleared at the start of each batch.
    variables: std::collections::HashMap<String, (ColumnType, SqlValue)>,
    /// Connection identity for session intrinsics (`DB_NAME()`,
    /// `SUSER_SNAME()`, `@@SPID`), set once when the session opens.
    database: String,
    login: String,
    spid: i32,
}

/// Session isolation level (defaults to READ COMMITTED, like SQL Server).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Isolation {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
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
            spid: self.spid,
        }
    }

    /// Records the connection identity used by session intrinsics. Called once
    /// when the session opens.
    pub fn set_session_identity(&mut self, database: String, login: String, spid: i32) {
        self.database = database;
        self.login = login;
        self.spid = spid;
    }

    /// Clears batch-scoped variables (called at the start of each batch).
    pub fn clear_variables(&mut self) {
        self.variables.clear();
    }

    /// True if a transaction is open (used by the session to decide whether a
    /// disconnect must roll back).
    pub fn has_open_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// The session's current isolation level (drives which locks reads take).
    pub fn isolation(&self) -> Isolation {
        self.isolation
    }

    /// Rolls back and discards any open transaction (connection teardown).
    pub fn abort(&mut self, storage: &mut Storage) {
        if let Some(txn) = self.txn.take() {
            let _ = storage.rel_rollback(txn);
        }
        self.trancount = 0;
        self.doomed = false;
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
pub fn execute_batch(storage: &mut Storage, sql: &str, txn_ctx: &mut TxnContext) -> BatchOutcome {
    execute_batch_with_params(storage, sql, txn_ctx, &[])
}

/// Like [`execute_batch`], but seeds `params` as batch variables before running
/// the statement text — the `sp_executesql` path. Parameters are injected as
/// already-typed values, never re-rendered into the SQL text, so a parameter
/// value can never alter the statement's structure (no injection surface).
pub fn execute_batch_with_params(
    storage: &mut Storage,
    sql: &str,
    txn_ctx: &mut TxnContext,
    params: &[RpcParam],
) -> BatchOutcome {
    // Variables are batch-scoped: each batch starts with none.
    txn_ctx.clear_variables();
    for param in params {
        // The lexer keys `@p1` as `p1` (leading `@` stripped, lowercased); the
        // RPC name arrives as `@p1`, so normalise it the same way to match.
        let key = param.name.trim_start_matches('@').to_ascii_lowercase();
        let value = value::datum_to_sql(&param.value, &param.column_type);
        txn_ctx.variables.insert(key, (param.column_type, value));
    }
    let statements = match truthdb_sql::parse(sql) {
        Ok(statements) => statements,
        Err(error) => {
            return BatchOutcome {
                results: Vec::new(),
                error: Some(error),
            };
        }
    };
    let mut results = Vec::with_capacity(statements.len());
    for statement in &statements {
        match exec_statement(storage, statement, txn_ctx) {
            Ok(result) => results.push(result),
            Err(error) => {
                // A statement failure inside an explicit transaction dooms it
                // (XACT_ABORT-style): only ROLLBACK is then permitted.
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return BatchOutcome {
                    results,
                    error: Some(error),
                };
            }
        }
    }
    BatchOutcome {
        results,
        error: None,
    }
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

pub fn analyze_locks(
    storage: &Storage,
    sql: &str,
    isolation: Isolation,
) -> Vec<(Resource, LockMode)> {
    let Ok(statements) = truthdb_sql::parse(sql) else {
        return Vec::new();
    };
    // Reads take shared locks except under READ UNCOMMITTED, which takes none.
    // A batch that raises the isolation level (e.g. `SET ISOLATION LEVEL
    // SERIALIZABLE; SELECT ...`) must lock its reads even if the session was
    // READ UNCOMMITTED on entry — otherwise the post-SET read would run
    // unlocked. We therefore take read locks unless the whole batch is READ
    // UNCOMMITTED: the session is RU and no SET raises it above RU.
    let escalates_reads = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(level))
                if !matches!(level, IsolationLevel::ReadUncommitted)
        )
    });
    let reads_lock = !matches!(isolation, Isolation::ReadUncommitted) || escalates_reads;
    let mut needs: std::collections::HashMap<Resource, LockMode> = std::collections::HashMap::new();
    let mut add = |resource: Resource, mode: LockMode| {
        needs
            .entry(resource)
            .and_modify(|m| *m = m.combine(mode))
            .or_insert(mode);
    };
    for statement in &statements {
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
                    if name.value.to_ascii_lowercase().starts_with("sys.") {
                        continue; // catalog view: unlocked
                    }
                    if let Some(def) = resolve_table(storage, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(def.object_id), LockMode::Shared);
                    }
                }
            }
            Statement::Insert(insert) => {
                if let Some(def) = resolve_table(storage, &insert.table.value) {
                    add(Resource::Database, LockMode::IntentExclusive);
                    add(Resource::Table(def.object_id), LockMode::Exclusive);
                    // A child INSERT reads its FK parents (integrity read).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
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
                        if name.value.to_ascii_lowercase().starts_with("sys.") {
                            continue;
                        }
                        if let Some(def) = resolve_table(storage, &name.value) {
                            add(Resource::Database, LockMode::IntentShared);
                            add(Resource::Table(def.object_id), LockMode::Shared);
                        }
                    }
                }
            }
            Statement::Update(Update { table, .. }) => {
                if let Some(def) = resolve_table(storage, &table.value) {
                    add(Resource::Database, LockMode::IntentExclusive);
                    add(Resource::Table(def.object_id), LockMode::Exclusive);
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
                }
            }
            Statement::Delete(Delete { table, .. }) => {
                if let Some(def) = resolve_table(storage, &table.value) {
                    add(Resource::Database, LockMode::IntentExclusive);
                    add(Resource::Table(def.object_id), LockMode::Exclusive);
                    // DELETE reads referencing children (NO ACTION check).
                    for oid in fk_child_object_ids(storage, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            // DDL serializes against every active transaction via a
            // database-exclusive lock (it is disallowed inside a txn anyway).
            Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_) => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // Transaction control, SET, and DECLARE take no data locks.
            Statement::BeginTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Set(_)
            | Statement::Declare(_) => {}
        }
    }
    needs.into_iter().collect()
}

/// Parses and executes a SQL batch, returning one result per statement, or
/// the first error (discarding earlier results). Kept for tests; the server
/// uses [`execute_batch`].
#[cfg(test)]
pub fn execute(storage: &mut Storage, sql: &str) -> Result<Vec<StatementResult>, SqlError> {
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
    storage: &mut Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // A doomed transaction rejects everything but ROLLBACK.
    if txn_ctx.doomed && !matches!(statement, Statement::Rollback { .. }) {
        return Err(SqlError::new(
            3930,
            16,
            1,
            "The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.",
        ));
    }
    match statement {
        Statement::BeginTransaction { .. } => exec_begin(storage, txn_ctx),
        Statement::Commit { .. } => exec_commit(storage, txn_ctx),
        Statement::Rollback { .. } => exec_rollback(storage, txn_ctx),
        Statement::Set(set) => exec_set(txn_ctx, set),
        Statement::Declare(decls) => exec_declare(txn_ctx, decls),
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
        Statement::Insert(insert) => {
            let eval_ctx = txn_ctx.eval_context();
            let mut scope = txn_ctx.scope();
            exec_insert(storage, insert, &mut scope, &eval_ctx)
        }
        Statement::Update(update) => {
            let eval_ctx = txn_ctx.eval_context();
            let mut scope = txn_ctx.scope();
            exec_update(storage, update, &mut scope, &eval_ctx)
        }
        Statement::Delete(delete) => {
            let eval_ctx = txn_ctx.eval_context();
            let mut scope = txn_ctx.scope();
            exec_delete(storage, delete, &mut scope, &eval_ctx)
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
    }
}

/// Builds a one-column `SHOWPLAN_TEXT` rowset describing a SELECT's access
/// path, without executing it.
fn showplan_rows(
    storage: &mut Storage,
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
                    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
                    let path = plan::choose(&def, &schema, &select.where_clause, eval_ctx);
                    plan::plan_text(&path, &def.name)
                }
                None => vec![format!("Table Scan({})", name.value)],
            }
        }
        Some(TableRef::Table { name, .. }) => vec![format!("Table Scan({})", name.value)],
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

fn exec_begin(storage: &mut Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
    if ctx.txn.is_none() {
        ctx.txn = Some(storage.rel_begin().map_err(|e| map_storage_err(e, ""))?);
    }
    // Nested BEGIN only bumps the count (SQL Server semantics).
    ctx.trancount += 1;
    Ok(StatementResult::Done)
}

fn exec_commit(storage: &mut Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
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
        storage
            .rel_commit(txn)
            .map_err(|e| map_storage_err(e, ""))?;
    }
    Ok(StatementResult::Done)
}

fn exec_rollback(storage: &mut Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3903,
            16,
            1,
            "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    // ROLLBACK always unwinds the whole transaction, regardless of nesting.
    // Reset the session's transaction counters even if the storage rollback
    // fails (which wedges the store): the transaction is over either way, so
    // leaving @@TRANCOUNT / doomed set would desync the session.
    let result = match ctx.txn.take() {
        Some(txn) => storage
            .rel_rollback(txn)
            .map_err(|e| map_storage_err(e, "")),
        None => Ok(()),
    };
    ctx.trancount = 0;
    ctx.doomed = false;
    result.map(|()| StatementResult::Done)
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
            }
        }
        SetStatement::ShowplanText(on) => ctx.showplan_text = *on,
        SetStatement::Variable { name, value } => {
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
        if ctx.variables.contains_key(&decl.name) {
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

fn exec_create_table(
    storage: &mut Storage,
    create: &CreateTable,
) -> Result<StatementResult, SqlError> {
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
    resolver: &Vec<String>,
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
    storage: &mut Storage,
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
        return Ok(batch
            .iter()
            .any(|r| child.key_columns.iter().zip(key).all(|(&i, k)| &r[i] == k)));
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
    storage: &mut Storage,
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

/// Enforces NO ACTION on the parent side: no surviving child row may reference
/// any of `removed_keys` (parent primary-key values being deleted or vacated by
/// an UPDATE). A referencing child is error 547. Referencing children are found
/// by scanning every table's foreign keys (FK-index optimization deferred).
fn enforce_parent_fks(
    storage: &mut Storage,
    parent: &TableDef,
    removed_keys: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    if removed_keys.is_empty() {
        return Ok(());
    }
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
        let child_rows = storage
            .rel_scan(&child.name)
            .map_err(|e| map_storage_err(e, &child.name))?;
        for fk in &child.foreign_keys {
            if !fk.parent.eq_ignore_ascii_case(&parent.name) {
                continue;
            }
            for row in &child_rows {
                // A self-referencing row that is itself being removed does not
                // count as a surviving reference.
                if self_ref {
                    let pk: Vec<Datum> =
                        parent.key_columns.iter().map(|&i| row[i].clone()).collect();
                    if removed_keys.contains(&pk) {
                        continue;
                    }
                }
                let Some(key) = fk_key(fk, row) else {
                    continue;
                };
                if removed_keys.contains(&key) {
                    return Err(SqlError::new(
                        547,
                        16,
                        0,
                        format!(
                            "The {verb} statement conflicted with the REFERENCE constraint \"{}\". The conflict occurred in database \"truthdb\", table \"dbo.{}\".",
                            fk.name, child.name
                        ),
                    ));
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
    })
}

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

fn exec_drop_table(storage: &mut Storage, drop: &DropTable) -> Result<StatementResult, SqlError> {
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

// ---- CREATE / DROP INDEX ------------------------------------------------

fn exec_create_index(
    storage: &mut Storage,
    create: &CreateIndex,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &create.table.value)
        .ok_or_else(|| SqlError::invalid_object(&create.table.value).at(create.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let mut columns = Vec::with_capacity(create.columns.len());
    for col in &create.columns {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.name.value))
            .ok_or_else(|| SqlError::invalid_column(&col.name.value).at(col.name.span))?;
        columns.push((index, col.ascending));
    }
    storage
        .rel_create_index(&def.name, create.name.value.clone(), columns, create.unique)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

fn exec_drop_index(storage: &mut Storage, drop: &DropIndex) -> Result<StatementResult, SqlError> {
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

fn exec_alter_table(
    storage: &mut Storage,
    alter: &AlterTable,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &alter.table.value)
        .ok_or_else(|| SqlError::invalid_object(&alter.table.value).at(alter.table.span))?;
    match &alter.action {
        AlterAction::AddCheck(check) => alter_add_check(storage, &def, check, eval_ctx),
        AlterAction::AddForeignKey(fk) => alter_add_foreign_key(storage, &def, fk),
        AlterAction::DropConstraint(name) => alter_drop_constraint(storage, &def, name),
    }
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY (...) REFERENCES ...`.
/// Validates the constraint and every existing row (WITH CHECK): a child row
/// referencing a missing parent is 547 and the constraint is not added.
fn alter_add_foreign_key(
    storage: &mut Storage,
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

/// `ALTER TABLE ... ADD [CONSTRAINT name] CHECK (expr)`. Validates the new
/// constraint against every existing row (SQL Server's default WITH CHECK); a
/// violating row is error 547 and the constraint is not added.
fn alter_add_check(
    storage: &mut Storage,
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
    let resolver = schema_names(&schema);
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
    storage: &mut Storage,
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
    storage: &mut Storage,
    insert: &Insert,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &insert.table.value)
        .ok_or_else(|| SqlError::invalid_object(&insert.table.value).at(insert.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    let increment = def.identity.map(|s| s.increment).unwrap_or(0);

    // CHECK constraints are parsed once and evaluated against each built row.
    let checks = parse_checks(&def)?;
    let check_resolver = schema_names(&schema);
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

    let inserted = rows.len() as u64;
    storage
        .rel_insert_many(&def.name, rows, scope)
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::RowsAffected(inserted))
}

/// Produces the input rows an INSERT supplies, each already in target-column
/// order and as [`SqlValue`]s: `VALUES` tuples are evaluated as constants; a
/// `SELECT` is executed and its rows converted. Rejects an arity mismatch
/// against the target column count (110 for VALUES, 120/121 for SELECT).
fn insert_input_rows(
    storage: &mut Storage,
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

fn exec_update(
    storage: &mut Storage,
    update: &Update,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &update.table.value)
        .ok_or_else(|| SqlError::invalid_object(&update.table.value).at(update.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = schema_names(&schema);
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
    let located = storage
        .rel_scan_located(&def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    let types = schema_types(&schema);
    let mut updates = Vec::new();
    for (locator, row) in located {
        if !predicate_true(&update.where_clause, &row, &types, &resolver, eval_ctx)? {
            continue;
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
        let post_pks: Vec<Vec<Datum>> = post_rows.iter().map(|r| pk_of(&def, r)).collect();
        for r in &post_rows {
            for fk in def
                .foreign_keys
                .iter()
                .filter(|fk| fk.parent.eq_ignore_ascii_case(&def.name))
            {
                if let Some(key) = fk_key(fk, r)
                    && !post_pks.contains(&key)
                {
                    return Err(fk_child_violation(&fk.name, "UPDATE", &fk.parent));
                }
            }
        }
    }

    let count = storage
        .rel_update_located(&def.name, updates, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

fn exec_delete(
    storage: &mut Storage,
    delete: &Delete,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &delete.table.value)
        .ok_or_else(|| SqlError::invalid_object(&delete.table.value).at(delete.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = schema_names(&schema);

    let types = schema_types(&schema);
    let located = storage
        .rel_scan_located(&def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    let mut targets = Vec::new();
    for (locator, row) in located {
        if predicate_true(&delete.where_clause, &row, &types, &resolver, eval_ctx)? {
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

    let count = storage
        .rel_delete_located(&def.name, targets, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

fn schema_names(schema: &Schema) -> Vec<String> {
    schema.columns.iter().map(|c| c.name.clone()).collect()
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
    resolver: &Vec<String>,
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

struct Source {
    columns: Vec<ResultColumn>,
    /// Per-column table qualifier (alias or table name; `None` = virtual/
    /// constant source), parallel to `columns`. Drives multi-table resolution.
    qualifiers: Vec<Option<String>>,
    /// Per-column collation names (parallel to `columns`; `None` = database
    /// default). Used by ORDER BY on character columns.
    collations: Vec<Option<String>>,
    /// Rows of typed values (real-table Datums; virtual sources build them).
    rows: Vec<Vec<Datum>>,
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
}

/// Resolver over an output RowSet's columns. Output columns are unqualified,
/// so a qualified `t.col` reference (e.g. in a grouped query's ORDER BY)
/// resolves by its bare name.
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
        // Both branches reject an ambiguous match (more than one column) by
        // returning None, so eval raises an invalid-column error rather than
        // silently picking the first.
        let mut found = None;
        let matches = |q: &Option<String>, c: &str| -> bool {
            if let Some((qualifier, column)) = name.rsplit_once('.') {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
                    && c.eq_ignore_ascii_case(column)
            } else {
                c.eq_ignore_ascii_case(name)
            }
        };
        for (index, (qualifier, column)) in self.columns.iter().enumerate() {
            if matches(qualifier, column) {
                if found.is_some() {
                    return None; // ambiguous
                }
                found = Some(index);
            }
        }
        found
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
    if select.ctes.is_empty() {
        return select.clone();
    }
    let mut resolved: CteMap = std::collections::HashMap::new();
    for cte in &select.ctes {
        let body = expand_select_ctes(&cte.query, &resolved);
        resolved.insert(cte.name.value.to_ascii_lowercase(), body);
    }
    let mut out = expand_select_ctes(select, &resolved);
    out.ctes = Vec::new();
    out
}

/// A copy of `select` with its FROM references and its embedded subqueries'
/// references to `resolved` CTEs replaced by derived tables. (The CTE is visible
/// to the whole statement, not just the top-level FROM.) The select's own `ctes`
/// field is left intact — a nested `WITH` keeps its own scope.
fn expand_select_ctes(select: &Select, resolved: &CteMap) -> Select {
    let mut out = select.clone();
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
    storage: &mut Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<Select, SqlError> {
    let items = select
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => Ok(SelectItem::Expr {
                expr: rewrite_subqueries(storage, expr, eval_ctx)?,
                alias: alias.clone(),
            }),
            other => Ok(other.clone()),
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    let rewrite_opt = |storage: &mut Storage, e: &Option<Expr>| -> Result<Option<Expr>, SqlError> {
        e.as_ref()
            .map(|e| rewrite_subqueries(storage, e, eval_ctx))
            .transpose()
    };
    let where_clause = rewrite_opt(storage, &select.where_clause)?;
    let having = rewrite_opt(storage, &select.having)?;
    let group_by = select
        .group_by
        .iter()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx))
        .collect::<Result<Vec<_>, SqlError>>()?;
    let order_by = select
        .order_by
        .iter()
        .map(|o| {
            Ok(OrderItem {
                expr: rewrite_subqueries(storage, &o.expr, eval_ctx)?,
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
    storage: &mut Storage,
    expr: &Expr,
    eval_ctx: &EvalContext,
) -> Result<Expr, SqlError> {
    let recur = |storage: &mut Storage, e: &Expr| rewrite_subqueries(storage, e, eval_ctx);
    let recur_box = |storage: &mut Storage, e: &Expr| -> Result<Box<Expr>, SqlError> {
        Ok(Box::new(recur(storage, e)?))
    };
    let recur_opt =
        |storage: &mut Storage, e: &Option<Box<Expr>>| -> Result<Option<Box<Expr>>, SqlError> {
            e.as_ref().map(|e| recur_box(storage, e)).transpose()
        };
    let kind = match &expr.kind {
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
    storage: &mut Storage,
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
    storage: &mut Storage,
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

fn exec_select(
    storage: &mut Storage,
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
    // numeric/string expression is rejected rather than silently coerced.
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    for row in source.rows {
        let keep = match &select.where_clause {
            None => true,
            Some(predicate) => {
                let value = eval::eval(predicate, &row_values(&row, &types), &resolver, eval_ctx)?;
                match value {
                    SqlValue::Bool(b) => b,
                    SqlValue::Null => false,
                    _ => {
                        return Err(SqlError::new(
                            4145,
                            15,
                            1,
                            "An expression of non-boolean type specified in a context where a condition is expected, near 'WHERE'.",
                        )
                        .at(predicate.span));
                    }
                }
            }
        };
        if keep {
            rows.push(row);
        }
    }

    // A grouped/aggregated or DISTINCT query projects first (its ORDER BY
    // references the output), while a plain query orders the source rows so it
    // can order by columns that are not in the SELECT list.
    if aggregate::is_aggregated(select) || select.distinct {
        let mut out = if aggregate::is_aggregated(select) {
            aggregate::execute(select, &rows, &types, &resolver, eval_ctx)?
        } else {
            project(
                &select.items,
                &source.columns,
                &rows,
                &types,
                &resolver,
                eval_ctx,
            )?
        };
        if select.distinct {
            dedup_rows(&mut out);
        }
        order_output(&mut out, &select.order_by, eval_ctx)?;
        if let Some(top) = select.top {
            out.rows.truncate(top as usize);
        }
        return Ok(out);
    }

    // ORDER BY (evaluated against the source row; stable so equal keys keep
    // input order).
    if !select.order_by.is_empty() {
        order_rows(
            &mut rows,
            &select.order_by,
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
        &select.items,
        &source.columns,
        &rows,
        &types,
        &resolver,
        eval_ctx,
    )
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
    storage: &mut Storage,
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
    Ok(StatementResult::Done)
}

/// Removes duplicate output rows (SELECT DISTINCT), keeping first occurrence.
/// NULLs are equal to each other (`Datum` equality), matching SQL Server.
fn dedup_rows(rowset: &mut RowSet) {
    let mut seen: Vec<Vec<Datum>> = Vec::new();
    rowset.rows.retain(|row| {
        if seen.iter().any(|s| s == row) {
            false
        } else {
            seen.push(row.clone());
            true
        }
    });
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

fn order_rows(
    rows: &mut [Vec<Datum>],
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<(), SqlError> {
    use std::cmp::Ordering;
    // A bare character column is ordered by its collation (its COLLATE clause,
    // else the database default); anything else uses value ordering.
    let collators: Vec<Option<collation::Collation>> = order_by
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
        .collect();

    // Precompute sort keys to keep comparisons cheap and to surface eval
    // errors before sorting.
    let mut keyed: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let values = row_values(row, types);
        let mut key = Vec::with_capacity(order_by.len());
        for item in order_by {
            key.push(eval::eval(&item.expr, &values, resolver, eval_ctx)?);
        }
        keyed.push((key, index));
    }
    keyed.sort_by(|(a, ai), (b, bi)| {
        for (col, item) in order_by.iter().enumerate() {
            let ord = match (&collators[col], &a[col], &b[col]) {
                (Some(coll), SqlValue::Str(x), SqlValue::Str(y)) => coll.compare(x, y),
                // NULL still sorts first even under a collation.
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
        ai.cmp(bi) // stable tie-break
    });
    let order: Vec<usize> = keyed.into_iter().map(|(_, i)| i).collect();
    let reordered: Vec<Vec<Datum>> = order.iter().map(|&i| rows[i].clone()).collect();
    rows.clone_from_slice(&reordered);
    Ok(())
}

fn project(
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
                // Evaluate the column for every row, then infer one type.
                let mut values = Vec::with_capacity(rows.len());
                for row in &row_sql {
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
    use truthdb_sql::eval::ColumnResolver;
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
    }
}

/// Collects every base table a SELECT reads for the lock set: its FROM tree
/// (including derived-table subqueries and join `ON` clauses) plus every
/// subquery embedded in its expressions (WHERE/SELECT list/HAVING/GROUP BY/
/// ORDER BY). Recurses through nested subqueries.
fn collect_locked_tables<'a>(select: &'a Select, out: &mut Vec<&'a Name>) {
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
    storage: &mut Storage,
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
    storage: &mut Storage,
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
            rows: vec![Vec::new()],
        }),
        // A single top-level table may use the WHERE for an index seek; base
        // tables inside a join scan fully (join-order planning is later).
        Some(TableRef::Table { name, alias }) => {
            build_table_source(storage, name, alias.as_ref(), where_clause, eval_ctx)
        }
        Some(join) => build_join(storage, join, eval_ctx),
    }
}

/// Builds the row source for one base table (or `sys.*` view), stamping every
/// column with the table's qualifier (its alias, else its name).
fn build_table_source(
    storage: &mut Storage,
    name: &Name,
    alias: Option<&Name>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let base = match name.value.to_ascii_lowercase().as_str() {
        "sys.tables" => sys_tables(storage),
        "sys.columns" => sys_columns(storage),
        "sys.indexes" => sys_indexes(storage),
        "sys.check_constraints" => sys_check_constraints(storage),
        "sys.foreign_keys" => sys_foreign_keys(storage),
        "sys.default_constraints" => sys_default_constraints(storage),
        _ => {
            let def = resolve_table(storage, &name.value)
                .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
            let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
            // An index seek narrows the candidate set; the WHERE filter later
            // re-checks, so results match a full scan.
            let rows = match plan::choose(&def, &schema, where_clause, eval_ctx) {
                plan::AccessPath::TableScan => storage
                    .rel_scan(&def.name)
                    .map_err(|err| map_storage_err(err, &def.name))?,
                plan::AccessPath::IndexSeek {
                    index_object_id,
                    lower,
                    upper,
                    ..
                } => storage
                    .rel_index_scan(&def.name, index_object_id, lower, upper)
                    .map_err(|err| map_storage_err(err, &def.name))?,
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

/// Recursively builds a join tree's combined row source (base tables scan
/// fully).
fn build_join(
    storage: &mut Storage,
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
            join_sources(left, right, *kind, on.as_ref(), eval_ctx)
        }
        TableRef::Derived { subquery, alias } => {
            build_derived_source(storage, subquery, alias, eval_ctx)
        }
    }
}

/// Builds a derived table's row source by executing its subquery and stamping
/// every output column with the derived-table alias. Every column must be named
/// (8155) and names must be unique within the derived table (8156).
fn build_derived_source(
    storage: &mut Storage,
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
        rows: rowset.rows,
    })
}

/// Nested-loop join of two materialized sources. The ON predicate (absent for
/// CROSS) is evaluated against the concatenated row; outer joins emit NULL-
/// extended rows for unmatched sides.
fn join_sources(
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

    let mut rows = Vec::new();
    match kind {
        JoinKind::Cross | JoinKind::Inner => {
            for l in &left.rows {
                for r in &right.rows {
                    if matches(l, r)? {
                        rows.push(concat(l, r));
                    }
                }
            }
        }
        JoinKind::Left => {
            for l in &left.rows {
                let mut matched = false;
                for r in &right.rows {
                    if matches(l, r)? {
                        rows.push(concat(l, r));
                        matched = true;
                    }
                }
                if !matched {
                    rows.push(concat(l, &right_nulls));
                }
            }
        }
        JoinKind::Right => {
            for r in &right.rows {
                let mut matched = false;
                for l in &left.rows {
                    if matches(l, r)? {
                        rows.push(concat(l, r));
                        matched = true;
                    }
                }
                if !matched {
                    rows.push(concat(&left_nulls, r));
                }
            }
        }
        JoinKind::Full => {
            let mut right_matched = vec![false; right.rows.len()];
            for l in &left.rows {
                let mut matched = false;
                for (index, r) in right.rows.iter().enumerate() {
                    if matches(l, r)? {
                        rows.push(concat(l, r));
                        matched = true;
                        right_matched[index] = true;
                    }
                }
                if !matched {
                    rows.push(concat(l, &right_nulls));
                }
            }
            for (index, r) in right.rows.iter().enumerate() {
                if !right_matched[index] {
                    rows.push(concat(&left_nulls, r));
                }
            }
        }
    }
    Ok(Source {
        columns,
        qualifiers,
        collations,
        rows,
    })
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
        .map(|def| vec![Datum::Int(def.object_id as i32), Datum::NVarChar(def.name)])
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows,
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
        rows,
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
        rows,
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
        rows,
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
        rows,
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
        rows,
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
        StorageError::InvalidConfig(msg) => SqlError::new(1701, 16, 1, msg),
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
