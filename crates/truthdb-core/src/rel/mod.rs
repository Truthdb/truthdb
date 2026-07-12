//! SQL execution over the relational storage engine.
//!
//! Parses a batch with [`truthdb_sql`], then binds and runs each statement
//! against [`Storage`]'s `rel_*` API. SELECT uses a simple Volcano-style
//! pipeline materialized in memory: source scan -> WHERE filter -> ORDER BY
//! sort -> TOP limit -> projection. `sys.tables`/`sys.columns` are virtual
//! sources built from the catalog. Storage errors are mapped to SQL Server
//! error numbers.

pub mod collation;
mod value;

use truthdb_sql::ast::{
    ColumnDef, CreateTable, DataType, Delete, DropTable, Expr, ExprKind, Insert, IsolationLevel,
    Name, OrderItem, Select, SelectItem, SetStatement, Statement, Update,
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
        }
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

/// Parses and executes a SQL batch. A parse error yields an empty batch with
/// the error; a runtime error stops the batch but keeps earlier results.
pub fn execute_batch(storage: &mut Storage, sql: &str, txn_ctx: &mut TxnContext) -> BatchOutcome {
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
                if let Some(from) = &select.from {
                    let lower = from.value.to_ascii_lowercase();
                    if lower.starts_with("sys.") {
                        continue; // catalog view: unlocked
                    }
                    if let Some(def) = resolve_table(storage, &from.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(def.object_id), LockMode::Shared);
                    }
                }
            }
            Statement::Insert(Insert { table, .. })
            | Statement::Update(Update { table, .. })
            | Statement::Delete(Delete { table, .. }) => {
                if let Some(def) = resolve_table(storage, &table.value) {
                    add(Resource::Database, LockMode::IntentExclusive);
                    add(Resource::Table(def.object_id), LockMode::Exclusive);
                }
            }
            // DDL serializes against every active transaction via a
            // database-exclusive lock (it is disallowed inside a txn anyway).
            Statement::CreateTable(_) | Statement::DropTable(_) => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // Transaction control and SET take no data locks.
            Statement::BeginTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Set(_) => {}
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
            let eval_ctx = txn_ctx.eval_context();
            Ok(StatementResult::Rows(exec_select(
                storage, select, &eval_ctx,
            )?))
        }
    }
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
    }
    Ok(StatementResult::Done)
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

    storage
        .rel_create_table(table_name, columns, &key_names, defaults, identity)
        .map_err(|err| map_storage_err(err, table_name))?;
    Ok(StatementResult::Done)
}

fn bind_column(column: &ColumnDef) -> Result<Column, SqlError> {
    let column_type = match &column.data_type {
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
            max_len: length(*n, column)?,
        },
        DataType::NVarChar(n) => ColumnType::NVarChar {
            max_len: length(*n, column)?,
        },
        DataType::VarBinary(n) => ColumnType::VarBinary {
            max_len: length(*n, column)?,
        },
    };
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

fn length(n: u32, column: &ColumnDef) -> Result<u16, SqlError> {
    u16::try_from(n).map_err(|_| {
        SqlError::new(
            131,
            15,
            2,
            format!(
                "The size for column '{}' exceeds the maximum.",
                column.name.value
            ),
        )
    })
}

// ---- DROP TABLE ---------------------------------------------------------

fn exec_drop_table(storage: &mut Storage, drop: &DropTable) -> Result<StatementResult, SqlError> {
    let name = resolve_table(storage, &drop.table.value).map(|d| d.name);
    match name {
        Some(name) => {
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

    // Reserve identity values for the whole batch up front. A failed insert
    // consumes them (a gap), but a value is never reused (SQL Server-faithful).
    let identity_first = if identity_col.is_some() {
        storage
            .rel_reserve_identity(&def.name, insert.rows.len())
            .map_err(|e| map_storage_err(e, &def.name))?
    } else {
        None
    };

    // Build every row up front; insert them as one atomic statement.
    let mut rows = Vec::with_capacity(insert.rows.len());
    for (row_no, row_exprs) in insert.rows.iter().enumerate() {
        if row_exprs.len() != target.len() {
            return Err(SqlError::new(
                110,
                15,
                1,
                "There are fewer or more columns in the INSERT statement than values specified in the VALUES clause.",
            ));
        }
        // Full row in schema order: unspecified columns start NULL.
        let mut values = vec![Datum::Null; ncols];
        for (position, expr) in target.iter().zip(row_exprs) {
            let column = &schema.columns[*position];
            let sql_value = eval_constant(expr, eval_ctx)?;
            if sql_value.is_null() && !column.nullable {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
            values[*position] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
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
        rows.push(values);
    }

    let inserted = rows.len() as u64;
    storage
        .rel_insert_many(&def.name, rows, scope)
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::RowsAffected(inserted))
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
        // Every SET expression sees the pre-update row.
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
        updates.push((locator, new_row));
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
            targets.push(locator);
        }
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
    /// Per-column collation names (parallel to `columns`; `None` = database
    /// default). Used by ORDER BY on character columns.
    collations: Vec<Option<String>>,
    /// Rows of typed values (real-table Datums; virtual sources build them).
    rows: Vec<Vec<Datum>>,
}

impl Source {
    fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    fn types(&self) -> Vec<ColumnType> {
        self.columns.iter().map(|c| c.column_type).collect()
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

fn exec_select(
    storage: &mut Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let source = build_source(storage, select.from.as_ref())?;
    let resolver = source.names();
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

fn order_rows(
    rows: &mut [Vec<Datum>],
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &Vec<String>,
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
    resolver: &Vec<String>,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // Output column plan: a source column (typed, pass-through) or a
    // computed expression (evaluated then typed by inference).
    enum Proj<'a> {
        SourceColumn { index: usize, name: String },
        Expr { name: String, expr: &'a Expr },
    }
    let source_names: Vec<String> = source_columns.iter().map(|c| c.name.clone()).collect();
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
            SelectItem::Expr { expr, alias } => {
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                match bare_column_index(expr, &source_names) {
                    // A bare column still carries its resolved output name so an
                    // `AS alias` (or the referenced name's casing) is preserved.
                    Some(index) => projs.push(Proj::SourceColumn { index, name }),
                    None => projs.push(Proj::Expr { name, expr }),
                }
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
        ExprKind::Column(name) => Some(name.value.clone()),
        _ => None,
    }
}

fn bare_column_index(expr: &Expr, columns: &[String]) -> Option<usize> {
    match &expr.kind {
        ExprKind::Column(name) => columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(&name.value)),
        _ => None,
    }
}

fn build_source(storage: &mut Storage, from: Option<&Name>) -> Result<Source, SqlError> {
    let Some(from) = from else {
        // No FROM: one row, no columns (constant SELECT).
        return Ok(Source {
            columns: Vec::new(),
            collations: Vec::new(),
            rows: vec![Vec::new()],
        });
    };
    match from.value.to_ascii_lowercase().as_str() {
        "sys.tables" => Ok(sys_tables(storage)),
        "sys.columns" => Ok(sys_columns(storage)),
        _ => {
            let def = resolve_table(storage, &from.value)
                .ok_or_else(|| SqlError::invalid_object(&from.value).at(from.span))?;
            let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
            let rows = storage
                .rel_scan(&def.name)
                .map_err(|err| map_storage_err(err, &def.name))?;
            let columns = schema
                .columns
                .iter()
                .map(|c| ResultColumn {
                    name: c.name.clone(),
                    column_type: c.column_type,
                })
                .collect();
            let collations = schema.columns.iter().map(|c| c.collation.clone()).collect();
            Ok(Source {
                columns,
                collations,
                rows,
            })
        }
    }
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
    Source {
        columns,
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
    Source {
        columns,
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
