//! SQL execution over the relational storage engine.
//!
//! Parses a batch with [`truthdb_sql`], then binds and runs each statement
//! against [`Storage`]'s `rel_*` API. SELECT uses a simple Volcano-style
//! pipeline materialized in memory: source scan -> WHERE filter -> ORDER BY
//! sort -> TOP limit -> projection. `sys.tables`/`sys.columns` are virtual
//! sources built from the catalog. Storage errors are mapped to SQL Server
//! error numbers.

mod value;

use truthdb_sql::ast::{
    ColumnDef, CreateTable, DataType, DropTable, Expr, ExprKind, Insert, Name, OrderItem, Select,
    SelectItem, Statement,
};
use truthdb_sql::error::SqlError;
use truthdb_sql::value::{SqlValue, order_key_cmp};
use truthdb_sql::{ast, eval};

use crate::relstore::catalog::TableDef;
use crate::relstore::row::{Column, Schema};
use crate::relstore::types::ColumnType;
use crate::storage::{Storage, StorageError};
use value::Cell;

/// Result of one executed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementResult {
    Rows(RowSet),
    RowsAffected(u64),
    /// DDL and other statements with no rowset and no count.
    Done,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RowSet {
    pub columns: Vec<String>,
    /// Cells; `None` renders as NULL.
    pub rows: Vec<Vec<Option<String>>>,
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
pub fn execute_batch(storage: &mut Storage, sql: &str) -> BatchOutcome {
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
        match exec_statement(storage, statement) {
            Ok(result) => results.push(result),
            Err(error) => {
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

/// Parses and executes a SQL batch, returning one result per statement, or
/// the first error (discarding earlier results). Kept for tests; the server
/// uses [`execute_batch`].
#[cfg(test)]
pub fn execute(storage: &mut Storage, sql: &str) -> Result<Vec<StatementResult>, SqlError> {
    let outcome = execute_batch(storage, sql);
    match outcome.error {
        Some(error) => Err(error),
        None => Ok(outcome.results),
    }
}

fn exec_statement(
    storage: &mut Storage,
    statement: &Statement,
) -> Result<StatementResult, SqlError> {
    match statement {
        Statement::CreateTable(create) => exec_create_table(storage, create),
        Statement::DropTable(drop) => exec_drop_table(storage, drop),
        Statement::Insert(insert) => exec_insert(storage, insert),
        Statement::Select(select) => Ok(StatementResult::Rows(exec_select(storage, select)?)),
    }
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

    storage
        .rel_create_table(table_name, columns, &key_names)
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
        DataType::Float => ColumnType::Float,
        DataType::VarChar(n) => ColumnType::VarChar {
            max_len: length(*n, column)?,
        },
        DataType::NVarChar(n) => ColumnType::NVarChar {
            max_len: length(*n, column)?,
        },
    };
    // Columns are nullable by default (SQL Server ANSI default), PK columns
    // and explicit NOT NULL are not.
    let nullable = column.nullable.unwrap_or(!column.primary_key);
    Ok(Column {
        name: column.name.value.clone(),
        column_type,
        nullable,
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

fn exec_insert(storage: &mut Storage, insert: &Insert) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, &insert.table.value)
        .ok_or_else(|| SqlError::invalid_object(&insert.table.value).at(insert.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;

    // Target column indices (explicit list or all columns in order). A
    // column may not be named twice.
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
        None => (0..schema.columns.len()).collect(),
    };

    // Build every row up front; insert them as one atomic statement.
    let mut rows = Vec::with_capacity(insert.rows.len());
    for row_exprs in &insert.rows {
        if row_exprs.len() != target.len() {
            return Err(SqlError::new(
                110,
                15,
                1,
                "There are fewer or more columns in the INSERT statement than values specified in the VALUES clause.",
            ));
        }
        // Full row in schema order: unspecified columns become NULL.
        let mut values = vec![crate::relstore::types::Datum::Null; schema.columns.len()];
        for (position, expr) in target.iter().zip(row_exprs) {
            let column = &schema.columns[*position];
            let sql_value = eval_constant(expr)?;
            if sql_value.is_null() && !column.nullable {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
            values[*position] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
        }
        // NOT NULL columns that were never targeted.
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
        .rel_insert_many(&def.name, rows)
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::RowsAffected(inserted))
}

// ---- SELECT -------------------------------------------------------------

struct Source {
    columns: Vec<String>,
    rows: Vec<Vec<Cell>>,
}

fn exec_select(storage: &mut Storage, select: &Select) -> Result<RowSet, SqlError> {
    let source = build_source(storage, select.from.as_ref())?;
    let resolver = source.columns.clone();

    // WHERE. The predicate must be boolean-typed (SQL Server 4145): a bare
    // numeric/string expression is rejected rather than silently coerced.
    let mut rows: Vec<Vec<Cell>> = Vec::new();
    for row in source.rows {
        let keep = match &select.where_clause {
            None => true,
            Some(predicate) => {
                let values: Vec<SqlValue> = row.iter().map(|c| c.value.clone()).collect();
                let value = eval::eval(predicate, &values, &resolver)?;
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
        order_rows(&mut rows, &select.order_by, &resolver)?;
    }

    // TOP.
    if let Some(top) = select.top {
        rows.truncate(top as usize);
    }

    // Projection.
    project(&select.items, &source.columns, &rows, &resolver)
}

fn order_rows(
    rows: &mut [Vec<Cell>],
    order_by: &[OrderItem],
    resolver: &Vec<String>,
) -> Result<(), SqlError> {
    // Precompute sort keys to keep comparisons cheap and to surface eval
    // errors before sorting.
    let mut keyed: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let values: Vec<SqlValue> = row.iter().map(|c| c.value.clone()).collect();
        let mut key = Vec::with_capacity(order_by.len());
        for item in order_by {
            key.push(eval::eval(&item.expr, &values, resolver)?);
        }
        keyed.push((key, index));
    }
    keyed.sort_by(|(a, ai), (b, bi)| {
        for (col, item) in order_by.iter().enumerate() {
            let ord = order_key_cmp(&a[col], &b[col]);
            let ord = if item.descending { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        ai.cmp(bi) // stable tie-break
    });
    let order: Vec<usize> = keyed.into_iter().map(|(_, i)| i).collect();
    apply_permutation(rows, &order);
    Ok(())
}

fn apply_permutation(rows: &mut [Vec<Cell>], order: &[usize]) {
    let reordered: Vec<Vec<Cell>> = order.iter().map(|&i| rows[i].clone()).collect();
    rows.clone_from_slice(&reordered);
}

fn project(
    items: &[SelectItem],
    source_columns: &[String],
    rows: &[Vec<Cell>],
    resolver: &Vec<String>,
) -> Result<RowSet, SqlError> {
    // Output column plan: (name, projector).
    enum Proj<'a> {
        SourceColumn(usize),
        Expr(&'a Expr),
    }
    let mut names = Vec::new();
    let mut projs = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                for (index, name) in source_columns.iter().enumerate() {
                    names.push(name.clone());
                    projs.push(Proj::SourceColumn(index));
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                names.push(name);
                match bare_column_index(expr, source_columns) {
                    Some(index) => projs.push(Proj::SourceColumn(index)),
                    None => projs.push(Proj::Expr(expr)),
                }
            }
        }
    }

    let mut out_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out = Vec::with_capacity(projs.len());
        for proj in &projs {
            match proj {
                Proj::SourceColumn(index) => out.push(row[*index].display.clone()),
                Proj::Expr(expr) => {
                    let values: Vec<SqlValue> = row.iter().map(|c| c.value.clone()).collect();
                    let value = eval::eval(expr, &values, resolver)?;
                    out.push(Cell::from_sql(value).display);
                }
            }
        }
        out_rows.push(out);
    }
    Ok(RowSet {
        columns: names,
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
            let raw = storage
                .rel_scan(&def.name)
                .map_err(|err| map_storage_err(err, &def.name))?;
            let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            let rows = raw
                .into_iter()
                .map(|datums| {
                    datums
                        .iter()
                        .zip(&schema.columns)
                        .map(|(d, c)| Cell::from_datum(d, &c.column_type))
                        .collect()
                })
                .collect();
            Ok(Source { columns, rows })
        }
    }
}

// ---- sys.* virtual sources ---------------------------------------------

fn sys_tables(storage: &Storage) -> Source {
    let columns = vec!["object_id".to_string(), "name".to_string()];
    let rows = storage
        .rel_tables()
        .into_iter()
        .map(|def| {
            vec![
                Cell::from_sql(SqlValue::Int(def.object_id as i64)),
                Cell::from_sql(SqlValue::Str(def.name)),
            ]
        })
        .collect();
    Source { columns, rows }
}

fn sys_columns(storage: &Storage) -> Source {
    let columns = vec![
        "object_id".to_string(),
        "name".to_string(),
        "column_id".to_string(),
        "type".to_string(),
        "is_nullable".to_string(),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        for (index, (name, type_spec, nullable)) in def.columns.iter().enumerate() {
            rows.push(vec![
                Cell::from_sql(SqlValue::Int(def.object_id as i64)),
                Cell::from_sql(SqlValue::Str(name.clone())),
                Cell::from_sql(SqlValue::Int(index as i64 + 1)),
                Cell::from_sql(SqlValue::Str(type_spec.clone())),
                Cell::from_sql(SqlValue::Bool(*nullable)),
            ]);
        }
    }
    Source { columns, rows }
}

// ---- helpers ------------------------------------------------------------

/// Evaluates a constant expression (INSERT VALUES): no columns in scope.
fn eval_constant(expr: &Expr) -> Result<SqlValue, SqlError> {
    let empty: Vec<String> = Vec::new();
    eval::eval(expr, &[], &empty)
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
