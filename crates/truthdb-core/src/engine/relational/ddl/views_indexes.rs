use super::super::prelude::*;

// ---- CREATE / DROP VIEW -------------------------------------------------

/// Parses a stored view definition back into its `SELECT`. The text was
/// validated at CREATE, so this only fails on catalog corruption.
pub(in crate::engine::relational) fn parse_view_query(
    text: &str,
    view_name: &str,
) -> Result<Select, SqlError> {
    match truthdb_sql::parse(text)?.into_iter().next() {
        Some(Statement::Select(select)) => Ok(select),
        _ => Err(SqlError::message_only(
            208,
            format!("The definition of view '{view_name}' is not a SELECT."),
        )),
    }
}

pub(in crate::engine::relational) fn exec_create_view(
    storage: &Storage,
    db_id: u32,
    create: &CreateView,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE VIEW", &create.name)?;
    if resolve_table(storage, db_id, &create.name.value).is_some() {
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
        .rel_create_view(db_id, bare, &create.query_text)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// A parameter default must be a CONSTANT (SQL Server rejects at CREATE):
/// literals, NULL, and a signed literal — never variables or functions,
/// which would otherwise evaluate against each CALLER's scope and drift.
pub(in crate::engine::relational) fn constant_default(expr: &Expr) -> bool {
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

pub(in crate::engine::relational) fn exec_drop_view(
    storage: &Storage,
    db_id: u32,
    drop: &DropView,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &drop.name.value) {
        Some(def) if def.is_view() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
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
pub(in crate::engine::relational) fn max_key_column_error(column: &str, table: &str) -> SqlError {
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

pub(in crate::engine::relational) fn exec_create_index(
    storage: &Storage,
    db_id: u32,
    create: &CreateIndex,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, db_id, &create.table.value)
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
            def.database_id,
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
pub(in crate::engine::relational) fn index_column_missing(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1911,
        16,
        1,
        format!("Column name '{column}' does not exist in the target table or view '{table}'."),
    )
}

pub(in crate::engine::relational) fn exec_drop_index(
    storage: &Storage,
    db_id: u32,
    drop: &DropIndex,
) -> Result<StatementResult, SqlError> {
    // Resolve the table so the index lookup is scoped to it (index names are
    // per-table; two tables may share an index name).
    let table = resolve_table(storage, db_id, &drop.table.value)
        .ok_or_else(|| SqlError::invalid_object(&drop.table.value).at(drop.table.span))?;
    let existed = storage
        .rel_drop_index(table.database_id, &table.name, &drop.name.value)
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
