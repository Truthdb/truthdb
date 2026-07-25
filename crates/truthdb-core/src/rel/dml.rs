use super::prelude::*;

// ---- INSERT -------------------------------------------------------------

pub(super) fn exec_insert(
    storage: &Storage,
    insert: &Insert,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<(StatementResult, Option<i64>), SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &insert.table.value)
        .ok_or_else(|| SqlError::invalid_object(&insert.table.value).at(insert.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Insert)
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
            .rel_reserve_identity(def.database_id, &def.name, input_rows.len())
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
                storage,
                &checks,
                &scope,
                &check_resolver,
                eval_ctx,
                "INSERT",
                &database_name_of(storage, def.database_id),
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
    // when a capture is armed — the no-trigger path clones nothing). Every column
    // counts as updated for an INSERT (SQL Server's UPDATE() semantics).
    capture_trigger_images(|| (rows.clone(), Vec::new()));
    capture_trigger_updated((0..ncols).collect());
    let inserted = rows.len() as u64;
    storage
        .rel_insert_many(def.database_id, &def.name, rows, scope)
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
pub(super) fn exec_insert_table_var(
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
pub(super) fn must_declare_table_var(name: &str) -> SqlError {
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
pub(super) fn insert_input_rows(
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
pub(super) fn eval_default(text: &str, eval_ctx: &EvalContext) -> Result<SqlValue, SqlError> {
    let expr = truthdb_sql::parse_expr(text)?;
    eval_constant(&expr, eval_ctx)
}

/// Coerces a generated identity value to its column's integer type, erroring
/// on overflow.
pub(super) fn identity_datum(column_type: &ColumnType, v: i64) -> Result<Datum, SqlError> {
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
pub(super) fn scan_located_for_dml(
    storage: &Storage,
    def: &TableDef,
) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, SqlError> {
    match current_snapshot() {
        Some(snap) => storage
            .rel_scan_located_snapshot(def.database_id, &def.name, snap)
            .map_err(|e| map_storage_err(e, &def.name)),
        None => Ok(storage
            .rel_scan_located(def.database_id, &def.name)
            .map_err(|e| map_storage_err(e, &def.name))?
            .into_iter()
            .map(|(locator, row)| (locator, row, false))
            .collect()),
    }
}

/// SQL Server 3960: a SNAPSHOT transaction tried to write a row a later
/// committed transaction already changed. The whole transaction is rolled
/// back (see `exec_statement`'s 3960 handling), as SQL Server does.
pub(super) fn update_conflict_error(table: &str, database: &str) -> SqlError {
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

pub(super) fn exec_update(
    storage: &Storage,
    update: &Update,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &update.table.value)
        .ok_or_else(|| SqlError::invalid_object(&update.table.value).at(update.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Update)
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
            enforce_checks(
                storage,
                &checks,
                &scope,
                &resolver,
                eval_ctx,
                "UPDATE",
                &database_name_of(storage, def.database_id),
                &def.name,
            )?;
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
            .rel_scan(def.database_id, &def.name)
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
                    return Err(fk_child_violation(
                        &database_name_of(storage, def.database_id),
                        &fk.name,
                        "UPDATE",
                        &fk.parent,
                    ));
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
    capture_trigger_updated(assignments.iter().map(|(i, _)| *i).collect());
    let count = storage
        .rel_update_located(def.database_id, &def.name, updates, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

pub(super) fn exec_delete(
    storage: &Storage,
    delete: &Delete,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &delete.table.value)
        .ok_or_else(|| SqlError::invalid_object(&delete.table.value).at(delete.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Delete)
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
        .rel_delete_located(def.database_id, &def.name, targets, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

/// Resolver over a single table's schema columns, carrying per-column collation.
/// UPDATE/DELETE/CHECK predicate evaluation must go through this (not a bare
/// `Vec<String>`, whose `ColumnResolver::collation` reports the case-insensitive
/// default for *every* column) so an explicit `_CS`/`_BIN` column compares
/// case-sensitively — otherwise a `DELETE ... WHERE cs_col = 'abc'` would fold
/// case and remove case-variant rows it must keep.
pub(super) struct SchemaScope<'a> {
    pub(super) schema: &'a Schema,
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

pub(super) fn schema_types(schema: &Schema) -> Vec<ColumnType> {
    schema.columns.iter().map(|c| c.column_type).collect()
}

/// Evaluates an optional WHERE predicate against a row. Absent WHERE matches
/// all rows; a NULL/UNKNOWN result does not match; a non-boolean predicate is
/// error 4145 (same rule as SELECT).
pub(super) fn predicate_true(
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
