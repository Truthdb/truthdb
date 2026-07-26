use super::super::prelude::*;

// ---- ALTER TABLE --------------------------------------------------------

/// `ALTER DATABASE {name | CURRENT} SET READ_COMMITTED_SNAPSHOT /
/// ALLOW_SNAPSHOT_ISOLATION {ON|OFF}`. The batch's Database X lock has
/// quiesced the store: no snapshot is live, no writer is mid-transaction.
pub(in crate::engine::relational) fn exec_alter_database(
    storage: &Storage,
    alter: &AlterDatabase,
    txn_ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if let Some(name) = &alter.name
        && storage.rel_database_id_by_name(&name.value).is_none()
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
    // FAILOVER (standby promotion) is offline-only, like RESTORE DATABASE: the
    // in-flight-transaction undo and the epoch bump run against a stopped
    // server. Checked before anything else — the pointer to the CLI is the
    // whole answer.
    if alter
        .options
        .iter()
        .any(|(option, _)| *option == DatabaseOption::Failover)
    {
        return Err(SqlError::new(
            3101,
            16,
            1,
            "Exclusive access could not be obtained because the database is in use. TruthDB \
             promotes a standby offline: stop the server and run `truthdb-cli promote`."
                .to_string(),
        ));
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
            // Returned as 3101 above, before this loop runs.
            DatabaseOption::Failover => unreachable!("failover is rejected before options apply"),
        }
    }
    storage
        .rel_set_db_options(rcsi, allow_snapshot, recovery_full)
        .map_err(|err| map_storage_err(err, &txn_ctx.database))?;
    Ok(StatementResult::Done)
}

pub(in crate::engine::relational) fn exec_alter_table(
    storage: &Storage,
    db_id: u32,
    alter: &AlterTable,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, db_id, &alter.table.value)
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
pub(in crate::engine::relational) fn alter_add_foreign_key(
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
        let parent = resolve_table(storage, def.database_id, &fk.parent.value)
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
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        if let Some(key) = fk_key(&new_def, row)
            && !fk_parent_exists(storage, &new_def, &key, def, &rows)?
        {
            return Err(fk_child_violation(
                &database_name_of(storage, def.database_id),
                &new_def.name,
                "ALTER TABLE",
                &new_def.parent,
            ));
        }
    }

    let mut fks = def.foreign_keys.clone();
    fks.push(new_def);
    storage
        .rel_set_foreign_keys(def.database_id, &def.name, fks)
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
pub(in crate::engine::relational) fn alter_add_column(
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
            .rel_scan_slice(
                def.database_id,
                &def.name,
                ScanCursor::start(),
                1,
                None,
                &mut probe,
            )
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
        .rel_alter_add_column(
            def.database_id,
            &def.name,
            bound,
            column.default.clone(),
            fill,
        )
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] CHECK (expr)`. Validates the new
/// constraint against every existing row (SQL Server's default WITH CHECK); a
/// violating row is error 547 and the constraint is not added.
pub(in crate::engine::relational) fn alter_add_check(
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
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        let scope = row_values(row, &types);
        enforce_checks(
            storage,
            &compiled,
            &scope,
            &resolver,
            eval_ctx,
            "ALTER TABLE",
            &database_name_of(storage, def.database_id),
            &def.name,
        )?;
    }

    let mut checks = def.check_constraints.clone();
    checks.push(new_def);
    storage
        .rel_set_check_constraints(def.database_id, &def.name, checks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... DROP CONSTRAINT name`. Removes a CHECK or FOREIGN KEY
/// constraint by name (case-insensitive); an unknown name is error 3728.
pub(in crate::engine::relational) fn alter_drop_constraint(
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
            .rel_set_check_constraints(def.database_id, &def.name, checks)
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
            .rel_set_foreign_keys(def.database_id, &def.name, fks)
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
