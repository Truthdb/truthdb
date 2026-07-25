use super::prelude::*;

/// `CREATE|ALTER TRIGGER <name> ON <table> AFTER <events> AS <body>`: registers
/// an AFTER DML trigger as a catalog object attached to its target table.
pub(super) fn exec_create_trigger(
    storage: &Storage,
    db_id: u32,
    create: &CreateTrigger,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE TRIGGER", &create.name)?;
    // The target must be an existing base table (not a view/procedure/function/
    // trigger). SQL Server 4929-class.
    let target = resolve_table(storage, db_id, &create.target.value)
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
    // A table may have at most one INSTEAD OF trigger per action (SQL Server).
    if create.instead_of {
        for def in storage.rel_tables() {
            if let Some(t) = &def.trigger
                && t.is_instead_of
                && t.parent_object_id == target.object_id
                && !def.name.eq_ignore_ascii_case(bare)
                && t.events.iter().any(|e| events.contains(e))
            {
                return Err(SqlError::new(
                    2113,
                    16,
                    1,
                    format!(
                        "Cannot create INSTEAD OF trigger '{bare}' on table '{}' because there is \
                         already an INSTEAD OF trigger '{}' for the same action.",
                        target.name, def.name
                    ),
                )
                .at(create.name.span));
            }
        }
    }
    let trigger = TriggerDef {
        parent_object_id: target.object_id,
        events,
        body: create.body.clone(),
        is_disabled: false,
        is_instead_of: create.instead_of,
    };
    if create.alter {
        match resolve_table(storage, db_id, &create.name.value) {
            Some(def) if def.is_trigger() => {
                storage
                    .rel_alter_trigger(def.database_id, &def.name, trigger)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, db_id, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_trigger(db_id, bare, trigger)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

pub(super) fn exec_drop_trigger(
    storage: &Storage,
    db_id: u32,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &name.value) {
        Some(def) if def.is_trigger() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
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

/// `{ENABLE | DISABLE} TRIGGER {<name> | ALL} ON <table>`: flips the disabled
/// flag on one trigger (or every trigger on the table). A disabled trigger stays
/// in the catalog but does not fire.
pub(super) fn exec_set_trigger_state(
    storage: &Storage,
    db_id: u32,
    trigger: &Option<Name>,
    table: &Name,
    enable: bool,
) -> Result<StatementResult, SqlError> {
    let target = resolve_table(storage, db_id, &table.value)
        .ok_or_else(|| SqlError::invalid_object(&table.value).at(table.span))?;
    if target.is_view() || target.is_procedure() || target.is_function() || target.is_trigger() {
        return Err(SqlError::invalid_object(&table.value).at(table.span));
    }
    let set_one = |def: &TableDef| -> Result<(), SqlError> {
        let mut td = def.trigger.clone().expect("is_trigger");
        td.is_disabled = !enable;
        storage
            .rel_alter_trigger(def.database_id, &def.name, td)
            .map_err(|e| map_storage_err(e, &def.name))
    };
    match trigger {
        Some(name) => {
            let def = resolve_table(storage, db_id, &name.value)
                .filter(|d| d.is_trigger())
                .filter(|d| {
                    d.trigger.as_ref().map(|t| t.parent_object_id) == Some(target.object_id)
                })
                .ok_or_else(|| {
                    SqlError::new(
                        3701,
                        11,
                        5,
                        format!(
                            "Cannot {} the trigger '{}', because it does not exist on table \
                             '{}' or you do not have permission.",
                            if enable { "enable" } else { "disable" },
                            name.value,
                            table.value
                        ),
                    )
                    .at(name.span)
                })?;
            set_one(&def)?;
        }
        None => {
            for def in storage.rel_tables() {
                if def.is_trigger()
                    && def.trigger.as_ref().map(|t| t.parent_object_id) == Some(target.object_id)
                {
                    set_one(&def)?;
                }
            }
        }
    }
    Ok(StatementResult::Done)
}

/// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` / `ALTER LOGIN <name>
/// {ENABLE | DISABLE}`. Logins are server principals in their own namespace
/// (disjoint from schema objects); the password is hashed here (on the worker —
/// CREATE/ALTER LOGIN is rare admin DDL, unlike verification which runs off the
/// worker per connection).
/// Resolves the AFTER triggers to fire for a DML on `target_name` for `event`,
/// plus the target's definition (for the pseudo-table schema). Empty when no
/// trigger exists anywhere (the cheap `rel_has_triggers` gate keeps the common
/// path free) or the target is not a base table.
/// The triggers on `target_name` for `event`, split into AFTER triggers (fired
/// after the DML) and the at-most-one INSTEAD OF trigger (fired in place of it).
pub(super) fn triggers_for(
    storage: &Storage,
    db_id: u32,
    target_name: &str,
    event: catalog::TriggerEvent,
) -> (Option<TableDef>, Vec<TableDef>, Option<TableDef>) {
    if !storage.rel_has_triggers() {
        return (None, Vec::new(), None);
    }
    match resolve_table(storage, db_id, target_name) {
        Some(def)
            if def.trigger.is_none()
                && def.procedure.is_none()
                && def.function.is_none()
                && def.view_query.is_none() =>
        {
            let (instead, after): (Vec<TableDef>, Vec<TableDef>) = storage
                .rel_triggers_for(def.object_id, event)
                .into_iter()
                .partition(|t| t.trigger.as_ref().is_some_and(|d| d.is_instead_of));
            (Some(def), after, instead.into_iter().next())
        }
        _ => (None, Vec::new(), None),
    }
}

/// Runs a DML statement (via `dml`) and fires its AFTER triggers atomically.
/// Under autocommit an implicit transaction is opened so the DML stages rather
/// than commits, so DML + triggers share one transaction (a trigger ROLLBACK
/// undoes the DML) and a trigger that ends the transaction raises 3609.
pub(super) fn run_dml_with_triggers(
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
        updated: images.updated,
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

/// The `(inserted, deleted)` row images an INSTEAD OF trigger's body sees.
pub(super) type TriggerImages = (Vec<Vec<Datum>>, Vec<Vec<Datum>>);

/// Fires an INSTEAD OF trigger in place of the DML: it runs the trigger body over
/// the *proposed* `inserted`/`deleted` images (the base operation and its
/// constraints are bypassed — the body decides what actually happens). Reuses the
/// DML+trigger transaction/firing/error machinery with a DML step that only
/// computes and captures the images, writing nothing.
pub(super) fn run_instead_of(
    storage: &Storage,
    txn_ctx: &mut TxnContext,
    target: &TableDef,
    trigger: TableDef,
    images: impl FnOnce(&EvalContext) -> Result<TriggerImages, SqlError>,
) -> Result<StatementResult, SqlError> {
    run_dml_with_triggers(storage, txn_ctx, target, vec![trigger], |txn_ctx| {
        let eval_ctx = txn_ctx.eval_context();
        let (inserted, deleted) = images(&eval_ctx)?;
        let count = inserted.len().max(deleted.len()) as u64;
        capture_trigger_images(|| (inserted, deleted));
        Ok(StatementResult::RowsAffected(count))
    })
}

/// The `inserted` image an INSTEAD OF INSERT trigger sees: the proposed rows with
/// DEFAULTs applied and the identity column left NULL (the body's own insert
/// generates it). Constraints are not enforced here.
pub(super) fn instead_of_insert_images(
    storage: &Storage,
    insert: &Insert,
    def: &TableDef,
    eval_ctx: &EvalContext,
) -> Result<TriggerImages, SqlError> {
    enforce_object_permission(storage, def, &eval_ctx.security, PermAction::Insert)
        .map_err(|e| e.at(insert.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    let target: Vec<usize> = match &insert.columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for n in names {
                indices.push(
                    column_index(&schema, &n.value)
                        .ok_or_else(|| SqlError::invalid_column(&n.value).at(n.span))?,
                );
            }
            indices
        }
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };
    let input_rows = insert_input_rows(storage, &insert.source, target.len(), eval_ctx)?;
    let mut inserted = Vec::with_capacity(input_rows.len());
    for input in &input_rows {
        let mut values = vec![Datum::Null; ncols];
        for (position, sql_value) in target.iter().zip(input) {
            let column = &schema.columns[*position];
            values[*position] = value::sql_to_datum(sql_value, &column.column_type, &column.name)?;
        }
        for (index, column) in schema.columns.iter().enumerate() {
            if !values[index].is_null() || target.contains(&index) || Some(index) == identity_col {
                continue;
            }
            if let Some(text) = def.default_for(index) {
                let sql_value = eval_default(text, eval_ctx)?;
                values[index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
            }
        }
        inserted.push(values);
    }
    capture_trigger_updated((0..ncols).collect());
    Ok((inserted, Vec::new()))
}

/// The (`inserted` = post-update, `deleted` = pre-update) images an INSTEAD OF
/// UPDATE trigger sees for the rows matching the WHERE clause. Constraints are
/// not enforced here.
pub(super) fn instead_of_update_images(
    storage: &Storage,
    update: &Update,
    def: &TableDef,
    eval_ctx: &EvalContext,
) -> Result<TriggerImages, SqlError> {
    enforce_object_permission(storage, def, &eval_ctx.security, PermAction::Update)
        .map_err(|e| e.at(update.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let mut assignments: Vec<(usize, &Expr)> = Vec::with_capacity(update.assignments.len());
    for a in &update.assignments {
        let index = column_index(&schema, &a.column.value)
            .ok_or_else(|| SqlError::invalid_column(&a.column.value).at(a.column.span))?;
        assignments.push((index, &a.value));
    }
    let mut old_rows = Vec::new();
    let mut new_rows = Vec::new();
    for row in storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?
    {
        check_cancelled()?;
        if !predicate_true(&update.where_clause, &row, &types, &resolver, eval_ctx)? {
            continue;
        }
        let old_scope = row_values(&row, &types);
        let mut new_row = row.clone();
        for (index, expr) in &assignments {
            let column = &schema.columns[*index];
            let value = eval::eval(expr, &old_scope, &resolver, eval_ctx)?;
            new_row[*index] = value::sql_to_datum(&value, &column.column_type, &column.name)?;
        }
        old_rows.push(row);
        new_rows.push(new_row);
    }
    capture_trigger_updated(assignments.iter().map(|(i, _)| *i).collect());
    Ok((new_rows, old_rows))
}

/// The `deleted` image an INSTEAD OF DELETE trigger sees: the rows matching the
/// WHERE clause (none are actually removed).
pub(super) fn instead_of_delete_images(
    storage: &Storage,
    delete: &Delete,
    def: &TableDef,
    eval_ctx: &EvalContext,
) -> Result<TriggerImages, SqlError> {
    enforce_object_permission(storage, def, &eval_ctx.security, PermAction::Delete)
        .map_err(|e| e.at(delete.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let mut deleted = Vec::new();
    for row in storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?
    {
        check_cancelled()?;
        if predicate_true(&delete.where_clause, &row, &types, &resolver, eval_ctx)? {
            deleted.push(row);
        }
    }
    Ok((Vec::new(), deleted))
}

/// Fires one trigger body: parses it, runs it in the firing statement's
/// transaction (procedure posture — shared txn, fresh variable scope) with the
/// `inserted`/`deleted` view armed, bounded by the nesting cap. Direct
/// self-recursion is suppressed (recursive triggers OFF).
pub(super) fn fire_one_trigger(
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
        flow.and_then(end_of_scope)
            .and_then(|()| match run.last_error.take() {
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
