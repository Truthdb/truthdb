use super::super::prelude::*;

pub(in crate::engine::relational) fn run_exec(
    storage: &RelStorage,
    exec: &ExecStatement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
    // Another engine's built-in procedure, ahead of the catalog.
    let lowered = exec.proc.value.to_ascii_lowercase();
    if let Some(provider) = storage.external_procedure(&lowered) {
        return run_external_procedure(storage, provider.as_ref(), &lowered, exec, txn_ctx, run);
    }
    if !strip_schema(&exec.proc.value).eq_ignore_ascii_case("sp_executesql") {
        // A user procedure, if the catalog has one; 2812 otherwise.
        if let Some(def) = resolve_table(storage, txn_ctx.database_id(), &exec.proc.value)
            && def.is_procedure()
        {
            enforce_object_permission(storage, &def, &txn_ctx.security, PermAction::Execute)
                .map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e.at(exec.proc.span))))?;
            return run_user_procedure(storage, exec, &def, txn_ctx, run, in_try);
        }
        let error = SqlError::new(
            2812,
            16,
            62,
            format!("Could not find stored procedure '{}'.", exec.proc.value),
        )
        .at(exec.proc.span);
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    }
    if exec.return_var.is_some() {
        let error = SqlError::new(
            179,
            16,
            1,
            "Cannot capture a return status from sp_executesql.",
        );
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    }
    let eval_ctx = txn_ctx.eval_context();
    let mut positional = Vec::new();
    let mut named: Vec<(String, SqlValue)> = Vec::new();
    for arg in &exec.args {
        let value = eval_constant(&arg.value, &eval_ctx)
            .map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e)))?;
        match &arg.name {
            Some(n) => named.push((n.value.clone(), value)),
            None => positional.push(value),
        }
    }
    let take_named = |named: &mut Vec<(String, SqlValue)>, keys: &[&str]| -> Option<SqlValue> {
        let index = named
            .iter()
            .position(|(n, _)| keys.iter().any(|k| n.eq_ignore_ascii_case(k)))?;
        Some(named.remove(index).1)
    };
    let mut positional = positional.into_iter();
    let stmt = match take_named(&mut named, &["stmt", "statement"]).or_else(|| positional.next()) {
        Some(value) => value,
        None => {
            let error = SqlError::new(
                214,
                16,
                2,
                "Procedure expects parameter '@statement' of type 'ntext/nchar/nvarchar'.",
            );
            return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
        }
    };
    let SqlValue::Str(sql) = stmt else {
        let error = SqlError::new(
            214,
            16,
            2,
            "Procedure expects parameter '@statement' of type 'ntext/nchar/nvarchar'.",
        );
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    };
    let decls =
        match take_named(&mut named, &["params", "parameters"]).or_else(|| positional.next()) {
            Some(SqlValue::Str(d)) => d,
            Some(SqlValue::Null) | None => String::new(),
            Some(_) => {
                let error = SqlError::new(
                    214,
                    16,
                    3,
                    "Procedure expects parameter '@params' of type 'ntext/nchar/nvarchar'.",
                );
                return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
            }
        };
    // Bind values: named ones by their own names, positional ones from the
    // declaration list, exactly as the RPC path binds unnamed wire values.
    let names = decl_names(&decls);
    let mut seeded: Vec<(String, SqlValue)> = named;
    for (i, value) in positional.enumerate() {
        let Some(name) = names.get(i) else {
            let error = SqlError::new(
                8144,
                16,
                2,
                "Procedure or function has too many arguments specified.",
            );
            return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
        };
        seeded.push((name.clone(), value));
    }
    let statements =
        truthdb_sql::parse(&sql).map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e)))?;

    // The inner batch is its own variable scope, on the shared transaction —
    // and SET options revert at scope exit, as SQL Server reverts them: an
    // inner SET (XACT_ABORT, ISOLATION LEVEL, SHOWPLAN) must not outlive the
    // EXEC, or a post-EXEC statement would run under an isolation the up-front
    // lock analysis never saw.
    let outer_database = txn_ctx.database.clone();
    let outer_database_id = txn_ctx.database_id();
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    let outer_xact_abort = txn_ctx.xact_abort;
    let outer_nocount = txn_ctx.nocount;
    let outer_isolation = txn_ctx.isolation;
    let outer_showplan = txn_ctx.showplan_text;
    for (name, value) in seeded {
        let key = name.trim_start_matches('@').to_ascii_lowercase();
        let column_type = value::infer_type(std::slice::from_ref(&value));
        txn_ctx.variables.insert(key, (column_type, value));
    }
    // Dynamic SQL run from a trigger body does not see inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // Dynamic SQL does NOT ownership-chain: reset the chaining depth so its
    // statements are permission-checked as the caller's own, even when this
    // sp_executesql sits inside a procedure body.
    let _dynamic = DynamicScope::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        let error = SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        );
        Err(ExecError::Own(doom_per_rule(txn_ctx, error)))
    } else {
        // An inner RETURN exits the inner batch only (Break/Continue cannot
        // escape — the inner parse rejects them, its own 135/136 scope). An
        // error crossing out already carries every decision: dooming, and by
        // crossing at all, termination of the whole nest.
        run_block(storage, &statements, txn_ctx, run, in_try)
            .and_then(end_of_scope)
            .map_err(ExecError::Inner)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    txn_ctx.xact_abort = outer_xact_abort;
    txn_ctx.nocount = outer_nocount;
    txn_ctx.isolation = outer_isolation;
    txn_ctx.showplan_text = outer_showplan;
    // A USE inside the dynamic batch is scoped to it (SQL Server's rule):
    // the caller's database context comes back at scope exit — and with it,
    // agreement with the lock analysis that resolved the OUTER batch.
    txn_ctx.set_current_database(outer_database, outer_database_id);
    result
}

/// `EXEC` of another engine's built-in procedure: its arguments evaluated in
/// the caller's scope, its result set (if any) emitted as a statement's, its
/// return status 0.
fn run_external_procedure(
    storage: &RelStorage,
    provider: &dyn crate::engine::ExternalProcedures,
    name: &str,
    exec: &ExecStatement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<(), ExecError> {
    let own = |txn_ctx: &mut TxnContext, error: SqlError| -> ExecError {
        ExecError::Own(doom_per_rule(txn_ctx, error.at(exec.proc.span)))
    };
    let eval_ctx = txn_ctx.eval_context();
    let mut args: Vec<(Option<String>, SqlValue)> = Vec::new();
    for (arg_index, arg) in exec.args.iter().enumerate() {
        if arg.name.is_none() && args.iter().any(|(name, _)| name.is_some()) {
            let error = SqlError::new(
                119,
                15,
                1,
                format!(
                    "Must pass parameter number {} and subsequent parameters as '@name = value'. \
                     After the form '@name = value' has been used, all subsequent parameters must \
                     be passed in the form '@name = value'.",
                    arg_index + 1
                ),
            );
            return Err(own(txn_ctx, error));
        }
        // None of these procedures has an OUTPUT parameter.
        if arg.output {
            let error = SqlError::new(
                8162,
                16,
                2,
                format!(
                    "The formal parameter \"{}\" was not declared as an OUTPUT parameter, but the \
                     actual parameter passed in requested output.",
                    arg.name
                        .as_ref()
                        .map_or_else(|| format!("#{}", arg_index + 1), |n| n.value.clone())
                ),
            );
            return Err(own(txn_ctx, error));
        }
        let value = eval_constant(&arg.value, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
        let arg_name = arg
            .name
            .as_ref()
            .map(|n| n.value.trim_start_matches('@').to_ascii_lowercase());
        args.push((arg_name, value));
    }
    if let Some(rc) = &exec.return_var
        && !txn_ctx.variables.contains_key(rc)
    {
        let error = undeclared_variable_err(rc);
        return Err(own(txn_ctx, error));
    }
    let caller = crate::engine::ExternalCaller {
        server_admin: txn_ctx.security.server_admin,
    };
    let result = provider
        .run(name, &args, caller)
        .map_err(|e| own(txn_ctx, e))?;
    let in_transaction = txn_ctx.in_txn();
    let count = result.as_ref().map_or(0, |rowset| rowset.rows.len() as u64);
    if let Some(rowset) = result {
        run.flush(storage).map_err(ExecError::Own)?;
        run.open_rowset(rowset.columns);
        run.rows(rowset.rows);
        run.done(
            (!txn_ctx.nocount).then_some(count),
            in_transaction,
            DoneCommand::Select,
        );
    }
    txn_ctx.rowcount = count as i64;
    if let Some(rc) = &exec.return_var {
        txn_ctx
            .variables
            .insert(rc.clone(), (ColumnType::Int, SqlValue::Int(0)));
    }
    Ok(())
}
