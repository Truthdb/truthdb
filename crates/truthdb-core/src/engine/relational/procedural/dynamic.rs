use super::super::prelude::*;

pub(in crate::engine::relational) fn run_exec(
    storage: &Storage,
    exec: &ExecStatement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
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
