use super::super::prelude::*;

/// Executes a user stored procedure: binds arguments to declared parameters
/// (positional and named, defaults filling gaps, OUTPUT validated), runs the
/// stored body text under a fresh variable scope with SET options reverting
/// at exit (the sp_executesql posture), captures the RETURN status into
/// `EXEC @rc =`, and copies OUTPUT parameters back — both only when the body
/// completes (SQL Server skips them when execution aborts).
pub(in crate::engine::relational) fn run_user_procedure(
    storage: &Storage,
    exec: &ExecStatement,
    def: &TableDef,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
    let procedure = def.procedure.as_ref().expect("checked by the caller");
    let own = |txn_ctx: &mut TxnContext, error: SqlError| -> ExecError {
        ExecError::Own(doom_per_rule(txn_ctx, error))
    };
    // Evaluate arguments in the CALLER's scope.
    let eval_ctx = txn_ctx.eval_context();
    let mut positional = Vec::new();
    let mut named: Vec<(String, SqlValue, bool, Option<String>)> = Vec::new();
    let mut positional_meta: Vec<(bool, Option<String>)> = Vec::new();
    for (arg_index, arg) in exec.args.iter().enumerate() {
        // Once an argument is named, the rest must be (SQL Server 119) —
        // silently continuing would bind positions past the named one.
        if arg.name.is_none() && !named.is_empty() {
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
        // An OUTPUT argument must be a bare variable (it receives a value).
        let arg_var = match &arg.value.kind {
            ExprKind::LocalVar(name) => Some(name.clone()),
            _ => None,
        };
        if arg.output && arg_var.is_none() {
            let error = SqlError::new(
                179,
                16,
                1,
                "Cannot use the OUTPUT option when passing a constant to a stored procedure.",
            );
            return Err(own(txn_ctx, error));
        }
        let value = eval_constant(&arg.value, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
        match &arg.name {
            Some(n) => {
                let key = n.value.trim_start_matches('@').to_ascii_lowercase();
                // A parameter supplied twice (named twice, or named on top
                // of a positional binding) is an error, not a silent pick.
                let position_of = |name: &str| procedure.params.iter().position(|p| p.name == name);
                let already_positional =
                    position_of(&key).is_some_and(|index| index < positional.len());
                if already_positional || named.iter().any(|(n, ..)| *n == key) {
                    let error = SqlError::new(
                        8143,
                        16,
                        1,
                        format!(
                            "Parameter '@{key}' was supplied multiple times for procedure {}.",
                            def.name
                        ),
                    );
                    return Err(own(txn_ctx, error));
                }
                named.push((key, value, arg.output, arg_var));
            }
            None => {
                positional.push(value);
                positional_meta.push((arg.output, arg_var));
            }
        }
    }
    // `EXEC @rc = p`: the status variable must already be declared (137).
    if let Some(rc) = &exec.return_var
        && !txn_ctx.variables.contains_key(rc)
    {
        let error = undeclared_variable_err(rc);
        return Err(own(txn_ctx, error));
    }
    if positional.len() > procedure.params.len() {
        let error = SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        );
        return Err(own(txn_ctx, error));
    }
    // Named arguments that match no declared parameter fail before any
    // binding (8145 precedes 201, as SQL Server orders it).
    for (name, ..) in &named {
        if !procedure.params.iter().any(|p| p.name == *name) {
            let error = SqlError::new(
                8145,
                16,
                2,
                format!("@{name} is not a parameter for procedure {}.", def.name),
            );
            return Err(own(txn_ctx, error));
        }
    }
    // Bind: positional in declaration order, then named by name, then
    // defaults; a missing non-default parameter is 201. OUTPUT copy-back
    // targets (param name -> caller variable) are collected as we bind.
    let mut bound: Vec<(String, ColumnType, SqlValue)> = Vec::new();
    let mut copy_back: Vec<(String, String)> = Vec::new();
    for (index, param) in procedure.params.iter().enumerate() {
        let column_type = ColumnType::parse(&param.type_spec).map_err(|e| {
            let error = SqlError::message_only(245, e.to_string());
            own(txn_ctx, error)
        })?;
        let supplied = if index < positional.len() {
            let (output, arg_var) = positional_meta[index].clone();
            Some((positional[index].clone(), output, arg_var))
        } else {
            named
                .iter()
                .find(|(n, ..)| *n == param.name)
                .map(|(_, v, output, arg_var)| (v.clone(), *output, arg_var.clone()))
        };
        let coerce = |value: SqlValue| -> Result<SqlValue, SqlError> {
            let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
            Ok(value::datum_to_sql(&datum, &column_type))
        };
        let value = match supplied {
            Some((value, output, arg_var)) => {
                if output {
                    if !param.output {
                        let error = SqlError::new(
                            8162,
                            16,
                            2,
                            format!(
                                "The formal parameter \"@{}\" was not declared as an OUTPUT \
                                 parameter, but the actual parameter passed in requested output.",
                                param.name
                            ),
                        );
                        return Err(own(txn_ctx, error));
                    }
                    copy_back.push((
                        param.name.clone(),
                        arg_var.expect("validated: OUTPUT arguments are variables"),
                    ));
                }
                // Bind-time conversion to the DECLARED type, as SQL Server
                // converts (or errors) at the call — without it a string
                // argument flows into an INT parameter mistagged.
                coerce(value).map_err(|e| own(txn_ctx, e))?
            }
            None => match &param.default {
                Some(text) => {
                    let expr = truthdb_sql::parse_expr(text).map_err(|e| own(txn_ctx, e))?;
                    let value = eval_constant(&expr, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
                    coerce(value).map_err(|e| own(txn_ctx, e))?
                }
                None => {
                    let error = SqlError::new(
                        201,
                        16,
                        4,
                        format!(
                            "Procedure or function '{}' expects parameter '@{}', which was not \
                             supplied.",
                            def.name, param.name
                        ),
                    );
                    return Err(own(txn_ctx, error));
                }
            },
        };
        bound.push((param.name.clone(), column_type, value));
    }
    // The stored body parses under the in-procedure grammar.
    let statements =
        truthdb_sql::parse_procedure_body(&procedure.body).map_err(|e| own(txn_ctx, e))?;

    // Fresh scope, SET options reverting at exit — the sp_executesql shape.
    let outer_database = txn_ctx.database.clone();
    let outer_database_id = txn_ctx.database_id();
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    let outer_xact_abort = txn_ctx.xact_abort;
    let outer_nocount = txn_ctx.nocount;
    let outer_isolation = txn_ctx.isolation;
    let outer_showplan = txn_ctx.showplan_text;
    for (name, column_type, value) in bound {
        txn_ctx.variables.insert(name, (column_type, value));
    }
    // The body's unqualified names resolve in the procedure's HOME database,
    // not the caller's (SQL Server's rule). The body cannot USE (parser 154),
    // so this holds for its whole extent; the caller's context returns below.
    txn_ctx.set_current_database(database_name_of(storage, def.database_id), def.database_id);
    txn_ctx.proc_stack.push(def.name.clone());
    txn_ctx.proc_return = None;
    // A procedure called from a trigger body does NOT see the trigger's
    // inserted/deleted (they are visible only in the trigger's own statements).
    let _trigger_shadow = TriggerScope::clear();
    // A procedure body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
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
        run_block(storage, &statements, txn_ctx, run, in_try)
            .and_then(end_of_scope)
            .map_err(ExecError::Inner)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.proc_stack.pop();
    // Capture OUTPUT values from the inner scope BEFORE restoring the outer.
    let output_values: Vec<(String, (ColumnType, SqlValue))> = copy_back
        .iter()
        .filter_map(|(param, var)| {
            txn_ctx
                .variables
                .get(param)
                .map(|slot| (var.clone(), slot.clone()))
        })
        .collect();
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    txn_ctx.xact_abort = outer_xact_abort;
    txn_ctx.nocount = outer_nocount;
    txn_ctx.isolation = outer_isolation;
    txn_ctx.showplan_text = outer_showplan;
    txn_ctx.set_current_database(outer_database, outer_database_id);
    let return_status = txn_ctx.proc_return.take().unwrap_or(0);
    if result.is_ok() {
        // OUTPUT copy-back and the return status land only when the body
        // completed (SQL Server skips both when execution aborts).
        for (var, slot) in output_values {
            txn_ctx.variables.insert(var, slot);
        }
        if let Some(rc) = &exec.return_var {
            txn_ctx
                .variables
                .insert(rc.clone(), (ColumnType::Int, SqlValue::Int(return_status)));
        }
    }
    result
}

/// Runs a scalar user-defined function's body once with `arg_values` bound to
/// its parameters, returning the value its `RETURN` produced, coerced to the
/// declared return type.
///
/// The body runs in an isolated throwaway context — only the parameters are
/// visible (SQL Server functions do not see caller locals), no transaction is
/// open (functions are side-effect-free), and any table reads observe the
/// caller's ambient snapshot on this thread. Nesting shares the `EXEC_DEPTH`
/// budget (217 at depth 32). Because the context has no transaction, an error in
/// the body always terminates the function (there is no XACT_ABORT-OFF continue
/// path), which is exactly the SQL Server posture: a function error aborts the
/// statement that called it.
pub(in crate::engine::relational) fn run_user_scalar_function(
    storage: &Storage,
    def: &TableDef,
    arg_values: &[SqlValue],
    caller: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let function = def.function.as_ref().expect("checked by the caller");
    // The caller (resolve_scalar_function) only routes scalar functions here.
    let FunctionReturns::Scalar { type_spec, body } = &function.returns else {
        return Err(function_not_a_table(&def.name));
    };
    // Invoking a scalar function needs EXECUTE permission.
    enforce_object_permission(storage, def, &caller.security, PermAction::Execute)?;
    if arg_values.len() < function.params.len() {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {}.",
                def.name
            ),
        ));
    }
    if arg_values.len() > function.params.len() {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        ));
    }
    let return_type =
        ColumnType::parse(type_spec).map_err(|e| SqlError::message_only(245, e.to_string()))?;
    // Fresh scope with only the parameters; the caller's session identity is
    // carried so DB_NAME()/SUSER_SNAME()/USER_NAME()/@@SPID and role membership
    // resolve inside the body. The sids are left 0 (the body reuses the caller's
    // already-computed role set rather than re-resolving membership).
    let mut txn_ctx = TxnContext::default();
    // The body's unqualified names resolve in the FUNCTION's home database
    // (matching collect_read_lock_ids); DB_ID/DB_NAME keep working via the
    // caller's databases snapshot.
    txn_ctx.set_session_identity(
        database_name_of(storage, def.database_id),
        def.database_id,
        caller.login.clone(),
        caller.spid,
        caller.user.clone(),
        0,
        0,
    );
    txn_ctx.databases_snapshot = caller.databases.clone();
    txn_ctx.session_server_roles = caller.server_roles.clone();
    txn_ctx.session_db_roles = caller.db_roles.clone();
    txn_ctx.security = caller.security.clone();
    for (param, value) in function.params.iter().zip(arg_values) {
        let column_type = ColumnType::parse(&param.type_spec)
            .map_err(|e| SqlError::message_only(245, e.to_string()))?;
        let datum = value::sql_to_datum(value, &column_type, &param.name)?;
        let coerced = value::datum_to_sql(&datum, &column_type);
        txn_ctx
            .variables
            .insert(param.name.clone(), (column_type, coerced));
    }
    let statements = truthdb_sql::parse_function_body(body)?;
    // A scalar function called from a trigger body does not see inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // A function body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        Err(SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        ))
    } else {
        let mut emitter = DiscardEmitter;
        let mut run = BatchRun {
            emitter: &mut emitter,
            deferred: Vec::new(),
            rowset_open: false,
            durability_failed: false,
            committed: false,
            last_error: None,
            function_return_type: Some(return_type),
        };
        run_block(storage, &statements, &mut txn_ctx, &mut run, false).and_then(end_of_scope)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    result?;
    // The body ends in `RETURN <expr>` (enforced at CREATE, 455), so a completed
    // body always set `func_return`.
    txn_ctx.func_return.take().ok_or_else(|| {
        SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )
    })
}
