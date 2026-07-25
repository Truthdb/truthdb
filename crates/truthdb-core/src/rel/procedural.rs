use super::prelude::*;

/// The inner SQL text of an `EXEC sp_executesql N'...'` whose statement
/// argument is a string LITERAL — the analyzable case. `None` for any other
/// procedure or a non-literal statement argument.
/// Runs a statement list, recursing into `TRY`/`CATCH`. `in_try` is true while
/// executing inside a `TRY` block, where a statement error transfers control to
/// the matching `CATCH` (returned as `Err`) instead of applying the normal
/// batch policy. Returns `Err` when the enclosing context must stop: a cancel,
/// an error that propagates to a `CATCH`, or a dooming/terminating error at the
/// top level.
pub(super) fn exec_literal_sql(exec: &ExecStatement) -> Option<String> {
    if !strip_schema(&exec.proc.value).eq_ignore_ascii_case("sp_executesql") {
        return None;
    }
    let stmt = exec
        .args
        .iter()
        .find(|a| {
            a.name.as_ref().is_some_and(|n| {
                n.value.eq_ignore_ascii_case("stmt") || n.value.eq_ignore_ascii_case("statement")
            })
        })
        .or_else(|| exec.args.first().filter(|a| a.name.is_none()))?;
    match &stmt.value.kind {
        ExprKind::Str(text) => Some(text.clone()),
        _ => None,
    }
}

thread_local! {
    /// Nesting depth of EXEC inner batches on this worker (SQL Server caps
    /// procedure nesting at 32, error 217).
    pub(super) static EXEC_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Ownership-chaining depth for object-permission checks: how many OWNED
    /// stored-object bodies (procedure, scalar UDF, multi-statement TVF, trigger)
    /// enclose the current statement. Distinct from [`EXEC_DEPTH`] because
    /// `sp_executesql` bumps that but does NOT chain — dynamic SQL runs in the
    /// caller's own permission context. Permission checks fire only where this
    /// (and `VIEW_DEPTH`) is 0.
    pub(super) static CHAIN_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard entered when running an OWNED stored-object body (procedure,
/// scalar UDF, multi-statement TVF, trigger): it raises the ownership-chaining
/// depth so the body's object reads are not re-permission-checked (the caller's
/// permission on the object suffices — single `dbo` owner).
pub(super) struct ChainGuard;

impl ChainGuard {
    pub(super) fn enter() -> Self {
        CHAIN_DEPTH.with(|d| d.set(d.get() + 1));
        ChainGuard
    }
}

impl Drop for ChainGuard {
    fn drop(&mut self) {
        CHAIN_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// RAII guard entered when running DYNAMIC SQL (`sp_executesql`): it RESETS the
/// ownership-chaining depth to 0 for the duration, then restores it — dynamic
/// SQL never chains, so its statements are permission-checked as the caller's
/// own, even when the `sp_executesql` call sits inside a procedure body.
pub(super) struct DynamicScope(u32);

impl DynamicScope {
    fn enter() -> Self {
        let saved = CHAIN_DEPTH.with(|d| d.replace(0));
        DynamicScope(saved)
    }
}

impl Drop for DynamicScope {
    fn drop(&mut self) {
        CHAIN_DEPTH.with(|d| d.set(self.0));
    }
}

/// Runs `EXEC sp_executesql @stmt [, @params, values...]`: evaluates the
/// arguments against the CURRENT variables, then runs the inner text as its
/// own batch scope — fresh variables seeded from the declared parameters
/// (inner DECLAREs do not leak out; outer variables are not visible inside),
/// sharing the transaction context. Each inner statement emits its own
/// events, exactly like a top-level statement. Any other procedure answers
/// 2812, the same as the RPC path.
/// An EXEC failure, tagged by ORIGIN — the fact the EXEC arm needs and must
/// not guess: `run_exec`'s own validation/depth errors are statement-scope at
/// the EXEC site, while an error that crossed out of the inner batch already
/// terminated it (batch-abort scope is the whole nest).
pub(super) enum ExecError {
    Own(SqlError),
    Inner(SqlError),
}

/// Applies the standard doom rule to an error raised outside any statement's
/// own execution — `run_exec`'s validation and depth errors, which no inner
/// `run_block` arm will see. The decision is made here, at the source, so the
/// TRY boundary never has to re-derive it (it cannot know the error's origin).
pub(super) fn doom_per_rule(txn_ctx: &mut TxnContext, error: SqlError) -> SqlError {
    if txn_ctx.in_txn() && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY) {
        txn_ctx.doomed = true;
    }
    error
}

/// Executes a user stored procedure: binds arguments to declared parameters
/// (positional and named, defaults filling gaps, OUTPUT validated), runs the
/// stored body text under a fresh variable scope with SET options reverting
/// at exit (the sp_executesql posture), captures the RETURN status into
/// `EXEC @rc =`, and copies OUTPUT parameters back — both only when the body
/// completes (SQL Server skips them when execution aborts).
pub(super) fn run_user_procedure(
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
pub(super) fn run_user_scalar_function(
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

pub(super) fn run_exec(
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

/// The ONE place a failed statement's fate is decided — continue the batch
/// (`Ok(())`), or end it (`Err`, dooming already applied). The doom decision
/// needs the statement's KIND (RAISERROR is exempt from XACT_ABORT; THROW is
/// batch-terminating without dooming), so every decide-now error site funnels
/// here: the generic statement arm and IF/WHILE condition failures. (EXEC
/// boundary errors do NOT — theirs were decided at the source, in the inner
/// `run_block` or `doom_per_rule`.)
pub(super) fn statement_error_ladder(
    statement: &Statement,
    error: SqlError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), SqlError> {
    // A cancelled statement aborts the batch immediately: key on the cancel
    // marker, not any flag, so an Attention landing concurrently with an
    // unrelated failure cannot suppress that failure's dooming. A cancel is
    // not a SQL error, so `@@ERROR` is untouched.
    if error.number == CANCEL_ERROR {
        return Err(error);
    }
    txn_ctx.record_error(error.number);
    // A durability failure wedged the store (a flush inside the statement,
    // e.g. before a snapshot capture): never continue past a lost commit.
    if run.durability_failed {
        return Err(error);
    }
    // Severity >= 20 is fatal to the connection: it bypasses TRY (the
    // TryCatch arm refuses it too), dooms the transaction, and the protocol
    // layer closes the stream after delivering it.
    if error.level >= FATAL_SEVERITY {
        if txn_ctx.in_txn() {
            txn_ctx.doomed = true;
        }
        return Err(error);
    }
    // The doom decision is made HERE, where the failing statement's kind is
    // known — never re-derived at the TRY boundary, which cannot see it.
    // `SET XACT_ABORT` (or severity >= 17) dooms; RAISERROR is exempt by
    // definition (SQL Server: "errors raised by RAISERROR are not affected
    // by SET XACT_ABORT") and never dooms.
    let dooms = !matches!(statement, Statement::RaiseError(_))
        && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY);
    if txn_ctx.in_txn() && dooms {
        txn_ctx.doomed = true;
    }
    // Inside a TRY, the error then transfers to the matching CATCH (which
    // sees XACT_STATE() = -1 when it doomed). The CATCH runs more statements,
    // so a result set this one already started streaming must be closed.
    if in_try {
        run.abort_open_rowset(txn_ctx.in_txn());
        return Err(error);
    }
    // RAISERROR is statement-scope: the batch always continues.
    if matches!(statement, Statement::RaiseError(_)) {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    // THROW always terminates the batch — even when it does not doom the
    // transaction (XACT_ABORT OFF leaves it open and committable later).
    if matches!(statement, Statement::Throw(_)) {
        return Err(error);
    }
    // Other statements: a non-dooming in-transaction error rolls back only
    // the statement and the batch continues; a dooming one ends the batch
    // (only ROLLBACK is then accepted, error 3930). This must stay keyed on the
    // ERROR (its severity / XACT_ABORT), NOT on whether the transaction is
    // already doomed: a doomed transaction still runs a CATCH's reads and
    // statement-terminating errors (division by zero, conversion) so the CATCH
    // can reach `IF XACT_STATE() <> 0 ROLLBACK` — terminating the batch on those
    // would leave the uncommittable transaction open holding its locks.
    if txn_ctx.in_txn() && !dooms {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    Err(error)
}

/// Enters the versioned-read scopes for an IF/WHILE condition that reads
/// tables — the SAME rules a SELECT gets in `exec_statement_streamed`: under
/// RCSI the condition reads its own statement snapshot; under SNAPSHOT
/// isolation it establishes/uses the transaction snapshot and enforces 3952.
/// Without this the condition read holds NEITHER lock nor snapshot (analysis
/// assumes versioned reads and drops Table S) — a live dirty read, the
/// Stage 13 seam class, caught by the control-flow review.
pub(super) fn enter_condition_scopes<'a>(
    storage: &'a Storage,
    condition: &Expr,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<(Option<SnapshotScope<'a>>, Option<TxnSnapshotScope>), SqlError> {
    let mut tables = Vec::new();
    collect_expr_tables(condition, &mut tables);
    // A scalar function the condition calls may read tables through its body;
    // those reads must observe the same snapshot as a direct read (the lock
    // analysis already resolved them), so arm the scope when the condition
    // reaches any table directly OR through a called function.
    if tables.is_empty()
        && expr_function_read_ids(storage, txn_ctx.database_id(), condition).is_empty()
    {
        return Ok((None, None));
    }
    match txn_ctx.isolation() {
        Isolation::ReadCommitted if storage.rcsi_enabled() => {
            // The snapshot is the durable commit prefix: the session's own
            // just-committed statements must be durable before capture.
            run.flush(storage)?;
            Ok((
                Some(SnapshotScope::enter(
                    storage,
                    txn_ctx.txn.as_ref().map(StorageTxn::txn_id),
                )),
                None,
            ))
        }
        Isolation::Snapshot => {
            if !storage.snapshot_isolation_allowed() {
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return Err(snapshot_not_allowed_error(&txn_ctx.database));
            }
            if txn_ctx.in_txn() {
                if txn_ctx.txn_snapshot.is_none() {
                    // First data access establishes the transaction's view —
                    // a condition read counts.
                    run.flush(storage)?;
                    let own = txn_ctx.txn.as_ref().map(StorageTxn::txn_id);
                    txn_ctx.txn_snapshot = Some(storage.capture_read_snapshot(own));
                }
                Ok((None, txn_ctx.txn_snapshot.map(TxnSnapshotScope::enter)))
            } else {
                run.flush(storage)?;
                Ok((Some(SnapshotScope::enter(storage, None)), None))
            }
        }
        // A readable STANDBY snapshots condition reads too (below the
        // RCSI/SNAPSHOT arms — see the statement arming): only the
        // last-applied-commit snapshot yields committed-state reads there.
        _ if storage.is_standby() => {
            run.flush(storage)?;
            Ok((Some(SnapshotScope::enter(storage, None)), None))
        }
        _ => Ok((None, None)),
    }
}

/// Evaluates an IF/WHILE condition: subqueries (EXISTS, scalar, IN) resolve
/// eagerly through the same machinery as WHERE-clause subqueries, then the
/// residual expression evaluates against the session context. T-SQL
/// three-valued: TRUE runs the branch; FALSE and NULL (UNKNOWN) do not.
pub(super) fn eval_condition(
    storage: &Storage,
    condition: &Expr,
    txn_ctx: &TxnContext,
) -> Result<bool, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let no_outer = |_: &str| -> Option<usize> { None };
    let resolved = substitute_correlated_in_expr(storage, condition, &no_outer, &[], &eval_ctx)?;
    match eval_constant(&resolved, &eval_ctx)? {
        SqlValue::Bool(taken) => Ok(taken),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is              expected.",
        )),
    }
}

/// How a statement block ended: normally, or via a control-flow statement
/// that must propagate to the construct that absorbs it (`WHILE` for
/// Break/Continue, the batch — later the procedure — for Return, the nearest
/// block holding the target label for `Goto`). TRY/CATCH and plain blocks pass
/// every non-Normal flow straight through (a `Goto` is first checked against the
/// current block's labels, then propagated).
#[derive(Clone, PartialEq, Eq)]
pub(super) enum Flow {
    Normal,
    Break,
    Continue,
    Return,
    /// A `GOTO <label>` still looking for its target label.
    Goto(String),
}

/// What `run_block`'s loop should do with a flow bubbling up from a nested
/// construct: a `GOTO` to a label in this block jumps there; anything else
/// propagates to the enclosing block.
pub(super) enum GotoAction {
    /// Resume at this statement index (a resolved `GOTO`).
    Jump(usize),
    /// The nested construct ended normally — fall through.
    Fall,
    /// Return this flow to the caller (Break/Continue/Return, or a `GOTO` to a
    /// label not defined in this block).
    Propagate(Flow),
}

pub(super) fn resolve_goto(
    flow: Flow,
    labels: &std::collections::HashMap<String, usize>,
) -> GotoAction {
    match flow {
        Flow::Normal => GotoAction::Fall,
        Flow::Goto(label) => match labels.get(&label.to_ascii_lowercase()) {
            Some(&target) => GotoAction::Jump(target),
            None => GotoAction::Propagate(Flow::Goto(label)),
        },
        other => GotoAction::Propagate(other),
    }
}

/// A statement list run as its own scope — a batch, or a procedure / function /
/// trigger body — cannot be a GOTO target from outside and a GOTO cannot jump
/// out of it. A GOTO that reaches the end of such a scope unresolved references
/// a label defined nowhere in scope: error 133.
pub(super) fn end_of_scope(flow: Flow) -> Result<(), SqlError> {
    match flow {
        Flow::Goto(label) => Err(SqlError::new(
            133,
            15,
            1,
            format!("A GOTO statement references the label '{label}:' which has not been defined."),
        )),
        _ => Ok(()),
    }
}

pub(super) fn run_block(
    storage: &Storage,
    statements: &[Statement],
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<Flow, SqlError> {
    // Label positions for GOTO. A jump sets the index to the label's position;
    // execution resumes there (the label statement itself is a no-op). A label
    // repeated in the same list is error 132.
    let mut labels: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (idx, s) in statements.iter().enumerate() {
        if let Statement::Label { name, .. } = s
            && labels.insert(name.to_ascii_lowercase(), idx).is_some()
        {
            return Err(SqlError::new(
                132,
                15,
                1,
                format!(
                    "The label '{name}:' has already been declared. Label names must be unique \
                     within a query batch or stored procedure."
                ),
            ));
        }
    }
    let mut i = 0;
    'stmts: while i < statements.len() {
        let statement = &statements[i];
        i += 1;
        // A TDS Attention (cancel) aborts the batch before the next statement.
        // It is never catchable — it propagates straight out, past any TRY.
        check_cancelled()?;
        if let Statement::Exec(exec) = statement {
            // The inner statements flow through `run_block` recursion, whose
            // own loop applies the per-statement flush and commit flag — the
            // same shape as TRY/CATCH dispatch. Errors take the ordinary
            // statement path: cancels and durability failures propagate, a
            // TRY transfers to CATCH, XACT_ABORT OFF continues the batch.
            match run_exec(storage, exec, txn_ctx, run, in_try) {
                Ok(()) => {}
                Err(exec_error) => {
                    // A failed EXEC sets @@ROWCOUNT to 0 like any failed
                    // statement.
                    txn_ctx.rowcount = 0;
                    let (error, from_inner) = match exec_error {
                        ExecError::Own(error) => (error, false),
                        ExecError::Inner(error) => (error, true),
                    };
                    if error.number == CANCEL_ERROR {
                        return Err(error);
                    }
                    // Inner errors were recorded at their raise site (the
                    // inner ladder), where the procedure frame was still
                    // live; re-recording here would blank ERROR_PROCEDURE().
                    if !from_inner {
                        txn_ctx.record_error(error.number);
                    }
                    if run.durability_failed {
                        return Err(error);
                    }
                    // Transfer to CATCH: decisions (dooming included) were
                    // already made where the error arose — per-statement in
                    // the inner `run_block`, or `doom_per_rule` for
                    // `run_exec`'s own errors. A fatal (>= 20) error is
                    // refused by the TryCatch arm's own filter.
                    if in_try {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        return Err(error);
                    }
                    // An error crossing OUT of the inner batch already
                    // terminated it — and batch-abort scope is the whole
                    // nest, so the outer batch ends too (a THROW inside
                    // EXEC'd text ends the calling batch even when nothing
                    // doomed; non-dooming ordinary errors never cross — the
                    // inner run_block continued past them). Nothing is
                    // re-derived from severity here: the review showed that
                    // second derivation dropped THROW's termination.
                    if from_inner {
                        return Err(error);
                    }
                    // run_exec's OWN failure (unknown proc, 214, 8144, parse,
                    // depth): statement-scope at the EXEC site. Dooming was
                    // applied at the source; this decides only continuation.
                    let terminates = txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY;
                    if txn_ctx.in_txn() && !terminates {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        run.last_error = Some(error);
                        continue;
                    }
                    return Err(error);
                }
            }
            continue;
        }
        match statement {
            Statement::Block { body, .. } => {
                match resolve_goto(run_block(storage, body, txn_ctx, run, in_try)?, &labels) {
                    GotoAction::Jump(t) => {
                        i = t;
                        continue 'stmts;
                    }
                    GotoAction::Propagate(flow) => return Ok(flow),
                    GotoAction::Fall => {}
                }
                continue;
            }
            Statement::If {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                // A successful condition evaluation resets `@@ERROR` (the IF
                // itself is a statement) — AFTER the condition read it, which
                // is what makes `IF @@ERROR <> 0` work.
                // A condition subquery reads table variables through the same
                // FROM path as a SELECT, so it needs the same read view armed —
                // the IF/WHILE arms bypass exec_statement_streamed, so arm here.
                let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                    .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                {
                    Ok(taken) => taken,
                    Err(error) => {
                        txn_ctx.rowcount = 0;
                        statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                        continue;
                    }
                };
                txn_ctx.last_error = 0;
                let branch = if taken {
                    Some(then_branch)
                } else {
                    else_branch.as_ref()
                };
                if let Some(branch) = branch {
                    let flow =
                        run_block(storage, std::slice::from_ref(branch), txn_ctx, run, in_try)?;
                    match resolve_goto(flow, &labels) {
                        GotoAction::Jump(t) => {
                            i = t;
                            continue 'stmts;
                        }
                        GotoAction::Propagate(flow) => return Ok(flow),
                        GotoAction::Fall => {}
                    }
                }
                continue;
            }
            Statement::While {
                condition, body, ..
            } => {
                loop {
                    // A TDS Attention lands between iterations too — an
                    // infinite `WHILE 1 = 1` must die on cancel even when its
                    // body runs no cancellable statement.
                    check_cancelled()?;
                    // Re-armed each iteration: the body may INSERT into @t, and
                    // the next condition read must see the updated rows.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                        .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                    {
                        Ok(taken) => taken,
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            break;
                        }
                    };
                    txn_ctx.last_error = 0;
                    if !taken {
                        break;
                    }
                    let flow =
                        run_block(storage, std::slice::from_ref(body), txn_ctx, run, in_try)?;
                    match flow {
                        Flow::Normal | Flow::Continue => {}
                        Flow::Break => break,
                        // RETURN or a GOTO leaves the loop: a GOTO to a label in
                        // this block jumps out of the WHILE to it, else propagate.
                        other => match resolve_goto(other, &labels) {
                            GotoAction::Jump(t) => {
                                i = t;
                                continue 'stmts;
                            }
                            GotoAction::Propagate(flow) => return Ok(flow),
                            GotoAction::Fall => {}
                        },
                    }
                }
                continue;
            }
            // The parser rejects BREAK/CONTINUE outside a WHILE (135/136), so
            // these only ever propagate up to an enclosing loop.
            Statement::Break { .. } => return Ok(Flow::Break),
            Statement::Continue { .. } => return Ok(Flow::Continue),
            // The parser rejects `RETURN <value>` outside a procedure (178);
            // inside one the status is stashed for `EXEC @rc =` to read.
            Statement::Return { value, .. } => {
                // A scalar function body's RETURN: evaluate its (mandatory)
                // value, coerce it to the declared return type, and stash it for
                // the caller. Nested user functions and subqueries in the RETURN
                // expression are rewritten to literals first, exactly like an
                // IF/WHILE condition.
                if let Some(return_type) = run.function_return_type {
                    let value = value
                        .as_ref()
                        .expect("a scalar function RETURN carries a value (parser-enforced)");
                    // A RETURN subquery reads table variables through the FROM
                    // path; arm the body's own (empty) view so it shadows the
                    // caller's rather than reading caller locals.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let eval_ctx = txn_ctx.eval_context();
                    let no_outer = |_: &str| -> Option<usize> { None };
                    let coerced =
                        substitute_correlated_in_expr(storage, value, &no_outer, &[], &eval_ctx)
                            .and_then(|bound| eval_constant(&bound, &eval_ctx))
                            .and_then(|raw| {
                                let datum =
                                    value::sql_to_datum(&raw, &return_type, "return value")?;
                                Ok(value::datum_to_sql(&datum, &return_type))
                            });
                    match coerced {
                        Ok(coerced) => {
                            txn_ctx.func_return = Some(coerced);
                            return Ok(Flow::Return);
                        }
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                if let Some(value) = value {
                    let eval_ctx = txn_ctx.eval_context();
                    match eval_constant(value, &eval_ctx) {
                        Ok(SqlValue::Int(status))
                            if (i32::MIN as i64..=i32::MAX as i64).contains(&status) =>
                        {
                            txn_ctx.proc_return = Some(status)
                        }
                        // A RETURN value outside int range overflows, as SQL
                        // Server does (8115) — the status is an int. Without this
                        // the out-of-range value would be stashed and later fail
                        // to encode (and, on the RPC path, read back as NULL and
                        // be mistaken for a procedure that never completed).
                        Ok(SqlValue::Int(_)) => {
                            let error = SqlError::new(
                                8115,
                                16,
                                2,
                                "Arithmetic overflow error converting expression to data type int.",
                            );
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                        Ok(SqlValue::Null) => {
                            // SQL Server warns and returns 0; we return 0.
                            txn_ctx.proc_return = Some(0);
                        }
                        Ok(_) | Err(_) => {
                            let error =
                                eval_constant(value, &eval_ctx).err().unwrap_or_else(|| {
                                    SqlError::new(
                                        257,
                                        16,
                                        3,
                                        "The RETURN status must be an integer.",
                                    )
                                });
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                return Ok(Flow::Return);
            }
            // A label is a no-op when reached in sequence.
            Statement::Label { .. } => continue,
            // GOTO jumps to a label in this block, or propagates to an enclosing
            // one (the batch top turns an unresolved GOTO into error 133).
            Statement::Goto { label, .. } => match labels.get(&label.to_ascii_lowercase()) {
                Some(&target) => {
                    i = target;
                    continue 'stmts;
                }
                None => return Ok(Flow::Goto(label.clone())),
            },
            _ => {}
        }
        if let Statement::TryCatch {
            try_block,
            catch_block,
            ..
        } = statement
        {
            match run_block(storage, try_block, txn_ctx, run, true) {
                Ok(Flow::Normal) => {}
                // BREAK/CONTINUE/RETURN/GOTO cross a TRY without running its
                // CATCH; a GOTO to a label in this block jumps there.
                Ok(flow) => match resolve_goto(flow, &labels) {
                    GotoAction::Jump(t) => {
                        i = t;
                        continue 'stmts;
                    }
                    GotoAction::Propagate(flow) => return Ok(flow),
                    GotoAction::Fall => {}
                },
                // An Attention that landed inside the TRY block is not caught.
                Err(cancel) if cancel.number == CANCEL_ERROR => return Err(cancel),
                // A durability failure wedged the store: no CATCH swallows a
                // lost commit (the old batch-end fsync ran past every TRY).
                Err(error) if run.durability_failed => return Err(error),
                // Severity >= 20 is fatal to the connection: no CATCH sees
                // it. Already recorded (and doomed) at the raise site.
                Err(error) if error.level >= FATAL_SEVERITY => return Err(error),
                Err(error) => {
                    // The failed statement's own writes were already undone to
                    // its savepoint (`rel_statement_scoped`), and the doom
                    // decision was made where the statement failed — the inner
                    // `run_block` knows the statement's kind (RAISERROR is
                    // exempt from XACT_ABORT), this boundary does not. Control
                    // transfers to CATCH either way; a doomed transaction
                    // reports XACT_STATE() = -1 there.
                    txn_ctx.push_error(&error);
                    // The CATCH block runs in the *enclosing* try-context: its
                    // own errors are not caught here, so they propagate to an
                    // outer CATCH (or end the batch) per `in_try`.
                    let caught = run_block(storage, catch_block, txn_ctx, run, in_try);
                    txn_ctx.pop_error();
                    match resolve_goto(caught?, &labels) {
                        GotoAction::Jump(t) => {
                            i = t;
                            continue 'stmts;
                        }
                        GotoAction::Propagate(flow) => return Ok(flow),
                        GotoAction::Fall => {}
                    }
                }
            }
            continue;
        }
        // A statement that can open a result set is a durability point: the
        // deferred DONEs must reach the stream before its columns do, and any
        // commit made so far must be fsync-durable before rows that can carry
        // its state (an identity value, via SCOPE_IDENTITY()) leave the server.
        if produces_rowset(statement) || matches!(statement, Statement::RaiseError(_)) {
            run.flush(storage)?;
        }
        // Flag durability by statement kind, before matching the result: a
        // write/DDL/COMMIT can commit even when it then errors — an autocommit
        // statement, an identity reservation (its own mini-commit, made even
        // inside an open transaction and even if the row insert later fails),
        // or the outermost COMMIT.
        run.committed |= statement_may_commit(statement);
        match exec_statement_streamed(storage, statement, txn_ctx, run) {
            Ok(outcome) => {
                // The statement succeeded: `@@ERROR` reads 0 — except after a
                // severity <= 10 RAISERROR, which set it itself (0, or 50000
                // under SETERROR).
                if !matches!(statement, Statement::RaiseError(_)) {
                    txn_ctx.last_error = 0;
                }
                let in_transaction = txn_ctx.in_txn();
                let command = done_command(statement);
                // `SET NOCOUNT ON` suppresses the DONE's count on the wire;
                // rows/results are untouched. `@@ROWCOUNT` records the true
                // count either way (NOCOUNT does not change it).
                let nocount = txn_ctx.nocount;
                let wire_count =
                    |count: u64| -> Option<u64> { if nocount { None } else { Some(count) } };
                // `USE` succeeded: earlier statements' deferred DONEs go out
                // first, then the database-context ENVCHANGE + 5701 INFO the
                // client (SSMS) expects, then the USE's own DONE below —
                // SQL Server's exact order.
                if let Statement::Use { .. } = statement {
                    run.flush(storage)?;
                    run.database_context(&txn_ctx.database);
                }
                match outcome {
                    StatementOutcome::Streamed { rows } => {
                        txn_ctx.rowcount = rows as i64;
                        run.done(wire_count(rows), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Rows(rowset)) => {
                        let count = rowset.rows.len() as u64;
                        txn_ctx.rowcount = count as i64;
                        run.open_rowset(rowset.columns);
                        run.rows(rowset.rows);
                        run.done(wire_count(count), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::RowsAffected(n)) => {
                        txn_ctx.rowcount = n as i64;
                        run.done(wire_count(n), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Done) => {
                        // A simple variable assignment (`SET @x = ...`) sets
                        // @@ROWCOUNT to 1 — recorded by exec_set, preserved
                        // here; every other Done statement resets it to 0.
                        if !matches!(
                            statement,
                            Statement::Set(SetStatement::Variable { .. }) | Statement::Declare(_)
                        ) {
                            txn_ctx.rowcount = 0;
                        }
                        run.done(None, in_transaction, command);
                    }
                }
            }
            Err(error) => {
                // A failed statement sets @@ROWCOUNT to 0, as SQL Server does.
                txn_ctx.rowcount = 0;
                statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
            }
        }
    }
    Ok(Flow::Normal)
}
