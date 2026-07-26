use super::super::prelude::*;
use super::binding::FnResolver;

pub(in crate::engine::relational) fn build_source(
    storage: &Storage,
    from: Option<&TableRef>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    if let Some(from) = from {
        check_exposed_names(from)?;
    }
    build_source_inner(storage, from, where_clause, eval_ctx)
}

pub(in crate::engine::relational) fn build_source_inner(
    storage: &Storage,
    from: Option<&TableRef>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    match from {
        None => Ok(Source {
            // No FROM: one row, no columns (constant SELECT).
            columns: Vec::new(),
            qualifiers: Vec::new(),
            collations: Vec::new(),
            rows: SourceRows::Materialized(vec![Vec::new()]),
        }),
        // A single top-level table may use the WHERE for an index seek; base
        // tables inside a join scan fully (join-order planning is later).
        Some(TableRef::Table { name, alias }) => {
            build_table_source(storage, name, alias.as_ref(), where_clause, eval_ctx)
        }
        Some(join) => build_join(storage, join, eval_ctx),
    }
}

/// SQL Server caps view/function nesting at 32 levels; a deeper chain (or a view
/// cycle) errors here rather than overflowing the stack.
pub(in crate::engine::relational) const MAX_VIEW_NESTING: u32 = 32;

thread_local! {
    /// Current view-expansion depth on this worker thread (each batch runs on
    /// one thread, so a thread-local is per-request).
    static VIEW_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard that increments the view-nesting depth on `enter` and restores it
/// on drop (including the error/`?` paths), erroring past [`MAX_VIEW_NESTING`].
pub(in crate::engine::relational) struct ViewDepthGuard;

impl ViewDepthGuard {
    fn enter(view_name: &str) -> Result<Self, SqlError> {
        let depth = VIEW_DEPTH.with(|d| d.get());
        if depth >= MAX_VIEW_NESTING {
            return Err(SqlError::message_only(
                436,
                format!(
                    "View '{view_name}' exceeds the maximum view nesting level of {MAX_VIEW_NESTING} (possibly a view cycle)."
                ),
            ));
        }
        VIEW_DEPTH.with(|d| d.set(depth + 1));
        Ok(ViewDepthGuard)
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// True where object-permission checks apply — not inside an OWNED stored-object
/// body (procedure, function, TVF, view, or trigger), whose reads are covered by
/// ownership chaining (all objects share the single `dbo` owner today), so the
/// caller's permission on the body suffices (the grant-EXECUTE-only pattern).
/// Dynamic SQL (`sp_executesql`) resets `CHAIN_DEPTH`, so it is checked here even
/// when nested in a procedure — matching SQL Server, which does not chain
/// through dynamic SQL.
pub(in crate::engine::relational) fn at_top_level() -> bool {
    CHAIN_DEPTH.with(|d| d.get()) == 0 && VIEW_DEPTH.with(|d| d.get()) == 0
}

/// Whether `sec` permits `action` on an object with these permission entries.
/// A matching DENY for any of the session's principals wins (DENY beats GRANT);
/// otherwise a matching GRANT permits; otherwise denied (no implicit grant).
pub(in crate::engine::relational) fn permits(
    perms: &[PermissionEntry],
    sec: &SecurityContext,
    action: PermAction,
) -> bool {
    let mut granted = false;
    for entry in perms {
        if entry.action == action && sec.principals.contains(&entry.grantee) {
            if entry.deny {
                return false;
            }
            granted = true;
        }
    }
    granted
}

/// Enforces `action` on the resolved object `def`, erroring 229 if the session
/// lacks the permission. A no-op for a bypassing session (sysadmin / dbo /
/// internal) and inside any stored-object body (ownership chaining).
pub(in crate::engine::relational) fn enforce_object_permission(
    storage: &Storage,
    def: &TableDef,
    sec: &SecurityContext,
    action: PermAction,
) -> Result<(), SqlError> {
    if sec.bypass || !at_top_level() || permits(&def.permissions, sec, action) {
        return Ok(());
    }
    Err(SqlError::new(
        229,
        14,
        5,
        format!(
            "The {} permission was denied on the object '{}', database '{}', schema 'dbo'.",
            action.name(),
            def.name,
            database_name_of(storage, def.database_id)
        ),
    ))
}

/// Builds the row source for one base table (or `sys.*` view), stamping every
/// column with the table's qualifier (its alias, else its name).
pub(in crate::engine::relational) fn build_table_source(
    storage: &Storage,
    name: &Name,
    alias: Option<&Name>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    // A `@t` table variable: serve its in-memory rows as a materialized source.
    // (The catalog resolver never matches an `@`-name, so this is the only path
    // that handles it — and it never touches Storage.)
    if name.value.starts_with('@') {
        let tv = current_table_var(&name.value)
            .ok_or_else(|| must_declare_table_var(&name.value).at(name.span))?;
        let count = tv.schema.columns.len();
        let columns = tv
            .schema
            .columns
            .iter()
            .map(|c| ResultColumn {
                name: c.name.clone(),
                column_type: c.column_type,
            })
            .collect();
        let collations = tv
            .schema
            .columns
            .iter()
            .map(|c| c.collation.clone())
            .collect();
        return Ok(Source {
            columns,
            qualifiers: vec![Some(qualifier); count],
            collations,
            rows: SourceRows::Materialized(tv.rows),
        });
    }
    // `inserted`/`deleted`: the firing trigger's pseudo-tables. Resolved before
    // the catalog so a real table named `inserted` cannot be reached from inside
    // a trigger body (SQL Server reserves them there too). Only matches when a
    // trigger scope is armed; otherwise falls through to catalog resolution.
    if let Some(source) = current_trigger_source(&name.value, &qualifier) {
        return Ok(source);
    }
    let base = match name.value.to_ascii_lowercase().as_str() {
        "sys.tables" => sys_tables(storage, eval_ctx.database_id),
        "sys.databases" => sys_databases(storage),
        "sys.dm_repl_replica_states" => sys_dm_repl_replica_states(storage),
        "sys.dm_repl_slots" => sys_dm_repl_slots(storage),
        "sys.configurations" => sys_configurations(),
        "sys.views" => sys_views(storage, eval_ctx.database_id),
        "sys.procedures" => sys_procedures(storage, eval_ctx.database_id),
        "sys.triggers" => sys_triggers(storage, eval_ctx.database_id),
        "sys.trigger_events" => sys_trigger_events(storage, eval_ctx.database_id),
        "sys.server_principals" => sys_server_principals(storage),
        "sys.sql_logins" => sys_sql_logins(storage),
        "sys.database_principals" => sys_database_principals(storage),
        "sys.database_role_members" => sys_database_role_members(storage),
        "sys.database_permissions" => sys_database_permissions(storage, eval_ctx.database_id),
        "sys.parameters" => sys_parameters(storage, eval_ctx.database_id),
        "sys.objects" => sys_objects(storage, eval_ctx.database_id),
        "sys.sql_modules" => sys_sql_modules(storage, eval_ctx.database_id),
        "sys.columns" => sys_columns(storage, eval_ctx.database_id),
        "sys.indexes" => sys_indexes(storage, eval_ctx.database_id),
        "sys.check_constraints" => sys_check_constraints(storage, eval_ctx.database_id),
        "sys.foreign_keys" => sys_foreign_keys(storage, eval_ctx.database_id),
        "sys.default_constraints" => sys_default_constraints(storage, eval_ctx.database_id),
        _ => {
            let def = resolve_table(storage, eval_ctx.database_id, &name.value)
                .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
            // A procedure is not a queryable object (SQL Server 2809).
            if def.is_procedure() {
                return Err(procedure_not_a_table(&def.name).at(name.span));
            }
            // A trigger is not a queryable object either — resolving it as a base
            // table would heap-scan its (empty) root page 0. 208 invalid object.
            if def.is_trigger() {
                return Err(SqlError::invalid_object(&name.value).at(name.span));
            }
            // A scalar function is not a rowset — it cannot appear in FROM.
            // (Table-valued functions, added later, expand here instead.)
            if def.is_function() {
                return Err(function_not_a_table(&def.name).at(name.span));
            }
            // SELECT permission on the base table or view (checked here, at the
            // top level, before a view body expands — the body's own reads are
            // covered by ownership chaining and not re-checked).
            enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Select)
                .map_err(|e| e.at(name.span))?;
            // A view: run its stored SELECT as a derived table under the view's
            // qualifier. A view over another view expands recursively — building
            // the derived source re-enters `build_table_source` for the inner
            // view — bounded by a nesting-depth guard that turns a view cycle
            // (self- or mutually-referential views) into a clean error instead
            // of a stack overflow.
            if let Some(query_text) = &def.view_query {
                let _guard = ViewDepthGuard::enter(&def.name)?;
                let body = parse_view_query(query_text, &def.name)?;
                let qual = Name {
                    value: qualifier,
                    quoted: false,
                    span: name.span,
                };
                // A view body is a stored-object scope, like a function/TVF
                // body: it must not read the CALLER's table variables. Shadow
                // the read view with an empty one so `SELECT ... FROM @t` inside
                // a view errors 1087 rather than returning caller rows. (An
                // in-statement derived table or CTE is NOT a separate scope and
                // keeps the statement's view — only stored bodies shadow.)
                let _table_var_scope = arm_table_var_view(&std::collections::HashMap::new());
                let _trigger_shadow = TriggerScope::clear();
                // The body's unqualified names are the VIEW's database's (a
                // cross-database view reads its own home, as SQL Server
                // resolves it) — matching collect_read_lock_ids' analysis.
                let mut view_ctx = eval_ctx.clone();
                view_ctx.database_id = def.database_id;
                return build_derived_source(storage, &body, &qual, &view_ctx);
            }
            let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
            // An index seek narrows the candidate set; the WHERE filter later
            // re-checks, so results match a full scan.
            // Fetched only when choose() can use it (it returns a scan
            // outright without a predicate or indexes).
            let row_count = if def.indexes.is_empty() || where_clause.is_none() {
                None
            } else {
                storage.rel_row_count(def.database_id, &def.name)
            };
            let rows = match plan::choose(&def, &schema, where_clause, eval_ctx, None, row_count) {
                // A scan is handed out LAZY: the consumer pulls slices, so a
                // filtering/folding reader holds one slice, not the table
                // (and the storage lock is still taken per slice, as before).
                // Under a read snapshot the scan materializes atomically
                // instead: a versioned reader holds no table lock, so the
                // sliced cursor's contract does not hold for it.
                plan::AccessPath::TableScan => match current_snapshot() {
                    Some(snapshot) => SourceRows::Materialized(
                        storage
                            .rel_scan_snapshot(def.database_id, &def.name, None, snapshot)
                            .map_err(|err| map_storage_err(err, &def.name))?,
                    ),
                    None => SourceRows::Scan(ScanStream {
                        db_id: def.database_id,
                        table: def.name.clone(),
                        cursor: ScanCursor::start(),
                    }),
                },
                plan::AccessPath::IndexSeek {
                    index_object_id,
                    lower,
                    upper,
                    ..
                } => SourceRows::Materialized(
                    storage
                        .rel_index_scan(
                            def.database_id,
                            &def.name,
                            index_object_id,
                            lower,
                            upper,
                            None,
                            false,
                            current_snapshot(),
                        )
                        .map_err(|err| map_storage_err(err, &def.name))?,
                ),
            };
            let columns = schema
                .columns
                .iter()
                .map(|c| ResultColumn {
                    name: c.name.clone(),
                    column_type: c.column_type,
                })
                .collect();
            let collations = schema.columns.iter().map(|c| c.collation.clone()).collect();
            Source {
                columns,
                qualifiers: Vec::new(),
                collations,
                rows,
            }
        }
    };
    let count = base.columns.len();
    Ok(Source {
        qualifiers: vec![Some(qualifier); count],
        ..base
    })
}

/// Expands an inline table-valued function call `dbo.f(args) [AS alias]` in a
/// FROM clause: binds the call's argument values to the function's `@params`,
/// then runs its stored body SELECT as a derived table under the call's
/// qualifier — a parameterized view. The body's table reads are locked and
/// snapshotted up front by the lock analysis and the snapshot-scope arming,
/// which both resolve the function name into its body (see collect_read_lock_ids
/// and statement_reads_tables); the body reads under the caller's ambient
/// snapshot on this thread. Recursion is bounded by the shared view-depth guard.
pub(in crate::engine::relational) fn build_function_source(
    storage: &Storage,
    name: &Name,
    args: &[Expr],
    alias: Option<&Name>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &name.value)
        .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
    let function = def
        .function
        .as_ref()
        .ok_or_else(|| function_not_a_table(&def.name).at(name.span))?;
    // A table-valued function in FROM is read like a table: SELECT permission.
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Select)
        .map_err(|e| e.at(name.span))?;
    if args.len() < function.params.len() {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {}.",
                def.name
            ),
        )
        .at(name.span));
    }
    if args.len() > function.params.len() {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        )
        .at(name.span));
    }
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let qual = Name {
        value: qualifier,
        quoted: false,
        span: name.span,
    };
    match &function.returns {
        FunctionReturns::InlineTable { select_text } => {
            // Bind the arguments to the parameters, coercing to the declared
            // types, in a FRESH variable scope (a TVF body sees only its
            // parameters, not caller locals). Arguments may themselves contain
            // subqueries or scalar UDFs.
            let no_outer = |_: &str| -> Option<usize> { None };
            let mut variables = std::collections::HashMap::new();
            for (param, arg) in function.params.iter().zip(args) {
                let column_type = ColumnType::parse(&param.type_spec)
                    .map_err(|e| SqlError::message_only(245, e.to_string()))?;
                let value = substitute_correlated_in_expr(storage, arg, &no_outer, &[], eval_ctx)
                    .and_then(|bound| eval_constant(&bound, eval_ctx))?;
                let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
                variables.insert(
                    param.name.clone(),
                    value::datum_to_sql(&datum, &column_type),
                );
            }
            let mut fn_ctx = eval_ctx.clone();
            fn_ctx.variables = variables;
            // The body's unqualified names are the FUNCTION's database's.
            fn_ctx.database_id = def.database_id;
            // Expand the body like a view (bounded by the shared nesting guard).
            let _guard = ViewDepthGuard::enter(&def.name)?;
            let body = parse_view_query(select_text, &def.name)?;
            // A TVF body sees only its parameters, not caller locals — the scalar
            // side is isolated above (fresh `variables`); do the same for the
            // table-variable read view. Without this the body's `FROM @t` would
            // resolve against the CALLER's table variable, since build_derived_
            // source runs under whatever scope the calling statement armed. An
            // empty view makes such a body error 1087, as SQL Server rejects it.
            let _table_var_scope = arm_table_var_view(&std::collections::HashMap::new());
            let _trigger_shadow = TriggerScope::clear();
            build_derived_source(storage, &body, &qual, &fn_ctx)
        }
        FunctionReturns::MultiStatementTable {
            returns_var,
            columns_text,
            body,
        } => run_multi_statement_tvf(
            storage,
            def.database_id,
            function,
            returns_var,
            columns_text,
            body,
            args,
            &qual,
            eval_ctx,
        ),
        // A scalar function called in table position is not a rowset.
        FunctionReturns::Scalar { .. } => Err(function_not_a_table(&def.name).at(name.span)),
    }
}

/// Runs a multi-statement TVF and returns its result table variable's rows as a
/// materialized source. The body runs in an isolated context (a fresh
/// `TxnContext`, like a scalar UDF: parameters only, no transaction, ambient
/// snapshot for its reads) seeded with the empty result table variable, which
/// its statements populate; the accumulated rows are the function's result.
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::relational) fn run_multi_statement_tvf(
    storage: &Storage,
    home_db_id: u32,
    function: &FunctionDef,
    returns_var: &str,
    columns_text: &str,
    body_text: &str,
    args: &[Expr],
    qual: &Name,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    // Rebuild the result table variable's schema (re-parsed per call, like the
    // body — the CREATE-time validation guarantees this succeeds).
    let (columns, primary_key) = truthdb_sql::parse_table_var_columns(columns_text)?;
    let (schema, key_columns, defaults) =
        build_table_var_definition(returns_var, &columns, &primary_key)?;
    // Fresh isolated scope: parameters only, caller session identity carried for
    // DB_NAME()/SUSER_SNAME()/USER_NAME()/@@SPID and role membership. Arguments
    // evaluate in the CALLER's context. The sids are left 0 (the body does not
    // re-resolve membership — it reuses the caller's already-computed role set).
    let mut txn_ctx = TxnContext::default();
    // The body's unqualified names resolve in the FUNCTION's home database;
    // DB_ID/DB_NAME keep working via the caller's databases snapshot.
    txn_ctx.set_session_identity(
        database_name_of(storage, home_db_id),
        home_db_id,
        eval_ctx.login.clone(),
        eval_ctx.spid,
        eval_ctx.user.clone(),
        0,
        0,
    );
    txn_ctx.databases_snapshot = eval_ctx.databases.clone();
    txn_ctx.session_server_roles = eval_ctx.server_roles.clone();
    txn_ctx.session_db_roles = eval_ctx.db_roles.clone();
    txn_ctx.security = eval_ctx.security.clone();
    let no_outer = |_: &str| -> Option<usize> { None };
    for (param, arg) in function.params.iter().zip(args) {
        let column_type = ColumnType::parse(&param.type_spec)
            .map_err(|e| SqlError::message_only(245, e.to_string()))?;
        let value = substitute_correlated_in_expr(storage, arg, &no_outer, &[], eval_ctx)
            .and_then(|bound| eval_constant(&bound, eval_ctx))?;
        let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
        txn_ctx.variables.insert(
            param.name.clone(),
            (column_type, value::datum_to_sql(&datum, &column_type)),
        );
    }
    // Seed the empty result table variable; the body populates it.
    txn_ctx.table_variables.insert(
        returns_var.to_string(),
        TableVar {
            schema,
            key_columns,
            defaults,
            rows: Vec::new(),
        },
    );
    let statements = truthdb_sql::parse_table_function_body(body_text)?;
    // A multi-statement TVF called from a trigger body does not see
    // inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // A multi-statement TVF body ownership-chains: reads are not re-checked.
    let _chain = ChainGuard::enter();
    // Same nesting cap as a scalar UDF (217), decremented on every exit path.
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
            // A multi-statement TVF's RETURN carries no value.
            function_return_type: None,
        };
        run_block(storage, &statements, &mut txn_ctx, &mut run, false).and_then(end_of_scope)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    result?;
    // The accumulated rows are the result. Serve them as a materialized source
    // stamped with the call's qualifier (identical shape to the @t FROM branch).
    let tv = txn_ctx
        .table_variables
        .get(returns_var)
        .expect("seeded above");
    let count = tv.schema.columns.len();
    let columns_out = tv
        .schema
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            column_type: c.column_type,
        })
        .collect();
    let collations = tv
        .schema
        .columns
        .iter()
        .map(|c| c.collation.clone())
        .collect();
    Ok(Source {
        columns: columns_out,
        qualifiers: vec![Some(qual.value.clone()); count],
        collations,
        rows: SourceRows::Materialized(tv.rows.clone()),
    })
}

/// Recursively builds a join tree's combined row source (base tables scan
/// fully).
pub(in crate::engine::relational) fn build_join(
    storage: &Storage,
    tref: &TableRef,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    match tref {
        TableRef::Table { name, alias } => {
            build_table_source(storage, name, alias.as_ref(), &None, eval_ctx)
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => {
            if matches!(kind, JoinKind::CrossApply | JoinKind::OuterApply) {
                return build_apply(
                    storage,
                    left,
                    right,
                    matches!(kind, JoinKind::OuterApply),
                    eval_ctx,
                );
            }
            let left = build_join(storage, left, eval_ctx)?;
            let right = build_join(storage, right, eval_ctx)?;
            join_sources(storage, left, right, *kind, on.as_ref(), eval_ctx)
        }
        TableRef::Derived { subquery, alias } => {
            build_derived_source(storage, subquery, alias, eval_ctx)
        }
        TableRef::Function { name, args, alias } => {
            build_function_source(storage, name, args, alias.as_ref(), eval_ctx)
        }
    }
}

/// `CROSS`/`OUTER APPLY`: the right side is re-evaluated once per left row,
/// correlated to it. For each left row the right `TableRef` is rebound to that
/// row's values (a TVF's arguments become literals; a derived table's outer
/// column references are substituted) and built; its rows are concatenated onto
/// the left row. CROSS APPLY drops a left row that produced none; OUTER APPLY
/// keeps it with NULLs for the right columns.
pub(in crate::engine::relational) fn build_apply(
    storage: &Storage,
    left: &TableRef,
    right: &TableRef,
    is_outer: bool,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let left_source = build_join(storage, left, eval_ctx)?;
    let left_types = left_source.types();
    let left_columns = left_source.columns.clone();
    let left_qualifiers = left_source.qualifiers.clone();
    let left_collations = left_source.collations.clone();
    // A resolver over the left columns so the right side's correlated references
    // (and TVF arguments) bind to the current left row.
    let left_scope = JoinScope {
        columns: left_qualifiers
            .iter()
            .zip(&left_columns)
            .map(|(q, c)| (q.clone(), c.name.clone()))
            .collect(),
        collations: left_collations.clone(),
    };
    let left_rows = left_source.rows.materialize(storage)?;

    let build_right_for = |vals: &[SqlValue]| -> Result<Source, SqlError> {
        let outer = |name: &str| left_scope.resolve(name);
        let bound = substitute_outer_in_tref(storage, right, &outer, vals, eval_ctx)?;
        build_join(storage, &bound, eval_ctx)
    };

    // (columns, qualifiers, collations) of the right source — learned from the
    // first built right and reused for the result's shape.
    type RightMeta = (Vec<ResultColumn>, Vec<Option<String>>, Vec<Option<String>>);
    let mut out_rows: Vec<Vec<Datum>> = Vec::new();
    let mut right_meta: Option<RightMeta> = None;
    for left_row in &left_rows {
        check_cancelled()?;
        let vals = row_values(left_row, &left_types);
        let right_source = build_right_for(&vals)?;
        let right_col_count = right_source.columns.len();
        if right_meta.is_none() {
            right_meta = Some((
                right_source.columns.clone(),
                right_source.qualifiers.clone(),
                right_source.collations.clone(),
            ));
        }
        let right_rows = right_source.rows.materialize(storage)?;
        if right_rows.is_empty() {
            if is_outer {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat_n(Datum::Null, right_col_count));
                out_rows.push(combined);
            }
        } else {
            for rr in &right_rows {
                let mut combined = left_row.clone();
                combined.extend(rr.iter().cloned());
                out_rows.push(combined);
            }
        }
    }
    // With no left rows the right was never built; build it once against a NULL
    // left row to learn its column shape (the result is still zero rows).
    let (right_columns, right_qualifiers, right_collations) = match right_meta {
        Some(meta) => meta,
        None => {
            let nulls = vec![SqlValue::Null; left_columns.len()];
            let right_source = build_right_for(&nulls)?;
            (
                right_source.columns,
                right_source.qualifiers,
                right_source.collations,
            )
        }
    };

    let mut columns = left_columns;
    columns.extend(right_columns);
    let mut qualifiers = left_qualifiers;
    qualifiers.extend(right_qualifiers);
    let mut collations = left_collations;
    collations.extend(right_collations);
    Ok(Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(out_rows),
    })
}

/// Rebinds a right-of-APPLY `TableRef` to one left row: a TVF's arguments are
/// evaluated against the left row to literals; a derived table's correlated
/// outer references are substituted; a base table is unchanged. The rebound
/// reference builds with no remaining correlation.
pub(in crate::engine::relational) fn substitute_outer_in_tref(
    storage: &Storage,
    tref: &TableRef,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
    eval_ctx: &EvalContext,
) -> Result<TableRef, SqlError> {
    match tref {
        TableRef::Table { .. } => Ok(tref.clone()),
        TableRef::Function { name, args, alias } => {
            let resolver = FnResolver(outer);
            let bound_args = args
                .iter()
                .map(|arg| {
                    let bound =
                        substitute_correlated_in_expr(storage, arg, outer, outer_row, eval_ctx)?;
                    let value = eval::eval(&bound, outer_row, &resolver, eval_ctx)?;
                    Ok(Expr {
                        kind: ExprKind::Literal(value),
                        span: arg.span,
                    })
                })
                .collect::<Result<Vec<_>, SqlError>>()?;
            Ok(TableRef::Function {
                name: name.clone(),
                args: bound_args,
                alias: alias.clone(),
            })
        }
        TableRef::Derived { subquery, alias } => {
            let bound = substitute_subquery_outer_refs(
                storage,
                eval_ctx.database_id,
                subquery,
                outer,
                outer_row,
            )
            .unwrap_or_else(|| (**subquery).clone());
            Ok(TableRef::Derived {
                subquery: Box::new(bound),
                alias: alias.clone(),
            })
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => Ok(TableRef::Join {
            left: Box::new(substitute_outer_in_tref(
                storage, left, outer, outer_row, eval_ctx,
            )?),
            right: Box::new(substitute_outer_in_tref(
                storage, right, outer, outer_row, eval_ctx,
            )?),
            kind: *kind,
            on: on
                .as_ref()
                .map(|e| substitute_correlated_in_expr(storage, e, outer, outer_row, eval_ctx))
                .transpose()?,
        }),
    }
}

/// Builds a derived table's row source by executing its subquery and stamping
/// every output column with the derived-table alias. Every column must be named
/// (8155) and names must be unique within the derived table (8156).
pub(in crate::engine::relational) fn build_derived_source(
    storage: &Storage,
    subquery: &Select,
    alias: &Name,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let rowset = exec_select(storage, subquery, eval_ctx)?;
    for (index, column) in rowset.columns.iter().enumerate() {
        if column.name.is_empty() {
            return Err(SqlError::new(
                8155,
                16,
                2,
                format!(
                    "No column name was specified for column {} of '{}'.",
                    index + 1,
                    alias.value
                ),
            ));
        }
        if rowset.columns[..index]
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&column.name))
        {
            return Err(SqlError::new(
                8156,
                16,
                1,
                format!(
                    "The column '{}' was specified multiple times for '{}'.",
                    column.name, alias.value
                ),
            ));
        }
    }
    let count = rowset.columns.len();
    Ok(Source {
        columns: rowset.columns,
        qualifiers: vec![Some(alias.value.clone()); count],
        // KNOWN LIMITATION: a RowSet carries no per-column collation, so a
        // derived character column loses its source collation and an outer
        // ORDER BY sorts it under the database default rather than the base
        // column's COLLATE. Fixing this needs collation threaded through the
        // project/RowSet boundary; deferred (narrow, non-default-collation only).
        collations: vec![None; count],
        rows: SourceRows::Materialized(rowset.rows),
    })
}
