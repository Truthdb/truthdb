use super::prelude::*;

// ---- transaction control -----------------------------------------------

/// `USE <database>`: a single-database instance, so the only accepted target
/// is the session's current database — the statement exists for the
/// database-context ENVCHANGE clients (SSMS) expect back (emitted by
/// `run_block` on success).
pub(super) fn exec_use(
    storage: &Storage,
    database: &Name,
    ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // ONE catalog read: a lookup-then-list pair would race a concurrent
    // DROP DATABASE into a panic between the two.
    let Some((db_id, canonical)) = storage
        .rel_databases()
        .into_iter()
        .find(|(_, name)| name.eq_ignore_ascii_case(&database.value))
    else {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                database.value
            ),
        )
        .at(database.span));
    };
    ctx.set_current_database(canonical, db_id);
    Ok(StatementResult::Done)
}

/// `CREATE DATABASE <name>`: a new naming namespace (level 1 — one shared
/// log and data file; nothing physical is allocated).
pub(super) fn exec_create_database(
    storage: &Storage,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    storage
        .rel_create_database(&name.value)
        .map_err(|err| match err {
            StorageError::Constraint(msg) if msg.contains("already exists") => SqlError::new(
                1801,
                16,
                3,
                format!(
                    "Database '{}' already exists. Choose a different database name.",
                    name.value
                ),
            )
            .at(name.span),
            other => map_storage_err(other, &name.value),
        })?;
    Ok(StatementResult::Done)
}

/// `DROP DATABASE [IF EXISTS] <name>`: drops the namespace and every object
/// in it. The session's current database (3702), the default database
/// (3708), and — without IF EXISTS — a missing one (3701) are refused.
pub(super) fn exec_drop_database(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
    ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if storage.rel_database_id_by_name(&name.value) == Some(ctx.database_id()) {
        return Err(SqlError::new(
            3702,
            16,
            4,
            format!(
                "Cannot drop database \"{}\" because it is currently in use.",
                name.value
            ),
        )
        .at(name.span));
    }
    match storage.rel_drop_database(&name.value) {
        Ok(true) => Ok(StatementResult::Done),
        Ok(false) if if_exists => Ok(StatementResult::Done),
        Ok(false) => Err(SqlError::new(
            3701,
            16,
            1,
            format!(
                "Cannot drop the database '{}', because it does not exist or you do not have permission.",
                name.value
            ),
        )
        .at(name.span)),
        Err(StorageError::Constraint(msg)) if msg.contains("system database") => {
            Err(SqlError::new(
                3708,
                16,
                5,
                format!(
                    "Cannot drop the database '{}' because it is a system database.",
                    name.value
                ),
            )
            .at(name.span))
        }
        Err(other) => Err(map_storage_err(other, &name.value)),
    }
}

/// `THROW`: builds the error to raise (the caller returns it — `run_block`
/// then applies THROW's batch-terminating rule). The bare form re-throws the
/// innermost `CATCH`'s error verbatim, severity included; the argument form
/// is always severity 16 with a user error number (>= 50000).
pub(super) fn exec_throw(throw: &ThrowStatement, ctx: &TxnContext) -> SqlError {
    let Some(args) = &throw.args else {
        return match ctx.error_stack.last() {
            Some(info) => {
                SqlError::new(info.number, info.severity, info.state, info.message.clone())
            }
            None => SqlError::new(
                10704,
                16,
                1,
                "To rethrow an error, a THROW statement must be used inside a CATCH block.",
            ),
        };
    };
    let eval_ctx = ctx.eval_context();
    match exec_throw_args(args, &eval_ctx) {
        // Both sides raise: the built error, or the argument evaluation's own.
        Ok(error) | Err(error) => error,
    }
}

pub(super) fn exec_throw_args(
    args: &ThrowArgs,
    eval_ctx: &EvalContext,
) -> Result<SqlError, SqlError> {
    let number = int_argument(&args.number, eval_ctx, "THROW", "error number")?;
    if !(50_000..=i64::from(i32::MAX)).contains(&number) {
        return Err(SqlError::new(
            35100,
            16,
            1,
            format!(
                "Error number {number} in the THROW statement is outside the valid range. \
                 Specify an error number in the valid range of 50000 to 2147483647."
            ),
        ));
    }
    let message = match eval_constant(&args.message, eval_ctx)? {
        SqlValue::Str(text) => text,
        other => {
            return Err(SqlError::new(
                102,
                15,
                1,
                format!(
                    "The THROW message must be a string, not {}.",
                    other.type_name()
                ),
            ));
        }
    };
    let state = int_argument(&args.state, eval_ctx, "THROW", "state")?;
    if !(0..=255).contains(&state) {
        return Err(SqlError::new(
            102,
            15,
            1,
            format!("The THROW state must be between 0 and 255, not {state}."),
        ));
    }
    Ok(SqlError::new(number as i32, 16, state as u8, message))
}

/// `RAISERROR(msg, severity, state, args...)`. Severity decides the shape:
/// <= 10 emits an informational message (a TDS INFO token, not an error) and
/// the statement SUCCEEDS; 11..=18 raises an ordinary error (statement-scope
/// — `run_block` exempts it from XACT_ABORT and never dooms for it);
/// 19..=25 additionally require `WITH LOG`, and >= 20 is fatal to the
/// connection. The error number is always 50000 (message-id RAISERROR needs
/// `sys.messages`, which TruthDB does not have — 18054 like an unknown id).
pub(super) fn exec_raiserror(
    raise: &RaiseError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let severity = int_argument(&raise.severity, &eval_ctx, "RAISERROR", "severity")?;
    if !(0..=25).contains(&severity) {
        return Err(SqlError::new(
            2754,
            16,
            1,
            format!("Error severity {severity} is out of the range 0 through 25."),
        ));
    }
    if severity > 18 && !raise.log {
        return Err(SqlError::new(
            2754,
            16,
            1,
            "Error severity levels greater than 18 can only be specified by members of the \
             sysadmin role, using the WITH LOG option.",
        ));
    }
    // State 0 is reported as 1, as SQL Server does.
    let state = int_argument(&raise.state, &eval_ctx, "RAISERROR", "state")?;
    if !(0..=255).contains(&state) {
        return Err(SqlError::new(
            2753,
            16,
            1,
            format!("The RAISERROR state must be between 0 and 255, not {state}."),
        ));
    }
    let state = (state as u8).max(1);
    let message = match eval_constant(&raise.message, &eval_ctx)? {
        SqlValue::Str(format) => {
            let mut args = Vec::with_capacity(raise.args.len());
            for arg in &raise.args {
                args.push(eval_constant(arg, &eval_ctx)?);
            }
            format_raiserror(&format, &args)?
        }
        // A message id: there is no `sys.messages`, so no id resolves.
        SqlValue::Int(id) => {
            return Err(SqlError::new(
                18054,
                16,
                1,
                format!(
                    "Error {id}, severity {severity}, state {state} was raised, but no message \
                     with that error number was found in sys.messages."
                ),
            ));
        }
        other => {
            return Err(SqlError::new(
                102,
                15,
                1,
                format!(
                    "The RAISERROR message must be a string or a message id, not {}.",
                    other.type_name()
                ),
            ));
        }
    };
    const AD_HOC_MESSAGE_NUMBER: i32 = 50000;
    if severity <= 10 {
        // Informational: `@@ERROR` reads 0 (or 50000 under SETERROR) — set
        // here because `run_block`'s success path leaves RAISERROR's value.
        txn_ctx.last_error = if raise.seterror {
            AD_HOC_MESSAGE_NUMBER
        } else {
            0
        };
        run.info(SqlError::new(
            AD_HOC_MESSAGE_NUMBER,
            severity as u8,
            state,
            message,
        ));
        return Ok(StatementOutcome::Result(StatementResult::Done));
    }
    Err(SqlError::new(
        AD_HOC_MESSAGE_NUMBER,
        severity as u8,
        state,
        message,
    ))
}

/// An integer statement argument (THROW/RAISERROR take constants or
/// variables).
pub(super) fn int_argument(
    expr: &Expr,
    eval_ctx: &EvalContext,
    statement: &str,
    what: &str,
) -> Result<i64, SqlError> {
    match eval_constant(expr, eval_ctx)? {
        SqlValue::Int(value) => Ok(value),
        other => Err(SqlError::new(
            102,
            15,
            1,
            format!(
                "The {statement} {what} must be an integer, not {}.",
                other.type_name()
            ),
        )),
    }
}

/// RAISERROR's printf subset: `%d`/`%i` (also `%u`, `%x`/`%X`, `%o`) for
/// integer arguments, `%s` for strings, `%%` for a literal percent. Anything
/// else is refused (2787), as is an argument of the wrong type or a missing
/// one (2786). Surplus arguments are ignored, as SQL Server does.
pub(super) fn format_raiserror(format: &str, args: &[SqlValue]) -> Result<String, SqlError> {
    let mut out = String::with_capacity(format.len());
    let mut next_arg = 0usize;
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(directive) = chars.next() else {
            return Err(SqlError::new(
                2787,
                16,
                1,
                "Invalid format specification: '%' at the end of the message.",
            ));
        };
        if directive == '%' {
            out.push('%');
            continue;
        }
        let argument = args.get(next_arg).ok_or_else(|| {
            SqlError::new(
                2786,
                16,
                1,
                format!(
                    "The data type of substitution parameter {} does not match the expected \
                     type of the format specification (missing argument).",
                    next_arg + 1
                ),
            )
        })?;
        let mismatch = || {
            SqlError::new(
                2786,
                16,
                1,
                format!(
                    "The data type of substitution parameter {} does not match the expected \
                     type of the format specification.",
                    next_arg + 1
                ),
            )
        };
        // A NULL argument prints "(null)" under every directive, as SQL
        // Server does. Integer arguments are int-typed (32-bit) there, so
        // the unsigned/hex forms wrap at 32 bits (-1 -> ffffffff) and a
        // value outside int range is a type mismatch (2786, the bigint
        // refusal).
        if matches!(argument, SqlValue::Null) {
            out.push_str("(null)");
            next_arg += 1;
            continue;
        }
        let int_arg = || -> Result<i32, SqlError> {
            match argument {
                SqlValue::Int(value) => i32::try_from(*value).map_err(|_| mismatch()),
                _ => Err(mismatch()),
            }
        };
        match directive {
            'd' | 'i' => out.push_str(&int_arg()?.to_string()),
            'u' => out.push_str(&(int_arg()? as u32).to_string()),
            'x' => out.push_str(&format!("{:x}", int_arg()? as u32)),
            'X' => out.push_str(&format!("{:X}", int_arg()? as u32)),
            'o' => out.push_str(&format!("{:o}", int_arg()? as u32)),
            's' => match argument {
                SqlValue::Str(value) => out.push_str(value),
                _ => return Err(mismatch()),
            },
            other => {
                return Err(SqlError::new(
                    2787,
                    16,
                    1,
                    format!("Invalid format specification: '%{other}'."),
                ));
            }
        }
        next_arg += 1;
    }
    Ok(out)
}

pub(super) fn exec_begin(
    storage: &Storage,
    ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    if ctx.txn.is_none() {
        ctx.txn = Some(storage.rel_begin().map_err(|e| map_storage_err(e, ""))?);
    }
    // Nested BEGIN only bumps the count (SQL Server semantics).
    ctx.trancount += 1;
    Ok(StatementResult::Done)
}

pub(super) fn exec_commit(
    storage: &Storage,
    ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3902,
            16,
            1,
            "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    ctx.trancount -= 1;
    // Only the outermost COMMIT actually commits.
    if ctx.trancount == 0
        && let Some(txn) = ctx.txn.take()
    {
        ctx.savepoints.clear();
        // The transaction is over either way the commit goes.
        ctx.release_txn_snapshot(storage);
        storage
            .rel_commit(txn)
            .map_err(|e| map_storage_err(e, ""))?;
    }
    Ok(StatementResult::Done)
}

pub(super) fn exec_rollback(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: Option<&Name>,
) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3903,
            16,
            1,
            "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    // ROLLBACK <savepoint>: partial rollback — the transaction stays open and
    // @@TRANCOUNT is unchanged; only the work done since the savepoint is undone.
    if let Some(name) = name {
        let Some(savepoint) = ctx
            .savepoints
            .get(&name.value.to_ascii_lowercase())
            .copied()
        else {
            return Err(SqlError::new(
                3908,
                16,
                1,
                format!(
                    "Cannot roll back {}. No transaction or savepoint of that name was found.",
                    name.value
                ),
            ));
        };
        if let Some(txn) = ctx.txn.as_mut() {
            storage
                .rel_rollback_to(txn, savepoint)
                .map_err(|e| map_storage_err(e, ""))?;
        }
        // Savepoints taken after this one are invalidated — their undo-log suffix
        // was just discarded (the target savepoint itself remains re-usable).
        ctx.savepoints
            .retain(|_, sp| sp.undo_len <= savepoint.undo_len);
        return Ok(StatementResult::Done);
    }
    // ROLLBACK (whole transaction), regardless of nesting. Reset the session's
    // transaction counters even if the storage rollback fails (which wedges the
    // store): the transaction is over either way, so leaving @@TRANCOUNT /
    // doomed set would desync the session.
    let result = match ctx.txn.take() {
        Some(txn) => storage
            .rel_rollback(txn)
            .map_err(|e| map_storage_err(e, "")),
        None => Ok(()),
    };
    ctx.release_txn_snapshot(storage);
    ctx.trancount = 0;
    ctx.doomed = false;
    ctx.savepoints.clear();
    result.map(|()| StatementResult::Done)
}

/// `SAVE TRANSACTION <name>`: record a savepoint the transaction can later roll
/// back to. Requires an active transaction (in autocommit there is nothing to
/// save, so it is a no-op). Re-saving an existing name overwrites it, as in
/// SQL Server.
pub(super) fn exec_save(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    if let Some(txn) = ctx.txn.as_ref() {
        let savepoint = storage.rel_savepoint(txn);
        ctx.savepoints
            .insert(name.value.to_ascii_lowercase(), savepoint);
    }
    Ok(StatementResult::Done)
}

/// A declared cursor: its query, and — once OPENed — the materialized result and
/// the current position (0 = before the first row; 1..=len = on a row; len+1 =
/// after the last). Static: the rows are snapshotted at OPEN.
pub(super) struct CursorState {
    select: Box<Select>,
    columns: Vec<ResultColumn>,
    rows: Vec<Vec<Datum>>,
    position: i64,
    open: bool,
}

pub(super) fn cursor_not_declared(name: &Name) -> SqlError {
    SqlError::new(
        16916,
        16,
        1,
        format!("A cursor with the name '{}' does not exist.", name.value),
    )
    .at(name.span)
}

pub(super) fn cursor_not_open(name: &Name) -> SqlError {
    SqlError::new(16917, 16, 1, "The cursor is not open.".to_string()).at(name.span)
}

pub(super) fn exec_declare_cursor(
    ctx: &mut TxnContext,
    name: &Name,
    select: &Select,
) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    if ctx.cursors.contains_key(&key) {
        return Err(SqlError::new(
            16915,
            16,
            1,
            format!("The cursor name '{}' already exists.", name.value),
        )
        .at(name.span));
    }
    ctx.cursors.insert(
        key,
        CursorState {
            select: Box::new(select.clone()),
            columns: Vec::new(),
            rows: Vec::new(),
            position: 0,
            open: false,
        },
    );
    Ok(StatementResult::Done)
}

pub(super) fn exec_open_cursor(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    let cursor = ctx
        .cursors
        .get(&key)
        .ok_or_else(|| cursor_not_declared(name))?;
    if cursor.open {
        return Err(
            SqlError::new(16905, 16, 1, "The cursor is already open.".to_string()).at(name.span),
        );
    }
    let select = cursor.select.clone();
    let eval_ctx = ctx.eval_context();
    let rowset = exec_select(storage, &select, &eval_ctx)?;
    let cursor = ctx.cursors.get_mut(&key).expect("cursor declared");
    cursor.columns = rowset.columns;
    cursor.rows = rowset.rows;
    cursor.position = 0;
    cursor.open = true;
    Ok(StatementResult::Done)
}

pub(super) fn exec_close_cursor(
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    let cursor = ctx
        .cursors
        .get_mut(&key)
        .ok_or_else(|| cursor_not_declared(name))?;
    if !cursor.open {
        return Err(cursor_not_open(name));
    }
    cursor.open = false;
    cursor.rows = Vec::new();
    cursor.columns = Vec::new();
    cursor.position = 0;
    Ok(StatementResult::Done)
}

pub(super) fn exec_deallocate_cursor(
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    if ctx.cursors.remove(&key).is_none() {
        return Err(cursor_not_declared(name));
    }
    Ok(StatementResult::Done)
}

pub(super) fn exec_fetch(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
    direction: &FetchDirection,
    into: &[String],
) -> Result<StatementResult, SqlError> {
    let _ = storage;
    let key = name.value.to_ascii_lowercase();
    // Evaluate an ABSOLUTE/RELATIVE offset (it may reference variables) up front.
    let offset = match direction {
        FetchDirection::Absolute(e) | FetchDirection::Relative(e) => {
            let eval_ctx = ctx.eval_context();
            Some(match eval_constant(e, &eval_ctx)? {
                SqlValue::Int(i) => i,
                SqlValue::Null => 0,
                _ => {
                    return Err(SqlError::message_only(
                        16924,
                        "The FETCH offset must be an integer.".to_string(),
                    ));
                }
            })
        }
        _ => None,
    };
    // Compute the target 1-based position from an immutable read of the cursor.
    let (columns, fetched, new_position, in_range) = {
        let cursor = ctx
            .cursors
            .get(&key)
            .ok_or_else(|| cursor_not_declared(name))?;
        if !cursor.open {
            return Err(cursor_not_open(name));
        }
        let n = cursor.rows.len() as i64;
        let mut target = match direction {
            FetchDirection::Next => cursor.position + 1,
            FetchDirection::Prior => cursor.position - 1,
            FetchDirection::First => 1,
            FetchDirection::Last => n,
            FetchDirection::Absolute(_) => offset.unwrap_or(0),
            // Saturate: a huge offset overflows `position + offset` (i64), which
            // panics in a checked build and silently wraps in release. Saturating
            // lands off the end, where the range check below maps it to -1.
            FetchDirection::Relative(_) => cursor.position.saturating_add(offset.unwrap_or(0)),
        };
        // ABSOLUTE -1 addresses the last row, -2 the second-to-last, etc.
        if matches!(direction, FetchDirection::Absolute(_)) && target < 0 {
            target = n + target + 1;
        }
        if target >= 1 && target <= n {
            (
                cursor.columns.clone(),
                Some(cursor.rows[(target - 1) as usize].clone()),
                target,
                true,
            )
        } else {
            (cursor.columns.clone(), None, target.clamp(0, n + 1), false)
        }
    };
    ctx.cursors.get_mut(&key).expect("cursor").position = new_position;
    if !in_range {
        // Off either end: @@FETCH_STATUS = -1, no row produced.
        ctx.fetch_status = -1;
        return Ok(StatementResult::Done);
    }
    ctx.fetch_status = 0;
    let row = fetched.expect("row in range");
    if into.is_empty() {
        // No INTO: the fetched row is returned to the client as a result set.
        return Ok(StatementResult::Rows(RowSet {
            columns,
            rows: vec![row],
        }));
    }
    if into.len() != columns.len() {
        return Err(SqlError::new(
            16924,
            16,
            1,
            "The number of variables declared in the INTO list must match that of selected columns."
                .to_string(),
        )
        .at(name.span));
    }
    let types: Vec<ColumnType> = columns.iter().map(|c| c.column_type).collect();
    for (var, (value, col_type)) in into.iter().zip(row.iter().zip(&types)) {
        let var_type = ctx
            .variables
            .get(var)
            .map(|(t, _)| *t)
            .ok_or_else(|| undeclared_variable_err(var))?;
        let sql_value = value::datum_to_sql(value, col_type);
        let expr = Expr {
            kind: ExprKind::Literal(sql_value),
            span: name.span,
        };
        let eval_ctx = ctx.eval_context();
        let coerced = coerce_variable(&expr, &var_type, var, &eval_ctx)?;
        ctx.variables.insert(var.clone(), (var_type, coerced));
    }
    Ok(StatementResult::Done)
}

pub(super) fn exec_set(
    ctx: &mut TxnContext,
    set: &SetStatement,
) -> Result<StatementResult, SqlError> {
    match set {
        SetStatement::XactAbort(on) => ctx.xact_abort = *on,
        SetStatement::IsolationLevel(level) => {
            ctx.isolation = match level {
                IsolationLevel::ReadUncommitted => Isolation::ReadUncommitted,
                IsolationLevel::ReadCommitted => Isolation::ReadCommitted,
                IsolationLevel::RepeatableRead => Isolation::RepeatableRead,
                IsolationLevel::Serializable => Isolation::Serializable,
                IsolationLevel::Snapshot => Isolation::Snapshot,
            }
        }
        SetStatement::ShowplanText(on) => ctx.showplan_text = *on,
        SetStatement::NoCount(on) => ctx.nocount = *on,
        SetStatement::Variable { name, value } => {
            // "Statements that make a simple assignment always set the
            // @@ROWCOUNT value to 1" — the Done result would reset it to 0,
            // so the assignment records its own count here.
            ctx.rowcount = 1;
            let column_type = ctx
                .variables
                .get(name)
                .map(|(t, _)| *t)
                .ok_or_else(|| undeclared_variable_err(name))?;
            let eval_ctx = ctx.eval_context();
            let coerced = coerce_variable(value, &column_type, name, &eval_ctx)?;
            ctx.variables.insert(name.clone(), (column_type, coerced));
        }
        SetStatement::Ignored => {}
    }
    Ok(StatementResult::Done)
}

/// `DECLARE @a TYPE [= expr], ...`. Each variable is added to the batch (error
/// 134 if already declared); an initializer (which may reference an earlier
/// variable) is coerced to the declared type, else the value starts NULL.
pub(super) fn exec_declare(
    ctx: &mut TxnContext,
    decls: &[Declaration],
) -> Result<StatementResult, SqlError> {
    for decl in decls {
        // A name occupies the scalar and table-variable stores jointly, so a
        // scalar DECLARE after a `DECLARE @t TABLE` of the same name is 134 too.
        if ctx.variables.contains_key(&decl.name) || ctx.table_variables.contains_key(&decl.name) {
            return Err(SqlError::new(
                134,
                15,
                2,
                format!(
                    "The variable name '@{}' has already been declared. Variable names must be unique within a query batch.",
                    decl.name
                ),
            ));
        }
        let column_type = data_type_to_column_type(&decl.data_type, &decl.name)?;
        let value = match &decl.initializer {
            Some(expr) => {
                let eval_ctx = ctx.eval_context();
                coerce_variable(expr, &column_type, &decl.name, &eval_ctx)?
            }
            None => SqlValue::Null,
        };
        ctx.variables
            .insert(decl.name.clone(), (column_type, value));
    }
    Ok(StatementResult::Done)
}

/// `DECLARE @t TABLE ( ... )`: registers an empty in-memory table variable. Its
/// schema is bound like a base table's columns; its declared PRIMARY KEY becomes
/// the key columns used for uniqueness at INSERT time.
pub(super) fn exec_declare_table_var(
    ctx: &mut TxnContext,
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[Name],
) -> Result<StatementResult, SqlError> {
    // A name occupies the scalar and table-variable stores jointly.
    if ctx.variables.contains_key(name) || ctx.table_variables.contains_key(name) {
        return Err(SqlError::new(
            134,
            15,
            2,
            format!(
                "The variable name '@{name}' has already been declared. Variable names must be \
                 unique within a query batch."
            ),
        ));
    }
    let (schema, key_columns, defaults) = build_table_var_definition(name, columns, primary_key)?;
    ctx.table_variables.insert(
        name.to_string(),
        TableVar {
            schema,
            key_columns,
            defaults,
            rows: Vec::new(),
        },
    );
    Ok(StatementResult::Done)
}

/// A table variable's built definition: its column schema, the schema indices of
/// its PRIMARY KEY columns, and the per-column DEFAULT source text (parallel to
/// the schema columns).
pub(super) type TableVarDefinition = (Schema, Vec<usize>, Vec<Option<String>>);

/// Builds the schema, key-column indices, and per-column DEFAULT source text for
/// a table-variable declaration (`DECLARE @name TABLE(cols)` and the RETURNS
/// clause of a multi-statement TVF share this): unique column names (2705), PK
/// columns forced NOT NULL (8111 on explicit-NULL, MAX-key rejected), and the
/// DEFAULT texts applied per INSERT. `name` (without `@`) names the table in the
/// error messages.
pub(super) fn build_table_var_definition(
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[Name],
) -> Result<TableVarDefinition, SqlError> {
    // Column names within the table variable must be unique (2705), the same
    // rule a base table enforces in exec_create_table.
    let mut seen: Vec<&str> = Vec::new();
    for column in columns {
        if seen
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&column.name.value))
        {
            return Err(SqlError::new(
                2705,
                16,
                3,
                format!(
                    "Column names in each table must be unique. Column name '{}' is specified more than once.",
                    column.name.value
                ),
            )
            .at(column.name.span));
        }
        seen.push(&column.name.value);
    }
    let bound = columns
        .iter()
        .map(bind_column)
        .collect::<Result<Vec<_>, _>>()?;
    let mut schema = Schema { columns: bound };
    let mut key_columns = Vec::new();
    for pk in primary_key {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&pk.value))
            .ok_or_else(|| {
                SqlError::new(
                    1911,
                    16,
                    1,
                    format!(
                        "Column name '{}' does not exist in the target table or view.",
                        pk.value
                    ),
                )
            })?;
        // A PRIMARY KEY column is implicitly NOT NULL; declaring it NULL is
        // 8111, and a MAX-typed column cannot be a key — the same rules a base
        // table enforces in exec_create_table.
        let declared_null = columns
            .iter()
            .find(|c| c.name.eq_ignore_case(&pk.value))
            .and_then(|c| c.nullable)
            == Some(true);
        if declared_null {
            return Err(SqlError::new(
                8111,
                16,
                1,
                format!(
                    "Cannot define PRIMARY KEY constraint on nullable column in table '@{name}'."
                ),
            ));
        }
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&pk.value, &format!("@{name}")).at(pk.span));
        }
        schema.columns[index].nullable = false;
        key_columns.push(index);
    }
    // Per-column DEFAULT source text (parallel to the schema columns), applied
    // at INSERT to columns left unspecified — same as a base table.
    let defaults: Vec<Option<String>> = columns.iter().map(|c| c.default.clone()).collect();
    Ok((schema, key_columns, defaults))
}

pub(super) fn undeclared_variable_err(name: &str) -> SqlError {
    SqlError::new(
        137,
        15,
        2,
        format!("Must declare the scalar variable \"@{name}\"."),
    )
}

/// Evaluates a variable initializer/assignment (a constant expression that may
/// reference already-declared variables) and coerces it to the declared type.
pub(super) fn coerce_variable(
    expr: &Expr,
    column_type: &ColumnType,
    name: &str,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let sql_value = eval_constant(expr, eval_ctx)?;
    let datum = value::sql_to_datum(&sql_value, column_type, name)?;
    Ok(value::datum_to_sql(&datum, column_type))
}
