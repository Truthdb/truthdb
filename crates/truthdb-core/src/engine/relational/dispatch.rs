use super::prelude::*;

/// Parses and executes a SQL batch, returning one result per statement, or
/// the first error (discarding earlier results). Kept for tests; the server
/// uses [`execute_batch`].
#[cfg(test)]
#[allow(dead_code)]
pub fn execute(storage: &Storage, sql: &str) -> Result<Vec<StatementResult>, SqlError> {
    let mut txn_ctx = TxnContext::default();
    let outcome = execute_batch(storage, sql, &mut txn_ctx);
    match outcome.error {
        Some(error) => Err(error),
        None => Ok(outcome.results),
    }
}

impl TxnContext {
    fn scope(&mut self) -> TxnScope<'_> {
        match &mut self.txn {
            Some(txn) => TxnScope::Explicit(txn),
            None => TxnScope::Auto,
        }
    }
}

pub(super) fn exec_statement(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // A doomed (uncommittable) transaction rejects log writes with 3930, but —
    // like SQL Server — still allows reads (`SELECT`), `SET`, `DECLARE`, and a
    // full `ROLLBACK`, so a CATCH block can inspect `XACT_STATE()`/`ERROR_*()`
    // and then roll back. A partial rollback to a savepoint and `SAVE` stay
    // rejected (an uncommittable transaction can only be fully rolled back).
    if txn_ctx.doomed && !doomed_allows(statement) {
        return Err(SqlError::new(
            3930,
            16,
            1,
            "The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.",
        ));
    }
    let result = exec_statement_dispatch(storage, statement, txn_ctx);
    // SQL Server rolls a SNAPSHOT transaction back entirely on an update
    // conflict — "transaction aborted", not statement-failed-transaction-
    // doomed. @@TRANCOUNT drops to zero and the session continues.
    if let Err(error) = &result
        && error.number == 3960
        && txn_ctx.in_txn()
    {
        txn_ctx.abort(storage);
    }
    result
}

pub(super) fn exec_statement_dispatch(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // A session whose current database was dropped errors on every statement
    // except USE (its way out) — never silently resolving in a dead
    // namespace. Dropped ids are tombstoned (never reallocated), so this
    // check is exact. The per-batch snapshot makes it one Vec scan.
    if !matches!(statement, Statement::Use { .. })
        && !txn_ctx
            .databases_snapshot
            .iter()
            .any(|(id, _)| *id == txn_ctx.database_id())
        && !txn_ctx.databases_snapshot.is_empty()
    {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                txn_ctx.database
            ),
        ));
    }
    // DDL (schema + security) requires a privileged principal (sysadmin / dbo /
    // db_owner / the internal channel). A restricted database user is refused
    // before any change is made.
    if !txn_ctx.security.bypass && is_privileged_ddl(statement) {
        return Err(SqlError::new(
            15247,
            16,
            1,
            "User does not have permission to perform this action.".to_string(),
        ));
    }
    match statement {
        Statement::BeginTransaction { .. } => exec_begin(storage, txn_ctx),
        Statement::Use { database, .. } => exec_use(storage, database, txn_ctx),
        Statement::Throw(throw) => Err(exec_throw(throw, txn_ctx)),
        Statement::CreateProcedure(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_procedure(storage, txn_ctx.database_id(), create)
        }
        Statement::DropProcedure {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_procedure(storage, txn_ctx.database_id(), name, *if_exists)
        }
        Statement::CreateFunction(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_function(storage, txn_ctx.database_id(), create)
        }
        Statement::DropFunction {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_function(storage, txn_ctx.database_id(), name, *if_exists)
        }
        Statement::CreateTrigger(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_trigger(storage, txn_ctx.database_id(), create)
        }
        Statement::DropTrigger {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_trigger(storage, txn_ctx.database_id(), name, *if_exists)
        }
        Statement::SetTriggerState {
            trigger,
            table,
            enable,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_set_trigger_state(storage, txn_ctx.database_id(), trigger, table, *enable)
        }
        Statement::CreateLogin(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_login(storage, create)
        }
        Statement::DropLogin {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_login(storage, name, *if_exists)
        }
        Statement::CreateUser(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_user(storage, create)
        }
        Statement::DropUser {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_database_principal(storage, name, *if_exists, false)
        }
        Statement::CreateRole { name, .. } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_role(storage, name)
        }
        Statement::DropRole {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_database_principal(storage, name, *if_exists, true)
        }
        Statement::AlterRole {
            name,
            action,
            member,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_alter_role_member(storage, name, *action, member)
        }
        Statement::Permission(stmt) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_permission(storage, txn_ctx.database_id(), stmt, &txn_ctx.security)
        }
        Statement::BackupDatabase {
            database,
            path,
            checksum,
            copy_only,
            ..
        } => {
            // BACKUP manages its own (per-chunk) locking, so it cannot run
            // inside a transaction that holds locks, and it is a privileged
            // operation (gated by is_privileged_ddl above).
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    3021,
                    16,
                    1,
                    "Cannot perform a backup or restore operation within a transaction."
                        .to_string(),
                ));
            }
            // Any catalog database is a valid target: a backup is
            // instance-granular (it contains every database) — the name is
            // validated, not scoping.
            if storage.rel_database_id_by_name(&database.value).is_none() {
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
            }
            storage
                .backup_full_with(std::path::Path::new(path), *checksum, *copy_only)
                .map_err(|e| {
                    SqlError::new(
                        3013,
                        16,
                        1,
                        format!("BACKUP DATABASE is terminating abnormally. {e}"),
                    )
                })?;
            Ok(StatementResult::Done)
        }
        Statement::BackupLog {
            database,
            path,
            checksum,
            copy_only,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    3021,
                    16,
                    1,
                    "Cannot perform a backup or restore operation within a transaction."
                        .to_string(),
                ));
            }
            if storage.rel_database_id_by_name(&database.value).is_none() {
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
            }
            if !storage.recovery_model_full() {
                return Err(SqlError::new(
                    4208,
                    16,
                    1,
                    "The statement BACKUP LOG is not allowed while the recovery model is SIMPLE. \
                     Use BACKUP DATABASE or change the recovery model to FULL with ALTER DATABASE."
                        .to_string(),
                ));
            }
            storage
                .backup_log(std::path::Path::new(path), *checksum, *copy_only)
                .map_err(|e| {
                    SqlError::new(
                        3013,
                        16,
                        1,
                        format!("BACKUP LOG is terminating abnormally. {e}"),
                    )
                })?;
            Ok(StatementResult::Done)
        }
        Statement::Restore { mode, path, .. } => exec_restore(*mode, path, txn_ctx),
        Statement::DeclareCursor { name, select, .. } => exec_declare_cursor(txn_ctx, name, select),
        Statement::OpenCursor { name, .. } => exec_open_cursor(storage, txn_ctx, name),
        Statement::FetchCursor {
            name,
            direction,
            into,
            ..
        } => exec_fetch(storage, txn_ctx, name, direction, into),
        Statement::CloseCursor { name, .. } => exec_close_cursor(txn_ctx, name),
        Statement::DeallocateCursor { name, .. } => exec_deallocate_cursor(txn_ctx, name),
        // Executed by `run_block`'s own arms; nothing routes them here.
        Statement::Block { .. }
        | Statement::If { .. }
        | Statement::While { .. }
        | Statement::Break { .. }
        | Statement::Continue { .. }
        | Statement::Return { .. }
        | Statement::Goto { .. }
        | Statement::Label { .. } => {
            unreachable!("control flow is executed by run_block")
        }
        // Handled in `exec_statement_streamed_inner` (severity <= 10 emits an
        // INFO event, which needs the emitter); nothing else routes it here.
        Statement::RaiseError(_) => unreachable!("RAISERROR reaches only the streaming executor"),
        Statement::Commit { .. } => exec_commit(storage, txn_ctx),
        Statement::Rollback { name, .. } => exec_rollback(storage, txn_ctx, name.as_ref()),
        Statement::SaveTransaction { name, .. } => exec_save(storage, txn_ctx, name),
        Statement::Set(set) => exec_set(txn_ctx, set),
        Statement::Declare(decls) => exec_declare(txn_ctx, decls),
        Statement::DeclareTableVar {
            name,
            columns,
            primary_key,
            ..
        } => exec_declare_table_var(txn_ctx, name, columns, primary_key),
        Statement::CreateTable(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_table(storage, txn_ctx.database_id(), create)
        }
        Statement::DropTable(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_table(storage, txn_ctx.database_id(), drop)
        }
        Statement::CreateView(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_view(storage, txn_ctx.database_id(), create)
        }
        Statement::DropView(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_view(storage, txn_ctx.database_id(), drop)
        }
        Statement::CreateIndex(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_index(storage, txn_ctx.database_id(), create)
        }
        Statement::DropIndex(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_index(storage, txn_ctx.database_id(), drop)
        }
        Statement::AlterTable(alter) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            let eval_ctx = txn_ctx.eval_context();
            exec_alter_table(storage, txn_ctx.database_id(), alter, &eval_ctx)
        }
        Statement::AlterDatabase(alter) => {
            if txn_ctx.in_txn() {
                // SQL Server 226: ALTER DATABASE is not allowed inside a
                // multi-statement transaction.
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "ALTER DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_alter_database(storage, alter, txn_ctx)
        }
        Statement::CreateDatabase { name, .. } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "CREATE DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_create_database(storage, name)
        }
        Statement::DropDatabase {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "DROP DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_drop_database(storage, name, *if_exists, txn_ctx)
        }
        Statement::Insert(insert) => {
            // INSERT into a `@t` table variable is pure session memory (no
            // Storage, no lock, no WAL) — handled here where `&mut TxnContext`
            // is in hand, before the storage scope is taken.
            if insert.table.value.starts_with('@') {
                let eval_ctx = txn_ctx.eval_context();
                return exec_insert_table_var(storage, insert, txn_ctx, &eval_ctx);
            }
            let (target, after, instead_of) = triggers_for(
                storage,
                txn_ctx.database_id(),
                &insert.table.value,
                catalog::TriggerEvent::Insert,
            );
            let run_insert = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let (result, identity) = {
                    let mut scope = txn_ctx.scope();
                    exec_insert(storage, insert, &mut scope, &eval_ctx)?
                };
                // An identity INSERT updates SCOPE_IDENTITY(); a non-identity one
                // (identity == None) leaves it unchanged.
                if let Some(value) = identity {
                    txn_ctx.scope_identity = Some(value);
                }
                Ok(result)
            };
            match target {
                Some(target) => {
                    if let Some(io) = instead_of {
                        run_instead_of(storage, txn_ctx, &target, io, |eval_ctx| {
                            instead_of_insert_images(storage, insert, &target, eval_ctx)
                        })
                    } else if !after.is_empty() {
                        run_dml_with_triggers(storage, txn_ctx, &target, after, run_insert)
                    } else {
                        run_insert(txn_ctx)
                    }
                }
                None => run_insert(txn_ctx),
            }
        }
        Statement::Update(update) => {
            let (target, after, instead_of) = triggers_for(
                storage,
                txn_ctx.database_id(),
                &update.table.value,
                catalog::TriggerEvent::Update,
            );
            let run_update = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let mut scope = txn_ctx.scope();
                exec_update(storage, update, &mut scope, &eval_ctx)
            };
            match target {
                Some(target) => {
                    if let Some(io) = instead_of {
                        run_instead_of(storage, txn_ctx, &target, io, |eval_ctx| {
                            instead_of_update_images(storage, update, &target, eval_ctx)
                        })
                    } else if !after.is_empty() {
                        run_dml_with_triggers(storage, txn_ctx, &target, after, run_update)
                    } else {
                        run_update(txn_ctx)
                    }
                }
                None => run_update(txn_ctx),
            }
        }
        Statement::Delete(delete) => {
            let (target, after, instead_of) = triggers_for(
                storage,
                txn_ctx.database_id(),
                &delete.table.value,
                catalog::TriggerEvent::Delete,
            );
            let run_delete = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let mut scope = txn_ctx.scope();
                exec_delete(storage, delete, &mut scope, &eval_ctx)
            };
            match target {
                Some(target) => {
                    if let Some(io) = instead_of {
                        run_instead_of(storage, txn_ctx, &target, io, |eval_ctx| {
                            instead_of_delete_images(storage, delete, &target, eval_ctx)
                        })
                    } else if !after.is_empty() {
                        run_dml_with_triggers(storage, txn_ctx, &target, after, run_delete)
                    } else {
                        run_delete(txn_ctx)
                    }
                }
                None => run_delete(txn_ctx),
            }
        }
        Statement::Select(select) => {
            if select
                .items
                .iter()
                .any(|i| matches!(i, SelectItem::Assign { .. }))
            {
                return exec_select_assign(storage, select, txn_ctx);
            }
            let eval_ctx = txn_ctx.eval_context();
            if txn_ctx.showplan_text {
                Ok(StatementResult::Rows(showplan_rows(
                    storage, select, &eval_ctx,
                )?))
            } else {
                Ok(StatementResult::Rows(exec_select(
                    storage, select, &eval_ctx,
                )?))
            }
        }
        // TRY/CATCH is control flow, handled by `run_block`, which never routes
        // it here.
        Statement::TryCatch { .. } => Err(SqlError::message_only(
            0,
            "internal error: TRY/CATCH must be executed by run_block",
        )),
        // EXEC recurses into its inner batch, handled by `run_block` too.
        Statement::Exec(_) => Err(SqlError::message_only(
            0,
            "internal error: EXEC must be executed by run_block",
        )),
    }
}

/// Statements a doomed (uncommittable) transaction still permits: reads
/// (`SELECT`, including `SELECT @v = ...`), session-state changes (`SET`,
/// `DECLARE`), and a full `ROLLBACK`. Everything else (DML/DDL, `COMMIT`,
/// `SAVE`, a partial `ROLLBACK` to a savepoint) writes to the log and is
/// rejected with 3930.
pub(super) fn doomed_allows(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Select(_)
            | Statement::Set(_)
            | Statement::Declare(_)
            | Statement::Use { .. }
            | Statement::Throw(_)
            | Statement::RaiseError(_)
            | Statement::Rollback { name: None, .. }
    )
}

/// Flattens `TRY`/`CATCH` blocks into the leaf statements they contain, so lock
/// analysis (which pre-acquires every table lock a batch needs) sees the
/// statements nested inside try/catch blocks too.
pub(super) fn flatten_statements<'a>(statements: &'a [Statement], out: &mut Vec<&'a Statement>) {
    for statement in statements {
        match statement {
            Statement::TryCatch {
                try_block,
                catch_block,
                ..
            } => {
                flatten_statements(try_block, out);
                flatten_statements(catch_block, out);
            }
            Statement::Block { body, .. } => flatten_statements(body, out),
            // IF/WHILE stay in the list (their CONDITIONS take read locks);
            // their bodies flatten so the leaf statements analyze as
            // themselves — a WHILE body's INSERT needs its lock up front like
            // any other, and both IF branches are analyzed (conservative:
            // which one runs is a runtime fact).
            Statement::If {
                then_branch,
                else_branch,
                ..
            } => {
                out.push(statement);
                flatten_statements(std::slice::from_ref(then_branch), out);
                if let Some(else_branch) = else_branch {
                    flatten_statements(std::slice::from_ref(else_branch), out);
                }
            }
            Statement::While { body, .. } => {
                out.push(statement);
                flatten_statements(std::slice::from_ref(body), out);
            }
            other => out.push(other),
        }
    }
}

/// Builds a one-column `SHOWPLAN_TEXT` rowset describing a SELECT's access
/// path, without executing it.
pub(super) fn showplan_rows(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let lines = match select.from.as_ref() {
        None => vec!["Constant Scan".to_string()],
        Some(TableRef::Table { name, .. })
            if !name.value.to_ascii_lowercase().starts_with("sys.") =>
        {
            match resolve_table(storage, eval_ctx.database_id, &name.value) {
                Some(def) => {
                    // The scan shape carries the covering decision (it knows
                    // which columns the query reads); other shapes never
                    // cover, so the plain choose() answer is exact for them.
                    if let Some(plan) = scan_plan(storage, select, eval_ctx) {
                        plan::plan_text(&plan.access, &def.name, plan.covering)
                    } else {
                        let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
                        // Fetched only when choose() can use it (it returns a
                        // scan outright without a predicate or indexes).
                        let row_count = if def.indexes.is_empty() || select.where_clause.is_none() {
                            None
                        } else {
                            storage.rel_row_count(def.database_id, &def.name)
                        };
                        let path = plan::choose(
                            &def,
                            &schema,
                            &select.where_clause,
                            eval_ctx,
                            None,
                            row_count,
                        );
                        plan::plan_text(&path, &def.name, false)
                    }
                }
                None => vec![format!("Table Scan({})", name.value)],
            }
        }
        Some(TableRef::Table { name, .. }) => vec![format!("Table Scan({})", name.value)],
        // A lone table-valued function call: name it honestly rather than
        // letting it fall into the join catch-all (which would invent a
        // "Nested Loops" over a phantom base table named after the function).
        Some(TableRef::Function { name, .. }) => {
            vec![format!("Table-valued Function({})", name.value)]
        }
        Some(join) => {
            // Multi-table: a nested-loop join over full scans (Stage 8).
            let mut tables = Vec::new();
            collect_table_names(join, &mut tables);
            let mut lines = vec!["Nested Loops (join)".to_string()];
            for table in tables {
                lines.push(format!("  Table Scan({})", strip_schema(&table.value)));
            }
            lines
        }
    };
    Ok(RowSet {
        columns: vec![ResultColumn {
            name: "StmtText".to_string(),
            column_type: ColumnType::NVarChar { max_len: 4000 },
        }],
        rows: lines
            .into_iter()
            .map(|line| vec![Datum::NVarChar(line)])
            .collect(),
    })
}

pub(super) fn ddl_in_txn_err() -> SqlError {
    SqlError::new(
        226,
        16,
        1,
        "DDL statements are not allowed inside an explicit transaction in this version.",
    )
}
