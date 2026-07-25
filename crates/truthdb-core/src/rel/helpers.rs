use super::prelude::*;

// ---- helpers ------------------------------------------------------------

/// Evaluates a constant expression (INSERT VALUES): no columns in scope.
pub(super) fn eval_constant(expr: &Expr, eval_ctx: &EvalContext) -> Result<SqlValue, SqlError> {
    let empty: Vec<String> = Vec::new();
    eval::eval(expr, &[], &empty, eval_ctx)
}

pub(super) fn column_index(schema: &Schema, name: &str) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

/// Strips an optional `dbo.` schema prefix (Stage 3 has a single user
/// schema); `sys.` names are handled separately as catalog views.
pub(super) fn strip_schema(name: &str) -> &str {
    name.split_once('.')
        .filter(|(schema, _)| schema.eq_ignore_ascii_case("dbo"))
        .map(|(_, rest)| rest)
        .unwrap_or(name)
}

/// Case-insensitive table resolution (single `dbo` schema in Stage 3). An
/// optional `dbo.` schema prefix is accepted and stripped.
/// The base-table object ids that a read of `name` must Shared-lock: the table
/// itself, or — for a view — the base tables its definition reads. `sys.*`
/// views take no lock. Nested views (a view over a view) are not expanded here
/// (they error at query time), so they contribute no locks; view expansion is
/// one level deep, matching the executor.
pub(super) fn read_lock_object_ids(storage: &Storage, db_id: u32, name: &str) -> Vec<u32> {
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    collect_read_lock_ids(storage, db_id, name, 0, &mut out, &mut visited);
    out
}

/// Adds the locks the AFTER-`event` trigger bodies of `parent_object_id` take,
/// so a DML that fires them holds every lock its bodies acquire UP FRONT (strict
/// 2PL — a trigger body reading/writing another table with no pre-acquired lock
/// is the recurring seam-defect class). Each body is analyzed by the SAME
/// machinery the batch uses (`analyze_statements_locks`), so its EXEC, TRY/CATCH,
/// FK-integrity reads, subquery reads, and its own nested triggers are ALL
/// covered — not a hand-rolled subset. Recursion (a trigger whose body DMLs a
/// table with its own triggers) is bounded by `trigger_visited` (trigger
/// object_ids), so a trigger cycle terminates cleanly rather than hanging
/// analysis under the scheduler mutex.
#[allow(clippy::too_many_arguments)]
pub(super) fn add_trigger_locks(
    db_id: u32,
    storage: &Storage,
    parent_object_id: u32,
    event: catalog::TriggerEvent,
    isolation: Isolation,
    visited: &mut std::collections::HashSet<(String, Isolation)>,
    trigger_visited: &mut std::collections::HashSet<(u32, Isolation)>,
    add: &mut impl FnMut(Resource, LockMode),
) {
    for trig in storage.rel_triggers_for(parent_object_id, event) {
        if !trigger_visited.insert((trig.object_id, isolation)) {
            continue;
        }
        let Some(t) = &trig.trigger else { continue };
        if let Ok(statements) = truthdb_sql::parse_procedure_body(&t.body) {
            for (resource, mode) in analyze_statements_locks(
                storage,
                db_id,
                &statements,
                isolation,
                visited,
                trigger_visited,
            ) {
                add(resource, mode);
            }
        }
    }
}

/// The base-table object ids the scalar functions called in a SELECT read
/// through their bodies (deduped). Non-function names collected along the way
/// resolve to nothing.
pub(super) fn select_function_read_ids(storage: &Storage, db_id: u32, select: &Select) -> Vec<u32> {
    let mut tables = Vec::new();
    let mut funcs = Vec::new();
    collect_select_read_names(select, &mut tables, &mut funcs);
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    for func in &funcs {
        collect_read_lock_ids(storage, db_id, func, 0, &mut out, &mut visited);
    }
    out
}

/// Like [`select_function_read_ids`] but for a bare expression (an IF/WHILE
/// condition).
pub(super) fn expr_function_read_ids(storage: &Storage, db_id: u32, expr: &Expr) -> Vec<u32> {
    let mut tables = Vec::new();
    let mut funcs = Vec::new();
    collect_expr_read_names(expr, &mut tables, &mut funcs);
    let mut out = Vec::new();
    let mut visited = std::collections::HashSet::new();
    for func in &funcs {
        collect_read_lock_ids(storage, db_id, func, 0, &mut out, &mut visited);
    }
    out
}

/// Resolves `name` to the base-table object ids the executor will read,
/// recursing through nested views (so a view over a view locks the inner view's
/// base tables). Bounded by [`MAX_VIEW_NESTING`] so a view cycle terminates.
pub(super) fn collect_read_lock_ids(
    storage: &Storage,
    db_id: u32,
    name: &str,
    depth: u32,
    out: &mut Vec<u32>,
    visited: &mut std::collections::HashSet<u32>,
) {
    if depth > MAX_VIEW_NESTING || name.to_ascii_lowercase().starts_with("sys.") {
        return;
    }
    let Some(def) = resolve_table(storage, db_id, name) else {
        return;
    };
    // Expand each function/view body at most once per analysis. The depth guard
    // bounds recursion depth but NOT fan-out: a self- or mutually-referential
    // body that references itself twice would otherwise recurse exponentially
    // (2^depth), hanging analysis — and, because analyze_locks runs under the
    // scheduler mutex, freezing every session.
    if (def.is_function() || def.view_query.is_some()) && !visited.insert(def.object_id) {
        return;
    }
    // A function: its inner reads (subqueries in a scalar body, or an inline
    // TVF's body SELECT, plus nested function calls) must be locked up front, or
    // the body would read tables with no lock held under 2PL — the seam-defect
    // class. Recurse into the body's read targets, bounded by the same guard.
    if let Some(function) = &def.function {
        let mut tables = Vec::new();
        let mut funcs = Vec::new();
        match &function.returns {
            FunctionReturns::Scalar { body, .. } => {
                if let Ok(statements) = truthdb_sql::parse_function_body(body) {
                    for statement in &statements {
                        collect_statement_read_names(statement, &mut tables, &mut funcs);
                    }
                }
            }
            FunctionReturns::InlineTable { select_text } => {
                if let Ok(body) = parse_view_query(select_text, &def.name) {
                    let expanded = expand_ctes(&body);
                    collect_select_read_names(&expanded, &mut tables, &mut funcs);
                }
            }
            // A multi-statement TVF body may read real tables (e.g. INSERT @t
            // SELECT FROM base): those reads must be locked up front, exactly
            // like a scalar body. (@-targets are session-local and are skipped
            // by the read-name collectors, so they add no lock.)
            FunctionReturns::MultiStatementTable { body, .. } => {
                if let Ok(statements) = truthdb_sql::parse_table_function_body(body) {
                    for statement in &statements {
                        collect_statement_read_names(statement, &mut tables, &mut funcs);
                    }
                }
            }
        }
        for referenced in tables.iter().chain(funcs.iter()) {
            // The body's unqualified names are the FUNCTION's database's.
            collect_read_lock_ids(
                storage,
                def.database_id,
                referenced,
                depth + 1,
                out,
                visited,
            );
        }
        return;
    }
    let Some(text) = &def.view_query else {
        // A base table.
        if !out.contains(&def.object_id) {
            out.push(def.object_id);
        }
        return;
    };
    // A view: recurse into every table its body references — and every scalar
    // function it calls, whose body may read further tables (else a UDF reached
    // through a view would read unlocked). Inline the body's own CTEs so a base
    // table reached only through a CTE is still locked.
    let Ok(body) = parse_view_query(text, &def.name) else {
        return;
    };
    let expanded = expand_ctes(&body);
    let mut tables = Vec::new();
    let mut funcs = Vec::new();
    collect_select_read_names(&expanded, &mut tables, &mut funcs);
    for referenced in tables.iter().chain(funcs.iter()) {
        // The body's unqualified names are the VIEW's database's.
        collect_read_lock_ids(
            storage,
            def.database_id,
            referenced,
            depth + 1,
            out,
            visited,
        );
    }
}

/// Views are read-only here; INSERT/UPDATE/DELETE against one is rejected —
/// and a PROCEDURE is not a data object at all (SQL Server 2809).
pub(super) fn reject_dml_on_view(def: &TableDef) -> Result<(), SqlError> {
    if def.is_procedure() {
        return Err(procedure_not_a_table(&def.name));
    }
    if def.is_function() {
        return Err(function_not_a_table(&def.name));
    }
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&def.name));
    }
    if def.is_view() {
        return Err(SqlError::new(
            4406,
            16,
            1,
            format!(
                "Update or insert of view '{}' is not supported (the view is read-only).",
                def.name
            ),
        ));
    }
    Ok(())
}

/// SQL Server 2809: a procedure referenced where a table/view is required.
pub(super) fn procedure_not_a_table(name: &str) -> SqlError {
    SqlError::new(
        2809,
        16,
        1,
        format!(
            "The request for procedure '{name}' failed because '{name}' is a procedure object."
        ),
    )
}

/// A scalar function used where a table is required (`FROM`, DML target,
/// table-only DDL). A scalar function is not a rowset; SQL Server 4121-class.
pub(super) fn function_not_a_table(name: &str) -> SqlError {
    SqlError::new(
        4121,
        16,
        1,
        format!(
            "Cannot find the user-defined function '{name}', or the name refers to a scalar \
             function that cannot be used where a table is expected."
        ),
    )
}

/// Table-only DDL (ALTER TABLE, CREATE INDEX) rejects a view. Without this a
/// view's `root_page = 0` would be heap-scanned — and page 0 is the catalog
/// root, so a bare `ALTER TABLE view ADD CHECK (1=1)` could corrupt the catalog.
pub(super) fn reject_view_as_table(def: &TableDef) -> Result<(), SqlError> {
    if def.is_procedure() {
        return Err(procedure_not_a_table(&def.name));
    }
    if def.is_function() {
        return Err(function_not_a_table(&def.name));
    }
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&def.name));
    }
    if def.is_view() {
        return Err(SqlError::new(
            4928,
            16,
            1,
            format!(
                "Cannot perform this operation on '{}' because it is a view, not a table.",
                def.name
            ),
        ));
    }
    Ok(())
}

/// Validates a CREATE'd object name: one part, or `dbo.<name>`. A database
/// prefix is refused (SQL Server 166 — CREATE resolves in the current
/// database only) and an unknown schema is refused (2760). Returns the bare
/// name to store.
pub(super) fn create_object_name<'a>(kind: &str, name: &'a Name) -> Result<&'a str, SqlError> {
    // A quoted identifier (`[sys.tables]`) is one name, dots and all — the
    // parser records quoting for the first part, and splitting it here would
    // invent a schema the user never wrote.
    if name.quoted {
        return Ok(strip_schema(&name.value));
    }
    let parts: Vec<&str> = name.value.split('.').collect();
    match parts[..] {
        [bare] => Ok(bare),
        [schema, bare] if schema.eq_ignore_ascii_case("dbo") => Ok(bare),
        [schema, _] => Err(SqlError::new(
            2760,
            16,
            1,
            format!(
                "The specified schema name \"{schema}\" either does not exist or you do not have permission to use it."
            ),
        )
        .at(name.span)),
        _ => Err(SqlError::new(
            166,
            15,
            1,
            format!(
                "'{kind}' does not allow specifying the database name as a prefix to the object name."
            ),
        )
        .at(name.span)),
    }
}

pub(super) fn resolve_table(storage: &Storage, db_id: u32, name: &str) -> Option<TableDef> {
    // Three-part names (`db.dbo.t`, `db..t`) resolve in the named database;
    // one- and two-part names in the session's. An unknown database or a
    // schema other than dbo resolves to nothing (208 at the call sites).
    let parts: Vec<&str> = name.split('.').collect();
    let (target_db, bare_owned);
    let bare: &str = match parts[..] {
        // `[dbo].[my.table]` flattens to dbo.my.table: a leading dbo is the
        // schema ('dbo' is a reserved database name, so this cannot shadow a
        // real database) and the REMAINDER is one name containing dots.
        [schema, ..] if schema.eq_ignore_ascii_case("dbo") => {
            target_db = db_id;
            &name[schema.len() + 1..]
        }
        [db, schema, t] if schema.is_empty() || schema.eq_ignore_ascii_case("dbo") => {
            target_db = storage.rel_database_id_by_name(db)?;
            t
        }
        [_, _, _] => return None,
        _ => {
            target_db = db_id;
            bare_owned = strip_schema(name);
            bare_owned
        }
    };
    if let Some(def) = storage.rel_table(target_db, bare) {
        return Some(def);
    }
    storage
        .rel_tables()
        .into_iter()
        .find(|d| d.database_id == target_db && d.name.eq_ignore_ascii_case(bare))
}

/// Maps a storage error to a SQL Server-numbered error. PK and NULL
/// violations are recognized by their storage messages.
pub(super) fn map_storage_err(err: StorageError, table: &str) -> SqlError {
    match err {
        StorageError::Constraint(msg) if msg.contains("duplicate primary key") => {
            SqlError::pk_violation(table)
        }
        StorageError::Constraint(msg) if msg.contains("duplicate unique index") => {
            // 2601: cannot insert a duplicate key row in a unique index.
            SqlError::new(2601, 14, 1, msg)
        }
        StorageError::Constraint(msg) if msg.contains("already exists") => {
            // 1913: an index with that name already exists on the table.
            SqlError::new(1913, 16, 1, msg)
        }
        StorageError::Constraint(msg) if msg.contains("does not allow NULL") => {
            SqlError::new(515, 16, 2, msg)
        }
        StorageError::Constraint(msg) => SqlError::new(547, 16, 0, msg),
        StorageError::SnapshotSchemaChange(name) => SqlError::new(
            3961,
            16,
            1,
            format!(
                "Snapshot isolation transaction failed in database because the object accessed \
                 by the statement has been modified by a DDL statement in another concurrent \
                 transaction since the start of this transaction. It is disallowed because the \
                 metadata is not versioned. Object: '{name}'."
            ),
        ),
        StorageError::InvalidConfig(msg) => SqlError::new(1701, 16, 1, msg),
        // The WAL ring is full — under FULL recovery this is typically because
        // un-backed-up log pins truncation (run BACKUP LOG); under SIMPLE it is
        // an oversized active transaction. SQL Server reports 9002 either way.
        StorageError::WalFull(msg) => SqlError::new(
            9002,
            17,
            2,
            format!("The transaction log for the database is full. {msg}"),
        ),
        other => SqlError::new(
            3621,
            16,
            1,
            format!("The statement has been terminated. {other}"),
        ),
    }
}

pub use ast::Statement as SqlStatement;

/// Renders a result cell to its display string (`None` = NULL). Shared by
/// the JSON envelope and any text renderer.
pub fn render_cell(datum: &Datum, column_type: &ColumnType) -> Option<String> {
    value::display(datum, column_type)
}
