use super::super::prelude::*;

pub(in crate::engine::relational) fn exec_create_procedure(
    storage: &Storage,
    db_id: u32,
    create: &CreateProcedure,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE PROCEDURE", &create.name)?;
    // The builtin dispatcher checks `sp_executesql` BEFORE the catalog, so a
    // user procedure with that name would execute as the builtin while lock
    // ANALYSIS resolved the catalog first — an unanalyzed inner batch (the
    // review's shadow finding). Refuse the shadow outright.
    if bare.eq_ignore_ascii_case("sp_executesql") {
        return Err(SqlError::new(
            2714,
            16,
            6,
            "The name 'sp_executesql' is reserved for the system procedure.",
        ));
    }
    let params = create
        .params
        .iter()
        .map(|p| -> Result<ProcParamDef, SqlError> {
            // The declared type round-trips through the column-type spec
            // parser, exactly like table columns.
            let column_type = data_type_to_column_type(&p.data_type, &p.name)?;
            if let Some(text) = &p.default_text {
                let expr = truthdb_sql::parse_expr(text)?;
                if !constant_default(&expr) {
                    return Err(SqlError::new(
                        102,
                        15,
                        1,
                        format!(
                            "The default for parameter '@{}' must be a constant.",
                            p.name
                        ),
                    )
                    .at(p.span));
                }
            }
            Ok(ProcParamDef {
                name: p.name.clone(),
                type_spec: column_type.name(),
                default: p.default_text.clone(),
                output: p.output,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let procedure = ProcedureDef {
        params,
        body: create.body.clone(),
    };
    if create.alter {
        match resolve_table(storage, db_id, &create.name.value) {
            Some(def) if def.is_procedure() => {
                storage
                    .rel_alter_procedure(def.database_id, &def.name, procedure)
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
        .rel_create_procedure(db_id, bare, procedure)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

pub(in crate::engine::relational) fn exec_drop_procedure(
    storage: &Storage,
    db_id: u32,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &name.value) {
        Some(def) if def.is_procedure() => {
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
                "Cannot drop the procedure '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

pub(in crate::engine::relational) fn exec_create_function(
    storage: &Storage,
    db_id: u32,
    create: &CreateFunction,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE FUNCTION", &create.name)?;
    let params = create
        .params
        .iter()
        .map(|p| -> Result<ProcParamDef, SqlError> {
            let column_type = data_type_to_column_type(&p.data_type, &p.name)?;
            if let Some(text) = &p.default_text {
                let expr = truthdb_sql::parse_expr(text)?;
                if !constant_default(&expr) {
                    return Err(SqlError::new(
                        102,
                        15,
                        1,
                        format!(
                            "The default for parameter '@{}' must be a constant.",
                            p.name
                        ),
                    )
                    .at(p.span));
                }
            }
            Ok(ProcParamDef {
                name: p.name.clone(),
                type_spec: column_type.name(),
                default: p.default_text.clone(),
                output: false,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let returns = match &create.returns {
        ReturnsClause::Scalar(return_type) => {
            let return_type = data_type_to_column_type(return_type, bare)?;
            // Validate the body: side-effect-free, ending in RETURN <expr> (SQL
            // Server's function-body rules). Re-parse under the function grammar.
            let body = truthdb_sql::parse_function_body(&create.body)?;
            validate_scalar_function_body(&body)?;
            FunctionReturns::Scalar {
                type_spec: return_type.name(),
                body: create.body.clone(),
            }
        }
        ReturnsClause::InlineTable => {
            // The body is a single SELECT expanded like a parameterized view —
            // validate it parses (no side-effect body check: it is a query).
            parse_view_query(&create.body, bare)?;
            FunctionReturns::InlineTable {
                select_text: create.body.clone(),
            }
        }
        ReturnsClause::MultiTable {
            var_name,
            columns_text,
        } => {
            // Validate the RETURNS table declaration builds (mirrors DECLARE @t
            // TABLE) and the body parses under the multi-statement TVF rules (may
            // populate the result / local table variables but not touch real
            // tables; must end in RETURN). Both are stored as text, re-parsed and
            // re-built per call.
            let (columns, primary_key) = truthdb_sql::parse_table_var_columns(columns_text)?;
            build_table_var_definition(var_name, &columns, &primary_key)?;
            let body = truthdb_sql::parse_table_function_body(&create.body)?;
            validate_multi_tvf_body(&body)?;
            FunctionReturns::MultiStatementTable {
                returns_var: var_name.clone(),
                columns_text: columns_text.clone(),
                body: create.body.clone(),
            }
        }
    };
    let function = FunctionDef { params, returns };
    if create.alter {
        match resolve_table(storage, db_id, &create.name.value) {
            Some(def) if def.is_function() => {
                storage
                    .rel_alter_function(def.database_id, &def.name, function)
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
        .rel_create_function(db_id, bare, function)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// Validates a scalar function's body against SQL Server's rules: every
/// statement must be side-effect-free (443 otherwise; a data-returning SELECT is
/// 444), and the last statement must be a `RETURN <expr>` (455).
pub(in crate::engine::relational) fn validate_scalar_function_body(
    statements: &[Statement],
) -> Result<(), SqlError> {
    for statement in statements {
        check_function_statement(statement)?;
    }
    match last_effective_statement(statements) {
        Some(Statement::Return { value: Some(_), .. }) => Ok(()),
        _ => Err(SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )),
    }
}

/// The body's terminal statement, unwrapping a trailing `BEGIN...END` block —
/// SQL Server's 455 check looks at the last statement of the body block.
pub(in crate::engine::relational) fn last_effective_statement(
    statements: &[Statement],
) -> Option<&Statement> {
    match statements.last() {
        Some(Statement::Block { body, .. }) => last_effective_statement(body),
        other => other,
    }
}

/// Rejects a statement a function body may not contain. Side-effecting
/// statements (DML, DDL, EXEC, transaction control, THROW/RAISERROR) are 443; a
/// data-returning SELECT is 444; control flow recurses.
pub(in crate::engine::relational) fn check_function_statement(
    statement: &Statement,
) -> Result<(), SqlError> {
    match statement {
        Statement::Declare(_)
        | Statement::Set(_)
        | Statement::Return { .. }
        | Statement::Break { .. }
        | Statement::Continue { .. } => Ok(()),
        Statement::Block { body, .. } => {
            for inner in body {
                check_function_statement(inner)?;
            }
            Ok(())
        }
        Statement::If {
            then_branch,
            else_branch,
            ..
        } => {
            check_function_statement(then_branch)?;
            if let Some(else_branch) = else_branch {
                check_function_statement(else_branch)?;
            }
            Ok(())
        }
        Statement::While { body, .. } => check_function_statement(body),
        // An assignment SELECT (`SELECT @x = …`) is allowed — it returns no
        // rows. A SELECT that produces a result set cannot (444).
        Statement::Select(select)
            if select
                .items
                .iter()
                .all(|i| matches!(i, SelectItem::Assign { .. })) =>
        {
            Ok(())
        }
        Statement::Select(_) => Err(SqlError::new(
            444,
            16,
            2,
            "Select statements included within a function cannot return data to a client.",
        )),
        _ => Err(SqlError::new(
            443,
            16,
            1,
            "Invalid use of a side-effecting operator within a function.",
        )),
    }
}

/// Validates a multi-statement TVF body: like a scalar function it is
/// side-effect-free against the database, but it MAY populate table variables
/// (its result and any locals it declares), and its last statement must be a
/// (valueless) RETURN.
pub(in crate::engine::relational) fn validate_multi_tvf_body(
    statements: &[Statement],
) -> Result<(), SqlError> {
    for statement in statements {
        check_multi_tvf_statement(statement)?;
    }
    match last_effective_statement(statements) {
        Some(Statement::Return { .. }) => Ok(()),
        _ => Err(SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )),
    }
}

/// Rejects a statement a multi-statement TVF body may not contain. The only
/// difference from a scalar body (`check_function_statement`) is that DML into a
/// table variable (an `@`-target) is allowed — that is how the result is built.
pub(in crate::engine::relational) fn check_multi_tvf_statement(
    statement: &Statement,
) -> Result<(), SqlError> {
    match statement {
        // INSERT into a table variable (the result or a local) is how a
        // multi-statement TVF produces rows.
        Statement::Insert(insert) if insert.table.value.starts_with('@') => Ok(()),
        Statement::DeclareTableVar { .. } => Ok(()),
        Statement::Block { body, .. } => {
            for inner in body {
                check_multi_tvf_statement(inner)?;
            }
            Ok(())
        }
        Statement::If {
            then_branch,
            else_branch,
            ..
        } => {
            check_multi_tvf_statement(then_branch)?;
            if let Some(else_branch) = else_branch {
                check_multi_tvf_statement(else_branch)?;
            }
            Ok(())
        }
        Statement::While { body, .. } => check_multi_tvf_statement(body),
        // Everything else defers to the scalar-body rules (DECLARE/SET/RETURN/
        // assignment-SELECT allowed; real-table DML/EXEC/DDL 443; data SELECT
        // 444).
        other => check_function_statement(other),
    }
}

pub(in crate::engine::relational) fn exec_drop_function(
    storage: &Storage,
    db_id: u32,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &name.value) {
        Some(def) if def.is_function() => {
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
                "Cannot drop the function '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}
