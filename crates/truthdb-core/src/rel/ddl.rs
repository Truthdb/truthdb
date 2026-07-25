use super::prelude::*;

// ---- CREATE TABLE -------------------------------------------------------

pub(super) fn exec_create_table(
    storage: &Storage,
    db_id: u32,
    create: &CreateTable,
) -> Result<StatementResult, SqlError> {
    // Strip an optional `dbo.` schema prefix so the table is stored (and
    // later resolved) under its bare name.
    let table_name = create_object_name("CREATE TABLE", &create.table)?;
    if resolve_table(storage, db_id, table_name).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{table_name}' in the database."),
        ));
    }

    let mut seen = Vec::new();
    let mut columns = Vec::with_capacity(create.columns.len());
    for column in &create.columns {
        if seen
            .iter()
            .any(|n: &String| n.eq_ignore_ascii_case(&column.name.value))
        {
            return Err(SqlError::new(
                2705,
                16,
                3,
                format!(
                    "Column names in each table must be unique. Column name '{}' is specified more than once.",
                    column.name.value
                ),
            ));
        }
        seen.push(column.name.value.clone());
        columns.push(bind_column(column)?);
    }

    // Primary key columns must exist and are implicitly NOT NULL (declaring
    // one explicitly NULL is an error, matching SQL Server 8111).
    let mut key_names = Vec::new();
    for key in &create.primary_key {
        let Some(index) = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&key.value))
        else {
            return Err(SqlError::new(
                1750,
                16,
                0,
                format!(
                    "Column '{}' in the PRIMARY KEY is not a column of the table.",
                    key.value
                ),
            )
            .at(key.span));
        };
        let declared_null = create
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_case(&key.value))
            .and_then(|c| c.nullable)
            == Some(true);
        if declared_null {
            return Err(SqlError::new(
                8111,
                16,
                1,
                format!(
                    "Cannot define PRIMARY KEY constraint on nullable column in table '{table_name}'."
                ),
            ));
        }
        if columns[index].column_type.is_max() {
            return Err(max_key_column_error(&key.value, table_name).at(key.span));
        }
        columns[index].nullable = false;
        key_names.push(columns[index].name.clone());
    }

    // Per-column DEFAULT source text (parallel to columns).
    let defaults: Vec<Option<String>> = create.columns.iter().map(|c| c.default.clone()).collect();

    // At most one IDENTITY column, on an integer type.
    let mut identity: Option<catalog::IdentitySpec> = None;
    for (index, column) in create.columns.iter().enumerate() {
        let Some(id) = column.identity else { continue };
        if identity.is_some() {
            return Err(SqlError::new(
                2744,
                16,
                2,
                format!(
                    "Multiple identity columns specified for table '{table_name}'. Only one identity column per table is allowed."
                ),
            ));
        }
        if !matches!(
            columns[index].column_type,
            ColumnType::TinyInt | ColumnType::SmallInt | ColumnType::Int | ColumnType::BigInt
        ) {
            return Err(SqlError::new(
                2749,
                16,
                2,
                format!(
                    "Identity column '{}' must be of a data type that is an integer.",
                    column.name.value
                ),
            )
            .at(column.span));
        }
        if column.default.is_some() {
            return Err(SqlError::new(
                1754,
                16,
                1,
                "Defaults cannot be created on columns with an IDENTITY attribute.".to_string(),
            )
            .at(column.span));
        }
        identity = Some(catalog::IdentitySpec {
            column: index,
            seed: id.seed,
            increment: id.increment,
            next: id.seed,
        });
    }

    // CHECK constraints (column-level + table-level): validate, name, and
    // fold into the catalog. Validation needs the bound columns.
    let check_constraints = build_check_defs(create, &columns, table_name)?;
    // FOREIGN KEY constraints: validate against the (possibly self-)referenced
    // table's primary key and order each child column to the parent's PK.
    // Constraint names are unique across kinds, so seed with the check names.
    let check_names: Vec<String> = check_constraints.iter().map(|c| c.name.clone()).collect();
    let foreign_keys =
        build_foreign_key_defs(db_id, storage, create, &columns, table_name, &check_names)?;

    // UNIQUE constraints become unique indexes. Resolve their columns now (while
    // `columns` is in hand) so an invalid column errors before the table exists.
    let mut unique_indexes: Vec<(String, Vec<(usize, bool)>)> = Vec::new();
    for (i, uc) in create.unique_constraints.iter().enumerate() {
        let mut cols = Vec::with_capacity(uc.columns.len());
        for col in &uc.columns {
            let index = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col.value))
                .ok_or_else(|| SqlError::invalid_column(&col.value).at(col.span))?;
            cols.push((index, true));
        }
        let name = uc
            .name
            .as_ref()
            .map(|n| n.value.clone())
            .unwrap_or_else(|| format!("UQ_{table_name}_{}", i + 1));
        unique_indexes.push((name, cols));
    }

    storage
        .rel_create_table(
            db_id,
            table_name,
            columns,
            &key_names,
            defaults,
            identity,
            check_constraints,
            foreign_keys,
        )
        .map_err(|err| map_storage_err(err, table_name))?;
    for (name, cols) in unique_indexes {
        storage
            .rel_create_index(db_id, table_name, name, cols, true, Vec::new())
            .map_err(|err| map_storage_err(err, table_name))?;
    }
    Ok(StatementResult::Done)
}

// ---- DROP TABLE ---------------------------------------------------------

pub(super) fn exec_drop_table(
    storage: &Storage,
    db_id: u32,
    drop: &DropTable,
) -> Result<StatementResult, SqlError> {
    // DROP TABLE does not drop a view or a procedure (use the matching DROP).
    // The object exists but is the wrong type, so error even under IF EXISTS
    // rather than silently no-op — the review showed DROP TABLE silently
    // DESTROYING a procedure through the shared catalog path.
    if resolve_table(storage, db_id, &drop.table.value)
        .is_some_and(|d| d.is_view() || d.is_procedure() || d.is_function() || d.is_trigger())
    {
        return Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the table '{}', because it does not exist or you do not have permission.",
                drop.table.value
            ),
        ));
    }
    let resolved = resolve_table(storage, db_id, &drop.table.value);
    match resolved {
        Some(def) => {
            // Everything below acts on the RESOLVED table's database — a
            // three-part DROP TABLE names another database's table.
            let (target_db, name, parent_oid) = (def.database_id, def.name, def.object_id);
            // A table still referenced by another table's foreign key cannot be
            // dropped (SQL Server 3726) — it would leave a dangling reference.
            if let Some(child) = storage.rel_tables().into_iter().find(|t| {
                t.database_id == target_db
                    && !t.name.eq_ignore_ascii_case(&name)
                    && t.foreign_keys
                        .iter()
                        .any(|fk| fk.parent.eq_ignore_ascii_case(&name))
            }) {
                let referencing = child
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.parent.eq_ignore_ascii_case(&name))
                    .map(|fk| fk.name.clone())
                    .unwrap_or_default();
                return Err(SqlError::new(
                    3726,
                    16,
                    1,
                    format!(
                        "Could not drop object '{name}' because it is referenced by a FOREIGN KEY constraint '{referencing}'."
                    ),
                ));
            }
            // Cascade-drop the table's triggers — a trigger outlives its parent
            // table nowhere in SQL Server, and an orphan would permanently block
            // its own name (and dangle in sys.triggers).
            let orphan_triggers: Vec<String> = storage
                .rel_tables()
                .into_iter()
                .filter(|d| {
                    d.trigger
                        .as_ref()
                        .is_some_and(|t| t.parent_object_id == parent_oid)
                })
                .map(|d| d.name)
                .collect();
            for trigger_name in orphan_triggers {
                storage
                    .rel_drop_table(target_db, &trigger_name)
                    .map_err(|err| map_storage_err(err, &trigger_name))?;
            }
            storage
                .rel_drop_table(target_db, &name)
                .map_err(|err| map_storage_err(err, &drop.table.value))?;
            Ok(StatementResult::Done)
        }
        None if drop.if_exists => Ok(StatementResult::Done),
        None => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the table '{}', because it does not exist or you do not have permission.",
                drop.table.value
            ),
        )),
    }
}

// ---- CREATE / DROP VIEW -------------------------------------------------

/// Parses a stored view definition back into its `SELECT`. The text was
/// validated at CREATE, so this only fails on catalog corruption.
pub(super) fn parse_view_query(text: &str, view_name: &str) -> Result<Select, SqlError> {
    match truthdb_sql::parse(text)?.into_iter().next() {
        Some(Statement::Select(select)) => Ok(select),
        _ => Err(SqlError::message_only(
            208,
            format!("The definition of view '{view_name}' is not a SELECT."),
        )),
    }
}

pub(super) fn exec_create_view(
    storage: &Storage,
    db_id: u32,
    create: &CreateView,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE VIEW", &create.name)?;
    if resolve_table(storage, db_id, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    // Validate the definition parses as a SELECT now; base-table and column
    // resolution is deferred to query time (SQL Server-style deferred name
    // resolution — a view over a not-yet-created table is allowed).
    parse_view_query(&create.query_text, bare)?;
    storage
        .rel_create_view(db_id, bare, &create.query_text)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// A parameter default must be a CONSTANT (SQL Server rejects at CREATE):
/// literals, NULL, and a signed literal — never variables or functions,
/// which would otherwise evaluate against each CALLER's scope and drift.
pub(super) fn constant_default(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_) => true,
        ExprKind::Unary { expr, .. } => constant_default(expr),
        _ => false,
    }
}

pub(super) fn exec_create_procedure(
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

pub(super) fn exec_drop_procedure(
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

pub(super) fn exec_create_function(
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
pub(super) fn validate_scalar_function_body(statements: &[Statement]) -> Result<(), SqlError> {
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
pub(super) fn last_effective_statement(statements: &[Statement]) -> Option<&Statement> {
    match statements.last() {
        Some(Statement::Block { body, .. }) => last_effective_statement(body),
        other => other,
    }
}

/// Rejects a statement a function body may not contain. Side-effecting
/// statements (DML, DDL, EXEC, transaction control, THROW/RAISERROR) are 443; a
/// data-returning SELECT is 444; control flow recurses.
pub(super) fn check_function_statement(statement: &Statement) -> Result<(), SqlError> {
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
pub(super) fn validate_multi_tvf_body(statements: &[Statement]) -> Result<(), SqlError> {
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
pub(super) fn check_multi_tvf_statement(statement: &Statement) -> Result<(), SqlError> {
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

pub(super) fn exec_drop_function(
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

pub(super) fn exec_create_login(
    storage: &Storage,
    create: &CreateLogin,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if create.alter {
        let Some(existing) = storage.rel_login(bare) else {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot alter the login '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(create.name.span));
        };
        let mut principal = existing
            .principal
            .clone()
            .expect("rel_login returns a login");
        if let Some(password) = &create.password {
            principal.password_blob = crate::auth::hash_password(password);
        }
        if let Some(disable) = create.disable {
            principal.is_disabled = disable;
        }
        storage
            .rel_alter_login(bare, principal)
            .map_err(|e| map_storage_err(e, &create.name.value))?;
        return Ok(StatementResult::Done);
    }
    if storage.rel_login(bare).is_some() {
        return Err(SqlError::new(
            15025,
            16,
            1,
            format!("The server principal '{bare}' already exists."),
        )
        .at(create.name.span));
    }
    let password = create
        .password
        .as_ref()
        .expect("CREATE LOGIN carries a password (parser-enforced)");
    let principal = PrincipalDef::login(
        crate::auth::hash_password(password),
        create.disable.unwrap_or(false),
    );
    storage
        .rel_create_login(bare, principal)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

pub(super) fn exec_drop_login(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let dropped = storage
        .rel_drop_login(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    if !dropped && !if_exists {
        return Err(SqlError::new(
            15151,
            16,
            1,
            format!(
                "Cannot drop the login '{bare}', because it does not exist or you do not have permission."
            ),
        )
        .at(name.span));
    }
    Ok(StatementResult::Done)
}

/// `CREATE USER <name> [FOR LOGIN <login>]`. A database principal in its own
/// namespace (out of the object namespace), optionally mapped to a login.
pub(super) fn exec_create_user(
    storage: &Storage,
    create: &CreateUser,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(create.name.span));
    }
    let login_sid = match &create.for_login {
        Some(login) => {
            let login_bare = strip_schema(&login.value);
            let Some(def) = storage.rel_login(login_bare) else {
                return Err(SqlError::new(
                    15007,
                    16,
                    1,
                    format!("'{login_bare}' is not a valid login or you do not have permission."),
                )
                .at(login.span));
            };
            Some(def.object_id)
        }
        None => None,
    };
    storage
        .rel_create_database_principal(bare, PrincipalDef::user(login_sid))
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// `CREATE ROLE <name>`.
pub(super) fn exec_create_role(
    storage: &Storage,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(name.span));
    }
    storage
        .rel_create_database_principal(bare, PrincipalDef::role())
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `DROP USER`/`DROP ROLE`. `expect_role` selects which kind is being dropped;
/// a mismatch (DROP USER on a role, or vice versa) reports not-found for the
/// requested kind, as SQL Server does.
pub(super) fn exec_drop_database_principal(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
    expect_role: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let kind = if expect_role { "role" } else { "user" };
    match storage.rel_database_principal(bare) {
        Some(def) if def.is_role() == expect_role => {}
        _ if if_exists => return Ok(StatementResult::Done),
        _ => {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot drop the {kind} '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(name.span));
        }
    }
    storage
        .rel_drop_database_principal(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
pub(super) fn exec_alter_role_member(
    storage: &Storage,
    role: &Name,
    action: RoleMemberAction,
    member: &Name,
) -> Result<StatementResult, SqlError> {
    let role_bare = strip_schema(&role.value);
    let member_bare = strip_schema(&member.value);
    match action {
        RoleMemberAction::Add => storage.rel_add_role_member(role_bare, member_bare),
        RoleMemberAction::Drop => storage.rel_drop_role_member(role_bare, member_bare),
    }
    .map_err(|e| map_storage_err(e, &role.value))?;
    Ok(StatementResult::Done)
}

/// Maps a parsed permission action to its catalog form.
pub(super) fn map_perm_action(action: PermissionAction) -> PermAction {
    match action {
        PermissionAction::Select => PermAction::Select,
        PermissionAction::Insert => PermAction::Insert,
        PermissionAction::Update => PermAction::Update,
        PermissionAction::Delete => PermAction::Delete,
        PermissionAction::Execute => PermAction::Execute,
        PermissionAction::References => PermAction::References,
        PermissionAction::Alter => PermAction::Alter,
    }
}

/// `GRANT|DENY|REVOKE <actions> ON <object> TO|FROM <grantees>`. The authority to
/// manage permissions is enforced by the DDL privilege gate in the dispatcher
/// (a bypassing principal — sysadmin / dbo / db_owner / internal). Here we just
/// resolve the securable and apply each (grantee, action).
pub(super) fn exec_permission(
    storage: &Storage,
    db_id: u32,
    stmt: &PermissionStatement,
    _sec: &SecurityContext,
) -> Result<StatementResult, SqlError> {
    // The securable must be a schema object (table, view, procedure, function).
    let Some(def) = resolve_table(storage, db_id, &stmt.object.value) else {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    };
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    }
    let object = def.name.clone(); // the canonical name = the rel.tables key
    for grantee in &stmt.grantees {
        let grantee_bare = strip_schema(&grantee.value);
        for action in &stmt.actions {
            let catalog_action = map_perm_action(*action);
            match stmt.kind {
                PermissionKind::Grant => storage.rel_grant_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                    false,
                ),
                PermissionKind::Deny => storage.rel_grant_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                    true,
                ),
                PermissionKind::Revoke => storage.rel_revoke_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                ),
            }
            .map_err(|e| map_storage_err(e, &grantee.value).at(grantee.span))?;
        }
    }
    Ok(StatementResult::Done)
}

/// Schema and security DDL a non-privileged principal may not run. (GRANT/DENY/
/// REVOKE — `Permission` — is included: only a privileged principal manages
/// permissions.) Fine-grained database-scoped CREATE grants and the db_ddladmin
/// role are deferred: today any DDL requires bypass privilege.
pub(super) fn is_privileged_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. }
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. }
            | Statement::Restore { .. }
    )
}

pub(super) fn exec_drop_view(
    storage: &Storage,
    db_id: u32,
    drop: &DropView,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &drop.name.value) {
        Some(def) if def.is_view() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        // The object exists but is a base table, not a view.
        Some(_) => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the view '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        )),
        None if drop.if_exists => Ok(StatementResult::Done),
        None => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the view '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        )),
    }
}

// ---- CREATE / DROP INDEX ------------------------------------------------

/// SQL Server 1919: a (MAX)-class column cannot be an index/key column.
pub(super) fn max_key_column_error(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1919,
        16,
        1,
        format!(
            "Column '{column}' in table '{table}' is of a type that is invalid for use as a \
             key column in an index."
        ),
    )
}

pub(super) fn exec_create_index(
    storage: &Storage,
    db_id: u32,
    create: &CreateIndex,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, db_id, &create.table.value)
        .ok_or_else(|| SqlError::invalid_object(&create.table.value).at(create.table.span))?;
    reject_view_as_table(&def)?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let mut columns = Vec::with_capacity(create.columns.len());
    for col in &create.columns {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.name.value))
            .ok_or_else(|| index_column_missing(&col.name.value, &def.name).at(col.name.span))?;
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&col.name.value, &def.name).at(col.name.span));
        }
        columns.push((index, col.ascending));
    }
    // INCLUDE columns: resolved against the schema, no duplicates (1909, as
    // SQL Server). A *key* column may be INCLUDEd — a deliberate divergence
    // from SQL Server, which rejects that: our index keys are one-way
    // collation sort keys, so a query reading the key column itself can only
    // be covered by also storing its original value.
    let mut include = Vec::with_capacity(create.include.len());
    for col in &create.include {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.value))
            .ok_or_else(|| index_column_missing(&col.value, &def.name).at(col.span))?;
        // (MAX) columns cannot be INCLUDEd either — a divergence from SQL
        // Server (whose row-overflow indexes can carry them): our include
        // payloads live in ordinary index leaf cells under the tree cell cap.
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&col.value, &def.name).at(col.span));
        }
        if include.contains(&index) {
            return Err(SqlError::new(
                1909,
                16,
                1,
                format!(
                    "Cannot use duplicate column names in index. Column name '{}' listed more than once.",
                    col.value
                ),
            )
            .at(col.span));
        }
        include.push(index);
    }
    storage
        .rel_create_index(
            def.database_id,
            &def.name,
            create.name.value.clone(),
            columns,
            create.unique,
            include,
        )
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// SQL Server's 1911 for a `CREATE INDEX` column (key or `INCLUDE`) that does
/// not exist on the target table — where most statements answer 207.
pub(super) fn index_column_missing(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1911,
        16,
        1,
        format!("Column name '{column}' does not exist in the target table or view '{table}'."),
    )
}

pub(super) fn exec_drop_index(
    storage: &Storage,
    db_id: u32,
    drop: &DropIndex,
) -> Result<StatementResult, SqlError> {
    // Resolve the table so the index lookup is scoped to it (index names are
    // per-table; two tables may share an index name).
    let table = resolve_table(storage, db_id, &drop.table.value)
        .ok_or_else(|| SqlError::invalid_object(&drop.table.value).at(drop.table.span))?;
    let existed = storage
        .rel_drop_index(table.database_id, &table.name, &drop.name.value)
        .map_err(|e| map_storage_err(e, &drop.name.value))?;
    if !existed {
        return Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the index '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        ));
    }
    Ok(StatementResult::Done)
}

// ---- ALTER TABLE --------------------------------------------------------

/// `ALTER DATABASE {name | CURRENT} SET READ_COMMITTED_SNAPSHOT /
/// ALLOW_SNAPSHOT_ISOLATION {ON|OFF}`. The batch's Database X lock has
/// quiesced the store: no snapshot is live, no writer is mid-transaction.
pub(super) fn exec_alter_database(
    storage: &Storage,
    alter: &AlterDatabase,
    txn_ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if let Some(name) = &alter.name
        && storage.rel_database_id_by_name(&name.value).is_none()
    {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                name.value
            ),
        )
        .at(name.span));
    }
    // FAILOVER (standby promotion) is offline-only, like RESTORE DATABASE: the
    // in-flight-transaction undo and the epoch bump run against a stopped
    // server. Checked before anything else — the pointer to the CLI is the
    // whole answer.
    if alter
        .options
        .iter()
        .any(|(option, _)| *option == DatabaseOption::Failover)
    {
        return Err(SqlError::new(
            3101,
            16,
            1,
            "Exclusive access could not be obtained because the database is in use. TruthDB \
             promotes a standby offline: stop the server and run `truthdb-cli promote`."
                .to_string(),
        ));
    }
    // A SNAPSHOT transaction idle between batches holds no locks, so the
    // batch's Database X does not prove no snapshot is live. Flipping the
    // options under one would reset (or stop publishing to) the store its
    // reads depend on; SQL Server waits the transactions out, TruthDB
    // refuses and lets the operator retry.
    if storage.has_registered_snapshots() {
        return Err(SqlError::new(
            5061,
            16,
            1,
            format!(
                "ALTER DATABASE failed because a lock could not be placed on database '{}'. \
                 Try again later.",
                txn_ctx.database
            ),
        ));
    }
    let mut rcsi = None;
    let mut allow_snapshot = None;
    let mut recovery_full = None;
    for (option, on) in &alter.options {
        match option {
            DatabaseOption::ReadCommittedSnapshot => rcsi = Some(*on),
            DatabaseOption::AllowSnapshotIsolation => allow_snapshot = Some(*on),
            // For Recovery the bool is the mode: true = FULL, false = SIMPLE.
            DatabaseOption::Recovery => recovery_full = Some(*on),
            // Returned as 3101 above, before this loop runs.
            DatabaseOption::Failover => unreachable!("failover is rejected before options apply"),
        }
    }
    storage
        .rel_set_db_options(rcsi, allow_snapshot, recovery_full)
        .map_err(|err| map_storage_err(err, &txn_ctx.database))?;
    Ok(StatementResult::Done)
}

pub(super) fn exec_alter_table(
    storage: &Storage,
    db_id: u32,
    alter: &AlterTable,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, db_id, &alter.table.value)
        .ok_or_else(|| SqlError::invalid_object(&alter.table.value).at(alter.table.span))?;
    reject_view_as_table(&def)?;
    match &alter.action {
        AlterAction::AddColumn(column) => alter_add_column(storage, &def, column, eval_ctx),
        AlterAction::AddCheck(check) => alter_add_check(storage, &def, check, eval_ctx),
        AlterAction::AddForeignKey(fk) => alter_add_foreign_key(storage, &def, fk),
        AlterAction::DropConstraint(name) => alter_drop_constraint(storage, &def, name),
    }
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY (...) REFERENCES ...`.
/// Validates the constraint and every existing row (WITH CHECK): a child row
/// referencing a missing parent is 547 and the constraint is not added.
pub(super) fn alter_add_foreign_key(
    storage: &Storage,
    def: &TableDef,
    fk: &ForeignKey,
) -> Result<StatementResult, SqlError> {
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let parent_bare = strip_schema(&fk.parent.value);
    let parent_pk: Vec<(String, ColumnType)> = if parent_bare.eq_ignore_ascii_case(&def.name) {
        def.key_columns
            .iter()
            .map(|&i| {
                (
                    schema.columns[i].name.clone(),
                    schema.columns[i].column_type,
                )
            })
            .collect()
    } else {
        let parent = resolve_table(storage, def.database_id, &fk.parent.value)
            .ok_or_else(|| SqlError::invalid_object(&fk.parent.value).at(fk.parent.span))?;
        let pschema = parent
            .schema()
            .map_err(|e| map_storage_err(e, &parent.name))?;
        parent
            .key_columns
            .iter()
            .map(|&i| {
                (
                    pschema.columns[i].name.clone(),
                    pschema.columns[i].column_type,
                )
            })
            .collect()
    };
    let existing_names: Vec<String> = def
        .check_constraints
        .iter()
        .map(|c| c.name.clone())
        .chain(def.foreign_keys.iter().map(|f| f.name.clone()))
        .collect();
    let new_def = bind_foreign_key(
        fk,
        &schema.columns,
        &def.name,
        &parent_pk,
        parent_bare,
        &existing_names,
    )?;

    // WITH CHECK: every existing child row must satisfy the new foreign key
    // (its sibling rows count for a self-reference).
    let rows = storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        if let Some(key) = fk_key(&new_def, row)
            && !fk_parent_exists(storage, &new_def, &key, def, &rows)?
        {
            return Err(fk_child_violation(
                &database_name_of(storage, def.database_id),
                &new_def.name,
                "ALTER TABLE",
                &new_def.parent,
            ));
        }
    }

    let mut fks = def.foreign_keys.clone();
    fks.push(new_def);
    storage
        .rel_set_foreign_keys(def.database_id, &def.name, fks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ADD <column>`: appends the column to the catalog and
/// rewrites every existing row under the new schema. The row codec is
/// positional (every offset derives from the schema, with no per-row version
/// stamp), so a metadata-only ADD cannot exist — the rewrite is the honest
/// implementation, one transactional statement under the ALTER's exclusive
/// lock. Existing rows take a FROZEN fill: NULL, or the DEFAULT evaluated
/// once now (SQL Server freezes it the same way); later INSERTs evaluate the
/// live default text per row like any other column.
pub(super) fn alter_add_column(
    storage: &Storage,
    def: &catalog::TableDef,
    column: &ColumnDef,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    if def
        .columns
        .iter()
        .any(|(name, _, _)| name.eq_ignore_ascii_case(&column.name.value))
    {
        return Err(SqlError::new(
            2705,
            16,
            4,
            format!(
                "Column names in each table must be unique. Column name '{}' is specified more than once.",
                column.name.value
            ),
        )
        .at(column.name.span));
    }
    // The plan's scope: a plain column with nullability, DEFAULT and COLLATE.
    // Constraint-carrying additions are their own statements in T-SQL anyway.
    if column.primary_key
        || column.unique
        || column.identity.is_some()
        || !column.checks.is_empty()
        || !column.foreign_keys.is_empty()
    {
        return Err(SqlError::new(
            40510,
            16,
            1,
            "ALTER TABLE ADD supports a plain column (with NULL/NOT NULL, DEFAULT and COLLATE); add constraints with their own ALTER TABLE ADD CONSTRAINT statements.",
        )
        .at(column.span));
    }
    let bound = bind_column(column)?;
    // An authoritative emptiness probe (one-row scan under the ALTER's
    // exclusive lock) — the row counter is a statistic and must not become
    // load-bearing here: an under-count would let NULL fills into a NOT NULL
    // column, and a pre-upgrade table without a counter would 4901 even when
    // empty.
    let has_rows = {
        let mut probe = Vec::new();
        storage
            .rel_scan_slice(
                def.database_id,
                &def.name,
                ScanCursor::start(),
                1,
                None,
                &mut probe,
            )
            .map_err(|err| map_storage_err(err, &def.name))?;
        !probe.is_empty()
    };
    // The frozen fill existing rows take.
    let fill = match &column.default {
        Some(text) => {
            let sql_value = eval_default(text, eval_ctx)?;
            value::sql_to_datum(&sql_value, &bound.column_type, &bound.name)?
        }
        None => Datum::Null,
    };
    if !bound.nullable && fill.is_null() && has_rows {
        return Err(SqlError::new(
            4901,
            16,
            1,
            format!(
                "ALTER TABLE only allows columns to be added that can contain nulls, or have a DEFAULT definition specified, or the column being added is an identity or timestamp column, or alternatively if none of the previous conditions are satisfied the table must be empty to allow addition of this column. Column '{}' cannot be added to non-empty table '{}' because it does not satisfy these conditions.",
                bound.name, def.name
            ),
        )
        .at(column.span));
    }
    storage
        .rel_alter_add_column(
            def.database_id,
            &def.name,
            bound,
            column.default.clone(),
            fill,
        )
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] CHECK (expr)`. Validates the new
/// constraint against every existing row (SQL Server's default WITH CHECK); a
/// violating row is error 547 and the constraint is not added.
pub(super) fn alter_add_check(
    storage: &Storage,
    def: &TableDef,
    check: &CheckConstraint,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    // Constraint names are unique across kinds (CHECK and FOREIGN KEY).
    let existing: Vec<String> = def
        .check_constraints
        .iter()
        .map(|c| c.name.clone())
        .chain(def.foreign_keys.iter().map(|f| f.name.clone()))
        .collect();
    let new_def = bind_check(check, &schema.columns, &def.name, &existing)?;

    // WITH CHECK: no existing row may violate the new constraint.
    let compiled = vec![(
        new_def.name.clone(),
        truthdb_sql::parse_expr(&new_def.predicate)?,
    )];
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let rows = storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        let scope = row_values(row, &types);
        enforce_checks(
            storage,
            &compiled,
            &scope,
            &resolver,
            eval_ctx,
            "ALTER TABLE",
            &database_name_of(storage, def.database_id),
            &def.name,
        )?;
    }

    let mut checks = def.check_constraints.clone();
    checks.push(new_def);
    storage
        .rel_set_check_constraints(def.database_id, &def.name, checks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... DROP CONSTRAINT name`. Removes a CHECK or FOREIGN KEY
/// constraint by name (case-insensitive); an unknown name is error 3728.
pub(super) fn alter_drop_constraint(
    storage: &Storage,
    def: &TableDef,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    if def
        .check_constraints
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&name.value))
    {
        let checks: Vec<catalog::CheckDef> = def
            .check_constraints
            .iter()
            .filter(|c| !c.name.eq_ignore_ascii_case(&name.value))
            .cloned()
            .collect();
        storage
            .rel_set_check_constraints(def.database_id, &def.name, checks)
            .map_err(|e| map_storage_err(e, &def.name))?;
        return Ok(StatementResult::Done);
    }
    if def
        .foreign_keys
        .iter()
        .any(|f| f.name.eq_ignore_ascii_case(&name.value))
    {
        let fks: Vec<catalog::ForeignKeyDef> = def
            .foreign_keys
            .iter()
            .filter(|f| !f.name.eq_ignore_ascii_case(&name.value))
            .cloned()
            .collect();
        storage
            .rel_set_foreign_keys(def.database_id, &def.name, fks)
            .map_err(|e| map_storage_err(e, &def.name))?;
        return Ok(StatementResult::Done);
    }
    Err(SqlError::new(
        3728,
        16,
        1,
        format!("'{}' is not a constraint.", name.value),
    )
    .at(name.span))
}
