use super::super::prelude::*;

// ---- CREATE TABLE -------------------------------------------------------

pub(in crate::engine::relational) fn exec_create_table(
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

pub(in crate::engine::relational) fn exec_drop_table(
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
