use super::*;

pub(in crate::engine::relational) fn sys_tables(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![int_col("object_id"), nvarchar("name", 128)];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.database_id == db_id)
        // Only base tables. (The `!is_view()` filter alone let procedures leak
        // in — a pre-existing gap — so exclude every non-table object kind.)
        .filter(|def| {
            !def.is_view() && !def.is_procedure() && !def.is_function() && !def.is_trigger()
        })
        .map(|def| vec![Datum::Int(def.object_id as i32), Datum::NVarChar(def.name)])
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_parameters(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        int_col("object_id"),
        nvarchar("name", 128),
        int_col("parameter_id"),
        nvarchar("system_type_name", 128),
        int_col("is_output"),
        int_col("has_default_value"),
    ];
    let mut rows = Vec::new();
    let mut push_param = |object_id: u32,
                          name: String,
                          id: i32,
                          type_spec: String,
                          output: bool,
                          has_default: bool| {
        rows.push(vec![
            Datum::Int(object_id as i32),
            Datum::NVarChar(name),
            Datum::Int(id),
            Datum::NVarChar(type_spec),
            Datum::Int(i32::from(output)),
            Datum::Int(i32::from(has_default)),
        ]);
    };
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        if let Some(procedure) = &def.procedure {
            for (index, param) in procedure.params.iter().enumerate() {
                push_param(
                    def.object_id,
                    format!("@{}", param.name),
                    index as i32 + 1,
                    param.type_spec.clone(),
                    param.output,
                    param.default.is_some(),
                );
            }
        } else if let Some(function) = &def.function {
            // A SCALAR function's return value is parameter_id 0 (empty name,
            // is_output set — SQL Server's convention). A table-valued function
            // returns a table, so it has no scalar return parameter.
            if let FunctionReturns::Scalar { type_spec, .. } = &function.returns {
                push_param(
                    def.object_id,
                    String::new(),
                    0,
                    type_spec.clone(),
                    true,
                    false,
                );
            }
            for (index, param) in function.params.iter().enumerate() {
                push_param(
                    def.object_id,
                    format!("@{}", param.name),
                    index as i32 + 1,
                    param.type_spec.clone(),
                    false,
                    param.default.is_some(),
                );
            }
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_objects(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("object_id"),
        nvarchar("type", 2),
        nvarchar("type_desc", 60),
    ];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.database_id == db_id)
        .map(|def| {
            // SQL Server's single-letter codes carry a trailing space; the
            // two-letter function codes fill CHAR(2) exactly.
            let (code, desc) = if let Some(function) = &def.function {
                match function.returns {
                    FunctionReturns::Scalar { .. } => ("FN", "SQL_SCALAR_FUNCTION"),
                    FunctionReturns::InlineTable { .. } => {
                        ("IF", "SQL_INLINE_TABLE_VALUED_FUNCTION")
                    }
                    FunctionReturns::MultiStatementTable { .. } => {
                        ("TF", "SQL_TABLE_VALUED_FUNCTION")
                    }
                }
            } else if def.is_procedure() {
                ("P ", "SQL_STORED_PROCEDURE")
            } else if def.is_trigger() {
                ("TR", "SQL_TRIGGER")
            } else if def.is_view() {
                ("V ", "VIEW")
            } else {
                ("U ", "USER_TABLE")
            };
            vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(code.to_string()),
                Datum::NVarChar(desc.to_string()),
            ]
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_columns(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        int_col("object_id"),
        nvarchar("name", 128),
        int_col("column_id"),
        nvarchar("type", 128),
        ResultColumn {
            name: "is_nullable".to_string(),
            column_type: ColumnType::Bit,
        },
        nvarchar("collation_name", 128),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        for (index, (name, type_spec, nullable)) in def.columns.iter().enumerate() {
            let collation = def
                .collations
                .get(index)
                .cloned()
                .flatten()
                .map(Datum::NVarChar)
                .unwrap_or(Datum::Null);
            rows.push(vec![
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(name.clone()),
                Datum::Int(index as i32 + 1),
                Datum::NVarChar(type_spec.clone()),
                Datum::Bit(*nullable),
                collation,
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_indexes(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        int_col("object_id"),
        int_col("index_id"),
        nvarchar("name", 128),
        ResultColumn {
            name: "is_unique".to_string(),
            column_type: ColumnType::Bit,
        },
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        for index in &def.indexes {
            rows.push(vec![
                Datum::Int(def.object_id as i32),
                Datum::Int(index.object_id as i32),
                Datum::NVarChar(index.name.clone()),
                Datum::Bit(index.unique),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_check_constraints(
    storage: &Storage,
    db_id: u32,
) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("parent_object_id"),
        nvarchar("definition", 4000),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        for check in &def.check_constraints {
            rows.push(vec![
                Datum::NVarChar(check.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(format!("({})", check.predicate)),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_foreign_keys(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("parent_object_id"),
        int_col("referenced_object_id"),
    ];
    // Resolve parent (referenced) table names to object ids.
    let tables: Vec<TableDef> = storage
        .rel_tables()
        .into_iter()
        .filter(|t| t.database_id == db_id)
        .collect();
    let oid_of = |name: &str| {
        tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .map(|t| t.object_id)
    };
    let mut rows = Vec::new();
    for def in &tables {
        for fk in &def.foreign_keys {
            rows.push(vec![
                Datum::NVarChar(fk.name.clone()),
                Datum::Int(def.object_id as i32),
                oid_of(&fk.parent)
                    .map(|o| Datum::Int(o as i32))
                    .unwrap_or(Datum::Null),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_default_constraints(
    storage: &Storage,
    db_id: u32,
) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("parent_object_id"),
        int_col("parent_column_id"),
        nvarchar("definition", 4000),
    ];
    // Inline column DEFAULTs are unnamed; SQL Server auto-names them
    // `DF__<table>__<column>__...`. We synthesize a stable `DF__<table>__<col>`.
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        for (index, text) in def.defaults.iter().enumerate() {
            let Some(text) = text else { continue };
            let column = &def.columns[index].0;
            rows.push(vec![
                Datum::NVarChar(format!("DF__{}__{}", def.name, column)),
                Datum::Int(def.object_id as i32),
                Datum::Int(index as i32 + 1),
                Datum::NVarChar(format!("({text})")),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}
