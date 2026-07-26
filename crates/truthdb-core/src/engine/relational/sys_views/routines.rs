use super::*;

pub(in crate::engine::relational) fn sys_views(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        int_col("object_id"),
        nvarchar("name", 128),
        nvarchar("definition", 4000),
    ];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.database_id == db_id)
        .filter_map(|def| {
            def.view_query.map(|q| {
                vec![
                    Datum::Int(def.object_id as i32),
                    Datum::NVarChar(def.name),
                    Datum::NVarChar(q),
                ]
            })
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

/// `sys.sql_modules`: the SQL definition of each module (currently views), keyed
/// by `object_id`. SQL Server surfaces view/procedure/trigger text here; today
/// only views carry a definition.
pub(in crate::engine::relational) fn sys_sql_modules(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![int_col("object_id"), nvarchar("definition", 4000)];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.database_id == db_id)
        .filter_map(|def| {
            // Views store their SELECT; procedures and functions their body.
            let definition = def
                .view_query
                .clone()
                .or_else(|| def.procedure.as_ref().map(|p| p.body.clone()))
                .or_else(|| {
                    def.function.as_ref().map(|f| match &f.returns {
                        FunctionReturns::Scalar { body, .. } => body.clone(),
                        FunctionReturns::InlineTable { select_text } => select_text.clone(),
                        FunctionReturns::MultiStatementTable { body, .. } => body.clone(),
                    })
                })
                .or_else(|| def.trigger.as_ref().map(|t| t.body.clone()))?;
            Some(vec![
                Datum::Int(def.object_id as i32),
                Datum::NVarChar(definition),
            ])
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

pub(in crate::engine::relational) fn sys_procedures(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![nvarchar("name", 128), int_col("object_id")];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.database_id == db_id)
        .filter(|def| def.is_procedure())
        .map(|def| {
            vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
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

pub(in crate::engine::relational) fn sys_triggers(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("object_id"),
        int_col("parent_id"),
        nvarchar("type", 2),
        int_col("is_disabled"),
        int_col("is_instead_of_trigger"),
    ];
    let rows = storage
        .rel_tables()
        .into_iter()
        .filter(|def| def.database_id == db_id)
        .filter_map(|def| {
            let trigger = def.trigger.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::Int(trigger.parent_object_id as i32),
                Datum::NVarChar("TR".to_string()),
                Datum::Int(i32::from(trigger.is_disabled)),
                Datum::Int(i32::from(trigger.is_instead_of)),
            ])
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

pub(in crate::engine::relational) fn sys_trigger_events(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        int_col("object_id"),
        int_col("type"),
        nvarchar("type_desc", 128),
    ];
    let mut rows = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        let Some(trigger) = def.trigger.as_ref() else {
            continue;
        };
        for event in &trigger.events {
            let (code, desc) = match event {
                crate::relstore::catalog::TriggerEvent::Insert => (1, "INSERT"),
                crate::relstore::catalog::TriggerEvent::Update => (2, "UPDATE"),
                crate::relstore::catalog::TriggerEvent::Delete => (3, "DELETE"),
            };
            rows.push(vec![
                Datum::Int(def.object_id as i32),
                Datum::Int(code),
                Datum::NVarChar(desc.to_string()),
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
