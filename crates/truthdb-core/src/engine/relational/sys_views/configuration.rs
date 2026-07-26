use super::*;

/// `sys.databases` (Stage 14, SSMS query-window probes): the one database
/// this instance serves, with the columns tools actually read. The
/// versioning flags report the live `ALTER DATABASE` options.
pub(in crate::engine::relational) fn sys_databases(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("database_id"),
        int_col("compatibility_level"),
        nvarchar("collation_name", 128),
        nvarchar("user_access_desc", 60),
        nvarchar("state_desc", 60),
        nvarchar("recovery_model_desc", 60),
        int_col("snapshot_isolation_state"),
        ResultColumn {
            name: "is_read_committed_snapshot_on".into(),
            column_type: ColumnType::Bit,
        },
        ResultColumn {
            name: "is_read_only".into(),
            column_type: ColumnType::Bit,
        },
    ];
    // One row per database. The option columns are instance-wide (one
    // shared log and version store), so every row reports the same values —
    // the documented level-1 deviation.
    let rows = storage
        .rel_databases()
        .into_iter()
        .map(|(id, name)| {
            vec![
                Datum::NVarChar(name),
                Datum::Int(id as i32),
                Datum::Int(160),
                Datum::NVarChar("SQL_Latin1_General_CP1_CI_AS".into()),
                Datum::NVarChar("MULTI_USER".into()),
                Datum::NVarChar("ONLINE".into()),
                Datum::NVarChar(
                    if storage.recovery_model_full() {
                        "FULL"
                    } else {
                        "SIMPLE"
                    }
                    .into(),
                ),
                Datum::Int(storage.snapshot_isolation_allowed() as i32),
                Datum::Bit(storage.rcsi_enabled()),
                Datum::Bit(false),
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

/// `sys.configurations` (Stage 14): the handful of static rows connection
/// tools probe. Values are INT here (SQL Server uses sql_variant) — a
/// documented simplification.
pub(in crate::engine::relational) fn sys_configurations() -> Source {
    let columns = vec![
        int_col("configuration_id"),
        nvarchar("name", 35),
        int_col("value"),
        int_col("minimum"),
        int_col("maximum"),
        int_col("value_in_use"),
        nvarchar("description", 255),
        ResultColumn {
            name: "is_dynamic".into(),
            column_type: ColumnType::Bit,
        },
        ResultColumn {
            name: "is_advanced".into(),
            column_type: ColumnType::Bit,
        },
    ];
    let entry =
        |id: i32, name: &str, value: i32, min: i32, max: i32, dynamic: bool, advanced: bool| {
            vec![
                Datum::Int(id),
                Datum::NVarChar(name.into()),
                Datum::Int(value),
                Datum::Int(min),
                Datum::Int(max),
                Datum::Int(value),
                Datum::NVarChar(name.into()),
                Datum::Bit(dynamic),
                Datum::Bit(advanced),
            ]
        };
    let rows = vec![
        entry(16384, "show advanced options", 0, 0, 1, true, false),
        entry(1539, "user options", 0, 0, 32767, true, false),
        entry(
            1544,
            "max server memory (MB)",
            i32::MAX,
            16,
            i32::MAX,
            true,
            true,
        ),
    ];
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}
