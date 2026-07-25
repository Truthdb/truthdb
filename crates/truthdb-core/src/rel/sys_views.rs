use super::prelude::*;

// ---- sys.* virtual sources ---------------------------------------------

pub(super) fn nvarchar(name: &str, max_len: u16) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::NVarChar { max_len },
    }
}

pub(super) fn int_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::Int,
    }
}

pub(super) fn bigint_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::BigInt,
    }
}

pub(super) fn bit_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::Bit,
    }
}

/// `RESTORE VERIFYONLY/HEADERONLY/FILELISTONLY` (online, read-only), plus a clear
/// error for the offline-only `RESTORE DATABASE`/`LOG`. Like BACKUP, restore is
/// illegal inside a transaction (3021).
pub(super) fn exec_restore(
    mode: RestoreMode,
    path: &str,
    txn_ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if txn_ctx.in_txn() {
        return Err(SqlError::new(
            3021,
            16,
            1,
            "Cannot perform a backup or restore operation within a transaction.".to_string(),
        ));
    }
    let file = std::path::Path::new(path);
    let terminating = |verb: &str, e: std::io::Error| {
        SqlError::new(
            3013,
            16,
            1,
            format!("RESTORE {verb} is terminating abnormally. {e}"),
        )
    };
    match mode {
        RestoreMode::VerifyOnly => {
            crate::backup::verify(file).map_err(|e| terminating("VERIFYONLY", e))?;
            Ok(StatementResult::Done)
        }
        RestoreMode::HeaderOnly => {
            let header =
                crate::backup::read_header(file).map_err(|e| terminating("HEADERONLY", e))?;
            Ok(StatementResult::Rows(restore_headeronly_rows(&header)))
        }
        RestoreMode::FileListOnly => {
            let header =
                crate::backup::read_header(file).map_err(|e| terminating("FILELISTONLY", e))?;
            Ok(StatementResult::Rows(restore_filelist_rows(&header)))
        }
        RestoreMode::Database | RestoreMode::Log => Err(SqlError::new(
            3101,
            16,
            1,
            "Exclusive access could not be obtained because the database is in use. TruthDB \
             restores a database offline: stop the server and run `truthdb-cli restore`."
                .to_string(),
        )),
    }
}

/// One metadata row for `RESTORE HEADERONLY`.
pub(super) fn restore_headeronly_rows(header: &crate::backup::BackupHeader) -> RowSet {
    let columns = vec![
        int_col("BackupType"),
        int_col("FormatVersion"),
        int_col("PageSize"),
        bigint_col("DatabaseSize"),
        bit_col("Checksum"),
        bit_col("CopyOnly"),
        bigint_col("RedoStartLSN"),
        bigint_col("LastCommittedSeq"),
        int_col("DbOptions"),
        nvarchar("Collation", 128),
        bigint_col("BackupFinishDate"),
    ];
    // BackupType: 1 = full database, 2 = transaction log (SQL Server's coding).
    let backup_type = if header.flags.log_backup { 2 } else { 1 };
    let row = vec![
        Datum::Int(backup_type),
        Datum::Int(header.format_version as i32),
        Datum::Int(header.page_size as i32),
        Datum::BigInt(header.total_size as i64),
        Datum::Bit(header.flags.checksum),
        Datum::Bit(header.flags.copy_only),
        Datum::BigInt(header.redo_start_lsn as i64),
        Datum::BigInt(header.last_committed_seq as i64),
        Datum::Int(header.db_options as i32),
        match &header.default_collation {
            Some(c) => Datum::NVarChar(c.clone()),
            None => Datum::Null,
        },
        Datum::BigInt(header.finished_at_millis as i64),
    ];
    RowSet {
        columns,
        rows: vec![row],
    }
}

/// The data + log "files" for `RESTORE FILELISTONLY`. A log-only archive reports
/// a zero data size (it holds no page data) but keeps its WAL region size;
/// `HEADERONLY`'s BackupType = 2 marks it as a log backup.
pub(super) fn restore_filelist_rows(header: &crate::backup::BackupHeader) -> RowSet {
    let columns = vec![
        nvarchar("LogicalName", 128),
        nvarchar("Type", 1),
        bigint_col("Size"),
    ];
    let rows = vec![
        vec![
            Datum::NVarChar("truthdb_data".to_string()),
            Datum::NVarChar("D".to_string()),
            Datum::BigInt(header.data_size as i64),
        ],
        vec![
            Datum::NVarChar("truthdb_log".to_string()),
            Datum::NVarChar("L".to_string()),
            Datum::BigInt(header.wal_size as i64),
        ],
    ];
    RowSet { columns, rows }
}

pub(super) fn sys_tables(storage: &Storage, db_id: u32) -> Source {
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

/// `sys.views` — one row per view, with its stored definition text.
/// `sys.dm_repl_slots` (Stage 18): the primary's replication slots and the
/// WAL-ring positions they pin. Empty on a standby (and on a primary with no
/// standby ever connected).
pub(super) fn sys_dm_repl_slots(storage: &Storage) -> Source {
    let columns = vec![
        int_col("slot_id"),
        bigint_col("held_lsn"),
        bigint_col("wal_head"),
        bigint_col("wal_tail"),
        bigint_col("retained_bytes"),
    ];
    let head = storage.wal_head();
    let tail = storage.wal_tail();
    let rows: Vec<Vec<Datum>> = storage
        .repl_slots_snapshot()
        .into_iter()
        .map(|(id, lsn)| {
            vec![
                Datum::Int(id as i32),
                Datum::BigInt(lsn as i64),
                Datum::BigInt(head as i64),
                Datum::BigInt(tail as i64),
                Datum::BigInt(tail.saturating_sub(lsn) as i64),
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

/// `sys.dm_repl_replica_states` (Stage 18): this node's replication role and,
/// on a primary, one row per replication slot with its connection and lag
/// state; on a standby, one row describing its own applied position.
pub(super) fn sys_dm_repl_replica_states(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("role", 20),
        int_col("node_id"),
        ResultColumn {
            name: "is_connected".into(),
            column_type: ColumnType::Bit,
        },
        bigint_col("acked_lsn"),
        bigint_col("durable_lsn"),
        bigint_col("lag_bytes"),
        nvarchar("sync_state", 30),
        bigint_col("epoch"),
    ];
    let epoch = storage.epoch() as i64;
    let rows = if storage.is_standby() {
        vec![vec![
            Datum::NVarChar("STANDBY".into()),
            Datum::Null,
            Datum::Null,
            Datum::BigInt(storage.applied_lsn() as i64),
            Datum::BigInt(storage.wal_flushed_lsn() as i64),
            Datum::BigInt(0),
            Datum::NVarChar("NOT_APPLICABLE".into()),
            Datum::BigInt(epoch),
        ]]
    } else {
        let durable = storage.wal_flushed_lsn();
        let connected = storage.repl_connected_nodes();
        let sync_state = match storage.sync_commit_status() {
            None => "ASYNC",
            Some(false) => "SYNCHRONIZED",
            Some(true) => "NOT_SYNCHRONIZED",
        };
        storage
            .repl_slots_snapshot()
            .into_iter()
            .map(|(id, lsn)| {
                vec![
                    Datum::NVarChar("PRIMARY".into()),
                    Datum::Int(id as i32),
                    Datum::Bit(connected.contains(&id)),
                    Datum::BigInt(lsn as i64),
                    Datum::BigInt(durable as i64),
                    Datum::BigInt(durable.saturating_sub(lsn) as i64),
                    Datum::NVarChar(sync_state.into()),
                    Datum::BigInt(epoch),
                ]
            })
            .collect()
    };
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

/// `sys.databases` (Stage 14, SSMS query-window probes): the one database
/// this instance serves, with the columns tools actually read. The
/// versioning flags report the live `ALTER DATABASE` options.
pub(super) fn sys_databases(storage: &Storage) -> Source {
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
pub(super) fn sys_configurations() -> Source {
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

pub(super) fn sys_views(storage: &Storage, db_id: u32) -> Source {
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
pub(super) fn sys_sql_modules(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_procedures(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_triggers(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_trigger_events(storage: &Storage, db_id: u32) -> Source {
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
                catalog::TriggerEvent::Insert => (1, "INSERT"),
                catalog::TriggerEvent::Update => (2, "UPDATE"),
                catalog::TriggerEvent::Delete => (3, "DELETE"),
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

pub(super) fn sys_server_principals(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        nvarchar("type", 1),
        nvarchar("type_desc", 60),
        int_col("is_disabled"),
    ];
    let mut rows: Vec<Vec<Datum>> = storage
        .rel_logins()
        .into_iter()
        .filter_map(|def| {
            let principal = def.principal.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                // SQL logins: type 'S' / SQL_LOGIN.
                Datum::NVarChar("S".to_string()),
                Datum::NVarChar("SQL_LOGIN".to_string()),
                Datum::Int(i32::from(principal.is_disabled)),
            ])
        })
        .collect();
    // The fixed server roles (today: sysadmin) — type 'R' / SERVER_ROLE.
    for fixed in crate::storage::FIXED_PRINCIPALS
        .iter()
        .filter(|p| p.is_server)
    {
        rows.push(vec![
            Datum::NVarChar(fixed.name.to_string()),
            Datum::Int(fixed.id as i32),
            Datum::NVarChar("R".to_string()),
            Datum::NVarChar("SERVER_ROLE".to_string()),
            Datum::Int(0),
        ]);
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

pub(super) fn sys_sql_logins(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        int_col("is_disabled"),
    ];
    let rows = storage
        .rel_logins()
        .into_iter()
        .filter_map(|def| {
            let principal = def.principal.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::Int(i32::from(principal.is_disabled)),
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

pub(super) fn sys_database_principals(storage: &Storage) -> Source {
    use crate::relstore::catalog::PrincipalKind;
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        nvarchar("type", 1),
        nvarchar("type_desc", 60),
        nvarchar("default_schema_name", 128),
        int_col("owning_principal_id"),
    ];
    // A user (SQL_USER 'S') defaults to the dbo schema; a role (DATABASE_ROLE
    // 'R') has no default schema.
    let row = |name: String, id: u32, kind: PrincipalKind| {
        let (type_code, type_desc) = match kind {
            PrincipalKind::Role => ("R", "DATABASE_ROLE"),
            _ => ("S", "SQL_USER"),
        };
        let default_schema = if matches!(kind, PrincipalKind::User) {
            Datum::NVarChar("dbo".to_string())
        } else {
            Datum::Null
        };
        vec![
            Datum::NVarChar(name),
            Datum::Int(id as i32),
            Datum::NVarChar(type_code.to_string()),
            Datum::NVarChar(type_desc.to_string()),
            default_schema,
            Datum::Null, // owning_principal_id
        ]
    };
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // Fixed database principals (dbo + the fixed database roles + public); the
    // server-scoped sysadmin role belongs to sys.server_principals instead.
    for fixed in crate::storage::FIXED_PRINCIPALS
        .iter()
        .filter(|p| !p.is_server)
    {
        rows.push(row(fixed.name.to_string(), fixed.id, fixed.kind));
    }
    for def in storage.rel_database_principals() {
        if let Some(principal) = def.principal.as_ref() {
            rows.push(row(def.name.clone(), def.object_id, principal.kind));
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

pub(super) fn sys_database_role_members(storage: &Storage) -> Source {
    let columns = vec![int_col("role_principal_id"), int_col("member_principal_id")];
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // Synthesized: the dbo user is a member of db_owner.
    rows.push(vec![
        Datum::Int(crate::storage::DB_OWNER_ID as i32),
        Datum::Int(crate::storage::DBO_ID as i32),
    ]);
    // Stored database membership edges (member -> role, from each member's row).
    for def in storage.rel_database_principals() {
        if let Some(principal) = def.principal.as_ref() {
            for &role_id in &principal.member_of {
                rows.push(vec![
                    Datum::Int(role_id as i32),
                    Datum::Int(def.object_id as i32),
                ]);
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

pub(super) fn sys_database_permissions(storage: &Storage, db_id: u32) -> Source {
    let columns = vec![
        int_col("class"),
        nvarchar("class_desc", 60),
        int_col("major_id"),
        int_col("minor_id"),
        int_col("grantee_principal_id"),
        nvarchar("permission_name", 128),
        nvarchar("state", 1),
        nvarchar("state_desc", 60),
    ];
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        for perm in &def.permissions {
            let (state, state_desc) = if perm.deny {
                ("D", "DENY")
            } else {
                ("G", "GRANT")
            };
            rows.push(vec![
                Datum::Int(1), // class 1 = OBJECT_OR_COLUMN
                Datum::NVarChar("OBJECT_OR_COLUMN".to_string()),
                Datum::Int(def.object_id as i32),
                Datum::Int(0), // minor_id 0 = the whole object (no column-level)
                Datum::Int(perm.grantee as i32),
                Datum::NVarChar(perm.action.name().to_string()),
                Datum::NVarChar(state.to_string()),
                Datum::NVarChar(state_desc.to_string()),
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

pub(super) fn sys_parameters(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_objects(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_columns(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_indexes(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_check_constraints(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_foreign_keys(storage: &Storage, db_id: u32) -> Source {
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

pub(super) fn sys_default_constraints(storage: &Storage, db_id: u32) -> Source {
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
