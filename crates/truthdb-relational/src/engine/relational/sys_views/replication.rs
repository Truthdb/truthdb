use super::*;

/// `sys.views` — one row per view, with its stored definition text.
/// `sys.dm_repl_slots` (Stage 18): the primary's replication slots and the
/// WAL-ring positions they pin. Empty on a standby (and on a primary with no
/// standby ever connected).
pub(in crate::engine::relational) fn sys_dm_repl_slots(storage: &RelStorage) -> Source {
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

/// `sys.dm_database_encryption_keys`: the database's encryption at rest, in
/// SQL Server's columns. `encryption_state` 1 = unencrypted, 3 = encrypted.
/// Every database of the file shares it; the row names the current one.
pub(in crate::engine::relational) fn sys_dm_database_encryption_keys(
    storage: &RelStorage,
    database_id: u32,
) -> Source {
    let columns = vec![
        int_col("database_id"),
        int_col("encryption_state"),
        nvarchar("key_algorithm", 32),
        int_col("key_length"),
        nvarchar("encryptor_type", 32),
        nvarchar("encryptor_thumbprint", 64),
        nvarchar("key_fingerprint", 32),
        int_col("percent_complete"),
    ];
    let status = storage.encryption_status();
    let text = |value: Option<String>| value.map_or(Datum::Null, Datum::NVarChar);
    let row = if status.encrypted {
        vec![
            Datum::Int(database_id as i32),
            Datum::Int(3),
            Datum::NVarChar("AES".to_string()),
            Datum::Int(256),
            Datum::NVarChar(status.encryptor_type.to_string()),
            text(status.key_id),
            text(
                status
                    .fingerprint
                    .map(|bytes| bytes.iter().map(|byte| format!("{byte:02x}")).collect()),
            ),
            Datum::Int(0),
        ]
    } else {
        vec![
            Datum::Int(database_id as i32),
            Datum::Int(1),
            Datum::Null,
            Datum::Null,
            Datum::NVarChar("NONE".to_string()),
            Datum::Null,
            Datum::Null,
            Datum::Int(0),
        ]
    };
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(vec![row]),
    }
}

/// `sys.dm_repl_replica_states` (Stage 18): this node's replication role and,
/// on a primary, one row per replication slot with its connection and lag
/// state; on a standby, one row describing its own applied position.
pub(in crate::engine::relational) fn sys_dm_repl_replica_states(storage: &RelStorage) -> Source {
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
