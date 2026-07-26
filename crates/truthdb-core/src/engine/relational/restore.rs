use super::prelude::*;

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
