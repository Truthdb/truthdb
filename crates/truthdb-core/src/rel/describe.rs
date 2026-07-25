use super::prelude::*;

/// `sp_describe_first_result_set`: the column metadata of `tsql`'s first
/// statement's result set, without executing it. Covers exactly the shapes
/// whose output columns are statically derivable — the single-table SELECTs
/// [`scan_plan`] recognises (their COLMETADATA is derived before the first
/// row at execution too). A statement producing no result set describes as
/// zero rows, matching SQL Server. Everything else — joins, aggregates,
/// computed columns — has result types this engine only learns by executing
/// (`infer_type` over the values), so it answers 11514: honest, and the same
/// static-type-derivation gap Stage 8's output streaming tracks.
///
/// `@tsql`'s declared parameters need no binding here: the planner treats an
/// unresolvable variable as a non-seekable predicate and falls back to the
/// scan, and output columns come from the table schema either way — pinned by
/// the parameterized-describe test.
pub fn describe_first_result_set(storage: &Storage, tsql: &str) -> Result<RowSet, SqlError> {
    let undeterminable = || {
        SqlError::new(
            11514,
            16,
            1,
            "The metadata could not be determined because the statement's result-set \
             types are not statically derivable by this server (single-table SELECTs \
             are; joins, aggregates and computed columns are typed at execution).",
        )
    };
    let statements = truthdb_sql::parse(tsql)?;
    // The contract is the batch's first RESULT SET, not its first statement:
    // `INSERT ...; SELECT ...` describes the SELECT. TRY blocks are entered
    // (their statements run unless an error preempts them); a rowset only a
    // CATCH can produce stays undescribed — that path is conditional.
    let columns = match first_rowset_statement(&statements) {
        None => Vec::new(),
        Some(Statement::Select(select)) => {
            // TOP never changes the columns, and `scan_plan` rejects `TOP 0`
            // for an execution-path reason describe does not share.
            let mut select = select.clone();
            select.top = None;
            let mut ctx = TxnContext::default();
            // Describe derives column metadata without executing or reading data;
            // it does not enforce object permissions (and its throwaway context
            // carries no session identity), so the shared `scan_plan` must not
            // treat it as a denied read.
            ctx.security.bypass = true;
            match scan_plan(storage, &select, &ctx.eval_context()) {
                Some(plan) => plan.columns,
                None => return Err(undeterminable()),
            }
        }
        Some(_) => return Err(undeterminable()),
    };

    let mut rows = Vec::new();
    for (ordinal, column) in columns.iter().enumerate() {
        let (type_id, type_name) = describe_type(&column.column_type);
        rows.push(vec![
            Datum::Bit(false),                    // is_hidden
            Datum::Int(ordinal as i32 + 1),       // column_ordinal
            Datum::NVarChar(column.name.clone()), // name
            Datum::Bit(true), // is_nullable (result metadata is nullable, as at execution)
            Datum::Int(type_id), // system_type_id
            Datum::NVarChar(type_name), // system_type_name
        ]);
    }
    Ok(RowSet {
        columns: vec![
            ResultColumn {
                name: "is_hidden".into(),
                column_type: ColumnType::Bit,
            },
            ResultColumn {
                name: "column_ordinal".into(),
                column_type: ColumnType::Int,
            },
            ResultColumn {
                name: "name".into(),
                column_type: ColumnType::NVarChar { max_len: 128 },
            },
            ResultColumn {
                name: "is_nullable".into(),
                column_type: ColumnType::Bit,
            },
            ResultColumn {
                name: "system_type_id".into(),
                column_type: ColumnType::Int,
            },
            ResultColumn {
                name: "system_type_name".into(),
                column_type: ColumnType::NVarChar { max_len: 256 },
            },
        ],
        rows,
    })
}

/// The first statement in `statements` that opens a result set, descending
/// into TRY blocks (entered unless an error preempts them) but not CATCH
/// blocks (conditional).
pub(super) fn first_rowset_statement(statements: &[Statement]) -> Option<&Statement> {
    statements.iter().find_map(|statement| match statement {
        Statement::TryCatch { try_block, .. } => first_rowset_statement(try_block),
        s if produces_rowset(s) => Some(s),
        _ => None,
    })
}

/// A column type's `sys.types` id and its `system_type_name` spelling, as
/// `sp_describe_first_result_set` reports them.
pub(super) fn describe_type(column_type: &ColumnType) -> (i32, String) {
    match column_type {
        ColumnType::TinyInt => (48, "tinyint".into()),
        ColumnType::SmallInt => (52, "smallint".into()),
        ColumnType::Int => (56, "int".into()),
        ColumnType::BigInt => (127, "bigint".into()),
        ColumnType::Bit => (104, "bit".into()),
        ColumnType::Real => (59, "real".into()),
        ColumnType::Float => (62, "float".into()),
        ColumnType::Decimal { precision, scale } => (106, format!("decimal({precision},{scale})")),
        ColumnType::Date => (40, "date".into()),
        ColumnType::Time => (41, "time".into()),
        ColumnType::DateTime2 => (42, "datetime2".into()),
        ColumnType::UniqueIdentifier => (36, "uniqueidentifier".into()),
        ColumnType::VarChar { max_len } => (167, format!("varchar({max_len})")),
        ColumnType::NVarChar { max_len } => (231, format!("nvarchar({max_len})")),
        ColumnType::VarBinary { max_len } => (165, format!("varbinary({max_len})")),
        ColumnType::VarCharMax => (167, "varchar(max)".into()),
        ColumnType::NVarCharMax => (231, "nvarchar(max)".into()),
        ColumnType::VarBinaryMax => (165, "varbinary(max)".into()),
    }
}

/// Whether a statement can open a result set on the stream: a `SELECT` that
/// returns rows (an assignment `SELECT @v = ...` returns none). `SET
/// SHOWPLAN_TEXT`'s plan rows ride the same `SELECT` arm.
pub(super) fn produces_rowset(statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => !select
            .items
            .iter()
            .any(|i| matches!(i, SelectItem::Assign { .. })),
        // EXEC's inner batch — and any control-flow body — may open result
        // sets; conservative.
        Statement::Exec(_) => true,
        Statement::Block { .. } | Statement::If { .. } | Statement::While { .. } => true,
        // The header/file-list restore verbs return a rowset; VERIFYONLY and the
        // offline DATABASE/LOG forms do not.
        Statement::Restore { mode, .. } => {
            matches!(mode, RestoreMode::HeaderOnly | RestoreMode::FileListOnly)
        }
        // A `FETCH` without an INTO list returns the fetched row to the client.
        Statement::FetchCursor { into, .. } => into.is_empty(),
        _ => false,
    }
}
