use super::super::prelude::*;

/// The ONE place a failed statement's fate is decided — continue the batch
/// (`Ok(())`), or end it (`Err`, dooming already applied). The doom decision
/// needs the statement's KIND (RAISERROR is exempt from XACT_ABORT; THROW is
/// batch-terminating without dooming), so every decide-now error site funnels
/// here: the generic statement arm and IF/WHILE condition failures. (EXEC
/// boundary errors do NOT — theirs were decided at the source, in the inner
/// `run_block` or `doom_per_rule`.)
pub(in crate::engine::relational) fn statement_error_ladder(
    statement: &Statement,
    error: SqlError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), SqlError> {
    // A cancelled statement aborts the batch immediately: key on the cancel
    // marker, not any flag, so an Attention landing concurrently with an
    // unrelated failure cannot suppress that failure's dooming. A cancel is
    // not a SQL error, so `@@ERROR` is untouched.
    if error.number == CANCEL_ERROR {
        return Err(error);
    }
    txn_ctx.record_error(error.number);
    // A durability failure wedged the store (a flush inside the statement,
    // e.g. before a snapshot capture): never continue past a lost commit.
    if run.durability_failed {
        return Err(error);
    }
    // Severity >= 20 is fatal to the connection: it bypasses TRY (the
    // TryCatch arm refuses it too), dooms the transaction, and the protocol
    // layer closes the stream after delivering it.
    if error.level >= FATAL_SEVERITY {
        if txn_ctx.in_txn() {
            txn_ctx.doomed = true;
        }
        return Err(error);
    }
    // The doom decision is made HERE, where the failing statement's kind is
    // known — never re-derived at the TRY boundary, which cannot see it.
    // `SET XACT_ABORT` (or severity >= 17) dooms; RAISERROR is exempt by
    // definition (SQL Server: "errors raised by RAISERROR are not affected
    // by SET XACT_ABORT") and never dooms.
    let dooms = !matches!(statement, Statement::RaiseError(_))
        && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY);
    if txn_ctx.in_txn() && dooms {
        txn_ctx.doomed = true;
    }
    // Inside a TRY, the error then transfers to the matching CATCH (which
    // sees XACT_STATE() = -1 when it doomed). The CATCH runs more statements,
    // so a result set this one already started streaming must be closed.
    if in_try {
        run.abort_open_rowset(txn_ctx.in_txn());
        return Err(error);
    }
    // RAISERROR is statement-scope: the batch always continues.
    if matches!(statement, Statement::RaiseError(_)) {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    // THROW always terminates the batch — even when it does not doom the
    // transaction (XACT_ABORT OFF leaves it open and committable later).
    if matches!(statement, Statement::Throw(_)) {
        return Err(error);
    }
    // Other statements: a non-dooming in-transaction error rolls back only
    // the statement and the batch continues; a dooming one ends the batch
    // (only ROLLBACK is then accepted, error 3930). This must stay keyed on the
    // ERROR (its severity / XACT_ABORT), NOT on whether the transaction is
    // already doomed: a doomed transaction still runs a CATCH's reads and
    // statement-terminating errors (division by zero, conversion) so the CATCH
    // can reach `IF XACT_STATE() <> 0 ROLLBACK` — terminating the batch on those
    // would leave the uncommittable transaction open holding its locks.
    if txn_ctx.in_txn() && !dooms {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    Err(error)
}

/// Enters the versioned-read scopes for an IF/WHILE condition that reads
/// tables — the SAME rules a SELECT gets in `exec_statement_streamed`: under
/// RCSI the condition reads its own statement snapshot; under SNAPSHOT
/// isolation it establishes/uses the transaction snapshot and enforces 3952.
/// Without this the condition read holds NEITHER lock nor snapshot (analysis
/// assumes versioned reads and drops Table S) — a live dirty read, the
/// Stage 13 seam class, caught by the control-flow review.
pub(in crate::engine::relational) fn enter_condition_scopes<'a>(
    storage: &'a Storage,
    condition: &Expr,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<(Option<SnapshotScope<'a>>, Option<TxnSnapshotScope>), SqlError> {
    let mut tables = Vec::new();
    collect_expr_tables(condition, &mut tables);
    // A scalar function the condition calls may read tables through its body;
    // those reads must observe the same snapshot as a direct read (the lock
    // analysis already resolved them), so arm the scope when the condition
    // reaches any table directly OR through a called function.
    if tables.is_empty()
        && expr_function_read_ids(storage, txn_ctx.database_id(), condition).is_empty()
    {
        return Ok((None, None));
    }
    match txn_ctx.isolation() {
        Isolation::ReadCommitted if storage.rcsi_enabled() => {
            // The snapshot is the durable commit prefix: the session's own
            // just-committed statements must be durable before capture.
            run.flush(storage)?;
            Ok((
                Some(SnapshotScope::enter(
                    storage,
                    txn_ctx.txn.as_ref().map(StorageTxn::txn_id),
                )),
                None,
            ))
        }
        Isolation::Snapshot => {
            if !storage.snapshot_isolation_allowed() {
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return Err(snapshot_not_allowed_error(&txn_ctx.database));
            }
            if txn_ctx.in_txn() {
                if txn_ctx.txn_snapshot.is_none() {
                    // First data access establishes the transaction's view —
                    // a condition read counts.
                    run.flush(storage)?;
                    let own = txn_ctx.txn.as_ref().map(StorageTxn::txn_id);
                    txn_ctx.txn_snapshot = Some(storage.capture_read_snapshot(own));
                }
                Ok((None, txn_ctx.txn_snapshot.map(TxnSnapshotScope::enter)))
            } else {
                run.flush(storage)?;
                Ok((Some(SnapshotScope::enter(storage, None)), None))
            }
        }
        // A readable STANDBY snapshots condition reads too (below the
        // RCSI/SNAPSHOT arms — see the statement arming): only the
        // last-applied-commit snapshot yields committed-state reads there.
        _ if storage.is_standby() => {
            run.flush(storage)?;
            Ok((Some(SnapshotScope::enter(storage, None)), None))
        }
        _ => Ok((None, None)),
    }
}

/// Evaluates an IF/WHILE condition: subqueries (EXISTS, scalar, IN) resolve
/// eagerly through the same machinery as WHERE-clause subqueries, then the
/// residual expression evaluates against the session context. T-SQL
/// three-valued: TRUE runs the branch; FALSE and NULL (UNKNOWN) do not.
pub(in crate::engine::relational) fn eval_condition(
    storage: &Storage,
    condition: &Expr,
    txn_ctx: &TxnContext,
) -> Result<bool, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let no_outer = |_: &str| -> Option<usize> { None };
    let resolved = substitute_correlated_in_expr(storage, condition, &no_outer, &[], &eval_ctx)?;
    match eval_constant(&resolved, &eval_ctx)? {
        SqlValue::Bool(taken) => Ok(taken),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is              expected.",
        )),
    }
}
