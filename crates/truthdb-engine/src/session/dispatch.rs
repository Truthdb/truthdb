use super::*;

/// Resolves a prepared-statement RPC. `Prepare`/`Unprepare` touch only the
/// session's handle table and answer immediately; `Execute`/`PrepExec`
/// re-enter [`dispatch_batch`] with the resolved statement text, so locks,
/// parking and streaming behave exactly as for a plain batch. There is no
/// cached plan: execution re-parses and re-binds against the live catalog
/// (like `sp_executesql`), so DDL between prepare and execute needs no
/// invalidation — the next execute simply sees the new schema.
pub(super) fn dispatch_rpc(
    shared: &Arc<Shared>,
    session: SessionId,
    command: PreparedRpc,
    cancel: Arc<AtomicBool>,
    mut reply: BatchSink,
) {
    // The immediate replies (no batch runs) still stamp DONE_INXACT from the
    // session's real transaction state.
    let immediate = |reply: &BatchSink, error: Option<SqlError>| {
        let in_transaction = {
            let sched = shared.scheduler.lock().expect("scheduler poisoned");
            sched.sessions.in_transaction(session)
        };
        if let Some(error) = error {
            reply.send(BatchEvent::Error(error));
        }
        reply.send(BatchEvent::Complete { in_transaction });
    };
    let missing_handle = |handle: i32| {
        SqlError::new(
            8179,
            16,
            1,
            format!("Could not find prepared statement with handle {handle}."),
        )
    };
    match command {
        PreparedRpc::Prepare { decls, stmt } => {
            // Parse now so a syntax error surfaces at prepare time, as SQL
            // Server's compile does. Binding stays at execute (names resolve
            // against the live catalog there) — a divergence: an unknown
            // table or column errors at execute, not prepare.
            if let Err(error) = truthdb_sql::parse(&stmt) {
                immediate(&reply, Some(error));
                return;
            }
            let handle = {
                let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.prepare(session, decls, stmt)
            };
            reply.send(BatchEvent::PreparedHandle(handle));
            immediate(&reply, None);
        }
        PreparedRpc::Unprepare { handle } => {
            let dropped = {
                let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.unprepare(session, handle)
            };
            immediate(&reply, (!dropped).then(|| missing_handle(handle)));
        }
        PreparedRpc::Describe { tsql } => match shared.engine.describe_first_result_set(&tsql) {
            Ok(rowset) => {
                let count = rowset.rows.len() as u64;
                let in_transaction = {
                    let sched = shared.scheduler.lock().expect("scheduler poisoned");
                    sched.sessions.in_transaction(session)
                };
                reply.send(BatchEvent::Columns(rowset.columns));
                reply.send(BatchEvent::Rows(rowset.rows));
                reply.send(BatchEvent::StatementDone {
                    count: Some(count),
                    in_transaction,
                    command: crate::engine::DoneCommand::Select,
                });
                reply.send(BatchEvent::Complete { in_transaction });
            }
            Err(error) => immediate(&reply, Some(error)),
        },
        PreparedRpc::Execute { handle, values } => {
            let resolved = {
                let sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.prepared(session, handle)
            };
            let Some((decls, text)) = resolved else {
                immediate(&reply, Some(missing_handle(handle)));
                return;
            };
            let values = match bind_decl_names(&decls, values) {
                Ok(values) => values,
                Err(error) => {
                    immediate(&reply, Some(error));
                    return;
                }
            };
            dispatch_batch(shared, session, text, values, None, cancel, reply);
        }
        PreparedRpc::PrepExec {
            decls,
            stmt,
            values,
        } => {
            if let Err(error) = truthdb_sql::parse(&stmt) {
                immediate(&reply, Some(error));
                return;
            }
            let handle = {
                let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
                sched.sessions.prepare(session, decls.clone(), stmt.clone())
            };
            reply.prepared_handle = Some(handle);
            let values = match bind_decl_names(&decls, values) {
                Ok(values) => values,
                Err(error) => {
                    immediate(&reply, Some(error));
                    return;
                }
            };
            dispatch_batch(shared, session, stmt, values, None, cancel, reply);
        }
    }
}

/// Names any unnamed value parameters from the declaration list, in order —
/// `sp_execute` values arrive unnamed on the wire, and seeding a batch
/// variable needs its name. A value that already has a name keeps it.
pub(super) fn bind_decl_names(
    decls: &str,
    mut values: Vec<crate::engine::RpcParam>,
) -> Result<Vec<crate::engine::RpcParam>, SqlError> {
    let names = crate::engine::decl_names(decls);
    // An unnamed value with no declaration to name it is SQL Server's 8144.
    // (Fewer values than declarations is legal — a declared parameter the
    // statement never reads goes unmissed, and one it does read errors when
    // the variable lookup fails at execution. Extra NAMED values pass
    // through: they seed variables by their own names, which keeps the
    // `run_rpc` wrappers' seed-named-params contract intact.)
    if values
        .iter()
        .skip(names.len())
        .any(|value| value.name.is_empty())
    {
        return Err(SqlError::new(
            8144,
            16,
            2,
            "Procedure or function has too many arguments specified.",
        ));
    }
    for (value, name) in values.iter_mut().zip(names) {
        if value.name.is_empty() {
            value.name = name;
        }
    }
    Ok(values)
}

/// Acquires a batch's locks and runs it, or parks it behind a conflict. Either
/// way, drains anything the batch's completion (or a deadlock abort) unblocked.
pub(super) fn dispatch_batch(
    shared: &Arc<Shared>,
    session: SessionId,
    sql: String,
    params: Vec<crate::engine::RpcParam>,
    proc_tail: Option<ProcRpcTail>,
    cancel: Arc<AtomicBool>,
    reply: BatchSink,
) {
    let runnable = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        let isolation = sched.isolation(session);
        // Parameters are values, not statements, so they never change which
        // locks the batch needs — analyse the statement text as usual.
        let epoch = shared.engine.lock_analysis_epoch();
        let needs = shared
            .engine
            .analyze_locks(sched.session_db(session), &sql, isolation);
        if sched.try_acquire(session.raw(), &needs, true) {
            sched.sessions.touch(session);
            let txn_ctx = sched.take_ctx(session);
            Some(Runnable {
                session,
                sql,
                params,
                proc_tail,
                cancel,
                reply,
                txn_ctx,
            })
        } else {
            let deadline = Instant::now() + sched.lock_wait_timeout;
            sched.parked.push_back(Parked {
                session,
                sql,
                params,
                proc_tail,
                cancel,
                reply,
                needs,
                deadline,
                epoch,
            });
            // The new waiter may have closed a lock-wait cycle; break it now
            // rather than waiting for the deadline backstop.
            sched.detect_deadlock(&shared.engine);
            None
        }
    };
    if let Some(work) = runnable {
        run_and_finish(shared, work);
    }
    drain_ready(shared);
}

/// Runs a batch whose locks are already held (execution holds no scheduler
/// lock, so batches run concurrently), then re-locks the scheduler to return
/// the session's transaction context and release the locks that do not outlive
/// the batch.
pub(super) fn run_and_finish(shared: &Arc<Shared>, work: Runnable) {
    let Runnable {
        session,
        sql,
        params,
        proc_tail,
        cancel,
        reply,
        mut txn_ctx,
    } = work;
    // Bind the cancel flag to this worker thread for the batch, so the executor's
    // `check_cancelled` polls see a TDS Attention; the guard clears it on return.
    let _cancel_guard = crate::engine::CancelScope::enter(cancel);
    // Statement events stream out *while the batch runs* — the executor emits
    // each result as it is produced, and the send never blocks, so a client
    // that reads slowly delays neither this worker nor the locks it holds.
    let mut reply = reply;
    let outcome = shared
        .engine
        .sql_batch_streamed(&sql, &mut txn_ctx, &params, &mut reply);
    // RPC-by-name tail: a procedure copies its OUTPUT parameters back and stores
    // its RETURN status only when it *completed* — which is not the same as the
    // *batch* running clean. A procedure that raises a continued (non-dooming)
    // error and then returns completes, yet the batch surfaces that error
    // (`Ok(Some(err))`); SQL Server still sends the tail there. So read the tail
    // on any `Ok` outcome and let `read_proc_tail` decide from the completion
    // signal (the status variable is non-NULL only if the procedure completed).
    // Read it now, before `finish` hands the context back to the scheduler.
    let tail_events = match (&proc_tail, &outcome) {
        (Some(tail), Ok(_)) => Some(read_proc_tail(&txn_ctx, tail)),
        _ => None,
    };
    let in_transaction = {
        let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
        sched.finish(&shared.engine, session, txn_ctx)
    };
    // The terminal events wait for the locks to settle: `Complete` carries the
    // post-batch transaction state, which only `finish` knows.
    match outcome {
        Ok(error) => {
            if let Some(events) = tail_events {
                for event in events {
                    if !reply.send(event) {
                        return;
                    }
                }
            }
            reply.send_tail(error, in_transaction);
        }
        Err(err) => {
            reply.send(BatchEvent::Failed(err));
        }
    }
}

/// Reads an RPC-by-name call's response tail off the finished context: the
/// RETURN status, then each OUTPUT parameter as a typed RETURNVALUE.
///
/// The status variable is the completion signal. It was seeded NULL, and
/// `run_user_procedure` overwrites it with an Int (and copies OUTPUT parameters
/// back) only when the body completes — both under the one `result.is_ok()`
/// gate. So a NULL status here means the procedure aborted: emit nothing, which
/// is what SQL Server does for a failed procedure. A non-NULL Int status means
/// it completed (possibly after a continued, non-dooming error), and the OUTPUT
/// copy-back happened too, so the whole tail is emitted. This single observable
/// keeps the emission decision in agreement with the copy-back decision.
pub(super) fn read_proc_tail(txn_ctx: &TxnContext, tail: &ProcRpcTail) -> Vec<BatchEvent> {
    let status = match txn_ctx.variable_datum(&tail.status_var) {
        Some((_, Datum::Int(status))) => status,
        _ => return Vec::new(),
    };
    let mut events = Vec::with_capacity(tail.output_vars.len() + 1);
    events.push(BatchEvent::ReturnStatus(status));
    for (read_var, wire_name, ordinal) in &tail.output_vars {
        if let Some((column_type, value)) = txn_ctx.variable_datum(read_var) {
            events.push(BatchEvent::ReturnValue {
                ordinal: *ordinal,
                name: wire_name.clone(),
                column_type,
                value,
            });
        }
    }
    events
}

/// Runs every parked batch whose locks are now grantable, in FIFO order, until
/// none remain. Each finished batch may release locks that unblock the next, so
/// this re-checks after every one.
pub(super) fn drain_ready(shared: &Arc<Shared>) {
    loop {
        let work = {
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            sched.next_grantable(&shared.engine)
        };
        match work {
            Some(work) => run_and_finish(shared, work),
            None => break,
        }
    }
}
