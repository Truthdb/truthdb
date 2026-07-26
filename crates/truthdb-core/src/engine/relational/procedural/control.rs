use super::super::prelude::*;

/// How a statement block ended: normally, or via a control-flow statement
/// that must propagate to the construct that absorbs it (`WHILE` for
/// Break/Continue, the batch — later the procedure — for Return, the nearest
/// block holding the target label for `Goto`). TRY/CATCH and plain blocks pass
/// every non-Normal flow straight through (a `Goto` is first checked against the
/// current block's labels, then propagated).
#[derive(Clone, PartialEq, Eq)]
pub(in crate::engine::relational) enum Flow {
    Normal,
    Break,
    Continue,
    Return,
    /// A `GOTO <label>` still looking for its target label.
    Goto(String),
}

/// What `run_block`'s loop should do with a flow bubbling up from a nested
/// construct: a `GOTO` to a label in this block jumps there; anything else
/// propagates to the enclosing block.
pub(in crate::engine::relational) enum GotoAction {
    /// Resume at this statement index (a resolved `GOTO`).
    Jump(usize),
    /// The nested construct ended normally — fall through.
    Fall,
    /// Return this flow to the caller (Break/Continue/Return, or a `GOTO` to a
    /// label not defined in this block).
    Propagate(Flow),
}

pub(in crate::engine::relational) fn resolve_goto(
    flow: Flow,
    labels: &std::collections::HashMap<String, usize>,
) -> GotoAction {
    match flow {
        Flow::Normal => GotoAction::Fall,
        Flow::Goto(label) => match labels.get(&label.to_ascii_lowercase()) {
            Some(&target) => GotoAction::Jump(target),
            None => GotoAction::Propagate(Flow::Goto(label)),
        },
        other => GotoAction::Propagate(other),
    }
}

/// A statement list run as its own scope — a batch, or a procedure / function /
/// trigger body — cannot be a GOTO target from outside and a GOTO cannot jump
/// out of it. A GOTO that reaches the end of such a scope unresolved references
/// a label defined nowhere in scope: error 133.
pub(in crate::engine::relational) fn end_of_scope(flow: Flow) -> Result<(), SqlError> {
    match flow {
        Flow::Goto(label) => Err(SqlError::new(
            133,
            15,
            1,
            format!("A GOTO statement references the label '{label}:' which has not been defined."),
        )),
        _ => Ok(()),
    }
}

pub(in crate::engine::relational) fn run_block(
    storage: &Storage,
    statements: &[Statement],
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<Flow, SqlError> {
    // Label positions for GOTO. A jump sets the index to the label's position;
    // execution resumes there (the label statement itself is a no-op). A label
    // repeated in the same list is error 132.
    let mut labels: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (idx, s) in statements.iter().enumerate() {
        if let Statement::Label { name, .. } = s
            && labels.insert(name.to_ascii_lowercase(), idx).is_some()
        {
            return Err(SqlError::new(
                132,
                15,
                1,
                format!(
                    "The label '{name}:' has already been declared. Label names must be unique \
                     within a query batch or stored procedure."
                ),
            ));
        }
    }
    let mut i = 0;
    'stmts: while i < statements.len() {
        let statement = &statements[i];
        i += 1;
        // A TDS Attention (cancel) aborts the batch before the next statement.
        // It is never catchable — it propagates straight out, past any TRY.
        check_cancelled()?;
        if let Statement::Exec(exec) = statement {
            // The inner statements flow through `run_block` recursion, whose
            // own loop applies the per-statement flush and commit flag — the
            // same shape as TRY/CATCH dispatch. Errors take the ordinary
            // statement path: cancels and durability failures propagate, a
            // TRY transfers to CATCH, XACT_ABORT OFF continues the batch.
            match run_exec(storage, exec, txn_ctx, run, in_try) {
                Ok(()) => {}
                Err(exec_error) => {
                    // A failed EXEC sets @@ROWCOUNT to 0 like any failed
                    // statement.
                    txn_ctx.rowcount = 0;
                    let (error, from_inner) = match exec_error {
                        ExecError::Own(error) => (error, false),
                        ExecError::Inner(error) => (error, true),
                    };
                    if error.number == CANCEL_ERROR {
                        return Err(error);
                    }
                    // Inner errors were recorded at their raise site (the
                    // inner ladder), where the procedure frame was still
                    // live; re-recording here would blank ERROR_PROCEDURE().
                    if !from_inner {
                        txn_ctx.record_error(error.number);
                    }
                    if run.durability_failed {
                        return Err(error);
                    }
                    // Transfer to CATCH: decisions (dooming included) were
                    // already made where the error arose — per-statement in
                    // the inner `run_block`, or `doom_per_rule` for
                    // `run_exec`'s own errors. A fatal (>= 20) error is
                    // refused by the TryCatch arm's own filter.
                    if in_try {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        return Err(error);
                    }
                    // An error crossing OUT of the inner batch already
                    // terminated it — and batch-abort scope is the whole
                    // nest, so the outer batch ends too (a THROW inside
                    // EXEC'd text ends the calling batch even when nothing
                    // doomed; non-dooming ordinary errors never cross — the
                    // inner run_block continued past them). Nothing is
                    // re-derived from severity here: the review showed that
                    // second derivation dropped THROW's termination.
                    if from_inner {
                        return Err(error);
                    }
                    // run_exec's OWN failure (unknown proc, 214, 8144, parse,
                    // depth): statement-scope at the EXEC site. Dooming was
                    // applied at the source; this decides only continuation.
                    let terminates = txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY;
                    if txn_ctx.in_txn() && !terminates {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        run.last_error = Some(error);
                        continue;
                    }
                    return Err(error);
                }
            }
            continue;
        }
        match statement {
            Statement::Block { body, .. } => {
                match resolve_goto(run_block(storage, body, txn_ctx, run, in_try)?, &labels) {
                    GotoAction::Jump(t) => {
                        i = t;
                        continue 'stmts;
                    }
                    GotoAction::Propagate(flow) => return Ok(flow),
                    GotoAction::Fall => {}
                }
                continue;
            }
            Statement::If {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                // A successful condition evaluation resets `@@ERROR` (the IF
                // itself is a statement) — AFTER the condition read it, which
                // is what makes `IF @@ERROR <> 0` work.
                // A condition subquery reads table variables through the same
                // FROM path as a SELECT, so it needs the same read view armed —
                // the IF/WHILE arms bypass exec_statement_streamed, so arm here.
                let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                    .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                {
                    Ok(taken) => taken,
                    Err(error) => {
                        txn_ctx.rowcount = 0;
                        statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                        continue;
                    }
                };
                txn_ctx.last_error = 0;
                let branch = if taken {
                    Some(then_branch)
                } else {
                    else_branch.as_ref()
                };
                if let Some(branch) = branch {
                    let flow =
                        run_block(storage, std::slice::from_ref(branch), txn_ctx, run, in_try)?;
                    match resolve_goto(flow, &labels) {
                        GotoAction::Jump(t) => {
                            i = t;
                            continue 'stmts;
                        }
                        GotoAction::Propagate(flow) => return Ok(flow),
                        GotoAction::Fall => {}
                    }
                }
                continue;
            }
            Statement::While {
                condition, body, ..
            } => {
                loop {
                    // A TDS Attention lands between iterations too — an
                    // infinite `WHILE 1 = 1` must die on cancel even when its
                    // body runs no cancellable statement.
                    check_cancelled()?;
                    // Re-armed each iteration: the body may INSERT into @t, and
                    // the next condition read must see the updated rows.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                        .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                    {
                        Ok(taken) => taken,
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            break;
                        }
                    };
                    txn_ctx.last_error = 0;
                    if !taken {
                        break;
                    }
                    let flow =
                        run_block(storage, std::slice::from_ref(body), txn_ctx, run, in_try)?;
                    match flow {
                        Flow::Normal | Flow::Continue => {}
                        Flow::Break => break,
                        // RETURN or a GOTO leaves the loop: a GOTO to a label in
                        // this block jumps out of the WHILE to it, else propagate.
                        other => match resolve_goto(other, &labels) {
                            GotoAction::Jump(t) => {
                                i = t;
                                continue 'stmts;
                            }
                            GotoAction::Propagate(flow) => return Ok(flow),
                            GotoAction::Fall => {}
                        },
                    }
                }
                continue;
            }
            // The parser rejects BREAK/CONTINUE outside a WHILE (135/136), so
            // these only ever propagate up to an enclosing loop.
            Statement::Break { .. } => return Ok(Flow::Break),
            Statement::Continue { .. } => return Ok(Flow::Continue),
            // The parser rejects `RETURN <value>` outside a procedure (178);
            // inside one the status is stashed for `EXEC @rc =` to read.
            Statement::Return { value, .. } => {
                // A scalar function body's RETURN: evaluate its (mandatory)
                // value, coerce it to the declared return type, and stash it for
                // the caller. Nested user functions and subqueries in the RETURN
                // expression are rewritten to literals first, exactly like an
                // IF/WHILE condition.
                if let Some(return_type) = run.function_return_type {
                    let value = value
                        .as_ref()
                        .expect("a scalar function RETURN carries a value (parser-enforced)");
                    // A RETURN subquery reads table variables through the FROM
                    // path; arm the body's own (empty) view so it shadows the
                    // caller's rather than reading caller locals.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let eval_ctx = txn_ctx.eval_context();
                    let no_outer = |_: &str| -> Option<usize> { None };
                    let coerced =
                        substitute_correlated_in_expr(storage, value, &no_outer, &[], &eval_ctx)
                            .and_then(|bound| eval_constant(&bound, &eval_ctx))
                            .and_then(|raw| {
                                let datum =
                                    value::sql_to_datum(&raw, &return_type, "return value")?;
                                Ok(value::datum_to_sql(&datum, &return_type))
                            });
                    match coerced {
                        Ok(coerced) => {
                            txn_ctx.func_return = Some(coerced);
                            return Ok(Flow::Return);
                        }
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                if let Some(value) = value {
                    let eval_ctx = txn_ctx.eval_context();
                    match eval_constant(value, &eval_ctx) {
                        Ok(SqlValue::Int(status))
                            if (i32::MIN as i64..=i32::MAX as i64).contains(&status) =>
                        {
                            txn_ctx.proc_return = Some(status)
                        }
                        // A RETURN value outside int range overflows, as SQL
                        // Server does (8115) — the status is an int. Without this
                        // the out-of-range value would be stashed and later fail
                        // to encode (and, on the RPC path, read back as NULL and
                        // be mistaken for a procedure that never completed).
                        Ok(SqlValue::Int(_)) => {
                            let error = SqlError::new(
                                8115,
                                16,
                                2,
                                "Arithmetic overflow error converting expression to data type int.",
                            );
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                        Ok(SqlValue::Null) => {
                            // SQL Server warns and returns 0; we return 0.
                            txn_ctx.proc_return = Some(0);
                        }
                        Ok(_) | Err(_) => {
                            let error =
                                eval_constant(value, &eval_ctx).err().unwrap_or_else(|| {
                                    SqlError::new(
                                        257,
                                        16,
                                        3,
                                        "The RETURN status must be an integer.",
                                    )
                                });
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                return Ok(Flow::Return);
            }
            // A label is a no-op when reached in sequence.
            Statement::Label { .. } => continue,
            // GOTO jumps to a label in this block, or propagates to an enclosing
            // one (the batch top turns an unresolved GOTO into error 133).
            Statement::Goto { label, .. } => match labels.get(&label.to_ascii_lowercase()) {
                Some(&target) => {
                    i = target;
                    continue 'stmts;
                }
                None => return Ok(Flow::Goto(label.clone())),
            },
            _ => {}
        }
        if let Statement::TryCatch {
            try_block,
            catch_block,
            ..
        } = statement
        {
            match run_block(storage, try_block, txn_ctx, run, true) {
                Ok(Flow::Normal) => {}
                // BREAK/CONTINUE/RETURN/GOTO cross a TRY without running its
                // CATCH; a GOTO to a label in this block jumps there.
                Ok(flow) => match resolve_goto(flow, &labels) {
                    GotoAction::Jump(t) => {
                        i = t;
                        continue 'stmts;
                    }
                    GotoAction::Propagate(flow) => return Ok(flow),
                    GotoAction::Fall => {}
                },
                // An Attention that landed inside the TRY block is not caught.
                Err(cancel) if cancel.number == CANCEL_ERROR => return Err(cancel),
                // A durability failure wedged the store: no CATCH swallows a
                // lost commit (the old batch-end fsync ran past every TRY).
                Err(error) if run.durability_failed => return Err(error),
                // Severity >= 20 is fatal to the connection: no CATCH sees
                // it. Already recorded (and doomed) at the raise site.
                Err(error) if error.level >= FATAL_SEVERITY => return Err(error),
                Err(error) => {
                    // The failed statement's own writes were already undone to
                    // its savepoint (`rel_statement_scoped`), and the doom
                    // decision was made where the statement failed — the inner
                    // `run_block` knows the statement's kind (RAISERROR is
                    // exempt from XACT_ABORT), this boundary does not. Control
                    // transfers to CATCH either way; a doomed transaction
                    // reports XACT_STATE() = -1 there.
                    txn_ctx.push_error(&error);
                    // The CATCH block runs in the *enclosing* try-context: its
                    // own errors are not caught here, so they propagate to an
                    // outer CATCH (or end the batch) per `in_try`.
                    let caught = run_block(storage, catch_block, txn_ctx, run, in_try);
                    txn_ctx.pop_error();
                    match resolve_goto(caught?, &labels) {
                        GotoAction::Jump(t) => {
                            i = t;
                            continue 'stmts;
                        }
                        GotoAction::Propagate(flow) => return Ok(flow),
                        GotoAction::Fall => {}
                    }
                }
            }
            continue;
        }
        // A statement that can open a result set is a durability point: the
        // deferred DONEs must reach the stream before its columns do, and any
        // commit made so far must be fsync-durable before rows that can carry
        // its state (an identity value, via SCOPE_IDENTITY()) leave the server.
        if produces_rowset(statement) || matches!(statement, Statement::RaiseError(_)) {
            run.flush(storage)?;
        }
        // Flag durability by statement kind, before matching the result: a
        // write/DDL/COMMIT can commit even when it then errors — an autocommit
        // statement, an identity reservation (its own mini-commit, made even
        // inside an open transaction and even if the row insert later fails),
        // or the outermost COMMIT.
        run.committed |= statement_may_commit(statement);
        match exec_statement_streamed(storage, statement, txn_ctx, run) {
            Ok(outcome) => {
                // The statement succeeded: `@@ERROR` reads 0 — except after a
                // severity <= 10 RAISERROR, which set it itself (0, or 50000
                // under SETERROR).
                if !matches!(statement, Statement::RaiseError(_)) {
                    txn_ctx.last_error = 0;
                }
                let in_transaction = txn_ctx.in_txn();
                let command = done_command(statement);
                // `SET NOCOUNT ON` suppresses the DONE's count on the wire;
                // rows/results are untouched. `@@ROWCOUNT` records the true
                // count either way (NOCOUNT does not change it).
                let nocount = txn_ctx.nocount;
                let wire_count =
                    |count: u64| -> Option<u64> { if nocount { None } else { Some(count) } };
                // `USE` succeeded: earlier statements' deferred DONEs go out
                // first, then the database-context ENVCHANGE + 5701 INFO the
                // client (SSMS) expects, then the USE's own DONE below —
                // SQL Server's exact order.
                if let Statement::Use { .. } = statement {
                    run.flush(storage)?;
                    run.database_context(&txn_ctx.database);
                }
                match outcome {
                    StatementOutcome::Streamed { rows } => {
                        txn_ctx.rowcount = rows as i64;
                        run.done(wire_count(rows), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Rows(rowset)) => {
                        let count = rowset.rows.len() as u64;
                        txn_ctx.rowcount = count as i64;
                        run.open_rowset(rowset.columns);
                        run.rows(rowset.rows);
                        run.done(wire_count(count), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::RowsAffected(n)) => {
                        txn_ctx.rowcount = n as i64;
                        run.done(wire_count(n), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Done) => {
                        // A simple variable assignment (`SET @x = ...`) sets
                        // @@ROWCOUNT to 1 — recorded by exec_set, preserved
                        // here; every other Done statement resets it to 0.
                        if !matches!(
                            statement,
                            Statement::Set(SetStatement::Variable { .. }) | Statement::Declare(_)
                        ) {
                            txn_ctx.rowcount = 0;
                        }
                        run.done(None, in_transaction, command);
                    }
                }
            }
            Err(error) => {
                // A failed statement sets @@ROWCOUNT to 0, as SQL Server does.
                txn_ctx.rowcount = 0;
                statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
            }
        }
    }
    Ok(Flow::Normal)
}
