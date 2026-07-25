use super::*;

pub fn execute_batch(storage: &Storage, sql: &str, txn_ctx: &mut TxnContext) -> BatchOutcome {
    execute_batch_with_params(storage, sql, txn_ctx, &[])
}

/// Like [`execute_batch`], but seeds `params` as batch variables before running
/// the statement text — the `sp_executesql` path. Parameters are injected as
/// already-typed values, never re-rendered into the SQL text, so a parameter
/// value can never alter the statement's structure (no injection surface).
pub fn execute_batch_with_params(
    storage: &Storage,
    sql: &str,
    txn_ctx: &mut TxnContext,
    params: &[RpcParam],
) -> BatchOutcome {
    let mut collector = Collector::default();
    let error = execute_batch_streamed(storage, sql, txn_ctx, params, &mut collector);
    collector.into_outcome(error)
}

/// Like [`execute_batch_with_params`], but each statement's result leaves
/// through `emitter` as the statement produces it instead of accumulating into
/// a [`BatchOutcome`]: a result set opens with its columns, its rows follow (in
/// chunks read straight off the scan for the streamed shape), and each
/// statement's DONE carries the transaction state *after that statement* —
/// which is what TDS `DONE_INXACT` means per statement. The returned error is
/// the batch's terminal error (what `BatchOutcome::error` carried), reported by
/// the caller after the statement events.
///
/// Durability keeps its ordering: nothing the client can rely on — a DONE
/// acknowledging a commit, or rows carrying commit-derived state such as a
/// reserved identity value — is emitted before the commit behind it is
/// fsync-durable. DONEs queue in the run and flush before the next result set
/// opens and at the end of the batch; both points fsync first when any
/// statement since the last one may have committed (the same kind-based test
/// as before), so a batch of writes with nothing to stream between them still
/// costs one fsync.
pub fn execute_batch_streamed(
    storage: &Storage,
    sql: &str,
    txn_ctx: &mut TxnContext,
    params: &[RpcParam],
    emitter: &mut dyn BatchEmitter,
) -> Option<SqlError> {
    // A transaction reaped for idleness is reported to the session's next batch
    // (once). 1205 is the code this engine already uses for a server-initiated
    // transaction abort (the parked deadlock victim), and every driver treats it
    // as "the transaction is gone, retry it" — which is exactly the right
    // recovery here.
    if txn_ctx.take_reaped() {
        return Some(SqlError::new(
            1205,
            13,
            51,
            "The transaction was rolled back because the session was idle for too long. Rerun the transaction.",
        ));
    }
    // Variables are batch-scoped: each batch starts with none.
    txn_ctx.clear_variables();
    // Refresh the session's effective role set from the membership cache, so a
    // security DDL committed since the last batch is reflected in this one's
    // IS_ROLEMEMBER/IS_SRVROLEMEMBER (SQL Server's per-batch permission caching).
    txn_ctx.refresh_session_roles(storage);
    for param in params {
        // The lexer keys `@p1` as `p1` (leading `@` stripped, lowercased); the
        // RPC name arrives as `@p1`, so normalise it the same way to match.
        let key = param.name.trim_start_matches('@').to_ascii_lowercase();
        let value = value::datum_to_sql(&param.value, &param.column_type);
        txn_ctx.variables.insert(key, (param.column_type, value));
    }
    let statements = match truthdb_sql::parse(sql) {
        Ok(statements) => statements,
        Err(error) => return Some(error),
    };
    let mut run = BatchRun {
        emitter,
        deferred: Vec::new(),
        rowset_open: false,
        durability_failed: false,
        committed: false,
        last_error: None,
        function_return_type: None,
    };
    // `run_block` returns Err only when the batch must terminate (a cancel, or a
    // dooming/uncaught error outside any TRY); a non-dooming error under
    // `XACT_ABORT OFF` is recorded in `run.last_error` and the batch continues.
    let terminating = run_block(storage, &statements, txn_ctx, &mut run, false)
        .and_then(end_of_scope)
        .err();
    // The batch-end durability point, and the DONEs it was holding back. A
    // durability failure outranks any statement error: a lost commit is more
    // severe than an error the client asked about, and a benign continued error
    // must not mask one.
    let durability = run.finish(storage);
    durability.or(terminating).or(run.last_error)
}

/// Receives a batch's results as the executor produces them. The session layer
/// forwards each call as a `BatchEvent` onto the reply channel; buffered
/// callers (the native command path, the SLT runner, tests) use [`Collector`]
/// to reassemble a [`BatchOutcome`].
pub(super) struct BatchRun<'a> {
    pub(super) emitter: &'a mut dyn BatchEmitter,
    /// Finished statements' DONEs, held back until the next durability point
    /// (the next result set opening, or the end of the batch) so a DONE that
    /// acknowledges a commit is never emitted before that commit is durable.
    pub(super) deferred: Vec<DeferredDone>,
    /// A result set's columns have been emitted but its statement has not
    /// finished — the state [`BatchRun::abort_open_rowset`] closes on failure.
    pub(super) rowset_open: bool,
    /// A durability (fsync) failure wedged the store. The error terminates the
    /// batch and is never catchable: the old batch-end fsync ran past every
    /// TRY, and a CATCH must not be able to swallow a lost commit.
    pub(super) durability_failed: bool,
    /// Whether any executed statement may have made a durable commit: group
    /// commit defers the WAL fsync, so the end of the batch fsyncs once if so.
    pub(super) committed: bool,
    /// The last non-dooming statement error under `SET XACT_ABORT OFF` (outside
    /// any TRY) — the batch continues past it (SQL Server default) and it is
    /// reported alongside the results rather than terminating the batch.
    pub(super) last_error: Option<SqlError>,
    /// Set while running a scalar function body: the declared return type. The
    /// `RETURN <expr>` arm then evaluates its value, coerces it to this type,
    /// and stashes it in `TxnContext::func_return` (rather than the procedure
    /// int-status path).
    pub(super) function_return_type: Option<ColumnType>,
}

/// A statement's DONE, parked until the next durability point.
pub(super) fn done_command(statement: &Statement) -> DoneCommand {
    match statement {
        Statement::Select(_) => DoneCommand::Select,
        Statement::Insert(_) => DoneCommand::Insert,
        Statement::Update(_) => DoneCommand::Update,
        Statement::Delete(_) => DoneCommand::Delete,
        _ => DoneCommand::Other,
    }
}

impl BatchRun<'_> {
    /// Opens a result set. [`run_block`] flushed the deferred DONEs before any
    /// statement that can produce one, so statement order on the stream holds.
    pub(super) fn open_rowset(&mut self, columns: Vec<ResultColumn>) {
        debug_assert!(
            self.deferred.is_empty(),
            "a result set opened over deferred DONEs"
        );
        self.rowset_open = true;
        self.emitter.columns(columns);
    }

    /// Emits a chunk of rows for the open result set.
    pub(super) fn rows(&mut self, rows: Vec<Vec<Datum>>) {
        if !rows.is_empty() {
            self.emitter.rows(rows);
        }
    }

    /// Forwards a database-context change (`USE`) to the emitter, ahead of
    /// the statement's (deferred) DONE.
    pub(super) fn database_context(&mut self, database: &str) {
        self.emitter.database_context(database);
    }

    /// Emits an informational message (RAISERROR severity <= 10). `run_block`
    /// flushed the deferred DONEs before the statement, so stream order holds.
    pub(super) fn info(&mut self, error: SqlError) {
        debug_assert!(
            self.deferred.is_empty(),
            "an INFO message over deferred DONEs"
        );
        self.emitter.info(&error);
    }

    /// Ends a statement. Its DONE is deferred to the next durability point.
    pub(super) fn done(&mut self, count: Option<u64>, in_transaction: bool, command: DoneCommand) {
        self.rowset_open = false;
        self.deferred.push(DeferredDone {
            count,
            in_transaction,
            command,
        });
    }

    /// Closes the open result set of a statement that failed after its columns
    /// (and possibly rows) were already emitted, so the stream stays framed for
    /// the statements that follow (a caught or continued error). No-op when
    /// nothing is open — a statement that failed before emitting anything
    /// leaves no trace, as before.
    pub(super) fn abort_open_rowset(&mut self, in_transaction: bool) {
        if self.rowset_open {
            self.rowset_open = false;
            self.emitter.statement_aborted(in_transaction);
        }
    }

    /// Emits the deferred DONEs, fsyncing first when any statement since the
    /// last durability point may have committed. The gate is the same
    /// kind-based `committed` flag the batch-end fsync uses — not "does some
    /// DONE acknowledge a commit" — because commit-derived state escapes
    /// through the *rows* of whatever result set opens next, not only through
    /// DONEs: an identity value reserved by an in-transaction INSERT (a
    /// mini-commit) is readable one statement later via `SELECT
    /// SCOPE_IDENTITY()`, and a value the client has seen must never be
    /// reissued after a crash. On a durability failure the DONEs the batch
    /// can no longer stand behind are dropped and the batch terminates (see
    /// [`Self::make_durable`]).
    pub(super) fn flush(&mut self, storage: &Storage) -> Result<(), SqlError> {
        if self.committed {
            self.committed = false;
            if let Some(error) = self.make_durable(storage) {
                return Err(error);
            }
        }
        for done in self.deferred.drain(..) {
            self.emitter
                .statement_done(done.count, done.in_transaction, done.command);
        }
        Ok(())
    }

    /// The end of the batch: one fsync if any statement may have committed
    /// since the last durability point — by kind, not transaction state, so a
    /// hidden mini-commit (an identity reservation, even inside an open
    /// transaction or under a statement that then failed) is never missed —
    /// then the remaining DONEs. Returns the durability error, if any.
    pub(super) fn finish(&mut self, storage: &Storage) -> Option<SqlError> {
        if self.committed {
            self.committed = false;
            if let Some(error) = self.make_durable(storage) {
                return Some(error);
            }
        }
        for done in self.deferred.drain(..) {
            self.emitter
                .statement_done(done.count, done.in_transaction, done.command);
        }
        None
    }

    /// Blocks until the batch's commit records are fsync-durable (group
    /// commit). A durability failure wedges the store — the in-memory state is
    /// now ahead of the log, so no further op may serve it — and drops the
    /// deferred DONEs, which would otherwise acknowledge commits a restart is
    /// about to undo.
    pub(super) fn make_durable(&mut self, storage: &Storage) -> Option<SqlError> {
        match storage.ensure_durable(storage.wal_tail()) {
            Ok(()) => None,
            Err(err) => {
                storage.wedge();
                self.deferred.clear();
                self.durability_failed = true;
                Some(map_storage_err(err, ""))
            }
        }
    }
}
