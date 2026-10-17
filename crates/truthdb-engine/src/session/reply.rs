use super::*;

/// The reply channel for one batch.
///
/// **Unbounded, deliberately.** A bounded queue would make the worker block
/// once the connection fell behind — and a worker blocks *inside* the batch,
/// which is to say while it still holds the batch's table locks (`finish` runs
/// after the batch returns). A client reading slowly would then hold `Table(t)`
/// S for as long as it liked, and `LOCK_WAIT_TIMEOUT` is 5 s, so every other
/// session touching that table would be reaped with a 1205 naming a deadlock
/// that never happened. Nothing in this module can abort a *running* batch —
/// victims come only from the parked queue — so the engine could not even
/// respond. A hard cap on a reply's memory needs a reader that holds no S
/// locks (Stage 13's RCSI); until then, not blocking the worker is worth more
/// than the cap.
///
/// What it costs: a connection that never drains lets its reply accumulate. The
/// ceiling is the whole result — which is exactly what the non-streaming path
/// held *unconditionally*, for every client — so this is never worse, and for
/// any client that keeps up it is bounded by what is in flight.
pub struct BatchSink {
    pub(super) tx: mpsc::UnboundedSender<BatchEvent>,
    /// A handle `sp_prepexec` allocated, reported as a `PreparedHandle` event
    /// just before `Complete` — return values follow every result set.
    pub(super) prepared_handle: Option<i32>,
}

impl BatchSink {
    pub(super) fn new(tx: mpsc::UnboundedSender<BatchEvent>) -> BatchSink {
        BatchSink {
            tx,
            prepared_handle: None,
        }
    }

    /// Sends one event. Never blocks: `UnboundedSender::send` is a plain
    /// function, which is the point (see the type's docs). `false` once the
    /// receiver is gone — the client disconnected, or its connection task was
    /// dropped — which is the producer's signal to stop.
    pub(super) fn send(&self, event: BatchEvent) -> bool {
        if matches!(event, BatchEvent::Complete { .. })
            && let Some(handle) = self.prepared_handle
        {
            let _ = self.tx.send(BatchEvent::PreparedHandle(handle));
        }
        self.tx.send(event).is_ok()
    }

    /// Sends a finished outcome as events — the reply of a batch that never
    /// ran (the parked deadlock victim) and the tests' shorthand. A batch that
    /// runs streams through the [`crate::engine::BatchEmitter`] impl below
    /// instead, stamping each DONE with its own statement's state; here every
    /// DONE carries the one final state, which is all an error-only reply has.
    pub(super) fn send_outcome(&self, outcome: BatchOutcome, in_transaction: bool) {
        for result in outcome.results {
            let sent = match result {
                StatementResult::Rows(rowset) => self.send_rowset(rowset, in_transaction),
                StatementResult::RowsAffected(n) => self.send(BatchEvent::StatementDone {
                    count: Some(n),
                    in_transaction,
                    command: crate::engine::DoneCommand::Other,
                }),
                StatementResult::Done => self.send(BatchEvent::StatementDone {
                    count: None,
                    in_transaction,
                    command: crate::engine::DoneCommand::Other,
                }),
            };
            if !sent {
                return;
            }
        }
        if let Some(error) = outcome.error
            && !self.send(BatchEvent::Error(error))
        {
            return;
        }
        self.send(BatchEvent::Complete { in_transaction });
    }

    /// Sends one result set: metadata, then rows in [`EVENT_ROWS`] chunks.
    pub(super) fn send_rowset(&self, rowset: RowSet, in_transaction: bool) -> bool {
        let count = rowset.rows.len() as u64;
        if !self.send(BatchEvent::Columns(rowset.columns)) {
            return false;
        }
        // Taken from the front through the iterator, not `split_off`: splitting
        // hands back the *remainder* each time, so every chunk memmoves what is
        // left and a large result costs O(n²). This moves each row once.
        let mut rows = rowset.rows.into_iter();
        loop {
            let chunk: Vec<Vec<Datum>> = rows.by_ref().take(EVENT_ROWS).collect();
            if chunk.is_empty() {
                break;
            }
            if !self.send(BatchEvent::Rows(chunk)) {
                return false;
            }
        }
        self.send(BatchEvent::StatementDone {
            count: Some(count),
            in_transaction,
            command: crate::engine::DoneCommand::Select,
        })
    }

    /// Sends a batch's terminal events: its error, if it ended with one, then
    /// `Complete` with the post-batch transaction state (which the TDS
    /// transaction-manager path reads — it stays batch-final by design).
    pub(super) fn send_tail(
        &self,
        error: Option<truthdb_sql::error::SqlError>,
        in_transaction: bool,
    ) {
        if let Some(error) = error
            && !self.send(BatchEvent::Error(error))
        {
            return;
        }
        self.send(BatchEvent::Complete { in_transaction });
    }
}

/// The worker-side face of the reply channel: the executor emits each
/// statement's results through this as it runs, which is what puts rows on
/// the wire while the batch still executes. Send failures mean the client is
/// gone; the batch still runs to completion (its effects do not depend on
/// anyone listening) and the disconnect path's cancel flag stops it early.
impl crate::engine::BatchEmitter for BatchSink {
    fn columns(&mut self, columns: Vec<ResultColumn>) {
        self.send(BatchEvent::Columns(columns));
    }

    fn rows(&mut self, rows: Vec<Vec<Datum>>) {
        self.send(BatchEvent::Rows(rows));
    }

    fn statement_done(
        &mut self,
        count: Option<u64>,
        in_transaction: bool,
        command: crate::engine::DoneCommand,
    ) {
        self.send(BatchEvent::StatementDone {
            count,
            in_transaction,
            command,
        });
    }

    fn statement_aborted(&mut self, in_transaction: bool) {
        self.send(BatchEvent::StatementAborted { in_transaction });
    }

    fn database_context(&mut self, database: &str) {
        self.send(BatchEvent::DatabaseContext {
            database: database.to_string(),
        });
    }

    fn info(&mut self, error: &truthdb_sql::error::SqlError) {
        self.send(BatchEvent::Info(error.clone()));
    }
}

/// Reassembles an event stream into a whole [`BatchReply`] — the shape every
/// caller that wants the entire result still asks for (the transaction-manager
/// path, the tests). Draining as the worker produces is what keeps this from
/// being a second copy on top of the first.
pub(super) async fn collect_reply(
    events: &mut mpsc::UnboundedReceiver<BatchEvent>,
) -> Result<BatchReply, EngineError> {
    let mut results: Vec<StatementResult> = Vec::new();
    let mut error = None;
    // The result set currently streaming, if this statement opened one.
    let mut open: Option<RowSet> = None;
    while let Some(event) = events.recv().await {
        match event {
            BatchEvent::Columns(columns) => {
                open = Some(RowSet {
                    columns,
                    rows: Vec::new(),
                });
            }
            BatchEvent::Rows(mut rows) => {
                if let Some(rowset) = open.as_mut() {
                    rowset.rows.append(&mut rows);
                }
            }
            BatchEvent::StatementDone { count, .. } => results.push(match open.take() {
                Some(rowset) => StatementResult::Rows(rowset),
                None => match count {
                    Some(n) => StatementResult::RowsAffected(n),
                    None => StatementResult::Done,
                },
            }),
            // The aborted statement contributes no result; its partly-streamed
            // rowset is dropped, which is what the buffered path returned too.
            BatchEvent::StatementAborted { .. } => open = None,
            // A whole-reply caller has nowhere to carry a prepared handle —
            // only the TDS renderer (RETURNVALUE) consumes it. Same for a
            // database-context change (the ENVCHANGE is wire-only).
            BatchEvent::PreparedHandle(_) => {}
            BatchEvent::DatabaseContext { .. } => {}
            // An informational message (RAISERROR <= 10) is wire-only too:
            // it is not an error and carries no result.
            BatchEvent::Info(_) => {}
            // Return status/values are wire-only (RETURNSTATUS/RETURNVALUE).
            BatchEvent::ReturnStatus(_) | BatchEvent::ReturnValue { .. } => {}
            BatchEvent::Error(err) => error = Some(err),
            BatchEvent::Complete { in_transaction } => {
                return Ok(BatchReply {
                    outcome: BatchOutcome { results, error },
                    in_transaction,
                });
            }
            BatchEvent::Failed(err) => return Err(err),
        }
    }
    // The stream ended without a terminal event: the worker pool is gone.
    Err(EngineError::Unavailable)
}
