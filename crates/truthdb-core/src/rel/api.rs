use super::*;

/// Result of one executed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementResult {
    Rows(RowSet),
    RowsAffected(u64),
    /// DDL and other statements with no rowset and no count.
    Done,
}

/// A result column: its name and resolved SQL type (drives TDS
/// COLMETADATA and display rendering alike).
#[derive(Debug, Clone, PartialEq)]
pub struct ResultColumn {
    pub name: String,
    pub column_type: ColumnType,
}

/// A typed result set: column metadata plus rows of typed [`Datum`]s.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSet {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Datum>>,
}

/// Error severity at or above which a statement failure dooms the whole
/// transaction even under `SET XACT_ABORT OFF` (SQL Server treats severity ≥ 17
/// as resource/batch-level, versus 11–16 statement-level). Constraint violations
/// (2627/2601/515/547, severity 14–16) stay below it, so they roll back only the
/// failing statement and the transaction survives.
pub(super) const XACT_ABORT_SEVERITY: u8 = 17;

/// Error severity at or above which the error is fatal to the CONNECTION
/// (SQL Server severity >= 20): it bypasses every `TRY`, dooms the
/// transaction, and the protocol layers close the stream after delivering
/// it. Only RAISERROR ... WITH LOG can currently produce one.
pub const FATAL_SEVERITY: u8 = 20;

/// A batch's outcome: the results of the statements that ran, plus the error
/// that stopped the batch (if any). Statements before an error have already
/// committed (each statement is autocommit in Stage 3), so their results are
/// preserved rather than discarded.
pub struct BatchOutcome {
    pub results: Vec<StatementResult>,
    pub error: Option<SqlError>,
}

/// One `sp_executesql` parameter: its `@name` (as it appears in the RPC
/// stream), declared type, and decoded value. Passed by the TDS layer to
/// [`execute_batch_with_params`], which seeds them as batch variables the
/// statement text can read by name.
#[derive(Debug, Clone)]
pub struct RpcParam {
    pub name: String,
    pub column_type: ColumnType,
    pub value: Datum,
}

/// Parses and executes a SQL batch. A parse error yields an empty batch with
/// the error; a runtime error stops the batch but keeps earlier results.
pub trait BatchEmitter {
    /// Opens a result set: its column metadata.
    fn columns(&mut self, columns: Vec<ResultColumn>);
    /// A chunk of rows for the open result set.
    fn rows(&mut self, rows: Vec<Vec<Datum>>);
    /// Ends one statement: its row count / rows-affected (`None` for DDL),
    /// the transaction state after it ran, and its command class (the DONE's
    /// `CurCmd` on the wire).
    fn statement_done(&mut self, count: Option<u64>, in_transaction: bool, command: DoneCommand);
    /// Ends a statement that failed after its result set had begun streaming:
    /// the set is closed so the stream stays framed for the statements that
    /// follow. The error itself is reported separately — at the end of the
    /// batch for a continued error, or not at all for one a `CATCH` handled.
    fn statement_aborted(&mut self, in_transaction: bool);

    /// The session's database context was (re-)established (`USE`): TDS
    /// renders the ENVCHANGE + 5701 INFO SSMS expects. Emitters that have no
    /// wire (the collecting native path, tests) ignore it.
    fn database_context(&mut self, _database: &str) {}

    /// An informational message (RAISERROR severity <= 10): TDS renders an
    /// INFO token in-stream, not an error. Emitters with no wire ignore it.
    fn info(&mut self, _error: &SqlError) {}
}

/// A [`BatchEmitter`] that drops everything: a scalar function body produces no
/// result sets (a data-returning SELECT is rejected at CREATE, 444), so its
/// per-statement DONE events have nowhere to go.
pub(super) struct DiscardEmitter;

impl BatchEmitter for DiscardEmitter {
    fn columns(&mut self, _columns: Vec<ResultColumn>) {}
    fn rows(&mut self, _rows: Vec<Vec<Datum>>) {}
    fn statement_done(
        &mut self,
        _count: Option<u64>,
        _in_transaction: bool,
        _command: DoneCommand,
    ) {
    }
    fn statement_aborted(&mut self, _in_transaction: bool) {}
}

/// Reassembles emitted results into the whole-batch [`BatchOutcome`] for the
/// callers that want everything at once.
#[derive(Default)]
pub struct Collector {
    results: Vec<StatementResult>,
    /// The result set currently streaming, if a statement opened one.
    open: Option<RowSet>,
}

impl Collector {
    /// The collected outcome. A result set still open belongs to a statement
    /// that failed after its rows started streaming; a failed statement
    /// contributes no result, so it is dropped.
    pub fn into_outcome(self, error: Option<SqlError>) -> BatchOutcome {
        BatchOutcome {
            results: self.results,
            error,
        }
    }
}

impl BatchEmitter for Collector {
    fn columns(&mut self, columns: Vec<ResultColumn>) {
        self.open = Some(RowSet {
            columns,
            rows: Vec::new(),
        });
    }

    fn rows(&mut self, mut rows: Vec<Vec<Datum>>) {
        if let Some(rowset) = self.open.as_mut() {
            rowset.rows.append(&mut rows);
        }
    }

    fn statement_done(&mut self, count: Option<u64>, _in_transaction: bool, _command: DoneCommand) {
        self.results.push(match self.open.take() {
            Some(rowset) => StatementResult::Rows(rowset),
            None => match count {
                Some(n) => StatementResult::RowsAffected(n),
                None => StatementResult::Done,
            },
        });
    }

    fn statement_aborted(&mut self, _in_transaction: bool) {
        self.open = None;
    }
}

/// The mutable accumulator threaded through [`run_block`] across a batch and
/// its nested `TRY`/`CATCH` blocks: the emitter results leave through, the
/// DONEs held back for durability, and the batch's error state.
pub(super) struct DeferredDone {
    pub(super) count: Option<u64>,
    pub(super) in_transaction: bool,
    pub(super) command: DoneCommand,
}

/// The command class a statement's DONE reports in its `CurCmd` field.
/// mssql-jdbc discards a DONE's row count unless `CurCmd` names a DML
/// command, so `executeUpdate` returns -1 against a server that leaves it
/// zero (pytds and go-mssqldb ignore the field).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DoneCommand {
    Select,
    Insert,
    Update,
    Delete,
    /// DDL, SET, transaction control — anything whose count nobody reads.
    Other,
}
