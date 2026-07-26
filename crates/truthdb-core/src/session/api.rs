use super::*;

/// The result of running a batch for a session: its typed outcome plus
/// whether the connection is still inside a transaction afterwards (so the
/// TDS gateway can set `DONE_INXACT`).
pub struct BatchReply {
    pub outcome: BatchOutcome,
    pub in_transaction: bool,
}

/// How many rows one [`BatchEvent::Rows`] carries.
pub(super) const EVENT_ROWS: usize = 256;

/// One event in a batch's reply.
///
/// A batch emits, per statement, either `Columns` followed by zero or more
/// `Rows` chunks and then `StatementDone`, or a bare `StatementDone` (DDL, a
/// row count). A batch-stopping `Error` may follow the statements that ran.
/// Every stream ends with exactly one terminal event — `Complete` or `Failed` —
/// unless the receiver is dropped first.
#[derive(Debug)]
pub enum BatchEvent {
    /// Starts a result set: its column metadata.
    Columns(Vec<ResultColumn>),
    /// A chunk of rows for the result set the last `Columns` opened.
    Rows(Vec<Vec<Datum>>),
    /// Ends one statement. `count` is its row count / rows-affected, or `None`
    /// for a statement that reports neither (DDL).
    StatementDone {
        count: Option<u64>,
        /// The transaction state to stamp on this statement's DONE
        /// (`DONE_INXACT`).
        in_transaction: bool,
        /// The DONE's `CurCmd` class — mssql-jdbc drops the count without it.
        command: crate::engine::DoneCommand,
    },
    /// Ends a statement that failed after its result set had begun streaming:
    /// closes the set (with a clean DONE — an error-flagged DONE without an
    /// ERROR token reads as "severe failure" to real drivers) so the stream
    /// stays framed for the statements that follow. The error itself travels
    /// separately — in the batch-final [`BatchEvent::Error`] for a continued
    /// error, or not at all for one a `CATCH` handled.
    StatementAborted { in_transaction: bool },
    /// The session's database context was (re-)established (`USE`): the TDS
    /// layer renders the ENVCHANGE + 5701 INFO clients (SSMS) expect.
    DatabaseContext { database: String },
    /// A SQL error that stopped the batch. The statements before it kept their
    /// results, which were already sent.
    Error(SqlError),
    /// An informational message (RAISERROR severity <= 10): TDS renders an
    /// INFO token in-stream; it is not an error and stops nothing.
    Info(SqlError),
    /// A procedure's RETURN status for an RPC-by-name call: the RETURNSTATUS
    /// token's value (hardcoded 0 before this event existed).
    ReturnStatus(i32),
    /// A procedure OUTPUT parameter's final value for an RPC-by-name call,
    /// rendered as a typed RETURNVALUE token after RETURNSTATUS. `ordinal` is
    /// the parameter's 0-based position in the RPC call (ordinal-keyed drivers
    /// place the value there).
    ReturnValue {
        ordinal: u16,
        name: String,
        column_type: crate::relstore::types::ColumnType,
        value: Datum,
    },
    /// The handle `sp_prepare`/`sp_prepexec` allocated, reported to the client
    /// as a RETURNVALUE token. Sent after every statement's events, just
    /// before `Complete` — where SQL Server puts return values.
    PreparedHandle(i32),
    /// Terminal: the batch ended. Carries the batch's *final* transaction
    /// state, which is what the TDS transaction-manager path needs.
    Complete { in_transaction: bool },
    /// Terminal: the engine could not run the batch at all.
    Failed(EngineError),
}

/// A prepared-statement RPC (the `sp_prepare` handle family). `Prepare` and
/// `Unprepare` touch only session state; `Execute` and `PrepExec` run the
/// statement through the ordinary batch path (locks, parking, streaming).
pub enum PreparedRpc {
    Prepare {
        decls: String,
        stmt: String,
    },
    Execute {
        handle: i32,
        values: Vec<crate::engine::RpcParam>,
    },
    PrepExec {
        decls: String,
        stmt: String,
        values: Vec<crate::engine::RpcParam>,
    },
    Unprepare {
        handle: i32,
    },
    /// `sp_describe_first_result_set`: metadata discovery, no execution.
    Describe {
        tsql: String,
    },
}

/// Identifies a connection's session on the engine thread.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(pub(super) u64);

impl SessionId {
    /// The raw id used as the lock-manager owner key.
    pub(super) fn raw(self) -> u64 {
        self.0
    }
}

/// A login's stored authentication data, read by the TDS handshake before any
/// session exists. The credential is verified in the TDS task (off the worker
/// pool) so a ~30 ms PBKDF2 does not occupy a worker thread.
#[derive(Debug, Clone)]
pub struct LoginRecord {
    pub principal_id: u32,
    /// The stored login name in its canonical casing (for `SUSER_SNAME()`).
    pub name: String,
    pub password_blob: String,
    pub is_disabled: bool,
}
