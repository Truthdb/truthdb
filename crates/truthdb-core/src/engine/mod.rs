mod relational;
mod search;

pub use relational::{
    BatchEmitter, BatchOutcome, CancelScope, Collector, DoneCommand, FATAL_SEVERITY, Isolation,
    ResultColumn, RowSet, RpcParam, SqlStatement, StatementResult, TxnContext, analyze_locks,
    decl_names, describe_first_result_set, execute_batch, execute_batch_streamed, render_cell,
};
#[cfg(any(test, feature = "test-hooks"))]
pub use relational::{
    clear_test_cancel, execute, execute_batch_with_params, set_test_cancel, set_test_sort_budget,
    without_scan_path,
};
pub use search::{
    Command, CommandError, ENGINE_WAL_ENTRY_TYPE, ENGINE_WAL_ENTRY_VERSION, EngineMeta,
    EngineState, SearchQuery, WalEvent, decode_snapshot, parse_command, render_json,
};
#[cfg(any(test, feature = "test-hooks"))]
pub use search::{Document, FieldType, IndexState};

use thiserror::Error;

use crate::storage::StorageError;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{0}")]
    Command(#[from] CommandError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("{0}")]
    Replay(String),

    #[error("engine is shutting down")]
    Unavailable,
}
