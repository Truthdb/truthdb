//! SQL execution over the relational storage engine.
//!
//! Parses a batch with [`truthdb_sql`], then binds and runs each statement
//! against [`Storage`]'s `rel_*` API. SELECT uses a simple Volcano-style
//! pipeline materialized in memory: source scan -> WHERE filter -> ORDER BY
//! sort -> TOP limit -> projection. `sys.tables`/`sys.columns` are virtual
//! sources built from the catalog. Storage errors are mapped to SQL Server
//! error numbers.

mod aggregate;
mod api;
mod batch;
mod cancel;
pub mod collation;
mod constraints;
mod context;
mod ddl;
mod describe;
mod dispatch;
mod dml;
mod hash;
mod helpers;
mod lock_analysis;
mod parameters;
mod plan;
mod prelude;
mod procedural;
mod query;
mod sys_views;
mod transaction;
mod triggers;
mod value;

pub use api::{
    BatchEmitter, BatchOutcome, Collector, DoneCommand, FATAL_SEVERITY, ResultColumn, RowSet,
    RpcParam, StatementResult,
};
#[cfg(test)]
pub use batch::execute_batch_with_params;
pub use batch::{execute_batch, execute_batch_streamed};
pub use cancel::{CancelScope, check_cancelled};
#[cfg(test)]
pub(crate) use cancel::{clear_test_cancel, set_test_cancel};
pub use context::{Isolation, TxnContext};
pub use describe::describe_first_result_set;
#[cfg(test)]
pub use dispatch::execute;
pub use helpers::{SqlStatement, render_cell};
pub use lock_analysis::analyze_locks;
pub(crate) use parameters::decl_names;
#[cfg(test)]
pub(crate) use query::{set_test_sort_budget, without_scan_path};

#[cfg(test)]
mod tests;
