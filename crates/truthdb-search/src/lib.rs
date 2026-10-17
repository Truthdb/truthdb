//! Native document-search implementation: the legacy Elasticsearch-like
//! engine (index definitions, documents, postings) over the shared log.

mod command;
mod index;
mod persistence;

pub use command::CommandError;

use thiserror::Error;

/// A search failure: a rejected command, or state the log cannot rebuild.
#[derive(Debug, Error)]
pub enum SearchError {
    #[error("{0}")]
    Command(#[from] CommandError),

    #[error("{0}")]
    Replay(String),
}
pub use command::{Command, parse_command, render_json};
pub use index::{Document, FieldType, IndexState};
pub use index::{EngineState, SearchQuery};
pub use persistence::{
    ENGINE_WAL_ENTRY_TYPE, ENGINE_WAL_ENTRY_VERSION, EngineMeta, WalEvent, decode_snapshot,
};
