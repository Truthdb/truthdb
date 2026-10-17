//! Native document-search implementation owned by [`super::Engine`].

mod command;
mod index;
mod persistence;

pub use command::CommandError;
pub use command::{Command, parse_command, render_json};
#[cfg(any(test, feature = "test-hooks"))]
pub use index::{Document, FieldType, IndexState};
pub use index::{EngineState, SearchQuery};
pub use persistence::{
    ENGINE_WAL_ENTRY_TYPE, ENGINE_WAL_ENTRY_VERSION, EngineMeta, WalEvent, decode_snapshot,
};
