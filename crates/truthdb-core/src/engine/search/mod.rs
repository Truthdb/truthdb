//! Native document-search implementation owned by [`super::Engine`].

mod command;
mod index;
mod persistence;

pub use command::CommandError;
pub(super) use command::{Command, parse_command, render_json};
#[cfg(test)]
pub(super) use index::{Document, FieldType, IndexState};
pub(super) use index::{EngineState, SearchQuery};
pub(super) use persistence::{
    ENGINE_WAL_ENTRY_TYPE, ENGINE_WAL_ENTRY_VERSION, EngineMeta, WalEvent, decode_snapshot,
};
