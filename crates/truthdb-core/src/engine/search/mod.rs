//! Native document-search implementation owned by [`super::Engine`].

mod command;
mod index;

pub use command::CommandError;
pub(super) use command::{Command, parse_command, render_json};
pub(super) use index::{Document, EngineState, FieldType, IndexState, SearchQuery};
