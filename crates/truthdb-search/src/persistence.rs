use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::index::{Document, EngineState, FieldType, IndexState};
use super::{CommandError, SearchError};

pub const ENGINE_WAL_ENTRY_VERSION: u16 = 1;
pub const ENGINE_WAL_ENTRY_TYPE: u16 = 1;
/// The engine's search-subsystem state, mutated only on the native path
/// ([`Engine::execute`]) and read by the checkpointer. Guarded by
/// [`Engine::meta`], a `RwLock` that doubles as the execution gate that keeps
/// the two paths from observing each other's torn state (see [`Engine`]).
pub struct EngineMeta {
    pub state: EngineState,
    pub next_seq_no: u64,
    pub next_doc_id: u64,
}

impl EngineMeta {
    pub fn apply_event(&mut self, event: &WalEvent) -> Result<(), SearchError> {
        match event {
            WalEvent::CreateIndex { name, mappings } => {
                if self.state.indices.contains_key(name) {
                    return Err(SearchError::Replay(format!(
                        "wal attempted to recreate existing index '{name}'"
                    )));
                }
                let index = IndexState::new(mappings.clone());
                self.state.indices.insert(name.clone(), index);
            }
            WalEvent::InsertDocument {
                index,
                id,
                document,
            } => {
                let index_state = self.state.indices.get_mut(index).ok_or_else(|| {
                    SearchError::Replay(format!("wal references unknown index '{index}'"))
                })?;
                index_state.insert_document(id, document)?;
                self.next_doc_id = self
                    .next_doc_id
                    .max(id.parse::<u64>().unwrap_or(0).saturating_add(1));
            }
        }
        Ok(())
    }

    pub fn validate_create_index(
        &self,
        name: &str,
        mappings: &BTreeMap<String, FieldType>,
    ) -> Result<(), SearchError> {
        if self.state.indices.contains_key(name) {
            return Err(CommandError::IndexAlreadyExists(name.to_string()).into());
        }
        if mappings.is_empty() {
            return Err(CommandError::InvalidCommand(
                "index must define at least one mapped field".to_string(),
            )
            .into());
        }
        Ok(())
    }

    pub fn validate_insert_document(
        &self,
        index: &str,
        document: &Document,
    ) -> Result<(), SearchError> {
        let index_state = self
            .state
            .indices
            .get(index)
            .ok_or_else(|| CommandError::UnknownIndex(index.to_string()))?;
        index_state.validate_document(document)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEvent {
    CreateIndex {
        name: String,
        mappings: BTreeMap<String, FieldType>,
    },
    InsertDocument {
        index: String,
        id: String,
        document: Document,
    },
}
/// Decodes a snapshot payload: JSON (current format), falling back to
/// bincode for snapshots written by older versions. Legacy bincode snapshots
/// can only have been document-free (bincode cannot deserialize
/// `serde_json::Value`, so document-bearing ones were never loadable).
pub fn decode_snapshot(data: &[u8]) -> Result<EngineState, SearchError> {
    match serde_json::from_slice(data) {
        Ok(state) => Ok(state),
        Err(json_err) => bincode::deserialize(data).map_err(|bincode_err| {
            SearchError::Replay(format!(
                "failed to decode snapshot: as json: {json_err}; as legacy bincode: {bincode_err}"
            ))
        }),
    }
}
