//! Native document-search implementation owned by [`super::Engine`].

mod index;

pub(super) use index::{
    Document, EngineState, FieldType, IndexState, SearchQuery, value_type_name,
};
