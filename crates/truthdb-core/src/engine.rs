use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::relstore::row::{Column, Schema};
use crate::relstore::types::{ColumnType, Datum};
use crate::storage::{Storage, StorageError};

const ENGINE_WAL_ENTRY_VERSION: u16 = 1;
const ENGINE_WAL_ENTRY_TYPE: u16 = 1;
const WAL_CHECKPOINT_THRESHOLD: f64 = 0.75;

type Document = Map<String, Value>;

pub struct Engine {
    storage: Storage,
    state: EngineState,
    next_seq_no: u64,
    next_doc_id: u64,
}

impl Engine {
    pub fn new(storage: Storage) -> Result<Self, EngineError> {
        let mut engine = Engine {
            storage,
            state: EngineState::default(),
            next_seq_no: 1,
            next_doc_id: 1,
        };

        // Try to load a snapshot first
        if let Some(snapshot) = engine.storage.load_snapshot()? {
            engine.state = decode_snapshot(&snapshot.data)?;
            engine.next_seq_no = snapshot.next_seq_no;
            engine.next_doc_id = snapshot.next_doc_id;
            // Rebuild postings (not serialized)
            for index_state in engine.state.indices.values_mut() {
                index_state.rebuild_postings()?;
            }
        }

        // Replay any WAL entries after the snapshot. The ring is shared with
        // other subsystems (relational records use different entry types);
        // only search events are ours to apply. Records the snapshot already
        // covers (seq_no below its next_seq_no) are skipped: a crash between
        // the snapshot descriptor becoming durable and the WAL head
        // advancing legitimately leaves them in the ring, and re-applying
        // them would fail (duplicate index/document errors).
        let records = engine.storage.replay_wal_entries()?;
        for record in records {
            if record.entry_type != ENGINE_WAL_ENTRY_TYPE || record.seq_no < engine.next_seq_no {
                continue;
            }
            let event: WalEvent = serde_json::from_slice(&record.payload)
                .map_err(|err| EngineError::Replay(format!("failed to decode wal event: {err}")))?;
            engine.apply_event(&event)?;
            engine.next_seq_no = record.seq_no.saturating_add(1);
        }

        Ok(engine)
    }

    pub fn execute(&mut self, input: &str) -> Result<String, EngineError> {
        match parse_command(input)? {
            Command::CreateIndex { name, mappings } => {
                self.validate_create_index(&name, &mappings)?;
                let event = WalEvent::CreateIndex {
                    name: name.clone(),
                    mappings: mappings.clone(),
                };
                self.persist_event(&event)?;
                self.apply_event(&event)?;
                self.maybe_checkpoint()?;
                render_json(&json!({
                    "acknowledged": true,
                    "index": name,
                }))
            }
            Command::InsertDocument { index, document } => {
                self.validate_insert_document(&index, &document)?;
                let doc_id = self.next_doc_id.to_string();
                let event = WalEvent::InsertDocument {
                    index: index.clone(),
                    id: doc_id.clone(),
                    document: document.clone(),
                };
                self.persist_event(&event)?;
                self.apply_event(&event)?;
                self.maybe_checkpoint()?;
                render_json(&json!({
                    "_id": doc_id,
                    "_index": index,
                    "result": "created",
                }))
            }
            Command::Search { index, query } => {
                let index_state = self.state.indices.get(&index).ok_or_else(|| {
                    EngineError::Command(CommandError::UnknownIndex(index.clone()))
                })?;
                let hits = index_state.search(&query)?;
                let total = hits.len();
                render_json(&json!({
                    "hits": {
                        "total": total,
                        "hits": hits,
                    }
                }))
            }
            Command::Table(table_command) => self.execute_table(table_command),
        }
    }

    /// Temporary Stage 2 debug surface over the relational store; replaced
    /// by SQL in Stage 3.
    fn execute_table(&mut self, command: TableCommand) -> Result<String, EngineError> {
        match command {
            TableCommand::Create { name, body } => {
                let (columns, key_names) = parse_table_create_body(&body)?;
                self.storage.rel_create_table(&name, columns, &key_names)?;
                self.maybe_checkpoint()?;
                render_json(&json!({ "acknowledged": true, "table": name }))
            }
            TableCommand::Insert { name, body } => {
                let schema = self.table_schema(&name)?;
                let values = parse_row_values(&schema, &body)?;
                self.storage.rel_insert(&name, values)?;
                self.maybe_checkpoint()?;
                render_json(&json!({ "rows_affected": 1 }))
            }
            TableCommand::Scan { name } => {
                let schema = self.table_schema(&name)?;
                let rows = self.storage.rel_scan(&name)?;
                let rendered: Vec<Value> = rows
                    .iter()
                    .map(|row| {
                        let mut object = Map::new();
                        for (column, value) in schema.columns.iter().zip(row) {
                            object.insert(column.name.clone(), value.to_json(&column.column_type));
                        }
                        Value::Object(object)
                    })
                    .collect();
                render_json(&json!({ "count": rendered.len(), "rows": rendered }))
            }
            TableCommand::Update { name, body } => {
                let schema = self.table_schema(&name)?;
                let (column, value) = parse_where(&schema, &body)?;
                let assignments = parse_set(&schema, &body)?;
                let count = self
                    .storage
                    .rel_update_where(&name, &column, &value, &assignments)?;
                self.maybe_checkpoint()?;
                render_json(&json!({ "rows_affected": count }))
            }
            TableCommand::Delete { name, body } => {
                let schema = self.table_schema(&name)?;
                let (column, value) = parse_where(&schema, &body)?;
                let count = self.storage.rel_delete_where(&name, &column, &value)?;
                self.maybe_checkpoint()?;
                render_json(&json!({ "rows_affected": count }))
            }
        }
    }

    fn table_schema(&self, name: &str) -> Result<Schema, EngineError> {
        let def = self
            .storage
            .rel_table(name)
            .ok_or_else(|| CommandError::InvalidCommand(format!("unknown table '{name}'")))?;
        Ok(def.schema()?)
    }

    pub fn checkpoint(&mut self) -> Result<(), EngineError> {
        // JSON, not bincode: documents hold serde_json::Value, which bincode
        // can serialize but never deserialize (`deserialize_any`), so bincode
        // snapshots with documents could not be loaded back.
        let data = serde_json::to_vec(&self.state)
            .map_err(|err| EngineError::Replay(format!("failed to serialize state: {err}")))?;
        let checkpoint_seq = self.next_seq_no.saturating_sub(1);
        self.storage
            .write_checkpoint(&data, checkpoint_seq, self.next_seq_no, self.next_doc_id)?;
        Ok(())
    }

    pub fn wal_usage_ratio(&self) -> f64 {
        self.storage.wal_usage_ratio()
    }

    fn maybe_checkpoint(&mut self) -> Result<(), EngineError> {
        if self.wal_usage_ratio() >= WAL_CHECKPOINT_THRESHOLD {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn persist_event(&mut self, event: &WalEvent) -> Result<(), EngineError> {
        let payload = serde_json::to_vec(event)
            .map_err(|err| EngineError::Replay(format!("failed to encode wal event: {err}")))?;
        let seq_no = self.next_seq_no;
        self.storage.append_wal_entry(
            ENGINE_WAL_ENTRY_TYPE,
            ENGINE_WAL_ENTRY_VERSION,
            seq_no,
            &payload,
        )?;
        self.next_seq_no = self.next_seq_no.saturating_add(1);
        Ok(())
    }

    fn apply_event(&mut self, event: &WalEvent) -> Result<(), EngineError> {
        match event {
            WalEvent::CreateIndex { name, mappings } => {
                if self.state.indices.contains_key(name) {
                    return Err(EngineError::Replay(format!(
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
                    EngineError::Replay(format!("wal references unknown index '{index}'"))
                })?;
                index_state.insert_document(id, document)?;
                self.next_doc_id = self
                    .next_doc_id
                    .max(id.parse::<u64>().unwrap_or(0).saturating_add(1));
            }
        }
        Ok(())
    }

    fn validate_create_index(
        &self,
        name: &str,
        mappings: &BTreeMap<String, FieldType>,
    ) -> Result<(), EngineError> {
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

    fn validate_insert_document(
        &self,
        index: &str,
        document: &Document,
    ) -> Result<(), EngineError> {
        let index_state = self
            .state
            .indices
            .get(index)
            .ok_or_else(|| CommandError::UnknownIndex(index.to_string()))?;
        index_state.validate_document(document)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct EngineState {
    indices: BTreeMap<String, IndexState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexState {
    mappings: BTreeMap<String, FieldType>,
    documents: BTreeMap<String, Document>,
    #[serde(skip)]
    text_postings: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    #[serde(skip)]
    exact_postings: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
}

impl IndexState {
    fn new(mappings: BTreeMap<String, FieldType>) -> Self {
        IndexState {
            mappings,
            documents: BTreeMap::new(),
            text_postings: BTreeMap::new(),
            exact_postings: BTreeMap::new(),
        }
    }

    fn rebuild_postings(&mut self) -> Result<(), EngineError> {
        self.text_postings.clear();
        self.exact_postings.clear();
        let doc_ids: Vec<String> = self.documents.keys().cloned().collect();
        for doc_id in doc_ids {
            let document = self.documents.get(&doc_id).unwrap().clone();
            self.index_document(&doc_id, &document)?;
        }
        Ok(())
    }

    fn validate_document(&self, document: &Document) -> Result<(), EngineError> {
        for (field, value) in document {
            let field_type = self
                .mappings
                .get(field)
                .ok_or_else(|| CommandError::UnknownField(field.clone()))?;
            field_type.validate_value(field, value)?;
        }
        Ok(())
    }

    fn insert_document(&mut self, id: &str, document: &Document) -> Result<(), EngineError> {
        if self.documents.contains_key(id) {
            return Err(EngineError::Replay(format!(
                "wal attempted to insert duplicate document id '{id}'"
            )));
        }
        self.validate_document(document)?;
        self.index_document(id, document)?;
        self.documents.insert(id.to_string(), document.clone());
        Ok(())
    }

    fn index_document(&mut self, id: &str, document: &Document) -> Result<(), EngineError> {
        for (field, value) in document {
            let Some(field_type) = self.mappings.get(field) else {
                continue;
            };
            match field_type {
                FieldType::Text => {
                    let Some(text) = value.as_str() else {
                        return Err(CommandError::InvalidFieldType {
                            field: field.clone(),
                            expected: "text string".to_string(),
                            actual: value_type_name(value).to_string(),
                        }
                        .into());
                    };
                    let postings = self.text_postings.entry(field.clone()).or_default();
                    for term in tokenize(text) {
                        postings.entry(term).or_default().insert(id.to_string());
                    }
                }
                FieldType::Keyword | FieldType::Float => {
                    let term = exact_term_key(field, *field_type, value)?;
                    self.exact_postings
                        .entry(field.clone())
                        .or_default()
                        .entry(term)
                        .or_default()
                        .insert(id.to_string());
                }
            }
        }
        Ok(())
    }

    fn search(&self, query: &SearchQuery) -> Result<Vec<SearchHit>, EngineError> {
        let scores = self.evaluate_query(query)?;
        let mut hits = scores
            .into_iter()
            .filter_map(|(id, score)| {
                self.documents.get(&id).map(|document| SearchHit {
                    id,
                    score,
                    source: Value::Object(document.clone()),
                })
            })
            .collect::<Vec<_>>();

        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(hits)
    }

    fn evaluate_query(&self, query: &SearchQuery) -> Result<BTreeMap<String, f64>, EngineError> {
        match query {
            SearchQuery::Match { field, query } => self.evaluate_match_query(field, query),
            SearchQuery::Term { field, value } => self.evaluate_term_query(field, value),
            SearchQuery::Bool { must, filter } => self.evaluate_bool_query(must, filter),
        }
    }

    fn evaluate_match_query(
        &self,
        field: &str,
        query: &str,
    ) -> Result<BTreeMap<String, f64>, EngineError> {
        self.require_field_type(field, FieldType::Text)?;
        let Some(postings) = self.text_postings.get(field) else {
            return Ok(BTreeMap::new());
        };

        let mut scores = BTreeMap::new();
        for term in tokenize(query) {
            if let Some(doc_ids) = postings.get(&term) {
                for doc_id in doc_ids {
                    *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0;
                }
            }
        }
        Ok(scores)
    }

    fn evaluate_term_query(
        &self,
        field: &str,
        value: &Value,
    ) -> Result<BTreeMap<String, f64>, EngineError> {
        let field_type = self.field_type(field)?;
        let term = exact_term_key(field, field_type, value)?;
        let Some(postings) = self.exact_postings.get(field) else {
            return Ok(BTreeMap::new());
        };
        let Some(doc_ids) = postings.get(&term) else {
            return Ok(BTreeMap::new());
        };

        let mut scores = BTreeMap::new();
        for doc_id in doc_ids {
            scores.insert(doc_id.clone(), 1.0);
        }
        Ok(scores)
    }

    fn evaluate_bool_query(
        &self,
        must: &[SearchQuery],
        filter: &[SearchQuery],
    ) -> Result<BTreeMap<String, f64>, EngineError> {
        if must.is_empty() && filter.is_empty() {
            return Err(CommandError::InvalidCommand(
                "bool query must contain at least one must or filter clause".to_string(),
            )
            .into());
        }

        let mut scores = if must.is_empty() {
            self.documents
                .keys()
                .map(|id| (id.clone(), 0.0))
                .collect::<BTreeMap<_, _>>()
        } else {
            let mut iter = must.iter();
            let first = iter.next().ok_or_else(|| {
                CommandError::InvalidCommand("missing bool.must clause".to_string())
            })?;
            let mut scores = self.evaluate_query(first)?;
            for clause in iter {
                let clause_scores = self.evaluate_query(clause)?;
                scores.retain(|doc_id, score| {
                    let Some(clause_score) = clause_scores.get(doc_id) else {
                        return false;
                    };
                    *score += clause_score;
                    true
                });
            }
            scores
        };

        for clause in filter {
            let allowed = self.evaluate_query(clause)?;
            scores.retain(|doc_id, _| allowed.contains_key(doc_id));
        }

        Ok(scores)
    }

    fn field_type(&self, field: &str) -> Result<FieldType, EngineError> {
        self.mappings
            .get(field)
            .copied()
            .ok_or_else(|| CommandError::UnknownField(field.to_string()).into())
    }

    fn require_field_type(&self, field: &str, expected: FieldType) -> Result<(), EngineError> {
        let actual = self.field_type(field)?;
        if actual != expected {
            return Err(CommandError::InvalidCommand(format!(
                "field '{field}' is {actual}, expected {expected}"
            ))
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum Command {
    CreateIndex {
        name: String,
        mappings: BTreeMap<String, FieldType>,
    },
    InsertDocument {
        index: String,
        document: Document,
    },
    Search {
        index: String,
        query: SearchQuery,
    },
    Table(TableCommand),
}

/// Temporary Stage 2 debug commands over the relational store.
#[derive(Debug, Clone)]
enum TableCommand {
    Create { name: String, body: Document },
    Insert { name: String, body: Document },
    Scan { name: String },
    Update { name: String, body: Document },
    Delete { name: String, body: Document },
}

#[derive(Debug, Clone)]
enum SearchQuery {
    Match {
        field: String,
        query: String,
    },
    Term {
        field: String,
        value: Value,
    },
    Bool {
        must: Vec<SearchQuery>,
        filter: Vec<SearchQuery>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum FieldType {
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "keyword")]
    Keyword,
    #[serde(rename = "float")]
    Float,
}

impl FieldType {
    fn validate_value(self, field: &str, value: &Value) -> Result<(), EngineError> {
        match self {
            FieldType::Text | FieldType::Keyword => {
                if value.as_str().is_none() {
                    return Err(CommandError::InvalidFieldType {
                        field: field.to_string(),
                        expected: "string".to_string(),
                        actual: value_type_name(value).to_string(),
                    }
                    .into());
                }
            }
            FieldType::Float => {
                if !value.is_number() {
                    return Err(CommandError::InvalidFieldType {
                        field: field.to_string(),
                        expected: "number".to_string(),
                        actual: value_type_name(value).to_string(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::Text => f.write_str("text"),
            FieldType::Keyword => f.write_str("keyword"),
            FieldType::Float => f.write_str("float"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WalEvent {
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

#[derive(Debug, Serialize)]
struct SearchHit {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_score")]
    score: f64,
    #[serde(rename = "_source")]
    source: Value,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{0}")]
    Command(#[from] CommandError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("{0}")]
    Replay(String),
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("invalid command: {0}")]
    InvalidCommand(String),

    #[error("malformed json: {0}")]
    MalformedJson(String),

    #[error("index '{0}' already exists")]
    IndexAlreadyExists(String),

    #[error("unknown index '{0}'")]
    UnknownIndex(String),

    #[error("unknown field '{0}'")]
    UnknownField(String),

    #[error("invalid field type for '{field}': expected {expected}, got {actual}")]
    InvalidFieldType {
        field: String,
        expected: String,
        actual: String,
    },
}

fn parse_command(input: &str) -> Result<Command, CommandError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(CommandError::InvalidCommand(
            "command cannot be empty".to_string(),
        ));
    }

    if let Some((header, body)) = split_command(trimmed, "create index")? {
        let name = parse_single_name(header, "create index")?;
        let mappings = parse_create_index_body(body)?;
        return Ok(Command::CreateIndex { name, mappings });
    }

    if let Some((header, body)) = split_command(trimmed, "insert document")? {
        let index = parse_single_name(header, "insert document")?;
        let document = parse_document_body(body)?;
        return Ok(Command::InsertDocument { index, document });
    }

    if let Some((header, body)) = split_command(trimmed, "search")? {
        let index = parse_single_name(header, "search")?;
        let query = parse_search_body(body)?;
        return Ok(Command::Search { index, query });
    }

    if let Some((header, body)) = split_command(trimmed, "table create")? {
        let name = parse_single_name(header, "table create")?;
        let body = parse_document_body(body)?;
        return Ok(Command::Table(TableCommand::Create { name, body }));
    }
    if let Some((header, body)) = split_command(trimmed, "table insert")? {
        let name = parse_single_name(header, "table insert")?;
        let body = parse_document_body(body)?;
        return Ok(Command::Table(TableCommand::Insert { name, body }));
    }
    if let Some((header, body)) = split_command(trimmed, "table update")? {
        let name = parse_single_name(header, "table update")?;
        let body = parse_document_body(body)?;
        return Ok(Command::Table(TableCommand::Update { name, body }));
    }
    if let Some((header, body)) = split_command(trimmed, "table delete")? {
        let name = parse_single_name(header, "table delete")?;
        let body = parse_document_body(body)?;
        return Ok(Command::Table(TableCommand::Delete { name, body }));
    }
    if let Some((header, _body)) = split_command(trimmed, "table scan")? {
        let name = parse_single_name(header, "table scan")?;
        return Ok(Command::Table(TableCommand::Scan { name }));
    }
    // `table scan` takes no body, so also accept the bodyless form (every
    // other route requires a `{`, which split_command enforces).
    if trimmed.to_ascii_lowercase().starts_with("table scan") {
        let name = parse_single_name(trimmed, "table scan")?;
        return Ok(Command::Table(TableCommand::Scan { name }));
    }

    Err(CommandError::InvalidCommand(
        "expected one of: create index, insert document, search, table create/insert/scan/update/delete".to_string(),
    ))
}

fn split_command<'a>(
    input: &'a str,
    prefix: &str,
) -> Result<Option<(&'a str, &'a str)>, CommandError> {
    let Some(body_start) = input.find('{') else {
        return Ok(None);
    };
    let (header, body) = input.split_at(body_start);
    let header = header.trim();
    if !header
        .to_ascii_lowercase()
        .starts_with(&prefix.to_ascii_lowercase())
    {
        return Ok(None);
    }
    if body.trim().is_empty() {
        return Err(CommandError::InvalidCommand(format!(
            "{prefix} command requires a json body"
        )));
    }
    Ok(Some((header, body)))
}

fn parse_single_name(header: &str, prefix: &str) -> Result<String, CommandError> {
    let rest = header[prefix.len()..].trim();
    if rest.is_empty() {
        return Err(CommandError::InvalidCommand(format!(
            "{prefix} command requires a name"
        )));
    }
    if rest.split_whitespace().count() != 1 {
        return Err(CommandError::InvalidCommand(format!(
            "{prefix} command takes exactly one name before the json body"
        )));
    }
    Ok(rest.to_string())
}

fn parse_create_index_body(body: &str) -> Result<BTreeMap<String, FieldType>, CommandError> {
    let value = parse_json(body)?;
    let root = as_object(&value, "create index body")?;
    let mappings = root
        .get("mappings")
        .ok_or_else(|| CommandError::InvalidCommand("missing mappings object".to_string()))?;
    let mappings = as_object(mappings, "mappings")?;
    let properties = mappings.get("properties").ok_or_else(|| {
        CommandError::InvalidCommand("missing mappings.properties object".to_string())
    })?;
    let properties = as_object(properties, "mappings.properties")?;

    let mut fields = BTreeMap::new();
    for (field_name, field_value) in properties {
        let field_obj = as_object(field_value, &format!("field mapping '{field_name}'"))?;
        let Some(field_type_value) = field_obj.get("type") else {
            return Err(CommandError::InvalidCommand(format!(
                "field mapping '{field_name}' is missing type"
            )));
        };
        let Some(field_type_str) = field_type_value.as_str() else {
            return Err(CommandError::InvalidCommand(format!(
                "field mapping '{field_name}' type must be a string"
            )));
        };
        let field_type = parse_field_type(field_type_str)?;
        fields.insert(field_name.clone(), field_type);
    }

    Ok(fields)
}

fn parse_document_body(body: &str) -> Result<Document, CommandError> {
    let value = parse_json(body)?;
    let object = as_object(&value, "document body")?;
    Ok(object.clone())
}

/// Parses `{"columns": [{"name","type","nullable"?}...], "primary_key": [..]?}`.
fn parse_table_create_body(body: &Document) -> Result<(Vec<Column>, Vec<String>), EngineError> {
    let bad = |msg: &str| EngineError::Command(CommandError::InvalidCommand(msg.to_string()));
    let columns_value = body
        .get("columns")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("table create requires a 'columns' array"))?;
    let mut columns = Vec::with_capacity(columns_value.len());
    for column_value in columns_value {
        let object = column_value
            .as_object()
            .ok_or_else(|| bad("each column must be an object"))?;
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| bad("column missing 'name'"))?;
        let type_spec = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| bad("column missing 'type'"))?;
        let nullable = object
            .get("nullable")
            .map(|v| v.as_bool().ok_or_else(|| bad("'nullable' must be a bool")))
            .transpose()?
            .unwrap_or(true);
        let column_type = ColumnType::parse(type_spec)
            .map_err(|err| EngineError::Command(CommandError::InvalidCommand(err.0)))?;
        columns.push(Column {
            name: name.to_string(),
            column_type,
            nullable,
        });
    }
    let key_names = match body.get("primary_key") {
        None => Vec::new(),
        Some(value) => value
            .as_array()
            .ok_or_else(|| bad("'primary_key' must be an array"))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| bad("'primary_key' entries must be strings"))
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    Ok((columns, key_names))
}

/// Converts an insert body (column -> JSON value) into a full row in schema
/// order; absent columns become NULL.
fn parse_row_values(schema: &Schema, body: &Document) -> Result<Vec<Datum>, EngineError> {
    for field in body.keys() {
        if !schema.columns.iter().any(|c| &c.name == field) {
            return Err(CommandError::InvalidCommand(format!("unknown column '{field}'")).into());
        }
    }
    schema
        .columns
        .iter()
        .map(|column| {
            let value = body.get(&column.name).unwrap_or(&Value::Null);
            Datum::from_json(&column.column_type, value)
                .map_err(|err| CommandError::InvalidCommand(err.0).into())
        })
        .collect()
}

/// Parses `{"where": {"col": value}}` into a typed equality predicate.
fn parse_where(schema: &Schema, body: &Document) -> Result<(String, Datum), EngineError> {
    let bad = |msg: &str| EngineError::Command(CommandError::InvalidCommand(msg.to_string()));
    let object = body
        .get("where")
        .and_then(Value::as_object)
        .ok_or_else(|| bad("expected a 'where' object with exactly one column"))?;
    if object.len() != 1 {
        return Err(bad("'where' must contain exactly one column"));
    }
    let (name, value) = object.iter().next().expect("one entry");
    let column = schema
        .columns
        .iter()
        .find(|c| &c.name == name)
        .ok_or_else(|| bad(&format!("unknown column '{name}'")))?;
    let datum = Datum::from_json(&column.column_type, value)
        .map_err(|err| EngineError::Command(CommandError::InvalidCommand(err.0)))?;
    Ok((name.clone(), datum))
}

/// Parses `{"set": {"col": value, ...}}` into typed assignments.
fn parse_set(schema: &Schema, body: &Document) -> Result<Vec<(String, Datum)>, EngineError> {
    let bad = |msg: &str| EngineError::Command(CommandError::InvalidCommand(msg.to_string()));
    let object = body
        .get("set")
        .and_then(Value::as_object)
        .ok_or_else(|| bad("expected a 'set' object"))?;
    if object.is_empty() {
        return Err(bad("'set' must assign at least one column"));
    }
    object
        .iter()
        .map(|(name, value)| {
            let column = schema
                .columns
                .iter()
                .find(|c| &c.name == name)
                .ok_or_else(|| bad(&format!("unknown column '{name}'")))?;
            let datum = Datum::from_json(&column.column_type, value)
                .map_err(|err| EngineError::Command(CommandError::InvalidCommand(err.0)))?;
            Ok((name.clone(), datum))
        })
        .collect()
}

fn parse_search_body(body: &str) -> Result<SearchQuery, CommandError> {
    let value = parse_json(body)?;
    let root = as_object(&value, "search body")?;
    let query = root
        .get("query")
        .ok_or_else(|| CommandError::InvalidCommand("missing query object".to_string()))?;
    parse_search_query(query)
}

fn parse_search_query(value: &Value) -> Result<SearchQuery, CommandError> {
    let object = as_object(value, "query")?;

    if let Some(match_value) = object.get("match") {
        let field_map = as_object(match_value, "match")?;
        if field_map.len() != 1 {
            return Err(CommandError::InvalidCommand(
                "match query must contain exactly one field".to_string(),
            ));
        }
        let (field, query_value) = field_map.iter().next().unwrap();
        let Some(query) = query_value.as_str() else {
            return Err(CommandError::InvalidCommand(format!(
                "match query for field '{field}' must be a string"
            )));
        };
        return Ok(SearchQuery::Match {
            field: field.clone(),
            query: query.to_string(),
        });
    }

    if let Some(term_value) = object.get("term") {
        let field_map = as_object(term_value, "term")?;
        if field_map.len() != 1 {
            return Err(CommandError::InvalidCommand(
                "term query must contain exactly one field".to_string(),
            ));
        }
        let (field, value) = field_map.iter().next().unwrap();
        return Ok(SearchQuery::Term {
            field: field.clone(),
            value: value.clone(),
        });
    }

    if let Some(bool_value) = object.get("bool") {
        let bool_map = as_object(bool_value, "bool")?;
        let must = parse_query_array(bool_map.get("must"), "bool.must")?;
        let filter = parse_query_array(bool_map.get("filter"), "bool.filter")?;
        return Ok(SearchQuery::Bool { must, filter });
    }

    Err(CommandError::InvalidCommand(
        "query must contain one of: match, term, bool".to_string(),
    ))
}

fn parse_query_array(value: Option<&Value>, label: &str) -> Result<Vec<SearchQuery>, CommandError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(items) = value.as_array() else {
        return Err(CommandError::InvalidCommand(format!(
            "{label} must be an array"
        )));
    };
    items.iter().map(parse_search_query).collect()
}

fn parse_json(body: &str) -> Result<Value, CommandError> {
    serde_json::from_str(body).map_err(|err| CommandError::MalformedJson(err.to_string()))
}

fn parse_field_type(raw: &str) -> Result<FieldType, CommandError> {
    match raw {
        "text" => Ok(FieldType::Text),
        "keyword" => Ok(FieldType::Keyword),
        "float" => Ok(FieldType::Float),
        other => Err(CommandError::InvalidCommand(format!(
            "unsupported field type '{other}'"
        ))),
    }
}

fn as_object<'a>(value: &'a Value, label: &str) -> Result<&'a Document, CommandError> {
    value.as_object().ok_or_else(|| {
        CommandError::InvalidCommand(format!(
            "{label} must be a json object, got {}",
            value_type_name(value)
        ))
    })
}

fn exact_term_key(
    field: &str,
    field_type: FieldType,
    value: &Value,
) -> Result<String, EngineError> {
    match field_type {
        FieldType::Keyword => value.as_str().map(ToString::to_string).ok_or_else(|| {
            CommandError::InvalidFieldType {
                field: field.to_string(),
                expected: "string".to_string(),
                actual: value_type_name(value).to_string(),
            }
            .into()
        }),
        FieldType::Float => value
            .as_number()
            .map(|value| value.to_string())
            .ok_or_else(|| {
                CommandError::InvalidFieldType {
                    field: field.to_string(),
                    expected: "number".to_string(),
                    actual: value_type_name(value).to_string(),
                }
                .into()
            }),
        FieldType::Text => Err(CommandError::InvalidCommand(
            "term query is not supported on text fields".to_string(),
        )
        .into()),
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_ascii_lowercase())
        .collect()
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn render_json(value: &Value) -> Result<String, EngineError> {
    serde_json::to_string_pretty(value)
        .map_err(|err| EngineError::Replay(format!("failed to render json response: {err}")))
}

/// Decodes a snapshot payload: JSON (current format), falling back to
/// bincode for snapshots written by older versions. Legacy bincode snapshots
/// can only have been document-free (bincode cannot deserialize
/// `serde_json::Value`, so document-bearing ones were never loadable).
fn decode_snapshot(data: &[u8]) -> Result<EngineState, EngineError> {
    match serde_json::from_slice(data) {
        Ok(state) => Ok(state),
        Err(json_err) => bincode::deserialize(data).map_err(|bincode_err| {
            EngineError::Replay(format!(
                "failed to decode snapshot: as json: {json_err}; as legacy bincode: {bincode_err}"
            ))
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::storage::StorageOptions;

    #[test]
    fn parses_multiline_create_index_command() {
        let cmd = parse_command(
            r#"
            create index products {
              "mappings": {
                "properties": {
                  "name": { "type": "text" },
                  "category": { "type": "keyword" }
                }
              }
            }
            "#,
        )
        .expect("command should parse");

        match cmd {
            Command::CreateIndex { name, mappings } => {
                assert_eq!(name, "products");
                assert_eq!(mappings["name"], FieldType::Text);
                assert_eq!(mappings["category"], FieldType::Keyword);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn create_insert_search_and_replay() {
        let path = unique_temp_path("basic-search");
        let storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");
        let mut engine = Engine::new(storage).expect("engine create");

        engine
            .execute(
                r#"
                create index products {
                  "mappings": {
                    "properties": {
                      "name": { "type": "text" },
                      "category": { "type": "keyword" },
                      "price": { "type": "float" },
                      "description": { "type": "text" }
                    }
                  }
                }
                "#,
            )
            .expect("create index");

        engine
            .execute(
                r#"
                insert document products {
                  "name": "Red Running Shoes",
                  "category": "shoes",
                  "price": 79.99,
                  "description": "Lightweight shoes for road running"
                }
                "#,
            )
            .expect("insert first doc");

        engine
            .execute(
                r#"
                insert document products {
                  "name": "Blue Hiking Boots",
                  "category": "boots",
                  "price": 129.99,
                  "description": "Durable boots for mountain trails"
                }
                "#,
            )
            .expect("insert second doc");

        let response = engine
            .execute(
                r#"
                search products {
                  "query": {
                    "match": {
                      "description": "running shoes"
                    }
                  }
                }
                "#,
            )
            .expect("search");
        let response: Value = serde_json::from_str(&response).expect("valid json search response");
        assert_eq!(response["hits"]["total"].as_u64(), Some(1));
        assert_eq!(
            response["hits"]["hits"][0]["_source"]["name"].as_str(),
            Some("Red Running Shoes")
        );

        drop(engine);

        let storage = Storage::open(path.clone()).expect("storage reopen");
        let mut engine = Engine::new(storage).expect("engine replay");
        let response = engine
            .execute(
                r#"
                search products {
                  "query": {
                    "bool": {
                      "must": [
                        { "match": { "description": "running" } }
                      ],
                      "filter": [
                        { "term": { "category": "shoes" } }
                      ]
                    }
                  }
                }
                "#,
            )
            .expect("replayed search");
        let response: Value = serde_json::from_str(&response).expect("valid replayed search json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(1));
        assert_eq!(
            response["hits"]["hits"][0]["_source"]["name"].as_str(),
            Some("Red Running Shoes")
        );

        let _ = std::fs::remove_file(path);
    }

    /// Regression (review finding, pre-existing): snapshots holding
    /// documents could never be decoded again (bincode cannot deserialize
    /// serde_json::Value). Round-trip a checkpoint with real documents plus
    /// a post-checkpoint WAL event.
    #[test]
    fn checkpoint_with_documents_survives_restart() {
        let path = unique_temp_path("checkpoint-roundtrip");
        let storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");
        let mut engine = Engine::new(storage).expect("engine create");
        engine
            .execute(
                r#"
                create index notes {
                  "mappings": { "properties": { "body": { "type": "text" } } }
                }
                "#,
            )
            .expect("create index");
        engine
            .execute(r#"insert document notes { "body": "first snapshot doc" }"#)
            .expect("insert 1");
        engine
            .execute(r#"insert document notes { "body": "second snapshot doc" }"#)
            .expect("insert 2");
        engine.checkpoint().expect("checkpoint with documents");
        engine
            .execute(r#"insert document notes { "body": "post checkpoint doc" }"#)
            .expect("insert 3");
        drop(engine);

        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("engine restart after checkpoint");
        let response = engine
            .execute(r#"search notes { "query": { "match": { "body": "doc" } } }"#)
            .expect("search");
        let response: Value = serde_json::from_str(&response).expect("valid json");
        assert_eq!(
            response["hits"]["total"].as_u64(),
            Some(3),
            "snapshot docs and post-checkpoint doc must all survive"
        );
        // Doc-id continuity: the next insert must not collide.
        engine
            .execute(r#"insert document notes { "body": "post restart doc" }"#)
            .expect("insert after restart");
        let _ = std::fs::remove_file(path);
    }

    /// Regression (review finding): a crash between the snapshot descriptor
    /// becoming durable and the WAL head advancing leaves snapshot-covered
    /// events in the ring; replay must skip them instead of failing on
    /// duplicate applies.
    #[test]
    fn replay_skips_events_already_covered_by_snapshot() {
        let path = unique_temp_path("covered-replay");
        let mut storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");

        // Snapshot state: index "notes" with one document, next_seq_no = 3.
        let mut mappings = BTreeMap::new();
        mappings.insert("body".to_string(), FieldType::Text);
        let create_event = WalEvent::CreateIndex {
            name: "notes".to_string(),
            mappings: mappings.clone(),
        };
        let mut doc = Document::new();
        doc.insert("body".to_string(), Value::String("covered".to_string()));
        let insert_event = WalEvent::InsertDocument {
            index: "notes".to_string(),
            id: "1".to_string(),
            document: doc,
        };
        let mut state = EngineState::default();
        let mut index = IndexState::new(mappings);
        if let WalEvent::InsertDocument { id, document, .. } = &insert_event {
            index.insert_document(id, document).expect("apply insert");
        }
        state.indices.insert("notes".to_string(), index);
        let snapshot = serde_json::to_vec(&state).expect("encode state");
        storage
            .write_checkpoint(&snapshot, 2, 3, 2)
            .expect("checkpoint");

        // The crash window: events 1 and 2 (already in the snapshot) sit in
        // the ring after the checkpoint.
        for (seq, event) in [(1u64, &create_event), (2u64, &insert_event)] {
            let payload = serde_json::to_vec(event).expect("encode event");
            storage
                .append_wal_entry(
                    ENGINE_WAL_ENTRY_TYPE,
                    ENGINE_WAL_ENTRY_VERSION,
                    seq,
                    &payload,
                )
                .expect("append covered event");
        }
        drop(storage);

        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("open must skip covered events");
        let response = engine
            .execute(r#"search notes { "query": { "match": { "body": "covered" } } }"#)
            .expect("search");
        let response: Value = serde_json::from_str(&response).expect("valid json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(1));
        let _ = std::fs::remove_file(path);
    }

    /// Stage 2 exit criterion: search events and relational records share
    /// one WAL ring; a crash must recover both, each through its own
    /// mechanism, regardless of interleaving.
    #[test]
    fn mixed_search_and_relational_wal_replays_in_order() {
        let path = unique_temp_path("mixed-wal");
        let storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");
        let mut engine = Engine::new(storage).expect("engine create");

        engine
            .execute(
                r#"create index docs { "mappings": { "properties": { "body": { "type": "text" } } } }"#,
            )
            .expect("create index");
        engine
            .execute(
                r#"table create items { "columns": [ {"name":"id","type":"int","nullable":false}, {"name":"label","type":"nvarchar(50)"} ], "primary_key": ["id"] }"#,
            )
            .expect("create table");
        // Interleave the two subsystems in one ring.
        for i in 0..10 {
            engine
                .execute(&format!(
                    r#"insert document docs {{ "body": "search event {i}" }}"#
                ))
                .expect("insert doc");
            engine
                .execute(&format!(
                    r#"table insert items {{ "id": {i}, "label": "row {i}" }}"#
                ))
                .expect("insert row");
        }
        engine
            .execute(r#"table delete items { "where": { "id": 3 } }"#)
            .expect("delete row");
        drop(engine); // crash: everything lives in the shared WAL only

        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("recover both subsystems");

        let response = engine
            .execute(r#"search docs { "query": { "match": { "body": "search" } } }"#)
            .expect("search");
        let response: Value = serde_json::from_str(&response).expect("json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(10));

        let response = engine.execute(r#"table scan items {}"#).expect("scan");
        let response: Value = serde_json::from_str(&response).expect("json");
        assert_eq!(response["count"].as_u64(), Some(9));
        let ids: Vec<i64> = response["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![0, 1, 2, 4, 5, 6, 7, 8, 9], "key order, id 3 gone");

        // Both surfaces stay writable after recovery.
        engine
            .execute(
                r#"table update items { "where": { "id": 4 }, "set": { "label": "updated" } }"#,
            )
            .expect("update after recovery");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn table_scan_accepts_bodyless_form() {
        let path = unique_temp_path("scan-bodyless");
        let storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");
        let mut engine = Engine::new(storage).expect("engine create");
        engine
            .execute(
                r#"table create t { "columns": [ {"name":"id","type":"int","nullable":false} ], "primary_key": ["id"] }"#,
            )
            .expect("create table");
        engine
            .execute(r#"table insert t { "id": 1 }"#)
            .expect("insert");
        for command in ["table scan t", "table scan t {}"] {
            let response = engine.execute(command).expect("scan");
            let response: Value = serde_json::from_str(&response).expect("json");
            assert_eq!(response["count"].as_u64(), Some(1), "for '{command}'");
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn engine_replay_ignores_relational_wal_records() {
        let path = unique_temp_path("rel-coexistence");
        let mut storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");
        // Relational records land in the same ring before and between search
        // events; search replay must skip them.
        let extent = storage.allocate_extent(false).expect("extent");
        let mut engine = Engine::new(storage).expect("engine create");
        engine
            .execute(
                r#"
                create index notes {
                  "mappings": { "properties": { "body": { "type": "text" } } }
                }
                "#,
            )
            .expect("create index");
        engine
            .execute(r#"insert document notes { "body": "relational coexistence" }"#)
            .expect("insert");
        drop(engine);

        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("engine replay with rel records");
        let response = engine
            .execute(r#"search notes { "query": { "match": { "body": "coexistence" } } }"#)
            .expect("search after replay");
        let response: Value = serde_json::from_str(&response).expect("valid json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(1));
        let _ = extent;

        let _ = std::fs::remove_file(path);
    }

    fn test_storage_options() -> StorageOptions {
        StorageOptions {
            size_gib: 1,
            wal_ratio: 0.05,
            metadata_ratio: 0.08,
            snapshot_ratio: 0.02,
            allocator_ratio: 0.02,
            reserved_ratio: 0.17,
        }
    }

    fn unique_temp_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        path.push(format!("truthdb-{label}-{nanos}.db"));
        path
    }
}
