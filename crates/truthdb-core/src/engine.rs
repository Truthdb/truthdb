use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;

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
        // Routing: the legacy ES commands all carry a `{` JSON body; that
        // shape routes to the frozen search path. Everything else is SQL.
        match parse_command(input)? {
            Some(command) => self.execute_es(command),
            None => self.execute_sql(input),
        }
    }

    fn execute_es(&mut self, command: Command) -> Result<String, EngineError> {
        match command {
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
        }
    }

    /// Executes a SQL batch. Statements before an error have already
    /// committed (each is autocommit in Stage 3), so their results ride
    /// along with any error in one envelope, transported as a normal
    /// response (TDS-like) rather than failing the connection.
    fn execute_sql(&mut self, input: &str) -> Result<String, EngineError> {
        let outcome = crate::rel::execute_batch(&mut self.storage, input);
        self.maybe_checkpoint()?;
        Ok(render_sql_outcome(&outcome))
    }

    /// Runs a SQL batch and returns the typed outcome (result sets +
    /// optional error). The TDS gateway uses this to emit COLMETADATA / ROW
    /// / DONE / ERROR token streams; a TDS client only ever speaks SQL, so
    /// there is no ES routing here.
    pub fn sql_batch(&mut self, input: &str) -> Result<crate::rel::BatchOutcome, EngineError> {
        let outcome = crate::rel::execute_batch(&mut self.storage, input);
        self.maybe_checkpoint()?;
        Ok(outcome)
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

/// Parses a legacy ES command. Returns `Ok(None)` when the input is not an
/// ES command (a `{`-bodied create index / insert document / search) — the
/// caller then routes it to the SQL engine. `Ok(Some(_))` is a well-formed
/// ES command; `Err` is a malformed one.
fn parse_command(input: &str) -> Result<Option<Command>, CommandError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(CommandError::InvalidCommand(
            "command cannot be empty".to_string(),
        ));
    }

    if let Some((header, body)) = split_command(trimmed, "create index")? {
        let name = parse_single_name(header, "create index")?;
        let mappings = parse_create_index_body(body)?;
        return Ok(Some(Command::CreateIndex { name, mappings }));
    }

    if let Some((header, body)) = split_command(trimmed, "insert document")? {
        let index = parse_single_name(header, "insert document")?;
        let document = parse_document_body(body)?;
        return Ok(Some(Command::InsertDocument { index, document }));
    }

    if let Some((header, body)) = split_command(trimmed, "search")? {
        let index = parse_single_name(header, "search")?;
        let query = parse_search_body(body)?;
        return Ok(Some(Command::Search { index, query }));
    }

    // Not an ES command: route to SQL.
    Ok(None)
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

/// Renders a SQL batch outcome (statement results + an optional trailing
/// error) as the `{"kind":"sql",...}` envelope the CLI turns into aligned
/// tables, `(N rows affected)` lines, and `Msg <n>` errors.
fn render_sql_outcome(outcome: &crate::rel::BatchOutcome) -> String {
    use crate::rel::StatementResult;
    let rendered: Vec<Value> = outcome
        .results
        .iter()
        .map(|result| match result {
            StatementResult::Rows(rowset) => {
                let columns: Vec<&str> = rowset.columns.iter().map(|c| c.name.as_str()).collect();
                let rows: Vec<Value> = rowset
                    .rows
                    .iter()
                    .map(|row| {
                        Value::Array(
                            row.iter()
                                .zip(&rowset.columns)
                                .map(|(datum, column)| {
                                    match crate::rel::render_cell(datum, &column.column_type) {
                                        Some(text) => Value::String(text),
                                        None => Value::Null,
                                    }
                                })
                                .collect(),
                        )
                    })
                    .collect();
                json!({
                    "type": "rows",
                    "columns": columns,
                    "rows": rows,
                })
            }
            StatementResult::RowsAffected(n) => json!({ "type": "count", "rows_affected": n }),
            StatementResult::Done => json!({ "type": "done" }),
        })
        .collect();
    let error = outcome.error.as_ref().map(|err| {
        json!({
            "number": err.number,
            "level": err.level,
            "state": err.state,
            "message": err.message,
        })
    });
    json!({ "kind": "sql", "results": rendered, "error": error }).to_string()
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
    use std::path::{Path, PathBuf};

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
            Some(Command::CreateIndex { name, mappings }) => {
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
            .execute("CREATE TABLE items (id INT NOT NULL PRIMARY KEY, label NVARCHAR(50))")
            .expect("create table");
        // Interleave the two subsystems in one ring.
        for i in 0..10 {
            engine
                .execute(&format!(
                    r#"insert document docs {{ "body": "search event {i}" }}"#
                ))
                .expect("insert doc");
            engine
                .execute(&format!("INSERT INTO items VALUES ({i}, 'row {i}')"))
                .expect("insert row");
        }
        drop(engine); // crash: everything lives in the shared WAL only

        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("recover both subsystems");

        let response = engine
            .execute(r#"search docs { "query": { "match": { "body": "search" } } }"#)
            .expect("search");
        let response: Value = serde_json::from_str(&response).expect("json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(10));

        let ids = sql_column_i64(&mut engine, "SELECT id FROM items ORDER BY id", 0);
        assert_eq!(
            ids,
            (0..10).collect::<Vec<_>>(),
            "all rows recovered in key order"
        );

        // Both surfaces stay writable after recovery.
        engine
            .execute("INSERT INTO items VALUES (10, 'after recovery')")
            .expect("insert after recovery");
        let ids = sql_column_i64(
            &mut engine,
            "SELECT id FROM items WHERE id > 8 ORDER BY id",
            0,
        );
        assert_eq!(ids, vec![9, 10]);
        let _ = std::fs::remove_file(path);
    }

    /// Extracts one integer column from a SELECT via the SQL envelope.
    fn sql_column_i64(engine: &mut Engine, sql: &str, column: usize) -> Vec<i64> {
        let response = engine.execute(sql).expect("sql");
        let response: Value = serde_json::from_str(&response).expect("json");
        assert_eq!(
            response["kind"], "sql",
            "expected a rows envelope: {response}"
        );
        response["results"][0]["rows"]
            .as_array()
            .expect("rows array")
            .iter()
            .map(|row| row[column].as_str().expect("cell").parse().expect("i64"))
            .collect()
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

    /// Runs SQL and returns the parsed envelope.
    fn sql(engine: &mut Engine, text: &str) -> Value {
        let response = engine.execute(text).expect("execute");
        serde_json::from_str(&response).expect("json envelope")
    }

    /// Runs SQL expected to error and returns the SQL error number from the
    /// envelope's trailing `error`.
    fn sql_error_number(engine: &mut Engine, text: &str) -> i64 {
        let env = sql(engine, text);
        env["error"]["number"]
            .as_i64()
            .unwrap_or_else(|| panic!("expected an error envelope, got {env}"))
    }

    /// Runs a single-statement SELECT and returns its (columns, rows) where
    /// each cell is `Option<String>` (None = NULL).
    fn sql_rows(engine: &mut Engine, text: &str) -> (Vec<String>, Vec<Vec<Option<String>>>) {
        let env = sql(engine, text);
        assert_eq!(env["kind"], "sql", "expected rows, got {env}");
        let result = &env["results"][0];
        assert_eq!(result["type"], "rows", "expected a rowset, got {result}");
        let columns = result["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap().to_string())
            .collect();
        let rows = result["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                row.as_array()
                    .unwrap()
                    .iter()
                    .map(|cell| cell.as_str().map(str::to_string))
                    .collect()
            })
            .collect();
        (columns, rows)
    }

    fn new_engine(path: &Path) -> Engine {
        let storage = Storage::create(path.to_path_buf(), test_storage_options()).expect("create");
        Engine::new(storage).expect("engine")
    }

    #[test]
    fn sql_create_insert_select_survive_restart() {
        let path = unique_temp_path("sql-roundtrip");
        let mut engine = new_engine(&path);

        engine
            .execute(
                "CREATE TABLE products (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50), price FLOAT)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO products VALUES (1, 'Skor', 79.99), (2, 'Kangor', 129.5), (3, 'Sockar', NULL)")
            .expect("insert");

        let (columns, rows) = sql_rows(&mut engine, "SELECT id, name FROM products ORDER BY id");
        assert_eq!(columns, vec!["id", "name"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("Skor".into())],
                vec![Some("2".into()), Some("Kangor".into())],
                vec![Some("3".into()), Some("Sockar".into())],
            ]
        );
        drop(engine);

        // Restart: schema + rows recovered.
        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("engine");
        let (_, rows) = sql_rows(&mut engine, "SELECT name FROM products WHERE price IS NULL");
        assert_eq!(rows, vec![vec![Some("Sockar".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_update_and_delete_with_where() {
        let path = unique_temp_path("sql-update-delete");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, n INT, label NVARCHAR(20))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')")
            .expect("insert");

        // UPDATE a non-key column; SET expression sees the pre-update row.
        engine
            .execute("UPDATE t SET n = n + 5, label = 'x' WHERE id = 2")
            .expect("update");
        let (_, rows) = sql_rows(&mut engine, "SELECT n, label FROM t WHERE id = 2");
        assert_eq!(rows, vec![vec![Some("25".into()), Some("x".into())]]);

        // DELETE a subset.
        engine
            .execute("DELETE FROM t WHERE n < 20")
            .expect("delete");
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_update_primary_key_rekeys() {
        let path = unique_temp_path("sql-update-pk");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
            .expect("insert");
        // Move row 1 to key 5 (delete + insert under the hood).
        engine
            .execute("UPDATE t SET id = 5 WHERE id = 1")
            .expect("update");
        let (_, rows) = sql_rows(&mut engine, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Some("2".into()), Some("200".into())],
                vec![Some("5".into()), Some("100".into())],
            ]
        );
        // Re-keying onto an existing key collides (2627).
        assert_eq!(
            sql_error_number(&mut engine, "UPDATE t SET id = 2 WHERE id = 5"),
            2627
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_delete_all_and_update_null_violation() {
        let path = unique_temp_path("sql-del-all");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, n INT NOT NULL)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .expect("insert");
        // Updating a NOT NULL column to NULL is 515.
        assert_eq!(
            sql_error_number(&mut engine, "UPDATE t SET n = NULL WHERE id = 1"),
            515
        );
        // DELETE with no WHERE clears the table.
        engine.execute("DELETE FROM t").expect("delete all");
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t");
        assert!(rows.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_default_values_applied() {
        let path = unique_temp_path("sql-default");
        let mut engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                 n INT NOT NULL DEFAULT 7, label NVARCHAR(10) DEFAULT 'none')",
            )
            .expect("create");
        // Omit the defaulted columns.
        engine
            .execute("INSERT INTO t (id) VALUES (1)")
            .expect("insert");
        // An explicit NULL into a nullable column is kept (not defaulted).
        engine
            .execute("INSERT INTO t (id, label) VALUES (2, NULL)")
            .expect("insert2");
        let (_, rows) = sql_rows(&mut engine, "SELECT id, n, label FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("7".into()), Some("none".into())],
                vec![Some("2".into()), Some("7".into()), None],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_identity_assigns_and_survives_restart() {
        let path = unique_temp_path("sql-identity");
        let mut engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY IDENTITY(1,1), name NVARCHAR(10))",
            )
            .expect("create");
        engine
            .execute("INSERT INTO t (name) VALUES ('a')")
            .expect("i1");
        engine
            .execute("INSERT INTO t (name) VALUES ('b'), ('c')")
            .expect("i2");
        // Deleting the max row must not let its identity be reused.
        engine.execute("DELETE FROM t WHERE id = 3").expect("del");
        engine
            .execute("INSERT INTO t (name) VALUES ('d')")
            .expect("i3");
        let (_, rows) = sql_rows(&mut engine, "SELECT id, name FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("a".into())],
                vec![Some("2".into()), Some("b".into())],
                vec![Some("4".into()), Some("d".into())],
            ]
        );
        // Providing an explicit value for an identity column is rejected.
        assert_eq!(
            sql_error_number(&mut engine, "INSERT INTO t (id, name) VALUES (9, 'z')"),
            8101
        );
        // Identity cannot be updated.
        assert_eq!(
            sql_error_number(&mut engine, "UPDATE t SET id = 100 WHERE id = 1"),
            8102
        );
        drop(engine);

        // Restart: the counter continues from 5, never reusing 3.
        let storage = Storage::open(path.clone()).expect("reopen");
        let mut engine = Engine::new(storage).expect("engine");
        engine
            .execute("INSERT INTO t (name) VALUES ('e')")
            .expect("i4");
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t WHERE name = 'e'");
        assert_eq!(rows, vec![vec![Some("5".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_decimal_arithmetic_and_rendering() {
        let path = unique_temp_path("sql-decimal");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, price DECIMAL(10,2))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 12.50), (2, 3.30)")
            .expect("insert");
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT price, price * 2 AS dbl, price + 0.05 AS bump FROM t ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![
                vec![
                    Some("12.50".into()),
                    Some("25.00".into()),
                    Some("12.55".into())
                ],
                vec![
                    Some("3.30".into()),
                    Some("6.60".into()),
                    Some("3.35".into())
                ],
            ]
        );
        // Division derives scale = max(6, ...) per SQL Server.
        let (_, rows) = sql_rows(&mut engine, "SELECT price / 3 FROM t WHERE id = 1");
        assert_eq!(rows, vec![vec![Some("4.166667".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_temporal_types_round_trip() {
        let path = unique_temp_path("sql-temporal");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME2)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, '2020-06-15', '2020-06-15 13:45:30.5')")
            .expect("insert");
        let (_, rows) = sql_rows(&mut engine, "SELECT d, dt FROM t");
        assert_eq!(
            rows,
            vec![vec![
                Some("2020-06-15".into()),
                Some("2020-06-15 13:45:30.5000000".into())
            ]]
        );
        // A character literal implicitly converts to DATE for the comparison.
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t WHERE d = '2020-06-15'");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_expression_operators() {
        let path = unique_temp_path("sql-expr-ops");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20), score INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', NULL), (3, 'Carol', 70)")
            .expect("insert");

        // LIKE + IN + BETWEEN combine in a WHERE.
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT id FROM t WHERE name LIKE 'A%' OR id IN (3) OR score BETWEEN 85 AND 95 ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("3".into())]]);

        // CASE (searched) + ISNULL + a scalar function.
        let (cols, rows) = sql_rows(
            &mut engine,
            "SELECT UPPER(name) AS u, ISNULL(score, 0) AS s, \
             CASE WHEN score >= 85 THEN 'hi' WHEN score IS NULL THEN 'none' ELSE 'lo' END AS grade \
             FROM t ORDER BY id",
        );
        assert_eq!(cols, vec!["u", "s", "grade"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("ALICE".into()), Some("90".into()), Some("hi".into())],
                vec![Some("BOB".into()), Some("0".into()), Some("none".into())],
                vec![Some("CAROL".into()), Some("70".into()), Some("lo".into())],
            ]
        );

        // CAST and NOT LIKE.
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT CAST(score AS NVARCHAR(10)) FROM t WHERE id = 1",
        );
        assert_eq!(rows, vec![vec![Some("90".into())]]);
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT id FROM t WHERE name NOT LIKE '%o%' ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_swedish_collation_order_by() {
        let path = unique_temp_path("sql-collation");
        let mut engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                 w NVARCHAR(20) COLLATE Finnish_Swedish_CI_AS)",
            )
            .expect("create");
        engine
            .execute(
                "INSERT INTO t VALUES (1, 'öl'), (2, 'apa'), (3, 'åre'), \
                 (4, 'zebra'), (5, 'ängel'), (6, 'björn')",
            )
            .expect("insert");
        // Swedish sorts å, ä, ö after z: apa, björn, zebra, åre, ängel, öl.
        let (_, rows) = sql_rows(&mut engine, "SELECT w FROM t ORDER BY w");
        let order: Vec<String> = rows.into_iter().map(|r| r[0].clone().unwrap()).collect();
        assert_eq!(order, vec!["apa", "björn", "zebra", "åre", "ängel", "öl"]);
        // The collation is surfaced in sys.columns.
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT collation_name FROM sys.columns WHERE name = 'w'",
        );
        assert_eq!(rows, vec![vec![Some("Finnish_Swedish_CI_AS".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_stage5_review_fixes() {
        let path = unique_temp_path("sql-review-fixes");
        let mut engine = new_engine(&path);
        // CAST decimal/float to int truncates toward zero (not rounds).
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT CAST(10.6496 AS INT), CAST(2.9 AS INT), CAST(-10.6496 AS INT)",
        );
        assert_eq!(
            rows,
            vec![vec![
                Some("10".into()),
                Some("2".into()),
                Some("-10".into())
            ]]
        );
        // REPLICATE with a huge count is bounded (no panic / mutex-poison DoS).
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT LEN(REPLICATE('abc', 9223372036854775807)) AS n",
        );
        assert_eq!(rows, vec![vec![Some("999999".into())]]);
        // A mixed int/decimal computed column infers enough precision (no 220).
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1), (2)")
            .expect("insert");
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT CASE WHEN id = 1 THEN 100000 ELSE 0.5 END AS v FROM t ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![vec![Some("100000.0".into())], vec![Some("0.5".into())]]
        );
        // UPDATE with a duplicated SET column is rejected (264).
        assert_eq!(
            sql_error_number(&mut engine, "UPDATE t SET id = 3, id = 4 WHERE id = 1"),
            264
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_duplicate_pk_reports_error_2627() {
        let path = unique_temp_path("sql-pk-dup");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        assert_eq!(
            sql_error_number(&mut engine, "INSERT INTO t VALUES (1)"),
            2627
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_where_order_top_projection() {
        let path = unique_temp_path("sql-select");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE nums (n INT NOT NULL PRIMARY KEY, label NVARCHAR(10))")
            .expect("create");
        for n in 1..=10 {
            engine
                .execute(&format!("INSERT INTO nums VALUES ({n}, 'r{n}')"))
                .expect("insert");
        }
        // WHERE + ORDER DESC + TOP + computed projection.
        let (columns, rows) = sql_rows(
            &mut engine,
            "SELECT TOP 3 n, n * 10 AS ten FROM nums WHERE n > 4 ORDER BY n DESC",
        );
        assert_eq!(columns, vec!["n", "ten"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("10".into()), Some("100".into())],
                vec![Some("9".into()), Some("90".into())],
                vec![Some("8".into()), Some("80".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_bare_column_alias_is_preserved() {
        let path = unique_temp_path("sql-alias");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE nums (n INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine
            .execute("INSERT INTO nums VALUES (1)")
            .expect("insert");
        // A bare column with an alias must report the alias, not the source
        // column name (regression guard for the typed-projection refactor).
        let (columns, rows) = sql_rows(&mut engine, "SELECT n AS foo FROM nums");
        assert_eq!(columns, vec!["foo"]);
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_three_valued_where_keeps_only_true_rows() {
        let path = unique_temp_path("sql-3vl");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)")
            .expect("insert");
        // v <> 10 is UNKNOWN for the NULL row, which is filtered out.
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t WHERE v <> 10 ORDER BY id");
        assert_eq!(rows, vec![vec![Some("3".into())]]);
        // IS NULL is two-valued.
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t WHERE v IS NULL");
        assert_eq!(rows, vec![vec![Some("2".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_sys_catalog_is_queryable() {
        let path = unique_temp_path("sql-syscat");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE alpha (id INT PRIMARY KEY, name NVARCHAR(20))")
            .expect("create alpha");
        engine
            .execute("CREATE TABLE beta (x BIGINT NOT NULL)")
            .expect("create beta");
        let (_, rows) = sql_rows(&mut engine, "SELECT name FROM sys.tables ORDER BY name");
        assert_eq!(
            rows,
            vec![vec![Some("alpha".into())], vec![Some("beta".into())]]
        );
        // sys.columns: alpha has two columns.
        let (_, rows) = sql_rows(
            &mut engine,
            "SELECT name, type FROM sys.columns WHERE object_id = 2 ORDER BY column_id",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("id".into()), Some("int".into())],
                vec![Some("name".into()), Some("nvarchar(20)".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_drop_table_and_errors() {
        let path = unique_temp_path("sql-drop");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .expect("create");
        // Selecting a missing table -> 208.
        assert_eq!(sql_error_number(&mut engine, "SELECT * FROM nope"), 208);
        // Duplicate CREATE -> 2714.
        assert_eq!(
            sql_error_number(&mut engine, "CREATE TABLE t (id INT)"),
            2714
        );
        // DROP then it's gone; DROP IF EXISTS is a no-op; bare DROP -> 3701.
        engine.execute("DROP TABLE t").expect("drop");
        assert_eq!(sql_error_number(&mut engine, "SELECT * FROM t"), 208);
        engine
            .execute("DROP TABLE IF EXISTS t")
            .expect("drop if exists");
        assert_eq!(sql_error_number(&mut engine, "DROP TABLE t"), 3701);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_not_null_violation_reports_515() {
        let path = unique_temp_path("sql-notnull");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(10) NOT NULL)")
            .expect("create");
        assert_eq!(
            sql_error_number(&mut engine, "INSERT INTO t (id) VALUES (1)"),
            515
        );
        // String too long -> 8152.
        assert_eq!(
            sql_error_number(
                &mut engine,
                "INSERT INTO t VALUES (1, 'this is far too long')"
            ),
            8152
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_and_search_share_the_engine() {
        // The SQL front door must not disturb the frozen ES surface.
        let path = unique_temp_path("sql-es-coexist");
        let mut engine = new_engine(&path);
        engine
            .execute(r#"create index docs { "mappings": { "properties": { "body": { "type": "text" } } } }"#)
            .expect("create index");
        engine
            .execute(r#"insert document docs { "body": "hello world" }"#)
            .expect("insert doc");
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .expect("create table");
        engine
            .execute("INSERT INTO t VALUES (42)")
            .expect("insert row");

        let search = sql(
            &mut engine,
            r#"search docs { "query": { "match": { "body": "hello" } } }"#,
        );
        assert_eq!(search["hits"]["total"], 1);
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t");
        assert_eq!(rows, vec![vec![Some("42".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_bit_column_compares_to_integer_literal() {
        let path = unique_temp_path("sql-bit-cmp");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, active BIT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
            .expect("insert");
        // `active = 1` (BIT vs int) must work, not clash.
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t WHERE active = 1 ORDER BY id");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_multi_row_insert_is_atomic() {
        let path = unique_temp_path("sql-insert-atomic");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (5)").expect("seed");
        // The 3rd row duplicates PK 5: the whole INSERT must roll back, so
        // rows 10 and 11 must NOT be present.
        assert_eq!(
            sql_error_number(&mut engine, "INSERT INTO t VALUES (10), (11), (5)"),
            2627
        );
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows, vec![vec![Some("5".into())]], "no partial rows");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_batch_keeps_earlier_results_before_an_error() {
        let path = unique_temp_path("sql-batch-partial");
        let mut engine = new_engine(&path);
        // One batch: a good CREATE + INSERT, then a failing INSERT.
        let env = sql(
            &mut engine,
            "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);",
        );
        assert_eq!(env["kind"], "sql");
        // Two statements succeeded (done, count) before the error.
        assert_eq!(env["results"].as_array().unwrap().len(), 2);
        assert_eq!(env["results"][1]["rows_affected"], 1);
        assert_eq!(env["error"]["number"], 2627);
        // The first row is durably present.
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM t");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_non_boolean_where_is_rejected_4145() {
        let path = unique_temp_path("sql-where-4145");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        // `WHERE id + 1` is numeric, not boolean.
        assert_eq!(
            sql_error_number(&mut engine, "SELECT id FROM t WHERE id + 1"),
            4145
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_schema_qualified_names_resolve() {
        let path = unique_temp_path("sql-dbo");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE dbo.products (id INT NOT NULL PRIMARY KEY)")
            .expect("create dbo.");
        engine
            .execute("INSERT INTO dbo.products VALUES (1)")
            .expect("insert dbo.");
        // Reachable by both qualified and bare names.
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM products");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let (_, rows) = sql_rows(&mut engine, "SELECT id FROM dbo.products");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_unicode_round_trips_through_insert_and_select() {
        let path = unique_temp_path("sql-unicode");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'café åäö 😀')")
            .expect("insert");
        let (_, rows) = sql_rows(&mut engine, "SELECT name FROM t");
        assert_eq!(rows, vec![vec![Some("café åäö 😀".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_bigint_overflow_literal_errors_not_saturates() {
        let path = unique_temp_path("sql-bigint-of");
        let mut engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, big BIGINT)")
            .expect("create");
        // 1e30 overflows i64; must error, not silently saturate.
        assert_eq!(
            sql_error_number(
                &mut engine,
                "INSERT INTO t VALUES (1, 1000000000000000000000000000000)"
            ),
            220
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_table_level_pk_column_is_not_null() {
        let path = unique_temp_path("sql-tablepk");
        let mut engine = new_engine(&path);
        // A table-level PK on a column with no explicit nullability succeeds
        // (the column is promoted to NOT NULL).
        engine
            .execute("CREATE TABLE t (id INT, v NVARCHAR(10), PRIMARY KEY (id))")
            .expect("create");
        // Inserting NULL into the PK column is then a NOT NULL violation.
        assert_eq!(
            sql_error_number(&mut engine, "INSERT INTO t (v) VALUES ('x')"),
            515
        );
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
