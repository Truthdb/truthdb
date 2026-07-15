use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::storage::{Storage, StorageError};

const ENGINE_WAL_ENTRY_VERSION: u16 = 1;
const ENGINE_WAL_ENTRY_TYPE: u16 = 1;
const WAL_CHECKPOINT_THRESHOLD: f64 = 0.75;

type Document = Map<String, Value>;

/// The engine's search-subsystem state, mutated only on the native path
/// ([`Engine::execute`]) and read by the checkpointer. Guarded by
/// [`Engine::meta`], a `RwLock` that doubles as the execution gate that keeps
/// the two paths from observing each other's torn state (see [`Engine`]).
struct EngineMeta {
    state: EngineState,
    next_seq_no: u64,
    next_doc_id: u64,
}

/// The database engine, shared across the worker pool as `Arc<Engine>`.
///
/// All methods take `&self`: [`Storage`] is internally synchronized, and the
/// search-subsystem state lives behind `meta`. `meta` also serves as the
/// **execution gate** decoupling the two execution paths, which do not share a
/// lock manager: a relational batch ([`Self::sql_batch_with_params`]) holds
/// `meta.read()` for its whole run (many run concurrently), while a native
/// command ([`Self::execute`], which bypasses table locks) takes `meta.write()`
/// and so runs exclusively. Without this, a concurrent native batch could read
/// a relational batch's half-applied writes — which the old single-threaded
/// actor prevented for free.
pub struct Engine {
    storage: Storage,
    meta: RwLock<EngineMeta>,
}

impl Engine {
    pub fn new(storage: Storage) -> Result<Self, EngineError> {
        let mut meta = EngineMeta {
            state: EngineState::default(),
            next_seq_no: 1,
            next_doc_id: 1,
        };

        // Try to load a snapshot first
        if let Some(snapshot) = storage.load_snapshot()? {
            meta.state = decode_snapshot(&snapshot.data)?;
            meta.next_seq_no = snapshot.next_seq_no;
            meta.next_doc_id = snapshot.next_doc_id;
            // Rebuild postings (not serialized)
            for index_state in meta.state.indices.values_mut() {
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
        let records = storage.replay_wal_entries()?;
        for record in records {
            if record.entry_type != ENGINE_WAL_ENTRY_TYPE || record.seq_no < meta.next_seq_no {
                continue;
            }
            let event: WalEvent = serde_json::from_slice(&record.payload)
                .map_err(|err| EngineError::Replay(format!("failed to decode wal event: {err}")))?;
            meta.apply_event(&event)?;
            meta.next_seq_no = record.seq_no.saturating_add(1);
        }

        Ok(Engine {
            storage,
            meta: RwLock::new(meta),
        })
    }

    pub fn execute(&self, input: &str) -> Result<String, EngineError> {
        // Native path: exclusive on the execution gate (see [`Engine`]), which
        // also gives the search state the `&mut` it needs.
        let mut meta = self.meta.write().expect("engine meta poisoned");
        // Routing: the legacy ES commands all carry a `{` JSON body; that
        // shape routes to the frozen search path. Everything else is SQL.
        match parse_command(input)? {
            Some(command) => self.execute_es(&mut meta, command),
            None => self.execute_sql(&mut meta, input),
        }
    }

    fn execute_es(&self, meta: &mut EngineMeta, command: Command) -> Result<String, EngineError> {
        match command {
            Command::CreateIndex { name, mappings } => {
                meta.validate_create_index(&name, &mappings)?;
                let event = WalEvent::CreateIndex {
                    name: name.clone(),
                    mappings: mappings.clone(),
                };
                self.persist_event(meta, &event)?;
                meta.apply_event(&event)?;
                self.maybe_checkpoint(meta)?;
                render_json(&json!({
                    "acknowledged": true,
                    "index": name,
                }))
            }
            Command::InsertDocument { index, document } => {
                meta.validate_insert_document(&index, &document)?;
                let doc_id = meta.next_doc_id.to_string();
                let event = WalEvent::InsertDocument {
                    index: index.clone(),
                    id: doc_id.clone(),
                    document: document.clone(),
                };
                self.persist_event(meta, &event)?;
                meta.apply_event(&event)?;
                self.maybe_checkpoint(meta)?;
                render_json(&json!({
                    "_id": doc_id,
                    "_index": index,
                    "result": "created",
                }))
            }
            Command::Search { index, query } => {
                let index_state = meta.state.indices.get(&index).ok_or_else(|| {
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
    fn execute_sql(&self, meta: &mut EngineMeta, input: &str) -> Result<String, EngineError> {
        // The native (session-less) path has nowhere to carry an open
        // transaction across calls, so it uses a transient context and rolls
        // back anything an unbalanced BEGIN leaves dangling.
        let mut txn_ctx = crate::rel::TxnContext::default();
        let outcome = crate::rel::execute_batch(&self.storage, input, &mut txn_ctx);
        txn_ctx.abort(&self.storage);
        self.maybe_checkpoint(meta)?;
        Ok(render_sql_outcome(&outcome))
    }

    /// Runs a SQL batch and returns the typed outcome (result sets +
    /// optional error). The TDS gateway uses this to emit COLMETADATA / ROW
    /// / DONE / ERROR token streams; a TDS client only ever speaks SQL, so
    /// there is no ES routing here. The `txn_ctx` carries transaction state
    /// (open transaction, `@@TRANCOUNT`, isolation) across batches within a
    /// session.
    pub fn sql_batch(
        &self,
        input: &str,
        txn_ctx: &mut crate::rel::TxnContext,
    ) -> Result<crate::rel::BatchOutcome, EngineError> {
        self.sql_batch_with_params(input, txn_ctx, &[])
    }

    /// Runs a SQL batch with `sp_executesql` parameters seeded as batch
    /// variables (see [`crate::rel::execute_batch_with_params`]).
    pub fn sql_batch_with_params(
        &self,
        input: &str,
        txn_ctx: &mut crate::rel::TxnContext,
        params: &[crate::rel::RpcParam],
    ) -> Result<crate::rel::BatchOutcome, EngineError> {
        // Hold the execution gate shared for the whole batch: concurrent
        // relational batches run together, but a native writer is excluded (see
        // [`Engine`]). The guard also gives the checkpointer its `meta` read.
        let meta = self.meta.read().expect("engine meta poisoned");
        let outcome = crate::rel::execute_batch_with_params(&self.storage, input, txn_ctx, params);
        self.maybe_checkpoint(&meta)?;
        Ok(outcome)
    }

    /// Rolls back and discards a session's open transaction (connection
    /// teardown). No-op when the session has no transaction.
    pub fn abort_session_txn(&self, txn_ctx: &mut crate::rel::TxnContext) {
        txn_ctx.abort(&self.storage);
    }

    /// Rolls back a transaction the idle reaper is reclaiming. Unlike
    /// [`Self::abort_session_txn`] the session lives on, so the rollback is
    /// recorded and reported to its next batch.
    pub fn abort_idle_session_txn(&self, txn_ctx: &mut crate::rel::TxnContext) {
        txn_ctx.abort_idle(&self.storage);
    }

    /// The table/database locks a SQL batch needs at the given isolation
    /// level (see [`crate::rel::analyze_locks`]). The session loop acquires
    /// these before running the batch.
    pub fn analyze_locks(
        &self,
        input: &str,
        isolation: crate::rel::Isolation,
    ) -> Vec<(crate::lock::Resource, crate::lock::LockMode)> {
        crate::rel::analyze_locks(&self.storage, input, isolation)
    }

    pub fn checkpoint(&self) -> Result<(), EngineError> {
        let meta = self.meta.read().expect("engine meta poisoned");
        self.checkpoint_locked(&meta)
    }

    fn checkpoint_locked(&self, meta: &EngineMeta) -> Result<(), EngineError> {
        // JSON, not bincode: documents hold serde_json::Value, which bincode
        // can serialize but never deserialize (`deserialize_any`), so bincode
        // snapshots with documents could not be loaded back.
        let data = serde_json::to_vec(&meta.state)
            .map_err(|err| EngineError::Replay(format!("failed to serialize state: {err}")))?;
        let checkpoint_seq = meta.next_seq_no.saturating_sub(1);
        self.storage
            .write_checkpoint(&data, checkpoint_seq, meta.next_seq_no, meta.next_doc_id)?;
        Ok(())
    }

    pub fn wal_usage_ratio(&self) -> f64 {
        self.storage.wal_usage_ratio()
    }

    fn maybe_checkpoint(&self, meta: &EngineMeta) -> Result<(), EngineError> {
        // A (fuzzy) checkpoint flushes dirty pages and truncates the WAL head to
        // the oldest open transaction's begin LSN, so it may run with open
        // transactions (their undo survives). The decision is (re-)made under the
        // storage lock in `checkpoint_if_wal_full`; this bare pre-check just
        // avoids serializing state on every batch below the WAL threshold.
        if self.wal_usage_ratio() < WAL_CHECKPOINT_THRESHOLD {
            return Ok(());
        }
        let data = serde_json::to_vec(&meta.state)
            .map_err(|err| EngineError::Replay(format!("failed to serialize state: {err}")))?;
        let checkpoint_seq = meta.next_seq_no.saturating_sub(1);
        self.storage.checkpoint_if_wal_full(
            &data,
            checkpoint_seq,
            meta.next_seq_no,
            meta.next_doc_id,
            WAL_CHECKPOINT_THRESHOLD,
        )?;
        Ok(())
    }

    fn persist_event(&self, meta: &mut EngineMeta, event: &WalEvent) -> Result<(), EngineError> {
        let payload = serde_json::to_vec(event)
            .map_err(|err| EngineError::Replay(format!("failed to encode wal event: {err}")))?;
        let seq_no = meta.next_seq_no;
        self.storage.append_wal_entry(
            ENGINE_WAL_ENTRY_TYPE,
            ENGINE_WAL_ENTRY_VERSION,
            seq_no,
            &payload,
        )?;
        meta.next_seq_no = meta.next_seq_no.saturating_add(1);
        Ok(())
    }
}

impl EngineMeta {
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

    #[error("engine is shutting down")]
    Unavailable,
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
        let engine = Engine::new(storage).expect("engine create");

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
        let engine = Engine::new(storage).expect("engine replay");
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
        let engine = Engine::new(storage).expect("engine create");
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
        let engine = Engine::new(storage).expect("engine restart after checkpoint");
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
        let engine = Engine::new(storage).expect("open must skip covered events");
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
        let engine = Engine::new(storage).expect("engine create");

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
        let engine = Engine::new(storage).expect("recover both subsystems");

        let response = engine
            .execute(r#"search docs { "query": { "match": { "body": "search" } } }"#)
            .expect("search");
        let response: Value = serde_json::from_str(&response).expect("json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(10));

        let ids = sql_column_i64(&engine, "SELECT id FROM items ORDER BY id", 0);
        assert_eq!(
            ids,
            (0..10).collect::<Vec<_>>(),
            "all rows recovered in key order"
        );

        // Both surfaces stay writable after recovery.
        engine
            .execute("INSERT INTO items VALUES (10, 'after recovery')")
            .expect("insert after recovery");
        let ids = sql_column_i64(&engine, "SELECT id FROM items WHERE id > 8 ORDER BY id", 0);
        assert_eq!(ids, vec![9, 10]);
        let _ = std::fs::remove_file(path);
    }

    /// Extracts one integer column from a SELECT via the SQL envelope.
    fn sql_column_i64(engine: &Engine, sql: &str, column: usize) -> Vec<i64> {
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
        let engine = Engine::new(storage).expect("engine create");
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
        let engine = Engine::new(storage).expect("engine replay with rel records");
        let response = engine
            .execute(r#"search notes { "query": { "match": { "body": "coexistence" } } }"#)
            .expect("search after replay");
        let response: Value = serde_json::from_str(&response).expect("valid json");
        assert_eq!(response["hits"]["total"].as_u64(), Some(1));
        let _ = extent;

        let _ = std::fs::remove_file(path);
    }

    /// Runs SQL and returns the parsed envelope.
    fn sql(engine: &Engine, text: &str) -> Value {
        let response = engine.execute(text).expect("execute");
        serde_json::from_str(&response).expect("json envelope")
    }

    /// Runs SQL expected to error and returns the SQL error number from the
    /// envelope's trailing `error`.
    fn sql_error_number(engine: &Engine, text: &str) -> i64 {
        let env = sql(engine, text);
        env["error"]["number"]
            .as_i64()
            .unwrap_or_else(|| panic!("expected an error envelope, got {env}"))
    }

    /// Runs a single-statement SELECT and returns its (columns, rows) where
    /// each cell is `Option<String>` (None = NULL).
    fn sql_rows(engine: &Engine, text: &str) -> (Vec<String>, Vec<Vec<Option<String>>>) {
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

    /// A table's catalog object id (via `sys.tables`).
    fn table_object_id(engine: &Engine, name: &str) -> u32 {
        let (_, rows) = sql_rows(
            engine,
            &format!("SELECT object_id FROM sys.tables WHERE name = '{name}'"),
        );
        rows[0][0]
            .as_ref()
            .expect("object_id")
            .parse()
            .expect("u32")
    }

    #[test]
    fn sql_create_insert_select_survive_restart() {
        let path = unique_temp_path("sql-roundtrip");
        let engine = new_engine(&path);

        engine
            .execute(
                "CREATE TABLE products (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50), price FLOAT)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO products VALUES (1, 'Skor', 79.99), (2, 'Kangor', 129.5), (3, 'Sockar', NULL)")
            .expect("insert");

        let (columns, rows) = sql_rows(&engine, "SELECT id, name FROM products ORDER BY id");
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
        let engine = Engine::new(storage).expect("engine");
        let (_, rows) = sql_rows(&engine, "SELECT name FROM products WHERE price IS NULL");
        assert_eq!(rows, vec![vec![Some("Sockar".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_update_and_delete_with_where() {
        let path = unique_temp_path("sql-update-delete");
        let engine = new_engine(&path);
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
        let (_, rows) = sql_rows(&engine, "SELECT n, label FROM t WHERE id = 2");
        assert_eq!(rows, vec![vec![Some("25".into()), Some("x".into())]]);

        // DELETE a subset.
        engine
            .execute("DELETE FROM t WHERE n < 20")
            .expect("delete");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_update_primary_key_rekeys() {
        let path = unique_temp_path("sql-update-pk");
        let engine = new_engine(&path);
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
        let (_, rows) = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Some("2".into()), Some("200".into())],
                vec![Some("5".into()), Some("100".into())],
            ]
        );
        // Re-keying onto an existing key collides (2627).
        assert_eq!(
            sql_error_number(&engine, "UPDATE t SET id = 2 WHERE id = 5"),
            2627
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_delete_all_and_update_null_violation() {
        let path = unique_temp_path("sql-del-all");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, n INT NOT NULL)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .expect("insert");
        // Updating a NOT NULL column to NULL is 515.
        assert_eq!(
            sql_error_number(&engine, "UPDATE t SET n = NULL WHERE id = 1"),
            515
        );
        // DELETE with no WHERE clears the table.
        engine.execute("DELETE FROM t").expect("delete all");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t");
        assert!(rows.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_default_values_applied() {
        let path = unique_temp_path("sql-default");
        let engine = new_engine(&path);
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
        let (_, rows) = sql_rows(&engine, "SELECT id, n, label FROM t ORDER BY id");
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
        let engine = new_engine(&path);
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
        let (_, rows) = sql_rows(&engine, "SELECT id, name FROM t ORDER BY id");
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
            sql_error_number(&engine, "INSERT INTO t (id, name) VALUES (9, 'z')"),
            8101
        );
        // Identity cannot be updated.
        assert_eq!(
            sql_error_number(&engine, "UPDATE t SET id = 100 WHERE id = 1"),
            8102
        );
        drop(engine);

        // Restart: the counter continues from 5, never reusing 3.
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine");
        engine
            .execute("INSERT INTO t (name) VALUES ('e')")
            .expect("i4");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'e'");
        assert_eq!(rows, vec![vec![Some("5".into())]]);
        let _ = std::fs::remove_file(path);
    }

    /// Runs SQL expected to error and returns the SQL error message.
    fn sql_error_message(engine: &Engine, text: &str) -> String {
        let env = sql(engine, text);
        env["error"]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("expected an error envelope, got {env}"))
            .to_string()
    }

    #[test]
    fn sql_check_constraints_enforced_on_insert_and_update() {
        let path = unique_temp_path("sql-check");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE items (\
                   id INT NOT NULL PRIMARY KEY, \
                   qty INT CHECK (qty >= 0), \
                   price INT, \
                   CONSTRAINT ck_price CHECK ((price - qty) > 0))",
            )
            .expect("create");

        // A row satisfying both checks inserts.
        engine
            .execute("INSERT INTO items VALUES (1, 5, 10)")
            .expect("insert ok");

        // Column check violation (qty < 0) → 547.
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO items VALUES (2, -1, 10)"),
            547
        );
        // Named table check violation (price <= qty) → 547, name in message.
        let msg = sql_error_message(&engine, "INSERT INTO items VALUES (3, 5, 5)");
        assert!(
            msg.contains("ck_price"),
            "message should name the constraint: {msg}"
        );

        // A NULL in a checked column yields UNKNOWN, which passes.
        engine
            .execute("INSERT INTO items VALUES (4, NULL, 10)")
            .expect("null qty passes check");

        // UPDATE is checked against the new row.
        assert_eq!(
            sql_error_number(&engine, "UPDATE items SET qty = -3 WHERE id = 1"),
            547
        );
        engine
            .execute("UPDATE items SET qty = 2 WHERE id = 1")
            .expect("update ok");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM items ORDER BY id");
        assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("4".into())],]);

        // The constraint survives a restart and still fires.
        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO items VALUES (5, -9, 10)"),
            547
        );
        // sys.check_constraints lists both (the auto-named column check and the
        // explicitly named table check).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT name FROM sys.check_constraints ORDER BY name",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("CK__items__1".into())],
                vec![Some("ck_price".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_check_constraint_rejects_unknown_column_and_duplicate_name() {
        let path = unique_temp_path("sql-check-invalid");
        let engine = new_engine(&path);
        // A CHECK referencing a non-existent column is rejected at CREATE (207).
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, CHECK (missing > 0))",
            ),
            207
        );
        // Two constraints with the same explicit name collide (2714).
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                   CONSTRAINT c CHECK (id > 0), CONSTRAINT c CHECK (id < 100))",
            ),
            2714
        );
        // A multi-part (qualified) identifier in a CHECK is rejected at CREATE
        // (4104) rather than producing a table that rejects every INSERT.
        assert_eq!(
            sql_error_number(&engine, "CREATE TABLE t (col INT, CHECK (t.col > 0))",),
            4104
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_insert_select_copies_rows() {
        let path = unique_temp_path("sql-insert-select");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE src (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20), keep BIT)")
            .expect("create src");
        engine
            .execute("INSERT INTO src VALUES (1, 'a', 1), (2, 'b', 0), (3, 'c', 1)")
            .expect("seed src");
        // Target has an IDENTITY and a DEFAULT; the SELECT feeds the two named
        // columns and the rest are server-generated / defaulted.
        engine
            .execute(
                "CREATE TABLE dst (rid INT NOT NULL PRIMARY KEY IDENTITY(1,1), \
                   id INT, label NVARCHAR(20), note NVARCHAR(10) DEFAULT 'copied')",
            )
            .expect("create dst");
        engine
            .execute(
                "INSERT INTO dst (id, label) SELECT id, name FROM src WHERE keep = 1 ORDER BY id",
            )
            .expect("insert select");
        let (_, rows) = sql_rows(&engine, "SELECT rid, id, label, note FROM dst ORDER BY rid");
        assert_eq!(
            rows,
            vec![
                vec![
                    Some("1".into()),
                    Some("1".into()),
                    Some("a".into()),
                    Some("copied".into())
                ],
                vec![
                    Some("2".into()),
                    Some("3".into()),
                    Some("c".into()),
                    Some("copied".into())
                ],
            ]
        );

        // Column-count mismatch between SELECT list and insert list.
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO dst (id) SELECT id, name FROM src"),
            121
        );
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO dst (id, label) SELECT id FROM src"),
            120
        );

        // Self-insert is Halloween-safe: the SELECT is fully materialized
        // before any row is inserted, so it doubles the table exactly once.
        engine
            .execute("INSERT INTO dst (id, label) SELECT id, label FROM dst")
            .expect("self insert select");
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM dst");
        assert_eq!(rows, vec![vec![Some("4".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn insert_select_locks_source_table_shared() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("insert-select-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)")
            .expect("t1");
        engine
            .execute("CREATE TABLE t2 (v INT NOT NULL)")
            .expect("t2");
        let t1 = table_object_id(&engine, "t1");
        let t2 = table_object_id(&engine, "t2");

        // The SELECT's source table must be read-locked (Shared) and the target
        // write-locked (Exclusive); without the Shared lock this INSERT could
        // read another transaction's uncommitted rows.
        let locks = engine.analyze_locks(
            "INSERT INTO t2 (v) SELECT v FROM t1",
            Isolation::ReadCommitted,
        );
        assert!(
            locks.contains(&(Resource::Table(t1), LockMode::Shared)),
            "source t1 must be Shared: {locks:?}"
        );
        assert!(
            locks.contains(&(Resource::Table(t2), LockMode::Exclusive)),
            "target t2 must be Exclusive: {locks:?}"
        );

        // A self-insert combines the read and write into a single Exclusive lock.
        let self_locks = engine.analyze_locks(
            "INSERT INTO t1 (id, v) SELECT id, v FROM t1",
            Isolation::ReadCommitted,
        );
        let t1_locks: Vec<_> = self_locks
            .iter()
            .filter(|(r, _)| *r == Resource::Table(t1))
            .collect();
        assert_eq!(
            t1_locks,
            vec![&(Resource::Table(t1), LockMode::Exclusive)],
            "self-insert takes a single Exclusive lock on t1"
        );

        // READ UNCOMMITTED takes no read lock on the source.
        let ru = engine.analyze_locks(
            "INSERT INTO t2 (v) SELECT v FROM t1",
            Isolation::ReadUncommitted,
        );
        assert!(
            !ru.iter()
                .any(|(r, m)| *r == Resource::Table(t1) && *m == LockMode::Shared),
            "READ UNCOMMITTED takes no shared lock: {ru:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_alter_table_add_drop_check() {
        let path = unique_temp_path("sql-alter-check");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, qty INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 5), (2, 10)")
            .expect("seed");

        // ADD CONSTRAINT validates existing rows: a constraint every row
        // satisfies is accepted and then enforced on new writes.
        engine
            .execute("ALTER TABLE t ADD CONSTRAINT ck_qty CHECK (qty >= 0)")
            .expect("add check");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t VALUES (3, -1)"),
            547
        );

        // ADD CONSTRAINT that an existing row violates is rejected (547) and
        // not persisted (a later insert violating it still succeeds after DROP).
        assert_eq!(
            sql_error_number(
                &engine,
                "ALTER TABLE t ADD CONSTRAINT ck_big CHECK (qty > 8)"
            ),
            547
        );
        // ck_big was not added, so it is not enforced.
        engine
            .execute("INSERT INTO t VALUES (4, 1)")
            .expect("insert allowed (ck_big not added)");

        // DROP CONSTRAINT removes enforcement.
        engine
            .execute("ALTER TABLE t DROP CONSTRAINT ck_qty")
            .expect("drop check");
        engine
            .execute("INSERT INTO t VALUES (5, -7)")
            .expect("insert allowed after drop");

        // Dropping an unknown constraint errors.
        assert_eq!(
            sql_error_number(&engine, "ALTER TABLE t DROP CONSTRAINT nope"),
            3728
        );
        // ALTER TABLE is DDL and is not allowed inside an explicit transaction
        // (needs a persistent txn context, so run it as one batch).
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; ALTER TABLE t ADD CHECK (qty < 100)",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(226));
        batch(&engine, &mut ctx, "ROLLBACK");

        // A constraint added via ALTER survives a restart.
        engine
            .execute("ALTER TABLE t ADD CONSTRAINT ck_id CHECK (id > 0)")
            .expect("add ck_id");
        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine");
        let (_, rows) = sql_rows(
            &engine,
            "SELECT name FROM sys.check_constraints ORDER BY name",
        );
        assert_eq!(rows, vec![vec![Some("ck_id".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_foreign_key_child_and_parent_enforcement() {
        let path = unique_temp_path("sql-fk");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE parent (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
            .expect("parent");
        engine
            .execute(
                "CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT REFERENCES parent (id))",
            )
            .expect("child");
        engine
            .execute("INSERT INTO parent VALUES (1, 'a'), (2, 'b')")
            .expect("seed parent");

        // Child side: a referenced parent must exist; NULL skips enforcement.
        engine
            .execute("INSERT INTO child VALUES (10, 1)")
            .expect("child -> parent 1");
        engine
            .execute("INSERT INTO child VALUES (11, NULL)")
            .expect("NULL fk allowed");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO child VALUES (12, 99)"),
            547
        );

        // Parent side (DELETE, NO ACTION): a referenced parent cannot be deleted.
        assert_eq!(
            sql_error_number(&engine, "DELETE FROM parent WHERE id = 1"),
            547
        );
        engine
            .execute("DELETE FROM parent WHERE id = 2")
            .expect("unreferenced parent deletes");

        // Parent side (UPDATE of the PK): cannot vacate a referenced key; a
        // non-key update is fine.
        assert_eq!(
            sql_error_number(&engine, "UPDATE parent SET id = 5 WHERE id = 1"),
            547
        );
        engine
            .execute("UPDATE parent SET name = 'z' WHERE id = 1")
            .expect("non-key parent update");

        // Child UPDATE re-checks the new value.
        assert_eq!(
            sql_error_number(&engine, "UPDATE child SET pid = 42 WHERE id = 10"),
            547
        );
        engine
            .execute("UPDATE child SET pid = NULL WHERE id = 10")
            .expect("child update to NULL");
        // With no child referencing parent 1, it can now be deleted.
        engine
            .execute("DELETE FROM parent WHERE id = 1")
            .expect("now-unreferenced parent deletes");

        // The constraint is enforced again after a restart.
        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO child VALUES (20, 7)"),
            547
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_foreign_key_self_reference() {
        let path = unique_temp_path("sql-fk-self");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE emp (id INT NOT NULL PRIMARY KEY, mgr INT REFERENCES emp (id))")
            .expect("emp");
        // A root has a NULL manager; a subordinate references an existing row.
        engine
            .execute("INSERT INTO emp VALUES (1, NULL)")
            .expect("root");
        engine
            .execute("INSERT INTO emp VALUES (2, 1)")
            .expect("sub");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO emp VALUES (3, 99)"),
            547
        );
        // A batch may reference a sibling row inserted in the same statement
        // (row 4 references 5, which is created alongside it).
        engine
            .execute("INSERT INTO emp VALUES (4, 5), (5, 1)")
            .expect("self-ref batch");
        // A referenced row cannot be deleted while a subordinate remains.
        assert_eq!(
            sql_error_number(&engine, "DELETE FROM emp WHERE id = 1"),
            547
        );

        // A primary-key change that would orphan a self-reference is rejected
        // (row 4 references row 5, so changing id 5 dangles mgr=5). This must be
        // validated against the post-update state, not the stale pre-update row.
        assert_eq!(
            sql_error_number(&engine, "UPDATE emp SET id = 50 WHERE id = 5"),
            547
        );
        // A primary-key change with no dependents is allowed (nothing points at
        // row 2, and its own mgr=1 still exists).
        engine
            .execute("UPDATE emp SET id = 6 WHERE id = 2")
            .expect("unreferenced self-ref pk change");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_constraint_name_unique_across_kinds() {
        let path = unique_temp_path("sql-constraint-names");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY)")
            .expect("p");
        // A CHECK and a FOREIGN KEY cannot share a name within one CREATE.
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TABLE c (x INT, CONSTRAINT dup CHECK (x > 0), \
                   CONSTRAINT dup FOREIGN KEY (x) REFERENCES p (id))",
            ),
            2714
        );
        // Nor across ALTER, in either order.
        engine.execute("CREATE TABLE c (x INT)").expect("c");
        engine
            .execute("ALTER TABLE c ADD CONSTRAINT dup CHECK (x > 0)")
            .expect("add check");
        assert_eq!(
            sql_error_number(
                &engine,
                "ALTER TABLE c ADD CONSTRAINT dup FOREIGN KEY (x) REFERENCES p (id)",
            ),
            2714
        );
        engine
            .execute("ALTER TABLE c ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES p (id)")
            .expect("add fk");
        assert_eq!(
            sql_error_number(&engine, "ALTER TABLE c ADD CONSTRAINT fk1 CHECK (x < 100)"),
            2714
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_foreign_key_alter_drop_and_catalog() {
        let path = unique_temp_path("sql-fk-alter");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY)")
            .expect("p");
        engine
            .execute("CREATE TABLE c (id INT NOT NULL PRIMARY KEY, pid INT)")
            .expect("c");
        engine.execute("INSERT INTO p VALUES (1)").expect("seed p");
        // Row 11 references a missing parent (no FK yet, so it is allowed).
        engine
            .execute("INSERT INTO c VALUES (10, 1), (11, 99)")
            .expect("seed c");

        // ADD FOREIGN KEY validates existing rows: row 11 orphans -> 547.
        assert_eq!(
            sql_error_number(
                &engine,
                "ALTER TABLE c ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES p (id)",
            ),
            547
        );
        // Fix the orphan, then the constraint is added and enforced.
        engine
            .execute("UPDATE c SET pid = 1 WHERE id = 11")
            .expect("fix orphan");
        engine
            .execute("ALTER TABLE c ADD CONSTRAINT fk FOREIGN KEY (pid) REFERENCES p (id)")
            .expect("add fk");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO c VALUES (12, 77)"),
            547
        );

        // sys.foreign_keys lists it, referencing p.
        let p_oid = table_object_id(&engine, "p");
        let (cols, rows) = sql_rows(
            &engine,
            "SELECT name, referenced_object_id FROM sys.foreign_keys",
        );
        assert_eq!(cols, vec!["name", "referenced_object_id"]);
        assert_eq!(rows, vec![vec![Some("fk".into()), Some(p_oid.to_string())]]);

        // A referenced parent cannot be dropped.
        assert_eq!(sql_error_number(&engine, "DROP TABLE p"), 3726);

        // DROP CONSTRAINT removes enforcement; the FK survives a restart until
        // then. Confirm durability first.
        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO c VALUES (13, 55)"),
            547
        );
        engine
            .execute("ALTER TABLE c DROP CONSTRAINT fk")
            .expect("drop fk");
        engine
            .execute("INSERT INTO c VALUES (14, 55)")
            .expect("insert allowed after drop");
        // Now p can be dropped.
        engine.execute("DROP TABLE p").expect("drop unref parent");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_foreign_key_validation_errors() {
        let path = unique_temp_path("sql-fk-invalid");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY, other INT)")
            .expect("p");
        engine
            .execute("CREATE TABLE bignum (id BIGINT NOT NULL PRIMARY KEY)")
            .expect("bignum");
        // Referencing a non-primary-key column of the parent.
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TABLE r1 (id INT NOT NULL PRIMARY KEY, pid INT REFERENCES p (other))",
            ),
            1776
        );
        // Type mismatch between child (INT) and parent PK (BIGINT).
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TABLE r2 (id INT NOT NULL PRIMARY KEY, bid INT REFERENCES bignum (id))",
            ),
            1778
        );
        // Referencing a table that does not exist.
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TABLE r3 (id INT NOT NULL PRIMARY KEY, x INT REFERENCES nope (id))",
            ),
            208
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn nested_cte_locks_its_base_table() {
        // A CTE whose body itself declares a CTE (`WITH c AS (WITH d AS ...)`)
        // must still Shared-lock the base table the inner CTE reads — directly
        // and through a view — or it dirty-reads under READ COMMITTED.
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("nested-cte-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        let secret = table_object_id(&engine, "secret");

        // Plain query with a nested CTE.
        let direct = engine.analyze_locks(
            "WITH c AS (WITH d AS (SELECT z FROM secret) SELECT z FROM d) SELECT z FROM c",
            Isolation::ReadCommitted,
        );
        assert!(
            direct.contains(&(Resource::Table(secret), LockMode::Shared)),
            "nested-CTE query must lock secret: {direct:?}"
        );

        // Same through a view.
        engine
            .execute(
                "CREATE VIEW v AS WITH c AS (WITH d AS (SELECT z FROM secret) SELECT z FROM d) SELECT z FROM c",
            )
            .expect("view");
        let via_view = engine.analyze_locks("SELECT z FROM v", Isolation::ReadCommitted);
        assert!(
            via_view.contains(&(Resource::Table(secret), LockMode::Shared)),
            "view over a nested-CTE body must lock secret: {via_view:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn selecting_a_view_locks_its_base_tables() {
        // A read through a view must Shared-lock the view's base tables (else a
        // dirty read under READ COMMITTED), including a base table the view body
        // reaches only through its own CTE.
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("view-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE base (x INT NOT NULL PRIMARY KEY)")
            .expect("base");
        engine
            .execute("CREATE VIEW v AS WITH c AS (SELECT x FROM base) SELECT x FROM c")
            .expect("view");
        let base = table_object_id(&engine, "base");

        let locks = engine.analyze_locks("SELECT x FROM v", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(base), LockMode::Shared)),
            "a view's base table (via its CTE) must be Shared-locked: {locks:?}"
        );

        // A view over the view must reach the base table through both levels.
        engine
            .execute("CREATE VIEW v2 AS SELECT x FROM v")
            .expect("v2");
        let nested = engine.analyze_locks("SELECT x FROM v2", Isolation::ReadCommitted);
        assert!(
            nested.contains(&(Resource::Table(base), LockMode::Shared)),
            "a nested view must Shared-lock the base table through both views: {nested:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn view_with_nested_cte_in_exists_locks_both_tables() {
        // A view whose body has `WHERE EXISTS (WITH d AS (SELECT ... FROM secret)
        // ...)` reads both `base` and `secret`; both must be Shared-locked.
        // EXISTS is the only expression position the parser lets a subquery start
        // with WITH, so it is the nested-CTE-in-expression case for locks.
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("view-exists-cte-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE base (x INT NOT NULL PRIMARY KEY)")
            .expect("base");
        engine
            .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        engine
            .execute("CREATE VIEW v AS SELECT x FROM base WHERE EXISTS (WITH d AS (SELECT z FROM secret) SELECT z FROM d)")
            .expect("view");
        let base = table_object_id(&engine, "base");
        let secret = table_object_id(&engine, "secret");

        let locks = engine.analyze_locks("SELECT x FROM v", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(base), LockMode::Shared)),
            "base must be locked: {locks:?}"
        );
        assert!(
            locks.contains(&(Resource::Table(secret), LockMode::Shared)),
            "secret (behind the EXISTS nested CTE) must be locked: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn view_definition_survives_restart() {
        // A view lives in the persisted catalog, so it must be queryable after
        // the engine is reopened.
        let path = unique_temp_path("view-persist");
        let storage =
            Storage::create(path.clone(), test_storage_options()).expect("storage create");
        let engine = Engine::new(storage).expect("engine create");
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("table");
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .expect("insert");
        engine
            .execute("CREATE VIEW hi AS SELECT id FROM t WHERE v >= 20")
            .expect("view");
        drop(engine);

        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine restart");
        let out = engine.execute("SELECT id FROM hi").expect("query view");
        assert!(out.contains('2'), "view query after restart: {out}");
        let listed = engine
            .execute("SELECT name FROM sys.views")
            .expect("sys.views");
        assert!(listed.contains("hi"), "sys.views after restart: {listed}");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn assignment_select_locks_base_table_behind_a_cte_value() {
        // A CTE referenced only inside an assignment SELECT's value subquery
        // must still lock the real base table, or the read could dirty-read a
        // concurrent uncommitted write under READ COMMITTED.
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("assign-cte-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE secret (x INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        let secret = table_object_id(&engine, "secret");

        let locks = engine.analyze_locks(
            "DECLARE @v INT; WITH c AS (SELECT x FROM secret) SELECT @v = (SELECT MAX(x) FROM c)",
            Isolation::ReadCommitted,
        );
        assert!(
            locks.contains(&(Resource::Table(secret), LockMode::Shared)),
            "base table behind the CTE-in-value must be Shared-locked: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn order_by_spills_and_matches_in_memory_sort() {
        // A tiny sort budget forces the external merge sort (spill sorted runs
        // to temp extents + k-way merge); its output must be byte-identical to
        // the in-memory sort, ties included.
        let path = unique_temp_path("sort-spill");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, grp INT, tag NVARCHAR(20))")
            .expect("t");
        // 600 rows with many tied `grp` values (exercises stable cross-run ties).
        for i in 0..600 {
            engine
                .execute(&format!(
                    "INSERT INTO t VALUES ({i}, {}, 'tag-{}')",
                    (i * 7) % 50,
                    i % 13
                ))
                .expect("insert");
        }
        let query = "SELECT id, grp, tag FROM t ORDER BY grp, tag, id";

        // Reference: default (in-memory) budget.
        let (_, reference) = sql_rows(&engine, query);

        // Forced spill: a 300-byte budget makes almost every row its own run.
        crate::rel::set_test_sort_budget(Some(300));
        let (_, spilled) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);

        assert_eq!(reference.len(), 600);
        assert_eq!(
            spilled, reference,
            "spilled sort must match the in-memory sort"
        );
        // Sanity: the result really is ordered by (grp, tag, id).
        let key = |r: &Vec<Option<String>>| {
            (
                r[1].clone().unwrap().parse::<i64>().unwrap(),
                r[2].clone().unwrap(),
                r[0].clone().unwrap().parse::<i64>().unwrap(),
            )
        };
        assert!(spilled.windows(2).all(|w| key(&w[0]) <= key(&w[1])));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn inner_join_grace_hash_spills_and_matches_in_memory() {
        // A tiny budget forces the grace-hash INNER join (partition both sides by
        // key hash to temp extents, join per partition). Results must match the
        // in-memory hash join — many-to-many keys, NULL keys (never match), and a
        // residual ON predicate.
        let path = unique_temp_path("join-spill");
        let engine = new_engine(&path);
        engine.execute("CREATE TABLE l (k INT, v INT)").expect("l");
        engine.execute("CREATE TABLE r (k INT, w INT)").expect("r");
        for i in 0..300 {
            let lk = if i % 41 == 0 {
                "NULL".into()
            } else {
                (i % 25).to_string()
            };
            engine
                .execute(&format!("INSERT INTO l VALUES ({lk}, {i})"))
                .expect("l ins");
            let rk = if i % 43 == 0 {
                "NULL".into()
            } else {
                (i % 25).to_string()
            };
            engine
                .execute(&format!("INSERT INTO r VALUES ({rk}, {i})"))
                .expect("r ins");
        }
        let query = "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k AND r.w > 100 ORDER BY l.v, r.w";

        let (_, reference) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(Some(500));
        let (_, spilled) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);

        assert!(!reference.is_empty());
        assert_eq!(
            spilled, reference,
            "grace-hash INNER join must match in-memory"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn outer_joins_grace_hash_spill_and_match_in_memory() {
        // A tiny budget forces the grace-hash join for LEFT/RIGHT/FULL. Each must
        // match its in-memory result, including probe-side unmatched rows,
        // build-side unmatched rows (FULL), and NULL-keyed rows on both sides
        // (never match; the outer side's are null-extended, the inner's dropped).
        let path = unique_temp_path("outer-join-spill");
        let engine = new_engine(&path);
        engine.execute("CREATE TABLE l (k INT, v INT)").expect("l");
        engine.execute("CREATE TABLE r (k INT, w INT)").expect("r");
        for i in 0..300 {
            // Disjoint-ish key ranges so both sides have unmatched rows.
            let lk = if i % 41 == 0 {
                "NULL".into()
            } else {
                (i % 30).to_string()
            };
            engine
                .execute(&format!("INSERT INTO l VALUES ({lk}, {i})"))
                .expect("l ins");
            let rk = if i % 43 == 0 {
                "NULL".into()
            } else {
                (i % 25 + 10).to_string()
            };
            engine
                .execute(&format!("INSERT INTO r VALUES ({rk}, {i})"))
                .expect("r ins");
        }
        for kind in ["LEFT", "RIGHT", "FULL"] {
            let query =
                format!("SELECT l.v, r.w FROM l {kind} JOIN r ON l.k = r.k ORDER BY l.v, r.w");
            let (_, reference) = sql_rows(&engine, &query);
            crate::rel::set_test_sort_budget(Some(500));
            let (_, spilled) = sql_rows(&engine, &query);
            crate::rel::set_test_sort_budget(None);
            assert!(!reference.is_empty(), "{kind}: reference empty");
            assert_eq!(
                spilled, reference,
                "grace-hash {kind} join must match in-memory"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn distinct_grace_hash_spills_and_matches_in_memory() {
        // A tiny budget forces grace-hash DISTINCT (partition rows by key hash to
        // temp extents, dedup each partition). Results must match the in-memory
        // hash DISTINCT — many duplicates, NULLs, and a multi-column key.
        let path = unique_temp_path("distinct-spill");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, b INT)")
            .expect("t");
        for i in 0..900 {
            let a = if i % 53 == 0 {
                "NULL".to_string()
            } else {
                (i % 20).to_string()
            };
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {a}, {})", i % 15))
                .expect("insert");
        }
        let query = "SELECT DISTINCT a, b FROM t ORDER BY a, b";

        let (_, reference) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(Some(400));
        let (_, spilled) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);

        assert!(!reference.is_empty());
        assert_eq!(
            spilled, reference,
            "grace-hash DISTINCT must match in-memory"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn group_by_grace_hash_spills_and_matches_in_memory() {
        // A tiny budget forces grace-hash aggregation (partition rows by
        // group-key hash to temp extents, aggregate each partition). Results
        // must match the in-memory hash aggregate — group keys, SUM, COUNT, and
        // COUNT(DISTINCT), including a NULL group and HAVING.
        let path = unique_temp_path("agg-spill");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, grp INT, amt INT)")
            .expect("t");
        for i in 0..800 {
            let grp = if i % 37 == 0 {
                "NULL".to_string()
            } else {
                (i % 60).to_string()
            };
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {grp}, {})", i % 10))
                .expect("insert");
        }
        let query = "SELECT grp, SUM(amt), COUNT(*), COUNT(DISTINCT amt) FROM t GROUP BY grp HAVING COUNT(*) > 2 ORDER BY grp";

        let (_, reference) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(Some(400));
        let (_, spilled) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);

        assert!(!reference.is_empty());
        assert_eq!(
            spilled, reference,
            "grace-hash aggregate must match in-memory"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn order_by_spills_wide_join_rows() {
        // A join's source row is the concatenation of both tables' columns. Each
        // per-table row fits (< the ~2020 B clustered cell cap), but the joined
        // source row (two ~1950 B strings) exceeds the 3900 B in-row table cap —
        // sorting it (pre-projection) must still spill: the spill codec is
        // cap-free, whereas reusing the table codec would error 1701.
        let path = unique_temp_path("sort-spill-wide");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE a (k INT NOT NULL PRIMARY KEY, s VARCHAR(2000))")
            .expect("a");
        engine
            .execute("CREATE TABLE b (k INT NOT NULL PRIMARY KEY, s VARCHAR(2000))")
            .expect("b");
        for i in 0..40 {
            engine
                .execute(&format!(
                    "INSERT INTO a VALUES ({i}, '{}')",
                    "x".repeat(1950)
                ))
                .expect("a ins");
            engine
                .execute(&format!(
                    "INSERT INTO b VALUES ({i}, '{}')",
                    "y".repeat(1950)
                ))
                .expect("b ins");
        }
        let query = "SELECT a.k FROM a JOIN b ON a.k = b.k ORDER BY a.k DESC";
        let (_, reference) = sql_rows(&engine, query);
        assert_eq!(
            reference.len(),
            40,
            "join+sort should return 40 rows in memory"
        );
        // Each joined source row is ~3.9 KB (> 3900) — forced to spill.
        crate::rel::set_test_sort_budget(Some(300));
        let (_, rows) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);
        assert_eq!(
            rows, reference,
            "spilled wide-join sort must match in-memory"
        );
        assert_eq!(rows[0][0].as_deref(), Some("39"));
        assert_eq!(rows[39][0].as_deref(), Some("0"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn row_locks_for_point_operations() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("row-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("t");
        let t = table_object_id(&engine, "t");
        let rc = Isolation::ReadCommitted;

        let has_table_x = |locks: &[(Resource, LockMode)]| {
            locks.contains(&(Resource::Table(t), LockMode::Exclusive))
        };
        let row_x = |locks: &[(Resource, LockMode)]| -> Option<u64> {
            locks.iter().find_map(|(r, m)| match r {
                Resource::Row(oid, h) if *oid == t && *m == LockMode::Exclusive => Some(*h),
                _ => None,
            })
        };

        // Point UPDATE: Table IX + a single Row X, no Table X.
        let up = engine.analyze_locks("UPDATE t SET v = 9 WHERE id = 5", rc);
        assert!(up.contains(&(Resource::Table(t), LockMode::IntentExclusive)));
        assert!(
            !has_table_x(&up),
            "point UPDATE must not take Table X: {up:?}"
        );
        let k5 = row_x(&up).expect("point UPDATE row lock");

        // Point DELETE: same row key as the UPDATE of id = 5.
        let del = engine.analyze_locks("DELETE FROM t WHERE id = 5", rc);
        assert_eq!(row_x(&del), Some(k5), "DELETE id=5 must lock the same row");

        // A different key → a different row resource (so the two run concurrently).
        let up6 = engine.analyze_locks("UPDATE t SET v = 1 WHERE id = 6", rc);
        assert_ne!(row_x(&up6), Some(k5));

        // Point INSERT (literal key) row-locks; INSERT ... SELECT does not.
        let ins = engine.analyze_locks("INSERT INTO t VALUES (7, 1)", rc);
        assert!(row_x(&ins).is_some() && !has_table_x(&ins));
        let ins_sel = engine.analyze_locks("INSERT INTO t SELECT id, v FROM t", rc);
        assert!(has_table_x(&ins_sel) && row_x(&ins_sel).is_none());

        // Range / OR / partial predicates fall back to Table X.
        for sql in [
            "UPDATE t SET v = 1 WHERE id > 5",
            "DELETE FROM t WHERE id = 5 OR id = 6",
            "UPDATE t SET v = 1",
            "UPDATE t SET id = 2 WHERE id = 5", // key change moves the row
            "DELETE FROM t WHERE id = (SELECT MAX(id) FROM t)",
        ] {
            let locks = engine.analyze_locks(sql, rc);
            assert!(
                has_table_x(&locks) && row_x(&locks).is_none(),
                "table lock for `{sql}`: {locks:?}"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn row_lock_safety_guards() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("row-lock-guards");
        let engine = new_engine(&path);
        // Character PK, a table with a secondary UNIQUE index, and a FLOAT PK.
        engine
            .execute("CREATE TABLE cs (id VARCHAR(10) NOT NULL PRIMARY KEY, v INT)")
            .expect("cs");
        engine
            .execute("CREATE TABLE u (id INT NOT NULL PRIMARY KEY, email VARCHAR(50))")
            .expect("u");
        engine
            .execute("CREATE UNIQUE INDEX ux ON u (email)")
            .expect("ux");
        engine
            .execute("CREATE TABLE f (k FLOAT NOT NULL PRIMARY KEY, v INT)")
            .expect("f");
        let cs = table_object_id(&engine, "cs");
        let u = table_object_id(&engine, "u");
        let f = table_object_id(&engine, "f");
        let rc = Isolation::ReadCommitted;
        let table_x = |locks: &[(Resource, LockMode)], t: u32| {
            locks.contains(&(Resource::Table(t), LockMode::Exclusive))
        };
        let has_row = |locks: &[(Resource, LockMode)], t: u32| {
            locks
                .iter()
                .any(|(r, _)| matches!(r, Resource::Row(o, _) if *o == t))
        };

        // Character PK vs a *string* literal row-locks; vs a *numeric* literal it
        // does not (the executor's string->number match is many-to-one).
        let str_lit = engine.analyze_locks("UPDATE cs SET v = 1 WHERE id = '05'", rc);
        assert!(has_row(&str_lit, cs));
        let num_lit = engine.analyze_locks("UPDATE cs SET v = 1 WHERE id = 5", rc);
        assert!(table_x(&num_lit, cs) && !has_row(&num_lit, cs));

        // A table with a secondary UNIQUE index: INSERT/UPDATE keep Table X;
        // DELETE may still row-lock (a delete cannot create a duplicate).
        let ins = engine.analyze_locks("INSERT INTO u VALUES (1, 'a@b.com')", rc);
        assert!(table_x(&ins, u) && !has_row(&ins, u));
        let upd = engine.analyze_locks("UPDATE u SET email = 'x' WHERE id = 1", rc);
        assert!(table_x(&upd, u) && !has_row(&upd, u));
        let del = engine.analyze_locks("DELETE FROM u WHERE id = 1", rc);
        assert!(has_row(&del, u) && !table_x(&del, u));

        // FLOAT PK is never row-locked (signed zero / NaN encode ambiguity).
        let fl = engine.analyze_locks("UPDATE f SET v = 1 WHERE k = 1.0", rc);
        assert!(table_x(&fl, f) && !has_row(&fl, f));

        // A batch that point-writes AND reads the same table must end up with an
        // exclusive table lock (the IX+S -> X combine fix), not a Shared lock.
        let batch =
            engine.analyze_locks("UPDATE cs SET v = 1 WHERE id = '05'; SELECT * FROM cs", rc);
        assert!(
            table_x(&batch, cs),
            "point-write + same-table read must hold Table X: {batch:?}"
        );
        assert!(
            !batch.contains(&(Resource::Table(cs), LockMode::Shared)),
            "must not downgrade to Shared: {batch:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn row_locks_require_full_composite_key() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("row-locks-composite");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (a, b))")
            .expect("t");
        let t = table_object_id(&engine, "t");
        let rc = Isolation::ReadCommitted;
        let row_x = |locks: &[(Resource, LockMode)]| {
            locks.iter().any(|(r, m)| {
                matches!(r, Resource::Row(oid, _) if *oid == t) && *m == LockMode::Exclusive
            })
        };
        // Both key columns pinned → row lock.
        assert!(row_x(
            &engine.analyze_locks("UPDATE t SET v = 1 WHERE a = 1 AND b = 2", rc)
        ));
        // Only one pinned → table lock.
        let partial = engine.analyze_locks("UPDATE t SET v = 1 WHERE a = 1", rc);
        assert!(!row_x(&partial) && partial.contains(&(Resource::Table(t), LockMode::Exclusive)));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn foreign_key_insert_locks_parent_shared() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("fk-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE p (id INT NOT NULL PRIMARY KEY)")
            .expect("p");
        engine
            .execute("CREATE TABLE c (id INT NOT NULL PRIMARY KEY, pid INT REFERENCES p (id))")
            .expect("c");
        let p = table_object_id(&engine, "p");
        let c = table_object_id(&engine, "c");

        // INSERT into the child reads the parent, so it must take a Shared lock
        // on the parent (else it could read an uncommitted parent row). The
        // child is not itself an FK parent, so its point INSERT row-locks:
        // Table IntentExclusive + a Row Exclusive on the inserted key.
        let locks = engine.analyze_locks("INSERT INTO c VALUES (1, 1)", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(c), LockMode::IntentExclusive)),
            "child IntentExclusive: {locks:?}"
        );
        assert!(
            locks
                .iter()
                .any(|(r, m)| matches!(r, Resource::Row(t, _) if *t == c)
                    && *m == LockMode::Exclusive),
            "child Row Exclusive: {locks:?}"
        );
        assert!(
            locks.contains(&(Resource::Table(p), LockMode::Shared)),
            "parent Shared: {locks:?}"
        );
        // DELETE of the parent reads the child (NO ACTION check) -> child Shared.
        let del = engine.analyze_locks("DELETE FROM p WHERE id = 1", Isolation::ReadCommitted);
        assert!(
            del.contains(&(Resource::Table(p), LockMode::Exclusive)),
            "parent Exclusive: {del:?}"
        );
        assert!(
            del.contains(&(Resource::Table(c), LockMode::Shared)),
            "child Shared: {del:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_batch_variables() {
        let path = unique_temp_path("sql-vars");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        // DECLARE, SET, and read a variable within one batch.
        let out = batch(&engine, &mut ctx, "DECLARE @n INT; SET @n = 42; SELECT @n");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![42]);

        // An initializer may reference an earlier variable in the same DECLARE.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @a INT = 5, @b INT = @a + 1; SELECT @b",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![6]);

        // A variable used in a WHERE clause.
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        );
        batch(
            &engine,
            &mut ctx,
            "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @min INT; SET @min = 20; SELECT id FROM t WHERE v >= @min ORDER BY id",
        );
        assert_eq!(ids(&out), vec![2, 3]);

        // Using an undeclared variable is error 137 (SET and read).
        assert_eq!(
            batch(&engine, &mut ctx, "SET @nope = 1")
                .error
                .as_ref()
                .map(|e| e.number),
            Some(137)
        );
        assert_eq!(
            batch(&engine, &mut ctx, "SELECT @nope")
                .error
                .as_ref()
                .map(|e| e.number),
            Some(137)
        );

        // Redeclaring within the same batch is error 134.
        assert_eq!(
            batch(&engine, &mut ctx, "DECLARE @d INT; DECLARE @d INT")
                .error
                .as_ref()
                .map(|e| e.number),
            Some(134)
        );

        // Variables are batch-scoped: one declared in a prior batch is gone.
        batch(&engine, &mut ctx, "DECLARE @scoped INT");
        assert_eq!(
            batch(&engine, &mut ctx, "SELECT @scoped")
                .error
                .as_ref()
                .map(|e| e.number),
            Some(137)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_scalar_in_exists_subqueries() {
        let path = unique_temp_path("sql-subquery");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("nums");
        engine
            .execute("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30)")
            .expect("seed");
        engine
            .execute("CREATE TABLE picks (id INT NOT NULL PRIMARY KEY, target INT)")
            .expect("picks");
        engine
            .execute("INSERT INTO picks VALUES (1, 2), (2, 3)")
            .expect("seed2");

        // Scalar subquery in WHERE.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE v = (SELECT MAX(v) FROM nums)",
        );
        assert_eq!(rows, vec![vec![Some("3".into())]]);

        // Scalar subquery in the SELECT list (evaluated once).
        let (cols, rows) = sql_rows(
            &engine,
            "SELECT id, (SELECT COUNT(*) FROM picks) AS pc FROM nums ORDER BY id",
        );
        assert_eq!(cols, vec!["id", "pc"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("2".into())],
                vec![Some("2".into()), Some("2".into())],
                vec![Some("3".into()), Some("2".into())],
            ]
        );

        // IN (SELECT) and NOT IN (SELECT).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE id IN (SELECT target FROM picks) ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE id NOT IN (SELECT target FROM picks)",
        );
        assert_eq!(rows, vec![vec![Some("1".into())]]);

        // EXISTS / NOT EXISTS (uncorrelated).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE EXISTS (SELECT 1 FROM picks WHERE target = 3) ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into())],
                vec![Some("2".into())],
                vec![Some("3".into())],
            ]
        );
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE NOT EXISTS (SELECT 1 FROM picks WHERE target = 99)",
        );
        assert_eq!(rows.len(), 3);

        // A scalar subquery with no rows is NULL (so the `=` is unknown).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE v = (SELECT v FROM nums WHERE id = 99)",
        );
        assert!(rows.is_empty());

        // More than one row from a scalar subquery is 512; more than one column
        // is 116.
        assert_eq!(
            sql_error_number(
                &engine,
                "SELECT id FROM nums WHERE v = (SELECT v FROM nums)"
            ),
            512
        );
        assert_eq!(
            sql_error_number(
                &engine,
                "SELECT id FROM nums WHERE v = (SELECT id, v FROM nums WHERE id = 1)",
            ),
            116
        );
        // Correlated subqueries: the inner query references an outer column and
        // is re-run per outer row (Stage 11).
        // EXISTS: nums with a pick whose target equals the num id -> 2, 3.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE EXISTS (SELECT 1 FROM picks WHERE picks.target = nums.id) ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
        // NOT EXISTS is the complement.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE NOT EXISTS (SELECT 1 FROM picks WHERE picks.target = nums.id) ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        // Correlated scalar subquery: the pick sharing the num's id has target 2.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE (SELECT target FROM picks WHERE picks.id = nums.id) = 2",
        );
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        // Correlated IN: num id is among the targets of picks sharing that id.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE id IN (SELECT target FROM picks WHERE picks.id = nums.id - 1) ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);

        // `NOT IN (empty subquery)` is TRUE for every row, including a NULL
        // outer value — the comparison set is empty, so nothing is unknown.
        engine
            .execute("INSERT INTO nums VALUES (4, NULL)")
            .expect("null row");
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE v NOT IN (SELECT target FROM picks WHERE target > 1000) ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into())],
                vec![Some("2".into())],
                vec![Some("3".into())],
                vec![Some("4".into())],
            ]
        );
        // `IN (empty subquery)` is FALSE for every row (no rows returned).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM nums WHERE v IN (SELECT target FROM picks WHERE target > 1000)",
        );
        assert!(rows.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn subquery_locks_referenced_tables_shared() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("subquery-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE a (id INT NOT NULL PRIMARY KEY)")
            .expect("a");
        engine
            .execute("CREATE TABLE b (id INT NOT NULL PRIMARY KEY)")
            .expect("b");
        let a = table_object_id(&engine, "a");
        let b = table_object_id(&engine, "b");
        // A subquery over `b` inside `a`'s WHERE reads `b`, so it must take a
        // Shared lock on `b` (else it could read `b`'s uncommitted rows).
        let locks = engine.analyze_locks(
            "SELECT id FROM a WHERE id IN (SELECT id FROM b)",
            Isolation::ReadCommitted,
        );
        assert!(
            locks.contains(&(Resource::Table(a), LockMode::Shared)),
            "a Shared: {locks:?}"
        );
        assert!(
            locks.contains(&(Resource::Table(b), LockMode::Shared)),
            "b Shared (subquery): {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_common_table_expressions() {
        let path = unique_temp_path("sql-cte");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, dept NVARCHAR(4), amount INT)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO sales VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',50)")
            .expect("seed");

        // A basic CTE referenced in FROM.
        let (cols, rows) = sql_rows(
            &engine,
            "WITH big AS (SELECT id, amount FROM sales WHERE amount >= 20) SELECT id FROM big ORDER BY id",
        );
        assert_eq!(cols, vec!["id"]);
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("4".into())]]);

        // A CTE that aggregates, filtered by the outer query.
        let (_, rows) = sql_rows(
            &engine,
            "WITH s AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) \
               SELECT dept FROM s WHERE total > 30 ORDER BY dept",
        );
        assert_eq!(rows, vec![vec![Some("b".into())]]);

        // A later CTE references an earlier one.
        let (_, rows) = sql_rows(
            &engine,
            "WITH a AS (SELECT id, amount FROM sales WHERE amount >= 10), \
                  b AS (SELECT id FROM a WHERE amount >= 20) \
               SELECT id FROM b ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("4".into())]]);

        // A CTE joined to a base table.
        let (_, rows) = sql_rows(
            &engine,
            "WITH s AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) \
               SELECT t.id, s.total FROM sales t JOIN s ON t.dept = s.dept WHERE t.id = 3",
        );
        assert_eq!(rows, vec![vec![Some("3".into()), Some("55".into())]]);

        // The optional column-rename list is not supported yet.
        assert_eq!(
            sql_error_number(
                &engine,
                "WITH c(x) AS (SELECT id FROM sales) SELECT x FROM c",
            ),
            102
        );
        // A recursive / self-reference resolves as a (non-existent) base table.
        assert_eq!(
            sql_error_number(&engine, "WITH r AS (SELECT id FROM r) SELECT id FROM r"),
            208
        );

        // A CTE is visible to a subquery in the WHERE clause, not just the FROM.
        let (_, rows) = sql_rows(
            &engine,
            "WITH s AS (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) \
               SELECT id FROM sales WHERE dept IN (SELECT dept FROM s WHERE total > 30) ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("3".into())], vec![Some("4".into())]]);

        // Duplicate CTE names are rejected.
        assert_eq!(
            sql_error_number(
                &engine,
                "WITH a AS (SELECT 1 AS x), a AS (SELECT 2 AS x) SELECT x FROM a",
            ),
            460
        );
        // A schema-qualified reference does not match a CTE (dbo.s is a base
        // table name, which here does not exist).
        assert_eq!(
            sql_error_number(&engine, "WITH s AS (SELECT 1 AS v) SELECT v FROM dbo.s"),
            208
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_derived_tables() {
        let path = unique_temp_path("sql-derived");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, dept NVARCHAR(4), amount INT)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO sales VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',50)")
            .expect("seed");

        // A derived table filtered further by the outer query; columns resolve
        // by the derived alias.
        let (cols, rows) = sql_rows(
            &engine,
            "SELECT s.id, s.amount FROM (SELECT id, amount FROM sales WHERE amount >= 10) s \
               WHERE s.id < 3 ORDER BY s.id",
        );
        assert_eq!(cols, vec!["id", "amount"]);
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into()), Some("10".into())],
                vec![Some("2".into()), Some("20".into())],
            ]
        );

        // A derived table may aggregate; the outer query filters on the alias.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT d.dept, d.total FROM (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) d \
               WHERE d.total > 30 ORDER BY d.dept",
        );
        assert_eq!(rows, vec![vec![Some("b".into()), Some("55".into())]]);

        // A derived table joined to a base table.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT t.dept, d.total FROM sales t \
               JOIN (SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept) d ON t.dept = d.dept \
               WHERE t.id = 1",
        );
        assert_eq!(rows, vec![vec![Some("a".into()), Some("30".into())]]);

        // A derived table must have an alias.
        assert_eq!(
            sql_error_number(&engine, "SELECT * FROM (SELECT id FROM sales)"),
            102
        );
        // Every derived column must be named.
        assert_eq!(
            sql_error_number(&engine, "SELECT * FROM (SELECT amount + 1 FROM sales) x"),
            8155
        );
        // Duplicate derived column names are rejected.
        assert_eq!(
            sql_error_number(
                &engine,
                "SELECT * FROM (SELECT id, amount AS id FROM sales) x",
            ),
            8156
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_sys_default_constraints() {
        let path = unique_temp_path("sql-default-constraints");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, \
                   qty INT DEFAULT 0, note NVARCHAR(10) DEFAULT 'n/a', plain INT)",
            )
            .expect("create");
        // One row per column that carries a DEFAULT (plain has none).
        let (cols, rows) = sql_rows(
            &engine,
            "SELECT name, parent_column_id, definition FROM sys.default_constraints ORDER BY parent_column_id",
        );
        assert_eq!(cols, vec!["name", "parent_column_id", "definition"]);
        assert_eq!(
            rows,
            vec![
                vec![
                    Some("DF__t__qty".into()),
                    Some("2".into()),
                    Some("(0)".into())
                ],
                vec![
                    Some("DF__t__note".into()),
                    Some("3".into()),
                    Some("('n/a')".into()),
                ],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_decimal_arithmetic_and_rendering() {
        let path = unique_temp_path("sql-decimal");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, price DECIMAL(10,2))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 12.50), (2, 3.30)")
            .expect("insert");
        let (_, rows) = sql_rows(
            &engine,
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
        let (_, rows) = sql_rows(&engine, "SELECT price / 3 FROM t WHERE id = 1");
        assert_eq!(rows, vec![vec![Some("4.166667".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_temporal_types_round_trip() {
        let path = unique_temp_path("sql-temporal");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME2)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, '2020-06-15', '2020-06-15 13:45:30.5')")
            .expect("insert");
        let (_, rows) = sql_rows(&engine, "SELECT d, dt FROM t");
        assert_eq!(
            rows,
            vec![vec![
                Some("2020-06-15".into()),
                Some("2020-06-15 13:45:30.5000000".into())
            ]]
        );
        // A character literal implicitly converts to DATE for the comparison.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE d = '2020-06-15'");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_expression_operators() {
        let path = unique_temp_path("sql-expr-ops");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20), score INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', NULL), (3, 'Carol', 70)")
            .expect("insert");

        // LIKE + IN + BETWEEN combine in a WHERE.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM t WHERE name LIKE 'A%' OR id IN (3) OR score BETWEEN 85 AND 95 ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("3".into())]]);

        // CASE (searched) + ISNULL + a scalar function.
        let (cols, rows) = sql_rows(
            &engine,
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
            &engine,
            "SELECT CAST(score AS NVARCHAR(10)) FROM t WHERE id = 1",
        );
        assert_eq!(rows, vec![vec![Some("90".into())]]);
        let (_, rows) = sql_rows(
            &engine,
            "SELECT id FROM t WHERE name NOT LIKE '%o%' ORDER BY id",
        );
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_swedish_collation_order_by() {
        let path = unique_temp_path("sql-collation");
        let engine = new_engine(&path);
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
        let (_, rows) = sql_rows(&engine, "SELECT w FROM t ORDER BY w");
        let order: Vec<String> = rows.into_iter().map(|r| r[0].clone().unwrap()).collect();
        assert_eq!(order, vec!["apa", "björn", "zebra", "åre", "ängel", "öl"]);
        // The collation is surfaced in sys.columns.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT collation_name FROM sys.columns WHERE name = 'w'",
        );
        assert_eq!(rows, vec![vec![Some("Finnish_Swedish_CI_AS".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_stage5_review_fixes() {
        let path = unique_temp_path("sql-review-fixes");
        let engine = new_engine(&path);
        // CAST decimal/float to int truncates toward zero (not rounds).
        let (_, rows) = sql_rows(
            &engine,
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
            &engine,
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
            &engine,
            "SELECT CASE WHEN id = 1 THEN 100000 ELSE 0.5 END AS v FROM t ORDER BY id",
        );
        assert_eq!(
            rows,
            vec![vec![Some("100000.0".into())], vec![Some("0.5".into())]]
        );
        // UPDATE with a duplicated SET column is rejected (264).
        assert_eq!(
            sql_error_number(&engine, "UPDATE t SET id = 3, id = 4 WHERE id = 1"),
            264
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_duplicate_pk_reports_error_2627() {
        let path = unique_temp_path("sql-pk-dup");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        assert_eq!(sql_error_number(&engine, "INSERT INTO t VALUES (1)"), 2627);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_where_order_top_projection() {
        let path = unique_temp_path("sql-select");
        let engine = new_engine(&path);
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
            &engine,
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
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE nums (n INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine
            .execute("INSERT INTO nums VALUES (1)")
            .expect("insert");
        // A bare column with an alias must report the alias, not the source
        // column name (regression guard for the typed-projection refactor).
        let (columns, rows) = sql_rows(&engine, "SELECT n AS foo FROM nums");
        assert_eq!(columns, vec!["foo"]);
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_three_valued_where_keeps_only_true_rows() {
        let path = unique_temp_path("sql-3vl");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)")
            .expect("insert");
        // v <> 10 is UNKNOWN for the NULL row, which is filtered out.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE v <> 10 ORDER BY id");
        assert_eq!(rows, vec![vec![Some("3".into())]]);
        // IS NULL is two-valued.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE v IS NULL");
        assert_eq!(rows, vec![vec![Some("2".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_sys_catalog_is_queryable() {
        let path = unique_temp_path("sql-syscat");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE alpha (id INT PRIMARY KEY, name NVARCHAR(20))")
            .expect("create alpha");
        engine
            .execute("CREATE TABLE beta (x BIGINT NOT NULL)")
            .expect("create beta");
        let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.tables ORDER BY name");
        assert_eq!(
            rows,
            vec![vec![Some("alpha".into())], vec![Some("beta".into())]]
        );
        // sys.columns: alpha has two columns.
        let (_, rows) = sql_rows(
            &engine,
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
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .expect("create");
        // Selecting a missing table -> 208.
        assert_eq!(sql_error_number(&engine, "SELECT * FROM nope"), 208);
        // Duplicate CREATE -> 2714.
        assert_eq!(sql_error_number(&engine, "CREATE TABLE t (id INT)"), 2714);
        // DROP then it's gone; DROP IF EXISTS is a no-op; bare DROP -> 3701.
        engine.execute("DROP TABLE t").expect("drop");
        assert_eq!(sql_error_number(&engine, "SELECT * FROM t"), 208);
        engine
            .execute("DROP TABLE IF EXISTS t")
            .expect("drop if exists");
        assert_eq!(sql_error_number(&engine, "DROP TABLE t"), 3701);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_not_null_violation_reports_515() {
        let path = unique_temp_path("sql-notnull");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(10) NOT NULL)")
            .expect("create");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t (id) VALUES (1)"),
            515
        );
        // String too long -> 8152.
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t VALUES (1, 'this is far too long')"),
            8152
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_and_search_share_the_engine() {
        // The SQL front door must not disturb the frozen ES surface.
        let path = unique_temp_path("sql-es-coexist");
        let engine = new_engine(&path);
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
            &engine,
            r#"search docs { "query": { "match": { "body": "hello" } } }"#,
        );
        assert_eq!(search["hits"]["total"], 1);
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t");
        assert_eq!(rows, vec![vec![Some("42".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_bit_column_compares_to_integer_literal() {
        let path = unique_temp_path("sql-bit-cmp");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, active BIT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
            .expect("insert");
        // `active = 1` (BIT vs int) must work, not clash.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE active = 1 ORDER BY id");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_multi_row_insert_is_atomic() {
        let path = unique_temp_path("sql-insert-atomic");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (5)").expect("seed");
        // The 3rd row duplicates PK 5: the whole INSERT must roll back, so
        // rows 10 and 11 must NOT be present.
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t VALUES (10), (11), (5)"),
            2627
        );
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows, vec![vec![Some("5".into())]], "no partial rows");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_batch_keeps_earlier_results_before_an_error() {
        let path = unique_temp_path("sql-batch-partial");
        let engine = new_engine(&path);
        // One batch: a good CREATE + INSERT, then a failing INSERT.
        let env = sql(
            &engine,
            "CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);",
        );
        assert_eq!(env["kind"], "sql");
        // Two statements succeeded (done, count) before the error.
        assert_eq!(env["results"].as_array().unwrap().len(), 2);
        assert_eq!(env["results"][1]["rows_affected"], 1);
        assert_eq!(env["error"]["number"], 2627);
        // The first row is durably present.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    // ---- explicit transactions (Stage 6, M2) ---------------------------

    use crate::rel::{BatchOutcome, StatementResult, TxnContext};
    use crate::relstore::types::Datum;

    /// Runs a SQL batch through the session path with a persistent transaction
    /// context (as a TDS connection would), returning the typed outcome.
    fn batch(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> BatchOutcome {
        engine.sql_batch(sql, ctx).expect("sql_batch")
    }

    /// The integer `id` column (column 0) of the first rowset in an outcome.
    fn ids(outcome: &BatchOutcome) -> Vec<i32> {
        for result in &outcome.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset
                    .rows
                    .iter()
                    .map(|row| match row[0] {
                        Datum::TinyInt(v) => v as i32,
                        Datum::SmallInt(v) => v as i32,
                        Datum::Int(v) => v,
                        Datum::BigInt(v) => v as i32,
                        ref other => panic!("expected integer id, got {other:?}"),
                    })
                    .collect();
            }
        }
        panic!("no rowset in outcome: {:?}", outcome.results);
    }

    #[test]
    fn txn_commit_is_durable_across_restart() {
        let path = unique_temp_path("txn-commit-durable");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT TRANSACTION;",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(
            !ctx.has_open_transaction(),
            "COMMIT must close the transaction"
        );

        // Reopen: the committed rows must survive ARIES recovery.
        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1, 2]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_rollback_discards_all_writes() {
        let path = unique_temp_path("txn-rollback");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3); ROLLBACK TRANSACTION;",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(!ctx.has_open_transaction());

        // Only the pre-transaction row 1 remains.
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_trancount_reflects_nesting() {
        let path = unique_temp_path("txn-trancount");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        // Outside any transaction, @@TRANCOUNT is 0.
        let out = batch(&engine, &mut ctx, "SELECT @@TRANCOUNT AS n");
        assert_eq!(ids(&out), vec![0]);

        // Nested BEGINs bump the count; only the outermost COMMIT commits.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; BEGIN TRAN; SELECT @@TRANCOUNT AS n;",
        );
        assert_eq!(ids(&out), vec![2]);
        assert!(ctx.has_open_transaction());

        let out = batch(&engine, &mut ctx, "COMMIT; SELECT @@TRANCOUNT AS n;");
        assert_eq!(ids(&out), vec![1], "inner COMMIT only decrements");
        assert!(
            ctx.has_open_transaction(),
            "transaction still open at count 1"
        );

        batch(&engine, &mut ctx, "COMMIT");
        assert!(!ctx.has_open_transaction());
        let out = batch(&engine, &mut ctx, "SELECT @@TRANCOUNT AS n");
        assert_eq!(ids(&out), vec![0]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_statement_error_rolls_back_statement_not_transaction() {
        // SQL Server default (XACT_ABORT OFF): a non-fatal statement error rolls
        // back only that statement; the transaction stays open and the batch
        // continues past it.
        let path = unique_temp_path("txn-stmt-atomic");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        // The middle INSERT is a duplicate PK (2627, severity 14): it rolls back
        // only itself; the surrounding inserts still apply and COMMIT persists them.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(2627),
            "the duplicate is reported"
        );
        assert!(
            !ctx.has_open_transaction(),
            "COMMIT ran — the transaction was not doomed"
        );
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(
            ids(&out),
            vec![1, 2],
            "the dup rolled back; 1 and 2 committed"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_partial_multirow_insert_is_atomic() {
        // A multi-row INSERT that fails partway undoes ALL its rows (statement
        // atomicity), leaving no partial write in the surviving transaction.
        let path = unique_temp_path("txn-multirow-atomic");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (5)");
        batch(&engine, &mut ctx, "BEGIN TRAN");
        // (6) inserts, then (5) is a duplicate — the whole statement rolls back.
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (6), (5)");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2627));
        batch(&engine, &mut ctx, "COMMIT");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(
            ids(&out),
            vec![5],
            "the partial (6) was undone with the statement"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_error_dooms_transaction_when_xact_abort_on() {
        // SET XACT_ABORT ON: a statement error dooms the whole transaction — only
        // ROLLBACK is then accepted (error 3930).
        let path = unique_temp_path("txn-doomed");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "SET XACT_ABORT ON; BEGIN TRAN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (1);",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2627));

        // A doomed transaction rejects further writes with 3930...
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930));

        // ...but a read is still allowed (so a CATCH can inspect state)...
        let out = batch(&engine, &mut ctx, "SELECT 1 AS n");
        assert_eq!(ids(&out), vec![1]);

        // ...and ROLLBACK is allowed and clears the doom.
        let out = batch(&engine, &mut ctx, "ROLLBACK");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(!ctx.has_open_transaction());

        // The table is usable again and holds nothing (the txn rolled back).
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(ids(&out), Vec::<i32>::new());
        let _ = std::fs::remove_file(path);
    }

    // ---- TRY/CATCH + XACT_STATE() / ERROR_*() (Stage 6) ------------------

    /// Every rowset's integer column 0, in statement order (a batch with
    /// TRY/CATCH can emit several rowsets).
    fn all_int_rows(outcome: &BatchOutcome) -> Vec<Vec<i32>> {
        outcome
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => Some(
                    rowset
                        .rows
                        .iter()
                        .map(|row| match row[0] {
                            Datum::TinyInt(v) => v as i32,
                            Datum::SmallInt(v) => v as i32,
                            Datum::Int(v) => v,
                            Datum::BigInt(v) => v as i32,
                            ref other => panic!("expected integer, got {other:?}"),
                        })
                        .collect(),
                ),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn try_catch_error_transfers_to_catch() {
        let path = unique_temp_path("try-catch-basic");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        // The first TRY statement is a duplicate PK (2627); control jumps to the
        // CATCH, so the second INSERT never runs and the batch reports no error.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY \
               INSERT INTO t VALUES (1); \
               INSERT INTO t VALUES (99); \
             END TRY \
             BEGIN CATCH \
               SELECT ERROR_NUMBER() AS n; \
             END CATCH",
        );
        assert!(
            out.error.is_none(),
            "a caught error is not reported: {:?}",
            out.error
        );
        assert_eq!(ids(&out), vec![2627], "CATCH sees the error number");
        // The post-error TRY statement was skipped; only the seed row exists.
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_catch_error_message_and_null_outside() {
        let path = unique_temp_path("try-catch-msg");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
        );
        let StatementResult::Rows(rowset) = &out.results[0] else {
            panic!("expected a rowset");
        };
        let Datum::NVarChar(msg) = &rowset.rows[0][0] else {
            panic!("expected a string, got {:?}", rowset.rows[0][0]);
        };
        assert!(
            msg.contains("PRIMARY KEY") || msg.to_lowercase().contains("duplicate"),
            "ERROR_MESSAGE() text: {msg}"
        );
        // Outside any CATCH block, ERROR_*() are NULL.
        let out = batch(&engine, &mut ctx, "SELECT ERROR_NUMBER() AS n");
        let StatementResult::Rows(rowset) = &out.results[0] else {
            panic!("expected a rowset");
        };
        assert_eq!(rowset.rows[0][0], Datum::Null);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_catch_canonical_form_without_terminators_runs() {
        // The way TRY/CATCH is actually written: no `;` before END TRY/END CATCH.
        let path = unique_temp_path("try-catch-canonical");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY\n    INSERT INTO t VALUES (1)\nEND TRY\nBEGIN CATCH\n    SELECT ERROR_NUMBER() AS n\nEND CATCH",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![2627]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_catch_success_skips_catch() {
        let path = unique_temp_path("try-catch-ok");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY SELECT 1 AS n; END TRY BEGIN CATCH SELECT 2 AS n; END CATCH",
        );
        assert!(out.error.is_none());
        assert_eq!(all_int_rows(&out), vec![vec![1]], "the CATCH did not run");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_catch_xact_state_committable() {
        // A caught non-fatal error inside a transaction leaves it committable
        // (XACT_STATE = 1); the transaction survives and COMMIT persists its work.
        let path = unique_temp_path("try-catch-xs1");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH; \
             COMMIT;",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![1], "committable (1) inside the CATCH");
        assert!(!ctx.has_open_transaction(), "COMMIT closed the transaction");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(ids(&out), vec![1], "the surviving insert committed");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_catch_xact_state_doomed_under_xact_abort() {
        // Under SET XACT_ABORT ON, an error inside TRY still transfers to CATCH,
        // but the transaction is doomed (XACT_STATE = -1) and only ROLLBACK works.
        let path = unique_temp_path("try-catch-xs-doomed");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH",
        );
        assert!(out.error.is_none(), "the error was caught: {:?}", out.error);
        assert_eq!(ids(&out), vec![-1], "doomed (-1) inside the CATCH");
        assert!(ctx.has_open_transaction(), "still open, awaiting ROLLBACK");
        // A doomed transaction rejects further writes with 3930.
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930));
        // ROLLBACK clears it; nothing persisted.
        batch(&engine, &mut ctx, "ROLLBACK");
        assert!(!ctx.has_open_transaction());
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(ids(&out), Vec::<i32>::new());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn try_catch_nested_inner_handles_outer_continues() {
        // The inner CATCH handles the inner error; because it does not re-raise,
        // the outer TRY continues and the outer CATCH never runs.
        let path = unique_temp_path("try-catch-nested");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY \
               BEGIN TRY INSERT INTO t VALUES (1); END TRY \
               BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH; \
               SELECT 777 AS n; \
             END TRY \
             BEGIN CATCH SELECT 999 AS n; END CATCH",
        );
        assert!(out.error.is_none());
        assert_eq!(
            all_int_rows(&out),
            vec![vec![2627], vec![777]],
            "inner CATCH ran (2627), outer TRY continued (777), outer CATCH skipped"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_ddl_inside_transaction_is_rejected() {
        let path = unique_temp_path("txn-ddl");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; CREATE TABLE t (id INT NOT NULL PRIMARY KEY);",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(226));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_bare_commit_and_rollback_error() {
        let path = unique_temp_path("txn-bare");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        let out = batch(&engine, &mut ctx, "COMMIT TRANSACTION");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3902));

        let out = batch(&engine, &mut ctx, "ROLLBACK TRANSACTION");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3903));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_abort_on_disconnect_rolls_back() {
        let path = unique_temp_path("txn-disconnect");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "BEGIN TRAN; INSERT INTO t VALUES (7);");
        assert!(ctx.has_open_transaction());

        // Simulate the session teardown that CloseSession performs.
        engine.abort_session_txn(&mut ctx);
        assert!(!ctx.has_open_transaction());

        let mut ctx2 = TxnContext::default();
        let out = batch(&engine, &mut ctx2, "SELECT id FROM t");
        assert_eq!(
            ids(&out),
            Vec::<i32>::new(),
            "uncommitted insert was rolled back"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_uncommitted_explicit_txn_is_undone_after_crash() {
        let path = unique_temp_path("txn-crash-undo");
        let engine = new_engine(&path);
        batch(
            &engine,
            &mut TxnContext::default(),
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );

        // Session A opens a transaction and inserts 99 but never commits.
        let mut ctx_a = TxnContext::default();
        batch(
            &engine,
            &mut ctx_a,
            "BEGIN TRAN; INSERT INTO t VALUES (99);",
        );
        assert!(ctx_a.has_open_transaction());

        // An autocommit insert commits, forcing the WAL to disk — including
        // A's (earlier, still-uncommitted) log records.
        batch(
            &engine,
            &mut TxnContext::default(),
            "INSERT INTO t VALUES (1)",
        );

        // Crash: drop the engine and A's context without a graceful rollback
        // (StorageTxn has no Drop, so nothing is committed on the way out).
        drop(ctx_a);
        drop(engine);

        // Recovery on reopen redoes history then undoes the loser (A): row 99
        // is gone, the committed row 1 survives.
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        let out = batch(
            &engine,
            &mut TxnContext::default(),
            "SELECT id FROM t ORDER BY id",
        );
        assert_eq!(ids(&out), vec![1]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn txn_statement_rollback_then_crash_recovers_cleanly() {
        // A statement rolled back to a savepoint writes CLRs; if the transaction
        // then crashes uncommitted, recovery must undo the surviving ops and SKIP
        // the already-compensated statement (follow the CLR chain — never
        // double-undo). This exercises the ARIES correctness of `rollback_to`.
        let path = unique_temp_path("txn-stmt-rollback-crash");
        let engine = new_engine(&path);
        batch(
            &engine,
            &mut TxnContext::default(),
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );

        // Session A: open a transaction, insert 10, hit a duplicate-PK (rolled back
        // to a savepoint under XACT_ABORT OFF), then insert 11 — all uncommitted.
        let mut ctx_a = TxnContext::default();
        batch(
            &engine,
            &mut ctx_a,
            "BEGIN TRAN; INSERT INTO t VALUES (10); INSERT INTO t VALUES (10); INSERT INTO t VALUES (11)",
        );
        assert!(
            ctx_a.has_open_transaction(),
            "the transaction survived the statement error (XACT_ABORT OFF)"
        );

        // An autocommit insert forces A's WAL records — including the compensation
        // CLRs from the rolled-back statement — to disk.
        batch(
            &engine,
            &mut TxnContext::default(),
            "INSERT INTO t VALUES (1)",
        );

        // Crash before A commits.
        drop(ctx_a);
        drop(engine);

        // Recovery undoes A entirely (10 and 11 gone); the compensated duplicate is
        // skipped via its CLR chain (no double-undo / corruption); row 1 survives.
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        let out = batch(
            &engine,
            &mut TxnContext::default(),
            "SELECT id FROM t ORDER BY id",
        );
        assert_eq!(
            ids(&out),
            vec![1],
            "A fully undone; only the committed row survives"
        );
        // The table is writable and 10/11 are free again (fully rolled back).
        batch(
            &engine,
            &mut TxnContext::default(),
            "INSERT INTO t VALUES (10), (11)",
        );
        let out = batch(
            &engine,
            &mut TxnContext::default(),
            "SELECT id FROM t ORDER BY id",
        );
        assert_eq!(ids(&out), vec![1, 10, 11]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn attention_cancel_aborts_a_batch() {
        // A TDS Attention sets the batch's cancel flag; the executor polls it and
        // aborts, returning the internal cancel marker (3617) instead of results.
        // The transaction is not doomed.
        let path = unique_temp_path("attn-cancel");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        for i in 0..10 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i})"))
                .expect("ins");
        }
        // Simulate an Attention arriving: raise the cancel flag for this thread.
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        crate::rel::set_test_cancel(flag);
        let env = sql(&engine, "SELECT id FROM t");
        // Clear before asserting so a panic can't leak the flag to another test.
        crate::rel::clear_test_cancel();
        assert_eq!(
            env["error"]["number"], 3617,
            "a cancelled batch aborts instead of returning rows: {env}"
        );
        // The engine is still usable afterwards.
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM t");
        assert_eq!(rows, vec![vec![Some("10".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn save_transaction_partial_rollback_keeps_earlier_work() {
        // SAVE TRANSACTION + ROLLBACK TRANSACTION <name> undoes only the work done
        // since the savepoint; the transaction stays open and commits the rest.
        let path = unique_temp_path("save-tran");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; INSERT INTO t VALUES (1); SAVE TRANSACTION sp; INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
        );
        // Roll back to the savepoint: 2 and 3 are undone, 1 remains, txn open.
        let out = batch(&engine, &mut ctx, "ROLLBACK TRANSACTION sp");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(ctx.has_open_transaction(), "the transaction stays open");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1], "2 and 3 rolled back; 1 remains");
        // The transaction is still usable and commits the survivors.
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (4); COMMIT");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1, 4]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rollback_to_unknown_savepoint_errors_3908() {
        let path = unique_temp_path("save-tran-missing");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "BEGIN TRAN; INSERT INTO t VALUES (1)");
        let out = batch(&engine, &mut ctx, "ROLLBACK TRANSACTION nope");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3908),
            "rolling back to an unknown savepoint errors 3908"
        );
        // The transaction is untouched — a full ROLLBACK still works.
        batch(&engine, &mut ctx, "ROLLBACK");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn save_transaction_rollback_then_crash_recovers_cleanly() {
        // A ROLLBACK TO savepoint writes CLRs; if the transaction then crashes
        // uncommitted, recovery must undo it all without double-undoing the
        // savepoint-compensated work.
        let path = unique_temp_path("save-tran-crash");
        let engine = new_engine(&path);
        batch(
            &engine,
            &mut TxnContext::default(),
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        let mut ctx_a = TxnContext::default();
        batch(
            &engine,
            &mut ctx_a,
            "BEGIN TRAN; INSERT INTO t VALUES (10); SAVE TRANSACTION sp; INSERT INTO t VALUES (11); ROLLBACK TRANSACTION sp; INSERT INTO t VALUES (12)",
        );
        assert!(ctx_a.has_open_transaction());
        // Force A's WAL (incl. the savepoint-rollback CLRs) to disk.
        batch(
            &engine,
            &mut TxnContext::default(),
            "INSERT INTO t VALUES (1)",
        );
        drop(ctx_a);
        drop(engine);

        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        let out = batch(
            &engine,
            &mut TxnContext::default(),
            "SELECT id FROM t ORDER BY id",
        );
        assert_eq!(
            ids(&out),
            vec![1],
            "A (10/12) fully undone; committed row 1 survives"
        );
        let _ = std::fs::remove_file(path);
    }

    // ---- secondary indexes + planner (Stage 7) -------------------------

    /// Plan text lines for a SELECT under SHOWPLAN_TEXT (one batch so the SET
    /// persists to the SELECT).
    fn plan_lines(engine: &Engine, select: &str) -> Vec<String> {
        let env = sql(engine, &format!("SET SHOWPLAN_TEXT ON; {select}"));
        let results = env["results"].as_array().expect("results array");
        let rows = results
            .iter()
            .find(|r| r["type"] == "rows")
            .unwrap_or_else(|| panic!("no plan rows in {env}"));
        rows["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r[0].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn sql_index_ab_harness_identical_results() {
        let path = unique_temp_path("sql-index-ab");
        let engine = new_engine(&path);
        // Two identical tables; an index only on one.
        for t in ["noidx", "idx"] {
            engine
                .execute(&format!(
                    "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, a INT, name NVARCHAR(20))"
                ))
                .expect("create");
            engine
                .execute(&format!(
                    "INSERT INTO {t} VALUES (1,10,'a'),(2,20,'b'),(3,20,'c'),(4,30,NULL),(5,10,'e')"
                ))
                .expect("insert");
        }
        engine
            .execute("CREATE INDEX ix_a ON idx (a)")
            .expect("create index");

        // Every query returns identical rows whether it scans or seeks.
        for pred in [
            "a = 20",
            "a > 15",
            "a >= 20",
            "a < 25",
            "a = 10 AND id > 1",
            "a <> 20",
        ] {
            let q = |t: &str| format!("SELECT id, a FROM {t} WHERE {pred} ORDER BY id");
            let (_, base) = sql_rows(&engine, &q("noidx"));
            let (_, with_index) = sql_rows(&engine, &q("idx"));
            assert_eq!(base, with_index, "mismatch for predicate `{pred}`");
        }

        // The equality predicate actually uses the index.
        let plan = plan_lines(&engine, "SELECT id FROM idx WHERE a = 20");
        assert!(
            plan.iter()
                .any(|l| l.contains("Index Seek") && l.contains("ix_a")),
            "expected an index seek, got {plan:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_unique_index_rejects_duplicate_2601() {
        let path = unique_temp_path("sql-unique-index");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email NVARCHAR(50))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'a@x'), (2, 'b@x')")
            .expect("insert");
        engine
            .execute("CREATE UNIQUE INDEX ux_email ON t (email)")
            .expect("create unique index");
        // A duplicate email now violates the unique index (2601, not 2627).
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t VALUES (3, 'a@x')"),
            2601
        );
        // Updating to a duplicate also violates it.
        assert_eq!(
            sql_error_number(&engine, "UPDATE t SET email = 'a@x' WHERE id = 2"),
            2601
        );
        // A distinct value is fine.
        engine
            .execute("INSERT INTO t VALUES (3, 'c@x')")
            .expect("distinct insert");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_unique_index_build_rejects_existing_duplicates() {
        let path = unique_temp_path("sql-unique-build");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 5), (2, 5)")
            .expect("insert");
        // Building a unique index over duplicate data fails.
        assert_eq!(
            sql_error_number(&engine, "CREATE UNIQUE INDEX ux_a ON t (a)"),
            2601
        );
        // ...and the failed build left no index behind (still scannable).
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(rows.len(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_index_maintained_across_update_and_delete() {
        let path = unique_temp_path("sql-index-maint");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
            .expect("insert");
        engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");

        // Update moves a row from a=20 to a=25; delete removes a=30.
        engine
            .execute("UPDATE t SET a = 25 WHERE id = 2")
            .expect("update");
        engine
            .execute("DELETE FROM t WHERE a = 30")
            .expect("delete");

        // Index seeks reflect the mutations.
        let (_, at20) = sql_rows(&engine, "SELECT id FROM t WHERE a = 20");
        assert!(at20.is_empty(), "a=20 gone after update");
        let (_, at25) = sql_rows(&engine, "SELECT id FROM t WHERE a = 25");
        assert_eq!(at25, vec![vec![Some("2".into())]]);
        let (_, at30) = sql_rows(&engine, "SELECT id FROM t WHERE a = 30");
        assert!(at30.is_empty(), "a=30 gone after delete");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_showplan_text_reports_seek_versus_scan() {
        let path = unique_temp_path("sql-showplan");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
            .expect("create");
        engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");

        let seek = plan_lines(&engine, "SELECT id FROM t WHERE a = 7");
        assert_eq!(seek[0], "Index Seek(t.ix_a), SEEK: a = 7");
        assert_eq!(seek[1], "Key Lookup(t)");

        // No sargable predicate → a scan.
        let scan = plan_lines(&engine, "SELECT id FROM t WHERE a + 1 = 8");
        assert_eq!(scan, vec!["Table Scan(t)".to_string()]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_index_survives_restart() {
        let path = unique_temp_path("sql-index-restart");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,10),(2,20)")
            .expect("insert");
        engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");

        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        // The index is still usable after recovery.
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE a = 20");
        assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 20");
        assert_eq!(rows, vec![vec![Some("2".into())]]);
        // Maintenance still works post-restart.
        engine
            .execute("INSERT INTO t VALUES (3, 20)")
            .expect("insert after restart");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 20 ORDER BY id");
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_composite_and_descending_index_seek() {
        let path = unique_temp_path("sql-composite-index");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, b INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,1,100),(2,1,200),(3,2,100),(4,2,200)")
            .expect("insert");
        engine
            .execute("CREATE INDEX ix_ab ON t (a, b DESC)")
            .expect("create composite index");

        // Equality on the leading column + range on the second seeks the index.
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE a = 2 AND b = 200");
        assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 2 AND b = 200");
        assert_eq!(rows, vec![vec![Some("4".into())]]);
        // Leading-column-only seek returns both a=1 rows.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 1 ORDER BY id");
        assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("2".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_index_on_heap_table_uses_rid_locator() {
        let path = unique_temp_path("sql-heap-index");
        let engine = new_engine(&path);
        // No PRIMARY KEY → heap table.
        engine
            .execute("CREATE TABLE h (a INT, name NVARCHAR(20))")
            .expect("create heap");
        engine
            .execute("INSERT INTO h VALUES (10,'x'),(20,'y'),(10,'z')")
            .expect("insert");
        engine.execute("CREATE INDEX ix_a ON h (a)").expect("index");

        let plan = plan_lines(&engine, "SELECT name FROM h WHERE a = 10");
        assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
        let (_, mut rows) = sql_rows(&engine, "SELECT name FROM h WHERE a = 10");
        rows.sort();
        assert_eq!(rows, vec![vec![Some("x".into())], vec![Some("z".into())]]);
        // Update through a heap row keeps the index consistent.
        engine
            .execute("UPDATE h SET a = 99 WHERE name = 'x'")
            .expect("update");
        let (_, rows) = sql_rows(&engine, "SELECT name FROM h WHERE a = 10");
        assert_eq!(rows, vec![vec![Some("z".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_drop_index_and_sys_indexes() {
        let path = unique_temp_path("sql-drop-index");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
            .expect("create");
        engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");

        // sys.indexes lists it.
        let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.indexes");
        assert_eq!(rows, vec![vec![Some("ix_a".into())]]);

        engine.execute("DROP INDEX ix_a ON t").expect("drop index");
        let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.indexes");
        assert!(rows.is_empty(), "index gone from catalog");
        // Queries now scan.
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE a = 1");
        assert_eq!(plan, vec!["Table Scan(t)".to_string()]);
        // Dropping a missing index errors 3701.
        assert_eq!(sql_error_number(&engine, "DROP INDEX nope ON t"), 3701);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_nvarchar_equality_seeks_case_insensitively() {
        let path = unique_temp_path("sql-index-nvarchar");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC'), (3, 'xyz')")
            .expect("insert");
        engine
            .execute("CREATE INDEX ix_name ON t (name)")
            .expect("index");

        // Under the default (case-insensitive) collation, equality folds case.
        // The index key is folded the same way, so it still SEEKS (not scans) and
        // the seek finds every case-variant: 'abc' and 'ABC' share one folded key.
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE name = 'abc'");
        assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
        let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'abc'");
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![Some("1".into())], vec![Some("2".into())]],
            "case-insensitive equality matches both 'abc' and 'ABC'"
        );

        // A range on NVARCHAR must NOT seek (UTF-16BE key order can diverge from
        // the filter's order at astral characters); it scans and stays correct.
        // Case-insensitive: 'ABC' folds to 'abc' > 'a', so all three match.
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE name > 'a'");
        assert_eq!(plan, vec!["Table Scan(t)".to_string()]);
        let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE name > 'a'");
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec![Some("1".into())],
                vec![Some("2".into())],
                vec![Some("3".into())]
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_varchar_range_can_index_seek() {
        let path = unique_temp_path("sql-index-varchar");
        let engine = new_engine(&path);
        // VARCHAR keys are UTF-8 bytes, whose order equals code-point order, so
        // a range seek is correct.
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(20))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,'aaa'),(2,'mmm'),(3,'zzz')")
            .expect("insert");
        engine
            .execute("CREATE INDEX ix_code ON t (code)")
            .expect("index");

        let plan = plan_lines(&engine, "SELECT id FROM t WHERE code > 'b'");
        assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
        let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE code > 'b'");
        rows.sort();
        assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_drop_index_is_table_scoped() {
        let path = unique_temp_path("sql-drop-scoped");
        let engine = new_engine(&path);
        // Two tables with same-named indexes; DROP INDEX must only touch the
        // named table's index.
        for t in ["t1", "t2"] {
            engine
                .execute(&format!(
                    "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, a INT)"
                ))
                .expect("create");
            engine
                .execute(&format!("CREATE INDEX ix ON {t} (a)"))
                .expect("index");
        }
        engine.execute("DROP INDEX ix ON t1").expect("drop t1.ix");

        // t2's index survives; t1's is gone.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT object_id FROM sys.indexes ORDER BY object_id",
        );
        assert_eq!(rows.len(), 1, "only t2's index remains");
        let plan = plan_lines(&engine, "SELECT id FROM t2 WHERE a = 1");
        assert!(
            plan.iter().any(|l| l.contains("Index Seek")),
            "t2 still seeks"
        );
        let plan = plan_lines(&engine, "SELECT id FROM t1 WHERE a = 1");
        assert_eq!(plan, vec!["Table Scan(t1)".to_string()], "t1 index dropped");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_create_index_inside_transaction_is_rejected() {
        let path = unique_temp_path("sql-index-in-txn");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
            .expect("create");
        // DDL (incl. CREATE INDEX) is disallowed inside an explicit transaction.
        assert_eq!(
            sql_error_number(&engine, "BEGIN TRAN; CREATE INDEX ix_a ON t (a);"),
            226
        );
        let _ = std::fs::remove_file(path);
    }

    // ---- aggregation, GROUP BY, DISTINCT (Stage 8) ---------------------

    fn agg_setup(label: &str) -> (Engine, PathBuf) {
        let path = unique_temp_path(label);
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, dept NVARCHAR(10), amount INT)",
            )
            .expect("create");
        engine
            .execute(
                "INSERT INTO sales VALUES \
                 (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',NULL),(5,'a',20)",
            )
            .expect("insert");
        (engine, path)
    }

    #[test]
    fn sql_aggregates_over_whole_table() {
        let (engine, path) = agg_setup("agg-whole");
        let (_, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(amount), MAX(amount) FROM sales",
        );
        // COUNT(*)=5, COUNT(amount)=4 (skips NULL), SUM=80, MIN=10, MAX=30.
        assert_eq!(
            rows,
            vec![vec![
                Some("5".into()),
                Some("4".into()),
                Some("80".into()),
                Some("10".into()),
                Some("30".into()),
            ]]
        );
    }

    #[test]
    fn sql_avg_integer_truncates() {
        let (engine, path) = agg_setup("agg-avg");
        // AVG(amount) = 80/4 = 20 exactly here; use a truncating case too.
        let (_, rows) = sql_rows(&engine, "SELECT AVG(amount) FROM sales WHERE dept = 'a'");
        // dept 'a': 10,20,20 -> sum 50 / 3 = 16 (integer truncation).
        assert_eq!(rows, vec![vec![Some("16".into())]]);
    }

    #[test]
    fn sql_group_by_with_aggregates() {
        let (engine, path) = agg_setup("agg-group");
        let (cols, rows) = sql_rows(
            &engine,
            "SELECT dept, COUNT(*), SUM(amount) FROM sales GROUP BY dept ORDER BY dept",
        );
        assert_eq!(cols[0], "dept");
        assert_eq!(
            rows,
            vec![
                vec![Some("a".into()), Some("3".into()), Some("50".into())],
                vec![Some("b".into()), Some("2".into()), Some("30".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_having_filters_groups() {
        let (engine, path) = agg_setup("agg-having");
        let (_, rows) = sql_rows(
            &engine,
            "SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 40 ORDER BY dept",
        );
        assert_eq!(rows, vec![vec![Some("a".into()), Some("50".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_count_distinct() {
        let (engine, path) = agg_setup("agg-distinct");
        // amounts: 10,20,30,NULL,20 -> distinct non-null = {10,20,30} = 3.
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(DISTINCT amount) FROM sales");
        assert_eq!(rows, vec![vec![Some("3".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_select_distinct() {
        let (engine, path) = agg_setup("agg-select-distinct");
        let (_, mut rows) = sql_rows(&engine, "SELECT DISTINCT dept FROM sales");
        rows.sort();
        assert_eq!(rows, vec![vec![Some("a".into())], vec![Some("b".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_order_by_ordinal_and_aggregate() {
        let (engine, path) = agg_setup("agg-order");
        // ORDER BY 2 DESC = order by SUM(amount) descending.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT dept, SUM(amount) FROM sales GROUP BY dept ORDER BY 2 DESC",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("a".into()), Some("50".into())],
                vec![Some("b".into()), Some("30".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_count_star_over_empty_is_zero_but_group_by_is_empty_set() {
        let path = unique_temp_path("agg-empty");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        // No rows: COUNT(*) with no GROUP BY = one row (0); SUM = NULL.
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*), SUM(v) FROM t");
        assert_eq!(rows, vec![vec![Some("0".into()), None]]);
        // With GROUP BY, no rows = zero groups.
        let (_, rows) = sql_rows(&engine, "SELECT v, COUNT(*) FROM t GROUP BY v");
        assert!(rows.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_non_grouped_column_is_error_8120() {
        let (engine, path) = agg_setup("agg-8120");
        // `id` is neither grouped nor aggregated.
        assert_eq!(
            sql_error_number(&engine, "SELECT id, dept FROM sales GROUP BY dept"),
            8120
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_aggregate_in_where_is_error_147() {
        let (engine, path) = agg_setup("agg-147");
        assert_eq!(
            sql_error_number(&engine, "SELECT dept FROM sales WHERE COUNT(*) > 1"),
            147
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_group_by_cast_expression_key() {
        let path = unique_temp_path("agg-cast-key");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,10),(2,10),(3,20)")
            .expect("insert");
        // A CAST group key must match the identical SELECT expression (not
        // wrongly trigger 8120 by recursing into the inner column).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT CAST(v AS BIGINT), COUNT(*) FROM t GROUP BY CAST(v AS BIGINT) ORDER BY 1",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("10".into()), Some("2".into())],
                vec![Some("20".into()), Some("1".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_sum_of_character_column_is_error_8117() {
        let path = unique_temp_path("agg-sum-char");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, s VARCHAR(10))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,'1'),(2,'2'),(3,'3')")
            .expect("insert");
        // SUM/AVG of character data errors (never string-concatenates).
        assert_eq!(sql_error_number(&engine, "SELECT SUM(s) FROM t"), 8117);
        assert_eq!(sql_error_number(&engine, "SELECT AVG(s) FROM t"), 8117);
        let _ = std::fs::remove_file(path);
    }

    // ---- joins (Stage 8 part 2) ----------------------------------------

    fn join_setup(label: &str) -> (Engine, PathBuf) {
        let path = unique_temp_path(label);
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE cust (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
            .expect("create cust");
        engine
            .execute("CREATE TABLE ord (id INT NOT NULL PRIMARY KEY, cust_id INT, amount INT)")
            .expect("create ord");
        engine
            .execute("INSERT INTO cust VALUES (1,'alice'),(2,'bob'),(3,'carol')")
            .expect("insert cust");
        // carol(3) has no orders; order 13 references a missing customer (99).
        engine
            .execute("INSERT INTO ord VALUES (10,1,100),(11,1,200),(12,2,50),(13,99,7)")
            .expect("insert ord");
        (engine, path)
    }

    fn row_count(engine: &Engine, sql: &str) -> usize {
        sql_rows(engine, sql).1.len()
    }

    #[test]
    fn sql_inner_join() {
        let (engine, path) = join_setup("join-inner");
        let (_, rows) = sql_rows(
            &engine,
            "SELECT c.name, o.amount FROM cust c JOIN ord o ON c.id = o.cust_id ORDER BY o.id",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("alice".into()), Some("100".into())],
                vec![Some("alice".into()), Some("200".into())],
                vec![Some("bob".into()), Some("50".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_left_join_keeps_unmatched_left() {
        let (engine, path) = join_setup("join-left");
        // carol has no orders → one row with NULL amount.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT c.name, o.amount FROM cust c LEFT JOIN ord o ON c.id = o.cust_id \
             ORDER BY c.id, o.id",
        );
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[3], vec![Some("carol".into()), None]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_right_join_keeps_unmatched_right() {
        let (engine, path) = join_setup("join-right");
        // order 13 (cust 99) has no customer → NULL name.
        let (_, rows) = sql_rows(
            &engine,
            "SELECT c.name, o.id FROM cust c RIGHT JOIN ord o ON c.id = o.cust_id ORDER BY o.id",
        );
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[3], vec![None, Some("13".into())]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_full_join_keeps_both_unmatched() {
        let (engine, path) = join_setup("join-full");
        // 3 matched + carol (left-only) + order 13 (right-only) = 5 rows.
        assert_eq!(
            row_count(
                &engine,
                "SELECT c.name, o.id FROM cust c FULL JOIN ord o ON c.id = o.cust_id",
            ),
            5
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_cross_join_and_comma() {
        let (engine, path) = join_setup("join-cross");
        // 3 customers x 4 orders = 12.
        assert_eq!(
            row_count(&engine, "SELECT c.id, o.id FROM cust c CROSS JOIN ord o"),
            12
        );
        assert_eq!(
            row_count(&engine, "SELECT c.id, o.id FROM cust c, ord o"),
            12
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_join_with_where_and_qualified_wildcard() {
        let (engine, path) = join_setup("join-where");
        let (cols, rows) = sql_rows(
            &engine,
            "SELECT c.* FROM cust c JOIN ord o ON c.id = o.cust_id WHERE o.amount > 100 ORDER BY o.id",
        );
        // c.* expands to cust columns; only order 11 (amount 200, alice).
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(rows, vec![vec![Some("1".into()), Some("alice".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_aggregate_over_join() {
        let (engine, path) = join_setup("join-agg");
        // Total amount per customer (inner join).
        let (_, rows) = sql_rows(
            &engine,
            "SELECT c.name, SUM(o.amount) FROM cust c JOIN ord o ON c.id = o.cust_id \
             GROUP BY c.name ORDER BY c.name",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("alice".into()), Some("300".into())],
                vec![Some("bob".into()), Some("50".into())],
            ]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_ambiguous_column_errors() {
        let (engine, path) = join_setup("join-ambig");
        // `id` exists in both cust and ord → ambiguous (SQL Server 209).
        let err = sql_error_number(
            &engine,
            "SELECT id FROM cust c JOIN ord o ON c.id = o.cust_id",
        );
        assert_eq!(err, 209, "ambiguous column should be 209");
        // A genuinely missing column is still 207 (invalid), not 209.
        let missing = sql_error_number(
            &engine,
            "SELECT nope FROM cust c JOIN ord o ON c.id = o.cust_id",
        );
        assert_eq!(missing, 207, "unknown column should be 207");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_grouped_coercion_error_is_not_swallowed() {
        let path = unique_temp_path("agg-coerce");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, g INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,1),(2,123456)")
            .expect("insert");
        // A heterogeneous grouped output (short string in one group, a large
        // integer in another) must raise the truncation error, not mask it as
        // NULL — matching the plain-projection path.
        let plain = sql_error_number(&engine, "SELECT CASE WHEN g = 1 THEN 'x' ELSE g END FROM t");
        let grouped = sql_error_number(
            &engine,
            "SELECT CASE WHEN g = 1 THEN 'x' ELSE g END FROM t GROUP BY g",
        );
        assert_eq!(plain, grouped, "grouped path must raise the same error");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_non_boolean_where_is_rejected_4145() {
        let path = unique_temp_path("sql-where-4145");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        // `WHERE id + 1` is numeric, not boolean.
        assert_eq!(
            sql_error_number(&engine, "SELECT id FROM t WHERE id + 1"),
            4145
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_schema_qualified_names_resolve() {
        let path = unique_temp_path("sql-dbo");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE dbo.products (id INT NOT NULL PRIMARY KEY)")
            .expect("create dbo.");
        engine
            .execute("INSERT INTO dbo.products VALUES (1)")
            .expect("insert dbo.");
        // Reachable by both qualified and bare names.
        let (_, rows) = sql_rows(&engine, "SELECT id FROM products");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let (_, rows) = sql_rows(&engine, "SELECT id FROM dbo.products");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_unicode_round_trips_through_insert_and_select() {
        let path = unique_temp_path("sql-unicode");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'café åäö 😀')")
            .expect("insert");
        let (_, rows) = sql_rows(&engine, "SELECT name FROM t");
        assert_eq!(rows, vec![vec![Some("café åäö 😀".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_bigint_overflow_literal_errors_not_saturates() {
        let path = unique_temp_path("sql-bigint-of");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, big BIGINT)")
            .expect("create");
        // 1e30 overflows i64; must error, not silently saturate.
        assert_eq!(
            sql_error_number(
                &engine,
                "INSERT INTO t VALUES (1, 1000000000000000000000000000000)"
            ),
            220
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sql_table_level_pk_column_is_not_null() {
        let path = unique_temp_path("sql-tablepk");
        let engine = new_engine(&path);
        // A table-level PK on a column with no explicit nullability succeeds
        // (the column is promoted to NOT NULL).
        engine
            .execute("CREATE TABLE t (id INT, v NVARCHAR(10), PRIMARY KEY (id))")
            .expect("create");
        // Inserting NULL into the PK column is then a NOT NULL violation.
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t (v) VALUES ('x')"),
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
            default_collation: None,
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

    // ---- Stage 5: collation-aware equality (case-insensitive default) --------
    //
    // The database default collation is `..._CI_AS` (case-insensitive), so string
    // equality, grouping, DISTINCT, joins, key seeks, and uniqueness all fold
    // case unless a column is declared `_CS`/`_BIN`. These tests exercise every
    // folded-key path — a missed fold site would surface as a wrong result here.

    #[test]
    fn collation_ci_where_and_predicates_fold_case() {
        let path = unique_temp_path("coll-ci-where");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR(20))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC'), (3, 'Xyz')")
            .expect("insert");
        // Equality (scan path, no index): matches every case-variant.
        let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'aBc' ORDER BY id");
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![Some("1".into())], vec![Some("2".into())]],
            "CI equality matches both cases"
        );
        // IN, LIKE, BETWEEN all fold case too.
        let (_, r) = sql_rows(&engine, "SELECT COUNT(*) FROM t WHERE name IN ('ABC')");
        assert_eq!(r, vec![vec![Some("2".into())]], "CI IN");
        let (_, r) = sql_rows(&engine, "SELECT COUNT(*) FROM t WHERE name LIKE 'X%'");
        assert_eq!(r, vec![vec![Some("1".into())]], "CI LIKE matches 'Xyz'");
        let (_, r) = sql_rows(
            &engine,
            "SELECT COUNT(*) FROM t WHERE name BETWEEN 'A' AND 'D'",
        );
        assert_eq!(r, vec![vec![Some("2".into())]], "CI BETWEEN folds bounds");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_char_pk_uniqueness_and_lookup() {
        let path = unique_temp_path("coll-ci-pk");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (code VARCHAR(10) NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES ('ABC', 1)")
            .expect("ins1");
        // A case-variant is a duplicate PK under the case-insensitive collation.
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t VALUES ('abc', 2)"),
            2627,
            "CI clustered PK rejects a case-variant duplicate"
        );
        // A clustered PK lookup by a case-variant finds the stored row.
        let (_, rows) = sql_rows(&engine, "SELECT v FROM t WHERE code = 'aBc'");
        assert_eq!(rows, vec![vec![Some("1".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_unique_index_folds_case() {
        let path = unique_temp_path("coll-ci-uix");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(50))")
            .expect("create");
        engine
            .execute("CREATE UNIQUE INDEX ux ON t (email)")
            .expect("uix");
        engine
            .execute("INSERT INTO t VALUES (1, 'A@B.com')")
            .expect("ins1");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO t VALUES (2, 'a@b.COM')"),
            2601,
            "CI unique index rejects a case-variant duplicate"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_join_and_grouping_fold_case() {
        let path = unique_temp_path("coll-ci-join");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE l (id INT NOT NULL PRIMARY KEY, k VARCHAR(10))")
            .expect("l");
        engine
            .execute("CREATE TABLE r (id INT NOT NULL PRIMARY KEY, k VARCHAR(10))")
            .expect("r");
        engine
            .execute("INSERT INTO l VALUES (1, 'abc'), (2, 'ABC'), (3, 'zzz')")
            .expect("l ins");
        engine
            .execute("INSERT INTO r VALUES (10, 'ABC'), (20, 'aBc')")
            .expect("r ins");
        // Hash join equi-key folds: every l/r case-variant pair matches (2*2 = 4).
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM l JOIN r ON l.k = r.k");
        assert_eq!(
            rows,
            vec![vec![Some("4".into())]],
            "CI join matches all case pairs"
        );
        // GROUP BY folds case: 'abc'/'ABC' form one group of 2, 'zzz' one of 1.
        let (_, mut rows) = sql_rows(&engine, "SELECT COUNT(*) FROM l GROUP BY k");
        rows.sort();
        assert_eq!(
            rows,
            vec![vec![Some("1".into())], vec![Some("2".into())]],
            "CI GROUP BY collapses case-variants into 2 groups (sizes 1 and 2)"
        );
        // DISTINCT collapses case-variants to {abc, zzz}; COUNT(DISTINCT) counts once.
        let (_, rows) = sql_rows(&engine, "SELECT DISTINCT k FROM l");
        assert_eq!(rows.len(), 2, "CI DISTINCT collapses 'abc'/'ABC': {rows:?}");
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(DISTINCT k) FROM l");
        assert_eq!(rows, vec![vec![Some("2".into())]], "CI COUNT(DISTINCT)");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_foreign_key_folds_case() {
        let path = unique_temp_path("coll-ci-fk");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE parent (code VARCHAR(10) NOT NULL PRIMARY KEY)")
            .expect("parent");
        engine
            .execute(
                "CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pcode VARCHAR(10) \
                 REFERENCES parent(code))",
            )
            .expect("child");
        engine
            .execute("INSERT INTO parent VALUES ('ABC')")
            .expect("parent ins");
        // A child FK value matches the parent case-insensitively (folded probe).
        engine
            .execute("INSERT INTO child VALUES (1, 'aBc')")
            .expect("child ins with case-variant FK");
        // Deleting the parent is blocked by the case-variant child reference.
        assert_eq!(
            sql_error_number(&engine, "DELETE FROM parent WHERE code = 'ABC'"),
            547,
            "CI FK: child 'aBc' still references parent 'ABC'"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_cs_override_is_case_sensitive() {
        let path = unique_temp_path("coll-cs");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (code VARCHAR(10) COLLATE Latin1_General_CS_AS NOT NULL PRIMARY KEY)",
            )
            .expect("create cs pk");
        // Under a case-sensitive collation, case-variants are distinct keys.
        engine
            .execute("INSERT INTO t VALUES ('ABC')")
            .expect("ins ABC");
        engine
            .execute("INSERT INTO t VALUES ('abc')")
            .expect("case-sensitive PK admits 'abc' alongside 'ABC'");
        // Equality is exact: only the matching case.
        let (_, rows) = sql_rows(&engine, "SELECT code FROM t WHERE code = 'abc'");
        assert_eq!(rows, vec![vec![Some("abc".into())]], "CS equality is exact");
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM t");
        assert_eq!(rows, vec![vec![Some("2".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_folded_keys_survive_restart() {
        let path = unique_temp_path("coll-ci-restart");
        {
            let engine = new_engine(&path);
            engine
                .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR(20))")
                .expect("create");
            engine
                .execute("INSERT INTO t VALUES (1, 'Hello'), (2, 'WORLD')")
                .expect("insert");
            engine
                .execute("CREATE INDEX ix ON t (name)")
                .expect("index");
        }
        // Reopen: the folded index keys on disk must still seek case-insensitively.
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("engine");
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE name = 'hello'");
        assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'hello'");
        assert_eq!(
            rows,
            vec![vec![Some("1".into())]],
            "folded seek survives restart"
        );
        let _ = std::fs::remove_file(path);
    }

    // ---- Regression tests for adversarial-review findings on #82 -------------

    #[test]
    fn collation_ci_group_by_spill_folds_case() {
        // The grace-hash GROUP BY spill path must partition by the FOLDED key, or
        // case-variant groups split across partitions on large input. 'World'/
        // 'world' hash to *different* partitions unfolded (unlike 'ABC'/'abc',
        // which collide by luck), so this pair reliably catches the bug.
        let path = unique_temp_path("coll-ci-agg-spill");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k VARCHAR(10))")
            .expect("create");
        for i in 0..200 {
            let k = if i % 2 == 0 { "World" } else { "world" };
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, '{k}')"))
                .expect("ins");
        }
        let query = "SELECT COUNT(*) FROM t GROUP BY k";
        let (_, reference) = sql_rows(&engine, query);
        assert_eq!(
            reference,
            vec![vec![Some("200".into())]],
            "in-memory: one case-insensitive group of 200"
        );
        crate::rel::set_test_sort_budget(Some(400));
        let (_, spilled) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);
        assert_eq!(
            spilled, reference,
            "spilled GROUP BY must fold case like the in-memory path (one group)"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_cs_column_delete_is_exact() {
        // UPDATE/DELETE WHERE on a _CS column must compare exactly — a plain
        // Vec<String> resolver would report the CI default for every column and
        // delete case-variant rows it must keep (data loss).
        let path = unique_temp_path("coll-cs-dml");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) COLLATE Latin1_General_CS_AS)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC')")
            .expect("ins");
        engine
            .execute("DELETE FROM t WHERE code = 'abc'")
            .expect("delete");
        let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![vec![Some("2".into())]],
            "CS DELETE removes only the exact 'abc' (id=1), keeps 'ABC' (id=2)"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_self_ref_fk_same_statement() {
        // A self-referencing FK to a case-variant sibling in the SAME statement
        // must resolve case-insensitively (the sibling isn't committed yet, so the
        // batch match — not the folded rel_get — is the only satisfying path).
        let path = unique_temp_path("coll-ci-selfref");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id VARCHAR(10) NOT NULL PRIMARY KEY, pid VARCHAR(10) REFERENCES t(id))",
            )
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES ('ABC', NULL), ('X', 'abc')")
            .expect("case-variant self-reference to a same-statement sibling");
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM t");
        assert_eq!(rows, vec![vec![Some("2".into())]]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_cs_distinct_is_exact() {
        // DISTINCT on a _CS column must keep case-variants distinct, consistent
        // with GROUP BY and COUNT(DISTINCT) on the same column.
        let path = unique_temp_path("coll-cs-distinct");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) COLLATE Latin1_General_CS_AS)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC')")
            .expect("ins");
        let (_, rows) = sql_rows(&engine, "SELECT DISTINCT code FROM t");
        assert_eq!(rows.len(), 2, "CS DISTINCT keeps both cases: {rows:?}");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_mixed_fk_still_enforced_on_delete() {
        // A mixed-collation FK (child _CS, parent CI) must not take the index fast
        // path — that folds by the child collation and would miss a case-variant
        // reference (child 'abc' → parent 'ABC'), wrongly allowing the parent
        // delete. The collation-match gate routes it to the parent-collation scan.
        let path = unique_temp_path("coll-mixed-fk");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE parent (code VARCHAR(10) NOT NULL PRIMARY KEY)")
            .expect("parent");
        engine
            .execute(
                "CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pcode VARCHAR(10) \
                 COLLATE Latin1_General_CS_AS REFERENCES parent(code))",
            )
            .expect("child");
        engine
            .execute("CREATE INDEX ix ON child (pcode)")
            .expect("index"); // would otherwise enable the fast path
        engine
            .execute("INSERT INTO parent VALUES ('ABC')")
            .expect("parent ins");
        engine
            .execute("INSERT INTO child VALUES (1, 'abc')")
            .expect("child references parent case-insensitively");
        assert_eq!(
            sql_error_number(&engine, "DELETE FROM parent WHERE code = 'ABC'"),
            547,
            "mixed-collation FK is still enforced (child 'abc' references parent 'ABC')"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_cs_having_is_exact() {
        // HAVING (and the SELECT projection) evaluate against a synthetic group
        // row; on a _CS grouping column they must compare exactly, or a HAVING
        // re-merges groups that grouping kept apart.
        let path = unique_temp_path("coll-cs-having");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) COLLATE Latin1_General_CS_AS)",
            )
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC')")
            .expect("ins");
        let (_, rows) = sql_rows(
            &engine,
            "SELECT code FROM t GROUP BY code HAVING code = 'ABC'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("ABC".into())]],
            "CS HAVING matches only the exact 'ABC' group"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn collation_ci_self_ref_fk_update() {
        // UPDATE setting a self-referencing FK to a case-variant of an existing
        // PK must succeed under the case-insensitive collation.
        let path = unique_temp_path("coll-ci-selfref-upd");
        let engine = new_engine(&path);
        engine
            .execute(
                "CREATE TABLE t (id VARCHAR(10) NOT NULL PRIMARY KEY, pid VARCHAR(10) REFERENCES t(id))",
            )
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES ('ABC', NULL), ('X', NULL)")
            .expect("ins");
        engine
            .execute("UPDATE t SET pid = 'abc' WHERE id = 'X'")
            .expect("case-variant self-reference resolves on UPDATE");
        let (_, rows) = sql_rows(&engine, "SELECT pid FROM t WHERE id = 'X'");
        assert_eq!(rows, vec![vec![Some("abc".into())]], "UPDATE applied");
        let _ = std::fs::remove_file(path);
    }

    // ---- Stage 6: the row-at-a-time single-table scan ------------------------

    /// The first rowset in an outcome.
    fn first_rowset(outcome: &BatchOutcome) -> &crate::rel::RowSet {
        for result in &outcome.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset;
            }
        }
        panic!("no rowset in outcome: {:?}", outcome.results);
    }

    #[test]
    fn the_scan_path_returns_exactly_what_the_collecting_path_returns() {
        // The whole compatibility claim of the row-at-a-time path is that a
        // caller cannot tell it apart, so the oracle is the collecting path
        // itself: every shape the gate accepts must produce the identical
        // RowSet — same columns, same types, same rows, same order — through
        // both. `without_scan_path` makes the gate decline, which is the only
        // difference between the two runs.
        let path = unique_temp_path("scan-ab");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();

        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE ab (id INT PRIMARY KEY, v INT, s VARCHAR(20), n INT NULL)",
        );
        for i in 0..300 {
            let s = format!("row{i}");
            let n = if i % 3 == 0 {
                "NULL".to_string()
            } else {
                i.to_string()
            };
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO ab VALUES ({i}, {}, '{s}', {n})", i * 2),
            );
        }
        // A heap (no PK) as well, since the two take different cursors.
        batch(&engine, &mut ctx, "CREATE TABLE hp (id INT, v INT)");
        for i in 0..300 {
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO hp VALUES ({i}, {i})"),
            );
        }
        // A secondary index, so the seek access path is compared as well as the
        // scan — `plan::choose` only considers secondary indexes, so a PK
        // equality is not a seek here.
        batch(&engine, &mut ctx, "CREATE INDEX ix_ab_v ON ab (v)");

        let queries = [
            // Bare columns, wildcards, aliases, qualified names.
            "SELECT * FROM ab",
            "SELECT id FROM ab",
            "SELECT v, id FROM ab",
            "SELECT ab.* FROM ab",
            "SELECT a.* FROM ab a",
            "SELECT id AS ident, v AS value FROM ab",
            "SELECT a.v FROM ab a",
            "SELECT a.v AS vv FROM ab a",
            "SELECT * FROM ab a",
            // The projection may repeat and reorder columns.
            "SELECT v, v, id, s FROM ab",
            // WHERE, including NULL/3VL and a non-sargable predicate on a PK.
            "SELECT id FROM ab WHERE v > 100",
            "SELECT id FROM ab WHERE n IS NULL",
            "SELECT id FROM ab WHERE n > 50",
            "SELECT id FROM ab WHERE s = 'ROW7'",
            "SELECT id FROM ab WHERE id + 0 > 297",
            "SELECT id FROM ab WHERE v > 100 AND n IS NOT NULL",
            "SELECT id FROM ab WHERE 1 = 0",
            // TOP, with and without a filter, at and past the row count.
            "SELECT TOP 5 id FROM ab",
            "SELECT TOP 1 id FROM ab",
            "SELECT TOP 5 id FROM ab WHERE v > 100",
            "SELECT TOP 1000 id FROM ab",
            "SELECT TOP 5 * FROM ab",
            // The seek access path: `v` is indexed, so these choose IndexSeek
            // and its candidates are re-filtered and projected the same way.
            "SELECT id FROM ab WHERE v = 100",
            "SELECT id, v FROM ab WHERE v = 100",
            "SELECT * FROM ab WHERE v > 500",
            "SELECT * FROM ab WHERE v >= 100 AND v <= 200",
            "SELECT TOP 3 id FROM ab WHERE v > 100",
            "SELECT id FROM ab WHERE v = 99999",
            // The heap: 300 rows is inside one 1024-row slice either way.
            "SELECT id FROM hp",
            "SELECT TOP 3 id FROM hp",
            "SELECT id FROM hp WHERE v > 290",
        ];

        for query in queries {
            // Both guards are needed, and for the same reason: an A/B whose two
            // sides run the same code agrees with itself. The first proves the
            // scan path ran; the second proves `without_scan_path` really took
            // it away, which nothing else here would notice if it stopped
            // working.
            let before = engine.storage.scan_selects();
            let streamed = batch(&engine, &mut ctx, query);
            assert_eq!(
                engine.storage.scan_selects(),
                before + 1,
                "{query} did not take the scan path, so comparing it proves nothing"
            );
            let before = engine.storage.scan_selects();
            let collected = crate::rel::without_scan_path(|| batch(&engine, &mut ctx, query));
            assert_eq!(
                engine.storage.scan_selects(),
                before,
                "{query} took the scan path for both runs, so it was compared with itself"
            );
            assert!(streamed.error.is_none(), "{query}: {:?}", streamed.error);
            assert!(collected.error.is_none(), "{query}: {:?}", collected.error);
            assert_eq!(
                first_rowset(&streamed),
                first_rowset(&collected),
                "{query} differs between the scan path and the collecting path"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn top_stops_the_scan_rather_than_reading_the_table_and_truncating() {
        // The collecting path reads every row, then truncates. Counting slices
        // is what tells the two apart: the rows returned are identical either
        // way, so a result-only assertion would pass without the early exit.
        let path = unique_temp_path("scan-top");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE big (id INT PRIMARY KEY, v INT)",
        );
        // Several slices' worth (SCAN_SLICE_ROWS is 1024), so "stopped early"
        // and "read it all" differ by more than rounding.
        for i in 0..3000 {
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO big VALUES ({i}, {i})"),
            );
        }

        let before = engine.storage.scan_slices();
        let out = batch(&engine, &mut ctx, "SELECT TOP 1 id FROM big");
        let slices = engine.storage.scan_slices() - before;
        assert_eq!(first_rowset(&out).rows.len(), 1, "TOP 1 returns one row");
        assert_eq!(
            slices, 1,
            "TOP 1 must read one slice, not walk the whole table"
        );

        // The counter means what the assertions above assume: an unlimited scan
        // of the same table reads every slice. Without this the two could pass
        // against a scan that never ran.
        let before = engine.storage.scan_slices();
        let out = batch(&engine, &mut ctx, "SELECT id FROM big");
        assert_eq!(first_rowset(&out).rows.len(), 3000);
        assert!(
            engine.storage.scan_slices() - before >= 3,
            "3000 rows at 1024 per slice is at least three slices"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn top_0_is_left_to_the_collecting_path_so_an_invalid_where_still_errors() {
        // The engine has no separate binding pass: an unresolvable column (207)
        // and a non-boolean predicate (4145) are both raised by *evaluating* the
        // predicate on a row. `TOP 0` wants no rows, so a scan path that honours
        // it evaluates nothing and answers an invalid query with an empty result
        // set instead of rejecting it. The gate declines TOP 0 for that reason.
        let path = unique_temp_path("scan-top0");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE p (id INT PRIMARY KEY, v INT)",
        );
        for i in 1..4 {
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO p VALUES ({i}, {i})"),
            );
        }

        for (query, expected) in [
            ("SELECT TOP 0 id FROM p WHERE bogus = 1", 207),
            ("SELECT TOP 0 id FROM p WHERE id", 4145),
        ] {
            assert_eq!(
                batch(&engine, &mut ctx, query).error.map(|e| e.number),
                Some(expected),
                "{query} must still be rejected, not answered with no rows"
            );
        }
        // The same errors without TOP, so the cases above are about TOP 0 and
        // not about a query that is broken some other way.
        for (query, expected) in [
            ("SELECT id FROM p WHERE bogus = 1", 207),
            ("SELECT id FROM p WHERE id", 4145),
        ] {
            assert_eq!(
                batch(&engine, &mut ctx, query).error.map(|e| e.number),
                Some(expected),
                "{query}"
            );
        }
        // And a valid TOP 0 still answers with an empty result set.
        let out = batch(&engine, &mut ctx, "SELECT TOP 0 id FROM p");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(first_rowset(&out).rows.is_empty(), "TOP 0 returns no rows");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_sargable_where_still_seeks_its_index_instead_of_scanning() {
        // The scan path takes the planner's access path rather than declining a
        // seek — declining would throw away the table definition, the schema and
        // the choice, all of which build_table_source would then recompute. A
        // results-only test cannot see which path ran; the slice counter can.
        let path = unique_temp_path("scan-seek");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE sk (id INT PRIMARY KEY, v INT)",
        );
        for i in 0..2000 {
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO sk VALUES ({i}, {i})"),
            );
        }
        // A secondary index: `plan::choose` only considers those, so the
        // clustered PK is not a seekable path here.
        batch(&engine, &mut ctx, "CREATE INDEX ix_sk_v ON sk (v)");

        let slices = engine.storage.scan_slices();
        let selects = engine.storage.scan_selects();
        let out = batch(&engine, &mut ctx, "SELECT id FROM sk WHERE v = 1500");
        assert_eq!(
            engine.storage.scan_slices() - slices,
            0,
            "an equality on an indexed column must seek, not scan"
        );
        assert_eq!(
            engine.storage.scan_selects() - selects,
            1,
            "and the seek is still answered on the scan path, not handed back"
        );
        assert_eq!(first_rowset(&out).rows.len(), 1, "the seek finds the row");

        // The same column, with the predicate made non-sargable, does scan — so
        // the assertion above is about the plan, not about a dead counter.
        let before = engine.storage.scan_slices();
        let out = batch(&engine, &mut ctx, "SELECT id FROM sk WHERE v + 0 = 1500");
        assert!(
            engine.storage.scan_slices() - before > 0,
            "a non-sargable predicate scans"
        );
        assert_eq!(first_rowset(&out).rows.len(), 1, "and finds the same row");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_sys_catalog_view_is_not_answered_with_a_user_table_of_the_same_name() {
        // `build_table_source` answers `sys.tables` by its full name *before*
        // any catalog lookup. The gate has to apply that precedence itself: a
        // quoted `[sys.tables]` is a creatable, insertable user table, so a gate
        // that resolved the catalog first would scan it and answer the query
        // with its columns — silently, since both are perfectly good rowsets.
        let path = unique_temp_path("scan-sys");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE TABLE [sys.tables] (id INT PRIMARY KEY, decoy INT)",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        batch(&engine, &mut ctx, "INSERT INTO [sys.tables] VALUES (1, 9)");

        let out = batch(&engine, &mut ctx, "SELECT * FROM sys.tables");
        assert!(out.error.is_none(), "{:?}", out.error);
        let columns: Vec<&str> = first_rowset(&out)
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            columns,
            vec!["object_id", "name"],
            "sys.tables is the catalog view, not the user table shadowing its name"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_view_is_not_scanned_as_if_it_were_its_own_base_table() {
        // A view's rows come from running its SELECT; its `root_page` is not a
        // table's. Scanning one as a base table would read whatever object that
        // page belongs to.
        let path = unique_temp_path("scan-view");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE vt (id INT PRIMARY KEY, v INT)",
        );
        for i in 0..10 {
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO vt VALUES ({i}, {})", i * 10),
            );
        }
        batch(
            &engine,
            &mut ctx,
            "CREATE VIEW vv AS SELECT id, v FROM vt WHERE v >= 50",
        );

        // `SELECT *` is the query that finds this: a view's TableDef carries no
        // columns, so the wildcard expands to none and the gate would accept a
        // plan that projects nothing — then scan the view's `root_page` of 0,
        // which is the catalog root, not the view.
        let out = batch(&engine, &mut ctx, "SELECT * FROM vv");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            first_rowset(&out).columns.len(),
            2,
            "the view's columns: {:?}",
            first_rowset(&out).columns
        );
        assert_eq!(first_rowset(&out).rows.len(), 5, "the view's own rows");
        let _ = std::fs::remove_file(path);
    }
}
