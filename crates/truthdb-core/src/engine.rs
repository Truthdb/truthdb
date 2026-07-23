use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

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
/// WRITE ([`Self::execute`] on a mutating command) takes `meta.write()` and so
/// runs exclusively; a native SEARCH takes `meta.read()` like the batches (it
/// is `&self` throughout, and readers must scale). Without the write gate, a
/// concurrent native batch could read a relational batch's half-applied
/// writes — which the old single-threaded actor prevented for free.
pub struct Engine {
    storage: Arc<Storage>,
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
            storage: Arc::new(storage),
            meta: RwLock::new(meta),
        })
    }

    /// A shared handle to the underlying storage, for subsystems that run
    /// beside the engine's worker pool — the replication listener/sender on a
    /// primary and the replication receiver on a standby. `Storage` is
    /// internally synchronized, so the tasks and the workers share it safely.
    pub fn storage_arc(&self) -> Arc<Storage> {
        Arc::clone(&self.storage)
    }

    /// The underlying storage handle, so a test can drive an online backup
    /// while the engine is live.
    #[cfg(test)]
    pub(crate) fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn execute(&self, input: &str) -> Result<String, EngineError> {
        // Routing: the legacy ES commands all carry a `{` JSON body; that
        // shape routes to the frozen search path. Everything else is SQL.
        match parse_command(input)? {
            // A search only reads: it takes the gate SHARED, alongside other
            // searches and SQL batches (which already read-lock it), so
            // concurrent readers scale instead of serializing on the write
            // gate. Measured: 8-connection search throughput went from HALF
            // of 1-connection (2072 -> 966 ops/sec) to scaling with readers.
            Some(Command::Search { index, query }) => {
                let meta = self.meta.read().expect("engine meta poisoned");
                Self::render_search(&meta, &index, &query)
            }
            // Every other ES command writes; exclusive as before.
            Some(command) => {
                let mut meta = self.meta.write().expect("engine meta poisoned");
                self.execute_es(&mut meta, command)
            }
            None => {
                let mut meta = self.meta.write().expect("engine meta poisoned");
                self.execute_sql(&mut meta, input)
            }
        }
    }

    /// Reads a login's stored credential for the TDS handshake. This is a
    /// session-less catalog read: the handshake runs before any session or
    /// transaction exists. Returns `None` when no such login is registered.
    pub fn lookup_login(&self, name: &str) -> Option<crate::session::LoginRecord> {
        let def = self.storage.rel_login(name)?;
        let principal = def.principal?;
        Some(crate::session::LoginRecord {
            principal_id: def.object_id,
            name: def.name,
            password_blob: principal.password_blob,
            is_disabled: principal.is_disabled,
        })
    }

    /// First-boot migration of config-file `[tds.auth]` users into catalog
    /// logins. Each configured user becomes an enabled login; `sa` is always
    /// ensured — enabled with its configured password, or, when the config
    /// supplied none, created DISABLED with an unguessable random password so the
    /// principal exists (SUSER_SNAME, the dbo↔sa mapping) but cannot authenticate
    /// until an admin resets it over the unauthenticated native admin protocol.
    /// After this runs, `[tds.auth]` is dead for authentication: the catalog is
    /// authoritative, and a login is created here only if it does not already
    /// exist. Returns the names created, for startup logging.
    ///
    /// `sa` is created LAST and doubles as the completion marker: the migration
    /// is skipped entirely once `sa` exists. Because ARIES recovery restores only
    /// a contiguous durable prefix of the log, `sa` present after a crash implies
    /// every login written before it is durable too — so a crash mid-migration
    /// leaves `sa` absent and the whole thing simply re-runs, with the
    /// per-login existence check making that re-run idempotent (it also collapses
    /// a case-variant duplicate config key onto the first-seen login rather than
    /// erroring). This runs before the engine thread is spawned, single-threaded.
    pub fn migrate_logins(
        &self,
        config_users: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, EngineError> {
        if self.storage.rel_login("sa").is_some() {
            return Ok(Vec::new());
        }
        let mut created = Vec::new();
        let mut sa_password: Option<&String> = None;
        for (name, password) in config_users {
            if name.eq_ignore_ascii_case("sa") {
                sa_password = Some(password);
                continue; // ensured last, as the completion marker
            }
            if self.storage.rel_login(name).is_some() {
                continue; // already migrated (idempotent re-run or case-dup key)
            }
            self.storage.rel_create_login(
                name,
                crate::relstore::catalog::PrincipalDef::login(
                    crate::auth::hash_password(password),
                    false,
                ),
            )?;
            created.push(name.clone());
        }
        let (password_blob, is_disabled, label) = match sa_password {
            Some(password) => (
                crate::auth::hash_password(password),
                false,
                "sa".to_string(),
            ),
            None => (
                crate::auth::hash_random_password(),
                true,
                "sa (disabled — no password configured)".to_string(),
            ),
        };
        self.storage.rel_create_login(
            "sa",
            crate::relstore::catalog::PrincipalDef::login(password_blob, is_disabled),
        )?;
        created.push(label);
        Ok(created)
    }

    /// Resolves a login (its name and server principal_id) to the database user
    /// a new session runs as, and that user's database principal_id: a member of
    /// the sysadmin server role maps to `dbo`; otherwise the user created `FOR
    /// LOGIN` it, if any; otherwise the login name itself with no database
    /// principal (id 0). `login_sid` 0 is the identity-less native path.
    pub(crate) fn resolve_session_user(&self, login: &str, login_sid: u32) -> (String, u32) {
        if login_sid != 0
            && self
                .storage
                .effective_roles(login_sid)
                .contains(&crate::storage::SYSADMIN_ID)
        {
            return ("dbo".to_string(), crate::storage::DBO_ID);
        }
        if login_sid != 0
            && let Some(def) = self
                .storage
                .rel_database_principals()
                .into_iter()
                .find(|d| d.principal.as_ref().and_then(|p| p.login_sid) == Some(login_sid))
        {
            return (def.name.clone(), def.object_id);
        }
        (login.to_string(), 0)
    }

    #[cfg(test)]
    pub(crate) fn storage_effective_roles_for_test(
        &self,
        id: u32,
    ) -> std::collections::HashSet<u32> {
        self.storage.effective_roles(id)
    }

    /// Runs a search against an index and renders the hits.
    fn render_search(
        meta: &EngineMeta,
        index: &str,
        query: &SearchQuery,
    ) -> Result<String, EngineError> {
        let index_state =
            meta.state.indices.get(index).ok_or_else(|| {
                EngineError::Command(CommandError::UnknownIndex(index.to_string()))
            })?;
        let hits = index_state.search(query)?;
        let total = hits.len();
        render_json(&json!({
            "hits": {
                "total": total,
                "hits": hits,
            }
        }))
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
            Command::Search { index, query } => Self::render_search(meta, &index, &query),
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
        let mut collector = crate::rel::Collector::default();
        let error = self.sql_batch_streamed(input, txn_ctx, params, &mut collector)?;
        Ok(collector.into_outcome(error))
    }

    /// `sp_describe_first_result_set`: statically-derivable column metadata
    /// for `tsql`'s first result set, without executing anything.
    pub fn describe_first_result_set(
        &self,
        tsql: &str,
    ) -> Result<crate::rel::RowSet, truthdb_sql::error::SqlError> {
        let _meta = self.meta.read().expect("engine meta poisoned");
        crate::rel::describe_first_result_set(&self.storage, tsql)
    }

    /// Like [`Self::sql_batch_with_params`], but each statement's result
    /// leaves through `emitter` as it is produced (see
    /// [`crate::rel::execute_batch_streamed`]). Returns the batch's terminal
    /// error, which the caller reports after the statement events.
    pub fn sql_batch_streamed(
        &self,
        input: &str,
        txn_ctx: &mut crate::rel::TxnContext,
        params: &[crate::rel::RpcParam],
        emitter: &mut dyn crate::rel::BatchEmitter,
    ) -> Result<Option<truthdb_sql::error::SqlError>, EngineError> {
        // Hold the execution gate shared for the whole batch: concurrent
        // relational batches run together, but a native writer is excluded (see
        // [`Engine`]). The guard also gives the checkpointer its `meta` read.
        let meta = self.meta.read().expect("engine meta poisoned");
        let error =
            crate::rel::execute_batch_streamed(&self.storage, input, txn_ctx, params, emitter);
        self.maybe_checkpoint(&meta)?;
        Ok(error)
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

    /// Drops version-store history no live snapshot can need (called by the
    /// session pool's maintenance thread; cheap when nothing is versioned).
    pub(crate) fn version_prune(&self) {
        self.storage.version_prune();
    }

    /// The lock-analysis epoch (bumped by `ALTER DATABASE` option flips): the
    /// scheduler re-analyzes parked batches whose epoch is stale before
    /// granting them.
    pub(crate) fn lock_analysis_epoch(&self) -> u64 {
        self.storage.lock_analysis_epoch()
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

    #[test]
    fn full_backup_and_offline_restore_round_trips_relational_data() {
        let src = unique_temp_path("backup-src");
        let bak = unique_temp_path("backup-bak");
        let restored = unique_temp_path("backup-restored");

        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
            .expect("create t");
        engine
            .execute("CREATE INDEX ix_name ON t (name)")
            .expect("create index");
        // Enough rows to spread the heap and the secondary index over several
        // B+tree pages, so the copy is more than a single catalog page.
        for i in 1..=200 {
            engine
                .execute(&format!("INSERT INTO t (id, name) VALUES ({i}, 'row{i}')"))
                .expect("insert into t");
        }
        engine
            .execute("CREATE TABLE u (k INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create u");
        engine
            .execute("INSERT INTO u (k, v) VALUES (1, 10), (2, 20)")
            .expect("insert into u");

        let expected_t = sql_rows(&engine, "SELECT id, name FROM t ORDER BY id").1;

        // Online backup while the engine is live.
        let summary = engine
            .storage()
            .backup_full(&bak)
            .expect("online full backup");
        assert!(summary.pages_copied > 0, "the backup copied data pages");
        assert!(
            summary.backup_end_lsn >= summary.redo_start_lsn,
            "the log bracket is well-formed"
        );
        drop(engine);

        // Offline restore into a fresh file, then open it and compare.
        Storage::restore_full(&restored, &bak).expect("offline restore");
        let engine2 = Engine::new(Storage::open(restored.clone()).expect("open restored"))
            .expect("engine on restored file");

        assert_eq!(
            sql_rows(&engine2, "SELECT id, name FROM t ORDER BY id").1,
            expected_t,
            "table t round-trips row-for-row"
        );
        assert_eq!(
            sql_rows(&engine2, "SELECT k, v FROM u ORDER BY k").1,
            vec![
                vec![Some("1".into()), Some("10".into())],
                vec![Some("2".into()), Some("20".into())],
            ],
            "table u round-trips"
        );
        // The secondary index round-trips: a seek by name finds the row.
        assert_eq!(
            sql_rows(&engine2, "SELECT id FROM t WHERE name = 'row150'").1,
            vec![vec![Some("150".into())]],
            "the secondary index is intact after restore"
        );
        // The catalog round-trips: further DML on the restored database works.
        engine2
            .execute("INSERT INTO u (k, v) VALUES (3, 30)")
            .expect("insert into restored u");
        assert_eq!(
            sql_rows(&engine2, "SELECT v FROM u WHERE k = 3").1,
            vec![vec![Some("30".into())]]
        );
        drop(engine2);

        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(bak);
        let _ = std::fs::remove_file(restored);
    }

    #[test]
    fn backup_database_statement_backs_up_and_restores() {
        let src = unique_temp_path("backup-stmt-src");
        let bak = unique_temp_path("backup-stmt-bak");
        let restored = unique_temp_path("backup-stmt-restored");

        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
            .expect("create");
        for i in 1..=50 {
            engine
                .execute(&format!("INSERT INTO t (id, name) VALUES ({i}, 'r{i}')"))
                .expect("insert");
        }
        let expected = sql_rows(&engine, "SELECT id, name FROM t ORDER BY id").1;

        // The T-SQL BACKUP statement drives the online backup.
        let path_lit = bak.to_str().unwrap().replace('\'', "''");
        let env = sql(
            &engine,
            &format!("BACKUP DATABASE truthdb TO DISK = '{path_lit}' WITH CHECKSUM, COPY_ONLY"),
        );
        assert!(env["error"].is_null(), "BACKUP DATABASE failed: {env}");
        drop(engine);

        Storage::restore_full(&restored, &bak).expect("restore");
        let engine2 =
            Engine::new(Storage::open(restored.clone()).expect("open restored")).expect("engine");
        assert_eq!(
            sql_rows(&engine2, "SELECT id, name FROM t ORDER BY id").1,
            expected,
            "the BACKUP-statement backup restores row-for-row"
        );
        drop(engine2);

        let _ = std::fs::remove_file(src);
        let _ = std::fs::remove_file(bak);
        let _ = std::fs::remove_file(restored);
    }

    #[test]
    fn backup_database_is_rejected_inside_a_transaction() {
        let path = unique_temp_path("backup-in-txn");
        let engine = new_engine(&path);
        // BACKUP manages its own per-chunk locking and cannot run inside an
        // explicit transaction (3021).
        assert_eq!(
            sql_error_number(
                &engine,
                "BEGIN TRANSACTION; BACKUP DATABASE d TO DISK = '/tmp/truthdb-never.bak'"
            ),
            3021
        );
        // A side-effecting BACKUP is illegal inside a function body (156);
        // otherwise a per-row SELECT would run a backup per row.
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE FUNCTION dbo.f() RETURNS INT AS BEGIN \
                 BACKUP DATABASE d TO DISK = '/tmp/truthdb-never.bak'; RETURN 1 END"
            ),
            156
        );
        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_model_sets_persists_and_reports() {
        let path = unique_temp_path("recovery-model");
        let engine = new_engine(&path);
        let model = |e: &Engine| sql_rows(e, "SELECT recovery_model_desc FROM sys.databases").1;

        // SIMPLE is the default.
        assert_eq!(model(&engine), vec![vec![Some("SIMPLE".into())]]);

        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("set full");
        assert_eq!(model(&engine), vec![vec![Some("FULL".into())]]);

        // An unrelated option in the same statement family leaves it untouched.
        engine
            .execute("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON")
            .expect("rcsi on");
        assert_eq!(model(&engine), vec![vec![Some("FULL".into())]]);
        drop(engine);

        // FULL persists across a reopen (the set is itself durable).
        let engine2 = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("engine");
        assert_eq!(model(&engine2), vec![vec![Some("FULL".into())]]);
        assert_eq!(
            sql_rows(
                &engine2,
                "SELECT is_read_committed_snapshot_on FROM sys.databases"
            )
            .1,
            vec![vec![Some("1".into())]],
            "RCSI survived alongside the recovery model"
        );

        engine2
            .execute("ALTER DATABASE CURRENT SET RECOVERY SIMPLE")
            .expect("set simple");
        assert_eq!(model(&engine2), vec![vec![Some("SIMPLE".into())]]);
        drop(engine2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn backup_log_ships_the_log_advances_the_marker_and_chains() {
        use crate::backup::{BackupReader, BlockType};
        let src = unique_temp_path("backuplog-src");
        let trn1 = unique_temp_path("backuplog-1");
        let trn2 = unique_temp_path("backuplog-2");
        let engine = new_engine(&src);
        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("full");
        // Enabling FULL starts the log chain at the current tail and pins it.
        let chain_start = engine.storage().last_log_backup_lsn();
        assert_eq!(engine.storage().log_backup_hold(), Some(chain_start));
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }

        let read_log_archive = |path: &std::path::Path| -> (crate::backup::BackupHeader, u64) {
            let (mut r, header) =
                BackupReader::new(std::io::BufReader::new(std::fs::File::open(path).unwrap()))
                    .unwrap();
            let mut log_bytes = 0u64;
            while let Some((bt, payload)) = r.next_block().unwrap() {
                if bt == BlockType::LogChunk {
                    log_bytes += payload.len().saturating_sub(8) as u64; // minus the start-LSN prefix
                }
            }
            (header, log_bytes)
        };

        // First BACKUP LOG ships [chain_start, tail) and advances the marker.
        let lit1 = trn1.to_str().unwrap().replace('\'', "''");
        assert!(
            sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit1}'"))["error"].is_null(),
            "BACKUP LOG succeeded"
        );
        let marker1 = engine.storage().last_log_backup_lsn();
        assert!(marker1 > chain_start, "the marker advanced");
        assert_eq!(
            engine.storage().log_backup_hold(),
            Some(marker1),
            "the hold moved to the new marker"
        );
        let (header1, bytes1) = read_log_archive(&trn1);
        assert!(header1.flags.log_backup, "flagged as a log-only archive");
        assert_eq!(header1.redo_start_lsn, chain_start);
        assert_eq!(
            chain_start + bytes1,
            marker1,
            "the shipped range ends at the marker"
        );

        // A second BACKUP LOG chains contiguously from the first.
        engine
            .execute("INSERT INTO t VALUES (99, 99)")
            .expect("more");
        let lit2 = trn2.to_str().unwrap().replace('\'', "''");
        assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit2}'"))["error"].is_null());
        let (header2, _) = read_log_archive(&trn2);
        assert_eq!(
            header2.redo_start_lsn, marker1,
            "the second archive starts where the first ended"
        );
        drop(engine);
        for p in [src, trn1, trn2] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn restoring_a_full_recovery_database_opens_and_checkpoints_cleanly() {
        let src = unique_temp_path("restore-full-src");
        let bak = unique_temp_path("restore-full-bak");
        let restored = unique_temp_path("restore-full-restored");
        let trn = unique_temp_path("restore-full-trn");
        let engine = new_engine(&src);
        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("full");
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=30 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let expected = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1;
        engine
            .storage()
            .backup_full(&bak)
            .expect("full backup of a FULL-model db");
        drop(engine);

        Storage::restore_full(&restored, &bak).expect("restore");
        let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&engine2, "SELECT id, v FROM t ORDER BY id").1,
            expected
        );
        // The restored DB is FULL and its log-backup floor is seeded at the
        // restore point (backup_end), so the on-open hold sits at/above wal_head.
        assert_eq!(
            sql_rows(&engine2, "SELECT recovery_model_desc FROM sys.databases").1,
            vec![vec![Some("FULL".into())]]
        );
        assert!(
            engine2.storage().log_backup_hold().is_some(),
            "FULL-model hold re-registered on the restored db"
        );
        // A checkpoint would panic (set_head with a floor below the head) if the
        // marker had been left at 0 — the Fix-3 regression guard.
        engine2
            .storage()
            .write_checkpoint(b"cp", 1, 2, 1)
            .expect("checkpoint on the restored db");
        // And BACKUP LOG works on the restored (fresh) log chain.
        let lit = trn.to_str().unwrap().replace('\'', "''");
        assert!(sql(&engine2, &format!("BACKUP LOG truthdb TO DISK = '{lit}'"))["error"].is_null());
        drop(engine2);
        for p in [src, bak, restored, trn] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn restore_full_plus_log_chain_recovers_post_backup_changes() {
        let src = unique_temp_path("restlog-src");
        let bak = unique_temp_path("restlog-bak");
        let trn1 = unique_temp_path("restlog-1");
        let trn2 = unique_temp_path("restlog-2");
        let restored = unique_temp_path("restlog-restored");
        let restored_full_only = unique_temp_path("restlog-fullonly");
        let restored_gap = unique_temp_path("restlog-gap");

        let engine = new_engine(&src);
        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("full");
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        // Full backup captures rows 1..=20.
        engine.storage().backup_full(&bak).expect("full backup");
        // Changes AFTER the full backup, then a first log backup.
        for i in 21..=40 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let lit1 = trn1.to_str().unwrap().replace('\'', "''");
        assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit1}'"))["error"].is_null());
        // More changes, then a second (chained) log backup.
        engine
            .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
            .expect("update");
        for i in 41..=50 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let lit2 = trn2.to_str().unwrap().replace('\'', "''");
        assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit2}'"))["error"].is_null());
        let expected = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1;
        drop(engine);

        // Full + the whole log chain recovers EVERY committed change.
        Storage::restore_full_with_logs(&restored, &bak, &[trn1.clone(), trn2.clone()], None)
            .expect("restore full + log chain");
        let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&engine2, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "restore + log chain recovers all post-full-backup changes"
        );

        // The full backup alone recovers only the 20 rows at its point.
        Storage::restore_full(&restored_full_only, &bak).expect("restore full only");
        let engine3 =
            Engine::new(Storage::open(restored_full_only.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&engine3, "SELECT COUNT(*) FROM t").1,
            vec![vec![Some("20".into())]],
            "the full backup alone is the point-in-time it was taken"
        );

        // A gap in the chain (apply only the second log) is rejected (4305), and
        // the partial destination is removed so a retry can reuse the path.
        assert!(
            Storage::restore_full_with_logs(&restored_gap, &bak, &[trn2.clone()], None).is_err(),
            "a log-chain gap is rejected"
        );
        assert!(
            !restored_gap.exists(),
            "the partial destination is cleaned up on error"
        );
        Storage::restore_full_with_logs(&restored_gap, &bak, &[trn1.clone(), trn2.clone()], None)
            .expect("retry with the full chain to the same path succeeds after cleanup");

        drop(engine2);
        drop(engine3);
        for p in [
            src,
            bak,
            trn1,
            trn2,
            restored,
            restored_full_only,
            restored_gap,
        ] {
            let _ = std::fs::remove_file(p);
        }
    }

    // Stage 18 slice 2: a standby seeded from a full backup, fed the primary's
    // raw shipped WAL ring bytes (`read_wal_range`), recovers to a state that
    // matches the primary — the physical-replication apply path, offline.
    #[test]
    fn standby_applies_shipped_wal_ranges_and_matches_the_primary() {
        let src = unique_temp_path("repl-src");
        let bak = unique_temp_path("repl-bak");
        let standby = unique_temp_path("repl-standby");
        let standby_idem = unique_temp_path("repl-standby-idem");

        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        // Full backup captures rows 1..=20 up to `backup_end`.
        let summary = engine.storage().backup_full(&bak).expect("full backup");
        let backup_end = summary.backup_end_lsn;
        // Committed changes AFTER the backup — the log a standby must catch up on.
        for i in 21..=40 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        engine
            .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
            .expect("update");
        // Ship the primary's raw ring bytes `[backup_end, durable tail)`.
        let flushed = engine.storage().wal_flushed_lsn();
        assert!(flushed > backup_end, "there is post-backup log to ship");
        let delta = engine
            .storage()
            .read_wal_range(backup_end, flushed)
            .expect("ship the WAL range");
        let expected = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1;
        drop(engine);

        // Standby = the full backup + the shipped raw WAL range, recovered.
        Storage::restore_full_with_wal_ranges(&standby, &bak, &[(backup_end, delta.clone())])
            .expect("apply the shipped range to the standby");
        let s = Engine::new(Storage::open(standby.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&s, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "the standby matches the primary after applying the shipped WAL"
        );
        // The replication restartpoint persisted = the end of the applied range.
        assert_eq!(
            s.storage().applied_lsn(),
            backup_end + delta.len() as u64,
            "applied_lsn is the end of the applied range and survives the reopen"
        );

        // Idempotent: applying the SAME range twice yields the identical state
        // (seed overwrites identical bytes; redo is page-LSN-gated).
        Storage::restore_full_with_wal_ranges(
            &standby_idem,
            &bak,
            &[(backup_end, delta.clone()), (backup_end, delta.clone())],
        )
        .expect("re-applying the same range is accepted");
        let s2 = Engine::new(Storage::open(standby_idem.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&s2, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "re-applying the same range is idempotent"
        );

        drop(s);
        drop(s2);
        for p in [src, bak, standby, standby_idem] {
            let _ = std::fs::remove_file(p);
        }
    }

    // Stage 18 slice 4d: an OPEN standby applies a shipped WAL range LIVE (no
    // reopen) and its state matches the primary; the apply is idempotent and
    // survives a standby restart.
    #[test]
    fn a_standby_applies_a_live_wal_stream_and_matches_the_primary() {
        let primary_path = unique_temp_path("live-primary");
        let bak = unique_temp_path("live-bak");
        let standby_path = unique_temp_path("live-standby");

        // Primary: rows 1..=10, a backup, then post-backup changes to ship.
        let primary = new_engine(&primary_path);
        primary
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=10 {
            primary
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let summary = primary.storage().backup_full(&bak).expect("backup");
        let backup_end = summary.backup_end_lsn;
        for i in 11..=25 {
            primary
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        primary
            .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
            .expect("update");
        let flushed = primary.storage().wal_flushed_lsn();
        let delta = primary
            .storage()
            .read_wal_range(backup_end, flushed)
            .expect("ship the delta");
        let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;

        // Standby: restore the backup as an OPEN engine (only the 10 backed-up
        // rows), then apply the shipped delta LIVE — no reopen.
        Storage::restore_full(&standby_path, &bak).expect("restore");
        let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
        assert_eq!(
            sql_rows(&standby, "SELECT COUNT(*) FROM t").1,
            vec![vec![Some("10".into())]],
            "the standby starts at the backup point"
        );
        standby
            .storage()
            .apply_wal_stream(backup_end, &delta)
            .expect("live apply");
        assert_eq!(
            sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "the standby matches the primary after applying the live stream (no reopen)"
        );

        // Idempotent: re-applying the same range changes nothing.
        standby
            .storage()
            .apply_wal_stream(backup_end, &delta)
            .expect("re-apply");
        assert_eq!(
            sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "re-applying the same range is idempotent"
        );

        // Durable: the applied state survives a standby restart.
        drop(standby);
        let standby2 =
            Engine::new(Storage::open(standby_path.clone()).expect("reopen")).expect("eng");
        assert_eq!(
            sql_rows(&standby2, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "the live-applied state persists across a standby restart"
        );

        // A backup taken ON the standby captures the FULL applied state (the
        // in-memory WAL tail is resynced, so the backup is not truncated to the
        // pre-apply point).
        let standby_bak = unique_temp_path("live-standby-bak");
        let restored = unique_temp_path("live-standby-restored");
        standby2
            .storage()
            .backup_full(&standby_bak)
            .expect("backup on the standby");
        Storage::restore_full(&restored, &standby_bak).expect("restore the standby backup");
        let r = Engine::new(Storage::open(restored.clone()).expect("open")).expect("eng");
        assert_eq!(
            sql_rows(&r, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "a backup of the standby restores the full applied state"
        );

        // A checkpoint on a standby is refused (it reclaims ring space only at
        // promotion, to keep the in-flight undo log).
        assert!(
            standby2.checkpoint().is_err(),
            "checkpoint is refused on a standby"
        );

        // A standby is read-only: a local client write is rejected (it would
        // append to the replica's own WAL and diverge it from the primary).
        let write = sql(&standby2, "INSERT INTO t VALUES (999, 999)");
        assert!(
            !write["error"].is_null(),
            "a local write on a standby is rejected: {write}"
        );
        assert_eq!(
            sql_rows(&standby2, "SELECT COUNT(*) FROM t").1,
            vec![vec![Some("25".into())]],
            "the rejected write left the standby unchanged"
        );

        drop(primary);
        drop(standby2);
        drop(r);
        for p in [primary_path, bak, standby_path, standby_bak, restored] {
            let _ = std::fs::remove_file(p);
        }
    }

    // Stage 18 slice 4c: the full transport, end to end over a real TCP+TLS
    // socket — listener + handshake + per-standby sender on the primary,
    // reconnecting receiver on the standby. A backup-seeded standby catches up,
    // follows live commits (woken by the flushed watch, no polling), its
    // FlushAcks advance the primary's slot, and a receiver restart resumes from
    // the standby's persisted watermark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_replication_transport_streams_live_writes_to_a_standby() {
        use crate::repl::listener::{PrimaryReplContext, run_repl_listener};
        use crate::repl::receiver::{ReceiverConfig, run_standby_receiver};
        use crate::repl::tls::{client_config_trusting, server_config_from_pem};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::sync::watch;
        use tokio_rustls::TlsAcceptor;

        const SECRET: &[u8] = b"transport-secret";
        const UUID: [u8; 16] = [7u8; 16];

        let primary_path = unique_temp_path("xport-primary");
        let bak = unique_temp_path("xport-bak");
        let standby_path = unique_temp_path("xport-standby");

        // Primary: rows 1..=10, a backup to seed the standby, then post-backup
        // rows the transport must catch the standby up on.
        let primary = new_engine(&primary_path);
        primary
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=10 {
            primary
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        primary.storage().backup_full(&bak).expect("backup");
        for i in 11..=20 {
            primary
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }

        // The primary's replication listener on an ephemeral port.
        let (cert_pem, key_pem) = {
            let c = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            (c.cert.pem(), c.key_pair.serialize_pem())
        };
        let acceptor = TlsAcceptor::from(
            server_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctx = PrimaryReplContext {
            shared_secret: Arc::new(SECRET.to_vec()),
            cluster_uuid: UUID,
            storage: primary.storage_arc(),
            heartbeat: Duration::from_millis(200),
        };
        let listener_task = tokio::spawn(run_repl_listener(
            listener,
            acceptor,
            ctx,
            shutdown_rx.clone(),
        ));

        // Standby: seeded with a --standby restore (stamped redo-only +
        // read-only before its first open), opened live, then its receiver
        // dials the primary.
        Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
        let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
        assert!(
            standby.storage().is_standby(),
            "a --standby restore stamps the standby mode before the first open"
        );
        let receiver_cfg = ReceiverConfig {
            primary_addr: addr.to_string(),
            server_name: "localhost".to_string(),
            tls_ca_pem: cert_pem.as_bytes().to_vec(),
            shared_secret: SECRET.to_vec(),
            cluster_uuid: UUID,
            node_id: 7,
            reconnect_delay: Duration::from_millis(100),
        };
        let (rx_shutdown_tx, rx_shutdown_rx) = watch::channel(false);
        let receiver_task = tokio::spawn(run_standby_receiver(
            receiver_cfg.clone(),
            standby.storage_arc(),
            rx_shutdown_rx,
        ));

        async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while !cond() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for {what}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        // Catch-up: the standby reaches the primary's durable watermark.
        let target = primary.storage().wal_flushed_lsn();
        wait_until("catch-up", || standby.storage().wal_tail() >= target).await;
        let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
        assert_eq!(
            sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "the standby matches the primary after catch-up"
        );

        // Live follow: new commits arrive without any reconnect.
        for i in 21..=30 {
            primary
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        primary
            .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
            .expect("update");
        let target = primary.storage().wal_flushed_lsn();
        wait_until("live follow", || standby.storage().wal_tail() >= target).await;
        let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
        assert_eq!(
            sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "the standby follows live commits"
        );

        // The standby's FlushAcks advance the primary's slot to its watermark,
        // so the primary can reclaim the shipped log.
        wait_until("slot advance", || {
            primary.storage().repl_slot_lsn(7) == Some(target)
        })
        .await;

        // Reconnect: stop the receiver, commit more, restart it — the stream
        // resumes from the standby's persisted watermark (the slot held the
        // log in between).
        rx_shutdown_tx.send(true).unwrap();
        let _ = receiver_task.await;
        for i in 31..=40 {
            primary
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let (rx_shutdown_tx2, rx_shutdown_rx2) = watch::channel(false);
        let receiver_task2 = tokio::spawn(run_standby_receiver(
            receiver_cfg,
            standby.storage_arc(),
            rx_shutdown_rx2,
        ));
        let target = primary.storage().wal_flushed_lsn();
        wait_until("resume after reconnect", || {
            standby.storage().wal_tail() >= target
        })
        .await;
        let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
        assert_eq!(
            sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "the standby resumes and matches after a receiver restart"
        );

        rx_shutdown_tx2.send(true).unwrap();
        let _ = receiver_task2.await;
        shutdown_tx.send(true).unwrap();
        let _ = listener_task.await;
        drop(standby);
        drop(primary);
        for p in [primary_path, bak, standby_path] {
            let _ = std::fs::remove_file(p);
        }
    }

    // Stage 18 slice 4d (review fix): a shipped stream can end MID-transaction
    // (the primary's durable watermark can land between an in-flight txn's page
    // ops and its commit). A standby's reopen must REPEAT history (redo-only) —
    // a full ARIES undo would roll back the in-flight rows, and since the primary
    // commits them and resumes shipping ABOVE the standby's applied point, they
    // would be lost forever (silent replica divergence).
    #[test]
    fn a_standby_reopen_is_redo_only_and_keeps_in_flight_stream_data() {
        let primary_path = unique_temp_path("redo-primary");
        let bak = unique_temp_path("redo-bak");
        let standby_path = unique_temp_path("redo-standby");

        let primary = new_engine(&primary_path);
        let mut ctx = TxnContext::default();
        batch(
            &primary,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        );
        for i in 1..=10 {
            batch(
                &primary,
                &mut ctx,
                &format!("INSERT INTO t VALUES ({i}, {i})"),
            );
        }
        let summary = primary.storage().backup_full(&bak).expect("backup");
        let backup_end = summary.backup_end_lsn;

        // An explicit transaction whose inserts are made durable (fsynced) but NOT
        // committed — the durable watermark now lands mid-transaction.
        batch(&primary, &mut ctx, "BEGIN TRANSACTION");
        for i in 11..=20 {
            batch(
                &primary,
                &mut ctx,
                &format!("INSERT INTO t VALUES ({i}, {i})"),
            );
        }
        let mid = primary.storage().wal_flushed_lsn();
        let delta = primary
            .storage()
            .read_wal_range(backup_end, mid)
            .expect("ship the mid-transaction range");

        let count = |eng: &Engine| -> String {
            sql_rows(eng, "SELECT COUNT(*) FROM t").1[0][0]
                .clone()
                .unwrap_or_default()
        };

        // Standby applies the mid-transaction stream live (the in-flight rows are
        // present via redo), then RESTARTS.
        Storage::restore_full(&standby_path, &bak).expect("restore");
        {
            let standby =
                Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
            standby
                .storage()
                .apply_wal_stream(backup_end, &delta)
                .expect("apply mid-txn stream");
            assert_eq!(
                count(&standby),
                "20",
                "the in-flight rows are applied via redo"
            );
        }
        // THE FIX: a standby reopen is redo-only, so the applied rows survive. A
        // full ARIES undo here would drop the 10 in-flight rows permanently.
        let standby =
            Engine::new(Storage::open(standby_path.clone()).expect("reopen")).expect("eng");
        assert_eq!(
            count(&standby),
            "20",
            "redo-only reopen keeps the streamed in-flight rows"
        );

        // The primary commits and ships the continuation; the standby stays
        // consistent with the primary.
        batch(&primary, &mut ctx, "COMMIT TRANSACTION");
        let after = primary.storage().wal_flushed_lsn();
        let delta2 = primary
            .storage()
            .read_wal_range(mid, after)
            .expect("ship the commit");
        standby
            .storage()
            .apply_wal_stream(mid, &delta2)
            .expect("apply the commit");
        assert_eq!(
            sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
            sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1,
            "the standby matches the primary once the commit is shipped"
        );

        drop(primary);
        drop(standby);
        for p in [primary_path, bak, standby_path] {
            let _ = std::fs::remove_file(p);
        }
    }

    // Stage 18 slice 3: a replication slot holds WAL-ring truncation at a
    // standby's received LSN, survives a restart, and is invalidated once it
    // lags past `max_slot_retain_bytes`.
    #[test]
    fn replication_slots_hold_truncation_persist_and_reap() {
        let path = unique_temp_path("repl-slots");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=10 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let l1 = engine.storage().wal_flushed_lsn();
        assert!(l1 > 0, "there is log to pin");

        // A slot pinned at l1 holds the WAL head there even as the tail advances.
        engine
            .storage()
            .try_register_repl_slot(7, l1)
            .expect("register");
        for i in 11..=30 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        engine.checkpoint().expect("checkpoint");
        assert_eq!(
            engine.storage().wal_head(),
            l1,
            "the slot pins the truncation floor at its LSN"
        );

        // Advancing the slot lets the floor follow.
        let l2 = engine.storage().wal_flushed_lsn();
        engine.storage().advance_repl_slot(7, l2);
        engine.checkpoint().expect("checkpoint");
        assert_eq!(
            engine.storage().wal_head(),
            l2,
            "advancing the slot advances the floor"
        );

        // The slot's hold survives a restart (re-seeded from the superblock).
        drop(engine);
        let engine = Engine::new(Storage::open(path.clone()).expect("open")).expect("engine");
        assert_eq!(
            engine.storage().repl_slot_lsn(7),
            Some(l2),
            "the slot survives the restart"
        );

        // An explicit drop removes a slot (a standby that deregisters).
        engine
            .storage()
            .try_register_repl_slot(8, l2)
            .expect("register");
        engine.storage().drop_repl_slot(8);
        assert_eq!(
            engine.storage().repl_slot_lsn(8),
            None,
            "an explicitly dropped slot is gone"
        );

        // Lagging past max_slot_retain invalidates the slot at the next checkpoint.
        engine
            .storage()
            .set_max_slot_retain_bytes(64)
            .expect("set cap");
        for i in 31..=60 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        engine.checkpoint().expect("checkpoint");
        assert_eq!(
            engine.storage().repl_slot_lsn(7),
            None,
            "an over-lagging slot is invalidated"
        );
        assert!(
            engine.storage().wal_head() > l2,
            "the reclaimed floor advances past the dropped slot"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    // A replication sender wakes on the group-commit flushed watch instead of
    // polling: a committed write advances the watch past the subscriber's last
    // seen value, and the durable watermark it re-reads covers the commit.
    #[test]
    fn the_flushed_watch_wakes_on_a_committed_write() {
        let path = unique_temp_path("repl-watch");
        let engine = new_engine(&path);
        let mut rx = engine.storage().subscribe_wal_flushed();
        let before = *rx.borrow_and_update();
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        assert!(
            rx.has_changed().expect("sender alive"),
            "a durable commit signals the watch"
        );
        let hint = *rx.borrow_and_update();
        assert!(hint > before, "the watch value advances");
        assert!(
            engine.storage().wal_flushed_lsn() >= hint,
            "the re-read watermark covers the hint"
        );
        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    // Slot registration is atomically fenced against truncation: a slot below
    // the WAL head is refused (the log it needs is gone — reseed), and the
    // table is bounded so a slot never silently fails to persist.
    #[test]
    fn slot_registration_rejects_below_head_and_a_full_table() {
        let path = unique_temp_path("repl-slot-guards");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        for i in 1..=10 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i})"))
                .expect("insert");
        }
        engine.checkpoint().expect("checkpoint");
        let head = engine.storage().wal_head();
        assert!(head > 0, "the checkpoint truncated some log");
        let err = engine
            .storage()
            .try_register_repl_slot(1, head - 1)
            .expect_err("a below-head slot is refused");
        assert!(
            err.to_string().contains("reseed"),
            "the error tells the operator to reseed: {err}"
        );

        let lsn = engine.storage().wal_flushed_lsn();
        for id in 1..=8 {
            engine
                .storage()
                .try_register_repl_slot(id, lsn)
                .expect("register");
        }
        engine
            .storage()
            .try_register_repl_slot(9, lsn)
            .expect_err("a ninth slot exceeds the persisted table");
        // Re-registering an existing id is a reset, not a new slot.
        engine
            .storage()
            .try_register_repl_slot(8, lsn)
            .expect("re-register");
        // An ack for a never-registered (or reaped) slot must not create one.
        engine.storage().advance_repl_slot(42, lsn);
        assert_eq!(engine.storage().repl_slot_lsn(42), None);
        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    // The replication epoch persists across a checkpoint (whose superblock
    // rebuild zeroes the reserved area — the checkpoint-wipe carry) and a
    // restart, and the retention-cap setter refuses a cap the ring cannot
    // honor.
    #[test]
    fn the_replication_epoch_persists_and_the_retain_cap_is_bounded() {
        let path = unique_temp_path("repl-epoch");
        let engine = new_engine(&path);
        assert_eq!(engine.storage().epoch(), 0, "a fresh file is epoch 0");
        engine.storage().set_epoch(5).expect("set epoch");
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.checkpoint().expect("checkpoint");
        drop(engine);
        let engine = Engine::new(Storage::open(path.clone()).expect("open")).expect("engine");
        assert_eq!(
            engine.storage().epoch(),
            5,
            "the epoch survives the checkpoint and the restart"
        );
        engine
            .storage()
            .set_max_slot_retain_bytes(u64::MAX - 1)
            .expect_err("a cap at or above the usable ring is refused");
        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn point_in_time_restore_stops_at_a_commit_timestamp() {
        use crate::storage_layout::WAL_ENTRY_TYPE_REL;
        use crate::wal::records::{REL_KIND_TXN_COMMIT, RelRecord};

        let src = unique_temp_path("pitr-src");
        let bak = unique_temp_path("pitr-bak");
        let restored = unique_temp_path("pitr-restored");

        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        // Three autocommitted inserts, spaced so their commit records land in
        // distinct milliseconds; the sleeps make the timestamps separable.
        engine.execute("INSERT INTO t VALUES (1)").expect("ins 1");
        std::thread::sleep(std::time::Duration::from_millis(15));
        engine.execute("INSERT INTO t VALUES (2)").expect("ins 2");
        std::thread::sleep(std::time::Duration::from_millis(15));
        engine.execute("INSERT INTO t VALUES (3)").expect("ins 3");
        // The online full backup captures all three commits in its embedded log.
        engine.storage().backup_full(&bak).expect("full backup");
        drop(engine);

        // Reopen the source so recovery's ring scan fills the replay cache, then
        // read the commit timestamps in LSN order: create, insert-1, insert-2,
        // insert-3. Stopping at insert-1's timestamp must keep row 1 (ts == stop
        // is not "after") and undo rows 2 and 3 (ts > stop).
        let storage = Storage::open(src.clone()).expect("reopen src");
        let mut commit_ts: Vec<u64> = Vec::new();
        for r in storage.replay_wal_entries().expect("replay") {
            if r.entry_type != WAL_ENTRY_TYPE_REL {
                continue;
            }
            let rec = RelRecord::decode(&r.payload).expect("decode");
            if let Some(ts) = rec.commit_timestamp_millis() {
                commit_ts.push(ts);
            }
        }
        drop(storage);
        assert!(
            commit_ts.len() >= 4,
            "create + three inserts commit; got {commit_ts:?}"
        );
        let stop_at = commit_ts[1]; // insert-1's commit timestamp
        assert!(
            commit_ts[2] > stop_at,
            "insert-2 must commit strictly after insert-1 for a clean stop point: {commit_ts:?}"
        );

        // Point-in-time restore of the full backup, stopping after insert-1.
        Storage::restore_full_with_logs(&restored, &bak, &[], Some(stop_at)).expect("pitr restore");
        let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&engine2, "SELECT id FROM t ORDER BY id").1,
            vec![vec![Some("1".into())]],
            "only the commit at-or-before the stop point survives"
        );
        drop(engine2);

        // The undo persists: a plain reopen (no stop point) still shows only
        // row 1 — the losers were rolled back durably via CLRs, not re-derived
        // from stop_at.
        let engine3 =
            Engine::new(Storage::open(restored.clone()).expect("reopen")).expect("engine");
        assert_eq!(
            sql_rows(&engine3, "SELECT id FROM t ORDER BY id").1,
            vec![vec![Some("1".into())]],
            "point-in-time state survives a normal restart"
        );
        drop(engine3);

        for p in [src, bak, restored] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn restore_inspect_verbs_read_a_backup_without_restoring() {
        let src = unique_temp_path("restinspect-src");
        let bak = unique_temp_path("restinspect-bak");

        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=10 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        let baklit = bak.to_str().unwrap().replace('\'', "''");
        assert!(
            sql(
                &engine,
                &format!("BACKUP DATABASE truthdb TO DISK = '{baklit}'")
            )["error"]
                .is_null()
        );

        // HEADERONLY: exactly one metadata row; a full backup is BackupType 1.
        let (cols, rows) = sql_rows(
            &engine,
            &format!("RESTORE HEADERONLY FROM DISK = '{baklit}'"),
        );
        assert_eq!(rows.len(), 1, "one header row");
        assert!(cols.contains(&"BackupType".to_string()));
        assert!(cols.contains(&"Checksum".to_string()));
        let col = |name: &str| rows[0][cols.iter().position(|c| c == name).unwrap()].clone();
        assert_eq!(col("BackupType"), Some("1".to_string()), "full backup");
        assert_eq!(col("FormatVersion"), Some("1".to_string()));
        assert_eq!(col("PageSize"), Some("4096".to_string()));

        // FILELISTONLY: a data row ('D') and a log row ('L').
        let (fcols, frows) = sql_rows(
            &engine,
            &format!("RESTORE FILELISTONLY FROM DISK = '{baklit}'"),
        );
        assert_eq!(fcols, vec!["LogicalName", "Type", "Size"]);
        let types: Vec<Option<String>> = frows.iter().map(|r| r[1].clone()).collect();
        assert_eq!(types, vec![Some("D".to_string()), Some("L".to_string())]);

        // VERIFYONLY: a valid backup verifies with no error and opens no rowset.
        let env = sql(
            &engine,
            &format!("RESTORE VERIFYONLY FROM DISK = '{baklit}'"),
        );
        assert!(env["error"].is_null(), "valid backup verifies: {env}");

        // RESTORE DATABASE is offline-only: online it errors (3101).
        assert_eq!(
            sql_error_number(
                &engine,
                &format!("RESTORE DATABASE truthdb FROM DISK = '{baklit}'")
            ),
            3101
        );

        // Inside a transaction, restore is refused (3021), like BACKUP.
        assert_eq!(
            sql_error_number(
                &engine,
                &format!("BEGIN TRANSACTION; RESTORE VERIFYONLY FROM DISK = '{baklit}'")
            ),
            3021
        );
        drop(engine);

        for p in [src, bak] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn restore_verifyonly_rejects_a_corrupt_or_missing_backup() {
        let src = unique_temp_path("restverify-src");
        let bak = unique_temp_path("restverify-bak");
        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        let baklit = bak.to_str().unwrap().replace('\'', "''");
        assert!(
            sql(
                &engine,
                &format!("BACKUP DATABASE truthdb TO DISK = '{baklit}'")
            )["error"]
                .is_null()
        );

        let pristine = std::fs::read(&bak).expect("read bak");
        let verify = |bytes: &[u8]| {
            std::fs::write(&bak, bytes).expect("write");
            sql_error_number(
                &engine,
                &format!("RESTORE VERIFYONLY FROM DISK = '{baklit}'"),
            )
        };

        // Flip a payload byte mid-file: it no longer matches its xxh64, so
        // VERIFYONLY reports the restore terminating abnormally (3013).
        let mut payload_flip = pristine.clone();
        let mid = payload_flip.len() / 2;
        payload_flip[mid] ^= 0xFF;
        assert_eq!(verify(&payload_flip), 3013, "a flipped payload byte");

        // Corrupt the header block's length field (its high byte, outside the
        // checksum): recovery must report 3013, not allocate ~u64::MAX and crash.
        let mut len_flip = pristine.clone();
        len_flip[19] = 0xFF;
        assert_eq!(
            verify(&len_flip),
            3013,
            "a corrupt block length is not a crash"
        );

        // A missing file errors cleanly, not a panic.
        assert_eq!(
            sql_error_number(
                &engine,
                "RESTORE VERIFYONLY FROM DISK = '/nonexistent/nope.bak'"
            ),
            3013
        );
        drop(engine);
        for p in [src, bak] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn backup_under_concurrent_write_load_restores_to_a_consistent_prefix() {
        use std::sync::Arc;
        let src = unique_temp_path("bul-src");
        let bak = unique_temp_path("bul-bak");
        let restored = unique_temp_path("bul-restored");

        let engine = Arc::new(new_engine(&src));
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        // A baseline committed BEFORE the backup starts: the restore must contain
        // at least these 20 rows.
        for i in 1..=20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i})"))
                .expect("baseline insert");
        }

        // A writer thread commits ids 21..=80 while the backup runs, so the fuzzy
        // page copy interleaves with live commits (online backup under load). The
        // count is kept modest so the test stays short and does not hold io_uring
        // resources long enough to pressure other tests running in parallel.
        let writer = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                for i in 21..=80 {
                    engine
                        .execute(&format!("INSERT INTO t VALUES ({i})"))
                        .expect("concurrent insert");
                }
            })
        };
        engine
            .storage()
            .backup_full(&bak)
            .expect("online backup under write load");
        writer.join().expect("writer thread");
        drop(engine);

        // The restore recovers to a single consistent LSN (backup_end). Commits
        // are serialized in id order, so the restored ids must be a CONTIGUOUS
        // prefix 1..=k — a gap would mean a torn page, an id past k an
        // uncommitted write leaked in. k lies between the 20 baseline rows and
        // the writer's max (80).
        Storage::restore_full(&restored, &bak).expect("restore");
        let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
        let (_, rows) = sql_rows(&engine2, "SELECT id FROM t ORDER BY id");
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| r[0].as_ref().unwrap().parse().unwrap())
            .collect();
        assert!(
            (20..=80).contains(&ids.len()),
            "restored count is between the baseline and the writer's max, got {}",
            ids.len()
        );
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(
                *id,
                i as i64 + 1,
                "restored ids are a contiguous prefix (no gaps, no torn/phantom rows): {ids:?}"
            );
        }
        drop(engine2);
        for p in [src, bak, restored] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn a_failed_backup_leaves_the_database_writable_and_backuppable() {
        let src = unique_temp_path("killmid-src");
        let good = unique_temp_path("killmid-good");
        let restored = unique_temp_path("killmid-restored");
        let engine = new_engine(&src);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");

        // A backup to a nonexistent directory fails mid-flight: the hold is armed
        // (begin_backup succeeded), then write_backup's File::create errors. The
        // RAII hold guard must still release, or WAL truncation freezes and writes
        // eventually wedge.
        assert_eq!(
            sql_error_number(
                &engine,
                "BACKUP DATABASE truthdb TO DISK = '/nonexistent-truthdb-dir/b.bak'"
            ),
            3013
        );

        // The database is unharmed: writes still work...
        engine
            .execute("INSERT INTO t VALUES (2)")
            .expect("insert after a failed backup");
        // ...and a fresh backup to a good path succeeds — which it could not if
        // the failed backup's hold or single-flight guard were still set.
        let goodlit = good.to_str().unwrap().replace('\'', "''");
        assert!(
            sql(
                &engine,
                &format!("BACKUP DATABASE truthdb TO DISK = '{goodlit}'")
            )["error"]
                .is_null()
        );
        drop(engine);

        // And that good backup restores the surviving rows.
        Storage::restore_full(&restored, &good).expect("restore");
        let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
        assert_eq!(
            sql_rows(&engine2, "SELECT id FROM t ORDER BY id").1,
            vec![vec![Some("1".into())], vec![Some("2".into())]],
            "the post-failure database backs up and restores intact"
        );
        drop(engine2);
        for p in [src, good, restored] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn recovery_redo_is_idempotent_across_repeated_reopens() {
        // Redo is the resumable core of replication (a standby applies it as
        // records arrive): re-running it must be a no-op, gated by each page's
        // LSN. Reopening the same database repeatedly re-runs redo over the whole
        // log each time; the committed state must survive unchanged.
        let path = unique_temp_path("redo-idempotent");
        {
            let engine = new_engine(&path);
            engine
                .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
                .expect("create");
            for i in 1..=30 {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                    .expect("insert");
            }
            engine
                .execute("UPDATE t SET v = v + 100 WHERE id <= 10")
                .expect("update");
        }
        let expected = {
            let engine = Engine::new(Storage::open(path.clone()).expect("open")).expect("engine");
            sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1
        };
        // Reopen several more times: each reopen re-runs redo over the full log.
        // The state is invariant — proving redo re-application is idempotent.
        for _ in 0..3 {
            let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("engine");
            assert_eq!(
                sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1,
                expected,
                "redo re-application over the whole log is a no-op"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn commit_records_carry_a_recent_wall_clock_timestamp() {
        use crate::storage_layout::WAL_ENTRY_TYPE_REL;
        use crate::wal::records::{REL_KIND_TXN_COMMIT, RelRecord};
        let now_ms = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        };
        let path = unique_temp_path("commit-ts");
        let before = now_ms();
        {
            let engine = new_engine(&path);
            engine
                .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
                .expect("create");
            engine.execute("INSERT INTO t VALUES (1)").expect("insert"); // autocommits
        }
        let after = now_ms();

        // Reopen so recovery's ring scan populates the replay cache; some
        // committed transaction's record carries a v2 entry with a wall-clock
        // timestamp in [before, after] (for point-in-time restore).
        let storage = Storage::open(path.clone()).expect("reopen");
        let records = storage.replay_wal_entries().expect("replay");
        let mut found = false;
        for r in records {
            if r.entry_type != WAL_ENTRY_TYPE_REL {
                continue;
            }
            let rec = RelRecord::decode(&r.payload).expect("decode");
            if rec.kind == REL_KIND_TXN_COMMIT && rec.redo.len() >= 8 {
                assert_eq!(r.entry_version, 2, "new commit records are entry version 2");
                let ts = u64::from_le_bytes(rec.redo[..8].try_into().unwrap());
                if before <= ts && ts <= after {
                    found = true;
                }
            }
        }
        assert!(
            found,
            "a commit record carried a timestamp in [{before}, {after}]"
        );
        drop(storage);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn backup_log_requires_the_full_recovery_model() {
        let path = unique_temp_path("backuplog-simple");
        let engine = new_engine(&path);
        // SIMPLE (the default): BACKUP LOG is 4208.
        assert_eq!(
            sql_error_number(
                &engine,
                "BACKUP LOG truthdb TO DISK = '/tmp/truthdb-never.trn'"
            ),
            4208
        );
        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn full_recovery_holds_the_log_until_backup_log() {
        let path = unique_temp_path("backuplog-hold");
        let trn = unique_temp_path("backuplog-hold-trn");
        let engine = new_engine(&path);
        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("full");
        let marker = engine.storage().last_log_backup_lsn();
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=50 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        // A checkpoint cannot truncate past the log-backup floor under FULL.
        engine
            .storage()
            .write_checkpoint(b"cp", 1, 2, 1)
            .expect("checkpoint");
        assert!(
            engine.storage().wal_head() <= marker,
            "FULL pins the head at the log-backup floor"
        );

        // BACKUP LOG advances the floor, so a later checkpoint reclaims the log.
        let lit = trn.to_str().unwrap().replace('\'', "''");
        assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit}'"))["error"].is_null());
        assert!(engine.storage().last_log_backup_lsn() > marker);
        engine
            .storage()
            .write_checkpoint(b"cp2", 2, 3, 2)
            .expect("checkpoint");
        assert!(
            engine.storage().wal_head() > marker,
            "after BACKUP LOG the checkpoint reclaims past the old floor"
        );
        drop(engine);
        for p in [path, trn] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn log_backup_marker_and_hold_survive_reopen() {
        let path = unique_temp_path("backuplog-reopen");
        let trn = unique_temp_path("backuplog-reopen-trn");
        let engine = new_engine(&path);
        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("full");
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert");
        let lit = trn.to_str().unwrap().replace('\'', "''");
        assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit}'"))["error"].is_null());
        let marker = engine.storage().last_log_backup_lsn();
        drop(engine);

        let engine2 = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("engine");
        assert_eq!(
            engine2.storage().last_log_backup_lsn(),
            marker,
            "the marker persisted"
        );
        assert_eq!(
            engine2.storage().log_backup_hold(),
            Some(marker),
            "the hold re-registered on open"
        );
        assert_eq!(
            sql_rows(&engine2, "SELECT recovery_model_desc FROM sys.databases").1,
            vec![vec![Some("FULL".into())]]
        );
        drop(engine2);
        for p in [path, trn] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn full_recovery_ring_full_reports_9002() {
        let path = unique_temp_path("backuplog-9002");
        // A small ring so the un-backed-up log fills it quickly.
        let storage = Storage::create_with_wal_bounds(
            path.clone(),
            test_storage_options(),
            128 * 1024,
            128 * 1024,
        )
        .expect("create");
        let engine = Engine::new(storage).expect("engine");
        engine
            .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
            .expect("full");
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v NVARCHAR(120))")
            .expect("create");
        // FULL pins the head at the enable point, so with no BACKUP LOG the ring
        // fills and a write eventually reports 9002 (log full).
        let mut hit_9002 = false;
        for i in 0..4000 {
            let env = sql(
                &engine,
                &format!("INSERT INTO t VALUES ({i}, '{}')", "x".repeat(100)),
            );
            if let Some(n) = env["error"]["number"].as_i64() {
                assert_eq!(n, 9002, "ring-full under FULL reports 9002, got {env}");
                hit_9002 = true;
                break;
            }
        }
        assert!(hit_9002, "the ring filled and reported 9002");
        drop(engine);
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
    fn raiserror_is_exempt_from_xact_abort() {
        // SQL Server: "errors raised by RAISERROR are not affected by SET
        // XACT_ABORT" — the batch continues and the transaction stays
        // committable even under XACT_ABORT ON.
        let path = unique_temp_path("raiserror-xact-exempt");
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
             RAISERROR('mid-batch', 16, 1); \
             INSERT INTO t VALUES (2); \
             COMMIT",
        );
        // The RAISERROR is reported (the batch's continued error), but both
        // INSERTs ran and the COMMIT succeeded.
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(50000));
        assert!(!ctx.has_open_transaction(), "COMMIT went through");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1, 2], "the batch continued past RAISERROR");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn raiserror_in_try_enters_catch_without_dooming() {
        // Inside TRY, RAISERROR >= 11 transfers to CATCH — but even under
        // XACT_ABORT ON the transaction is NOT doomed (the exemption again):
        // XACT_STATE() reads 1 in the CATCH and COMMIT still works.
        let path = unique_temp_path("raiserror-try-undoomed");
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
             BEGIN TRY RAISERROR('caught', 16, 1); END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n, ERROR_NUMBER() AS e; END CATCH; \
             COMMIT",
        );
        assert!(out.error.is_none(), "caught: {:?}", out.error);
        let rowset = out
            .results
            .iter()
            .find_map(|r| match r {
                StatementResult::Rows(rowset) => Some(rowset),
                _ => None,
            })
            .expect("catch rowset");
        assert_eq!(
            rowset.rows[0],
            vec![Datum::BigInt(1), Datum::BigInt(50000)],
            "XACT_STATE 1 (not doomed), ERROR_NUMBER 50000"
        );
        assert!(!ctx.has_open_transaction(), "COMMIT went through");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn throw_terminates_the_batch_without_dooming() {
        // THROW ends the batch even when nothing dooms: under XACT_ABORT OFF
        // the transaction stays open and committable from the next batch.
        let path = unique_temp_path("throw-terminates");
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
             THROW 50001, 'stop here', 7; \
             INSERT INTO t VALUES (2)",
        );
        let error = out.error.expect("THROW surfaces");
        assert_eq!(
            (error.number, error.level, error.state),
            (50001, 16, 7),
            "THROW's number/severity/state"
        );
        assert!(ctx.has_open_transaction(), "not doomed, still open");
        let out = batch(&engine, &mut ctx, "COMMIT");
        assert!(out.error.is_none(), "committable: {:?}", out.error);
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1], "the second INSERT never ran");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn throw_under_xact_abort_dooms() {
        let path = unique_temp_path("throw-xact-dooms");
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
            "SET XACT_ABORT ON; BEGIN TRAN; INSERT INTO t VALUES (1); THROW 50001, 'x', 1;",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3930),
            "doomed: writes rejected"
        );
        batch(&engine, &mut ctx, "ROLLBACK");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(ids(&out), Vec::<i32>::new(), "nothing survived");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn bare_throw_rethrows_the_original_error() {
        // A bare THROW in a CATCH re-raises the caught error verbatim —
        // number, severity and state — and terminates the batch.
        let path = unique_temp_path("bare-throw");
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
             BEGIN CATCH THROW; END CATCH; \
             SELECT 99 AS n",
        );
        let error = out.error.as_ref().expect("re-thrown");
        assert_eq!(error.number, 2627, "the ORIGINAL number");
        assert_eq!(error.level, 14, "the ORIGINAL severity");
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "the statement after the construct never ran: {:?}",
            out.results
        );
        // Outside any CATCH, a bare THROW is 10704.
        let out = batch(&engine, &mut ctx, "THROW");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(10704));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn throw_number_below_50000_is_35100() {
        let path = unique_temp_path("throw-range");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "THROW 999, 'too low', 1");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(35100));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn at_at_error_tracks_the_previous_statement() {
        let path = unique_temp_path("at-at-error");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        // The duplicate INSERT fails (batch continues under XACT_ABORT OFF,
        // no transaction open): the next statement reads 2627, and the one
        // after reads 0 — reading @@ERROR is itself a statement that resets.
        // Inside a transaction (the batch-continue path; outside one an
        // error terminates the batch — the recorded divergence).
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             SELECT @@ERROR AS n; \
             SELECT @@ERROR AS n; \
             COMMIT",
        );
        let firsts: Vec<i64> = out
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => Some(i64::from(v)),
                    Datum::BigInt(v) => Some(v),
                    ref other => panic!("expected int, got {other:?}"),
                },
                _ => None,
            })
            .collect();
        assert_eq!(firsts, vec![2627, 0], "2627 then reset to 0");
        // Capturing into a variable inside the CATCH: the CATCH's first
        // statement still sees the number.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT @@ERROR AS n; END CATCH",
        );
        assert_eq!(ids(&out), vec![2627], "@@ERROR visible in the CATCH");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn raiserror_severity_10_is_informational() {
        // Severity <= 10 is a message, not an error: the statement SUCCEEDS,
        // the batch reports no error, and @@ERROR reads 0 — or 50000 under
        // WITH SETERROR.
        let path = unique_temp_path("raiserror-info");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "RAISERROR('just so you know', 10, 1); SELECT @@ERROR AS n",
        );
        assert!(out.error.is_none(), "informational: {:?}", out.error);
        assert_eq!(ids(&out), vec![0], "@@ERROR is 0");
        let out = batch(
            &engine,
            &mut ctx,
            "RAISERROR('noted', 5, 1) WITH SETERROR; SELECT @@ERROR AS n",
        );
        assert!(out.error.is_none());
        assert_eq!(ids(&out), vec![50000], "SETERROR stamps 50000");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn raiserror_formats_printf_arguments() {
        let path = unique_temp_path("raiserror-printf");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY RAISERROR('%s took %d ms (0x%x)', 16, 1, 'scan', 42, 255); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
        );
        assert!(out.error.is_none());
        let StatementResult::Rows(rows) = &out.results[0] else {
            panic!("expected rows");
        };
        assert_eq!(
            rows.rows[0][0],
            Datum::NVarChar("scan took 42 ms (0xff)".into())
        );
        // A directive with a wrong-typed argument is 2786; an unsupported
        // directive is 2787.
        let out = batch(&engine, &mut ctx, "RAISERROR('%d', 16, 1, 'not an int')");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2786));
        let out = batch(&engine, &mut ctx, "RAISERROR('%f', 16, 1, 1)");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2787));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn raiserror_above_18_requires_with_log() {
        let path = unique_temp_path("raiserror-log-gate");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "RAISERROR('big', 19, 1)");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2754));
        // With the option, 19 raises normally (catchable, not fatal).
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY RAISERROR('big', 19, 1) WITH LOG; END TRY \
             BEGIN CATCH SELECT ERROR_SEVERITY() AS n; END CATCH",
        );
        assert!(out.error.is_none());
        assert_eq!(ids(&out), vec![19]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn severity_20_bypasses_catch_and_dooms() {
        // Severity >= 20 is fatal: no CATCH sees it, the transaction dooms,
        // and (on TDS) the connection closes after delivery — the wire half
        // is pinned in the TDS end-to-end suite.
        let path = unique_temp_path("severity-20");
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
             BEGIN TRY RAISERROR('fatal', 20, 1) WITH LOG; END TRY \
             BEGIN CATCH SELECT 1 AS caught; END CATCH",
        );
        let error = out
            .error
            .as_ref()
            .expect("fatal error surfaces past the CATCH");
        assert_eq!((error.number, error.level), (50000, 20));
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "the CATCH block never ran: {:?}",
            out.results
        );
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (2)");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930), "doomed");
        batch(&engine, &mut ctx, "ROLLBACK");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn exec_inner_raiserror_in_try_reaches_catch_undoomed() {
        // The seam pin: a RAISERROR inside EXEC'd text, inside a TRY, under
        // XACT_ABORT ON. The doom decision is made where the statement kind
        // is known (the inner run_block) — the TRY and EXEC boundaries must
        // not re-derive it, or the exemption is lost in transit.
        let path = unique_temp_path("exec-raiserror-seam");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY EXEC sp_executesql N'RAISERROR(''inner'', 16, 1)'; END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH; \
             COMMIT",
        );
        assert!(out.error.is_none(), "caught: {:?}", out.error);
        assert_eq!(ids(&out), vec![1], "NOT doomed across both boundaries");
        assert!(!ctx.has_open_transaction(), "COMMIT went through");
        // Contrast: run_exec's OWN error (unknown proc) under the same setup
        // dooms per the ordinary rule — decided at its source.
        let out = batch(
            &engine,
            &mut ctx,
            "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY EXEC no_such_proc; END TRY \
             BEGIN CATCH SELECT XACT_STATE() AS n; END CATCH",
        );
        assert!(out.error.is_none(), "caught: {:?}", out.error);
        assert_eq!(ids(&out), vec![-1], "doomed: the EXEC's own failure");
        batch(&engine, &mut ctx, "ROLLBACK");
        let _ = std::fs::remove_file(path);
    }

    // ---- Stage 15 adversarial-review PoCs (severity/abort truth table) ----

    /// REVIEW PoC (defect: THROW loses its batch termination at the EXEC
    /// seam). SQL Server: an uncaught THROW terminates the batch — including
    /// the calling batch when it fires inside `sp_executesql` — without
    /// rolling back the transaction under XACT_ABORT OFF. At e49a515 the
    /// EXEC arm's fallback re-derives the continuation decision from
    /// severity alone (16, non-dooming), so inside an open transaction the
    /// OUTER batch continues past the EXEC. Pins the fixed behavior.
    #[test]
    fn review_poc_throw_inside_exec_terminates_the_outer_batch() {
        let path = unique_temp_path("review-throw-exec-seam");
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
             EXEC sp_executesql N'THROW 50001, ''from inner'', 1'; \
             INSERT INTO t VALUES (2); \
             COMMIT",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
        // The batch terminated at the THROW: the trailing INSERT and COMMIT
        // never ran, and the (undoomed) transaction is still open.
        assert!(
            ctx.has_open_transaction(),
            "THROW must terminate the batch before the COMMIT"
        );
        let out = batch(&engine, &mut ctx, "COMMIT");
        assert!(out.error.is_none(), "committable: {:?}", out.error);
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1], "the INSERT after the EXEC never ran");
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW probe (passes): severity >= 20 raised inside EXEC'd text
    /// bypasses a CATCH around the EXEC. No committed test covered the EXEC
    /// route for a fatal error; this pins it. (The EXEC arm's own fatal
    /// branch is behaviorally redundant — the in_try transfer plus the
    /// TryCatch arm's fatal filter produce the same outcome — so no mutation
    /// of that branch alone can fail a test; this pins the SEMANTICS.)
    #[test]
    fn review_poc_fatal_inside_exec_bypasses_catch() {
        let path = unique_temp_path("review-exec-fatal");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY EXEC sp_executesql N'RAISERROR(''die'', 20, 1) WITH LOG'; END TRY \
             BEGIN CATCH SELECT 1 AS caught; END CATCH",
        );
        let error = out.error.as_ref().expect("fatal surfaces past the CATCH");
        assert_eq!((error.number, error.level), (50000, 20));
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "the CATCH never ran: {:?}",
            out.results
        );
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW teeth probe (passes): @@ERROR is maintained by the EXEC arm's
    /// own error exits — the in_try transfer (the CATCH's first statement
    /// sees the EXEC's failure) and the batch-continue path (outside TRY,
    /// inside an open transaction). Dropping the EXEC arm's
    /// `txn_ctx.last_error` maintenance survives the committed suite; this
    /// pins it.
    #[test]
    fn review_poc_at_at_error_after_a_failed_exec() {
        let path = unique_temp_path("review-exec-at-at-error");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; EXEC no_such_proc; SELECT @@ERROR AS n; COMMIT",
        );
        assert_eq!(ids(&out), vec![2812], "@@ERROR after the failed EXEC");
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY EXEC also_missing; END TRY \
             BEGIN CATCH SELECT @@ERROR AS n; END CATCH",
        );
        assert_eq!(ids(&out), vec![2812], "@@ERROR visible in the CATCH");
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW teeth probe (passes): a bare THROW re-raises the INNERMOST
    /// CATCH's error. Reading `error_stack.first()` instead of `last()`
    /// survives the committed suite (every committed bare-THROW test has a
    /// one-deep stack); this pins the nested case.
    #[test]
    fn review_poc_bare_throw_in_nested_catch_rethrows_the_innermost() {
        let path = unique_temp_path("review-nested-bare-throw");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY THROW 50001, 'outer', 1; END TRY \
             BEGIN CATCH \
               BEGIN TRY THROW 50002, 'inner', 5; END TRY \
               BEGIN CATCH THROW; END CATCH \
             END CATCH",
        );
        let error = out.error.as_ref().expect("re-thrown");
        assert_eq!(
            (error.number, error.level, error.state),
            (50002, 16, 5),
            "the INNERMOST caught error, not the outer one"
        );
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW teeth probe (passes): RAISERROR state 0 reports as state 1.
    /// Dropping the `.max(1)` survives the committed suite (the slt line is
    /// a bare `statement ok`); this pins the reported state.
    #[test]
    fn review_poc_raiserror_state_zero_reports_as_one() {
        let path = unique_temp_path("review-state-zero");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY RAISERROR('x', 16, 0); END TRY \
             BEGIN CATCH SELECT ERROR_STATE() AS n; END CATCH",
        );
        assert!(out.error.is_none(), "caught: {:?}", out.error);
        assert_eq!(ids(&out), vec![1], "state 0 reports as 1");
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW teeth probe (passes): the flush before a RAISERROR statement.
    /// Removing `Statement::RaiseError` from run_block's flush condition
    /// survives the committed suite (every committed RAISERROR opens its
    /// batch, so no DONE is ever deferred when the INFO fires); with a
    /// deferred DONE in flight the mutation trips `BatchRun::info`'s
    /// debug_assert. This pins the shape.
    #[test]
    fn review_poc_info_after_a_write_flushes_the_deferred_dones() {
        let path = unique_temp_path("review-info-flush");
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
            "INSERT INTO t VALUES (1); RAISERROR('fyi', 5, 1); SELECT id FROM t",
        );
        assert!(out.error.is_none(), "informational: {:?}", out.error);
        assert_eq!(ids(&out), vec![1]);
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW record-only pin: an error raised while executing the RAISERROR
    /// statement itself (here 2786, a missing substitution argument) takes
    /// RAISERROR's lenient path — exempt from XACT_ABORT dooming, batch
    /// continues — because run_block classifies by statement KIND, not by
    /// which code produced the error. SQL Server's behavior for RAISERROR's
    /// own gate errors under XACT_ABORT ON is not clearly documented;
    /// pinned as-is so a change is deliberate.
    #[test]
    fn review_poc_raiserror_gate_errors_take_the_lenient_path() {
        let path = unique_temp_path("review-gate-lenient");
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
             RAISERROR('%d', 16, 1); \
             INSERT INTO t VALUES (2); \
             COMMIT",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2786));
        assert!(!ctx.has_open_transaction(), "COMMIT went through");
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1, 2], "the batch continued past 2786");
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW PoC (divergence, fix recommended): SQL Server substitutes
    /// "(null)" for a NULL substitution argument; e49a515 raises 2786. Pins
    /// the SQL Server behavior.
    #[test]
    fn review_poc_raiserror_null_argument_prints_null_marker() {
        let path = unique_temp_path("review-null-arg");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY RAISERROR('v=%s', 16, 1, NULL); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
        );
        assert!(out.error.is_none(), "caught: {:?}", out.error);
        let StatementResult::Rows(rows) = &out.results[0] else {
            panic!("expected rows, got {:?}", out.results);
        };
        assert_eq!(rows.rows[0][0], Datum::NVarChar("v=(null)".into()));
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW PoC (divergence, fix recommended): RAISERROR's integer
    /// substitution arguments are 32-bit in SQL Server (int is the widest
    /// accepted argument type), so %x on -1 prints ffffffff and %u prints
    /// 4294967295. e49a515 formats through u64 and prints the 64-bit
    /// two's complement. Pins the 32-bit width.
    #[test]
    fn review_poc_raiserror_hex_width_is_32_bit() {
        let path = unique_temp_path("review-hex-width");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY RAISERROR('%x %X %u', 16, 1, -1, -1, -1); END TRY \
             BEGIN CATCH SELECT ERROR_MESSAGE() AS m; END CATCH",
        );
        assert!(out.error.is_none(), "caught: {:?}", out.error);
        let StatementResult::Rows(rows) = &out.results[0] else {
            panic!("expected rows, got {:?}", out.results);
        };
        assert_eq!(
            rows.rows[0][0],
            Datum::NVarChar("ffffffff FFFFFFFF 4294967295".into())
        );
        let _ = std::fs::remove_file(path);
    }

    /// REVIEW probe (passes): THROW's bare-vs-args lookahead on tokens that
    /// start neither form. `THROW -1, ...` reads as a bare THROW (Minus is
    /// not an argument-start token) followed by junk, so the batch is a 102
    /// syntax error — the shape SQL Server reports too (its THROW arguments
    /// must be constants or variables).
    #[test]
    fn review_poc_throw_negative_number_is_a_syntax_error() {
        let path = unique_temp_path("review-throw-negative");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "THROW -1, 'x', 1");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(102));
        let out = batch(&engine, &mut ctx, "THROW (SELECT 1), 'x', 1");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(102));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn if_else_takes_the_right_branch_including_null() {
        // T-SQL three-valued conditions: TRUE runs THEN; FALSE and NULL
        // (UNKNOWN) take the ELSE.
        let path = unique_temp_path("if-else");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "IF 1 = 1 SELECT 1 AS n ELSE SELECT 2 AS n",
        );
        assert_eq!(ids(&out), vec![1]);
        let out = batch(
            &engine,
            &mut ctx,
            "IF 1 = 2 SELECT 1 AS n ELSE SELECT 2 AS n",
        );
        assert_eq!(ids(&out), vec![2]);
        let out = batch(
            &engine,
            &mut ctx,
            "IF NULL = NULL SELECT 1 AS n ELSE SELECT 2 AS n",
        );
        assert_eq!(ids(&out), vec![2], "UNKNOWN takes the ELSE");
        // ALIASLESS selects: `ELSE` must not be readable as an implicit
        // column alias (`SELECT 1 ELSE` would silently detach the branch).
        let out = batch(&engine, &mut ctx, "IF 1 = 2 SELECT 1 ELSE SELECT 2");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![2], "ELSE binds to the IF, not as an alias");
        // Without an ELSE, an untaken IF runs nothing.
        let out = batch(&engine, &mut ctx, "IF 1 = 2 SELECT 1 AS n; SELECT 3 AS n");
        assert_eq!(ids(&out), vec![3]);
        // A non-boolean condition is 4145.
        let out = batch(&engine, &mut ctx, "IF 7 SELECT 1 AS n");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(4145));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn if_exists_subquery_condition_works() {
        // The bread-and-butter SSMS shape: IF EXISTS (SELECT ...) over a real
        // table, both polarities, plus a scalar-subquery comparison.
        let path = unique_temp_path("if-exists");
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
            "IF EXISTS (SELECT * FROM t WHERE id = 1) SELECT 10 AS n ELSE SELECT 20 AS n",
        );
        assert_eq!(ids(&out), vec![10]);
        let out = batch(
            &engine,
            &mut ctx,
            "IF EXISTS (SELECT * FROM t WHERE id = 99) SELECT 10 AS n ELSE SELECT 20 AS n",
        );
        assert_eq!(ids(&out), vec![20]);
        let out = batch(
            &engine,
            &mut ctx,
            "IF (SELECT COUNT(*) FROM t) = 1 SELECT 30 AS n",
        );
        assert_eq!(ids(&out), vec![30], "scalar subquery in the condition");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn while_loop_runs_with_break_and_continue() {
        let path = unique_temp_path("while-loop");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        // A counted loop driven by a variable.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 1; \
             WHILE @i <= 5 \
             BEGIN \
               INSERT INTO t VALUES (@i); \
               SET @i = @i + 1; \
             END; \
             SELECT COUNT(*) AS n FROM t",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![5]);
        // CONTINUE skips even ids; BREAK stops at 8.
        batch(&engine, &mut ctx, "DELETE FROM t");
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i >= 8 BREAK; \
               IF @i % 2 = 0 CONTINUE; \
               INSERT INTO t VALUES (@i); \
             END; \
             SELECT id FROM t ORDER BY id",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![1, 3, 5, 7]);
        let _ = std::fs::remove_file(path);
    }

    /// The first rowset's first row, as `i32`s across its columns.
    fn row_ints(outcome: &BatchOutcome) -> Vec<i32> {
        for result in &outcome.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset.rows[0]
                    .iter()
                    .map(|d| match d {
                        Datum::TinyInt(v) => *v as i32,
                        Datum::SmallInt(v) => *v as i32,
                        Datum::Int(v) => *v,
                        Datum::BigInt(v) => *v as i32,
                        other => panic!("expected integer, got {other:?}"),
                    })
                    .collect();
            }
        }
        panic!("no rowset in outcome: {:?}", outcome.results);
    }

    #[test]
    fn cursors_iterate_scroll_and_report_fetch_status() {
        let path = unique_temp_path("cursors");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, v INT)",
        );
        batch(
            &engine,
            &mut ctx,
            "INSERT INTO nums VALUES (1,10),(2,20),(3,30)",
        );

        // Forward iteration: FETCH INTO drives a @@FETCH_STATUS loop that sums v.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @sum INT = 0; \
             DECLARE @v INT; \
             DECLARE c CURSOR FOR SELECT v FROM nums ORDER BY id; \
             OPEN c; \
             FETCH NEXT FROM c INTO @v; \
             WHILE @@FETCH_STATUS = 0 \
             BEGIN \
               SET @sum = @sum + @v; \
               FETCH NEXT FROM c INTO @v; \
             END; \
             CLOSE c; \
             DEALLOCATE c; \
             SELECT @sum AS total",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![60]);

        // A SCROLL cursor addresses rows by direction. The trailing FETCH PRIOR
        // runs off the start: @@FETCH_STATUS becomes -1 and @v keeps its value.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @last INT; DECLARE @first INT; DECLARE @abs INT; \
             DECLARE @rel INT; DECLARE @v INT; \
             DECLARE c SCROLL CURSOR FOR SELECT v FROM nums ORDER BY id; \
             OPEN c; \
             FETCH LAST FROM c INTO @last; \
             FETCH FIRST FROM c INTO @first; \
             FETCH ABSOLUTE 2 FROM c INTO @abs; \
             FETCH RELATIVE -1 FROM c INTO @rel; \
             SET @v = @rel; \
             FETCH PRIOR FROM c INTO @v; \
             DECLARE @st INT = @@FETCH_STATUS; \
             CLOSE c; DEALLOCATE c; \
             SELECT @last, @first, @abs, @rel, @v, @st",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        // LAST=30, FIRST=10, ABSOLUTE 2=20, RELATIVE -1 (from row 2)=10, @v held
        // at 10 (the off-start FETCH left it), @@FETCH_STATUS=-1.
        assert_eq!(row_ints(&out), vec![30, 10, 20, 10, 10, -1]);

        // FETCH without INTO returns the fetched row to the client.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE c CURSOR FOR SELECT v FROM nums ORDER BY id; \
             OPEN c; FETCH NEXT FROM c; CLOSE c; DEALLOCATE c",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![10]);

        // FETCH on an unopened cursor -> 16917.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE c CURSOR FOR SELECT v FROM nums; FETCH NEXT FROM c",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(16917));

        // A cursor name that was never declared -> 16916.
        let out = batch(&engine, &mut ctx, "OPEN nope");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(16916));

        // Re-declaring an existing cursor name -> 16915.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE c CURSOR FOR SELECT v FROM nums; \
             DECLARE c CURSOR FOR SELECT v FROM nums",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(16915));

        // OPEN of an already-open cursor -> 16905.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE c CURSOR FOR SELECT v FROM nums; OPEN c; OPEN c",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(16905));

        // A FETCH RELATIVE offset near i64::MAX must not overflow the position:
        // it saturates off the end (status -1), leaving @v unchanged. (A checked
        // build would panic without the saturating add.)
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @v INT = 7; \
             DECLARE c SCROLL CURSOR FOR SELECT v FROM nums WHERE id < 99 ORDER BY id; \
             OPEN c; \
             FETCH NEXT FROM c INTO @v; \
             FETCH RELATIVE 9223372036854775807 FROM c INTO @v; \
             DECLARE @st INT = @@FETCH_STATUS; \
             CLOSE c; DEALLOCATE c; \
             SELECT @v, @st",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(row_ints(&out), vec![10, -1]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn break_crosses_a_try_and_return_exits_the_batch() {
        let path = unique_temp_path("flow-crossings");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        // BREAK inside a TRY leaves the loop without touching the CATCH.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               BEGIN TRY IF @i = 3 BREAK; END TRY BEGIN CATCH SELECT 99 AS n; END CATCH \
             END; \
             SELECT @i AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![3], "the CATCH never ran, the loop broke");
        // RETURN exits the batch mid-way.
        let out = batch(
            &engine,
            &mut ctx,
            "INSERT INTO t VALUES (1); RETURN; INSERT INTO t VALUES (2)",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "SELECT id FROM t ORDER BY id");
        assert_eq!(ids(&out), vec![1], "the post-RETURN INSERT never ran");
        // RETURN with a value is a batch-context error (178), and
        // BREAK/CONTINUE outside a loop are compile-time 135/136.
        let out = batch(&engine, &mut ctx, "RETURN 5");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
        let out = batch(&engine, &mut ctx, "BREAK");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(135));
        let out = batch(&engine, &mut ctx, "CONTINUE");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(136));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn if_condition_reads_at_at_error_before_resetting_it() {
        // The canonical pattern: `IF @@ERROR <> 0` sees the failed statement's
        // number (the IF resets @@ERROR only AFTER its condition evaluated).
        let path = unique_temp_path("if-at-at-error");
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
            "BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             IF @@ERROR <> 0 SELECT 111 AS n ELSE SELECT 222 AS n; \
             SELECT @@ERROR AS n; \
             COMMIT",
        );
        let firsts: Vec<i64> = out
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => Some(i64::from(v)),
                    Datum::BigInt(v) => Some(v),
                    ref other => panic!("expected int, got {other:?}"),
                },
                _ => None,
            })
            .collect();
        assert_eq!(
            firsts,
            vec![111, 0],
            "the IF saw 2627, then reset @@ERROR to 0"
        );
        // An untaken IF with no ELSE: the IF's own reset is the ONLY one (no
        // branch statement runs to mask it) — @@ERROR reads 0 after it.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; \
             INSERT INTO t VALUES (1); \
             IF 1 = 2 SELECT 999 AS n; \
             SELECT @@ERROR AS n; \
             COMMIT",
        );
        assert_eq!(
            ids(&out),
            vec![0],
            "the IF itself reset @@ERROR though no branch ran"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn doomed_transaction_still_runs_the_canonical_catch_pattern() {
        // IF XACT_STATE() = -1 ROLLBACK — the documented CATCH idiom — must
        // work inside a doomed transaction.
        let path = unique_temp_path("doomed-if-rollback");
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
             BEGIN TRY INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH \
               IF XACT_STATE() = -1 ROLLBACK; \
             END CATCH; \
             SELECT CAST(@@TRANCOUNT AS INT) AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![0], "the doomed transaction was rolled back");
        assert!(!ctx.has_open_transaction());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_doomed_gate_condition_reads_branch_writes_gated() {
        // In a doomed transaction's CATCH: an IF condition's subquery READ is
        // legal (SQL Server allows reads in a doomed transaction), but a
        // write inside a taken branch still hits the 3930 gate.
        let path = unique_temp_path("cf-doomed-gate");
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
             BEGIN TRY INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH \
               IF EXISTS (SELECT * FROM t WHERE id = 1) SELECT 41 AS n ELSE SELECT 40 AS n; \
               IF XACT_STATE() = -1 ROLLBACK; \
             END CATCH; \
             SELECT 42 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let firsts: Vec<i64> = out
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => Some(i64::from(v)),
                    Datum::BigInt(v) => Some(v),
                    ref other => panic!("expected int, got {other:?}"),
                },
                _ => None,
            })
            .collect();
        assert_eq!(
            firsts,
            vec![41, 42],
            "the doomed CATCH's condition read saw the txn's own row"
        );
        // A branch WRITE in the doomed CATCH is still rejected with 3930.
        let out = batch(
            &engine,
            &mut ctx,
            "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             BEGIN TRY INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH \
               IF 1 = 1 INSERT INTO t VALUES (9); \
             END CATCH",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3930));
        batch(&engine, &mut ctx, "ROLLBACK");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_txn_control_inside_while() {
        // BEGIN TRAN / COMMIT (and ROLLBACK) balanced per iteration:
        // @@TRANCOUNT does not drift across iterations.
        let path = unique_temp_path("cf-txn-in-while");
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
            "DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               BEGIN TRAN; INSERT INTO t VALUES (@i); COMMIT; \
               SET @i = @i + 1; \
             END; \
             SELECT CAST(@@TRANCOUNT AS INT) AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![0], "trancount balanced after the loop");
        let out = batch(&engine, &mut ctx, "SELECT COUNT(*) AS n FROM t");
        assert_eq!(ids(&out), vec![3]);
        // Per-iteration ROLLBACK: every iteration's insert is undone.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 10; \
             WHILE @i < 13 \
             BEGIN \
               BEGIN TRAN; INSERT INTO t VALUES (@i); ROLLBACK; \
               SET @i = @i + 1; \
             END; \
             SELECT COUNT(*) AS n FROM t WHERE id >= 10",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![0]);
        assert!(!ctx.has_open_transaction());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_break_inside_catch_inside_while() {
        // BREAK issued from a CATCH block still terminates the enclosing
        // WHILE (the CATCH's flow propagates through the TryCatch arm).
        let path = unique_temp_path("cf-break-in-catch");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        // RETURN issued from a CATCH block exits the batch. This runs FIRST:
        // it fails fast if the CATCH's flow is swallowed, where the BREAK
        // case below would spin forever instead.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH RETURN; END CATCH; \
             SELECT 6 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "the CATCH's RETURN exited the batch: {:?}",
            out.results
        );
        let out = batch(
            &engine,
            &mut ctx,
            "WHILE 1 = 1 \
             BEGIN \
               BEGIN TRY INSERT INTO t VALUES (1); END TRY \
               BEGIN CATCH BREAK; END CATCH \
             END; \
             SELECT 77 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![77], "the CATCH's BREAK ended the loop");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_loop_body_error_continues_or_dooms() {
        // XACT_ABORT OFF in a transaction: a non-dooming body error rolls
        // back only that statement — the LOOP keeps iterating (it must not
        // swallow the error either: the batch reports it at the end).
        let path = unique_temp_path("cf-loop-body-error");
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
            "BEGIN TRAN; \
             DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               SET @i = @i + 1; \
               INSERT INTO t VALUES (1); \
             END; \
             SELECT @i AS n; \
             COMMIT",
        );
        assert_eq!(
            ids(&out),
            vec![3],
            "all three iterations ran despite the per-iteration 2627"
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(2627),
            "the continued error still surfaces at batch end"
        );
        assert!(!ctx.has_open_transaction(), "the COMMIT went through");
        // XACT_ABORT ON: the first body error dooms and ends the batch
        // mid-loop — the loop must NOT swallow it and keep iterating.
        let out = batch(
            &engine,
            &mut ctx,
            "SET XACT_ABORT ON; \
             BEGIN TRAN; \
             DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               SET @i = @i + 1; \
               INSERT INTO t VALUES (1); \
             END; \
             SELECT @i AS n; \
             COMMIT",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2627));
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "the batch ended mid-loop: no SELECT ran: {:?}",
            out.results
        );
        assert!(ctx.has_open_transaction(), "doomed transaction stays open");
        batch(&engine, &mut ctx, "ROLLBACK");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_raiserror_and_throw_inside_while() {
        // RAISERROR >= 11 outside TRY is statement-scope: the loop keeps
        // running and the error surfaces after the batch finishes. THROW is
        // batch-terminating: it ends the loop AND the batch.
        let path = unique_temp_path("cf-raise-throw-loop");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; \
             WHILE @i < 3 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i = 2 RAISERROR('boom', 16, 1); \
             END; \
             SELECT @i AS n",
        );
        assert_eq!(ids(&out), vec![3], "the loop survived the RAISERROR");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(50000),
            "the RAISERROR still surfaces at batch end"
        );
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i = 2 THROW 50001, 'stop', 1; \
             END; \
             SELECT 9 AS n",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "THROW ended the batch, not just the loop: {:?}",
            out.results
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_return_unwinds_nested_control_flow() {
        // RETURN inside WHILE exits the batch (not just the loop), and a
        // RETURN nested in WHILE-inside-TRY-inside-BEGIN..END unwinds
        // everything without running any CATCH.
        let path = unique_temp_path("cf-return-unwind");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; \
             WHILE 1 = 1 \
             BEGIN \
               SET @i = @i + 1; \
               IF @i = 2 RETURN; \
             END; \
             SELECT 1 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "RETURN exited the batch: the post-loop SELECT never ran: {:?}",
            out.results
        );
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY \
               BEGIN \
                 WHILE 1 = 1 \
                 BEGIN \
                   RETURN; \
                 END \
               END \
             END TRY \
             BEGIN CATCH SELECT 5 AS n; END CATCH; \
             SELECT 6 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "RETURN unwound block+loop+TRY without a CATCH: {:?}",
            out.results
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_while_condition_resets_last_error() {
        // The WHILE's per-iteration condition evaluation resets @@ERROR (like
        // the IF's) — a body error set on the LAST iteration reads 0 after
        // the final (false) condition evaluation. SQL Server ambiguity noted:
        // the IF analogy (every statement evaluation resets @@ERROR) is what
        // this pins.
        let path = unique_temp_path("cf-while-at-at-error");
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
            "BEGIN TRAN; \
             DECLARE @i INT = 0; \
             WHILE @i < 1 \
             BEGIN \
               SET @i = @i + 1; \
               INSERT INTO t VALUES (1); \
             END; \
             SELECT @@ERROR AS n; \
             COMMIT",
        );
        assert_eq!(
            ids(&out),
            vec![0],
            "the final condition evaluation reset @@ERROR"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_parser_edges() {
        let path = unique_temp_path("cf-parser-edges");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        // Dangling ELSE binds to the INNERMOST IF (as in SQL Server): the
        // inner condition is false, so the ELSE runs.
        let out = batch(
            &engine,
            &mut ctx,
            "IF 1 = 1 IF 1 = 2 SELECT 1 AS n ELSE SELECT 2 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![2], "the ELSE belongs to the inner IF");
        // ...so when the OUTER condition is false, nothing runs at all.
        let out = batch(
            &engine,
            &mut ctx,
            "IF 1 = 2 IF 1 = 1 SELECT 1 AS n ELSE SELECT 2 AS n; SELECT 3 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![3]);
        // CASE consumes its own ELSE; the IF grammar is unaffected.
        let out = batch(
            &engine,
            &mut ctx,
            "IF CASE WHEN 1 = 1 THEN 1 ELSE 2 END = 1 SELECT 4 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![4]);
        // A semicolon ends the IF: `; ELSE` is a syntax error, as in T-SQL.
        let out = batch(
            &engine,
            &mut ctx,
            "IF 1 = 1 SELECT 1 AS n; ELSE SELECT 2 AS n",
        );
        assert!(out.error.is_some(), "`; ELSE` must not attach to the IF");
        // A block whose first statement is BEGIN TRAN parses as a block.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN BEGIN TRAN; INSERT INTO t VALUES (21); COMMIT; END",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(!ctx.has_open_transaction());
        // WHILE whose body is a bare BREAK parses and runs zero-or-more times.
        let out = batch(&engine, &mut ctx, "WHILE 1 = 0 BREAK; SELECT 8 AS n");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![8]);
        // BREAK inside EXEC'd text is its own batch scope: compile-time 135
        // surfaces as the EXEC's error even though the EXEC sits in a WHILE.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; \
             WHILE @i < 1 \
             BEGIN \
               SET @i = 1; \
               EXEC sp_executesql N'BREAK'; \
             END",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(135));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_tsql_fidelity_gaps() {
        let path = unique_temp_path("cf-fidelity");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        // An empty block is a compile-time syntax error in T-SQL ("Incorrect
        // syntax near 'END'") — expectation is the recommended FIXED behavior
        // (3360df1 accepts it as a no-op).
        let out = batch(&engine, &mut ctx, "BEGIN END");
        assert!(
            out.error.is_some(),
            "T-SQL rejects an empty BEGIN END block"
        );
        // RETURN with a string value is context error 178 in a batch, like
        // any RETURN with a value — expectation is the recommended FIXED
        // behavior (3360df1 gives 102 near 'x': the string is not parsed as
        // the RETURN's value).
        let out = batch(&engine, &mut ctx, "RETURN 'x'");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
        // Recorded divergence (pinning CURRENT behavior): SQL Server's
        // IF EXISTS sets @@ROWCOUNT from the probe scan (0 here); TruthDB's
        // condition evaluation leaves @@ROWCOUNT untouched, so the INSERT's
        // count of 1 survives the untaken IF.
        let out = batch(
            &engine,
            &mut ctx,
            "INSERT INTO t VALUES (2); \
             IF EXISTS (SELECT * FROM t WHERE id = 99) SELECT 7 AS n; \
             SELECT CAST(@@ROWCOUNT AS INT) AS n",
        );
        assert_eq!(ids(&out), vec![1]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_condition_error_shapes() {
        let path = unique_temp_path("cf-cond-errors");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO t VALUES (1), (2)");
        // An undeclared variable in the condition is the usual 137.
        let out = batch(&engine, &mut ctx, "IF @nope = 1 SELECT 1 AS n");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(137));
        // A scalar condition subquery returning two rows is 512.
        let out = batch(&engine, &mut ctx, "IF (SELECT id FROM t) = 1 SELECT 1 AS n");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(512));
        // An assignment SELECT nested in a condition subquery is rejected.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @x INT; IF (SELECT @x = 1) = 1 SELECT 1 AS n",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(141));
        // Nested EXISTS inside the condition's subquery works.
        let out = batch(
            &engine,
            &mut ctx,
            "IF EXISTS (SELECT * FROM t WHERE EXISTS (SELECT * FROM t WHERE id = 2)) \
             SELECT 8 AS n ELSE SELECT 9 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![8]);
        // A condition error outside any transaction ends the batch (same
        // ladder as a failed statement); in a transaction with XACT_ABORT
        // OFF the batch continues past the failed IF, taking no branch.
        let out = batch(
            &engine,
            &mut ctx,
            "IF 1 / 0 = 1 SELECT 1 AS n ELSE SELECT 2 AS n",
        );
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(8134));
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "neither branch ran: {:?}",
            out.results
        );
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; \
             IF 1 / 0 = 1 SELECT 1 AS n ELSE SELECT 2 AS n; \
             SELECT 99 AS n; \
             COMMIT",
        );
        assert_eq!(ids(&out), vec![99], "no branch ran; the batch continued");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(8134));
        assert!(!ctx.has_open_transaction());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scalar_function_body_tables_locked_up_front() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("udf-lock-seam");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE secret (id INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        engine
            .execute("CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY)")
            .expect("t2");
        engine
            .execute(
                "CREATE FUNCTION dbo.secret_count () RETURNS INT AS \
                 BEGIN RETURN (SELECT COUNT(*) FROM secret) END",
            )
            .expect("fn");
        let secret = table_object_id(&engine, "secret");
        // A query that calls the function must Shared-lock the table its body
        // reads, up front — otherwise the body would read it with no lock held
        // under 2PL (the seam-defect class). Checked in the SELECT list, the
        // WHERE clause, and an IF condition.
        for sql in [
            "SELECT id, dbo.secret_count() FROM t2",
            "SELECT id FROM t2 WHERE dbo.secret_count() > 0",
            "IF dbo.secret_count() > 0 SELECT 1 AS n",
        ] {
            let locks = engine.analyze_locks(sql, Isolation::ReadCommitted);
            assert!(
                locks.contains(&(Resource::Table(secret), LockMode::Shared)),
                "the function's inner-read table must be Shared-locked for `{sql}`: {locks:?}"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scalar_function_isolation_error_and_nesting() {
        let path = unique_temp_path("udf-isolation");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        // A function does not see caller locals: a body reference to `@outer`
        // is undeclared inside the function scope (137), never the caller's
        // value.
        batch(
            &engine,
            &mut ctx,
            "CREATE FUNCTION dbo.leak () RETURNS INT AS BEGIN RETURN @outer END",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @outer INT = 5; SELECT dbo.leak() AS n",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(137),
            "a function must not see caller locals: {:?}",
            out.error
        );
        // An error inside the body aborts the calling statement.
        batch(
            &engine,
            &mut ctx,
            "CREATE FUNCTION dbo.divzero (@x INT) RETURNS INT AS BEGIN RETURN 1 / @x END",
        );
        let out = batch(&engine, &mut ctx, "SELECT dbo.divzero(0) AS n");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(8134),
            "divide-by-zero in a function aborts the query: {:?}",
            out.error
        );
        // Unbounded recursion hits the shared nesting cap (217), and unwinds.
        batch(
            &engine,
            &mut ctx,
            "CREATE FUNCTION dbo.recur (@x INT) RETURNS INT AS BEGIN RETURN dbo.recur(@x) END",
        );
        let out = batch(&engine, &mut ctx, "SELECT dbo.recur(1) AS n");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(217),
            "recursion must hit the nesting cap: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn goto_jumps_forward_backward_and_errors_on_missing_label() {
        let path = unique_temp_path("goto");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (n INT NOT NULL PRIMARY KEY)")
            .expect("create");

        // Forward GOTO skips the statement it jumps over.
        engine
            .execute(
                "INSERT INTO t VALUES (1); GOTO skip; INSERT INTO t VALUES (2); \
                 skip: INSERT INTO t VALUES (3)",
            )
            .expect("forward goto");
        assert_eq!(
            sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
            vec![vec![Some("1".into())], vec![Some("3".into())]],
            "forward GOTO skipped VALUES(2)"
        );

        // Backward GOTO from inside an IF drives a counting loop (10, 11, 12).
        engine.execute("DELETE FROM t").expect("clear");
        engine
            .execute(
                "DECLARE @i INT = 10; \
                 loop: INSERT INTO t VALUES (@i); SET @i = @i + 1; IF @i <= 12 GOTO loop",
            )
            .expect("backward goto loop");
        assert_eq!(
            sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
            vec![
                vec![Some("10".into())],
                vec![Some("11".into())],
                vec![Some("12".into())],
            ],
            "backward GOTO from an IF looped"
        );

        // A GOTO to a label defined nowhere in scope errors 133.
        assert_eq!(
            sql_error_number(&engine, "GOTO nowhere"),
            133,
            "a GOTO to an undefined label errors 133"
        );

        // A label inside a BEGIN...END block (no semicolon after the label).
        engine.execute("DELETE FROM t").expect("clear");
        engine
            .execute("BEGIN GOTO d; INSERT INTO t VALUES (7); d: INSERT INTO t VALUES (8) END")
            .expect("label in a block");
        assert_eq!(
            sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
            vec![vec![Some("8".into())]],
            "GOTO skipped VALUES(7) inside the block"
        );

        // A label inside a stored procedure body.
        engine.execute("DELETE FROM t").expect("clear");
        assert!(
            sql(
                &engine,
                "CREATE PROCEDURE fill AS BEGIN GOTO d; INSERT INTO t VALUES (20); d: INSERT INTO t VALUES (21) END"
            )["error"]
                .is_null(),
            "a procedure body with a label creates cleanly"
        );
        engine.execute("EXEC fill").expect("exec proc");
        assert_eq!(
            sql_rows(&engine, "SELECT n FROM t ORDER BY n").1,
            vec![vec![Some("21".into())]],
            "GOTO inside a procedure body skipped VALUES(20)"
        );

        // A label repeated in the same list errors 132.
        assert_eq!(
            sql_error_number(&engine, "d: SELECT 1; d: SELECT 2"),
            132,
            "a duplicate label errors 132"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cross_and_outer_apply_correlate_the_right_side_to_each_left_row() {
        let path = unique_temp_path("apply");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE seq (v INT NOT NULL PRIMARY KEY)")
            .expect("seq");
        for v in 1..=3 {
            engine
                .execute(&format!("INSERT INTO seq VALUES ({v})"))
                .expect("ins seq");
        }
        engine
            .execute("CREATE TABLE t (k INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine.execute("INSERT INTO t VALUES (2)").expect("t2");
        engine.execute("INSERT INTO t VALUES (0)").expect("t0");
        engine
            .execute("CREATE FUNCTION dbo.upto (@n INT) RETURNS TABLE AS RETURN (SELECT v FROM seq WHERE v <= @n)")
            .expect("tvf");

        // CROSS APPLY correlates upto(t.k) to each left row and drops the k=0 row
        // (upto(0) yields no rows).
        assert_eq!(
            sql_rows(
                &engine,
                "SELECT t.k, u.v FROM t CROSS APPLY dbo.upto(t.k) u ORDER BY t.k, u.v"
            )
            .1,
            vec![
                vec![Some("2".into()), Some("1".into())],
                vec![Some("2".into()), Some("2".into())],
            ],
            "CROSS APPLY correlates and drops empty-right rows"
        );

        // OUTER APPLY keeps the k=0 row with NULL for the right columns.
        assert_eq!(
            sql_rows(
                &engine,
                "SELECT t.k, u.v FROM t OUTER APPLY dbo.upto(t.k) u ORDER BY t.k, u.v"
            )
            .1,
            vec![
                vec![Some("0".into()), None],
                vec![Some("2".into()), Some("1".into())],
                vec![Some("2".into()), Some("2".into())],
            ],
            "OUTER APPLY keeps empty-right rows with NULL"
        );

        // A correlated derived table on the right side works too.
        assert_eq!(
            sql_rows(
                &engine,
                "SELECT t.k, d.v FROM t CROSS APPLY (SELECT v FROM seq WHERE v <= t.k) d ORDER BY t.k, d.v"
            )
            .1,
            vec![
                vec![Some("2".into()), Some("1".into())],
                vec![Some("2".into()), Some("2".into())],
            ],
            "CROSS APPLY over a correlated derived table"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn update_function_reports_touched_columns_in_a_trigger() {
        let path = unique_temp_path("update-fn");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, b INT)")
            .expect("t");
        engine
            .execute("CREATE TABLE tlog (which VARCHAR(20) NOT NULL PRIMARY KEY)")
            .expect("tlog");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER UPDATE AS IF UPDATE(a) INSERT INTO tlog VALUES ('a'); IF UPDATE(b) INSERT INTO tlog VALUES ('b')")
            .expect("trigger");
        engine
            .execute("INSERT INTO t VALUES (1, 10, 20)")
            .expect("seed");

        // Update only column a: UPDATE(a) is true, UPDATE(b) is false.
        engine
            .execute("UPDATE t SET a = 99 WHERE id = 1")
            .expect("update a");
        assert_eq!(
            sql_rows(&engine, "SELECT which FROM tlog ORDER BY which").1,
            vec![vec![Some("a".into())]],
            "only column a is reported updated"
        );

        // Now update column b: UPDATE(b) is true (a is not in this SET list).
        engine
            .execute("UPDATE t SET b = 30 WHERE id = 1")
            .expect("update b");
        assert_eq!(
            sql_rows(&engine, "SELECT which FROM tlog ORDER BY which").1,
            vec![vec![Some("a".into())], vec![Some("b".into())]],
            "column b is now reported updated"
        );

        // Outside a trigger, UPDATE()/COLUMNS_UPDATED() error 4101.
        assert_eq!(
            sql_error_number(&engine, "SELECT UPDATE(a)"),
            4101,
            "UPDATE() outside a trigger errors"
        );
        assert_eq!(
            sql_error_number(&engine, "SELECT COLUMNS_UPDATED()"),
            4101,
            "COLUMNS_UPDATED() outside a trigger errors"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn instead_of_triggers_replace_the_dml() {
        let path = unique_temp_path("instead-of");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE ilog (n INT NOT NULL PRIMARY KEY)")
            .expect("ilog");

        // INSTEAD OF INSERT: the base insert is bypassed; the body logs `inserted`.
        engine
            .execute("CREATE TABLE ti (id INT NOT NULL PRIMARY KEY)")
            .expect("ti");
        engine
            .execute("CREATE TRIGGER trg_i ON ti INSTEAD OF INSERT AS INSERT INTO ilog SELECT id FROM inserted")
            .expect("io insert");
        engine.execute("INSERT INTO ti VALUES (5)").expect("insert");
        assert_eq!(
            sql_rows(&engine, "SELECT COUNT(*) FROM ti").1,
            vec![vec![Some("0".into())]],
            "INSTEAD OF INSERT bypassed the base insert"
        );

        // INSTEAD OF DELETE: base delete bypassed; body logs `deleted` (+100).
        engine
            .execute("CREATE TABLE td (id INT NOT NULL PRIMARY KEY)")
            .expect("td");
        engine
            .execute("INSERT INTO td VALUES (7)")
            .expect("seed td");
        engine
            .execute("CREATE TRIGGER trg_d ON td INSTEAD OF DELETE AS INSERT INTO ilog SELECT id + 100 FROM deleted")
            .expect("io delete");
        engine
            .execute("DELETE FROM td WHERE id = 7")
            .expect("delete");
        assert_eq!(
            sql_rows(&engine, "SELECT COUNT(*) FROM td").1,
            vec![vec![Some("1".into())]],
            "INSTEAD OF DELETE bypassed the base delete"
        );

        // INSTEAD OF UPDATE: base update bypassed; body logs `inserted` (new v).
        engine
            .execute("CREATE TABLE tu (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("tu");
        engine
            .execute("INSERT INTO tu VALUES (1, 3)")
            .expect("seed tu");
        engine
            .execute("CREATE TRIGGER trg_u ON tu INSTEAD OF UPDATE AS INSERT INTO ilog SELECT v FROM inserted")
            .expect("io update");
        engine
            .execute("UPDATE tu SET v = 42 WHERE id = 1")
            .expect("update");
        assert_eq!(
            sql_rows(&engine, "SELECT v FROM tu").1,
            vec![vec![Some("3".into())]],
            "INSTEAD OF UPDATE bypassed the base update"
        );

        // Every INSTEAD OF body ran over the proposed images: 5 (ins), 42 (upd new
        // value), 107 (del id + 100).
        assert_eq!(
            sql_rows(&engine, "SELECT n FROM ilog ORDER BY n").1,
            vec![
                vec![Some("5".into())],
                vec![Some("42".into())],
                vec![Some("107".into())],
            ],
            "the INSTEAD OF bodies ran over inserted/deleted"
        );

        // A second INSTEAD OF trigger for the same action errors 2113.
        assert_eq!(
            sql_error_number(
                &engine,
                "CREATE TRIGGER trg_i2 ON ti INSTEAD OF INSERT AS SELECT 1"
            ),
            2113,
            "only one INSTEAD OF trigger per action"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn disable_and_enable_trigger_controls_firing() {
        let path = unique_temp_path("trigger-disable");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create t");
        engine
            .execute("CREATE TABLE log (n INT NOT NULL PRIMARY KEY)")
            .expect("create log");
        engine
            .execute(
                "CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO log SELECT id FROM inserted",
            )
            .expect("create trigger");
        let log_count = |e: &Engine| sql_rows(e, "SELECT COUNT(*) FROM log").1;

        // Enabled by default: the insert fires the trigger.
        engine
            .execute("INSERT INTO t VALUES (1)")
            .expect("insert 1");
        assert_eq!(
            log_count(&engine),
            vec![vec![Some("1".into())]],
            "trigger fired"
        );

        // DISABLE: the trigger no longer fires.
        engine.execute("DISABLE TRIGGER trg ON t").expect("disable");
        engine
            .execute("INSERT INTO t VALUES (2)")
            .expect("insert 2");
        assert_eq!(
            log_count(&engine),
            vec![vec![Some("1".into())]],
            "a disabled trigger does not fire"
        );

        // ENABLE: it fires again.
        engine.execute("ENABLE TRIGGER trg ON t").expect("enable");
        engine
            .execute("INSERT INTO t VALUES (3)")
            .expect("insert 3");
        assert_eq!(
            log_count(&engine),
            vec![vec![Some("2".into())]],
            "a re-enabled trigger fires"
        );

        // DISABLE TRIGGER ALL ON <table> disables every trigger on the table.
        engine
            .execute("DISABLE TRIGGER ALL ON t")
            .expect("disable all");
        engine
            .execute("INSERT INTO t VALUES (4)")
            .expect("insert 4");
        assert_eq!(
            log_count(&engine),
            vec![vec![Some("2".into())]],
            "DISABLE TRIGGER ALL stopped firing"
        );

        // A trigger that is not on the named table (or does not exist) errors.
        assert_eq!(
            sql_error_number(&engine, "DISABLE TRIGGER nope ON t"),
            3701,
            "a missing trigger errors"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn user_scalar_function_works_in_all_query_clause_positions() {
        let path = unique_temp_path("udf-clauses");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        for i in 1..=5 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i})"))
                .expect("insert");
        }
        engine
            .execute("CREATE FUNCTION dbo.dbl (@x INT) RETURNS INT AS BEGIN RETURN @x * 2 END")
            .expect("create fn");

        // ORDER BY a UDF (descending by dbl(id) == descending by id).
        assert_eq!(
            sql_rows(&engine, "SELECT id FROM t ORDER BY dbo.dbl(id) DESC").1,
            vec![
                vec![Some("5".into())],
                vec![Some("4".into())],
                vec![Some("3".into())],
                vec![Some("2".into())],
                vec![Some("1".into())],
            ],
            "UDF in ORDER BY"
        );

        // GROUP BY a UDF key: five distinct dbl(id) groups.
        assert_eq!(
            sql_rows(&engine, "SELECT COUNT(*) FROM t GROUP BY dbo.dbl(id)")
                .1
                .len(),
            5,
            "UDF in GROUP BY key"
        );

        // A UDF over an aggregate in the grouped SELECT list (dbl(count)=2 per id).
        assert_eq!(
            sql_rows(&engine, "SELECT dbo.dbl(COUNT(*)) FROM t GROUP BY id").1,
            vec![vec![Some("2".into())]; 5],
            "UDF over an aggregate in the grouped output"
        );

        // HAVING a UDF over the grouping column: dbl(id) > 6 keeps id 4 and 5.
        assert_eq!(
            sql_rows(
                &engine,
                "SELECT id FROM t GROUP BY id HAVING dbo.dbl(id) > 6 ORDER BY id"
            )
            .1,
            vec![vec![Some("4".into())], vec![Some("5".into())]],
            "UDF in HAVING"
        );

        // A UDF as an aggregate argument: SUM(dbl(id)) = 2*(1+2+3+4+5) = 30.
        assert_eq!(
            sql_rows(&engine, "SELECT SUM(dbo.dbl(id)) FROM t").1,
            vec![vec![Some("30".into())]],
            "UDF as an aggregate argument"
        );

        // A UDF in a join ON predicate: dbl(t.id) matches u.x for id 2 and 5.
        engine
            .execute("CREATE TABLE u (x INT NOT NULL PRIMARY KEY)")
            .expect("create u");
        engine.execute("INSERT INTO u VALUES (4)").expect("ins u");
        engine.execute("INSERT INTO u VALUES (10)").expect("ins u");
        assert_eq!(
            sql_rows(
                &engine,
                "SELECT t.id FROM t JOIN u ON dbo.dbl(t.id) = u.x ORDER BY t.id"
            )
            .1,
            vec![vec![Some("2".into())], vec![Some("5".into())]],
            "UDF in join ON"
        );

        // A UDF in a CHECK constraint: dbl(v) <= 10 rejects v = 6 (dbl = 12) with 547.
        engine
            .execute("CREATE TABLE c (v INT CHECK (dbo.dbl(v) <= 10))")
            .expect("create c");
        engine
            .execute("INSERT INTO c VALUES (5)")
            .expect("dbl(5)=10 passes the check");
        assert_eq!(
            sql_error_number(&engine, "INSERT INTO c VALUES (6)"),
            547,
            "UDF in CHECK: dbl(6)=12 > 10 conflicts (547)"
        );

        drop(engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scalar_function_snapshot_scope_covers_body_reads() {
        // The snapshot-scope determination must recurse into a called UDF's
        // body exactly as lock analysis does: under SNAPSHOT isolation with
        // snapshot isolation NOT allowed, a statement whose ONLY table access is
        // inside a UDF body must still raise 3952 (the body IS a data access).
        // Before the fix these silently succeeded and read live/unlocked — the
        // "neither lock nor snapshot" seam.
        let path = unique_temp_path("udf-snapshot-scope");
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
            "CREATE FUNCTION dbo.cnt () RETURNS INT AS BEGIN RETURN (SELECT COUNT(*) FROM t) END",
        );
        batch(
            &engine,
            &mut ctx,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        );
        // A SELECT whose only table read is inside the UDF body.
        let out = batch(&engine, &mut ctx, "SELECT dbo.cnt() AS n");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3952),
            "a UDF-only SELECT must arm the snapshot scope: {:?}",
            out.error
        );
        // An IF condition whose only table read is inside the UDF body.
        let out = batch(&engine, &mut ctx, "IF dbo.cnt() > 0 SELECT 1 AS n");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3952),
            "a UDF-only IF condition must arm the snapshot scope: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scalar_function_in_view_body_is_lock_analyzed() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("udf-view-lock");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE base (x INT NOT NULL PRIMARY KEY)")
            .expect("base");
        engine
            .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        engine
            .execute(
                "CREATE FUNCTION dbo.secret_count () RETURNS INT AS \
                 BEGIN RETURN (SELECT COUNT(*) FROM secret) END",
            )
            .expect("fn");
        engine
            .execute("CREATE VIEW v AS SELECT x, dbo.secret_count() AS sc FROM base")
            .expect("view");
        let secret = table_object_id(&engine, "secret");
        // A UDF reached THROUGH a view must still have its body's table Shared-
        // locked — else the view-nested UDF reads secret unlocked under 2PL.
        let locks = engine.analyze_locks("SELECT * FROM v", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(secret), LockMode::Shared)),
            "a view-nested UDF's body table must be Shared-locked: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn scalar_function_does_not_shadow_builtin() {
        let path = unique_temp_path("udf-builtin-shadow");
        let engine = new_engine(&path);
        engine
            .execute("CREATE FUNCTION dbo.abs (@x INT) RETURNS INT AS BEGIN RETURN 0 END")
            .expect("fn");
        // A bare call binds to the built-in ABS (5), not the same-named UDF (0).
        let (_, rows) = sql_rows(&engine, "SELECT abs(-5) AS n");
        assert_eq!(
            rows,
            vec![vec![Some("5".into())]],
            "bare abs() must be the built-in"
        );
        // The schema-qualified name still reaches the UDF.
        let (_, rows) = sql_rows(&engine, "SELECT dbo.abs(-5) AS n");
        assert_eq!(
            rows,
            vec![vec![Some("0".into())]],
            "dbo.abs() must be the UDF"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn inline_tvf_body_tables_locked_and_snapshotted() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("tvf-seam");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        engine
            .execute(
                "CREATE FUNCTION dbo.rows_of (@x INT) RETURNS TABLE AS \
                 RETURN (SELECT z FROM secret WHERE z >= @x)",
            )
            .expect("tvf");
        let secret = table_object_id(&engine, "secret");
        // A TVF in FROM must Shared-lock the table its body reads, up front.
        let locks = engine.analyze_locks("SELECT z FROM dbo.rows_of(1)", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(secret), LockMode::Shared)),
            "a TVF's body table must be Shared-locked: {locks:?}"
        );
        // And it must arm the snapshot scope: under SNAPSHOT-not-allowed a TVF
        // whose body reads a table raises 3952 (the body IS a data access).
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        );
        let out = batch(&engine, &mut ctx, "SELECT z FROM dbo.rows_of(1)");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3952),
            "a TVF-reading SELECT must arm the snapshot scope: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn table_variable_access_takes_no_table_locks() {
        use crate::lock::Resource;
        use crate::rel::Isolation;
        let path = unique_temp_path("tablevar-nolocks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE base (id INT NOT NULL PRIMARY KEY)")
            .expect("base");
        // A table variable is session memory: its name never resolves to a base
        // table, so reads and writes of @t take no table/row locks. (A Database
        // intent lock may still appear; only object locks are asserted absent.)
        let has_object_lock = |sql: &str| {
            engine
                .analyze_locks(sql, Isolation::ReadCommitted)
                .iter()
                .any(|(r, _)| matches!(r, Resource::Table(_) | Resource::Row(..)))
        };
        assert!(
            !has_object_lock("SELECT * FROM @t"),
            "SELECT FROM @t must take no object locks"
        );
        assert!(
            !has_object_lock("INSERT INTO @t VALUES (1)"),
            "INSERT @t VALUES must take no object locks"
        );
        // But an INSERT @t whose SOURCE reads a real table still locks the
        // source — the seam: the @t target is free, the source read is not.
        let base = table_object_id(&engine, "base");
        // A join of base with @t locks only base; the @t side adds nothing.
        let join_locks = engine.analyze_locks(
            "SELECT * FROM base AS b JOIN @t AS t ON b.id = t.id",
            Isolation::ReadCommitted,
        );
        assert!(
            !join_locks
                .iter()
                .any(|(r, _)| matches!(r, Resource::Table(id) if *id != base)),
            "the @t side of a join must add no table lock beyond base's: {join_locks:?}"
        );
        let locks = engine.analyze_locks(
            "INSERT INTO @t SELECT id FROM base",
            Isolation::ReadCommitted,
        );
        assert!(
            locks
                .iter()
                .any(|(r, _)| matches!(r, Resource::Table(id) if *id == base)),
            "INSERT @t SELECT FROM base must lock base: {locks:?}"
        );
        assert!(
            !locks
                .iter()
                .any(|(r, _)| matches!(r, Resource::Table(id) if *id != base)),
            "no phantom lock for @t itself: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn table_variable_read_does_not_arm_snapshot() {
        let path = unique_temp_path("tablevar-nosnapshot");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE base (id INT NOT NULL PRIMARY KEY)")
            .expect("base");
        engine.execute("INSERT INTO base VALUES (7)").expect("seed");
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        );
        // Under SNAPSHOT-not-allowed, a @t-only batch is NOT a data access: it
        // must run to completion, not raise 3952.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (1), (2); SELECT id FROM @t",
        );
        assert!(
            out.error.is_none(),
            "a table-variable-only batch must not raise 3952: {:?}",
            out.error
        );
        assert_eq!(ids(&out), vec![1, 2]);
        // But INSERT @t whose SOURCE reads a real table IS a data access and
        // must raise 3952 under SNAPSHOT-not-allowed (the source read needs the
        // snapshot the database forbids).
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); INSERT INTO @t SELECT id FROM base",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3952),
            "INSERT @t SELECT FROM base must arm the snapshot scope: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn function_body_cannot_read_caller_table_variable() {
        let path = unique_temp_path("tablevar-fn-isolation");
        let engine = new_engine(&path);
        // A scalar UDF and an inline TVF whose bodies reference @t are created
        // without a bind-time check, but at call time each runs with its OWN
        // (empty) table-variable scope — it must NOT see the caller's @t. The
        // body's `FROM @t` therefore errors 1087, not silently reading caller
        // rows. This is the scope seam: the read view armed by the calling
        // statement must be shadowed, not inherited, across the body boundary.
        engine
            .execute(
                "CREATE FUNCTION dbo.cnt () RETURNS INT AS BEGIN RETURN (SELECT COUNT(*) FROM @t) END",
            )
            .expect("create scalar udf");
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (1), (2), (3); SELECT dbo.cnt() AS n",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(1087),
            "a scalar UDF body must not read the caller's table variable: {:?}",
            out.error
        );

        engine
            .execute("CREATE FUNCTION dbo.readt () RETURNS TABLE AS RETURN (SELECT id FROM @t)")
            .expect("create inline tvf");
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (99); SELECT id FROM dbo.readt()",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(1087),
            "an inline TVF body must not read the caller's table variable: {:?}",
            out.error
        );

        // A VIEW body is the same stored-object scope: it must not read the
        // caller's @t either. (SQL Server rejects such a view at CREATE; TruthDB
        // defers name resolution, so the isolation must hold at query time.)
        engine
            .execute("CREATE VIEW dbo.vt AS SELECT id FROM @t")
            .expect("create view over @t");
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @t TABLE (id INT NOT NULL PRIMARY KEY); \
             INSERT INTO @t VALUES (1), (2); SELECT id FROM dbo.vt",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(1087),
            "a view body must not read the caller's table variable: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recursive_function_lock_analysis_terminates() {
        use crate::rel::Isolation;
        let path = unique_temp_path("udf-recursion-bomb");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        // A self-referencing TVF whose body references itself TWICE (fan-out 2):
        // without the visited-set memoization in collect_read_lock_ids this
        // recurses ~2^32 times and hangs analysis (and, under the scheduler
        // mutex, the whole server). Run in a thread so a regression FAILS
        // cleanly on the timeout rather than hanging the test binary.
        engine
            .execute(
                "CREATE FUNCTION dbo.bomb (@x INT) RETURNS TABLE AS \
                 RETURN (SELECT a.id FROM dbo.bomb(@x) AS a JOIN dbo.bomb(@x) AS b ON a.id = b.id)",
            )
            .expect("bomb");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = engine.analyze_locks("SELECT id FROM dbo.bomb(1)", Isolation::ReadCommitted);
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(10)).is_ok(),
            "lock analysis of a recursive function must terminate (memoization)"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn after_insert_trigger_fires_reading_inserted() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("trg-insert");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("t");
        engine
            .execute("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("audit");
        engine
            .execute("CREATE TRIGGER trg_t ON t AFTER INSERT AS INSERT INTO audit SELECT id, v FROM inserted")
            .expect("trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1, 100), (2, 200)");
        assert!(out.error.is_none(), "insert+trigger: {:?}", out.error);
        let (_c, rows) = sql_rows(&engine, "SELECT id, v FROM audit ORDER BY id");
        assert_eq!(
            rows,
            vec![
                vec![Some("1".to_string()), Some("100".to_string())],
                vec![Some("2".to_string()), Some("200".to_string())],
            ],
            "the AFTER INSERT trigger copied `inserted` into audit"
        );
        // The lock seam: analyze_locks over the INSERT must hold `audit`'s
        // Exclusive lock up front (the trigger body writes it) — else the body
        // writes unlocked under 2PL.
        let audit = table_object_id(&engine, "audit");
        let locks = engine.analyze_locks("INSERT INTO t VALUES (9, 9)", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(audit), LockMode::Exclusive)),
            "the trigger body's audit write must be X-locked up front: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn after_update_and_delete_triggers_read_deleted_and_inserted() {
        let path = unique_temp_path("trg-upd-del");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("t");
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            .expect("seed");
        engine
            .execute("CREATE TABLE log (k INT NOT NULL PRIMARY KEY, oldv INT, newv INT)")
            .expect("log");
        // UPDATE trigger sees both `deleted` (old) and `inserted` (new).
        engine
            .execute(
                "CREATE TRIGGER trg_u ON t AFTER UPDATE AS INSERT INTO log \
                 SELECT i.id, d.v, i.v FROM inserted AS i JOIN deleted AS d ON i.id = d.id",
            )
            .expect("update trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "UPDATE t SET v = 99 WHERE id = 1");
        assert!(out.error.is_none(), "update+trigger: {:?}", out.error);
        let (_c, rows) = sql_rows(&engine, "SELECT k, oldv, newv FROM log");
        assert_eq!(
            rows,
            vec![vec![
                Some("1".to_string()),
                Some("10".to_string()),
                Some("99".to_string())
            ]],
            "UPDATE trigger joined deleted(old) and inserted(new)"
        );
        // DELETE trigger sees `deleted`.
        engine
            .execute("CREATE TABLE gone (id INT NOT NULL PRIMARY KEY)")
            .expect("gone");
        engine
            .execute(
                "CREATE TRIGGER trg_d ON t AFTER DELETE AS INSERT INTO gone SELECT id FROM deleted",
            )
            .expect("delete trigger");
        let out = batch(&engine, &mut ctx, "DELETE FROM t WHERE id = 2");
        assert!(out.error.is_none(), "delete+trigger: {:?}", out.error);
        let (_c, rows) = sql_rows(&engine, "SELECT id FROM gone");
        assert_eq!(
            rows,
            vec![vec![Some("2".to_string())]],
            "DELETE trigger read deleted"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_rollback_raises_3609_and_undoes_the_dml() {
        let path = unique_temp_path("trg-3609");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        // A trigger that rolls back ends the transaction: 3609, and the firing
        // INSERT is undone (atomic under the implicit transaction).
        engine
            .execute("CREATE TRIGGER trg_rb ON t AFTER INSERT AS ROLLBACK")
            .expect("rollback trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3609),
            "a trigger ROLLBACK raises 3609: {:?}",
            out.error
        );
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM t");
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "the INSERT must be rolled back with the trigger"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recursive_trigger_does_not_refire_itself() {
        let path = unique_temp_path("trg-recursive");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        // A trigger whose body inserts into its OWN table must not re-fire itself
        // (recursive triggers OFF by default) — otherwise it would loop.
        engine
            .execute(
                "CREATE TRIGGER trg_self ON t AFTER INSERT AS INSERT INTO t SELECT id + 100 FROM inserted WHERE id < 100",
            )
            .expect("self trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        assert!(out.error.is_none(), "recursive-off insert: {:?}", out.error);
        // The original row plus one from the (non-re-firing) trigger body.
        let (_c, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
        assert_eq!(
            rows,
            vec![vec![Some("1".to_string())], vec![Some("101".to_string())]],
            "the trigger fired once, did not recurse on its own insert"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_cycle_lock_analysis_terminates() {
        use crate::rel::Isolation;
        let path = unique_temp_path("trg-cycle");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE a (id INT NOT NULL PRIMARY KEY)")
            .expect("a");
        engine
            .execute("CREATE TABLE b (id INT NOT NULL PRIMARY KEY)")
            .expect("b");
        // A trigger cycle: a's trigger writes b, b's trigger writes a. Lock
        // analysis recurses trigger bodies — without the visited-set it would
        // recurse forever and hang under the scheduler mutex. Run in a thread so
        // a regression fails on the timeout rather than hanging the test binary.
        engine
            .execute(
                "CREATE TRIGGER trg_a ON a AFTER INSERT AS INSERT INTO b SELECT id FROM inserted",
            )
            .expect("trg_a");
        engine
            .execute(
                "CREATE TRIGGER trg_b ON b AFTER INSERT AS INSERT INTO a SELECT id FROM inserted",
            )
            .expect("trg_b");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = engine.analyze_locks("INSERT INTO a VALUES (1)", Isolation::ReadCommitted);
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(10)).is_ok(),
            "trigger-cycle lock analysis must terminate (visited-set)"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_raiserror_aborts_and_undoes_the_dml() {
        let path = unique_temp_path("trg-validate");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        // A validation trigger: RAISERROR (severity 16) must abort the firing
        // statement and roll it back — not be silently swallowed.
        engine
            .execute(
                "CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('rejected by trigger', 16, 1)",
            )
            .expect("trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        assert!(
            out.error.is_some(),
            "a trigger RAISERROR must fail the INSERT, not be swallowed"
        );
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM t");
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "the rejected INSERT must be rolled back"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_body_exec_and_fk_reads_are_locked_up_front() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("trg-exec-fk-locks");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE w (id INT NOT NULL PRIMARY KEY)")
            .expect("w");
        engine
            .execute("CREATE PROCEDURE do_write AS INSERT INTO w VALUES (1)")
            .expect("proc");
        engine
            .execute("CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)")
            .expect("parent");
        engine
            .execute("CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pid INT NOT NULL REFERENCES parent(id))")
            .expect("child");
        // The body EXECs a proc that writes w, and inserts into child (FK to
        // parent). analyze_locks over the firing INSERT must include w's X lock
        // (the EXEC'd proc's write) AND parent's S lock (the FK integrity read) —
        // the trigger-body analysis now reuses the real lock analysis.
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS BEGIN EXEC do_write; INSERT INTO child VALUES (1, 1) END")
            .expect("trigger");
        let w = table_object_id(&engine, "w");
        let parent = table_object_id(&engine, "parent");
        let locks = engine.analyze_locks("INSERT INTO t VALUES (1)", Isolation::ReadCommitted);
        // The single-row proc INSERT locks w at Table-IX + Row-X; assert w is
        // locked at all (a write lock, IX or X), proving the EXEC was analyzed.
        assert!(
            locks
                .iter()
                .any(|(r, m)| matches!(r, Resource::Table(id) if *id == w)
                    && matches!(m, LockMode::Exclusive | LockMode::IntentExclusive)),
            "the EXEC'd proc's write to w must be write-locked up front: {locks:?}"
        );
        assert!(
            locks.contains(&(Resource::Table(parent), LockMode::Shared)),
            "the trigger body's FK read of parent must be S-locked up front: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn procedure_called_from_trigger_cannot_read_inserted() {
        let path = unique_temp_path("trg-proc-shadow");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE sink (id INT NOT NULL PRIMARY KEY)")
            .expect("sink");
        engine
            .execute("CREATE PROCEDURE logproc AS INSERT INTO sink SELECT id FROM inserted")
            .expect("proc");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS EXEC logproc")
            .expect("trigger");
        // inserted is visible only in the trigger's OWN statements; a proc it
        // EXECs cannot see it — the reference errors (and aborts the INSERT).
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        assert!(
            out.error.is_some(),
            "a proc called from a trigger must not resolve `inserted`"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_unbalanced_begin_transaction_raises_3609() {
        let path = unique_temp_path("trg-leak-txn");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        // A trigger that opens a transaction without closing it changes
        // @@TRANCOUNT — 3609, and the leaked transaction is rolled back.
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS BEGIN TRANSACTION")
            .expect("trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3609),
            "an unbalanced BEGIN in a trigger raises 3609: {:?}",
            out.error
        );
        assert!(
            !ctx.has_open_transaction(),
            "the leaked transaction must be rolled back"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_name_is_not_a_droppable_or_queryable_table() {
        let path = unique_temp_path("trg-not-a-table");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO t SELECT 0 WHERE 1 = 0")
            .expect("trigger");
        let mut ctx = TxnContext::default();
        // DROP TABLE on the trigger name must NOT silently destroy it (3701).
        let out = batch(&engine, &mut ctx, "DROP TABLE trg");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3701),
            "DROP TABLE must not destroy a trigger: {:?}",
            out.error
        );
        // SELECT FROM the trigger name must error, not heap-scan its root page.
        let out = batch(&engine, &mut ctx, "SELECT * FROM trg");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(208),
            "SELECT FROM a trigger name must be invalid object: {:?}",
            out.error
        );
        // sys.tables must not list the trigger.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.tables WHERE name = 'trg'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "sys.tables excludes triggers"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn indirect_trigger_recursion_is_allowed() {
        let path = unique_temp_path("trg-indirect");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE a (id INT NOT NULL PRIMARY KEY)")
            .expect("a");
        engine
            .execute("CREATE TABLE b (id INT NOT NULL PRIMARY KEY)")
            .expect("b");
        // Recursive-OFF suppresses only DIRECT self-recursion; indirect
        // recursion (a's trigger writes b, b's trigger writes a) is allowed and
        // bounded by the nesting cap. The IF EXISTS guard stops the FIRING (not
        // just the row insert) so the chain terminates — a bare WHERE would still
        // do a 0-row INSERT that fires the next trigger, looping to the cap.
        engine
            .execute("CREATE TRIGGER ta ON a AFTER INSERT AS INSERT INTO b SELECT id FROM inserted")
            .expect("ta");
        engine
            .execute("CREATE TRIGGER tb ON b AFTER INSERT AS IF EXISTS (SELECT 1 FROM inserted WHERE id < 10) INSERT INTO a SELECT id + 10 FROM inserted WHERE id < 10")
            .expect("tb");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "INSERT INTO a VALUES (1)");
        assert!(out.error.is_none(), "indirect recursion: {:?}", out.error);
        // a = {1 (seed), 11 (via a->b->a)}: the indirect path fired once.
        let (_c, rows) = sql_rows(&engine, "SELECT id FROM a ORDER BY id");
        assert_eq!(
            rows,
            vec![vec![Some("1".to_string())], vec![Some("11".to_string())]],
            "indirect a->b->a recursion must fire (id 11 came through b)"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_body_read_is_locked_under_inline_isolation_escalation() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("trg-escalation");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE r (id INT NOT NULL PRIMARY KEY)")
            .expect("r");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS SELECT id FROM r")
            .expect("trigger");
        let r = table_object_id(&engine, "r");
        // A batch that escalates the isolation in-line (SET SERIALIZABLE) under a
        // versioned session (Snapshot) must analyze the trigger body's read of r
        // lock-based — Table S — not drop it as a versioned read. The trigger
        // body analysis now forwards the escalation-corrected isolation.
        let locks = engine.analyze_locks(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; INSERT INTO t VALUES (1)",
            Isolation::Snapshot,
        );
        assert!(
            locks.contains(&(Resource::Table(r), LockMode::Shared)),
            "the trigger body's read of r must be Table-S locked under escalation: {locks:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_error_rolls_back_the_dml_inside_an_explicit_transaction() {
        let path = unique_temp_path("trg-explicit-txn");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
            .expect("trigger");
        // Inside an explicit transaction, a trigger error dooms it — the COMMIT
        // of the uncommittable transaction fails, so the firing row can never
        // durably commit (it stays staged in the doomed, still-open transaction).
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (1); COMMIT",
        );
        assert!(
            out.error.is_some(),
            "the trigger error (and the failed COMMIT of the doomed txn) must surface"
        );
        // Roll the doomed transaction back; nothing was ever durable.
        batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM t");
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "the firing row must never durably commit after a trigger error"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_error_dooms_explicit_transaction_caught_by_try_catch() {
        let path = unique_temp_path("trg-doomed-catch");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY)")
            .expect("audit");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
            .expect("trigger");
        // A trigger error inside an explicit transaction DOOMS it (does not tear
        // it down): the CATCH runs under an uncommittable transaction, so its
        // write is rejected (3930), never silently autocommitted. After the
        // ROLLBACK nothing is durable.
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH INSERT INTO audit VALUES (99); END CATCH; ROLLBACK",
        );
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM audit");
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "the CATCH write must be rejected under the doomed transaction, not autocommitted"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_that_rolls_back_and_errors_does_not_wedge_the_session() {
        let path = unique_temp_path("trg-rollback-error");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE other (id INT NOT NULL PRIMARY KEY)")
            .expect("other");
        // The idiomatic abort-in-trigger pattern: ROLLBACK then RAISERROR. The
        // trigger ends the transaction AND errors — it must abort cleanly (3609
        // path), not doom a torn-down transaction and leave the session wedged.
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS BEGIN ROLLBACK; RAISERROR('reject', 16, 1) END")
            .expect("trigger");
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (1)",
        );
        assert!(out.error.is_some(), "the trigger's RAISERROR must surface");
        // The session is not wedged: a subsequent autocommit write succeeds
        // (no leftover doomed state rejecting it with 3930).
        let out = batch(&engine, &mut ctx, "INSERT INTO other VALUES (1)");
        assert!(
            out.error.is_none(),
            "the session must not be wedged after ROLLBACK; RAISERROR: {:?}",
            out.error
        );
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM other");
        assert_eq!(
            rows,
            vec![vec![Some("1".to_string())]],
            "the later write committed"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn uncaught_trigger_error_dooms_so_later_writes_are_rejected() {
        let path = unique_temp_path("trg-uncaught-doom");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE other (id INT NOT NULL PRIMARY KEY)")
            .expect("other");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
            .expect("trigger");
        // An uncaught trigger error in an explicit transaction dooms it, so a
        // later write in the same transaction is rejected (3930) — it cannot
        // durably commit new work over the uncommittable transaction.
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; INSERT INTO t VALUES (1); INSERT INTO other VALUES (2)",
        );
        batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM other");
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "the doomed transaction must reject the later write, not commit it"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn doomed_transaction_catch_reaches_its_rollback_after_a_benign_error() {
        let path = unique_temp_path("trg-catch-benign");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS RAISERROR('reject', 16, 1)")
            .expect("trigger");
        // A trigger error dooms the explicit transaction and transfers to the
        // CATCH. A benign statement-terminating error inside the CATCH (division
        // by zero) must NOT abort the batch before the CATCH reaches its
        // ROLLBACK — otherwise the uncommittable transaction is left open holding
        // its locks (the wedge class). After the batch the transaction is closed.
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "BEGIN TRANSACTION; BEGIN TRY INSERT INTO t VALUES (1); END TRY \
             BEGIN CATCH SELECT 1 / 0 AS x; IF XACT_STATE() <> 0 ROLLBACK; END CATCH",
        );
        assert!(
            !ctx.has_open_transaction(),
            "the CATCH must reach ROLLBACK despite the divide-by-zero; no wedged transaction"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trigger_is_not_an_alter_or_dml_target() {
        let path = unique_temp_path("trg-alter-target");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO t SELECT 0 WHERE 1 = 0")
            .expect("trigger");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "ALTER TABLE trg ADD c INT");
        assert!(out.error.is_some(), "ALTER TABLE on a trigger must error");
        let out = batch(&engine, &mut ctx, "INSERT INTO trg VALUES (1)");
        assert!(out.error.is_some(), "INSERT into a trigger must error");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn drop_table_cascade_drops_its_triggers() {
        let path = unique_temp_path("trg-cascade-drop");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("t");
        engine
            .execute("CREATE TABLE audit (id INT NOT NULL PRIMARY KEY)")
            .expect("audit");
        engine
            .execute(
                "CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO audit SELECT id FROM inserted",
            )
            .expect("trigger");
        engine.execute("DROP TABLE t").expect("drop t");
        // The trigger is gone (not orphaned): its name is free to reuse.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.objects WHERE name = 'trg'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "DROP TABLE must cascade-drop its triggers"
        );
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "CREATE TABLE trg (x INT)");
        assert!(
            out.error.is_none(),
            "the orphaned trigger name must be reusable: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn create_login_persists_survives_restart_and_stays_out_of_the_object_namespace() {
        let path = unique_temp_path("login-ddl");
        let engine = new_engine(&path);
        engine
            .execute("CREATE LOGIN alice WITH PASSWORD = 'S3cret!'")
            .expect("create login");
        // sys.server_principals shows it as a SQL login.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT name, type, is_disabled FROM sys.server_principals WHERE name = 'alice'",
        );
        assert_eq!(
            rows,
            vec![vec![
                Some("alice".to_string()),
                Some("S".to_string()),
                Some("0".to_string())
            ]],
            "the login appears in sys.server_principals"
        );
        // It is NOT a schema object: not in sys.tables, not queryable as a table.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.tables WHERE name = 'alice'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "a login is not a table"
        );
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "SELECT * FROM alice");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(208),
            "a login name is not a queryable object: {:?}",
            out.error
        );
        // Survives a restart (persisted in the catalog b-tree).
        drop(engine);
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT name FROM sys.server_principals WHERE name = 'alice'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("alice".to_string())]],
            "the login survives restart"
        );

        // ALTER LOGIN ... DISABLE.
        engine
            .execute("ALTER LOGIN alice DISABLE")
            .expect("disable");
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT is_disabled FROM sys.sql_logins WHERE name = 'alice'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("1".to_string())]],
            "ALTER DISABLE sets is_disabled"
        );

        // DROP LOGIN.
        engine.execute("DROP LOGIN alice").expect("drop login");
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.server_principals WHERE name = 'alice'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "DROP LOGIN removes it"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn migrate_logins_is_idempotent_ensures_sa_last_and_survives_config_case_dups() {
        use std::collections::BTreeMap;
        let path = unique_temp_path("login-migrate");
        let engine = new_engine(&path);

        // Case-variant duplicate keys and a lowercase app user; NO sa configured.
        // The dup must NOT error the migration — the second is collapsed onto the
        // first-seen login (names are case-insensitive) — and sa is created LAST,
        // disabled, because no password was configured.
        let mut users = BTreeMap::new();
        users.insert("Admin".to_string(), "p1".to_string());
        users.insert("admin".to_string(), "p2".to_string());
        users.insert("app".to_string(), "app-pw".to_string());
        let created = engine.migrate_logins(&users).expect("first migration");
        assert!(
            created.iter().any(|c| c.starts_with("sa (disabled")),
            "sa is ensured disabled when unconfigured: {created:?}"
        );

        // Exactly one of the case-variant admins exists (the first-sorted, Admin),
        // plus app and sa — the dup did not create a second principal. Filter to
        // SQL logins (type 'S') so the synthesized sysadmin server role does not
        // appear.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT name, is_disabled FROM sys.server_principals WHERE type = 'S' ORDER BY name",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("Admin".to_string()), Some("0".to_string())],
                vec![Some("app".to_string()), Some("0".to_string())],
                vec![Some("sa".to_string()), Some("1".to_string())],
            ],
            "case-dup collapsed to one login; sa present and disabled: {rows:?}"
        );

        // Idempotent: a second run is a no-op (sa exists → the whole thing skips),
        // and it does NOT resurrect a login the admin dropped.
        engine.execute("DROP LOGIN app").expect("drop app");
        let again = engine.migrate_logins(&users).expect("second migration");
        assert!(again.is_empty(), "re-run is a no-op: {again:?}");
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.server_principals WHERE name = 'app'",
        );
        assert_eq!(
            rows,
            vec![vec![Some("0".to_string())]],
            "a dropped login is not resurrected by re-migration"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn migrate_logins_uses_the_configured_sa_password_and_enables_it() {
        use std::collections::BTreeMap;
        let path = unique_temp_path("login-migrate-sa");
        let engine = new_engine(&path);
        let mut users = BTreeMap::new();
        users.insert("sa".to_string(), "secret".to_string());
        engine.migrate_logins(&users).expect("migration");
        let rec = engine.lookup_login("sa").expect("sa exists");
        assert!(!rec.is_disabled, "configured sa is enabled");
        assert_eq!(
            crate::auth::verify_password(&rec.password_blob, "secret"),
            crate::auth::VerifyOutcome::Ok,
            "sa authenticates with its configured password"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn database_users_and_roles_persist_and_stay_out_of_the_object_namespace() {
        let path = unique_temp_path("principals");
        let engine = new_engine(&path);
        engine
            .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
            .expect("login");
        engine
            .execute("CREATE USER app FOR LOGIN sa")
            .expect("user");
        engine.execute("CREATE ROLE reporting").expect("role");

        // sys.database_principals shows the fixed dbo user and db_owner role plus
        // the created user ('S'=SQL_USER) and role ('R'=DATABASE_ROLE).
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT name, type FROM sys.database_principals \
             WHERE name IN ('dbo','db_owner','app','reporting') ORDER BY name",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("app".into()), Some("S".into())],
                vec![Some("db_owner".into()), Some("R".into())],
                vec![Some("dbo".into()), Some("S".into())],
                vec![Some("reporting".into()), Some("R".into())],
            ]
        );

        // They are not schema objects.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.tables WHERE name IN ('app','reporting')",
        );
        assert_eq!(rows, vec![vec![Some("0".into())]], "not tables");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "SELECT * FROM app");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(208),
            "a user name is not a queryable object"
        );

        // Survives restart.
        drop(engine);
        let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT name FROM sys.database_principals WHERE name = 'reporting'",
        );
        assert_eq!(rows, vec![vec![Some("reporting".into())]], "role survives");

        // DROP.
        engine.execute("DROP ROLE reporting").expect("drop role");
        engine.execute("DROP USER app").expect("drop user");
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.database_principals \
             WHERE name IN ('app','reporting')",
        );
        assert_eq!(rows, vec![vec![Some("0".into())]], "dropped");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn role_membership_is_transitive_and_cycle_checked() {
        let path = unique_temp_path("membership");
        let engine = new_engine(&path);
        engine.execute("CREATE ROLE r1").unwrap();
        engine.execute("CREATE ROLE r2").unwrap();
        engine.execute("CREATE USER u").unwrap();
        // u ∈ r1, r1 ∈ r2 (nesting).
        engine.execute("ALTER ROLE r1 ADD MEMBER u").unwrap();
        engine.execute("ALTER ROLE r2 ADD MEMBER r1").unwrap();

        // sys.database_role_members: the two edges plus the synthesized dbo→db_owner.
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT COUNT(*) AS n FROM sys.database_role_members",
        );
        assert_eq!(rows, vec![vec![Some("3".into())]]);

        // A cycle (r2 → r1 → r2) is refused, as is self-membership.
        let mut ctx = TxnContext::default();
        assert!(
            batch(&engine, &mut ctx, "ALTER ROLE r1 ADD MEMBER r2")
                .error
                .is_some(),
            "a membership cycle must be rejected"
        );
        assert!(
            batch(&engine, &mut ctx, "ALTER ROLE r1 ADD MEMBER r1")
                .error
                .is_some(),
            "self-membership must be rejected"
        );

        // A role with members cannot be dropped.
        assert!(
            batch(&engine, &mut ctx, "DROP ROLE r1").error.is_some(),
            "a role with members cannot be dropped"
        );
        // Remove the members, then it drops.
        engine.execute("ALTER ROLE r1 DROP MEMBER u").unwrap();
        engine.execute("ALTER ROLE r2 DROP MEMBER r1").unwrap();
        engine.execute("DROP ROLE r1").expect("now droppable");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn session_intrinsics_resolve_database_user_and_role_membership() {
        let path = unique_temp_path("intrinsics");
        let engine = new_engine(&path);
        engine
            .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
            .unwrap();
        engine.execute("CREATE ROLE reporting").unwrap();
        engine.execute("CREATE USER analyst").unwrap();
        engine
            .execute("ALTER ROLE reporting ADD MEMBER analyst")
            .unwrap();

        // Helper: read the first row of a batch's first rowset as strings.
        fn row_cells(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> Vec<Option<String>> {
            let out = batch(engine, ctx, sql);
            assert!(out.error.is_none(), "batch error: {:?}", out.error);
            for result in &out.results {
                if let StatementResult::Rows(rowset) = result {
                    return rowset.rows[0]
                        .iter()
                        .map(|d| match d {
                            Datum::Null => None,
                            Datum::Int(v) => Some(v.to_string()),
                            Datum::BigInt(v) => Some(v.to_string()),
                            Datum::NVarChar(s) | Datum::VarChar(s) => Some(s.clone()),
                            other => Some(format!("{other:?}")),
                        })
                        .collect();
                }
            }
            panic!("no rowset: {:?}", out.results);
        }

        // A session as sa maps to the dbo user (sysadmin), a member of db_owner.
        let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
        let (user, user_sid) = engine.resolve_session_user("sa", sa_sid);
        assert_eq!(user, "dbo");
        let mut ctx = TxnContext::default();
        ctx.set_session_identity("truthdb".into(), "sa".into(), 1, user, sa_sid, user_sid);
        let cells = row_cells(
            &engine,
            &mut ctx,
            "SELECT SUSER_SNAME() a, USER_NAME() b, IS_SRVROLEMEMBER('sysadmin') c, \
             IS_ROLEMEMBER('db_owner') d, IS_ROLEMEMBER('reporting') e, \
             IS_ROLEMEMBER('sysadmin') f, IS_SRVROLEMEMBER('db_owner') g",
        );
        assert_eq!(
            cells,
            vec![
                Some("sa".into()),
                Some("dbo".into()),
                Some("1".into()), // IS_SRVROLEMEMBER(sysadmin)
                Some("1".into()), // IS_ROLEMEMBER(db_owner)
                Some("0".into()), // IS_ROLEMEMBER(reporting)
                // The role families do not cross-answer: sysadmin is a SERVER
                // role (0 as a database role), db_owner a DATABASE role (0 as a
                // server role).
                Some("0".into()), // IS_ROLEMEMBER(sysadmin)
                Some("0".into()), // IS_SRVROLEMEMBER(db_owner)
            ],
            "sa → dbo; server/database role namespaces are distinct"
        );

        // A session as the analyst user is a member of reporting only.
        let analyst_sid: u32 = {
            let (_c, rows) = sql_rows(
                &engine,
                "SELECT principal_id FROM sys.database_principals WHERE name = 'analyst'",
            );
            rows[0][0].as_ref().unwrap().parse().unwrap()
        };
        let mut ctx = TxnContext::default();
        ctx.set_session_identity(
            "truthdb".into(),
            "analyst".into(),
            2,
            "analyst".into(),
            0,
            analyst_sid,
        );
        let cells = row_cells(
            &engine,
            &mut ctx,
            "SELECT USER_NAME() a, IS_ROLEMEMBER('reporting') b, IS_ROLEMEMBER('db_owner') c, \
             IS_SRVROLEMEMBER('sysadmin') d",
        );
        assert_eq!(
            cells,
            vec![
                Some("analyst".into()),
                Some("1".into()),
                Some("0".into()),
                Some("0".into()),
            ],
            "analyst ∈ reporting only"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sa_resolves_to_dbo_and_sysadmin_with_no_prior_security_ddl() {
        // Regression: the membership cache must populate on the very first query
        // even while security_version is still 0 (a fresh boot where only the sa
        // login was created), and again after a restart resets the counter to 0.
        let path = unique_temp_path("fresh-sa");
        let engine = new_engine(&path);
        engine
            .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
            .unwrap();
        // No CREATE USER/ROLE/ALTER ROLE has run: security_version is still 0.
        let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
        assert_eq!(
            engine.resolve_session_user("sa", sa_sid),
            ("dbo".to_string(), crate::storage::DBO_ID),
            "sa is sysadmin (→ dbo) with no prior security DDL"
        );

        // A durable role membership survives a restart (which resets the in-memory
        // counter and cache) and is visible immediately, before any new DDL.
        engine.execute("CREATE ROLE r").unwrap();
        engine.execute("CREATE USER u").unwrap();
        engine.execute("ALTER ROLE r ADD MEMBER u").unwrap();
        let u_sid: u32 = {
            let (_c, rows) = sql_rows(
                &engine,
                "SELECT principal_id FROM sys.database_principals WHERE name = 'u'",
            );
            rows[0][0].as_ref().unwrap().parse().unwrap()
        };
        drop(engine);
        let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
        let r_sid: u32 = {
            let (_c, rows) = sql_rows(
                &engine,
                "SELECT principal_id FROM sys.database_principals WHERE name = 'r'",
            );
            rows[0][0].as_ref().unwrap().parse().unwrap()
        };
        assert!(
            engine
                .storage_effective_roles_for_test(u_sid)
                .contains(&r_sid),
            "durable membership is visible after restart with no new DDL"
        );
        // And sa still resolves to sysadmin/dbo post-restart.
        let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
        assert_eq!(
            engine.resolve_session_user("sa", sa_sid).0,
            "dbo",
            "sa is still sysadmin after restart"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn creating_the_sa_login_against_a_warm_cache_is_seen() {
        // Regression: login DDL bumps the security version, so creating sa after
        // the membership cache is already warm still makes it sysadmin.
        let path = unique_temp_path("warm-sa");
        let engine = new_engine(&path);
        engine.execute("CREATE ROLE warm").unwrap(); // warms the cache at version >= 1
        // A resolve while sa is absent warms the cache without an sa edge.
        assert_eq!(
            engine.resolve_session_user("sa", 999),
            ("sa".to_string(), 0)
        );
        engine
            .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
            .unwrap();
        let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
        assert_eq!(
            engine.resolve_session_user("sa", sa_sid),
            ("dbo".to_string(), crate::storage::DBO_ID),
            "the freshly-created sa is sysadmin despite the warm cache"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_membership_change_is_visible_to_the_next_batch() {
        // The security-version counter invalidates the membership cache: a role
        // added between batches is reflected in the next batch's IS_ROLEMEMBER.
        let path = unique_temp_path("membership-invalidation");
        let engine = new_engine(&path);
        engine.execute("CREATE ROLE auditors").unwrap();
        engine.execute("CREATE USER clerk").unwrap();
        let clerk_sid: u32 = {
            let (_c, rows) = sql_rows(
                &engine,
                "SELECT principal_id FROM sys.database_principals WHERE name = 'clerk'",
            );
            rows[0][0].as_ref().unwrap().parse().unwrap()
        };
        let mut ctx = TxnContext::default();
        ctx.set_session_identity(
            "truthdb".into(),
            "clerk".into(),
            3,
            "clerk".into(),
            0,
            clerk_sid,
        );

        let member = |engine: &Engine, ctx: &mut TxnContext| -> i64 {
            let out = batch(engine, ctx, "SELECT IS_ROLEMEMBER('auditors')");
            match &out.results[0] {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => v as i64,
                    Datum::BigInt(v) => v,
                    ref other => panic!("expected an integer, got {other:?}"),
                },
                other => panic!("expected rows, got {other:?}"),
            }
        };

        assert_eq!(member(&engine, &mut ctx), 0, "not a member yet");
        engine
            .execute("ALTER ROLE auditors ADD MEMBER clerk")
            .unwrap();
        assert_eq!(
            member(&engine, &mut ctx),
            1,
            "the new membership is seen after the security-version bump"
        );
        let _ = std::fs::remove_file(path);
    }

    /// Opens a restricted (non-dbo, non-sysadmin) session context for `login`,
    /// so object-permission checks actually bite (unlike the login_sid-0 bypass).
    fn restricted_ctx(engine: &Engine, login: &str) -> TxnContext {
        let login_sid = engine.lookup_login(login).unwrap().principal_id;
        let (user, user_sid) = engine.resolve_session_user(login, login_sid);
        let mut ctx = TxnContext::default();
        ctx.set_session_identity("truthdb".into(), login.into(), 9, user, login_sid, user_sid);
        ctx
    }

    fn err_num(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> Option<i32> {
        batch(engine, ctx, sql).error.as_ref().map(|e| e.number)
    }

    #[test]
    fn object_permissions_enforce_grant_deny_revoke_and_public() {
        let path = unique_temp_path("perms");
        let engine = new_engine(&path);
        // Admin (login_sid 0 → bypass) sets up objects, a restricted login/user,
        // and a role.
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        engine
            .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
            .unwrap();
        engine
            .execute("CREATE USER appuser FOR LOGIN applogin")
            .unwrap();
        engine.execute("CREATE ROLE readers").unwrap();
        engine
            .execute("ALTER ROLE readers ADD MEMBER appuser")
            .unwrap();

        let mut r = restricted_ctx(&engine, "applogin");

        // Ungranted: SELECT denied 229.
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
        // GRANT SELECT via the role → allowed.
        engine.execute("GRANT SELECT ON t TO readers").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
        // A direct DENY beats the role's GRANT (both entries present).
        engine.execute("DENY SELECT ON t TO appuser").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
        // REVOKE the deny → the role GRANT is effective again.
        engine.execute("REVOKE SELECT ON t FROM appuser").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
        // REVOKE from the role → no grant → denied.
        engine.execute("REVOKE SELECT ON t FROM readers").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));

        // GRANT to public covers every user.
        engine.execute("GRANT SELECT ON t TO public").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);

        // INSERT/UPDATE/DELETE need their own grants.
        assert_eq!(
            err_num(&engine, &mut r, "INSERT INTO t VALUES (3)"),
            Some(229)
        );
        engine.execute("GRANT INSERT ON t TO appuser").unwrap();
        assert_eq!(err_num(&engine, &mut r, "INSERT INTO t VALUES (3)"), None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn execute_permission_and_ownership_chaining() {
        let path = unique_temp_path("perms-chain");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE secret (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO secret VALUES (42)").unwrap();
        engine
            .execute("CREATE PROCEDURE read_secret AS SELECT id FROM secret")
            .unwrap();
        engine
            .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
            .unwrap();
        engine
            .execute("CREATE USER appuser FOR LOGIN applogin")
            .unwrap();

        let mut r = restricted_ctx(&engine, "applogin");
        // No EXECUTE grant: EXEC denied.
        assert_eq!(err_num(&engine, &mut r, "EXEC read_secret"), Some(229));
        // GRANT EXECUTE only on the proc — NOT SELECT on the table it reads.
        engine
            .execute("GRANT EXECUTE ON read_secret TO appuser")
            .unwrap();
        // Ownership chaining: the proc runs, its body's SELECT on secret is not
        // re-checked (same dbo owner).
        assert_eq!(err_num(&engine, &mut r, "EXEC read_secret"), None);
        // But a DIRECT read of the table is still denied.
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM secret"), Some(229));

        // Dynamic SQL does NOT ownership-chain: a restricted user cannot escape
        // the check by wrapping the read (or a write) in sp_executesql.
        assert_eq!(
            err_num(
                &engine,
                &mut r,
                "EXEC sp_executesql N'SELECT id FROM secret'"
            ),
            Some(229)
        );
        assert_eq!(
            err_num(&engine, &mut r, "EXEC sp_executesql N'DELETE FROM secret'"),
            Some(229)
        );
        assert_eq!(
            err_num(
                &engine,
                &mut r,
                "EXEC sp_executesql N'INSERT INTO secret VALUES (7)'"
            ),
            Some(229)
        );
        // A GRANT makes the dynamic read work (proving the check, not a blanket ban).
        engine.execute("GRANT SELECT ON secret TO appuser").unwrap();
        assert_eq!(
            err_num(
                &engine,
                &mut r,
                "EXEC sp_executesql N'SELECT id FROM secret'"
            ),
            None
        );
        engine
            .execute("REVOKE SELECT ON secret FROM appuser")
            .unwrap();

        // Dynamic SQL nested INSIDE a procedure body still does not chain: the
        // dynamic read is checked as the caller (DynamicScope resets the chaining
        // depth). Grant EXECUTE on the wrapper proc but NOT SELECT on the table.
        engine
            .execute("CREATE PROCEDURE dyn_read AS EXEC sp_executesql N'SELECT id FROM secret'")
            .unwrap();
        engine
            .execute("GRANT EXECUTE ON dyn_read TO appuser")
            .unwrap();
        assert_eq!(err_num(&engine, &mut r, "EXEC dyn_read"), Some(229));
        // Granting the caller SELECT lets the nested dynamic read succeed.
        engine.execute("GRANT SELECT ON secret TO appuser").unwrap();
        assert_eq!(err_num(&engine, &mut r, "EXEC dyn_read"), None);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn dropping_a_grantee_scrubs_its_object_permissions() {
        let path = unique_temp_path("perms-scrub");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine.execute("CREATE USER alice").unwrap();
        engine.execute("GRANT SELECT ON t TO alice").unwrap();

        let perm_count = |engine: &Engine| -> String {
            let (_c, rows) = sql_rows(
                engine,
                "SELECT COUNT(*) FROM sys.database_permissions WHERE major_id = \
                 (SELECT object_id FROM sys.tables WHERE name = 't')",
            );
            rows[0][0].clone().unwrap()
        };
        assert_eq!(perm_count(&engine), "1");
        // Dropping the grantee removes the dangling entry (so a later object_id
        // reuse after restart cannot re-point it at a new principal).
        engine.execute("DROP USER alice").unwrap();
        assert_eq!(perm_count(&engine), "0");
        // Persisted: the scrub survives a restart.
        drop(engine);
        let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
        assert_eq!(perm_count(&engine), "0");
        let _ = std::fs::remove_file(path);
    }

    // ---- Stage 16 exit matrices --------------------------------------------

    #[test]
    fn deny_beats_grant_across_nested_roles() {
        // A user in a nested role chain (u ∈ r1 ∈ r2): a GRANT high in the chain
        // is overridden by a DENY at any level, and REVOKE of the DENY restores
        // the inherited GRANT — the DENY/GRANT truth table across nested roles.
        let path = unique_temp_path("exit-deny");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO t VALUES (1)").unwrap();
        engine
            .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
            .unwrap();
        engine
            .execute("CREATE USER appuser FOR LOGIN applogin")
            .unwrap();
        engine.execute("CREATE ROLE r1").unwrap();
        engine.execute("CREATE ROLE r2").unwrap();
        engine.execute("ALTER ROLE r1 ADD MEMBER appuser").unwrap();
        engine.execute("ALTER ROLE r2 ADD MEMBER r1").unwrap(); // u ∈ r1 ∈ r2

        let mut r = restricted_ctx(&engine, "applogin");
        // Ungranted → denied.
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
        // GRANT to the OUTER role r2 is inherited transitively through r1.
        engine.execute("GRANT SELECT ON t TO r2").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
        // A DENY at the INTERMEDIATE role r1 beats the inherited grant.
        engine.execute("DENY SELECT ON t TO r1").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
        // REVOKE the DENY → the r2 grant is inherited again.
        engine.execute("REVOKE SELECT ON t FROM r1").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
        // A DENY on the user directly also beats the role grant.
        engine.execute("DENY SELECT ON t TO appuser").unwrap();
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn ownership_chaining_through_a_proc_over_a_view_over_a_table() {
        // The classic chain: EXECUTE on a proc, which reads a view, which reads a
        // table — the restricted user needs neither SELECT on the view nor on the
        // table (all owned by dbo), only EXECUTE on the proc.
        let path = unique_temp_path("exit-chain");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE base (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine.execute("INSERT INTO base VALUES (7)").unwrap();
        engine
            .execute("CREATE VIEW v AS SELECT id FROM base")
            .unwrap();
        engine
            .execute("CREATE PROCEDURE p AS SELECT id FROM v")
            .unwrap();
        engine
            .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
            .unwrap();
        engine
            .execute("CREATE USER appuser FOR LOGIN applogin")
            .unwrap();
        engine.execute("GRANT EXECUTE ON p TO appuser").unwrap();

        let mut r = restricted_ctx(&engine, "applogin");
        // Runs via the chain — no SELECT on v or base needed.
        assert_eq!(err_num(&engine, &mut r, "EXEC p"), None);
        // Direct reads of the view and the table are still denied.
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM v"), Some(229));
        assert_eq!(err_num(&engine, &mut r, "SELECT id FROM base"), Some(229));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn config_migration_upgrades_an_existing_pre_stage16_catalog() {
        use std::collections::BTreeMap;
        let path = unique_temp_path("exit-migrate");
        // A "pre-Stage-16" database: it has schema objects but migration never
        // ran, so there are no catalog logins yet.
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        assert!(engine.lookup_login("sa").is_none(), "no logins yet");
        drop(engine);

        // Reopen under the new build and run first-boot migration with config
        // users — the existing catalog is upgraded in place.
        let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
        let mut users = BTreeMap::new();
        users.insert("sa".to_string(), "secret".to_string());
        users.insert("app".to_string(), "app-pw".to_string());
        let created = engine.migrate_logins(&users).expect("migrate");
        assert!(created.contains(&"app".to_string()), "config user migrated");
        assert!(engine.lookup_login("sa").is_some(), "sa seeded");
        assert!(engine.lookup_login("app").is_some(), "app seeded");
        // The pre-existing table is untouched.
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM sys.tables WHERE name = 't'");
        assert_eq!(rows, vec![vec![Some("1".to_string())]]);
        // Idempotent: a subsequent start does not re-migrate.
        assert!(engine.migrate_logins(&users).expect("second").is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn ddl_and_grant_require_privilege() {
        let path = unique_temp_path("perms-ddl");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine
            .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
            .unwrap();
        engine
            .execute("CREATE USER appuser FOR LOGIN applogin")
            .unwrap();

        let mut r = restricted_ctx(&engine, "applogin");
        // A restricted user cannot run DDL...
        assert_eq!(
            err_num(
                &engine,
                &mut r,
                "CREATE TABLE hax (id INT NOT NULL PRIMARY KEY)"
            ),
            Some(15247)
        );
        assert_eq!(err_num(&engine, &mut r, "DROP TABLE t"), Some(15247));
        assert_eq!(err_num(&engine, &mut r, "CREATE ROLE sneaky"), Some(15247));
        // ...and cannot grant itself permissions.
        assert_eq!(
            err_num(&engine, &mut r, "GRANT SELECT ON t TO appuser"),
            Some(15247)
        );
        // A sysadmin (sa) session bypasses — DDL and GRANT succeed.
        engine
            .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
            .unwrap();
        let mut admin = restricted_ctx(&engine, "sa"); // sa → dbo/sysadmin → bypass
        assert_eq!(
            err_num(&engine, &mut admin, "GRANT SELECT ON t TO appuser"),
            None
        );
        assert_eq!(
            err_num(
                &engine,
                &mut admin,
                "CREATE TABLE ok (id INT NOT NULL PRIMARY KEY)"
            ),
            None
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sys_database_permissions_reflects_grants_and_survives_restart() {
        let path = unique_temp_path("perms-catalog");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .unwrap();
        engine.execute("CREATE USER appuser").unwrap();
        engine.execute("GRANT SELECT ON t TO appuser").unwrap();
        engine.execute("DENY UPDATE ON t TO appuser").unwrap();

        let check = |engine: &Engine| {
            let (_c, rows) = sql_rows(
                engine,
                "SELECT permission_name, state_desc FROM sys.database_permissions \
                 WHERE major_id = (SELECT object_id FROM sys.tables WHERE name = 't') \
                 ORDER BY permission_name",
            );
            assert_eq!(
                rows,
                vec![
                    vec![Some("SELECT".into()), Some("GRANT".into())],
                    vec![Some("UPDATE".into()), Some("DENY".into())],
                ]
            );
        };
        check(&engine);
        // Survives restart (permissions ride the object's catalog row).
        drop(engine);
        let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
        check(&engine);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn showplan_names_a_table_valued_function() {
        let path = unique_temp_path("tvf-showplan");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, grp INT NOT NULL)")
            .expect("nums");
        engine
            .execute(
                "CREATE FUNCTION dbo.in_group (@g INT) RETURNS TABLE AS \
                 RETURN (SELECT id FROM nums WHERE grp = @g)",
            )
            .expect("tvf");
        // A lone TVF in FROM must not render as a phantom nested-loops join over
        // a base table named after the function.
        let plan = plan_lines(&engine, "SELECT id FROM dbo.in_group(20)");
        assert!(
            plan.iter().any(|l| l.contains("Table-valued Function")),
            "plan names the TVF: {plan:?}"
        );
        assert!(
            !plan.iter().any(|l| l.contains("Nested Loops")),
            "no phantom join: {plan:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn multi_statement_tvf_returns_body_populated_rows() {
        let path = unique_temp_path("multi-tvf-basic");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY)")
            .expect("nums");
        engine
            .execute("INSERT INTO nums VALUES (1),(2),(3),(4),(5),(6)")
            .expect("seed");
        // A multi-statement TVF: its body populates the RETURNS table variable
        // from a real table, and the accumulated rows are the result.
        engine
            .execute(
                "CREATE FUNCTION dbo.evens (@n INT) RETURNS @r TABLE (v INT NOT NULL PRIMARY KEY) \
                 AS BEGIN INSERT INTO @r SELECT id FROM nums WHERE id % 2 = 0 AND id <= @n; RETURN END",
            )
            .expect("create multi-tvf");
        let (_cols, rows) = sql_rows(&engine, "SELECT v FROM dbo.evens(5) ORDER BY v");
        assert_eq!(
            rows,
            vec![vec![Some("2".to_string())], vec![Some("4".to_string())]],
            "the body filtered nums to the evens ≤ 5"
        );
        // A different argument reruns the body.
        let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) AS n FROM dbo.evens(6)");
        assert_eq!(rows, vec![vec![Some("3".to_string())]], "evens ≤ 6 = 2,4,6");
        // The RETURNS table's PRIMARY KEY is enforced when the body populates it:
        // a duplicate key raises 2627 at call time (the body is not run at CREATE).
        engine
            .execute(
                "CREATE FUNCTION dbo.dup () RETURNS @r TABLE (id INT NOT NULL PRIMARY KEY) \
                 AS BEGIN INSERT INTO @r VALUES (1), (1); RETURN END",
            )
            .expect("create dup TVF");
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "SELECT id FROM dbo.dup()");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(2627),
            "a duplicate result PRIMARY KEY raises 2627: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn multi_statement_tvf_body_reads_are_locked_and_snapshotted() {
        use crate::lock::{LockMode, Resource};
        use crate::rel::Isolation;
        let path = unique_temp_path("multi-tvf-seam");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE secret (z INT NOT NULL PRIMARY KEY)")
            .expect("secret");
        engine
            .execute(
                "CREATE FUNCTION dbo.copy_secret () RETURNS @r TABLE (z INT NOT NULL PRIMARY KEY) \
                 AS BEGIN INSERT INTO @r SELECT z FROM secret; RETURN END",
            )
            .expect("multi-tvf");
        let secret = table_object_id(&engine, "secret");
        // The body's read of `secret` must be Shared-locked up front, just like
        // an inline TVF or a scalar UDF body — the lock seam.
        let locks =
            engine.analyze_locks("SELECT z FROM dbo.copy_secret()", Isolation::ReadCommitted);
        assert!(
            locks.contains(&(Resource::Table(secret), LockMode::Shared)),
            "a multi-statement TVF's body table must be Shared-locked: {locks:?}"
        );
        // And it must arm the snapshot scope: under SNAPSHOT-not-allowed the body
        // read is a data access, so the call raises 3952.
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        );
        let out = batch(&engine, &mut ctx, "SELECT z FROM dbo.copy_secret()");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3952),
            "a body-reading multi-statement TVF must arm the snapshot scope: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn multi_statement_tvf_rejects_real_table_dml_at_create() {
        let path = unique_temp_path("multi-tvf-sideeffect");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE log (id INT NOT NULL PRIMARY KEY)")
            .expect("log");
        let mut ctx = TxnContext::default();
        // A multi-statement TVF may DML its result table variable, but writing a
        // real table is a side effect rejected at CREATE (443).
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE FUNCTION dbo.bad () RETURNS @r TABLE (id INT NOT NULL PRIMARY KEY) \
             AS BEGIN INSERT INTO log VALUES (1); RETURN END",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(443),
            "side-effecting DML in a TVF body is 443: {:?}",
            out.error
        );
        // Its body must end in RETURN (455).
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE FUNCTION dbo.noret () RETURNS @r TABLE (id INT NOT NULL PRIMARY KEY) \
             AS BEGIN INSERT INTO @r VALUES (1) END",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(455),
            "a function body must end in RETURN: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_describe_stops_at_control_flow() {
        // sp_describe_first_result_set: a batch whose FIRST possible rowset
        // sits inside an IF must answer "not statically derivable" — skipping
        // the IF and describing a LATER statement would hand a prepared
        // driver the wrong COLMETADATA when the branch streams first.
        let path = unique_temp_path("cf-describe");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v NVARCHAR(10))",
        );
        let described =
            engine.describe_first_result_set("IF 1 = 1 SELECT id FROM t; SELECT v FROM t");
        assert!(
            described.is_err(),
            "an IF-guarded first rowset is not statically derivable: {described:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_own_txn_writes_visible_in_condition() {
        // A transaction's own uncommitted write is visible to its own IF
        // condition (plain READ COMMITTED, no versioning).
        let path = unique_temp_path("cf-own-writes");
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
             INSERT INTO t VALUES (5); \
             IF EXISTS (SELECT * FROM t WHERE id = 5) SELECT 1 AS n ELSE SELECT 0 AS n; \
             ROLLBACK",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![1]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_exec_inner_return_exits_inner_batch_only() {
        // A RETURN inside EXEC'd text ends the INNER batch; the outer batch
        // continues after the EXEC.
        let path = unique_temp_path("cf-exec-return");
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
            "EXEC sp_executesql N'INSERT INTO t VALUES (1); RETURN; INSERT INTO t VALUES (2)'; \
             INSERT INTO t VALUES (3); \
             SELECT id FROM t ORDER BY id",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            ids(&out),
            vec![1, 3],
            "inner RETURN skipped only the inner tail"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_cancel_lands_mid_while() {
        // An Attention arriving while a WHILE spins aborts the batch with the
        // cancel marker instead of looping forever.
        let path = unique_temp_path("cf-cancel-while");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        crate::rel::set_test_cancel(flag.clone());
        let setter = {
            let flag = flag.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(150));
                flag.store(true, std::sync::atomic::Ordering::Relaxed);
            })
        };
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @i INT = 0; WHILE 1 = 1 SET @i = @i + 1",
        );
        crate::rel::clear_test_cancel();
        setter.join().expect("setter thread");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3617),
            "the spin died on the Attention"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_condition_cte_executes_but_analysis_misses_it() {
        // Runtime reachability half of the CTE lock hole: the executor
        // inlines a WITH inside an IF condition's subquery and reads the base
        // table (see storage.rs cf_review_analyze_locks_condition_cte for the
        // analysis half — the lock set contains nothing for it).
        let path = unique_temp_path("cf-cond-cte-runtime");
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
            "IF EXISTS (WITH x AS (SELECT id FROM t) SELECT id FROM x) \
             SELECT 1 AS n ELSE SELECT 0 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(ids(&out), vec![1], "the CTE condition read the table");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_rcsi_condition_read_is_versioned() {
        // Under RCSI a READ COMMITTED read takes no Table S — it relies on
        // the per-statement snapshot instead. The IF condition's subquery
        // must therefore read through a snapshot exactly like a SELECT
        // statement does; reading the raw latest state is a dirty read
        // (expectations here are the FIXED behavior).
        let path = unique_temp_path("cf-rcsi-cond");
        let engine = new_engine(&path);
        let mut admin = TxnContext::default();
        batch(
            &engine,
            &mut admin,
            "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        );
        batch(
            &engine,
            &mut admin,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        let mut writer = TxnContext::default();
        let out = batch(&engine, &mut writer, "BEGIN TRAN; INSERT INTO t VALUES (1)");
        assert!(out.error.is_none(), "{:?}", out.error);
        let mut reader = TxnContext::default();
        let out = batch(&engine, &mut reader, "SELECT COUNT(*) AS n FROM t");
        assert_eq!(
            ids(&out),
            vec![0],
            "sanity: a plain SELECT reads the snapshot, not the writer's uncommitted row"
        );
        let out = batch(
            &engine,
            &mut reader,
            "IF EXISTS (SELECT * FROM t WHERE id = 1) SELECT 1 AS n ELSE SELECT 0 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            ids(&out),
            vec![0],
            "the IF condition must read the same snapshot a SELECT would — \
             seeing the writer's uncommitted row is a dirty read"
        );
        batch(&engine, &mut writer, "ROLLBACK");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cf_review_snapshot_isolation_condition_uses_txn_snapshot() {
        // SNAPSHOT isolation: every read in the transaction sees the
        // transaction's snapshot. A commit that lands after the snapshot was
        // established must stay invisible to an IF condition too
        // (expectations here are the FIXED behavior).
        let path = unique_temp_path("cf-snap-cond");
        let engine = new_engine(&path);
        let mut admin = TxnContext::default();
        batch(
            &engine,
            &mut admin,
            "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON",
        );
        batch(
            &engine,
            &mut admin,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut admin, "INSERT INTO t VALUES (1)");
        let mut reader = TxnContext::default();
        batch(
            &engine,
            &mut reader,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
        );
        let out = batch(
            &engine,
            &mut reader,
            "BEGIN TRAN; SELECT COUNT(*) AS n FROM t",
        );
        assert_eq!(ids(&out), vec![1], "the snapshot is established at 1 row");
        let out = batch(&engine, &mut admin, "INSERT INTO t VALUES (2)");
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(
            &engine,
            &mut reader,
            "IF EXISTS (SELECT * FROM t WHERE id = 2) SELECT 1 AS n ELSE SELECT 0 AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            ids(&out),
            vec![0],
            "the IF condition must read the transaction's snapshot, \
             not the post-snapshot commit"
        );
        batch(&engine, &mut reader, "COMMIT");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn procedure_ddl_round_trips_and_survives_reopen() {
        // CREATE/ALTER/DROP PROCEDURE: catalog persistence (the body is
        // stored text), name collision (2714), the first-statement rule
        // (111), and RETURN <value> legal only inside a body.
        let path = unique_temp_path("proc-ddl");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE add_pair @a INT, @b INT = 7 OUTPUT AS \
             SET @b = @a + @b; RETURN 0",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        // Name collision with any object class.
        let out = batch(&engine, &mut ctx, "CREATE PROC add_pair AS SELECT 1");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(2714));
        // Not the first statement in the batch: 111.
        let out = batch(&engine, &mut ctx, "SELECT 1; CREATE PROC late AS SELECT 1");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(111));
        // RETURN <value> stays illegal OUTSIDE a body.
        let out = batch(&engine, &mut ctx, "RETURN 3");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
        // ALTER replaces; ALTER of a missing procedure errors.
        let out = batch(
            &engine,
            &mut ctx,
            "ALTER PROCEDURE add_pair @a INT AS RETURN @a",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "ALTER PROC no_such AS SELECT 1");
        assert!(out.error.is_some(), "ALTER of a missing procedure fails");
        drop(engine);

        // The definition survives a reopen; DROP removes it.
        let engine = {
            let storage = Storage::open(path.clone()).expect("reopen");
            Engine::new(storage).expect("engine")
        };
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "CREATE PROC add_pair AS SELECT 1");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(2714),
            "the procedure survived the reopen"
        );
        let out = batch(&engine, &mut ctx, "DROP PROCEDURE add_pair");
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "DROP PROCEDURE add_pair");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3701));
        let out = batch(&engine, &mut ctx, "DROP PROCEDURE IF EXISTS add_pair");
        assert!(out.error.is_none(), "IF EXISTS swallows the miss");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn exec_user_procedure_binds_returns_and_copies_output() {
        let path = unique_temp_path("proc-exec");
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
            "CREATE PROCEDURE ins_and_double @a INT, @b INT = 10, @twice INT OUTPUT AS \
             INSERT INTO t VALUES (@a); \
             SET @twice = (@a + @b) * 2; \
             SELECT @a AS n; \
             RETURN @a + 1",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        // Positional + named + OUTPUT + @rc, default filling @b.
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @d INT, @rc INT; \
             EXEC @rc = ins_and_double 5, @twice = @d OUTPUT; \
             SELECT @rc AS n; SELECT @d AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let firsts: Vec<i64> = out
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => Some(i64::from(v)),
                    Datum::BigInt(v) => Some(v),
                    ref other => panic!("expected int, got {other:?}"),
                },
                _ => None,
            })
            .collect();
        assert_eq!(
            firsts,
            vec![5, 6, 30],
            "body SELECT streamed; @rc = RETURN @a+1; @twice = (5+10)*2"
        );
        let out = batch(&engine, &mut ctx, "SELECT id FROM t");
        assert_eq!(ids(&out), vec![5], "the body's INSERT landed");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn exec_user_procedure_argument_errors() {
        let path = unique_temp_path("proc-args");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE p2 @a INT, @o INT OUTPUT AS SET @o = @a",
        );
        let case = |sql: &str| -> Option<i32> {
            let mut c = TxnContext::default();
            batch(&engine, &mut c, sql).error.map(|e| e.number)
        };
        assert_eq!(case("EXEC p2"), Some(201), "missing @a");
        assert_eq!(
            case("DECLARE @x INT; EXEC p2 1, @x OUTPUT, 3"),
            Some(8144),
            "too many arguments"
        );
        assert_eq!(
            case("DECLARE @x INT; EXEC p2 @a = 1, @nope = 2"),
            Some(8145),
            "unknown named parameter"
        );
        assert_eq!(
            case("DECLARE @x INT; EXEC p2 @a = @x OUTPUT, @o = @x"),
            Some(8162),
            "OUTPUT on a non-OUTPUT parameter"
        );
        assert_eq!(
            case("EXEC p2 1, 2 OUTPUT"),
            Some(179),
            "OUTPUT with a constant"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recursive_procedure_hits_the_nesting_cap() {
        let path = unique_temp_path("proc-recurse");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE recur AS SELECT CAST(@@NESTLEVEL AS INT) AS n; EXEC recur",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "EXEC recur");
        // The batch surfaces 217 when the recursion exceeds depth 32...
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(217));
        // ...and the streamed @@NESTLEVEL values count 1, 2, 3, ...
        let levels: Vec<i64> = out
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => Some(i64::from(v)),
                    Datum::BigInt(v) => Some(v),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        assert_eq!(levels.first(), Some(&1));
        assert_eq!(levels.last(), Some(&32), "depth 32 ran; 33 was refused");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn error_procedure_names_the_failing_procedure() {
        let path = unique_temp_path("proc-errproc");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE boomer AS RAISERROR('inside', 16, 1)",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY EXEC boomer; END TRY \
             BEGIN CATCH SELECT ERROR_PROCEDURE() AS p, ERROR_NUMBER() AS n; END CATCH",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let StatementResult::Rows(rowset) = &out.results[0] else {
            panic!("expected rows");
        };
        assert_eq!(
            rowset.rows[0][0],
            Datum::NVarChar("boomer".into()),
            "ERROR_PROCEDURE() names the proc"
        );
        // Outside any procedure, ERROR_PROCEDURE() stays NULL.
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE u (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO u VALUES (1)");
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY INSERT INTO u VALUES (1); END TRY \
             BEGIN CATCH SELECT ERROR_PROCEDURE() AS p; END CATCH",
        );
        let StatementResult::Rows(rowset) = &out.results[0] else {
            panic!("expected rows");
        };
        assert_eq!(rowset.rows[0][0], Datum::Null, "NULL in an ad-hoc batch");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn output_and_return_skipped_when_the_body_aborts() {
        let path = unique_temp_path("proc-abort-output");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE fails @o INT OUTPUT AS SET @o = 99; THROW 50001, 'die', 1",
        );
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @v INT = 7, @rc INT = -1; \
             EXEC @rc = fails @o = @v OUTPUT; \
             SELECT @v AS n; SELECT @rc AS n",
        );
        // The THROW terminated the batch too, so nothing after the EXEC ran —
        // and neither copy-back nor @rc assignment happened.
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
        assert!(
            !out.results
                .iter()
                .any(|r| matches!(r, StatementResult::Rows(_))),
            "the post-EXEC selects never ran: {:?}",
            out.results
        );
        let _ = std::fs::remove_file(path);
    }

    // ---- adversarial review probes: Stage 15 stored procedures ----------

    /// First cell of every streamed rowset, as i64 (panics on non-integers).
    fn review_first_cells(out: &BatchOutcome) -> Vec<i64> {
        out.results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                    Datum::Int(v) => Some(i64::from(v)),
                    Datum::BigInt(v) => Some(v),
                    ref other => panic!("expected int, got {other:?}"),
                },
                _ => None,
            })
            .collect()
    }

    /// SQL Server refuses a positional argument after a named one (error
    /// 119). The current binder accepts it and binds BOTH: the positional
    /// value lands on the parameter by position and a named argument for the
    /// same parameter is silently discarded — a silent misbind.
    #[test]
    fn review_poc_positional_after_named_is_refused() {
        let path = unique_temp_path("proc-pos-after-named");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE pan @a INT, @b INT AS SELECT @a * 100 + @b AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "EXEC pan @b = 1, 2");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(119),
            "a positional argument after a named one is error 119; instead: \
             error {:?}, streamed {:?}",
            out.error,
            review_first_cells(&out)
        );
        let _ = std::fs::remove_file(path);
    }

    /// SQL Server coerces each argument to the declared parameter type at
    /// bind time ('7' for an INT parameter arrives as int 7; an unconvertible
    /// string is a conversion error at the EXEC). The current binder stores
    /// the raw value with the declared type tag and never converts, so the
    /// body sees a string where it declared an INT.
    #[test]
    fn review_poc_exec_argument_coerced_to_declared_type() {
        let path = unique_temp_path("proc-arg-coerce");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE echoi @a INT AS SELECT @a AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "EXEC echoi '7'");
        assert!(out.error.is_none(), "{:?}", out.error);
        let StatementResult::Rows(rowset) = &out.results[0] else {
            panic!("expected rows, got {:?}", out.results);
        };
        assert!(
            matches!(rowset.rows[0][0], Datum::Int(7) | Datum::BigInt(7)),
            "'7' bound to @a INT must arrive as int 7 (DECLARE/SET coerce; \
             EXEC binding must too), got {:?}",
            rowset.rows[0][0]
        );
        // An unconvertible string is a conversion error at the EXEC.
        let out = batch(&engine, &mut ctx, "EXEC echoi 'nope'");
        assert!(
            out.error.is_some(),
            "'nope' for @a INT must fail conversion at bind, streamed {:?}",
            out.results
        );
        let _ = std::fs::remove_file(path);
    }

    /// `EXEC @rc = p` with an UNDECLARED @rc is error 137 in SQL Server. The
    /// current code inserts the variable into the caller scope unconditionally,
    /// silently creating it.
    #[test]
    fn review_poc_undeclared_return_status_variable_is_137() {
        let path = unique_temp_path("proc-undeclared-rc");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "CREATE PROCEDURE r4 AS RETURN 4");
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "EXEC @rc = r4; SELECT @rc AS n");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(137),
            "an undeclared return-status variable must be 137; instead: \
             error {:?}, streamed {:?}",
            out.error,
            review_first_cells(&out)
        );
        let _ = std::fs::remove_file(path);
    }

    /// A proc error caught by the CALLER's TRY terminated the body: neither
    /// OUTPUT copy-back nor the @rc assignment happens, but the batch
    /// continues in the CATCH. (The committed abort test cannot observe this
    /// — its THROW ends the whole batch before anything reads @v/@rc.)
    #[test]
    fn review_poc_output_and_rc_skipped_when_proc_error_is_caught() {
        let path = unique_temp_path("proc-caught-output");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE failo @o INT OUTPUT AS \
             SET @o = 99; RAISERROR('die', 16, 1); SET @o = 98",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @v INT = 7, @rc INT = -1; \
             BEGIN TRY EXEC @rc = failo @o = @v OUTPUT; END TRY \
             BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH; \
             SELECT @v AS n; SELECT @rc AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            review_first_cells(&out),
            vec![50000, 7, -1],
            "caught proc error: no copy-back (7 stays), no @rc (-1 stays)"
        );
        let _ = std::fs::remove_file(path);
    }

    /// A statement-scope RAISERROR 16 with no TRY anywhere does NOT terminate
    /// the proc body: the body runs to completion, so OUTPUT copy-back DOES
    /// happen — with the value assigned after the error.
    #[test]
    fn review_poc_statement_scope_raiserror_completes_body_and_copies_output() {
        let path = unique_temp_path("proc-warn-output");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE warns @o INT OUTPUT AS \
             SET @o = 1; RAISERROR('w', 16, 1); SET @o = 2",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @v INT = 0; EXEC warns @o = @v OUTPUT; SELECT @v AS n",
        );
        assert_eq!(
            review_first_cells(&out),
            vec![2],
            "the body completed past the statement-scope error; copy-back ran"
        );
        let _ = std::fs::remove_file(path);
    }

    /// Nested procedures: each frame's RETURN status is its own. An inner
    /// EXEC's status never bleeds into the outer frame's `EXEC @rc =`, even
    /// when the outer body's LAST action is that inner EXEC.
    #[test]
    fn review_poc_nested_return_statuses_do_not_bleed() {
        let path = unique_temp_path("proc-nested-rc");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE PROCEDURE five AS RETURN 5",
            "CREATE PROCEDURE seven AS EXEC five; RETURN 7",
            "CREATE PROCEDURE tail AS EXEC five",
            "CREATE PROCEDURE captures AS DECLARE @x INT; EXEC @x = five; SELECT @x AS n",
        ] {
            let out = batch(&engine, &mut ctx, sql);
            assert!(out.error.is_none(), "{sql}: {:?}", out.error);
        }
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @a INT, @b INT, @c INT; \
             EXEC @a = seven; EXEC @b = tail; EXEC @c = captures; \
             SELECT @a AS n; SELECT @b AS n; SELECT @c AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            review_first_cells(&out),
            vec![5, 7, 0, 0],
            "captures streamed inner's 5; then @a=7 (outer RETURN wins), \
             @b=0 (tail's inner EXEC consumed its own status), @c=0"
        );
        let _ = std::fs::remove_file(path);
    }

    /// The 217 depth-cap error path unwinds EXEC_DEPTH all the way: a
    /// subsequent EXEC in the same session starts at nest level 1 again.
    #[test]
    fn review_poc_exec_depth_unwinds_after_217() {
        let path = unique_temp_path("proc-depth-unwind");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        for sql in [
            "CREATE PROCEDURE recur2 AS EXEC recur2",
            "CREATE PROCEDURE shallow AS SELECT CAST(@@NESTLEVEL AS INT) AS n",
        ] {
            let out = batch(&engine, &mut ctx, sql);
            assert!(out.error.is_none(), "{sql}: {:?}", out.error);
        }
        let out = batch(&engine, &mut ctx, "EXEC recur2");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(217));
        let out = batch(&engine, &mut ctx, "EXEC shallow");
        assert!(
            out.error.is_none(),
            "depth leaked past the 217: {:?}",
            out.error
        );
        assert_eq!(
            review_first_cells(&out),
            vec![1],
            "@@NESTLEVEL restarts at 1 after the failed recursion"
        );
        let _ = std::fs::remove_file(path);
    }

    /// A parameter named like a caller variable is a separate slot: the body
    /// mutates its own @a; the caller's @a is untouched after the EXEC.
    #[test]
    fn review_poc_param_scope_isolated_from_caller_variable() {
        let path = unique_temp_path("proc-shadow");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE shadow @a INT AS SET @a = @a + 1; SELECT @a AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(
            &engine,
            &mut ctx,
            "DECLARE @a INT = 1; EXEC shadow @a = @a; SELECT @a AS n",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            review_first_cells(&out),
            vec![2, 1],
            "inside: 2; caller's @a unchanged: 1"
        );
        let _ = std::fs::remove_file(path);
    }

    /// ERROR_PROCEDURE() precision under nested CATCHes: a second error in
    /// the ad-hoc CATCH pushes its own frame (procedure NULL, its own
    /// number); when that inner CATCH exits, the outer CATCH's ERROR_*()
    /// resolve to the procedure error again.
    #[test]
    fn review_poc_error_procedure_survives_second_error_in_catch() {
        let path = unique_temp_path("proc-errproc-nested");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE boom2 AS RAISERROR('inside', 16, 1)",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRY EXEC boom2; END TRY \
             BEGIN CATCH \
               SELECT ERROR_PROCEDURE() AS p; \
               BEGIN TRY SELECT 1/0 AS d; END TRY \
               BEGIN CATCH SELECT ERROR_PROCEDURE() AS p; SELECT ERROR_NUMBER() AS n; END CATCH; \
               SELECT ERROR_PROCEDURE() AS p; \
             END CATCH",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let cells: Vec<Datum> = out
            .results
            .iter()
            .filter_map(|r| match r {
                StatementResult::Rows(rowset) => Some(rowset.rows[0][0].clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            cells,
            vec![
                Datum::NVarChar("boom2".into()),
                Datum::Null,
                Datum::BigInt(8134),
                Datum::NVarChar("boom2".into()),
            ],
            "outer: boom2; inner: NULL + 8134; outer again: boom2"
        );
        let _ = std::fs::remove_file(path);
    }

    /// DROP TABLE of a procedure must be refused (SQL Server 3701: the
    /// procedure namespace is not the table namespace), exactly as DROP TABLE
    /// of a view already is. The current arm only guards views, so DROP TABLE
    /// silently destroys a procedure.
    #[test]
    fn review_poc_drop_table_does_not_drop_a_procedure() {
        let path = unique_temp_path("proc-drop-table");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "CREATE PROCEDURE keepp AS SELECT 1 AS n");
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "DROP TABLE keepp");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(3701),
            "DROP TABLE of a procedure must fail, got {:?}",
            out.error
        );
        let out = batch(&engine, &mut ctx, "EXEC keepp");
        assert!(
            out.error.is_none(),
            "the procedure survived the wrong-type DROP: {:?}",
            out.error
        );
        let _ = std::fs::remove_file(path);
    }

    /// DML against a procedure name errors cleanly (the object is not a
    /// table); none of these may succeed or panic.
    #[test]
    fn review_poc_dml_on_a_procedure_name_errors_cleanly() {
        let path = unique_temp_path("proc-dml");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(&engine, &mut ctx, "CREATE PROCEDURE nott AS SELECT 1 AS n");
        assert!(out.error.is_none(), "{:?}", out.error);
        // Observed today: SELECT * silently streams an EMPTY rowset and
        // DELETE silently reports 0 rows affected; only INSERT (110) and
        // UPDATE (207) error, for incidental column reasons.
        for sql in [
            "SELECT * FROM nott",
            "INSERT INTO nott VALUES (1)",
            "UPDATE nott SET x = 1",
            "DELETE FROM nott",
        ] {
            let out = batch(&engine, &mut ctx, sql);
            assert!(
                out.error.is_some(),
                "{sql}: must error (a procedure is not a table), streamed {:?}",
                out.results
            );
        }
        let _ = std::fs::remove_file(path);
    }

    /// SQL Server requires procedure parameter defaults to be CONSTANTS
    /// (literals or NULL). The current code stores the default's source text
    /// and evaluates it at EXEC against the CALLER's scope — so `@b INT = @a`
    /// captures whatever @a happens to be in each caller, and even a niladic
    /// function default drifts per call.
    #[test]
    fn review_poc_non_constant_parameter_default_rejected_at_create() {
        let path = unique_temp_path("proc-nonconst-default");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "CREATE PROCEDURE dflt @b INT = @a AS SELECT @b AS n",
        );
        assert!(
            out.error.is_some(),
            "a variable-referencing parameter default must be rejected at \
             CREATE (SQL Server: defaults are constants)"
        );
        let _ = std::fs::remove_file(path);
    }

    /// The stored body text round-trips exactly through sys.sql_modules:
    /// embedded quotes, a line comment, a newline, a trailing statement.
    #[test]
    fn review_poc_body_text_round_trips_through_sys_sql_modules() {
        let path = unique_temp_path("proc-body-roundtrip");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let body = "SELECT 'it''s' AS s -- trailing comment\n; SELECT 2 AS n";
        let out = batch(
            &engine,
            &mut ctx,
            &format!("CREATE PROCEDURE qbody AS {body}"),
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(
            &engine,
            &mut ctx,
            "SELECT m.definition FROM sys.sql_modules m \
             JOIN sys.procedures p ON m.object_id = p.object_id \
             WHERE p.name = 'qbody'",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let StatementResult::Rows(rowset) = &out.results[0] else {
            panic!("expected rows, got {:?}", out.results);
        };
        assert_eq!(
            rowset.rows[0][0],
            Datum::NVarChar(body.into()),
            "the definition is the verbatim body text"
        );
        // And the stored text still executes: both rowsets stream.
        let out = batch(&engine, &mut ctx, "EXEC qbody");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            out.results
                .iter()
                .filter(|r| matches!(r, StatementResult::Rows(_)))
                .count(),
            2,
            "both body statements ran: {:?}",
            out.results
        );
        let _ = std::fs::remove_file(path);
    }

    /// CREATE PROCEDURE as dynamic SQL: legal (it is the first statement of
    /// the inner batch, as SQL Server requires), and the DDL-in-transaction
    /// gate still applies through the dynamic path.
    #[test]
    fn review_poc_create_procedure_inside_dynamic_sql() {
        let path = unique_temp_path("proc-dyn-create");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        let out = batch(
            &engine,
            &mut ctx,
            "EXEC sp_executesql N'CREATE PROCEDURE dynp AS SELECT 42 AS n'",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "EXEC dynp");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(review_first_cells(&out), vec![42]);
        // The DDL-in-txn gate is not bypassed by the dynamic path.
        let out = batch(
            &engine,
            &mut ctx,
            "BEGIN TRAN; \
             EXEC sp_executesql N'CREATE PROCEDURE dynp2 AS SELECT 1 AS n';",
        );
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(226),
            "DDL inside an explicit transaction stays refused via dynamic SQL"
        );
        batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
        let out = batch(&engine, &mut ctx, "EXEC dynp2");
        assert_eq!(
            out.error.as_ref().map(|e| e.number),
            Some(2812),
            "dynp2 was never created"
        );
        let _ = std::fs::remove_file(path);
    }

    /// DROP PROCEDURE IF EXISTS of a TABLE name is a silent no-op (the
    /// procedure namespace has no such object, IF EXISTS suppresses the
    /// miss) and the table survives; without IF EXISTS it is 3701.
    #[test]
    fn review_poc_drop_procedure_if_exists_of_a_table_is_a_noop() {
        let path = unique_temp_path("proc-die-table");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE keept (id INT NOT NULL PRIMARY KEY)",
        );
        batch(&engine, &mut ctx, "INSERT INTO keept VALUES (1)");
        let out = batch(&engine, &mut ctx, "DROP PROCEDURE IF EXISTS keept");
        assert!(out.error.is_none(), "{:?}", out.error);
        let out = batch(&engine, &mut ctx, "SELECT id FROM keept");
        assert_eq!(ids(&out), vec![1], "the table survived");
        let out = batch(&engine, &mut ctx, "DROP PROCEDURE keept");
        assert_eq!(out.error.as_ref().map(|e| e.number), Some(3701));
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
    fn trigger_writes_are_fully_undone_after_a_crash() {
        let path = unique_temp_path("trg-crash-undo");
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
            "CREATE TABLE audit (id INT NOT NULL PRIMARY KEY)",
        );
        batch(
            &engine,
            &mut ctx,
            "CREATE TRIGGER trg ON t AFTER INSERT AS INSERT INTO audit SELECT id FROM inserted",
        );

        // Session A: an explicit transaction whose INSERT fires the trigger
        // (writing `audit`), never committed. The firing row and the trigger's
        // write both stage on A's transaction.
        let mut ctx_a = TxnContext::default();
        batch(&engine, &mut ctx_a, "BEGIN TRAN; INSERT INTO t VALUES (99)");
        assert!(ctx_a.has_open_transaction());
        // A committed autocommit insert forces the WAL (including A's
        // uncommitted records) to disk.
        batch(
            &engine,
            &mut TxnContext::default(),
            "INSERT INTO audit VALUES (1)",
        );

        // Crash: no graceful rollback.
        drop(ctx_a);
        drop(engine);

        // Recovery undoes the loser A entirely — the whole statement, DML AND
        // its trigger's write, is atomic: t=99 is gone and the trigger's
        // audit=99 is gone; the separately-committed audit=1 survives.
        let storage = Storage::open(path.clone()).expect("reopen");
        let engine = Engine::new(storage).expect("replay");
        let out_t = batch(
            &engine,
            &mut TxnContext::default(),
            "SELECT id FROM t ORDER BY id",
        );
        assert!(
            ids(&out_t).is_empty(),
            "the firing row must be undone after the crash"
        );
        let out_a = batch(
            &engine,
            &mut TxnContext::default(),
            "SELECT id FROM audit ORDER BY id",
        );
        assert_eq!(
            ids(&out_a),
            vec![1],
            "the trigger's write must be undone with the statement; the committed row survives"
        );
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
            // Pad past the tiny-table tie-break (identically on both sides).
            for i in 0..20 {
                engine
                    .execute(&format!("INSERT INTO {t} VALUES ({}, 900, 'p')", 100 + i))
                    .expect("pad");
            }
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
        // Pad past the tiny-table tie-break: a table of <= 16 rows plans as
        // a scan (the seek ties with it), and this test is about the seek.
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, 900)", 100 + i))
                .expect("pad");
        }

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
        // Pad past the tiny-table tie-break (a <= 16-row table plans as a scan).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, 900)", 100 + i))
                .expect("pad");
        }
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
        // Pad past the tiny-table tie-break (a <= 16-row table plans as a scan).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, 900, 900)", 100 + i))
                .expect("pad");
        }
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
        // Pad past the tiny-table tie-break (a <= 16-row table plans as a scan).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO h VALUES ({}, 'p')", 900 + i))
                .expect("pad");
        }
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
        // Pad past the tiny-table tie-break, or the post-drop "Table Scan"
        // assertion below would hold with the index still present (vacuous).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, 900)", 100 + i))
                .expect("pad");
        }

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
        // Pad past the tiny-table tie-break; '0...' sorts below 'a', so the
        // range assertions below keep their exact row sets.
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, '0p{i}')", 100 + i))
                .expect("pad");
        }
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

        // An NVARCHAR range SEEKS since the keys became collation sort keys
        // (#94): sort-key byte order IS the filter's compare order, so the old
        // UTF-16BE divergence that forced a scan no longer exists.
        // Case-insensitive: 'ABC' folds with 'abc' > 'a', so all three match.
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE name > 'a'");
        assert!(
            plan.iter().any(|l| l.contains("Index Seek")),
            "NVARCHAR ranges seek over sort keys: {plan:?}"
        );
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
        // Pad past the tiny-table tie-break; '0...' sorts below 'b', so the
        // range assertion below keeps its exact row set.
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, '0c{i}')", 100 + i))
                .expect("pad");
        }
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
    fn sql_range_seek_follows_linguistic_order_not_code_points() {
        // Under the default collation, accented letters sort next to their
        // base letter ('å' < 'b') while their UTF-8 bytes sort past 'z'. The
        // index keys are collation SORT KEYS, so a range seek's bounds agree
        // with the filter — a code-point-keyed index would exclude 'å'/'ä'
        // from `w < 'b'` and silently drop matching rows.
        let path = unique_temp_path("sql-index-locale-range");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, w VARCHAR(20))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1,'a'),(2,'å'),(3,'ä'),(4,'b'),(5,'z')")
            .expect("insert");
        // Pad past the tiny-table tie-break; 'z...' sorts above 'b' in every
        // collation involved, so the range's row set is untouched.
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, 'zz{i}')", 100 + i))
                .expect("pad");
        }
        engine.execute("CREATE INDEX ix_w ON t (w)").expect("index");

        let q = "SELECT id FROM t WHERE w < 'b' ORDER BY id";
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE w < 'b'");
        assert!(
            plan.iter().any(|l| l.contains("Index Seek")),
            "the range seeks: {plan:?}"
        );
        let (_, seeked) = sql_rows(&engine, q);
        assert_eq!(
            seeked,
            vec![
                vec![Some("1".into())],
                vec![Some("2".into())],
                vec![Some("3".into())]
            ],
            "'å' and 'ä' sort below 'b' linguistically and the seek keeps them"
        );

        // A/B: the scan agrees.
        engine.execute("DROP INDEX ix_w ON t").expect("drop");
        let (_, scanned) = sql_rows(&engine, q);
        assert_eq!(scanned, seeked, "seek == scan");
        let _ = std::fs::remove_file(path);
    }

    /// A/B (seek vs scan) equality for character range seeks
    /// across collations, with supplementary-plane characters, empty strings
    /// and NULLs in both the stored data and the bounds.
    #[test]
    fn character_range_seeks_match_scans_across_collations() {
        let values = [
            "a",
            "A",
            "b",
            "z",
            "Z",
            "å",
            "ä",
            "ö",
            "é",
            "e",
            "aa",
            "ab",
            "",
            "z\u{1F600}",
            "a\u{1F600}",
            "\u{1F600}",
            "\u{1F600}a",
            "\u{20000}",
            "\u{10000}",
            "\u{E000}",
            "\u{FFFD}",
            "\u{10FFFF}",
        ];
        let bounds = ["a", "å", "b", "z", "\u{1F600}", "\u{E000}", "\u{20000}", ""];
        let ops = [">", ">=", "<", "<="];
        for (label, decl) in [
            ("nv-default", "NVARCHAR(40)"),
            ("nv-cs", "NVARCHAR(40) COLLATE Latin1_General_CS_AS"),
            ("nv-ai", "NVARCHAR(40) COLLATE Latin1_General_CI_AI"),
            ("nv-bin2", "NVARCHAR(40) COLLATE Latin1_General_BIN2"),
            ("nv-sv", "NVARCHAR(40) COLLATE Finnish_Swedish_CI_AS"),
            ("vc-default", "VARCHAR(40)"),
            ("vc-bin2", "VARCHAR(40) COLLATE Latin1_General_BIN2"),
        ] {
            let path = unique_temp_path(&format!("probe-ab-{label}"));
            let engine = new_engine(&path);
            engine
                .execute(&format!(
                    "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, w {decl})"
                ))
                .expect("create");
            for (i, v) in values.iter().enumerate() {
                // BIN2/CS keep 'a'/'A' distinct; CI collations make some
                // values duplicate keys — allowed (non-unique index).
                engine
                    .execute(&format!("INSERT INTO t VALUES ({i}, '{v}')"))
                    .expect("insert");
            }
            // NULLs and padding past the tiny-table tie-break.
            for i in 0..12 {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({}, NULL)", 100 + i))
                    .expect("insert null");
            }
            engine.execute("CREATE INDEX ix_w ON t (w)").expect("index");
            let mut queries = Vec::new();
            for b in bounds {
                for op in ops {
                    queries.push(format!("SELECT id FROM t WHERE w {op} '{b}' ORDER BY id"));
                }
            }
            let mut with_index = Vec::new();
            for q in &queries {
                let plan = plan_lines(&engine, q.strip_suffix(" ORDER BY id").unwrap());
                assert!(
                    plan.iter().any(|l| l.contains("Index Seek")),
                    "{label}: expected seek for {q}: {plan:?}"
                );
                with_index.push(sql_rows(&engine, q).1);
            }
            engine.execute("DROP INDEX ix_w ON t").expect("drop");
            for (q, seeked) in queries.iter().zip(with_index) {
                let scanned = sql_rows(&engine, q).1;
                assert_eq!(scanned, seeked, "{label}: seek != scan for {q}");
            }
            let _ = std::fs::remove_file(path);
        }
    }

    /// Composite (eq prefix + NVARCHAR range) and DESC-column
    /// bounds, exercising prefix_upper_bound's carry over inverted bytes.
    #[test]
    fn composite_and_desc_index_bounds_match_scans() {
        let path = unique_temp_path("probe-composite-desc");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k INT, w NVARCHAR(40))")
            .expect("create");
        let values = [
            "a",
            "b",
            "z",
            "å",
            "ä",
            "\u{1F600}",
            "z\u{1F600}",
            "\u{20000}",
            "\u{E000}",
            "aa",
        ];
        let mut id = 0;
        for k in [1, 2, 3] {
            for v in values {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({id}, {k}, '{v}')"))
                    .expect("insert");
                id += 1;
            }
        }
        engine
            .execute("CREATE INDEX ix_kw ON t (k, w)")
            .expect("index");
        let mut queries = Vec::new();
        for b in ["å", "b", "\u{1F600}", "z"] {
            for op in [">", ">=", "<", "<="] {
                queries.push(format!(
                    "SELECT id FROM t WHERE k = 2 AND w {op} '{b}' ORDER BY id"
                ));
            }
        }
        // Equality-only on k too (prefix_upper_bound over the eq prefix).
        queries.push("SELECT id FROM t WHERE k = 2 ORDER BY id".to_string());
        let mut with_index = Vec::new();
        for q in &queries {
            let plan = plan_lines(&engine, q.strip_suffix(" ORDER BY id").unwrap());
            assert!(
                plan.iter().any(|l| l.contains("Index Seek")),
                "expected seek for {q}: {plan:?}"
            );
            with_index.push(sql_rows(&engine, q).1);
        }
        engine.execute("DROP INDEX ix_kw ON t").expect("drop");
        for (q, seeked) in queries.iter().zip(with_index) {
            let scanned = sql_rows(&engine, q).1;
            assert_eq!(scanned, seeked, "composite: seek != scan for {q}");
        }

        // DESC index: a range must NOT seek (bounds are not inverted), an
        // equality must seek correctly through inverted-byte bounds
        // (prefix_upper_bound's 0xFF carry path).
        engine
            .execute("CREATE INDEX ix_wd ON t (w DESC)")
            .expect("desc index");
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE w < 'b'");
        assert!(
            plan.iter().all(|l| !l.contains("Index Seek")),
            "a DESC column must not back a range seek: {plan:?}"
        );
        let q = "SELECT id FROM t WHERE w = 'å' ORDER BY id";
        let plan = plan_lines(&engine, "SELECT id FROM t WHERE w = 'å'");
        assert!(
            plan.iter().any(|l| l.contains("Index Seek")),
            "DESC equality seeks: {plan:?}"
        );
        let seeked = sql_rows(&engine, q).1;
        engine.execute("DROP INDEX ix_wd ON t").expect("drop");
        assert_eq!(sql_rows(&engine, q).1, seeked, "desc eq: seek != scan");
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
            // Pad past the tiny-table tie-break (an empty table plans as a scan).
            for i in 0..20 {
                engine
                    .execute(&format!("INSERT INTO {t} VALUES ({}, 900)", 100 + i))
                    .expect("pad");
            }
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
            // Pad past the tiny-table tie-break (a <= 16-row table scans).
            for i in 0..20 {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({}, '0p{i}')", 100 + i))
                    .expect("pad");
            }
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
    fn all_null_groups_aggregate_identically_through_the_spill_path() {
        // A group whose every value is NULL: SUM/MIN/MAX/AVG NULL, COUNT(col)
        // 0, COUNT(*) counts rows — and the grace-hash spill path must answer
        // exactly as the in-memory path does (an all-NULL group must not read
        // as "no group" after partitioning).
        let path = unique_temp_path("all-null-spill");
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k VARCHAR(10), v INT)")
            .expect("create");
        // 100 all-NULL rows in group 'a', 100 mixed rows in group 'b'.
        for i in 0..100 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, 'a', NULL)"))
                .expect("insert a");
            let v = if i % 2 == 0 {
                "NULL".to_string()
            } else {
                i.to_string()
            };
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, 'b', {v})", 100 + i))
                .expect("insert b");
        }
        let query = "SELECT k, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v)                      FROM t GROUP BY k ORDER BY k";
        let (_, in_memory) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(Some(400));
        let (_, spilled) = sql_rows(&engine, query);
        crate::rel::set_test_sort_budget(None);
        assert_eq!(spilled, in_memory, "spill path changes all-NULL groups");
        assert_eq!(
            in_memory[0],
            vec![
                Some("a".into()),
                Some("100".into()),
                Some("0".into()),
                None,
                None,
                None,
                None
            ],
            "all-NULL group: COUNT(*) counts, everything else is NULL/0"
        );
        let _ = std::fs::remove_file(path);
    }

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
        // A wide table, where projection pruning has something to prune.
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE wd (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), c INT, d VARCHAR(20), e INT NULL)",
        );
        for i in 0..200 {
            batch(
                &engine,
                &mut ctx,
                &format!(
                    "INSERT INTO wd VALUES ({i}, 'a{i}', 'b{i}', {i}, 'd{i}', {})",
                    if i % 4 == 0 {
                        "NULL".into()
                    } else {
                        i.to_string()
                    }
                ),
            );
        }

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
            // Projection pruning: the scan decodes only what the query reads,
            // so a WHERE on a column that is *not* projected must still keep
            // that column — these are the shapes that catch a pruned-away
            // predicate column.
            "SELECT id FROM wd",
            "SELECT id FROM wd WHERE a = 'a7'",
            "SELECT a FROM wd WHERE c > 100",
            "SELECT id, d FROM wd WHERE b = 'b3' AND e IS NULL",
            "SELECT e FROM wd WHERE e IS NOT NULL",
            "SELECT id FROM wd WHERE CASE WHEN c > 100 THEN a ELSE d END = 'a150'",
            "SELECT id FROM wd WHERE a LIKE 'a1%'",
            "SELECT id FROM wd WHERE c IN (1, 2, 3)",
            "SELECT id FROM wd WHERE c BETWEEN 10 AND 12",
            "SELECT id FROM wd WHERE LEN(a) > 3",
            "SELECT d, c, a FROM wd WHERE id = 5",
            "SELECT * FROM wd WHERE c = 5",
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
    fn a_scan_decodes_only_the_columns_the_query_reads() {
        // The rows returned are identical whether or not the projection is
        // pruned, so nothing about the result can see this — the width the scan
        // asked for is the only observable.
        let path = unique_temp_path("scan-prune");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE w (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), c INT)",
        );
        for i in 0..50 {
            batch(
                &engine,
                &mut ctx,
                &format!("INSERT INTO w VALUES ({i}, 'a{i}', 'b{i}', {i})"),
            );
        }

        for (query, expected, why) in [
            ("SELECT id FROM w", 1, "one projected column"),
            ("SELECT id, c FROM w", 2, "two projected columns"),
            ("SELECT * FROM w", 4, "a wildcard needs every column"),
            // The WHERE's columns are read even when nothing projects them.
            (
                "SELECT id FROM w WHERE a = 'a1'",
                2,
                "id + the predicate's a",
            ),
            (
                "SELECT id FROM w WHERE a = 'a1' AND c > 2",
                3,
                "id + both predicate columns",
            ),
            // A column named twice costs one decode.
            ("SELECT id, id FROM w WHERE id > 1", 1, "id, deduped"),
            // A predicate column that is also projected is not counted twice.
            ("SELECT a FROM w WHERE a = 'a1'", 1, "a, deduped"),
        ] {
            let out = batch(&engine, &mut ctx, query);
            assert!(out.error.is_none(), "{query}: {:?}", out.error);
            assert_eq!(
                engine.storage.last_scan_width(),
                expected,
                "{query} should decode {expected} columns ({why})"
            );
        }

        // The counter reports the whole row when nothing prunes, so the numbers
        // above are a pruned width and not a stuck reading.
        crate::rel::without_scan_path(|| batch(&engine, &mut ctx, "SELECT id FROM w"));
        assert_eq!(
            engine.storage.last_scan_width(),
            usize::MAX,
            "the collecting path decodes the whole row"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn pruning_carries_each_kept_column_its_own_type_and_collation() {
        // `needed` renumbers the columns, so `types` and `collations` are
        // rebuilt in the scanned row's coordinates. Both are silent when wrong:
        // a misindexed type mis-restores a DECIMAL's scale, and a misindexed
        // collation makes a _CS column compare case-insensitively. The columns
        // that matter sit at HIGH schema indices and are projected/filtered from
        // LOW ones, so a rebuild that kept the schema's numbering reads the
        // wrong entry rather than coincidentally the right one.
        let path = unique_temp_path("scan-prune-types");
        let engine = new_engine(&path);
        let mut ctx = TxnContext::default();
        batch(
            &engine,
            &mut ctx,
            "CREATE TABLE tc (id INT PRIMARY KEY, pad1 VARCHAR(10), pad2 VARCHAR(10), \
             amount DECIMAL(10,4), cs VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CS_AS, \
             ci VARCHAR(20))",
        );
        batch(
            &engine,
            &mut ctx,
            "INSERT INTO tc VALUES (1, 'p', 'q', 12.3456, 'Match', 'Match')",
        );
        batch(
            &engine,
            &mut ctx,
            "INSERT INTO tc VALUES (2, 'p', 'q', 0.5000, 'other', 'other')",
        );

        // A _CS column is exact; a default (_CI) one is not. Both are read via
        // the WHERE only, so both live at a remapped position.
        let out = batch(&engine, &mut ctx, "SELECT id FROM tc WHERE cs = 'MATCH'");
        assert!(out.error.is_none(), "{:?}", out.error);
        assert!(
            first_rowset(&out).rows.is_empty(),
            "a _CS column must not match a different casing"
        );
        let out = batch(&engine, &mut ctx, "SELECT id FROM tc WHERE cs = 'Match'");
        assert_eq!(
            first_rowset(&out).rows.len(),
            1,
            "_CS matches its own casing"
        );
        let out = batch(&engine, &mut ctx, "SELECT id FROM tc WHERE ci = 'MATCH'");
        assert_eq!(
            first_rowset(&out).rows.len(),
            1,
            "the default collation is case-insensitive"
        );

        // The DECIMAL's scale survives the `types` remap. It has to be read
        // through the WHERE to test that: `datum_to_sql` consults the column
        // type for a DECIMAL's precision/scale and for nothing else, so this is
        // the only shape a misindexed `types` is visible in. (Asserting the
        // *output* column's type would prove nothing — that comes from
        // `plan.columns`, which is not remapped.) Read at position 1 of
        // [id, amount] while the schema puts it at 3, so a rebuild that kept the
        // schema's numbering finds `pad1` and falls back to scale 0 — turning
        // 12.3456 into 123456.
        let out = batch(
            &engine,
            &mut ctx,
            "SELECT id FROM tc WHERE amount = 12.3456",
        );
        assert!(out.error.is_none(), "{:?}", out.error);
        assert_eq!(
            first_rowset(&out).rows.len(),
            1,
            "a DECIMAL in the WHERE keeps its scale through the remap"
        );
        let out = batch(&engine, &mut ctx, "SELECT amount FROM tc WHERE id = 1");
        assert_eq!(
            first_rowset(&out).columns[0].column_type,
            crate::relstore::types::ColumnType::Decimal {
                precision: 10,
                scale: 4
            },
            "the projected column keeps its schema type"
        );

        // And every one of these agrees with the collecting path, which reads
        // the whole row and so cannot be remapped wrong.
        for query in [
            "SELECT id FROM tc WHERE cs = 'MATCH'",
            "SELECT id FROM tc WHERE cs = 'Match'",
            "SELECT id FROM tc WHERE ci = 'MATCH'",
            "SELECT amount FROM tc WHERE id = 1",
            "SELECT id FROM tc WHERE amount = 12.3456",
            "SELECT amount, cs FROM tc WHERE ci = 'match'",
        ] {
            let streamed = batch(&engine, &mut ctx, query);
            let collected = crate::rel::without_scan_path(|| batch(&engine, &mut ctx, query));
            assert_eq!(
                first_rowset(&streamed),
                first_rowset(&collected),
                "{query} differs between the pruned scan and the collecting path"
            );
        }
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
