mod relational;
mod search;

pub(crate) use relational::collation;
#[cfg(test)]
pub(crate) use relational::execute_batch_with_params;
pub use relational::{
    BatchEmitter, BatchOutcome, DoneCommand, FATAL_SEVERITY, Isolation, ResultColumn, RowSet,
    RpcParam, SqlStatement, StatementResult, TxnContext, render_cell,
};
pub(crate) use relational::{
    CancelScope, Collector, analyze_locks, decl_names, describe_first_result_set, execute_batch,
    execute_batch_streamed,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use relational::{
    clear_test_cancel, execute, set_test_cancel, set_test_sort_budget, without_scan_path,
};

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::storage::{Storage, StorageError};
use search::{Document, EngineState, FieldType, IndexState, SearchQuery, value_type_name};

const ENGINE_WAL_ENTRY_VERSION: u16 = 1;
const ENGINE_WAL_ENTRY_TYPE: u16 = 1;
const WAL_CHECKPOINT_THRESHOLD: f64 = 0.75;
/// A standby takes a restartpoint earlier than a primary checkpoints (0.5 vs
/// 0.75): its reclaim is capped at the shipped undo floor, so starting earlier
/// leaves headroom when the floor lags.
const STANDBY_RESTARTPOINT_THRESHOLD: f64 = 0.5;

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
        // back anything an unbalanced BEGIN leaves dangling. It runs in the
        // default database under its canonical name, so DB_NAME() and USE
        // behave over the CLI (a USE lasts only for this command's batch —
        // the context is transient by design).
        let mut txn_ctx = crate::engine::TxnContext::default();
        txn_ctx.set_current_database(
            self.storage.default_database_name(),
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
        );
        let outcome = crate::engine::execute_batch(&self.storage, input, &mut txn_ctx);
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
        txn_ctx: &mut crate::engine::TxnContext,
    ) -> Result<crate::engine::BatchOutcome, EngineError> {
        self.sql_batch_with_params(input, txn_ctx, &[])
    }

    /// Runs a SQL batch with `sp_executesql` parameters seeded as batch
    /// variables (see [`crate::engine::execute_batch_with_params`]).
    pub fn sql_batch_with_params(
        &self,
        input: &str,
        txn_ctx: &mut crate::engine::TxnContext,
        params: &[crate::engine::RpcParam],
    ) -> Result<crate::engine::BatchOutcome, EngineError> {
        let mut collector = crate::engine::Collector::default();
        let error = self.sql_batch_streamed(input, txn_ctx, params, &mut collector)?;
        Ok(collector.into_outcome(error))
    }

    /// `sp_describe_first_result_set`: statically-derivable column metadata
    /// for `tsql`'s first result set, without executing anything.
    pub fn describe_first_result_set(
        &self,
        tsql: &str,
    ) -> Result<crate::engine::RowSet, truthdb_sql::error::SqlError> {
        let _meta = self.meta.read().expect("engine meta poisoned");
        crate::engine::describe_first_result_set(&self.storage, tsql)
    }

    /// Like [`Self::sql_batch_with_params`], but each statement's result
    /// leaves through `emitter` as it is produced (see
    /// [`crate::engine::execute_batch_streamed`]). Returns the batch's terminal
    /// error, which the caller reports after the statement events.
    pub fn sql_batch_streamed(
        &self,
        input: &str,
        txn_ctx: &mut crate::engine::TxnContext,
        params: &[crate::engine::RpcParam],
        emitter: &mut dyn crate::engine::BatchEmitter,
    ) -> Result<Option<truthdb_sql::error::SqlError>, EngineError> {
        // Hold the execution gate shared for the whole batch: concurrent
        // relational batches run together, but a native writer is excluded (see
        // [`Engine`]). The guard also gives the checkpointer its `meta` read.
        let meta = self.meta.read().expect("engine meta poisoned");
        let error =
            crate::engine::execute_batch_streamed(&self.storage, input, txn_ctx, params, emitter);
        self.maybe_checkpoint(&meta)?;
        Ok(error)
    }

    /// Rolls back and discards a session's open transaction (connection
    /// teardown). No-op when the session has no transaction.
    pub fn abort_session_txn(&self, txn_ctx: &mut crate::engine::TxnContext) {
        txn_ctx.abort(&self.storage);
    }

    /// Rolls back a transaction the idle reaper is reclaiming. Unlike
    /// [`Self::abort_session_txn`] the session lives on, so the rollback is
    /// recorded and reported to its next batch.
    pub fn abort_idle_session_txn(&self, txn_ctx: &mut crate::engine::TxnContext) {
        txn_ctx.abort_idle(&self.storage);
    }

    /// The table/database locks a SQL batch needs at the given isolation
    /// level (see [`crate::engine::analyze_locks`]). The session loop acquires
    /// these before running the batch. `db_id` is the session's current
    /// database — the same namespace execution will resolve names in; the two
    /// derivations must agree or a batch under-locks.
    pub fn analyze_locks(
        &self,
        db_id: u32,
        input: &str,
        isolation: crate::engine::Isolation,
    ) -> Vec<(crate::lock::Resource, crate::lock::LockMode)> {
        crate::engine::analyze_locks(&self.storage, db_id, input, isolation)
    }

    /// Stamps the default database's name (id 1) from the instance
    /// configuration, refusing a name a stored `CREATE DATABASE` row already
    /// uses. Called once at startup, before any session opens.
    pub fn set_default_database(&self, name: &str) -> Result<(), EngineError> {
        self.storage.set_default_database_name(name)?;
        Ok(())
    }

    /// Resolves a database name to `(id, canonical name)` — the login-time
    /// derivation, shared with USE via the same storage lookup.
    pub fn resolve_database(&self, name: &str) -> Option<(u32, String)> {
        let requested = if name.is_empty() {
            self.storage.default_database_name()
        } else {
            name.to_string()
        };
        let id = self.storage.rel_database_id_by_name(&requested)?;
        self.storage
            .rel_databases()
            .into_iter()
            .find(|(db, _)| *db == id)
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

    /// A standby's checkpoint-equivalent (see
    /// [`Storage::standby_restartpoint`]): reclaims WAL ring space up to the
    /// standby's own undo floor. Storage-only — the live search meta is NOT
    /// refreshed (a standby's search reads reflect the seed until a reopen or
    /// promotion; the restartpoint never truncates a search record the reopen
    /// replay needs).
    pub fn standby_restartpoint(&self) -> Result<bool, EngineError> {
        Ok(self.storage.standby_restartpoint()?)
    }

    /// The maintenance-thread trigger: take a restartpoint once the ring is
    /// half full. Cheap when this is not a standby or the ring is quiet.
    pub(crate) fn standby_restartpoint_if_needed(&self) -> Result<bool, EngineError> {
        if !self.storage.is_standby() || self.wal_usage_ratio() < STANDBY_RESTARTPOINT_THRESHOLD {
            return Ok(false);
        }
        self.standby_restartpoint()
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

/// Renders a SQL batch outcome (statement results + an optional trailing
/// error) as the `{"kind":"sql",...}` envelope the CLI turns into aligned
/// tables, `(N rows affected)` lines, and `Msg <n>` errors.
fn render_sql_outcome(outcome: &crate::engine::BatchOutcome) -> String {
    use crate::engine::StatementResult;
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
                                    match crate::engine::render_cell(datum, &column.column_type) {
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
mod tests;
