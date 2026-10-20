pub use truthdb_relational::engine::*;
pub use truthdb_search::{
    Command, CommandError, ENGINE_WAL_ENTRY_TYPE, ENGINE_WAL_ENTRY_VERSION, EngineMeta,
    EngineState, SearchError, SearchQuery, WalEvent, decode_snapshot, parse_command, render_json,
};
#[cfg(test)]
pub use truthdb_search::{Document, FieldType, IndexState};

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde_json::{Value, json};

use thiserror::Error;
use truthdb_relational::storage::{RelStorage, StorageError};

const WAL_CHECKPOINT_THRESHOLD: f64 = 0.75;
/// A standby takes a restartpoint earlier than a primary checkpoints (0.5 vs
/// 0.75): its reclaim is capped at the shipped undo floor, so starting earlier
/// leaves headroom when the floor lags.
const STANDBY_RESTARTPOINT_THRESHOLD: f64 = 0.5;

/// An S3 administration command: `s3 create account <name>`, `s3 list
/// accounts`, `s3 drop account <id>`, `s3 create user <account> <name>`,
/// `s3 drop user <account> <name>`, `s3 put user policy <account> <user>
/// <name> <json>`, `s3 delete user policy <account> <user> <name>`, `s3 list
/// user policies <account> <user>`, `s3 create access key [<account>
/// [<user>]]`, `s3 drop access key <id>`, `s3 list access keys`, `s3 enable
/// fulltext <bucket> [max_bytes]`, `s3 disable fulltext <bucket>`.
///
/// Over TDS it runs only for a `sysadmin` login. The native protocol is a
/// trusted connection, so there it runs with full privileges and no
/// authentication: whoever can reach the native port can mint, list and drop
/// S3 accounts and credentials (and a new secret is returned in cleartext
/// over that connection).
enum S3Admin {
    CreateAccount(String),
    ListAccounts,
    DropAccount(String),
    CreateUser(String, String),
    DropUser(String, String),
    PutUserPolicy {
        account: String,
        user: String,
        name: String,
        document: String,
    },
    DeleteUserPolicy(String, String, String),
    ListUserPolicies(String, String),
    CreateAccessKey(Option<String>, Option<String>),
    DropAccessKey(String),
    ListAccessKeys,
    EnableFulltext(String, Option<String>),
    DisableFulltext(String),
}

fn parse_s3_admin(input: &str) -> Option<S3Admin> {
    // The CLI ends every command with `;`.
    let input = input.trim().trim_end_matches(';');
    let words: Vec<&str> = input.split_whitespace().collect();
    let lower: Vec<String> = words.iter().map(|word| word.to_ascii_lowercase()).collect();
    let lower: Vec<&str> = lower.iter().map(String::as_str).collect();
    match lower.as_slice() {
        ["s3", "create", "account", _] => Some(S3Admin::CreateAccount(words[3].to_string())),
        ["s3", "list", "accounts"] => Some(S3Admin::ListAccounts),
        ["s3", "drop", "account", _] => Some(S3Admin::DropAccount(words[3].to_string())),
        ["s3", "create", "user", _, _] => Some(S3Admin::CreateUser(
            words[3].to_string(),
            words[4].to_string(),
        )),
        ["s3", "drop", "user", _, _] => Some(S3Admin::DropUser(
            words[3].to_string(),
            words[4].to_string(),
        )),
        ["s3", "put", "user", "policy", _, _, _, _, ..] => Some(S3Admin::PutUserPolicy {
            account: words[4].to_string(),
            user: words[5].to_string(),
            name: words[6].to_string(),
            document: after_words(input, 7).to_string(),
        }),
        ["s3", "delete", "user", "policy", _, _, _] => Some(S3Admin::DeleteUserPolicy(
            words[4].to_string(),
            words[5].to_string(),
            words[6].to_string(),
        )),
        ["s3", "list", "user", "policies", _, _] => Some(S3Admin::ListUserPolicies(
            words[4].to_string(),
            words[5].to_string(),
        )),
        ["s3", "create", "access", "key"] => Some(S3Admin::CreateAccessKey(None, None)),
        ["s3", "create", "access", "key", _] => {
            Some(S3Admin::CreateAccessKey(Some(words[4].to_string()), None))
        }
        ["s3", "create", "access", "key", _, _] => Some(S3Admin::CreateAccessKey(
            Some(words[4].to_string()),
            Some(words[5].to_string()),
        )),
        ["s3", "drop", "access", "key", _] => Some(S3Admin::DropAccessKey(words[4].to_string())),
        ["s3", "list", "access", "keys"] => Some(S3Admin::ListAccessKeys),
        ["s3", "enable", "fulltext", _] => {
            Some(S3Admin::EnableFulltext(words[3].to_string(), None))
        }
        ["s3", "enable", "fulltext", _, _] => Some(S3Admin::EnableFulltext(
            words[3].to_string(),
            Some(words[4].to_string()),
        )),
        ["s3", "disable", "fulltext", _] => Some(S3Admin::DisableFulltext(words[3].to_string())),
        _ => None,
    }
}

fn fulltext_error(bucket: &str, err: truthdb_object::FtError) -> EngineError {
    use truthdb_object::FtError;
    CommandError::InvalidCommand(match err {
        FtError::NoSuchBucket => format!("no bucket '{bucket}'"),
        FtError::NotEnabled => format!("full-text search is not enabled on bucket '{bucket}'"),
        FtError::Purging => format!(
            "bucket '{bucket}' is still removing its previous full-text index; try again shortly"
        ),
    })
    .into()
}

/// `input` after its first `count` words, as written (a policy document
/// keeps its spacing).
fn after_words(input: &str, count: usize) -> &str {
    let mut rest = input;
    for _ in 0..count {
        rest = rest.trim_start();
        rest = &rest[rest.find(char::is_whitespace).unwrap_or(rest.len())..];
    }
    rest.trim()
}

fn account_error(err: truthdb_object::AccountError) -> EngineError {
    use truthdb_object::AccountError;
    CommandError::InvalidCommand(match err {
        AccountError::NoSuchAccount => "no such account".to_string(),
        AccountError::AccountExists => "the account already exists".to_string(),
        AccountError::DefaultAccount => "the default account cannot be dropped".to_string(),
        AccountError::NoSuchUser => "no such user".to_string(),
        AccountError::UserExists => "the user already exists".to_string(),
        AccountError::NoSuchPolicy => "no such policy".to_string(),
        AccountError::PolicyExists => "the policy already exists".to_string(),
        AccountError::NotEmpty(what) => format!("drop its {what} first"),
        AccountError::InvalidName(why) => format!("the name {why}"),
    })
    .into()
}

/// A fresh 12-digit account id, drawn by the system's secure random
/// generator.
fn random_account_id() -> Result<String, EngineError> {
    use ring::rand::SecureRandom;
    let mut digits = String::new();
    while digits.len() < 12 {
        let mut bytes = [0u8; 16];
        ring::rand::SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| {
                EngineError::Storage(
                    std::io::Error::other("the system random generator failed").into(),
                )
            })?;
        // 250 is a multiple of 10: bytes below it map to digits without bias.
        digits.extend(
            bytes
                .iter()
                .filter(|byte| **byte < 250)
                .map(|byte| char::from(b'0' + byte % 10)),
        );
    }
    digits.truncate(12);
    Ok(digits)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

const ACCESS_KEY_ID_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const SECRET_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `len` characters drawn uniformly from `alphabet` (32 or 64 symbols, so a
/// random byte maps without bias) by the system's secure random generator.
fn random_text(len: usize, alphabet: &[u8]) -> Result<String, EngineError> {
    use ring::rand::SecureRandom;
    debug_assert!(alphabet.len() == 32 || alphabet.len() == 64);
    let mut bytes = vec![0u8; len];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| {
            EngineError::Storage(std::io::Error::other("the system random generator failed").into())
        })?;
    Ok(bytes
        .iter()
        .map(|byte| alphabet[*byte as usize % alphabet.len()] as char)
        .collect())
}

/// Object writes checkpoint at half the ring: a segment record is about 1 MiB,
/// and writes may only fill the ring up to its compensation reserve (a
/// quarter), so waiting for [`WAL_CHECKPOINT_THRESHOLD`] would run it full.
const OBJECT_CHECKPOINT_THRESHOLD: f64 = 0.5;

/// Budgeted transactions one maintenance sweep spends purging deleted
/// vector indexes at most.
const VECTOR_PURGE_STEPS: usize = 16;

/// Object bodies garbage collection frees per storage-lock hold.
const OBJECT_GC_BATCH: u64 = 8;

/// Object bodies one maintenance sweep frees at most.
const OBJECT_GC_SWEEP_LIMIT: u64 = 512;

/// A staging object body untouched for this long is an abandoned upload, and
/// garbage collection reclaims it.
const OBJECT_ABANDON_AFTER_MS: u64 = 60 * 60 * 1000;

/// The change journal (`sys.s3_journal`) keeps this much history.
const OBJECT_JOURNAL_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Journal rows one trimming transaction deletes at most.
const OBJECT_JOURNAL_TRIM_BATCH: usize = 512;

/// A multipart upload left open this long is abandoned: garbage collection
/// aborts it, releasing its parts (as an S3 lifecycle rule would).
const OBJECT_UPLOAD_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1000;

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
    storage: Arc<RelStorage>,
    meta: RwLock<EngineMeta>,
}

impl Engine {
    pub fn new(storage: RelStorage) -> Result<Self, EngineError> {
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

        // The object store's inventory, readable from SQL (`sys.s3_*`).
        storage.set_external_rowsets(Arc::new(s3_sql::S3Rowsets {
            core: storage.core_arc(),
        }));
        Ok(Engine {
            storage: Arc::new(storage),
            meta: RwLock::new(meta),
        })
    }

    /// A shared handle to the underlying storage, for subsystems that run
    /// beside the engine's worker pool — the replication listener/sender on a
    /// primary and the replication receiver on a standby. `Storage` is
    /// internally synchronized, so the tasks and the workers share it safely.
    pub fn storage_arc(&self) -> Arc<RelStorage> {
        Arc::clone(&self.storage)
    }

    /// The underlying storage handle, so a test can drive an online backup
    /// while the engine is live.
    #[cfg(test)]
    pub(crate) fn storage(&self) -> &RelStorage {
        &self.storage
    }

    pub fn execute(&self, input: &str) -> Result<String, EngineError> {
        if let Some(command) = parse_s3_admin(input) {
            return self.execute_s3_admin(command);
        }
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
        .map_err(EngineError::from)
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
                .map_err(EngineError::from)
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
                .map_err(EngineError::from)
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
        if let Some(command) = parse_s3_admin(input) {
            return Ok(self.s3_admin_batch(command, txn_ctx, emitter));
        }
        // Hold the execution gate shared for the whole batch: concurrent
        // relational batches run together, but a native writer is excluded (see
        // [`Engine`]). The guard also gives the checkpointer its `meta` read.
        let meta = self.meta.read().expect("engine meta poisoned");
        let error =
            crate::engine::execute_batch_streamed(&self.storage, input, txn_ctx, params, emitter);
        self.maybe_checkpoint(&meta)?;
        Ok(error)
    }

    /// An S3 administration command sent as a SQL batch: run for a
    /// `sysadmin` login only, its JSON reply returned as one row of one
    /// `result` column.
    fn s3_admin_batch(
        &self,
        command: S3Admin,
        txn_ctx: &mut crate::engine::TxnContext,
        emitter: &mut dyn crate::engine::BatchEmitter,
    ) -> Option<truthdb_sql::error::SqlError> {
        use truthdb_sql::error::SqlError;
        if !txn_ctx.is_server_admin(&self.storage) {
            return Some(SqlError::new(
                15247,
                16,
                1,
                "User does not have permission to perform this action.".to_string(),
            ));
        }
        match self.execute_s3_admin(command) {
            Ok(reply) => {
                emitter.columns(vec![crate::engine::ResultColumn {
                    name: "result".to_string(),
                    column_type: truthdb_relational::relstore::types::ColumnType::NVarCharMax,
                }]);
                emitter.rows(vec![vec![
                    truthdb_relational::relstore::types::Datum::NVarChar(reply),
                ]]);
                emitter.statement_done(
                    Some(1),
                    txn_ctx.in_transaction(),
                    crate::engine::DoneCommand::Select,
                );
                None
            }
            Err(err) => Some(SqlError::new(50000, 16, 1, err.to_string())),
        }
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

    /// The S3 API's account and access-key administration.
    fn execute_s3_admin(&self, command: S3Admin) -> Result<String, EngineError> {
        let rendered = match command {
            S3Admin::CreateAccount(name) => {
                // A drawn id may be taken (or be the default account's): draw
                // again.
                let mut attempts = 0;
                let id = loop {
                    let id = random_account_id()?;
                    match self.object(|file| {
                        file.transaction(|tx| tx.create_account(&id, &name, now_ms()))
                    })? {
                        Ok(()) => break id,
                        Err(truthdb_object::AccountError::AccountExists) if attempts < 8 => {
                            attempts += 1;
                        }
                        Err(err) => return Err(account_error(err)),
                    }
                };
                json!({ "account_id": id, "name": name })
            }
            S3Admin::ListAccounts => {
                let accounts = self.object(|file| file.accounts())?;
                json!({ "accounts": accounts.iter().map(|account| json!({
                    "account_id": account.id,
                    "name": account.name,
                })).collect::<Vec<_>>() })
            }
            S3Admin::DropAccount(id) => {
                // The vector engine's buckets are checked first; the object
                // engine checks its own, its users and keys in the drop. With
                // no access keys left, no request can create a bucket for it.
                let mut after: Option<String> = None;
                loop {
                    let page = self.vector(|file| file.list_buckets(after.as_deref(), 500))?;
                    if page
                        .iter()
                        .any(|bucket| bucket.owner.as_deref() == Some(id.as_str()))
                    {
                        return Err(account_error(truthdb_object::AccountError::NotEmpty(
                            "vector buckets",
                        )));
                    }
                    match page.last() {
                        Some(last) if page.len() == 500 => after = Some(last.name.clone()),
                        _ => break,
                    }
                }
                self.object(|file| file.transaction(|tx| tx.drop_account(&id)))?
                    .map_err(account_error)?;
                json!({ "dropped": id })
            }
            S3Admin::CreateUser(account, name) => {
                let user_id = format!("AIDA{}", random_text(17, ACCESS_KEY_ID_ALPHABET)?);
                self.object(|file| {
                    file.transaction(|tx| tx.create_user(&account, &name, &user_id, now_ms()))
                })?
                .map_err(account_error)?;
                json!({ "account_id": account, "user_name": name, "user_id": user_id })
            }
            S3Admin::DropUser(account, name) => {
                self.object(|file| file.transaction(|tx| tx.drop_user(&account, &name)))?
                    .map_err(account_error)?;
                json!({ "dropped": name })
            }
            S3Admin::PutUserPolicy {
                account,
                user,
                name,
                document,
            } => {
                truthdb_iam::Policy::parse(&document, truthdb_iam::PolicyKind::UserInline)
                    .map_err(|err| {
                        CommandError::InvalidCommand(format!("malformed policy: {}", err.0))
                    })?;
                self.object(|file| {
                    file.transaction(|tx| {
                        if !tx.user_exists(&account, &user)? {
                            return Ok(Err(truthdb_object::AccountError::NoSuchUser));
                        }
                        tx.put_user_policy(&account, &user, &name, document.as_bytes())
                            .map(Ok)
                    })
                })?
                .map_err(account_error)?;
                json!({ "account_id": account, "user_name": user, "policy_name": name })
            }
            S3Admin::DeleteUserPolicy(account, user, name) => {
                if !self.object(|file| {
                    file.transaction(|tx| tx.delete_user_policy(&account, &user, &name))
                })? {
                    return Err(CommandError::InvalidCommand(format!(
                        "user {user} of account {account} has no policy {name}"
                    ))
                    .into());
                }
                json!({ "deleted": name })
            }
            S3Admin::ListUserPolicies(account, user) => {
                let policies = self.object(|file| file.user_policies(&account, &user))?;
                json!({ "policies": policies.iter().map(|(name, document)| json!({
                    "policy_name": name,
                    "policy_document": String::from_utf8_lossy(document),
                })).collect::<Vec<_>>() })
            }
            S3Admin::CreateAccessKey(account, user) => {
                let account =
                    account.unwrap_or_else(|| truthdb_object::DEFAULT_ACCOUNT.to_string());
                let id = format!("TDB{}", random_text(17, ACCESS_KEY_ID_ALPHABET)?);
                let secret = random_text(40, SECRET_ALPHABET)?;
                self.object(|file| {
                    file.transaction(|tx| {
                        tx.create_access_key(&id, &secret, &account, user.as_deref(), now_ms())
                    })
                })?
                .map_err(account_error)?;
                json!({
                    "access_key_id": id,
                    "secret_access_key": secret,
                    "account_id": account,
                    "user_name": user,
                })
            }
            S3Admin::DropAccessKey(id) => {
                self.object(|file| file.transaction(|tx| tx.drop_access_key(&id)))?;
                json!({ "dropped": id })
            }
            S3Admin::ListAccessKeys => {
                let keys = self.object(|file| file.access_keys())?;
                json!({ "access_keys": keys.iter().map(|key| json!({
                    "access_key_id": key.id,
                    "account_id": key.account,
                    "user_name": key.user,
                    "status": if key.active { "Active" } else { "Inactive" },
                })).collect::<Vec<_>>() })
            }
            S3Admin::EnableFulltext(bucket, max_bytes) => {
                let max_bytes = match max_bytes {
                    None => truthdb_object::DEFAULT_FULLTEXT_MAX_BYTES,
                    Some(text) => text
                        .parse::<u64>()
                        .ok()
                        .filter(|n| (1..=truthdb_object::MAX_FULLTEXT_BYTES).contains(n))
                        .ok_or_else(|| {
                            CommandError::InvalidCommand(format!(
                                "max_bytes must be a number of bytes from 1 to {}, not '{text}'",
                                truthdb_object::MAX_FULLTEXT_BYTES
                            ))
                        })?,
                };
                self.object(|file| file.transaction(|tx| tx.enable_fulltext(&bucket, max_bytes)))?
                    .map_err(|err| fulltext_error(&bucket, err))?;
                json!({ "bucket": bucket, "fulltext": "backfilling", "max_bytes": max_bytes })
            }
            S3Admin::DisableFulltext(bucket) => {
                self.object(|file| file.transaction(|tx| tx.disable_fulltext(&bucket)))?
                    .map_err(|err| fulltext_error(&bucket, err))?;
                json!({ "bucket": bucket, "fulltext": "purging" })
            }
        };
        render_json(&rendered).map_err(EngineError::from)
    }

    /// Runs one object-engine operation under the storage lock, then takes a
    /// checkpoint if the log is filling (object bodies fill it fast).
    pub fn object<T>(
        &self,
        op: impl FnOnce(&mut truthdb_object::ObjFile<'_>) -> Result<T, StorageError>,
    ) -> Result<T, EngineError> {
        let out = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            match truthdb_object::ObjFile::try_new(&mut guard) {
                Some(mut file) => op(&mut file),
                None => Err(StorageError::InvalidConfig(
                    "this database does not run the object engine".to_string(),
                )),
            }
        };
        // Also after a failure: an operation that ran the ring full must not
        // leave it full for every writer until the next maintenance sweep. A
        // failed checkpoint does not undo the operation (it committed, or
        // holds what it took, such as a read lease), so the operation's result
        // stands; the next operation or sweep tries again.
        if !self.storage.is_standby() {
            let meta = self.meta.read().expect("engine meta poisoned");
            if let Err(err) = self.checkpoint_at(&meta, OBJECT_CHECKPOINT_THRESHOLD) {
                eprintln!("object checkpoint failed: {err}");
            }
        }
        Ok(out?)
    }

    /// Runs one vector-engine operation under the storage lock, then takes a
    /// checkpoint if the log is filling (a batch of vectors fills it fast).
    pub fn vector<T>(
        &self,
        op: impl FnOnce(&mut truthdb_vector::VecFile<'_>) -> Result<T, StorageError>,
    ) -> Result<T, EngineError> {
        let out = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            match truthdb_vector::VecFile::try_new(&mut guard) {
                Some(mut file) => op(&mut file),
                None => Err(StorageError::InvalidConfig(
                    "this database does not run the vector engine".to_string(),
                )),
            }
        };
        self.vector_checkpoint();
        Ok(out?)
    }

    /// The checkpoint a vector operation takes after it if the log is
    /// filling.
    fn vector_checkpoint(&self) {
        if !self.storage.is_standby() {
            let meta = self.meta.read().expect("engine meta poisoned");
            if let Err(err) = self.checkpoint_at(&meta, OBJECT_CHECKPOINT_THRESHOLD) {
                eprintln!("vector checkpoint failed: {err}");
            }
        }
    }

    /// Runs `gate` on the object engine, then (when it passes) `op` on the
    /// vector engine, in one storage-lock hold: what the gate checked (an S3
    /// Vectors request's access key) still holds when `op` runs. `None` when
    /// the gate refused.
    pub fn vector_gated<T>(
        &self,
        gate: impl FnOnce(&mut truthdb_object::ObjFile<'_>) -> Result<bool, StorageError>,
        op: impl FnOnce(&mut truthdb_vector::VecFile<'_>) -> Result<T, StorageError>,
    ) -> Result<Option<T>, EngineError> {
        let out = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            let passed = match truthdb_object::ObjFile::try_new(&mut guard) {
                Some(mut file) => gate(&mut file),
                None => Err(StorageError::InvalidConfig(
                    "this database does not run the object engine".to_string(),
                )),
            };
            match passed {
                Ok(true) => match truthdb_vector::VecFile::try_new(&mut guard) {
                    Some(mut file) => op(&mut file).map(Some),
                    None => Err(StorageError::InvalidConfig(
                        "this database does not run the vector engine".to_string(),
                    )),
                },
                Ok(false) => Ok(None),
                Err(err) => Err(err),
            }
        };
        self.vector_checkpoint();
        Ok(out?)
    }

    /// The maintenance sweep's purge of deleted vector indexes: a bounded
    /// number of budgeted transactions per sweep. A standby never purges (its
    /// log is the primary's).
    pub(crate) fn vector_purge(&self) -> Result<(), EngineError> {
        if self.storage.is_standby() {
            return Ok(());
        }
        let runs = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            truthdb_vector::VecFile::try_new(&mut guard).is_some()
        };
        if !runs || !self.vector(|file| file.purge_pending())? {
            return Ok(());
        }
        for _ in 0..VECTOR_PURGE_STEPS {
            if self.vector(|file| file.transaction(|tx| tx.purge_step()))? {
                break;
            }
        }
        Ok(())
    }

    /// Whether this database runs the object engine.
    fn runs_objects(&self) -> bool {
        let core = self.storage.core_arc();
        let mut guard = core.lock();
        truthdb_object::ObjFile::try_new(&mut guard).is_some()
    }

    /// The maintenance-thread garbage collection of object bodies: frees
    /// unreferenced bodies whose grace is up and abandoned uploads. A standby
    /// never collects (its log is the primary's).
    pub(crate) fn object_gc(&self) -> Result<(), EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(());
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // Abandoned multipart uploads, a batch per sweep: aborting releases
        // their parts, which the collection below then frees.
        let expired = self.object(|file| {
            file.expired_uploads(
                now_ms.saturating_sub(OBJECT_UPLOAD_EXPIRY_MS),
                OBJECT_GC_BATCH as usize,
            )
        })?;
        for upload in expired {
            // Gone meanwhile (completed or aborted) is fine.
            let _ =
                self.object(|file| file.transaction(|tx| tx.abort_upload(upload, now_ms, 0)))?;
        }
        // Journal rows past their retention, a bounded batch per transaction
        // and a bounded number of batches per sweep. Whether anything is due
        // is read first: an idle sweep writes nothing.
        for _ in 0..8 {
            let trimmed = self.object(|file| {
                if !file.journal_due(now_ms, OBJECT_JOURNAL_RETENTION_MS)? {
                    return Ok(0);
                }
                file.transaction(|tx| {
                    tx.trim_journal(
                        now_ms,
                        OBJECT_JOURNAL_RETENTION_MS,
                        OBJECT_JOURNAL_TRIM_BATCH,
                    )
                })
            })?;
            if trimmed < OBJECT_JOURNAL_TRIM_BATCH {
                break;
            }
        }
        // A few bodies per storage-lock hold (each is a forced transaction),
        // so a large backlog never blocks other work for long, and a bounded
        // number per sweep, so the sweep's other duties still run on time.
        let mut freed = 0;
        loop {
            let outcome = self.object(|file| {
                file.collect_garbage(now_ms, OBJECT_ABANDON_AFTER_MS, OBJECT_GC_BATCH)
            })?;
            freed += outcome.bodies;
            if !outcome.more || freed >= OBJECT_GC_SWEEP_LIMIT {
                return Ok(());
            }
        }
    }

    fn maybe_checkpoint(&self, meta: &EngineMeta) -> Result<(), EngineError> {
        self.checkpoint_at(meta, WAL_CHECKPOINT_THRESHOLD)
    }

    /// Takes a checkpoint once the ring is `threshold` full.
    fn checkpoint_at(&self, meta: &EngineMeta, threshold: f64) -> Result<(), EngineError> {
        // A (fuzzy) checkpoint flushes dirty pages and truncates the WAL head to
        // the oldest open transaction's begin LSN, so it may run with open
        // transactions (their undo survives). The decision is (re-)made under the
        // storage lock in `checkpoint_if_wal_full`; this bare pre-check just
        // avoids serializing state on every batch below the WAL threshold.
        if self.wal_usage_ratio() < threshold {
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
            threshold,
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

impl From<SearchError> for EngineError {
    fn from(err: SearchError) -> Self {
        match err {
            SearchError::Command(err) => EngineError::Command(err),
            SearchError::Replay(message) => EngineError::Replay(message),
        }
    }
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

mod fulltext;
#[cfg(test)]
mod relational_tests;
mod s3_parquet;
mod s3_read;
mod s3_sql;
#[cfg(test)]
mod search_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
