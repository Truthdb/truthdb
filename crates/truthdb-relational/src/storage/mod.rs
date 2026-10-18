//! Relational storage: the relational engine's state under the storage mutex
//! (catalog cache, row-version store, readable-standby bookkeeping), its view
//! of the locked file, the `RelStorage` handle with the `rel_*` API, and the
//! relational transaction types. The shared log's storage items are
//! re-exported, so `crate::storage` names both.

pub use truthdb_core::storage::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::relstore::RelState;
use crate::relstore::btree::{BTree, ScanCursor, TreeInsert, TreeRoots};
use crate::relstore::catalog::{self, FIRST_USER_OBJECT_ID, IndexDef, TableDef};
use crate::relstore::ctx::{OpMode, RelCtx, TxnLink};
use crate::relstore::heap::{Heap, Rid};
use crate::relstore::index::{self, Locator};
use crate::relstore::key::encode_key;
use crate::relstore::overflow::{self, OVERFLOW_INLINE_MAX};
use crate::relstore::recovery as rel_recovery;
use crate::relstore::recovery::LogicalUndo;
use crate::relstore::row::{Column, Schema, decode_row, decode_row_projected, encode_row};
use crate::relstore::types::{ColumnType, Datum};
use crate::relstore::version::{
    PendingVersion, ReadSnapshot, Resolved, RowChange, decode_rid_identity, rid_identity,
};
use crate::wal::records::RelRecord;

mod relational;
pub use relational::relational_participant;
#[cfg(test)]
mod tests;

#[allow(unused_imports)]
pub use relational::security::{
    DB_DATAREADER_ID, DB_DATAWRITER_ID, DB_DDLADMIN_ID, DB_OWNER_ID, DBO_ID, FIXED_PRINCIPAL_BASE,
    FIXED_PRINCIPALS, FixedPrincipal, PUBLIC_ID, SYSADMIN_ID, fixed_principal_by_id,
    fixed_principal_by_name,
};

/// Memoized role-membership closures, tagged with the security version they were
/// computed under (a mismatch discards `closure` and rebuilds `edges`). `version`
/// is `None` until first loaded — distinct from `Some(0)`, the valid initial
/// security version, so the very first query on a fresh or restarted database
/// (still at version 0) rebuilds from the live catalog rather than serving the
/// empty default.
#[derive(Default)]
struct MembershipCache {
    version: Option<u64>,
    /// principal_id -> its DIRECT role principal_ids (stored + synthesized).
    edges: std::collections::HashMap<u32, Vec<u32>>,
    /// principal_id -> its transitively-closed role set (computed on demand).
    closure: std::collections::HashMap<u32, std::collections::HashSet<u32>>,
}

/// The relational engine's handle on storage: the shared core plus the
/// relational state read without the storage mutex. Derefs to the core.
pub struct RelStorage {
    core: Arc<Storage>,
    /// `READ_COMMITTED_SNAPSHOT` / `ALLOW_SNAPSHOT_ISOLATION` mirrors of the
    /// version store's options, readable without the storage mutex (lock
    /// analysis and the per-statement snapshot gate are on the hot path).
    rcsi: std::sync::atomic::AtomicBool,
    allow_snapshot: std::sync::atomic::AtomicBool,
    /// FULL recovery model mirror (vs SIMPLE). Read without the mutex by
    /// `sys.databases` and (later) the log-backup hold / 9002 decision.
    recovery_full: std::sync::atomic::AtomicBool,
    /// Bumped whenever the options change: a parked batch whose lock set was
    /// analyzed under an older epoch is re-analyzed before it can be granted
    /// (its versioned-read decision may no longer match execution).
    lock_epoch: std::sync::atomic::AtomicU64,
    /// Bumped by every security DDL (CREATE/DROP USER/ROLE, ALTER ROLE ADD/DROP
    /// MEMBER, and later GRANT/DENY/REVOKE). Separate from `lock_epoch` because
    /// authorization changes no batch's lock set; it invalidates the effective-
    /// membership cache instead. In-memory only (rebuilt from the catalog on
    /// restart, so a 0 reset is correct — nothing membership-cached is durable).
    security_version: std::sync::atomic::AtomicU64,
    /// Memoized transitive-closure of role membership, tagged with the
    /// `security_version` it was computed under; a version mismatch discards it.
    membership: std::sync::Mutex<MembershipCache>,
    /// Scan slices read, so a test can prove a scan stopped early rather than
    /// reading the table and discarding the rest. On the instance and not in a
    /// `static`: the suite runs in parallel in one binary, so a static would
    /// count every other test's scans as well as this one's.
    #[cfg(any(test, feature = "test-hooks"))]
    scan_slices: std::sync::atomic::AtomicUsize,
    /// Times a SELECT took the row-at-a-time path, so a test comparing it with
    /// the collecting path can prove it actually ran — an A/B whose two sides
    /// are the same code agrees with itself. Per-instance for the same reason.
    #[cfg(any(test, feature = "test-hooks"))]
    scan_selects: std::sync::atomic::AtomicUsize,
    #[cfg(any(test, feature = "test-hooks"))]
    covering_scans: std::sync::atomic::AtomicUsize,
    #[cfg(any(test, feature = "test-hooks"))]
    scan_materializations: std::sync::atomic::AtomicUsize,
    /// Columns the last scan slice asked for (`usize::MAX` = the whole row), so
    /// a test can prove the planner pruned the projection. The rows returned are
    /// identical either way, so nothing else can see the difference.
    #[cfg(any(test, feature = "test-hooks"))]
    last_scan_width: std::sync::atomic::AtomicUsize,
}

impl std::ops::Deref for RelStorage {
    type Target = Storage;
    fn deref(&self) -> &Storage {
        &self.core
    }
}

/// Clustered tables locate by encoded PK key; heaps by RID.
#[derive(Debug, Clone)]
pub(crate) enum RowLocator {
    Key(Vec<u8>),
    Rid(Rid),
}

/// A caller-held (multi-statement) relational transaction: the WAL/undo chain
/// plus the tree-root snapshot taken at BEGIN (used to re-descend trees during
/// rollback).
pub(crate) struct StorageTxn {
    txn: RelTxn,
    roots: std::collections::HashMap<u32, u64>,
}

impl StorageTxn {
    /// The transaction's id — a versioned reader's "own transaction" for
    /// visibility of its own uncommitted writes.
    pub(crate) fn txn_id(&self) -> u64 {
        self.txn.txn_id
    }
}

/// A running relational transaction: the log's WAL/undo chain plus the row
/// versions it stages and publishes (Stage 13). Derefs to the chain.
pub(crate) struct RelTxn {
    link: TxnLink,
    /// Row versions staged by the running statement, published to the
    /// version store when the statement succeeds and discarded when it rolls
    /// back. Empty unless a versioned isolation is enabled.
    pending_versions: Vec<PendingVersion>,
    /// Versions this transaction has published, so a rollback (full or to a
    /// savepoint) can reverse the publications exactly.
    published_versions: Vec<crate::relstore::version::PublishRecord>,
}

impl RelTxn {
    fn new(link: TxnLink) -> Self {
        RelTxn {
            link,
            pending_versions: Vec::new(),
            published_versions: Vec::new(),
        }
    }

    /// Captures a savepoint at the current point in this transaction.
    fn savepoint(&self) -> RelSavepoint {
        RelSavepoint {
            link: self.link.savepoint(),
            published_len: self.published_versions.len(),
        }
    }
}

impl std::ops::Deref for RelTxn {
    type Target = TxnLink;
    fn deref(&self) -> &TxnLink {
        &self.link
    }
}

impl std::ops::DerefMut for RelTxn {
    fn deref_mut(&mut self) -> &mut TxnLink {
        &mut self.link
    }
}

/// A relational savepoint: the log's undo marker plus the published-version
/// count at capture, so a partial rollback unpublishes exactly the versions of
/// the statements it undoes.
#[derive(Clone, Copy)]
pub(crate) struct RelSavepoint {
    link: crate::relstore::ctx::Savepoint,
    published_len: usize,
}

impl RelSavepoint {
    /// The undo-log length at capture: a later savepoint has a longer one.
    pub(crate) fn undo_len(&self) -> usize {
        self.link.undo_len
    }
}

/// The transaction a statement runs under.
pub(crate) enum TxnScope<'a> {
    /// Autocommit: begin + commit around the single statement.
    Auto,
    /// A caller-held transaction; the statement's ops are appended and NOT
    /// committed. A statement error leaves its partial ops in place — the
    /// caller dooms the transaction and a later ROLLBACK undoes everything.
    Explicit(&'a mut StorageTxn),
}

/// The relational engine's in-memory state held under the storage mutex.
pub(crate) struct RelEngine {
    /// Relational store state (catalog cache).
    rel: RelState,
    /// Readable standby: per SHIPPED transaction, one stack entry per logged
    /// op `(record LSN, publish record if the op changed a row)`. A CLR pops
    /// every entry above its `undo_next` and unpublishes the popped row
    /// changes — mirroring savepoint/statement rollbacks exactly, since undo
    /// compensates ops in reverse LSN order — and a commit-less TXN_END
    /// unwinds whatever remains.
    standby_published:
        std::collections::HashMap<u64, Vec<(u64, Option<crate::relstore::version::PublishRecord>)>>,
    /// Readable standby: the LSN up to which shipped changes have been folded
    /// into the version store — an overlap re-ship must not publish the same
    /// change twice (duplicate chain entries).
    standby_version_floor: u64,
    /// The DEFAULT database's name (database id 1). The default database is
    /// synthesized — never a catalog row — and named by the instance
    /// configuration (`[tds] database`), exactly where the session default
    /// came from before databases existed. Stamped at engine construction;
    /// "truthdb" until then.
    default_db_name: String,
    /// The container tag the next `rel_ctx()` carries (see
    /// [`RelCtx::container`]): set by each attributed `rel_*` entry point to
    /// the database its statement mutates, and to 0 by server-scoped ones
    /// (principals). Recovery and reopen paths run with a fresh 0.
    current_container: u16,
    /// Stage 13 version store: row-version chains for snapshot reads, plus
    /// the RCSI / ALLOW_SNAPSHOT_ISOLATION options (persisted in the
    /// superblock reserved area; the chains themselves are memory-only — no
    /// snapshot survives a restart, so neither must they).
    version: crate::relstore::version::VersionState,
}

impl RelEngine {
    fn new() -> Self {
        RelEngine {
            rel: RelState::new(),
            standby_published: std::collections::HashMap::new(),
            standby_version_floor: 0,
            default_db_name: "truthdb".to_string(),
            current_container: 0,
            version: crate::relstore::version::VersionState::default(),
        }
    }
}

/// The relational view of the locked storage.
pub(crate) trait LockedRel {
    fn rel(&mut self) -> RelFile<'_>;
}

impl LockedRel for Locked {
    fn rel(&mut self) -> RelFile<'_> {
        let (core, eng) = self.engine_parts::<RelEngine>();
        RelFile { core, eng }
    }
}

/// A relational operation's view of the locked storage: the engine state
/// (through deref) and the core file (`core`), borrowed disjointly so a
/// relational context can hold both at once.
pub(crate) struct RelFile<'a> {
    core: &'a mut StorageFile,
    eng: &'a mut RelEngine,
}

impl std::ops::Deref for RelFile<'_> {
    type Target = RelEngine;
    fn deref(&self) -> &RelEngine {
        self.eng
    }
}

impl std::ops::DerefMut for RelFile<'_> {
    fn deref_mut(&mut self) -> &mut RelEngine {
        self.eng
    }
}
