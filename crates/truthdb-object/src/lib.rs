//! The object engine: S3-style buckets, versioned objects and their bodies,
//! as a second engine on the shared log beside the relational one.
//!
//! Everything durable lives in B+ trees and blob pages logged through the
//! shared log, so crash recovery, checkpoints, backups and replication cover
//! it with no engine-private state. The engine's root is a directory tree
//! mapping each of its trees to its root page; trees are created the first
//! time they are needed.
//!
//! Callers hold the storage lock for each whole transaction: every operation
//! here runs one transaction start to finish inside one lock hold (see
//! `RelCtx::allocate_extent_in_txn`).

mod accounts;
mod body;
mod buckets;
mod codec;
mod consumers;
mod credentials;
mod documents;
mod fulltext;
mod gc;
mod journal;
mod listing;
mod locks;
mod objects;
mod pack;
mod uploads;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use truthdb_core::relstore::btree::{BTree, TreeInsert, TreeRoots};
use truthdb_core::relstore::ctx::{OpMode, RelCtx, TxnLink};
use truthdb_core::relstore::recovery::{LogicalUndo, rollback};
use truthdb_core::storage::{
    EngineParticipant, FIRST_ENGINE_OBJECT_ID, Locked, StorageError, StorageFile,
};
use truthdb_core::wal::records::RelRecord;

pub use accounts::{AccountError, AccountInfo, DEFAULT_ACCOUNT, UserInfo, valid_account_id};
pub use body::{BodyInfo, BodyLease, BodyWriter, SEGMENT_BYTES, SealedBody};
pub use buckets::{BucketError, BucketInfo, Versioning};
pub use consumers::{Consumer, FULLTEXT_CONSUMER};
pub use credentials::AccessKey;
pub use documents::{
    BUCKET_ENCRYPTION, BUCKET_POLICY, ManagedPolicy, OBJECT_LOCK_DEFAULT, PUBLIC_ACCESS_BLOCK,
};
pub use fulltext::{
    DEFAULT_FULLTEXT_MAX_BYTES, FtBucket, FtError, FtKey, FtState, FtStatus, MAX_FULLTEXT_BYTES,
    fulltext_status,
};
pub use gc::GcOutcome;
pub use journal::{JournalObject, JournalRecord, Origin, RecordType};
pub use listing::{ListAfter, ListedObject, Listing};
pub use locks::{EventHold, ObjectLock, Retention, RetentionChange, RetentionMode};
pub use objects::{
    CustomerKey, Deleted, ObjectAttrs, ObjectError, ObjectInfo, StorageClass, VersionSel,
    version_id,
};
pub use pack::{PackStep, PackWork};
pub use uploads::{MAX_PART_NUMBER, MIN_PART_BYTES, PartInfo, UploadError, UploadInfo};

/// The object engine's id on the shared log (its root lives in the
/// superblock's engine-roots table).
pub const OBJECT_ENGINE_ID: u32 = 1;

/// The directory tree: tree object id -> root page, plus engine counters.
const META_TREE: u32 = FIRST_ENGINE_OBJECT_ID;
/// Body id -> [`codec::BodyRow`].
const BODIES_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 1;
/// (body id, segment index) -> [`codec::SegmentRow`].
const SEGMENTS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 2;
/// (due time, body id) -> (): bodies no one references, freed once due.
const GC_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 3;
/// (created time, body id) -> (): bodies still being written.
const STAGING_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 4;
/// Access key id -> its owner and secret: the S3 API's credentials.
const CREDENTIALS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 5;
/// Bucket name -> [`buckets::BucketRow`].
const BUCKETS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 6;
/// (bucket, object key, version) -> the version (see `objects`).
const OBJECTS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 7;
/// (version, chunk) -> a slice of the version's encoded attributes (and
/// (1, upload, chunk) -> an upload's).
const ATTRS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 8;
/// Upload id -> the upload's bucket, key and start time.
const UPLOADS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 9;
/// (bucket, 0, key, 0, upload) -> (): uploads in listing order.
const UPLOAD_INDEX_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 10;
/// (upload, part number) -> the part.
const PARTS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 11;
/// Sequence number -> a [`journal::JournalRecord`]: every change, in order.
const CHANGES_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 12;
/// Bucket name -> its full-text state (see `fulltext`).
const FT_BUCKETS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 13;
/// The full-text index's documents (see `truthdb_fulltext::Index`).
const FT_DOCS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 14;
/// The full-text index's postings.
const FT_POSTINGS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 15;
/// (bucket, 0, key) -> the version of the key the index holds, and how.
const FT_KEYS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 16;
/// (bucket, 0, version) -> key: the key of each version the index holds, to
/// name a search's hits.
const FT_VERSIONS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 17;
/// Account id -> the account (see `accounts`).
const ACCOUNTS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 18;
/// (account, 0, user name) -> the user (see `accounts`).
const USERS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 19;
/// (account, 0, user, 0, policy name, 0, chunk) -> a slice of a user's
/// policy (see `documents`).
const POLICIES_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 20;
/// (bucket, 0, kind, id, 0, chunk) -> a slice of a bucket's configuration
/// (see `documents`).
const BUCKET_CONFIG_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 21;
/// Version -> its Object Lock retention and legal hold (and (1, upload) ->
/// those an upload's object will have; see `locks`).
const LOCKS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 22;
/// Consumer name -> its place in the change journal (see `consumers`).
const CONSUMERS_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 23;
/// Body -> the zstd level it is to be packed at and its next segment (see
/// `pack`).
const PACK_QUEUE_TREE: u32 = FIRST_ENGINE_OBJECT_ID + 24;

/// The object id stamped into every body page's header. Not a tree.
pub const BODY_PAGES_OBJECT: u32 = FIRST_ENGINE_OBJECT_ID + 0x100;

/// The object ids this engine owns start at `FIRST_ENGINE_OBJECT_ID`; the
/// range after them is left for other engines.
const OBJECT_ID_SPAN: u32 = 0x1000;

/// META key of the next body id to hand out.
const NEXT_BODY_KEY: &[u8] = b"next_body";
/// META key of the next object version number to hand out.
const NEXT_VERSION_KEY: &[u8] = b"next_version";
/// META key of the next multipart upload id to hand out.
const NEXT_UPLOAD_KEY: &[u8] = b"next_upload";
/// META key of the journal's next sequence number.
const NEXT_CHANGE_KEY: &[u8] = b"next_change";
/// META key of the last journal row the full-text indexer has applied, in a
/// file written before journal consumers (see `consumers`).
const FT_CURSOR_KEY: &[u8] = b"ft_cursor";

/// The object engine's in-memory state under the storage lock.
#[derive(Default)]
pub struct ObjEngine {
    /// The directory tree's root page, once the engine has written anything.
    root: Option<u64>,
    /// Tree object id -> root page, loaded from the directory tree.
    trees: HashMap<u32, u64>,
    /// Body id -> readers currently streaming it; garbage collection leaves
    /// a leased body alone.
    leases: HashMap<u64, u32>,
}

impl ObjEngine {
    /// The factory entry for this engine.
    pub fn boxed() -> Box<dyn EngineParticipant> {
        Box::new(ObjEngine::default())
    }
}

impl EngineParticipant for ObjEngine {
    fn engine_id(&self) -> u32 {
        OBJECT_ENGINE_ID
    }

    fn owns_object(&self, object_id: u32) -> bool {
        (FIRST_ENGINE_OBJECT_ID..FIRST_ENGINE_OBJECT_ID + OBJECT_ID_SPAN).contains(&object_id)
    }

    fn open(&mut self, metadata_root: Option<u64>, _db_options: u8) {
        self.root = metadata_root;
    }

    fn metadata_root(&self) -> Option<u64> {
        self.root
    }

    fn set_metadata_root(&mut self, page: u64) {
        self.root = Some(page);
    }

    fn options_byte(&self) -> u8 {
        0
    }

    fn reload(&mut self, core: &mut StorageFile) -> Result<(), StorageError> {
        self.trees.clear();
        let Some(root) = self.root else {
            return Ok(());
        };
        let mut ctx = core.page_ctx(0);
        for (key, row) in meta_tree(root).scan(&mut ctx)? {
            if let (Ok(id), Ok(page)) = (
                <[u8; 4]>::try_from(key.as_slice()),
                <[u8; 8]>::try_from(row.as_slice()),
            ) {
                self.trees
                    .insert(u32::from_be_bytes(id), u64::from_le_bytes(page));
            }
        }
        Ok(())
    }

    fn recovery_undo(&self) -> Box<dyn LogicalUndo> {
        Box::new(self.tree_roots())
    }

    fn capture_standby(
        &mut self,
        _core: &mut StorageFile,
        _records: &[(u64, RelRecord)],
        _floor: u64,
    ) {
    }

    fn finish_recovery(&mut self) {}

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ObjEngine {
    fn tree_roots(&self) -> TreeRoots {
        let mut roots = self.trees.clone();
        if let Some(root) = self.root {
            roots.insert(META_TREE, root);
        }
        TreeRoots(roots)
    }
}

fn meta_tree(root: u64) -> BTree {
    BTree {
        object_id: META_TREE,
        root,
    }
}

/// The trees an object transaction may work on: object ids
/// `FIRST_ENGINE_OBJECT_ID` (the directory tree) up to this many after it.
const TREE_COUNT: usize = 25;

/// The root pages of the trees an object transaction works on, resolved
/// before it begins, by object id.
#[derive(Clone, Copy)]
pub(crate) struct Trees {
    roots: [u64; TREE_COUNT],
}

impl Trees {
    fn tree(&self, object_id: u32) -> BTree {
        BTree {
            object_id,
            root: self.roots[(object_id - FIRST_ENGINE_OBJECT_ID) as usize],
        }
    }
    pub fn meta(&self) -> BTree {
        self.tree(META_TREE)
    }
    pub fn bodies(&self) -> BTree {
        self.tree(BODIES_TREE)
    }
    pub fn segments(&self) -> BTree {
        self.tree(SEGMENTS_TREE)
    }
    pub fn gc(&self) -> BTree {
        self.tree(GC_TREE)
    }
    pub fn buckets(&self) -> BTree {
        self.tree(BUCKETS_TREE)
    }
    pub fn objects(&self) -> BTree {
        self.tree(OBJECTS_TREE)
    }
    pub fn uploads(&self) -> BTree {
        self.tree(UPLOADS_TREE)
    }
    pub fn upload_index(&self) -> BTree {
        self.tree(UPLOAD_INDEX_TREE)
    }
    pub fn parts(&self) -> BTree {
        self.tree(PARTS_TREE)
    }
    pub fn changes(&self) -> BTree {
        self.tree(CHANGES_TREE)
    }
    pub fn ft_buckets(&self) -> BTree {
        self.tree(FT_BUCKETS_TREE)
    }
    pub fn ft_index(&self) -> truthdb_fulltext::Index {
        truthdb_fulltext::Index {
            postings: self.tree(FT_POSTINGS_TREE),
            docs: self.tree(FT_DOCS_TREE),
        }
    }
    pub fn ft_keys(&self) -> BTree {
        self.tree(FT_KEYS_TREE)
    }
    pub fn ft_versions(&self) -> BTree {
        self.tree(FT_VERSIONS_TREE)
    }
    pub fn attrs(&self) -> BTree {
        self.tree(ATTRS_TREE)
    }
    pub fn credentials(&self) -> BTree {
        self.tree(CREDENTIALS_TREE)
    }
    pub fn staging(&self) -> BTree {
        self.tree(STAGING_TREE)
    }
    pub fn accounts(&self) -> BTree {
        self.tree(ACCOUNTS_TREE)
    }
    pub fn users(&self) -> BTree {
        self.tree(USERS_TREE)
    }
    pub fn policies(&self) -> BTree {
        self.tree(POLICIES_TREE)
    }
    pub fn bucket_config(&self) -> BTree {
        self.tree(BUCKET_CONFIG_TREE)
    }
    pub fn locks(&self) -> BTree {
        self.tree(LOCKS_TREE)
    }
    pub fn consumers(&self) -> BTree {
        self.tree(CONSUMERS_TREE)
    }
    pub fn pack_queue(&self) -> BTree {
        self.tree(PACK_QUEUE_TREE)
    }
}

/// One object-engine transaction in progress: the operations on it commit or
/// roll back together (see [`ObjFile::transaction`]).
pub struct ObjTxn<'t, 'c> {
    pub(crate) ctx: &'t mut RelCtx<'c>,
    pub(crate) txn: &'t mut TxnLink,
    pub(crate) trees: Trees,
    /// The request the transaction's changes come from (see `journal`).
    pub(crate) origin: Option<Origin>,
}

/// The object engine's view of the locked storage: the core file and the
/// engine state, borrowed disjointly.
pub struct ObjFile<'a> {
    core: &'a mut StorageFile,
    eng: &'a mut ObjEngine,
    /// The request this engine call is for: its transactions' changes carry
    /// it in the journal.
    origin: Option<Origin>,
}

impl<'a> ObjFile<'a> {
    /// # Panics
    /// If the storage was opened without the object engine.
    pub fn new(locked: &'a mut Locked) -> Self {
        let (core, eng) = locked.engine_parts::<ObjEngine>();
        ObjFile {
            core,
            eng,
            origin: None,
        }
    }

    /// [`Self::new`], or `None` when the storage was opened without the
    /// object engine.
    pub fn try_new(locked: &'a mut Locked) -> Option<Self> {
        let (core, eng) = locked.try_engine_parts::<ObjEngine>()?;
        Some(ObjFile {
            core,
            eng,
            origin: None,
        })
    }

    /// Records `origin` as the request this engine call is for.
    pub fn set_origin(&mut self, origin: Origin) {
        self.origin = Some(origin);
    }

    /// Runs `body` as one transaction over the engine's trees (created the
    /// first time): committed and forced to disk when it succeeds, rolled back
    /// when it fails.
    pub fn transaction<T>(
        &mut self,
        body: impl FnOnce(&mut ObjTxn<'_, '_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        self.core.ensure_rel_usable()?;
        let mut roots = [0; TREE_COUNT];
        roots[0] = self.meta()?.root;
        for (slot, object_id) in roots.iter_mut().zip(FIRST_ENGINE_OBJECT_ID..).skip(1) {
            *slot = self.tree(object_id)?.root;
        }
        let trees = Trees { roots };
        let origin = self.origin.clone();
        self.in_txn(|ctx, txn| {
            body(&mut ObjTxn {
                ctx,
                txn,
                trees,
                origin,
            })
        })
    }

    /// The tree with this object id if it exists, for reads: a read never
    /// creates anything (a standby's log is read-only), and asks only for the
    /// trees it needs (a tree added by a later release is missing from older
    /// files). Fails on a wedged store, whose pool may hold changes the log
    /// does not back.
    pub(crate) fn existing_tree(&self, object_id: u32) -> Result<Option<BTree>, StorageError> {
        self.core.ensure_rel_usable()?;
        Ok(self
            .eng
            .trees
            .get(&object_id)
            .map(|&root| BTree { object_id, root }))
    }

    /// The tree with this object id, creating it the first time.
    fn tree(&mut self, object_id: u32) -> Result<BTree, StorageError> {
        if let Some(&root) = self.eng.trees.get(&object_id) {
            return Ok(BTree { object_id, root });
        }
        let meta = self.meta()?;
        let root = self.in_txn(|ctx, txn| {
            let tree = BTree::create(ctx, object_id)?;
            let inserted = meta.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &object_id.to_be_bytes(),
                &tree.root.to_le_bytes(),
            )?;
            debug_assert_eq!(inserted, TreeInsert::Inserted);
            Ok(tree.root)
        })?;
        self.eng.trees.insert(object_id, root);
        Ok(BTree { object_id, root })
    }

    /// The directory tree, created (and announced as the engine root) the
    /// first time the engine writes anything.
    fn meta(&mut self) -> Result<BTree, StorageError> {
        if let Some(root) = self.eng.root {
            return Ok(meta_tree(root));
        }
        let mut ctx = self.core.page_ctx(0);
        let tree = BTree::create(&mut ctx, META_TREE)?;
        ctx.append(
            &RelRecord::set_engine_root(OBJECT_ENGINE_ID, tree.root),
            true,
        )?;
        self.eng.root = Some(tree.root);
        Ok(tree)
    }

    /// Runs `body` as one transaction: committed (and forced to disk) when it
    /// succeeds, rolled back when it fails. When the commit or the rollback
    /// cannot be logged, or the log cannot be forced, the store is wedged: the
    /// pool may hold changes the log cannot explain, and nothing may build on
    /// them (or checkpoint them) before a restart.
    fn in_txn<T>(
        &mut self,
        body: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        self.core.ensure_rel_usable()?;
        let roots = self.eng.tree_roots();
        let txn_id = self.core.alloc_txn_id();
        let mut wedge = false;
        let outcome = {
            let mut ctx = self.core.page_ctx(0);
            let mut txn = ctx.begin(txn_id)?;
            match body(&mut ctx, &mut txn) {
                Ok(value) => match ctx.commit(txn) {
                    Ok(_) => Ok(value),
                    Err(err) => {
                        wedge = true;
                        Err(err)
                    }
                },
                Err(err) => match rollback(&mut ctx, txn, &roots) {
                    Ok(()) => Err(err),
                    Err(rollback_err) => {
                        wedge = true;
                        Err(StorageError::InvalidFile(format!(
                            "{err}; and the rollback failed: {rollback_err}"
                        )))
                    }
                },
            }
        };
        let outcome = outcome.and_then(|value| {
            self.core
                .wal_sync_all()
                .map(|()| value)
                .inspect_err(|_| wedge = true)
        });
        if wedge {
            self.core.set_wedged();
        }
        outcome
    }
}

/// Hands out the next body id from the directory tree's counter
/// (transactional: a rolled-back body's id is handed out again).
fn next_body_id(tx: &mut ObjTxn<'_, '_>) -> Result<u64, StorageError> {
    next_counter(tx, NEXT_BODY_KEY)
}

/// Hands out the next value of the directory tree's counter `key`, starting
/// at 1 (transactional).
pub(crate) fn next_counter(tx: &mut ObjTxn<'_, '_>, key: &[u8]) -> Result<u64, StorageError> {
    let meta = tx.trees.meta();
    let current = meta
        .get(tx.ctx, key)?
        .map(|row| codec::u64_le(&row))
        .transpose()?;
    let next = current.unwrap_or(1);
    let row = (next + 1).to_le_bytes();
    match current {
        Some(_) => {
            meta.update(tx.ctx, &mut OpMode::Txn(tx.txn), key, &row)?;
        }
        None => {
            meta.insert_unique(tx.ctx, &mut OpMode::Txn(tx.txn), key, &row)?;
        }
    }
    Ok(next)
}
