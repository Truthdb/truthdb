//! The vector engine: S3 Vectors-style vector buckets, vector indexes and
//! float32 vectors with JSON metadata, as a third engine on the shared log
//! beside the relational and object engines.
//!
//! Everything durable lives in B+ trees logged through the shared log, so
//! crash recovery, checkpoints, backups and replication cover it with no
//! engine-private state. The engine's root is a directory tree mapping each
//! of its trees to its root page; trees are created the first time they are
//! needed. Searches are exact: an index is scanned a slice per storage-lock
//! hold (see [`query`]).
//!
//! Every transaction writes a bounded amount of log (an eighth of its
//! compensation reserve), so its rollback, or recovery's undo, always fits
//! the log: a batch of vectors is put, and a deleted index purged, over as
//! many transactions as that takes.

mod metadata;
pub mod names;
pub mod query;
mod store;

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

pub use metadata::{Filter, FilterError, Metadata, MetadataError};
pub use query::{Hit, Query, QueryError};
pub use store::{
    BucketInfo, DistanceMetric, IndexInfo, NewIndex, VectorEntry, VectorError, VectorInput,
};

/// The vector engine's id on the shared log.
pub const VECTOR_ENGINE_ID: u32 = 2;

/// The first object id of the vector engine's range (the object engine's is
/// the 0x1000 ids before it).
const FIRST: u32 = FIRST_ENGINE_OBJECT_ID + 0x1000;
/// The directory tree: tree object id -> root page, plus counters.
const META_TREE: u32 = FIRST;
/// Bucket name -> creation time.
const BUCKETS_TREE: u32 = FIRST + 1;
/// (bucket, 0, index name) -> the index's settings (see `store`).
const INDEXES_TREE: u32 = FIRST + 2;
/// (index, key) -> the vector's sequence number.
const KEYS_TREE: u32 = FIRST + 3;
/// (index, sequence, chunk) -> a slice of the vector's float32 values.
const DATA_TREE: u32 = FIRST + 4;
/// (index, sequence, chunk) -> a slice of the vector's filterable metadata.
const FILTER_TREE: u32 = FIRST + 5;
/// (index, sequence, chunk) -> a slice of the vector's whole metadata.
const META_DATA_TREE: u32 = FIRST + 6;
/// (index, sequence) -> the vector's key.
const SEQ_TREE: u32 = FIRST + 7;
/// Index -> (): deleted indexes whose vectors are being removed.
const PURGE_TREE: u32 = FIRST + 8;

/// The object ids this engine owns.
const OBJECT_ID_SPAN: u32 = 0x1000;

/// META key of the next index id to hand out.
const NEXT_INDEX_KEY: &[u8] = b"next_index";
/// META key of the next vector sequence number to hand out.
const NEXT_SEQ_KEY: &[u8] = b"next_seq";

/// The vector engine's in-memory state under the storage lock.
#[derive(Default)]
pub struct VecEngine {
    /// The directory tree's root page, once the engine has written anything.
    root: Option<u64>,
    /// Tree object id -> root page, loaded from the directory tree.
    trees: HashMap<u32, u64>,
}

impl VecEngine {
    /// The factory entry for this engine.
    pub fn boxed() -> Box<dyn EngineParticipant> {
        Box::new(VecEngine::default())
    }

    fn tree_roots(&self) -> TreeRoots {
        let mut roots = self.trees.clone();
        if let Some(root) = self.root {
            roots.insert(META_TREE, root);
        }
        TreeRoots(roots)
    }
}

impl EngineParticipant for VecEngine {
    fn engine_id(&self) -> u32 {
        VECTOR_ENGINE_ID
    }

    fn owns_object(&self, object_id: u32) -> bool {
        (FIRST..FIRST + OBJECT_ID_SPAN).contains(&object_id)
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

fn meta_tree(root: u64) -> BTree {
    BTree {
        object_id: META_TREE,
        root,
    }
}

/// The trees a vector transaction works on, resolved before it begins.
#[derive(Clone, Copy)]
pub(crate) struct Trees {
    meta: u64,
    buckets: u64,
    indexes: u64,
    keys: u64,
    data: u64,
    filter: u64,
    metadata: u64,
    seq: u64,
    purge: u64,
}

macro_rules! tree_accessors {
    ($($name:ident => $id:ident),* $(,)?) => {
        impl Trees {
            $(pub(crate) fn $name(&self) -> BTree {
                BTree { object_id: $id, root: self.$name }
            })*
        }
    };
}

tree_accessors!(
    buckets => BUCKETS_TREE,
    indexes => INDEXES_TREE,
    keys => KEYS_TREE,
    data => DATA_TREE,
    filter => FILTER_TREE,
    metadata => META_DATA_TREE,
    seq => SEQ_TREE,
    purge => PURGE_TREE,
);

impl Trees {
    pub(crate) fn meta(&self) -> BTree {
        meta_tree(self.meta)
    }
}

/// One vector transaction in progress (see [`VecFile::transaction`]).
pub struct VecTxn<'t, 'c> {
    pub(crate) ctx: &'t mut RelCtx<'c>,
    pub(crate) txn: &'t mut TxnLink,
    pub(crate) trees: Trees,
}

impl VecTxn<'_, '_> {
    /// The log one transaction may write: an eighth of the compensation
    /// reserve, so its rollback, or recovery's undo, always fits the log.
    pub(crate) fn log_budget(&mut self) -> u64 {
        self.ctx.io.wal.reserve() / 8
    }

    /// The log written since `start` (a WAL tail).
    pub(crate) fn log_since(&mut self, start: u64) -> u64 {
        self.ctx.io.wal.tail() - start
    }

    pub(crate) fn log_tail(&mut self) -> u64 {
        self.ctx.io.wal.tail()
    }

    /// Hands out the next value of the directory tree's counter `key`,
    /// starting at 1 (transactional).
    pub(crate) fn next_counter(&mut self, key: &[u8]) -> Result<u64, StorageError> {
        let meta = self.trees.meta();
        let current = meta
            .get(self.ctx, key)?
            .map(|row| {
                <[u8; 8]>::try_from(row.as_slice())
                    .map(u64::from_le_bytes)
                    .map_err(|_| StorageError::InvalidFile("corrupt vector counter".into()))
            })
            .transpose()?;
        let next = current.unwrap_or(1);
        let row = (next + 1).to_le_bytes();
        let mode = &mut OpMode::Txn(self.txn);
        if current.is_some() {
            meta.update(self.ctx, mode, key, &row)?;
        } else {
            meta.insert_unique(self.ctx, mode, key, &row)?;
        }
        Ok(next)
    }
}

/// The vector engine's view of the locked storage.
pub struct VecFile<'a> {
    core: &'a mut StorageFile,
    eng: &'a mut VecEngine,
}

impl<'a> VecFile<'a> {
    /// # Panics
    /// If the storage was opened without the vector engine.
    pub fn new(locked: &'a mut Locked) -> Self {
        let (core, eng) = locked.engine_parts::<VecEngine>();
        VecFile { core, eng }
    }

    /// [`Self::new`], or `None` when the storage was opened without the
    /// vector engine.
    pub fn try_new(locked: &'a mut Locked) -> Option<Self> {
        let (core, eng) = locked.try_engine_parts::<VecEngine>()?;
        Some(VecFile { core, eng })
    }

    /// Runs `body` as one transaction over the engine's trees (created the
    /// first time): committed and forced to disk when it succeeds, rolled
    /// back when it fails.
    pub fn transaction<T>(
        &mut self,
        body: impl FnOnce(&mut VecTxn<'_, '_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        self.core.ensure_rel_usable()?;
        let trees = Trees {
            meta: self.meta()?.root,
            buckets: self.tree(BUCKETS_TREE)?.root,
            indexes: self.tree(INDEXES_TREE)?.root,
            keys: self.tree(KEYS_TREE)?.root,
            data: self.tree(DATA_TREE)?.root,
            filter: self.tree(FILTER_TREE)?.root,
            metadata: self.tree(META_DATA_TREE)?.root,
            seq: self.tree(SEQ_TREE)?.root,
            purge: self.tree(PURGE_TREE)?.root,
        };
        self.in_txn(|ctx, txn| body(&mut VecTxn { ctx, txn, trees }))
    }

    /// The tree with this object id if it exists, for reads: a read never
    /// creates anything (a standby's log is read-only). Fails on a wedged
    /// store.
    pub(crate) fn existing_tree(&self, object_id: u32) -> Result<Option<BTree>, StorageError> {
        self.core.ensure_rel_usable()?;
        Ok(self
            .eng
            .trees
            .get(&object_id)
            .map(|&root| BTree { object_id, root }))
    }

    pub(crate) fn page_ctx(&mut self) -> RelCtx<'_> {
        self.core.page_ctx(0)
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
            &RelRecord::set_engine_root(VECTOR_ENGINE_ID, tree.root),
            true,
        )?;
        self.eng.root = Some(tree.root);
        Ok(tree)
    }

    /// Runs `body` as one transaction: committed (and forced to disk) when it
    /// succeeds, rolled back when it fails. When the commit or the rollback
    /// cannot be logged, or the log cannot be forced, the store is wedged.
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
