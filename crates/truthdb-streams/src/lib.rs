//! The streams engine: topics, each of one or more partitions, each an
//! ordered log of records (a timestamp, an optional key, a value) at
//! contiguous offsets, as a fourth engine on the shared log. Records are
//! appended and read by offset, and kept for the topic's retention period;
//! consumer groups commit the offsets they have read. It carries the
//! object store's event notifications, and is what a Kafka-compatible
//! listener serves.
//!
//! Everything durable lives in B+ trees logged through the shared log, so
//! crash recovery, checkpoints, backups and replication cover it with no
//! engine-private state. The engine's root is a directory tree mapping each
//! of its trees to its root page; trees are created the first time they are
//! needed. Every transaction stops writing once it has written an eighth of
//! the log's compensation reserve, so its rollback, or recovery's undo,
//! fits the log: a large trim or purge runs over as many transactions as
//! that takes. An append is one transaction (a source's sequence number
//! covers all of it); its size limits keep it within that budget on the
//! smallest log ring.

mod queue;
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

pub use queue::{MAX_VISIBILITY_MS, QueueError, QueueMessage};
pub use store::{
    Appended, MAX_APPEND_CHUNKS, MAX_PARTITIONS, MAX_RECORD_BYTES, NewRecord, Partition, Record,
    StreamError, Topic, valid_topic_name,
};

/// The streams engine's id on the shared log.
pub const STREAMS_ENGINE_ID: u32 = 3;

/// The first object id of the streams engine's range (after the object
/// engine's and the vector engine's).
const FIRST: u32 = FIRST_ENGINE_OBJECT_ID + 0x2000;
/// The directory tree: tree object id -> root page, plus counters.
const META_TREE: u32 = FIRST;
/// Topic name -> the topic (see `store`).
const TOPICS_TREE: u32 = FIRST + 1;
/// (topic, partition) -> where the partition's log starts and ends.
const PARTITIONS_TREE: u32 = FIRST + 2;
/// (topic, partition, offset, chunk) -> a slice of the record.
const RECORDS_TREE: u32 = FIRST + 3;
/// (topic, partition, source) -> the last sequence number that source
/// appended (so it appends each at most once).
const SOURCES_TREE: u32 = FIRST + 4;
/// (topic, partition, group) -> the offset the group has committed.
const GROUP_OFFSETS_TREE: u32 = FIRST + 5;
/// Topic id -> its partition count: dropped topics whose records are being
/// removed.
const PURGE_TREE: u32 = FIRST + 6;
/// (topic, partition, offset) -> a queue message's lease: when it is
/// visible again, how often it was received, and whether it is done (see
/// `queue`).
pub(crate) const QUEUE_LEASES_TREE: u32 = FIRST + 7;
/// (topic, partition) -> the queue's head: every message before it is done.
pub(crate) const QUEUE_HEADS_TREE: u32 = FIRST + 8;

/// The object ids this engine owns.
const OBJECT_ID_SPAN: u32 = 0x1000;

/// META key of the next topic id to hand out.
const NEXT_TOPIC_KEY: &[u8] = b"next_topic";

/// The streams engine's in-memory state under the storage lock.
#[derive(Default)]
pub struct StreamEngine {
    /// The directory tree's root page, once the engine has written anything.
    root: Option<u64>,
    /// Tree object id -> root page, loaded from the directory tree.
    trees: HashMap<u32, u64>,
}

impl StreamEngine {
    /// The factory entry for this engine.
    pub fn boxed() -> Box<dyn EngineParticipant> {
        Box::new(StreamEngine::default())
    }

    fn tree_roots(&self) -> TreeRoots {
        let mut roots = self.trees.clone();
        if let Some(root) = self.root {
            roots.insert(META_TREE, root);
        }
        TreeRoots(roots)
    }
}

impl EngineParticipant for StreamEngine {
    fn engine_id(&self) -> u32 {
        STREAMS_ENGINE_ID
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

/// The trees a streams transaction works on, resolved before it begins.
#[derive(Clone, Copy)]
pub(crate) struct Trees {
    meta: u64,
    topics: u64,
    partitions: u64,
    records: u64,
    sources: u64,
    group_offsets: u64,
    purge: u64,
    queue_leases: u64,
    queue_heads: u64,
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
    topics => TOPICS_TREE,
    partitions => PARTITIONS_TREE,
    records => RECORDS_TREE,
    sources => SOURCES_TREE,
    group_offsets => GROUP_OFFSETS_TREE,
    purge => PURGE_TREE,
    queue_leases => QUEUE_LEASES_TREE,
    queue_heads => QUEUE_HEADS_TREE,
);

impl Trees {
    pub(crate) fn meta(&self) -> BTree {
        meta_tree(self.meta)
    }
}

/// One streams transaction in progress (see [`StreamFile::transaction`]).
pub struct StreamTxn<'t, 'c> {
    pub(crate) ctx: &'t mut RelCtx<'c>,
    pub(crate) txn: &'t mut TxnLink,
    pub(crate) trees: Trees,
}

impl StreamTxn<'_, '_> {
    /// The log after which a transaction stops writing: an eighth of the
    /// compensation reserve, so its rollback, or recovery's undo, fits the
    /// log with room for the one record (or batch of rows) past it.
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
                    .map_err(|_| StorageError::InvalidFile("corrupt streams counter".into()))
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

/// The streams engine's view of the locked storage.
pub struct StreamFile<'a> {
    core: &'a mut StorageFile,
    eng: &'a mut StreamEngine,
}

impl<'a> StreamFile<'a> {
    /// # Panics
    /// If the storage was opened without the streams engine.
    pub fn new(locked: &'a mut Locked) -> Self {
        let (core, eng) = locked.engine_parts::<StreamEngine>();
        StreamFile { core, eng }
    }

    /// [`Self::new`], or `None` when the storage was opened without the
    /// streams engine.
    pub fn try_new(locked: &'a mut Locked) -> Option<Self> {
        let (core, eng) = locked.try_engine_parts::<StreamEngine>()?;
        Some(StreamFile { core, eng })
    }

    /// Runs `body` as one transaction over the engine's trees (created the
    /// first time): committed and forced to disk when it succeeds, rolled
    /// back when it fails.
    pub fn transaction<T>(
        &mut self,
        body: impl FnOnce(&mut StreamTxn<'_, '_>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        self.core.ensure_rel_usable()?;
        let trees = Trees {
            meta: self.meta()?.root,
            topics: self.tree(TOPICS_TREE)?.root,
            partitions: self.tree(PARTITIONS_TREE)?.root,
            records: self.tree(RECORDS_TREE)?.root,
            sources: self.tree(SOURCES_TREE)?.root,
            group_offsets: self.tree(GROUP_OFFSETS_TREE)?.root,
            purge: self.tree(PURGE_TREE)?.root,
            queue_leases: self.tree(QUEUE_LEASES_TREE)?.root,
            queue_heads: self.tree(QUEUE_HEADS_TREE)?.root,
        };
        self.in_txn(|ctx, txn| body(&mut StreamTxn { ctx, txn, trees }))
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
            &RelRecord::set_engine_root(STREAMS_ENGINE_ID, tree.root),
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
