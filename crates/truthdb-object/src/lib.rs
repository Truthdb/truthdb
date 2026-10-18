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

mod body;
mod codec;
mod gc;

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

pub use body::{BodyInfo, BodyWriter};
pub use gc::GcOutcome;

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

/// The object id stamped into every body page's header. Not a tree.
pub const BODY_PAGES_OBJECT: u32 = FIRST_ENGINE_OBJECT_ID + 0x100;

/// META key of the next body id to hand out.
const NEXT_BODY_KEY: &[u8] = b"next_body";

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
        object_id >= FIRST_ENGINE_OBJECT_ID
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

/// The object engine's view of the locked storage: the core file and the
/// engine state, borrowed disjointly.
pub struct ObjFile<'a> {
    core: &'a mut StorageFile,
    eng: &'a mut ObjEngine,
}

impl<'a> ObjFile<'a> {
    /// # Panics
    /// If the storage was opened without the object engine.
    pub fn new(locked: &'a mut Locked) -> Self {
        let (core, eng) = locked.engine_parts::<ObjEngine>();
        ObjFile { core, eng }
    }

    /// The tree with this object id, creating the directory tree and the
    /// tree itself the first time.
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
    /// succeeds, rolled back when it fails.
    fn in_txn<T>(
        &mut self,
        body: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let roots = self.eng.tree_roots();
        let txn_id = self.core.alloc_txn_id();
        let mut rollback_failed = false;
        let outcome = {
            let mut ctx = self.core.page_ctx(0);
            let mut txn = ctx.begin(txn_id)?;
            match body(&mut ctx, &mut txn) {
                Ok(value) => ctx.commit(txn).map(|_| value),
                Err(err) => match rollback(&mut ctx, txn, &roots) {
                    Ok(()) => Err(err),
                    Err(rollback_err) => {
                        rollback_failed = true;
                        Err(StorageError::InvalidFile(format!(
                            "{err}; and the rollback failed: {rollback_err}"
                        )))
                    }
                },
            }
        };
        if rollback_failed {
            // The pool may hold changes the log cannot explain.
            self.core.set_wedged();
        }
        let value = outcome?;
        self.core.wal_sync_all()?;
        Ok(value)
    }
}

/// Hands out the next body id from the directory tree's counter
/// (transactional: a rolled-back body's id is handed out again).
fn next_body_id(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    meta: &BTree,
) -> Result<u64, StorageError> {
    let current = meta
        .get(ctx, NEXT_BODY_KEY)?
        .map(|row| codec::u64_le(&row))
        .transpose()?;
    let next = current.unwrap_or(1);
    let row = (next + 1).to_le_bytes();
    match current {
        Some(_) => {
            meta.update(ctx, &mut OpMode::Txn(txn), NEXT_BODY_KEY, &row)?;
        }
        None => {
            meta.insert_unique(ctx, &mut OpMode::Txn(txn), NEXT_BODY_KEY, &row)?;
        }
    }
    Ok(next)
}
