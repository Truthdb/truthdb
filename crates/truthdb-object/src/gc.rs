//! Garbage collection: frees the pages of bodies nothing references any more
//! (once their grace period is up and no reader holds a lease) and of staging
//! bodies abandoned mid-write. Each body goes in its own transaction, which
//! frees its extents (landing at commit) and deletes its rows.

use truthdb_core::relstore::btree::BTree;
use truthdb_core::relstore::ctx::{OpMode, RelCtx, TxnLink};
use truthdb_core::storage::StorageError;

use crate::body::load_body;
use crate::codec::{self, BodyState, SegmentRow};
use crate::{BODIES_TREE, GC_TREE, ObjFile, SEGMENTS_TREE, STAGING_TREE};

/// What one garbage-collection pass freed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GcOutcome {
    pub bodies: u64,
    pub pages: u64,
}

impl ObjFile<'_> {
    /// Frees every unreferenced body due by `now_ms` that no reader leases, and
    /// every staging body started before `now_ms - abandon_after_ms`.
    pub fn collect_garbage(
        &mut self,
        now_ms: u64,
        abandon_after_ms: u64,
    ) -> Result<GcOutcome, StorageError> {
        let gc = self.tree(GC_TREE)?;
        let staging = self.tree(STAGING_TREE)?;
        let mut outcome = GcOutcome::default();
        for (tree, upper) in [
            (gc, now_ms),
            (staging, now_ms.saturating_sub(abandon_after_ms)),
        ] {
            let due = {
                let mut ctx = self.core.page_ctx(0);
                tree.scan_range(&mut ctx, None, Some(&codec::timed_key(upper, u64::MAX)))?
            };
            for (key, _) in due {
                let (_, body) = codec::decode_timed_key(&key)?;
                if self.eng.leases.contains_key(&body) {
                    continue;
                }
                outcome.pages += self.free_body(&tree, &key, body)?;
                outcome.bodies += 1;
            }
        }
        Ok(outcome)
    }

    /// Frees one body's extents and deletes its rows and its queue row `key`
    /// in `queue`, in one transaction. Returns the pages freed.
    fn free_body(&mut self, queue: &BTree, key: &[u8], body: u64) -> Result<u64, StorageError> {
        let bodies = self.tree(BODIES_TREE)?;
        let segments = self.tree(SEGMENTS_TREE)?;
        self.in_txn(|ctx, txn| {
            let row = load_body(ctx, &bodies, body)?;
            if row.state == BodyState::Sealed && row.refcount != 0 {
                return Err(StorageError::InvalidFile(format!(
                    "body {body} is queued for collection but still referenced"
                )));
            }
            let pages = free_segments(ctx, txn, &segments, body)?;
            bodies.delete(ctx, &mut OpMode::Txn(txn), &codec::body_key(body))?;
            queue.delete(ctx, &mut OpMode::Txn(txn), key)?;
            Ok(pages)
        })
    }
}

/// Frees every segment's extent of `body` and deletes the segment rows.
fn free_segments(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    segments: &BTree,
    body: u64,
) -> Result<u64, StorageError> {
    let rows = segments.scan_range(
        ctx,
        Some(&codec::segment_key(body, 0)),
        Some(&codec::segment_key(body, u64::MAX)),
    )?;
    let mut pages = 0;
    for (key, row) in rows {
        let segment = SegmentRow::decode(&row)?;
        ctx.free_extent_in_txn(txn, segment.start_page, segment.pages as u64)?;
        segments.delete(ctx, &mut OpMode::Txn(txn), &key)?;
        pages += segment.pages as u64;
    }
    Ok(pages)
}
