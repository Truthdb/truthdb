//! Garbage collection: frees the pages of bodies nothing references any more
//! (once their grace period is up and no reader holds a lease) and of staging
//! bodies abandoned mid-write. Each body goes in its own transaction, which
//! frees its extents (landing at commit) and deletes its rows.

use truthdb_core::relstore::btree::BTree;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{self, BodyState, SegmentRow};
use crate::{GC_TREE, ObjFile, ObjTxn, STAGING_TREE};

/// What one garbage-collection pass freed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GcOutcome {
    pub bodies: u64,
    pub pages: u64,
    /// The pass stopped at its limit with due bodies left.
    pub more: bool,
}

impl ObjFile<'_> {
    /// Frees up to `limit` bodies: unreferenced ones due by `now_ms` that no
    /// reader leases, and staging ones untouched since `now_ms -
    /// abandon_after_ms`. Each body is one transaction forced to disk, all
    /// under the caller's one lock hold, so callers bound the hold with
    /// `limit` and call again while [`GcOutcome::more`].
    pub fn collect_garbage(
        &mut self,
        now_ms: u64,
        abandon_after_ms: u64,
        limit: u64,
    ) -> Result<GcOutcome, StorageError> {
        let mut outcome = GcOutcome::default();
        for (queue, upper) in [
            (GC_TREE, now_ms),
            (STAGING_TREE, now_ms.saturating_sub(abandon_after_ms)),
        ] {
            let Some(queue) = self.existing_tree(queue)? else {
                continue;
            };
            let due = {
                let mut ctx = self.core.page_ctx(0);
                queue.scan_range(&mut ctx, None, Some(&codec::timed_key(upper, u64::MAX)))?
            };
            for (key, _) in due {
                let (_, body) = codec::decode_timed_key(&key)?;
                if self.eng.leases.contains_key(&body) {
                    continue;
                }
                if outcome.bodies == limit {
                    outcome.more = true;
                    return Ok(outcome);
                }
                outcome.pages += self.transaction(|tx| tx.free_body(&queue, &key, body))?;
                outcome.bodies += 1;
            }
        }
        Ok(outcome)
    }
}

impl ObjTxn<'_, '_> {
    /// Frees one body's extents and deletes its rows and its row `key` in
    /// `queue`. Returns the pages freed.
    fn free_body(&mut self, queue: &BTree, key: &[u8], body: u64) -> Result<u64, StorageError> {
        let row = crate::body::load_body(self.ctx, &self.trees.bodies(), body)?;
        if row.state == BodyState::Sealed && row.refcount != 0 {
            return Err(StorageError::InvalidFile(format!(
                "body {body} is queued for collection but still referenced"
            )));
        }
        let segments = self.trees.segments();
        let rows = segments.scan_range(
            self.ctx,
            Some(&codec::segment_key(body, 0)),
            Some(&codec::segment_key(body, u64::MAX)),
        )?;
        let mut pages = 0;
        for (segment_key, segment) in rows {
            let segment = SegmentRow::decode(&segment)?;
            self.ctx
                .free_extent_in_txn(self.txn, segment.start_page, segment.pages as u64)?;
            segments.delete(self.ctx, &mut OpMode::Txn(self.txn), &segment_key)?;
            pages += segment.pages as u64;
        }
        let bodies = self.trees.bodies();
        bodies.delete(self.ctx, &mut OpMode::Txn(self.txn), &codec::body_key(body))?;
        queue.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        Ok(pages)
    }
}
