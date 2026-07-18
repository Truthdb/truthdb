//! ARIES restart for the relational store: analysis, LSN-gated redo
//! (repeating history, including CLRs), and undo of loser transactions with
//! compensation logging.
//!
//! The WAL head is the recovery horizon: checkpoints flush every dirty
//! relational page and only advance the head past records no crash could
//! still need (no active transactions can span a checkpoint while the engine
//! is single-threaded), so analysis simply scans everything from the head.
//!
//! Undo of one record may need several page ops (logical tree undos can
//! split pages). Every CLR of that group carries `undo_next = record.lsn`
//! (re-run the whole group on a crash — the ops are written to tolerate
//! partial prior application), and a final no-op CLR with
//! `undo_next = record.prev_lsn` seals the group.

use std::collections::HashMap;

use crate::relstore::btree::{BTree, apply_tree_undo};
use crate::relstore::ctx::{OpMode, RelCtx, TxnLink, apply_redo_to_page};
use crate::relstore::page;
use crate::relstore::slotted::SlottedRead;
use crate::storage::StorageError;
use crate::wal::records::{
    PageOpRedo, PageOpUndo, REL_KIND_ALLOC_EXTENT, REL_KIND_CLR, REL_KIND_FREE_EXTENT,
    REL_KIND_PAGE_IMAGE, REL_KIND_PAGE_IMAGES, REL_KIND_PAGE_OP, REL_KIND_SET_CATALOG_ROOT,
    REL_KIND_TXN_BEGIN, REL_KIND_TXN_COMMIT, REL_KIND_TXN_END, RelRecord,
};

pub(crate) struct AnalysisRedoOutcome {
    /// Loser transactions: id -> last LSN of their chain.
    pub losers: HashMap<u64, u64>,
    /// Catalog root from the newest SET_CATALOG_ROOT record, if any.
    pub catalog_root: Option<u64>,
    /// Highest transaction id seen (0 when none).
    pub max_txn_id: u64,
}

/// Analysis + redo over the recovered records (`(lsn, record)`, log order).
///
/// `stop_at` drives point-in-time restore. Recovery keeps a log *prefix*: the
/// first commit (in log order) whose wall-clock timestamp is past `stop_at`
/// marks the cut, and that commit plus every later or still-uncommitted
/// transaction is a loser. Redo still repeats ALL history and undo rolls those
/// losers back with CLRs, so the point-in-time state persists across a later
/// normal reopen. Cutting by LSN — not by each commit's own timestamp — keeps
/// the restore internally consistent even when commit timestamps run backwards
/// across increasing LSN (a non-monotonic clock): a transaction is never kept
/// while an earlier one it depended on is dropped. `None` recovers to the end of
/// the log (normal open).
pub(crate) fn analyze_and_redo(
    ctx: &mut RelCtx<'_>,
    records: &[(u64, RelRecord)],
    stop_at: Option<u64>,
) -> Result<AnalysisRedoOutcome, StorageError> {
    // The point-in-time cut: every transaction committed at or after this LSN is
    // undone. `u64::MAX` (no stop, or a stop past every commit) keeps them all.
    let cut_lsn = match stop_at {
        Some(limit) => first_commit_past(records, limit)?,
        None => u64::MAX,
    };

    // Analysis: rebuild the active-transaction table.
    let mut att: HashMap<u64, u64> = HashMap::new();
    let mut catalog_root = None;
    let mut max_txn_id = 0u64;
    // Transactions committed at or past the cut: losers whose later TXN_END must
    // not clear them from the ATT.
    let mut forced_losers: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for (lsn, record) in records {
        max_txn_id = max_txn_id.max(record.txn_id);
        match record.kind {
            REL_KIND_TXN_BEGIN => {
                att.insert(record.txn_id, *lsn);
            }
            REL_KIND_TXN_COMMIT => {
                // A commit at or after the point-in-time cut does not count: keep
                // the txn in the ATT so undo rolls it back.
                if *lsn >= cut_lsn {
                    forced_losers.insert(record.txn_id);
                } else {
                    att.remove(&record.txn_id);
                }
            }
            REL_KIND_TXN_END => {
                if !forced_losers.contains(&record.txn_id) {
                    att.remove(&record.txn_id);
                }
            }
            REL_KIND_PAGE_OP | REL_KIND_PAGE_IMAGE | REL_KIND_PAGE_IMAGES | REL_KIND_CLR => {
                if record.txn_id != 0 {
                    att.insert(record.txn_id, *lsn);
                }
            }
            REL_KIND_SET_CATALOG_ROOT => {
                catalog_root = Some(record.decode_catalog_root()?);
            }
            REL_KIND_ALLOC_EXTENT | REL_KIND_FREE_EXTENT => {}
            other => {
                return Err(StorageError::InvalidFile(format!(
                    "unknown rel record kind {other} during analysis"
                )));
            }
        }
    }

    // Redo: repeat history in log order, gated by each page's LSN.
    for (lsn, record) in records {
        match record.kind {
            REL_KIND_PAGE_IMAGE => {
                let (page_no, image) = record.decode_page_image()?;
                redo_page_image(ctx, *lsn, page_no, image)?;
            }
            REL_KIND_PAGE_IMAGES => {
                for (page_no, image) in record.decode_page_images()? {
                    redo_page_image(ctx, *lsn, page_no, image)?;
                }
            }
            REL_KIND_PAGE_OP => {
                let redo = record.decode_page_op_redo()?;
                redo_page_op(ctx, *lsn, &redo)?;
            }
            REL_KIND_CLR => {
                let (_, redo) = record.decode_clr()?;
                if let Some(redo) = redo {
                    redo_page_op(ctx, *lsn, &redo)?;
                }
            }
            _ => {}
        }
    }

    Ok(AnalysisRedoOutcome {
        losers: att,
        catalog_root,
        max_txn_id,
    })
}

/// The LSN of the first commit record (lowest LSN) whose wall-clock timestamp is
/// strictly past `limit`, or `u64::MAX` when none is. Errors if any commit record
/// carries no timestamp (a pre-timestamp WAL entry): point-in-time restore cannot
/// place such a commit relative to the stop point, and silently treating it as a
/// winner would keep post-stop data.
fn first_commit_past(records: &[(u64, RelRecord)], limit: u64) -> Result<u64, StorageError> {
    let mut cut = u64::MAX;
    for (lsn, record) in records {
        if record.kind != REL_KIND_TXN_COMMIT {
            continue;
        }
        match record.commit_timestamp_millis() {
            Some(ts) => {
                if ts > limit && *lsn < cut {
                    cut = *lsn;
                }
            }
            None => {
                return Err(StorageError::InvalidFile(
                    "point-in-time restore requires timestamped commit records, but the \
                     recoverable log contains a commit without a timestamp (the backup \
                     predates the timestamped-commit WAL format)"
                        .to_string(),
                ));
            }
        }
    }
    Ok(cut)
}

/// Full-image redo: replaces the page wholesale when its LSN is older —
/// including pages whose on-disk state is torn (checksum-invalid), which is
/// exactly what the image is there to repair.
fn redo_page_image(
    ctx: &mut RelCtx<'_>,
    lsn: u64,
    page_no: u64,
    image: &[u8],
) -> Result<(), StorageError> {
    let frame = match ctx.fetch(page_no) {
        Ok(frame) => {
            if page::page_lsn(ctx.pool.page(frame)) >= lsn {
                ctx.pool.unpin(frame);
                return Ok(());
            }
            frame
        }
        // Torn page: take a zeroed frame and repair from the image.
        Err(_) => ctx.fetch_zeroed(page_no)?,
    };
    let bytes = ctx.pool.page_mut(frame);
    bytes.copy_from_slice(image);
    let mut header = page::read_header(bytes);
    header.page_lsn = lsn;
    page::write_header(bytes, &header);
    ctx.pool.unpin(frame);
    ctx.dpt.entry(page_no).or_insert(lsn);
    Ok(())
}

fn redo_page_op(ctx: &mut RelCtx<'_>, lsn: u64, redo: &PageOpRedo) -> Result<(), StorageError> {
    let page_no = redo.page();
    let frame = ctx.fetch(page_no)?;
    let bytes = ctx.pool.page(frame);
    if page::page_lsn(bytes) >= lsn {
        ctx.pool.unpin(frame);
        return Ok(());
    }
    let bytes = ctx.pool.page_mut(frame);
    let applied = apply_redo_to_page(bytes, redo);
    if applied.is_ok() {
        let mut header = page::read_header(bytes);
        header.page_lsn = lsn;
        page::write_header(bytes, &header);
    }
    ctx.pool.unpin(frame);
    applied?;
    ctx.dpt.entry(page_no).or_insert(lsn);
    Ok(())
}

/// Undoes every loser transaction, logging CLRs, ending each with a TXN_END.
/// `tree_roots` maps object ids to their (stable) root pages for logical
/// tree undos.
pub(crate) fn undo_losers(
    ctx: &mut RelCtx<'_>,
    records: &[(u64, RelRecord)],
    losers: &HashMap<u64, u64>,
    tree_roots: &HashMap<u32, u64>,
) -> Result<(), StorageError> {
    // Undo may append CLRs into a ring that a forward statement filled:
    // compensation records are allowed into the WAL's reserve.
    ctx.use_reserve = true;
    let by_lsn: HashMap<u64, &RelRecord> =
        records.iter().map(|(lsn, record)| (*lsn, record)).collect();

    // cursor = next record to undo; link = the CLR chain tail.
    let mut active: HashMap<u64, (u64, TxnLink)> = losers
        .iter()
        .map(|(&txn_id, &last_lsn)| {
            (
                txn_id,
                (
                    last_lsn,
                    TxnLink {
                        txn_id,
                        last_lsn,
                        undo_log: Vec::new(),
                        pending_versions: Vec::new(),
                        published_versions: Vec::new(),
                    },
                ),
            )
        })
        .collect();

    // Single backward sweep: always continue with the globally largest
    // cursor (ARIES multi-transaction undo).
    loop {
        let Some((txn_id, cursor)) = active
            .iter()
            .map(|(&txn_id, &(cursor, _))| (txn_id, cursor))
            .max_by_key(|&(_, cursor)| cursor)
        else {
            break;
        };
        let record = *by_lsn.get(&cursor).ok_or_else(|| {
            StorageError::InvalidFile(format!(
                "undo needs record at lsn {cursor}, which is no longer in the wal"
            ))
        })?;
        let next_cursor = match record.kind {
            REL_KIND_CLR => record.decode_clr()?.0,
            REL_KIND_PAGE_OP | REL_KIND_PAGE_IMAGE => {
                let undo = record.decode_page_op_undo()?;
                let (_, link) = active.get_mut(&txn_id).expect("active loser");
                undo_one(ctx, link, &undo, cursor, record.prev_lsn, tree_roots)?;
                record.prev_lsn
            }
            REL_KIND_TXN_BEGIN => 0,
            _ => record.prev_lsn,
        };
        if next_cursor == 0 {
            let (_, link) = active.remove(&txn_id).expect("active loser");
            ctx.append(&RelRecord::txn_end(txn_id, link.last_lsn), false)?;
        } else {
            active.get_mut(&txn_id).expect("active loser").0 = next_cursor;
        }
    }
    Ok(())
}

/// Undoes one record as a CLR group: the compensating ops (tolerant of
/// partial prior application), then a sealing no-op CLR that moves the undo
/// cursor past the record.
pub(crate) fn undo_one(
    ctx: &mut RelCtx<'_>,
    link: &mut TxnLink,
    undo: &PageOpUndo,
    record_lsn: u64,
    record_prev: u64,
    tree_roots: &HashMap<u32, u64>,
) -> Result<(), StorageError> {
    {
        // In-group CLRs point AT the record (`record_lsn`): the group re-runs
        // after a crash, and each op tolerates partial prior application —
        // heap arms by occupancy guard, tree arms by logical presence check.
        // A counter compensation is a blind arithmetic delta and CANNOT: a
        // crash after its CLR but before the sealing no-op would re-undo the
        // record and apply the delta twice, permanently. Its group is exactly
        // one op, so its CLR points *past* the compensated record instead
        // (textbook ARIES): a re-run resumes at `record_prev` and can never
        // revisit it.
        let undo_next = match undo {
            PageOpUndo::CounterAdd { .. } => record_prev,
            _ => record_lsn,
        };
        let mut mode = OpMode::Clr {
            txn: link,
            undo_next,
        };
        match undo {
            PageOpUndo::None => {}
            PageOpUndo::TreeDeleteKey { object_id, .. }
            | PageOpUndo::TreeInsertRow { object_id, .. }
            | PageOpUndo::TreeUpdateRow { object_id, .. } => {
                let root = *tree_roots.get(object_id).ok_or_else(|| {
                    StorageError::InvalidFile(format!(
                        "undo references unknown tree object {object_id}"
                    ))
                })?;
                let tree = BTree {
                    object_id: *object_id,
                    root,
                };
                apply_tree_undo(ctx, &mut mode, &tree, undo)?;
            }
            PageOpUndo::CounterAdd { page, delta } => {
                // The inverse delta was baked in when the undo record was
                // built. This CLR's `undo_next` is `record_prev` (see above),
                // which is what makes the compensation exactly-once across a
                // crash mid-undo.
                ctx.apply_op(
                    mode.log_mode(PageOpUndo::None),
                    PageOpRedo::CounterAdd {
                        page: *page,
                        delta: *delta,
                    },
                )?;
            }
            PageOpUndo::HeapDeleteSlot { page, slot } => {
                if heap_slot_occupied(ctx, *page, *slot)? {
                    ctx.apply_op(
                        mode.log_mode(PageOpUndo::None),
                        PageOpRedo::HeapDelete {
                            page: *page,
                            slot: *slot,
                        },
                    )?;
                }
            }
            PageOpUndo::HeapInsertRow { page, slot, bytes } => {
                if !heap_slot_occupied(ctx, *page, *slot)? {
                    ctx.apply_op(
                        mode.log_mode(PageOpUndo::None),
                        PageOpRedo::HeapInsert {
                            page: *page,
                            slot: *slot,
                            bytes: bytes.clone(),
                        },
                    )?;
                }
            }
            PageOpUndo::HeapUpdateRow { page, slot, bytes } => {
                ctx.apply_op(
                    mode.log_mode(PageOpUndo::None),
                    PageOpRedo::HeapUpdate {
                        page: *page,
                        slot: *slot,
                        bytes: bytes.clone(),
                    },
                )?;
            }
        }
    }
    // Seal the group: the undo cursor may now move past the record.
    let seal = RelRecord::clr_noop(link.txn_id, link.last_lsn, record_prev);
    link.last_lsn = ctx.append(&seal, false)?;
    Ok(())
}

/// Rolls a live transaction back to a [`Savepoint`], undoing only the work done
/// since it was taken (statement-level atomicity) while the transaction stays
/// open. Undoes the undo-log suffix in reverse with the same CLR discipline as a
/// full rollback, so a crash before the eventual commit/rollback recovers
/// correctly: each compensation CLR's `undo_next` points before the undone op, so
/// recovery's undo skips it (never double-undoing). The undo-log suffix is
/// dropped, so a later full rollback unwinds only the surviving prefix.
pub(crate) fn rollback_to(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    savepoint: crate::relstore::ctx::Savepoint,
    tree_roots: &HashMap<u32, u64>,
) -> Result<(), StorageError> {
    ctx.use_reserve = true;
    // Detach the suffix (work done after the savepoint) and undo it tail-first.
    let suffix: Vec<(u64, PageOpUndo)> = txn.undo_log.split_off(savepoint.undo_len);
    for (index, (lsn, undo)) in suffix.iter().enumerate().rev() {
        // The predecessor of the first undone op is the savepoint's chain tail,
        // so its sealing CLR moves the undo cursor back to the savepoint.
        let prev = if index == 0 {
            savepoint.last_lsn
        } else {
            suffix[index - 1].0
        };
        undo_one(ctx, txn, undo, *lsn, prev, tree_roots)?;
    }
    Ok(())
}

/// Rolls back a live transaction (statement failure) using its in-memory
/// undo log; same CLR discipline as recovery undo.
pub(crate) fn rollback(
    ctx: &mut RelCtx<'_>,
    mut txn: TxnLink,
    tree_roots: &HashMap<u32, u64>,
) -> Result<(), StorageError> {
    ctx.use_reserve = true;
    let entries: Vec<(u64, PageOpUndo)> = std::mem::take(&mut txn.undo_log);
    let begin_prev = 0u64;
    for (index, (lsn, undo)) in entries.iter().enumerate().rev() {
        let prev = if index == 0 {
            begin_prev
        } else {
            entries[index - 1].0
        };
        undo_one(ctx, &mut txn, undo, *lsn, prev, tree_roots)?;
    }
    ctx.append(&RelRecord::txn_end(txn.txn_id, txn.last_lsn), false)?;
    Ok(())
}

fn heap_slot_occupied(ctx: &mut RelCtx<'_>, page_no: u64, slot: u16) -> Result<bool, StorageError> {
    let frame = ctx.fetch(page_no)?;
    let page = SlottedRead::new(ctx.pool.page(frame));
    let occupied = (slot as usize) < page.slot_count() && page.get(slot as usize).is_some();
    ctx.pool.unpin(frame);
    Ok(occupied)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A version-2 commit record at `lsn` carrying `ts`.
    fn commit(lsn: u64, ts: u64) -> (u64, RelRecord) {
        (lsn, RelRecord::txn_commit(1, 0, ts))
    }

    /// A begin record at `lsn` (no timestamp) — the cut ignores non-commits.
    fn begin(lsn: u64) -> (u64, RelRecord) {
        (lsn, RelRecord::txn_begin(1))
    }

    #[test]
    fn cut_is_the_first_commit_over_the_limit() {
        let records = [
            begin(5),
            commit(10, 100),
            begin(15),
            commit(20, 200),
            commit(30, 300),
        ];
        assert_eq!(first_commit_past(&records, 150).unwrap(), 20);
        // A limit at a commit's exact timestamp keeps that commit (strictly past).
        assert_eq!(first_commit_past(&records, 200).unwrap(), 30);
    }

    #[test]
    fn cut_is_the_lowest_lsn_past_the_limit_under_a_backward_clock() {
        // The clock stepped back: LSN 20 committed at ts 100, earlier than LSN
        // 10's ts 300. The cut must be the lowest LSN whose ts is past the limit
        // (10), so that the later-LSN, lower-ts commit at 20 is also undone —
        // a log prefix, never a non-suffix loser set.
        let records = [commit(10, 300), commit(20, 100)];
        assert_eq!(first_commit_past(&records, 100).unwrap(), 10);
    }

    #[test]
    fn no_cut_when_every_commit_is_within_the_limit() {
        let records = [commit(10, 100), commit(20, 200)];
        assert_eq!(first_commit_past(&records, 500).unwrap(), u64::MAX);
    }

    #[test]
    fn a_timestampless_commit_is_rejected() {
        let v1_commit = RelRecord {
            prev_lsn: 0,
            txn_id: 1,
            kind: REL_KIND_TXN_COMMIT,
            flags: 0,
            redo: Vec::new(),
            undo: Vec::new(),
        };
        let records = [commit(10, 100), (20, v1_commit)];
        assert!(first_commit_past(&records, 50).is_err());
    }
}
