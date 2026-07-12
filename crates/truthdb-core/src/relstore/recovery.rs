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
pub(crate) fn analyze_and_redo(
    ctx: &mut RelCtx<'_>,
    records: &[(u64, RelRecord)],
) -> Result<AnalysisRedoOutcome, StorageError> {
    // Analysis: rebuild the active-transaction table.
    let mut att: HashMap<u64, u64> = HashMap::new();
    let mut catalog_root = None;
    let mut max_txn_id = 0u64;
    for (lsn, record) in records {
        max_txn_id = max_txn_id.max(record.txn_id);
        match record.kind {
            REL_KIND_TXN_BEGIN => {
                att.insert(record.txn_id, *lsn);
            }
            REL_KIND_TXN_COMMIT | REL_KIND_TXN_END => {
                att.remove(&record.txn_id);
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
        let mut mode = OpMode::Clr {
            txn: link,
            undo_next: record_lsn,
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
