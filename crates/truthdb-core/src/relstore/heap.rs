//! Heap table: unordered rows addressed by stable RIDs (page, slot), pages
//! linked into a chain. Updates that no longer fit in place move the row and
//! leave a forwarding stub at the home slot, so RIDs stay valid.
//!
//! Cell format (1-byte tag prefix):
//! - `0` home row: `[0][row bytes]`
//! - `1` forwarding stub: `[1][target page u64][target slot u16]`
//! - `2` moved row: `[2][home page u64][home slot u16][row bytes]` — the
//!   back-pointer lets deletes and moves keep the home stub consistent.
//!
//! All mutations are physical slot operations logged with physical undos
//! (RIDs are stable, so physical undo is exact).

use crate::relstore::ctx::{LogMode, RelCtx, TxnLink};
use crate::relstore::page::PAGE_TYPE_HEAP;
use crate::relstore::slotted::{NO_PAGE, STRUCT_HEADER_END, SlottedRead};
use crate::storage::StorageError;
use crate::storage_layout::PAGE_SIZE;
use crate::wal::records::{PageOpRedo, PageOpUndo};

const TAG_ROW: u8 = 0;
const TAG_STUB: u8 = 1;
const TAG_MOVED: u8 = 2;

const STUB_LEN: usize = 1 + 8 + 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rid {
    pub page: u64,
    pub slot: u16,
}

pub(crate) struct Heap {
    pub object_id: u32,
    pub first_page: u64,
}

fn row_cell(row: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(1 + row.len());
    cell.push(TAG_ROW);
    cell.extend_from_slice(row);
    cell
}

fn stub_cell(target: Rid) -> Vec<u8> {
    let mut cell = Vec::with_capacity(STUB_LEN);
    cell.push(TAG_STUB);
    cell.extend_from_slice(&target.page.to_le_bytes());
    cell.extend_from_slice(&target.slot.to_le_bytes());
    cell
}

fn moved_cell(home: Rid, row: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(11 + row.len());
    cell.push(TAG_MOVED);
    cell.extend_from_slice(&home.page.to_le_bytes());
    cell.extend_from_slice(&home.slot.to_le_bytes());
    cell.extend_from_slice(row);
    cell
}

fn parse_rid(bytes: &[u8]) -> Rid {
    Rid {
        page: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        slot: u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
    }
}

impl Heap {
    /// Creates the heap's first page (logged as a system image).
    pub fn create(ctx: &mut RelCtx<'_>, object_id: u32) -> Result<Heap, StorageError> {
        let first_page = ctx.allocate_page(0)?;
        let frame = ctx.format_page(first_page, PAGE_TYPE_HEAP, object_id, 0)?;
        ctx.pool.unpin(frame);
        ctx.log_system_image(first_page)?;
        Ok(Heap {
            object_id,
            first_page,
        })
    }

    /// Inserts a row, returning its RID.
    pub fn insert(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        row: &[u8],
    ) -> Result<Rid, StorageError> {
        let cell = row_cell(row);
        let (page_no, slot) = self.place_cell(ctx, txn, &cell)?;
        Ok(Rid {
            page: page_no,
            slot,
        })
    }

    /// Finds (or allocates) a page with room for `cell` and inserts it,
    /// logging the insert with an undo. Returns (page, slot).
    fn place_cell(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        cell: &[u8],
    ) -> Result<(u64, u16), StorageError> {
        if cell.len() + 4 > PAGE_SIZE - STRUCT_HEADER_END {
            return Err(StorageError::InvalidConfig(
                "row too large for a heap page".to_string(),
            ));
        }
        let mut page_no = self.first_page;
        loop {
            let frame = ctx.fetch(page_no)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            // Choose the slot without mutating: the actual mutation goes
            // through apply_op so it is logged.
            let slot = if page.total_free() >= cell.len() + 4 {
                Some(next_stable_slot(&page))
            } else {
                None
            };
            let next = page.next_page();
            ctx.pool.unpin(frame);

            if let Some(slot) = slot {
                ctx.apply_op(
                    LogMode::Txn(
                        txn,
                        PageOpUndo::HeapDeleteSlot {
                            page: page_no,
                            slot: slot as u16,
                        },
                    ),
                    PageOpRedo::HeapInsert {
                        page: page_no,
                        slot: slot as u16,
                        bytes: cell.to_vec(),
                    },
                )?;
                return Ok((page_no, slot as u16));
            }
            if next == NO_PAGE {
                // Grow the chain: new page (system image), then link it.
                let new_page = ctx.allocate_page(txn.txn_id)?;
                let frame = ctx.format_page(new_page, PAGE_TYPE_HEAP, self.object_id, 0)?;
                ctx.pool.unpin(frame);
                ctx.log_system_image(new_page)?;
                ctx.apply_op(
                    LogMode::System,
                    PageOpRedo::SetNextPage {
                        page: page_no,
                        next: new_page,
                    },
                )?;
                page_no = new_page;
            } else {
                page_no = next;
            }
        }
    }

    fn read_cell(&self, ctx: &mut RelCtx<'_>, rid: Rid) -> Result<Option<Vec<u8>>, StorageError> {
        let frame = ctx.fetch(rid.page)?;
        let page = SlottedRead::new(ctx.pool.page(frame));
        let copy = if (rid.slot as usize) < page.slot_count() {
            page.get(rid.slot as usize).map(|c| c.to_vec())
        } else {
            None
        };
        ctx.pool.unpin(frame);
        Ok(copy)
    }

    /// Deletes the row at `rid` (home slot), including a moved copy.
    /// Returns false if already gone.
    pub fn delete(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        rid: Rid,
    ) -> Result<bool, StorageError> {
        let Some(cell) = self.read_cell(ctx, rid)? else {
            return Ok(false);
        };
        match cell[0] {
            TAG_ROW | TAG_MOVED => {
                self.delete_cell(ctx, txn, rid, &cell)?;
                Ok(true)
            }
            TAG_STUB => {
                let target = parse_rid(&cell[1..]);
                if let Some(target_cell) = self.read_cell(ctx, target)? {
                    self.delete_cell(ctx, txn, target, &target_cell)?;
                }
                self.delete_cell(ctx, txn, rid, &cell)?;
                Ok(true)
            }
            other => Err(StorageError::InvalidFile(format!(
                "unknown heap cell tag {other}"
            ))),
        }
    }

    fn delete_cell(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        rid: Rid,
        old_cell: &[u8],
    ) -> Result<(), StorageError> {
        ctx.apply_op(
            LogMode::Txn(
                txn,
                PageOpUndo::HeapInsertRow {
                    page: rid.page,
                    slot: rid.slot,
                    bytes: old_cell.to_vec(),
                },
            ),
            PageOpRedo::HeapDelete {
                page: rid.page,
                slot: rid.slot,
            },
        )?;
        Ok(())
    }

    /// Updates the row at `rid` in place when possible, otherwise moves it
    /// and leaves/updates the forwarding stub. Returns false if the row is
    /// gone.
    pub fn update(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        rid: Rid,
        new_row: &[u8],
    ) -> Result<bool, StorageError> {
        let Some(cell) = self.read_cell(ctx, rid)? else {
            return Ok(false);
        };
        match cell[0] {
            TAG_ROW => {
                let new_cell = row_cell(new_row);
                if self.update_fits(ctx, rid, new_cell.len())? {
                    self.update_cell(ctx, txn, rid, &cell, &new_cell)?;
                } else {
                    // Move: place a moved-cell elsewhere, shrink home to a
                    // stub. A stub can be BIGGER than a tiny home cell on a
                    // full page — check before mutating anything so the
                    // statement fails cleanly instead of half-applied.
                    if !self.update_fits(ctx, rid, STUB_LEN)? {
                        return Err(StorageError::Constraint(format!(
                            "row cannot grow: page {} is too full to hold a forwarding stub",
                            rid.page
                        )));
                    }
                    let moved = moved_cell(rid, new_row);
                    let (page, slot) = self.place_cell(ctx, txn, &moved)?;
                    let stub = stub_cell(Rid { page, slot });
                    self.update_cell(ctx, txn, rid, &cell, &stub)?;
                }
                Ok(true)
            }
            TAG_MOVED => {
                let home = parse_rid(&cell[1..]);
                let new_cell = moved_cell(home, new_row);
                if self.update_fits(ctx, rid, new_cell.len())? {
                    self.update_cell(ctx, txn, rid, &cell, &new_cell)?;
                } else {
                    let (page, slot) = self.place_cell(ctx, txn, &new_cell)?;
                    // Repoint the home stub, then drop the old copy.
                    let home_cell = self.read_cell(ctx, home)?.ok_or_else(|| {
                        StorageError::InvalidFile("moved row without home stub".to_string())
                    })?;
                    self.update_cell(ctx, txn, home, &home_cell, &stub_cell(Rid { page, slot }))?;
                    self.delete_cell(ctx, txn, rid, &cell)?;
                }
                Ok(true)
            }
            TAG_STUB => {
                let target = parse_rid(&cell[1..]);
                if self.read_cell(ctx, target)?.is_none() {
                    return Ok(false);
                }
                self.update(ctx, txn, target, new_row)
            }
            other => Err(StorageError::InvalidFile(format!(
                "unknown heap cell tag {other}"
            ))),
        }
    }

    fn update_fits(
        &self,
        ctx: &mut RelCtx<'_>,
        rid: Rid,
        new_len: usize,
    ) -> Result<bool, StorageError> {
        let frame = ctx.fetch(rid.page)?;
        let page = SlottedRead::new(ctx.pool.page(frame));
        let old_len = page.get(rid.slot as usize).map(|c| c.len()).unwrap_or(0);
        let fits = new_len <= old_len || page.total_free() + old_len >= new_len;
        ctx.pool.unpin(frame);
        Ok(fits)
    }

    fn update_cell(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        rid: Rid,
        old_cell: &[u8],
        new_cell: &[u8],
    ) -> Result<(), StorageError> {
        ctx.apply_op(
            LogMode::Txn(
                txn,
                PageOpUndo::HeapUpdateRow {
                    page: rid.page,
                    slot: rid.slot,
                    bytes: old_cell.to_vec(),
                },
            ),
            PageOpRedo::HeapUpdate {
                page: rid.page,
                slot: rid.slot,
                bytes: new_cell.to_vec(),
            },
        )?;
        Ok(())
    }

    /// Reads the row at a home RID, following a forwarding stub to its moved
    /// copy. Returns the raw row bytes, or None if the slot is empty. Used by
    /// index key lookups (the index stores home RIDs).
    pub fn read_row(
        &self,
        ctx: &mut RelCtx<'_>,
        rid: Rid,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(cell) = self.read_cell(ctx, rid)? else {
            return Ok(None);
        };
        match cell[0] {
            TAG_ROW => Ok(Some(cell[1..].to_vec())),
            TAG_MOVED => Ok(Some(cell[11..].to_vec())),
            TAG_STUB => {
                let target = parse_rid(&cell[1..]);
                match self.read_cell(ctx, target)? {
                    Some(target_cell) if target_cell[0] == TAG_MOVED => {
                        Ok(Some(target_cell[11..].to_vec()))
                    }
                    _ => Ok(None),
                }
            }
            other => Err(StorageError::InvalidFile(format!(
                "unknown heap cell tag {other}"
            ))),
        }
    }

    /// Scans all rows: (home RID, row bytes). Moved rows report their home
    /// RID; stubs and tombstones are skipped.
    pub fn scan(&self, ctx: &mut RelCtx<'_>) -> Result<Vec<(Rid, Vec<u8>)>, StorageError> {
        let mut out = Vec::new();
        let mut page_no = self.first_page;
        while page_no != NO_PAGE {
            let frame = ctx.fetch(page_no)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            let next = page.next_page();
            let mut bad_tag = None;
            for slot in 0..page.slot_count() {
                let Some(cell) = page.get(slot) else { continue };
                match cell[0] {
                    TAG_ROW => out.push((
                        Rid {
                            page: page_no,
                            slot: slot as u16,
                        },
                        cell[1..].to_vec(),
                    )),
                    TAG_MOVED => out.push((parse_rid(&cell[1..]), cell[11..].to_vec())),
                    TAG_STUB => {}
                    other => {
                        bad_tag = Some(other);
                        break;
                    }
                }
            }
            ctx.pool.unpin(frame);
            if let Some(tag) = bad_tag {
                return Err(StorageError::InvalidFile(format!(
                    "unknown heap cell tag {tag}"
                )));
            }
            page_no = next;
        }
        Ok(out)
    }
}

/// Picks the slot a stable insert will use without mutating the page
/// (mirrors `SlottedPage::insert_stable`).
fn next_stable_slot(page: &SlottedRead<'_>) -> usize {
    for i in 0..page.slot_count() {
        if page.get(i).is_none() {
            return i;
        }
    }
    page.slot_count()
}
