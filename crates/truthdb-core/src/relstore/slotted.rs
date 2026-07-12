//! Slotted page layout shared by B+ tree and heap pages.
//!
//! ```text
//! 0..32    page header (checksum, page_lsn, type, object_id, page_no)
//! 32..48   structure header: next_page u64 | slot_count u16 |
//!          data_start u16 | level u16 | pad u16
//! 48..     slot directory, 4 bytes per slot: offset u16 | len u16
//! ...      free space
//! data_start..4096  cell data, packed downward from the page end
//! ```
//!
//! Two slot disciplines share the format:
//! - *positional* (B+ tree): the directory is kept sorted; inserts shift
//!   directory entries, so indices are positions, not identities.
//! - *stable* (heap): a slot index is a row identity (RID); deletes leave a
//!   tombstone (`offset == 0`) that later inserts may reuse.

use crate::relstore::page::PAGE_HEADER_SIZE;
use crate::storage_layout::PAGE_SIZE;

pub const STRUCT_HEADER_END: usize = PAGE_HEADER_SIZE + 16;
const SLOT_SIZE: usize = 4;

const NEXT_PAGE_AT: usize = PAGE_HEADER_SIZE;
const SLOT_COUNT_AT: usize = PAGE_HEADER_SIZE + 8;
const DATA_START_AT: usize = PAGE_HEADER_SIZE + 10;
const LEVEL_AT: usize = PAGE_HEADER_SIZE + 12;

/// No next page (page numbers are data-region relative; 0 is a valid page,
/// so the sentinel is all-ones).
pub const NO_PAGE: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageFull;

pub struct SlottedPage<'a> {
    data: &'a mut [u8],
}

impl<'a> SlottedPage<'a> {
    /// Wraps an existing formatted page.
    pub fn new(data: &'a mut [u8]) -> Self {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        SlottedPage { data }
    }

    /// Formats a fresh page: empty directory, full cell area, no next page.
    /// The caller sets the page header (type/object/page_no) separately.
    pub fn format(data: &'a mut [u8], level: u16) -> Self {
        let mut page = SlottedPage::new(data);
        page.set_next_page(NO_PAGE);
        page.set_u16(SLOT_COUNT_AT, 0);
        page.set_u16(DATA_START_AT, PAGE_SIZE as u16);
        page.set_u16(LEVEL_AT, level);
        page
    }

    pub fn next_page(&self) -> u64 {
        u64::from_le_bytes(
            self.data[NEXT_PAGE_AT..NEXT_PAGE_AT + 8]
                .try_into()
                .unwrap(),
        )
    }

    pub fn set_next_page(&mut self, next: u64) {
        self.data[NEXT_PAGE_AT..NEXT_PAGE_AT + 8].copy_from_slice(&next.to_le_bytes());
    }

    pub fn level(&self) -> u16 {
        self.get_u16(LEVEL_AT)
    }

    pub fn slot_count(&self) -> usize {
        self.get_u16(SLOT_COUNT_AT) as usize
    }

    fn data_start(&self) -> usize {
        self.get_u16(DATA_START_AT) as usize
    }

    /// Contiguous free bytes between the directory and the cell area.
    pub fn contiguous_free(&self) -> usize {
        self.data_start() - (STRUCT_HEADER_END + self.slot_count() * SLOT_SIZE)
    }

    /// Total reclaimable free bytes (contiguous + tombstone holes).
    pub fn total_free(&self) -> usize {
        let live: usize = (0..self.slot_count())
            .filter_map(|i| self.slot(i))
            .map(|(_, len)| len)
            .sum();
        PAGE_SIZE - STRUCT_HEADER_END - self.slot_count() * SLOT_SIZE - live
    }

    fn slot(&self, index: usize) -> Option<(usize, usize)> {
        let at = STRUCT_HEADER_END + index * SLOT_SIZE;
        let offset = u16::from_le_bytes([self.data[at], self.data[at + 1]]) as usize;
        let len = u16::from_le_bytes([self.data[at + 2], self.data[at + 3]]) as usize;
        if offset == 0 {
            None
        } else {
            Some((offset, len))
        }
    }

    fn set_slot(&mut self, index: usize, offset: usize, len: usize) {
        let at = STRUCT_HEADER_END + index * SLOT_SIZE;
        self.data[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        self.data[at + 2..at + 4].copy_from_slice(&(len as u16).to_le_bytes());
    }

    /// Cell bytes at a slot; `None` for tombstones.
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        debug_assert!(index < self.slot_count());
        let (offset, len) = self.slot(index)?;
        Some(&self.data[offset..offset + len])
    }

    /// Allocates a cell in the data area (compacting if fragmentation is the
    /// only obstacle) and returns its offset. `extra_slot` reserves room for
    /// one new directory entry.
    fn allocate_cell(&mut self, len: usize, extra_slot: bool) -> Result<usize, PageFull> {
        let slot_overhead = if extra_slot { SLOT_SIZE } else { 0 };
        if self.contiguous_free() < len + slot_overhead {
            if self.total_free() < len + slot_overhead {
                return Err(PageFull);
            }
            self.compact();
            if self.contiguous_free() < len + slot_overhead {
                return Err(PageFull);
            }
        }
        let offset = self.data_start() - len;
        self.set_u16(DATA_START_AT, offset as u16);
        Ok(offset)
    }

    /// Rewrites the cell area to squeeze out holes; slot indices unchanged.
    fn compact(&mut self) {
        let cells: Vec<(usize, Vec<u8>)> = (0..self.slot_count())
            .filter_map(|i| self.get(i).map(|bytes| (i, bytes.to_vec())))
            .collect();
        let mut cursor = PAGE_SIZE;
        self.set_u16(DATA_START_AT, PAGE_SIZE as u16);
        for (index, bytes) in cells {
            cursor -= bytes.len();
            self.data[cursor..cursor + bytes.len()].copy_from_slice(&bytes);
            self.set_slot(index, cursor, bytes.len());
        }
        self.set_u16(DATA_START_AT, cursor as u16);
    }

    // ---- positional discipline (B+ tree) -------------------------------

    /// Inserts a cell at directory position `index`, shifting later entries.
    pub fn insert_at(&mut self, index: usize, bytes: &[u8]) -> Result<(), PageFull> {
        let count = self.slot_count();
        debug_assert!(index <= count);
        let offset = self.allocate_cell(bytes.len(), true)?;
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        // Shift directory entries [index..count) one slot right.
        let src = STRUCT_HEADER_END + index * SLOT_SIZE;
        let end = STRUCT_HEADER_END + count * SLOT_SIZE;
        self.data.copy_within(src..end, src + SLOT_SIZE);
        self.set_slot(index, offset, bytes.len());
        self.set_u16(SLOT_COUNT_AT, (count + 1) as u16);
        Ok(())
    }

    /// Removes the cell at directory position `index`, shifting later
    /// entries left.
    pub fn remove_at(&mut self, index: usize) {
        let count = self.slot_count();
        debug_assert!(index < count);
        let src = STRUCT_HEADER_END + (index + 1) * SLOT_SIZE;
        let end = STRUCT_HEADER_END + count * SLOT_SIZE;
        self.data.copy_within(src..end, src - SLOT_SIZE);
        self.set_u16(SLOT_COUNT_AT, (count - 1) as u16);
    }

    /// Replaces the cell at position `index`.
    pub fn update_at(&mut self, index: usize, bytes: &[u8]) -> Result<(), PageFull> {
        let (offset, len) = self.slot(index).expect("update of tombstone");
        if bytes.len() <= len {
            self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
            self.set_slot(index, offset, bytes.len());
            return Ok(());
        }
        // Tombstone the old cell during allocation so compaction can reclaim
        // it, then restore on failure.
        self.set_slot(index, 0, 0);
        match self.allocate_cell(bytes.len(), false) {
            Ok(new_offset) => {
                self.data[new_offset..new_offset + bytes.len()].copy_from_slice(bytes);
                self.set_slot(index, new_offset, bytes.len());
                Ok(())
            }
            Err(PageFull) => {
                self.set_slot(index, offset, len);
                Err(PageFull)
            }
        }
    }

    // ---- stable discipline (heap) ---------------------------------------

    /// Inserts a cell into the first tombstone slot (or a new slot) and
    /// returns the slot index — the row's identity.
    pub fn insert_stable(&mut self, bytes: &[u8]) -> Result<usize, PageFull> {
        let reuse = (0..self.slot_count()).find(|&i| self.slot(i).is_none());
        let index = match reuse {
            Some(index) => index,
            None => self.slot_count(),
        };
        self.insert_stable_at(index, bytes)?;
        Ok(index)
    }

    /// Inserts at an exact slot index (recovery redo and undo need
    /// determinism). The slot must be a tombstone or the next fresh index.
    pub fn insert_stable_at(&mut self, index: usize, bytes: &[u8]) -> Result<(), PageFull> {
        let count = self.slot_count();
        debug_assert!(
            index < count && self.slot(index).is_none() || index == count,
            "stable insert into occupied slot {index}"
        );
        let new_slot = index == count;
        let offset = self.allocate_cell(bytes.len(), new_slot)?;
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
        if new_slot {
            self.set_u16(SLOT_COUNT_AT, (count + 1) as u16);
        }
        self.set_slot(index, offset, bytes.len());
        Ok(())
    }

    /// Tombstones a slot; the index stays valid (RID stability).
    pub fn delete_stable(&mut self, index: usize) {
        debug_assert!(index < self.slot_count());
        self.set_slot(index, 0, 0);
    }

    /// Replaces the cell at a stable slot.
    pub fn update_stable(&mut self, index: usize, bytes: &[u8]) -> Result<(), PageFull> {
        self.update_at(index, bytes)
    }

    fn get_u16(&self, at: usize) -> u16 {
        u16::from_le_bytes([self.data[at], self.data[at + 1]])
    }

    fn set_u16(&mut self, at: usize, value: u16) {
        self.data[at..at + 2].copy_from_slice(&value.to_le_bytes());
    }
}

/// Read-only view of a slotted page over a shared borrow (scans and space
/// checks; [`SlottedPage`] requires `&mut`).
pub struct SlottedRead<'a> {
    data: &'a [u8],
}

impl<'a> SlottedRead<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        SlottedRead { data }
    }

    pub fn next_page(&self) -> u64 {
        u64::from_le_bytes(
            self.data[NEXT_PAGE_AT..NEXT_PAGE_AT + 8]
                .try_into()
                .unwrap(),
        )
    }

    pub fn level(&self) -> u16 {
        u16::from_le_bytes([self.data[LEVEL_AT], self.data[LEVEL_AT + 1]])
    }

    pub fn slot_count(&self) -> usize {
        u16::from_le_bytes([self.data[SLOT_COUNT_AT], self.data[SLOT_COUNT_AT + 1]]) as usize
    }

    pub fn get(&self, index: usize) -> Option<&'a [u8]> {
        debug_assert!(index < self.slot_count());
        let at = STRUCT_HEADER_END + index * SLOT_SIZE;
        let offset = u16::from_le_bytes([self.data[at], self.data[at + 1]]) as usize;
        let len = u16::from_le_bytes([self.data[at + 2], self.data[at + 3]]) as usize;
        if offset == 0 {
            None
        } else {
            Some(&self.data[offset..offset + len])
        }
    }

    pub fn total_free(&self) -> usize {
        let live: usize = (0..self.slot_count())
            .filter_map(|i| self.get(i))
            .map(|c| c.len())
            .sum();
        PAGE_SIZE - STRUCT_HEADER_END - self.slot_count() * SLOT_SIZE - live
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> Vec<u8> {
        vec![0u8; PAGE_SIZE]
    }

    #[test]
    fn positional_insert_get_remove() {
        let mut data = fresh();
        let mut page = SlottedPage::format(&mut data, 0);
        page.insert_at(0, b"bb").unwrap();
        page.insert_at(0, b"aa").unwrap();
        page.insert_at(2, b"cc").unwrap();
        assert_eq!(page.get(0), Some(b"aa".as_slice()));
        assert_eq!(page.get(1), Some(b"bb".as_slice()));
        assert_eq!(page.get(2), Some(b"cc".as_slice()));
        page.remove_at(1);
        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.get(0), Some(b"aa".as_slice()));
        assert_eq!(page.get(1), Some(b"cc".as_slice()));
    }

    #[test]
    fn update_grows_and_shrinks() {
        let mut data = fresh();
        let mut page = SlottedPage::format(&mut data, 0);
        page.insert_at(0, b"short").unwrap();
        page.insert_at(1, b"other").unwrap();
        page.update_at(0, b"a considerably longer cell payload")
            .unwrap();
        assert_eq!(
            page.get(0),
            Some(b"a considerably longer cell payload".as_slice())
        );
        assert_eq!(page.get(1), Some(b"other".as_slice()));
        page.update_at(0, b"x").unwrap();
        assert_eq!(page.get(0), Some(b"x".as_slice()));
    }

    #[test]
    fn stable_slots_reuse_tombstones_and_keep_identity() {
        let mut data = fresh();
        let mut page = SlottedPage::format(&mut data, 0);
        let a = page.insert_stable(b"aaa").unwrap();
        let b = page.insert_stable(b"bbb").unwrap();
        let c = page.insert_stable(b"ccc").unwrap();
        assert_eq!((a, b, c), (0, 1, 2));
        page.delete_stable(b);
        assert_eq!(page.get(b), None);
        assert_eq!(page.get(c), Some(b"ccc".as_slice()), "identities stable");
        let reused = page.insert_stable(b"ddd").unwrap();
        assert_eq!(reused, b, "tombstone slot reused");
        assert_eq!(page.get(b), Some(b"ddd".as_slice()));
    }

    #[test]
    fn fills_up_then_compaction_reclaims_holes() {
        let mut data = fresh();
        let mut page = SlottedPage::format(&mut data, 0);
        let cell = vec![7u8; 100];
        let mut slots = Vec::new();
        loop {
            match page.insert_stable(&cell) {
                Ok(slot) => slots.push(slot),
                Err(PageFull) => break,
            }
        }
        assert!(slots.len() >= 35, "expected ~39 cells, got {}", slots.len());
        // Free every other cell: contiguous space stays tiny, but compaction
        // must make the holes usable.
        for &slot in slots.iter().step_by(2) {
            page.delete_stable(slot);
        }
        let big = vec![9u8; 1000];
        page.insert_stable(&big).expect("compaction reclaims holes");
    }

    #[test]
    fn insert_at_exact_slot_for_redo() {
        let mut data = fresh();
        let mut page = SlottedPage::format(&mut data, 0);
        page.insert_stable_at(0, b"row0").unwrap();
        page.insert_stable_at(1, b"row1").unwrap();
        page.delete_stable(0);
        page.insert_stable_at(0, b"row0-again").unwrap();
        assert_eq!(page.get(0), Some(b"row0-again".as_slice()));
    }

    #[test]
    fn next_page_and_level_round_trip() {
        let mut data = fresh();
        let mut page = SlottedPage::format(&mut data, 3);
        assert_eq!(page.next_page(), NO_PAGE);
        page.set_next_page(12345);
        assert_eq!(page.next_page(), 12345);
        assert_eq!(page.level(), 3);
    }
}
