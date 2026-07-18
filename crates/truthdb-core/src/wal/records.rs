//! ARIES-style relational WAL record framing.
//!
//! These records ride inside the existing WAL entry framing
//! (`WalEntryHeader` + payload + `WalEntryFooter`) under
//! `WAL_ENTRY_TYPE_REL`. The entry-level CRCs cover the whole record, so this
//! layer only defines the payload layout:
//!
//! ```text
//! prev_lsn u64 | txn_id u64 | kind u16 | flags u16 | redo_len u32 | undo_len u32 | redo | undo
//! ```
//!
//! The record's own LSN is implied by its position in the ring (unwrapped
//! byte position, stamped in the entry header's `logical_ts`). `prev_lsn`
//! chains records of one transaction for undo; `txn_id = 0` marks
//! non-transactional records (e.g. allocator state changes).

use crate::storage::StorageError;
use crate::storage_layout::PAGE_SIZE;

pub const REL_RECORD_HEADER_SIZE: usize = 8 + 8 + 2 + 2 + 4 + 4;

pub const REL_KIND_ALLOC_EXTENT: u16 = 1;
pub const REL_KIND_FREE_EXTENT: u16 = 2;
pub const REL_KIND_TXN_BEGIN: u16 = 3;
pub const REL_KIND_TXN_COMMIT: u16 = 4;
pub const REL_KIND_TXN_END: u16 = 5;
/// Physiological page operation: redo = [`PageOpRedo`], undo = [`PageOpUndo`].
pub const REL_KIND_PAGE_OP: u16 = 6;
/// Full page image (first touch after checkpoint, and structure changes
/// like splits): redo = page number + whole-page image, undo = [`PageOpUndo`].
pub const REL_KIND_PAGE_IMAGE: u16 = 7;
/// Compensation log record: redo = undo_next LSN + [`PageOpRedo`]; never
/// undone itself.
pub const REL_KIND_CLR: u16 = 8;
/// Redo-only: the catalog tree root page (page number in redo payload).
pub const REL_KIND_SET_CATALOG_ROOT: u16 = 9;
/// Atomic group of full page images (B+ tree splits): all pages of one
/// structure change land in ONE WAL entry, so a crash either keeps the whole
/// change or none of it.
pub const REL_KIND_PAGE_IMAGES: u16 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelRecord {
    /// Previous LSN of the same transaction (0 = none).
    pub prev_lsn: u64,
    /// Owning transaction (0 = non-transactional).
    pub txn_id: u64,
    pub kind: u16,
    pub flags: u16,
    pub redo: Vec<u8>,
    pub undo: Vec<u8>,
}

/// Physiological redo payload of a page operation. Applying it is
/// idempotent under the page-LSN gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageOpRedo {
    /// B+ tree positional insert (directory shifts right).
    InsertAt {
        page: u64,
        index: u16,
        bytes: Vec<u8>,
    },
    /// B+ tree positional remove (directory shifts left).
    RemoveAt { page: u64, index: u16 },
    /// B+ tree positional cell replacement.
    UpdateAt {
        page: u64,
        index: u16,
        bytes: Vec<u8>,
    },
    /// Heap insert at an exact stable slot.
    HeapInsert {
        page: u64,
        slot: u16,
        bytes: Vec<u8>,
    },
    /// Heap tombstone.
    HeapDelete { page: u64, slot: u16 },
    /// Heap cell replacement at a stable slot.
    HeapUpdate {
        page: u64,
        slot: u16,
        bytes: Vec<u8>,
    },
    /// Heap page chain link.
    SetNextPage { page: u64, next: u64 },
    /// Adds `delta` to a table's row-counter page (planner statistics). The
    /// same shape serves redo and — with the sign flipped, via a CLR — undo,
    /// and the page-LSN gate makes replay idempotent like any page op.
    CounterAdd { page: u64, delta: i64 },
}

impl PageOpRedo {
    pub fn page(&self) -> u64 {
        match self {
            PageOpRedo::InsertAt { page, .. }
            | PageOpRedo::RemoveAt { page, .. }
            | PageOpRedo::UpdateAt { page, .. }
            | PageOpRedo::HeapInsert { page, .. }
            | PageOpRedo::HeapDelete { page, .. }
            | PageOpRedo::HeapUpdate { page, .. }
            | PageOpRedo::SetNextPage { page, .. }
            | PageOpRedo::CounterAdd { page, .. } => *page,
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            PageOpRedo::InsertAt { page, index, bytes } => {
                out.push(1);
                put_u64(out, *page);
                put_u16(out, *index);
                put_bytes(out, bytes);
            }
            PageOpRedo::RemoveAt { page, index } => {
                out.push(2);
                put_u64(out, *page);
                put_u16(out, *index);
            }
            PageOpRedo::UpdateAt { page, index, bytes } => {
                out.push(3);
                put_u64(out, *page);
                put_u16(out, *index);
                put_bytes(out, bytes);
            }
            PageOpRedo::HeapInsert { page, slot, bytes } => {
                out.push(4);
                put_u64(out, *page);
                put_u16(out, *slot);
                put_bytes(out, bytes);
            }
            PageOpRedo::HeapDelete { page, slot } => {
                out.push(5);
                put_u64(out, *page);
                put_u16(out, *slot);
            }
            PageOpRedo::HeapUpdate { page, slot, bytes } => {
                out.push(6);
                put_u64(out, *page);
                put_u16(out, *slot);
                put_bytes(out, bytes);
            }
            PageOpRedo::SetNextPage { page, next } => {
                out.push(7);
                put_u64(out, *page);
                put_u64(out, *next);
            }
            PageOpRedo::CounterAdd { page, delta } => {
                out.push(8);
                put_u64(out, *page);
                put_u64(out, *delta as u64);
            }
        }
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, StorageError> {
        Ok(match cursor.u8()? {
            1 => PageOpRedo::InsertAt {
                page: cursor.u64()?,
                index: cursor.u16()?,
                bytes: cursor.bytes()?,
            },
            2 => PageOpRedo::RemoveAt {
                page: cursor.u64()?,
                index: cursor.u16()?,
            },
            3 => PageOpRedo::UpdateAt {
                page: cursor.u64()?,
                index: cursor.u16()?,
                bytes: cursor.bytes()?,
            },
            4 => PageOpRedo::HeapInsert {
                page: cursor.u64()?,
                slot: cursor.u16()?,
                bytes: cursor.bytes()?,
            },
            5 => PageOpRedo::HeapDelete {
                page: cursor.u64()?,
                slot: cursor.u16()?,
            },
            6 => PageOpRedo::HeapUpdate {
                page: cursor.u64()?,
                slot: cursor.u16()?,
                bytes: cursor.bytes()?,
            },
            7 => PageOpRedo::SetNextPage {
                page: cursor.u64()?,
                next: cursor.u64()?,
            },
            8 => PageOpRedo::CounterAdd {
                page: cursor.u64()?,
                delta: cursor.u64()? as i64,
            },
            other => {
                return Err(StorageError::InvalidFile(format!(
                    "unknown page-op redo tag {other}"
                )));
            }
        })
    }
}

/// Undo action of a page operation. Tree undos are *logical* (the row may
/// have moved through splits since); heap undos are physical (RIDs are
/// stable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageOpUndo {
    None,
    /// Undo of a tree insert: delete the key wherever it now lives.
    TreeDeleteKey {
        object_id: u32,
        key: Vec<u8>,
    },
    /// Undo of a tree delete: re-insert the row.
    TreeInsertRow {
        object_id: u32,
        key: Vec<u8>,
        row: Vec<u8>,
    },
    /// Undo of a tree update: restore the previous row for the key.
    TreeUpdateRow {
        object_id: u32,
        key: Vec<u8>,
        row: Vec<u8>,
    },
    /// Undo of a heap insert.
    HeapDeleteSlot {
        page: u64,
        slot: u16,
    },
    /// Undo of a heap delete: restore the exact slot.
    HeapInsertRow {
        page: u64,
        slot: u16,
        bytes: Vec<u8>,
    },
    /// Undo of a heap update: restore the previous cell.
    HeapUpdateRow {
        page: u64,
        slot: u16,
        bytes: Vec<u8>,
    },
    /// Undo of a counter add: apply the inverse delta (already negated when
    /// the undo record was built).
    CounterAdd {
        page: u64,
        delta: i64,
    },
}

impl PageOpUndo {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            PageOpUndo::None => out.push(0),
            PageOpUndo::TreeDeleteKey { object_id, key } => {
                out.push(1);
                put_u32(out, *object_id);
                put_bytes(out, key);
            }
            PageOpUndo::TreeInsertRow {
                object_id,
                key,
                row,
            } => {
                out.push(2);
                put_u32(out, *object_id);
                put_bytes(out, key);
                put_bytes(out, row);
            }
            PageOpUndo::TreeUpdateRow {
                object_id,
                key,
                row,
            } => {
                out.push(3);
                put_u32(out, *object_id);
                put_bytes(out, key);
                put_bytes(out, row);
            }
            PageOpUndo::HeapDeleteSlot { page, slot } => {
                out.push(4);
                put_u64(out, *page);
                put_u16(out, *slot);
            }
            PageOpUndo::HeapInsertRow { page, slot, bytes } => {
                out.push(5);
                put_u64(out, *page);
                put_u16(out, *slot);
                put_bytes(out, bytes);
            }
            PageOpUndo::HeapUpdateRow { page, slot, bytes } => {
                out.push(6);
                put_u64(out, *page);
                put_u16(out, *slot);
                put_bytes(out, bytes);
            }
            PageOpUndo::CounterAdd { page, delta } => {
                out.push(7);
                put_u64(out, *page);
                put_u64(out, *delta as u64);
            }
        }
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, StorageError> {
        Ok(match cursor.u8()? {
            0 => PageOpUndo::None,
            1 => PageOpUndo::TreeDeleteKey {
                object_id: cursor.u32()?,
                key: cursor.bytes()?,
            },
            2 => PageOpUndo::TreeInsertRow {
                object_id: cursor.u32()?,
                key: cursor.bytes()?,
                row: cursor.bytes()?,
            },
            3 => PageOpUndo::TreeUpdateRow {
                object_id: cursor.u32()?,
                key: cursor.bytes()?,
                row: cursor.bytes()?,
            },
            4 => PageOpUndo::HeapDeleteSlot {
                page: cursor.u64()?,
                slot: cursor.u16()?,
            },
            5 => PageOpUndo::HeapInsertRow {
                page: cursor.u64()?,
                slot: cursor.u16()?,
                bytes: cursor.bytes()?,
            },
            6 => PageOpUndo::HeapUpdateRow {
                page: cursor.u64()?,
                slot: cursor.u16()?,
                bytes: cursor.bytes()?,
            },
            7 => PageOpUndo::CounterAdd {
                page: cursor.u64()?,
                delta: cursor.u64()? as i64,
            },
            other => {
                return Err(StorageError::InvalidFile(format!(
                    "unknown page-op undo tag {other}"
                )));
            }
        })
    }

    pub fn decode_from(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut cursor = Cursor { bytes, at: 0 };
        let undo = PageOpUndo::decode(&mut cursor)?;
        cursor.finish()?;
        Ok(undo)
    }
}

impl RelRecord {
    pub fn alloc_extent(start_page: u64, num_pages: u64) -> Self {
        RelRecord {
            prev_lsn: 0,
            txn_id: 0,
            kind: REL_KIND_ALLOC_EXTENT,
            flags: 0,
            redo: extent_payload(start_page, num_pages),
            undo: Vec::new(),
        }
    }

    pub fn free_extent(start_page: u64, num_pages: u64) -> Self {
        RelRecord {
            prev_lsn: 0,
            txn_id: 0,
            kind: REL_KIND_FREE_EXTENT,
            flags: 0,
            redo: extent_payload(start_page, num_pages),
            undo: Vec::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(REL_RECORD_HEADER_SIZE + self.redo.len() + self.undo.len());
        out.extend_from_slice(&self.prev_lsn.to_le_bytes());
        out.extend_from_slice(&self.txn_id.to_le_bytes());
        out.extend_from_slice(&self.kind.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&(self.redo.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.undo.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.redo);
        out.extend_from_slice(&self.undo);
        out
    }

    pub fn decode(payload: &[u8]) -> Result<Self, StorageError> {
        if payload.len() < REL_RECORD_HEADER_SIZE {
            return Err(StorageError::InvalidFile(
                "rel wal record shorter than header".to_string(),
            ));
        }
        let prev_lsn = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let txn_id = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let kind = u16::from_le_bytes(payload[16..18].try_into().unwrap());
        let flags = u16::from_le_bytes(payload[18..20].try_into().unwrap());
        let redo_len = u32::from_le_bytes(payload[20..24].try_into().unwrap()) as usize;
        let undo_len = u32::from_le_bytes(payload[24..28].try_into().unwrap()) as usize;
        let expected = REL_RECORD_HEADER_SIZE
            .checked_add(redo_len)
            .and_then(|len| len.checked_add(undo_len))
            .ok_or_else(|| {
                StorageError::InvalidFile("rel wal record length overflow".to_string())
            })?;
        if payload.len() != expected {
            return Err(StorageError::InvalidFile(format!(
                "rel wal record length mismatch: payload {} vs declared {expected}",
                payload.len()
            )));
        }
        let redo = payload[REL_RECORD_HEADER_SIZE..REL_RECORD_HEADER_SIZE + redo_len].to_vec();
        let undo = payload[REL_RECORD_HEADER_SIZE + redo_len..].to_vec();
        Ok(RelRecord {
            prev_lsn,
            txn_id,
            kind,
            flags,
            redo,
            undo,
        })
    }

    /// Decodes the `(start_page, num_pages)` redo payload of an
    /// alloc-extent/free-extent record.
    pub fn decode_extent_redo(&self) -> Result<(u64, u64), StorageError> {
        if self.redo.len() != 16 {
            return Err(StorageError::InvalidFile(
                "extent wal record redo payload must be 16 bytes".to_string(),
            ));
        }
        let start_page = u64::from_le_bytes(self.redo[0..8].try_into().unwrap());
        let num_pages = u64::from_le_bytes(self.redo[8..16].try_into().unwrap());
        Ok((start_page, num_pages))
    }

    pub fn txn_begin(txn_id: u64) -> Self {
        RelRecord {
            prev_lsn: 0,
            txn_id,
            kind: REL_KIND_TXN_BEGIN,
            flags: 0,
            redo: Vec::new(),
            undo: Vec::new(),
        }
    }

    pub fn txn_commit(txn_id: u64, prev_lsn: u64, timestamp_millis: u64) -> Self {
        RelRecord {
            prev_lsn,
            txn_id,
            kind: REL_KIND_TXN_COMMIT,
            flags: 0,
            // The commit wall-clock time (millis since the Unix epoch), for
            // point-in-time restore. It rides in `redo` so the record stays
            // length-self-describing; recovery never redoes a commit record, so
            // these bytes are inert to redo. Entry-version-1 commit records have
            // an empty `redo` (no timestamp) and decode unchanged.
            redo: timestamp_millis.to_le_bytes().to_vec(),
            undo: Vec::new(),
        }
    }

    pub fn txn_end(txn_id: u64, prev_lsn: u64) -> Self {
        RelRecord {
            prev_lsn,
            txn_id,
            kind: REL_KIND_TXN_END,
            flags: 0,
            redo: Vec::new(),
            undo: Vec::new(),
        }
    }

    pub fn page_op(txn_id: u64, prev_lsn: u64, redo: &PageOpRedo, undo: &PageOpUndo) -> Self {
        let mut redo_bytes = Vec::new();
        redo.encode(&mut redo_bytes);
        let mut undo_bytes = Vec::new();
        undo.encode(&mut undo_bytes);
        RelRecord {
            prev_lsn,
            txn_id,
            kind: REL_KIND_PAGE_OP,
            flags: 0,
            redo: redo_bytes,
            undo: undo_bytes,
        }
    }

    /// Full page image (first touch since checkpoint or structure change);
    /// `undo` carries the logical undo of the operation the image subsumes.
    pub fn page_image(
        txn_id: u64,
        prev_lsn: u64,
        page: u64,
        image: &[u8],
        undo: &PageOpUndo,
    ) -> Self {
        debug_assert_eq!(image.len(), PAGE_SIZE);
        let mut redo_bytes = Vec::with_capacity(8 + PAGE_SIZE);
        put_u64(&mut redo_bytes, page);
        redo_bytes.extend_from_slice(image);
        let mut undo_bytes = Vec::new();
        undo.encode(&mut undo_bytes);
        RelRecord {
            prev_lsn,
            txn_id,
            kind: REL_KIND_PAGE_IMAGE,
            flags: 0,
            redo: redo_bytes,
            undo: undo_bytes,
        }
    }

    /// Compensation record: `undo_next` points at the next record of the
    /// transaction still to be undone (the undone record's `prev_lsn`).
    pub fn clr(txn_id: u64, prev_lsn: u64, undo_next: u64, redo: &PageOpRedo) -> Self {
        let mut redo_bytes = Vec::new();
        put_u64(&mut redo_bytes, undo_next);
        redo.encode(&mut redo_bytes);
        RelRecord {
            prev_lsn,
            txn_id,
            kind: REL_KIND_CLR,
            flags: 0,
            redo: redo_bytes,
            undo: Vec::new(),
        }
    }

    /// A CLR that compensates a record without any page effect to redo
    /// (e.g. the undone row was never physically placed).
    pub fn clr_noop(txn_id: u64, prev_lsn: u64, undo_next: u64) -> Self {
        let mut redo_bytes = Vec::new();
        put_u64(&mut redo_bytes, undo_next);
        RelRecord {
            prev_lsn,
            txn_id,
            kind: REL_KIND_CLR,
            flags: 0,
            redo: redo_bytes,
            undo: Vec::new(),
        }
    }

    /// Atomic multi-page image (system record, never undone).
    pub fn page_images(pages: &[(u64, &[u8])]) -> Self {
        let mut redo = Vec::with_capacity(2 + pages.len() * (8 + PAGE_SIZE));
        put_u16(&mut redo, pages.len() as u16);
        for (page_no, image) in pages {
            debug_assert_eq!(image.len(), PAGE_SIZE);
            put_u64(&mut redo, *page_no);
            redo.extend_from_slice(image);
        }
        RelRecord {
            prev_lsn: 0,
            txn_id: 0,
            kind: REL_KIND_PAGE_IMAGES,
            flags: 0,
            redo,
            undo: Vec::new(),
        }
    }

    /// Decodes a PAGE_IMAGES redo into `(page, image)` pairs.
    pub fn decode_page_images(&self) -> Result<Vec<(u64, &[u8])>, StorageError> {
        debug_assert_eq!(self.kind, REL_KIND_PAGE_IMAGES);
        if self.redo.len() < 2 {
            return Err(StorageError::InvalidFile(
                "page images record too short".to_string(),
            ));
        }
        let count = u16::from_le_bytes(self.redo[0..2].try_into().unwrap()) as usize;
        if self.redo.len() != 2 + count * (8 + PAGE_SIZE) {
            return Err(StorageError::InvalidFile(
                "page images record has wrong size".to_string(),
            ));
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let at = 2 + i * (8 + PAGE_SIZE);
            let page = u64::from_le_bytes(self.redo[at..at + 8].try_into().unwrap());
            out.push((page, &self.redo[at + 8..at + 8 + PAGE_SIZE]));
        }
        Ok(out)
    }

    pub fn set_catalog_root(page: u64) -> Self {
        let mut redo = Vec::new();
        put_u64(&mut redo, page);
        RelRecord {
            prev_lsn: 0,
            txn_id: 0,
            kind: REL_KIND_SET_CATALOG_ROOT,
            flags: 0,
            redo,
            undo: Vec::new(),
        }
    }

    pub fn decode_page_op_redo(&self) -> Result<PageOpRedo, StorageError> {
        debug_assert_eq!(self.kind, REL_KIND_PAGE_OP);
        let mut cursor = Cursor {
            bytes: &self.redo,
            at: 0,
        };
        let redo = PageOpRedo::decode(&mut cursor)?;
        cursor.finish()?;
        Ok(redo)
    }

    pub fn decode_page_op_undo(&self) -> Result<PageOpUndo, StorageError> {
        PageOpUndo::decode_from(&self.undo)
    }

    /// Decodes a PAGE_IMAGE redo into `(page, image)`.
    pub fn decode_page_image(&self) -> Result<(u64, &[u8]), StorageError> {
        debug_assert_eq!(self.kind, REL_KIND_PAGE_IMAGE);
        if self.redo.len() != 8 + PAGE_SIZE {
            return Err(StorageError::InvalidFile(
                "page image record has wrong size".to_string(),
            ));
        }
        let page = u64::from_le_bytes(self.redo[0..8].try_into().unwrap());
        Ok((page, &self.redo[8..]))
    }

    /// Decodes a CLR redo into `(undo_next, optional page op)`.
    pub fn decode_clr(&self) -> Result<(u64, Option<PageOpRedo>), StorageError> {
        debug_assert_eq!(self.kind, REL_KIND_CLR);
        if self.redo.len() < 8 {
            return Err(StorageError::InvalidFile(
                "clr record too short".to_string(),
            ));
        }
        let undo_next = u64::from_le_bytes(self.redo[0..8].try_into().unwrap());
        if self.redo.len() == 8 {
            return Ok((undo_next, None));
        }
        let mut cursor = Cursor {
            bytes: &self.redo,
            at: 8,
        };
        let redo = PageOpRedo::decode(&mut cursor)?;
        cursor.finish()?;
        Ok((undo_next, Some(redo)))
    }

    pub fn decode_catalog_root(&self) -> Result<u64, StorageError> {
        debug_assert_eq!(self.kind, REL_KIND_SET_CATALOG_ROOT);
        if self.redo.len() != 8 {
            return Err(StorageError::InvalidFile(
                "catalog root record has wrong size".to_string(),
            ));
        }
        Ok(u64::from_le_bytes(self.redo[0..8].try_into().unwrap()))
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], StorageError> {
        if self.at + len > self.bytes.len() {
            return Err(StorageError::InvalidFile(
                "truncated wal record payload".to_string(),
            ));
        }
        let slice = &self.bytes[self.at..self.at + len];
        self.at += len;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, StorageError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, StorageError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn finish(&self) -> Result<(), StorageError> {
        if self.at != self.bytes.len() {
            return Err(StorageError::InvalidFile(
                "trailing bytes in wal record payload".to_string(),
            ));
        }
        Ok(())
    }
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn extent_payload(start_page: u64, num_pages: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&start_page.to_le_bytes());
    out.extend_from_slice(&num_pages.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_record_round_trip() {
        let record = RelRecord {
            prev_lsn: 42,
            txn_id: 7,
            kind: REL_KIND_ALLOC_EXTENT,
            flags: 0,
            redo: vec![1, 2, 3],
            undo: vec![4, 5],
        };
        let decoded = RelRecord::decode(&record.encode()).expect("decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn extent_records_round_trip() {
        let record = RelRecord::alloc_extent(128, 64);
        let decoded = RelRecord::decode(&record.encode()).expect("decode");
        assert_eq!(decoded.decode_extent_redo().expect("redo"), (128, 64));
        assert_eq!(decoded.kind, REL_KIND_ALLOC_EXTENT);

        let record = RelRecord::free_extent(4096, 64);
        let decoded = RelRecord::decode(&record.encode()).expect("decode");
        assert_eq!(decoded.decode_extent_redo().expect("redo"), (4096, 64));
        assert_eq!(decoded.kind, REL_KIND_FREE_EXTENT);
    }

    #[test]
    fn page_op_records_round_trip() {
        let redo = PageOpRedo::InsertAt {
            page: 42,
            index: 7,
            bytes: vec![1, 2, 3],
        };
        let undo = PageOpUndo::TreeDeleteKey {
            object_id: 5,
            key: vec![9, 9],
        };
        let record = RelRecord::page_op(11, 1000, &redo, &undo);
        let decoded = RelRecord::decode(&record.encode()).expect("decode");
        assert_eq!(decoded.decode_page_op_redo().expect("redo"), redo);
        assert_eq!(decoded.decode_page_op_undo().expect("undo"), undo);
        assert_eq!(decoded.txn_id, 11);
        assert_eq!(decoded.prev_lsn, 1000);

        for redo in [
            PageOpRedo::RemoveAt { page: 1, index: 0 },
            PageOpRedo::HeapInsert {
                page: 2,
                slot: 3,
                bytes: vec![4],
            },
            PageOpRedo::HeapDelete { page: 2, slot: 3 },
            PageOpRedo::HeapUpdate {
                page: 2,
                slot: 3,
                bytes: vec![],
            },
            PageOpRedo::SetNextPage { page: 5, next: 6 },
        ] {
            let record = RelRecord::page_op(1, 0, &redo, &PageOpUndo::None);
            let decoded = RelRecord::decode(&record.encode()).expect("decode");
            assert_eq!(decoded.decode_page_op_redo().expect("redo"), redo);
            assert_eq!(
                decoded.decode_page_op_undo().expect("undo"),
                PageOpUndo::None
            );
        }
    }

    #[test]
    fn page_image_and_clr_round_trip() {
        let image = vec![0xABu8; PAGE_SIZE];
        let undo = PageOpUndo::HeapDeleteSlot { page: 9, slot: 2 };
        let record = RelRecord::page_image(3, 500, 9, &image, &undo);
        let decoded = RelRecord::decode(&record.encode()).expect("decode");
        let (page, decoded_image) = decoded.decode_page_image().expect("image");
        assert_eq!(page, 9);
        assert_eq!(decoded_image, image.as_slice());
        assert_eq!(decoded.decode_page_op_undo().expect("undo"), undo);

        let redo = PageOpRedo::RemoveAt { page: 9, index: 1 };
        let clr = RelRecord::clr(3, 600, 450, &redo);
        let decoded = RelRecord::decode(&clr.encode()).expect("decode");
        let (undo_next, op) = decoded.decode_clr().expect("clr");
        assert_eq!(undo_next, 450);
        assert_eq!(op, Some(redo));

        let clr = RelRecord::clr_noop(3, 700, 0);
        let decoded = RelRecord::decode(&clr.encode()).expect("decode");
        assert_eq!(decoded.decode_clr().expect("clr"), (0, None));
    }

    #[test]
    fn txn_and_catalog_records_round_trip() {
        for record in [
            RelRecord::txn_begin(7),
            RelRecord::txn_commit(7, 123, 1_700_000_000_000),
            RelRecord::txn_end(7, 456),
            RelRecord::set_catalog_root(99),
        ] {
            let decoded = RelRecord::decode(&record.encode()).expect("decode");
            assert_eq!(decoded, record);
        }
        // The commit timestamp survives the encode/decode round-trip and lands
        // in the commit record's redo.
        let commit = RelRecord::txn_commit(7, 123, 1_700_000_000_000);
        let decoded = RelRecord::decode(&commit.encode()).expect("decode");
        assert_eq!(
            u64::from_le_bytes(decoded.redo[..8].try_into().unwrap()),
            1_700_000_000_000
        );
        assert_eq!(
            RelRecord::set_catalog_root(99)
                .decode_catalog_root()
                .expect("root"),
            99
        );
    }

    #[test]
    fn decode_rejects_truncated_and_mismatched() {
        let record = RelRecord::alloc_extent(1, 2);
        let bytes = record.encode();
        assert!(RelRecord::decode(&bytes[..REL_RECORD_HEADER_SIZE - 1]).is_err());
        assert!(RelRecord::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut extended = bytes.clone();
        extended.push(0);
        assert!(RelRecord::decode(&extended).is_err());
    }
}
