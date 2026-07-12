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

pub const REL_RECORD_HEADER_SIZE: usize = 8 + 8 + 2 + 2 + 4 + 4;

pub const REL_KIND_ALLOC_EXTENT: u16 = 1;
pub const REL_KIND_FREE_EXTENT: u16 = 2;

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
