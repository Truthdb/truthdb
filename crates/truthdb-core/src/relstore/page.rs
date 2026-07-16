//! Shared 32-byte page header for all relational pages.
//!
//! ```text
//! offset  0: checksum   u64   xxh64 of the whole page with this field zeroed
//! offset  8: page_lsn   u64   LSN of the last WAL record that touched the page
//! offset 16: page_type  u16
//! offset 18: flags      u16
//! offset 20: object_id  u32   owning table/index (self-identity)
//! offset 24: page_no    u64   this page's number in the data region (self-identity)
//! ```
//!
//! The self-identity fields let recovery and consistency checks detect
//! misdirected writes: a page read from slot N must claim `page_no == N`.

use xxhash_rust::xxh64::xxh64;

use crate::storage_layout::PAGE_SIZE;

pub const PAGE_HEADER_SIZE: usize = 32;

pub const PAGE_TYPE_FREE: u16 = 0;
/// B+ tree page (leaf vs internal distinguished by the structure header's
/// level field).
pub const PAGE_TYPE_TREE: u16 = 1;
pub const PAGE_TYPE_HEAP: u16 = 2;
/// A table's row-counter page (planner statistics): one u64 count at
/// [`COUNTER_OFFSET`], maintained transactionally via `CounterAdd` page ops.
pub const PAGE_TYPE_COUNTER: u16 = 3;
/// Byte offset of the row count on a counter page (right after the header).
pub const COUNTER_OFFSET: usize = PAGE_HEADER_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PageHeader {
    pub page_lsn: u64,
    pub page_type: u16,
    pub flags: u16,
    pub object_id: u32,
    pub page_no: u64,
}

pub fn read_header(page: &[u8]) -> PageHeader {
    assert_eq!(page.len(), PAGE_SIZE);
    PageHeader {
        page_lsn: u64::from_le_bytes(page[8..16].try_into().unwrap()),
        page_type: u16::from_le_bytes(page[16..18].try_into().unwrap()),
        flags: u16::from_le_bytes(page[18..20].try_into().unwrap()),
        object_id: u32::from_le_bytes(page[20..24].try_into().unwrap()),
        page_no: u64::from_le_bytes(page[24..32].try_into().unwrap()),
    }
}

/// Writes the header fields; the checksum field is left untouched (it is
/// stamped just before the page goes to disk).
pub fn write_header(page: &mut [u8], header: &PageHeader) {
    assert_eq!(page.len(), PAGE_SIZE);
    page[8..16].copy_from_slice(&header.page_lsn.to_le_bytes());
    page[16..18].copy_from_slice(&header.page_type.to_le_bytes());
    page[18..20].copy_from_slice(&header.flags.to_le_bytes());
    page[20..24].copy_from_slice(&header.object_id.to_le_bytes());
    page[24..32].copy_from_slice(&header.page_no.to_le_bytes());
}

pub fn page_lsn(page: &[u8]) -> u64 {
    u64::from_le_bytes(page[8..16].try_into().unwrap())
}

pub fn stamp_checksum(page: &mut [u8]) {
    assert_eq!(page.len(), PAGE_SIZE);
    page[0..8].copy_from_slice(&0u64.to_le_bytes());
    let checksum = xxh64(page, 0);
    page[0..8].copy_from_slice(&checksum.to_le_bytes());
}

pub fn verify_checksum(page: &[u8]) -> bool {
    assert_eq!(page.len(), PAGE_SIZE);
    let stored = u64::from_le_bytes(page[0..8].try_into().unwrap());
    let mut copy = page.to_vec();
    copy[0..8].copy_from_slice(&0u64.to_le_bytes());
    xxh64(&copy, 0) == stored
}

/// A page that has never been written (fresh allocation) is all zeros.
pub fn is_zero_page(page: &[u8]) -> bool {
    page.iter().all(|b| *b == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let mut page = vec![0u8; PAGE_SIZE];
        let header = PageHeader {
            page_lsn: 0xDEAD_BEEF,
            page_type: 7,
            flags: 3,
            object_id: 42,
            page_no: 1234,
        };
        write_header(&mut page, &header);
        assert_eq!(read_header(&page), header);
        assert_eq!(page_lsn(&page), 0xDEAD_BEEF);
    }

    #[test]
    fn checksum_round_trip_and_corruption_detection() {
        let mut page = vec![0u8; PAGE_SIZE];
        page[100] = 0xAA;
        stamp_checksum(&mut page);
        assert!(verify_checksum(&page));
        page[200] ^= 1;
        assert!(!verify_checksum(&page));
    }

    #[test]
    fn zero_page_detection() {
        let mut page = vec![0u8; PAGE_SIZE];
        assert!(is_zero_page(&page));
        page[0] = 1;
        assert!(!is_zero_page(&page));
    }
}
