use core::fmt;

pub const PAGE_SIZE: usize = 4096;
pub const FILE_MAGIC: [u8; 8] = *b"TRUTHDB\0";
pub const FILE_VERSION: u32 = 1;

pub const FILE_HEADER_SIZE: usize = PAGE_SIZE;
pub const SUPERBLOCK_SIZE: usize = PAGE_SIZE;

pub const WAL_ENTRY_ALIGNMENT: usize = 8;
pub const WAL_ENTRY_HEADER_SIZE: usize = 40;
pub const WAL_ENTRY_FOOTER_SIZE: usize = 12;

pub const SUPERBLOCK_ACTIVE_A: u8 = 0;
pub const SUPERBLOCK_ACTIVE_B: u8 = 1;

const FILE_HEADER_FIXED_SIZE: usize = 8 + 4 + 4 + 16 + 16 + 8 + 8 + 8;
const FILE_HEADER_RESERVED_SIZE: usize = FILE_HEADER_SIZE - FILE_HEADER_FIXED_SIZE;

const SUPERBLOCK_FIXED_SIZE: usize = 8 + 1 + 7 + (8 * 8);
const SUPERBLOCK_RESERVED_SIZE: usize = SUPERBLOCK_SIZE - SUPERBLOCK_FIXED_SIZE;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FileHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub page_size: u32,
    pub file_uuid: [u8; 16],
    pub created_salt: [u8; 16],
    pub superblock_a_offset: u64,
    pub superblock_b_offset: u64,
    pub header_checksum: u64,
    pub reserved: [u8; FILE_HEADER_RESERVED_SIZE],
}

impl Default for FileHeader {
    fn default() -> Self {
        FileHeader {
            magic: FILE_MAGIC,
            version: FILE_VERSION,
            page_size: PAGE_SIZE as u32,
            file_uuid: [0; 16],
            created_salt: [0; 16],
            superblock_a_offset: 0,
            superblock_b_offset: 0,
            header_checksum: 0,
            reserved: [0; FILE_HEADER_RESERVED_SIZE],
        }
    }
}

impl fmt::Debug for FileHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHeader")
            .field("magic", &self.magic)
            .field("version", &self.version)
            .field("page_size", &self.page_size)
            .field("file_uuid", &self.file_uuid)
            .field("created_salt", &self.created_salt)
            .field("superblock_a_offset", &self.superblock_a_offset)
            .field("superblock_b_offset", &self.superblock_b_offset)
            .field("header_checksum", &self.header_checksum)
            .finish()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Superblock {
    pub generation: u64,
    pub active: u8,
    pub active_padding: [u8; 7],
    pub wal_head: u64,
    pub wal_tail: u64,
    pub last_committed_seq: u64,
    pub metadata_root: u64,
    pub data_root: u64,
    pub allocator_root: u64,
    pub snapshot_root: u64,
    pub checksum: u64,
    pub reserved: [u8; SUPERBLOCK_RESERVED_SIZE],
}

impl Default for Superblock {
    fn default() -> Self {
        Superblock {
            generation: 0,
            active: SUPERBLOCK_ACTIVE_A,
            active_padding: [0; 7],
            wal_head: 0,
            wal_tail: 0,
            last_committed_seq: 0,
            metadata_root: 0,
            data_root: 0,
            allocator_root: 0,
            snapshot_root: 0,
            checksum: 0,
            reserved: [0; SUPERBLOCK_RESERVED_SIZE],
        }
    }
}

impl fmt::Debug for Superblock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Superblock")
            .field("generation", &self.generation)
            .field("active", &self.active)
            .field("wal_head", &self.wal_head)
            .field("wal_tail", &self.wal_tail)
            .field("last_committed_seq", &self.last_committed_seq)
            .field("metadata_root", &self.metadata_root)
            .field("data_root", &self.data_root)
            .field("allocator_root", &self.allocator_root)
            .field("snapshot_root", &self.snapshot_root)
            .field("checksum", &self.checksum)
            .finish()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct WalEntryHeader {
    pub entry_type: u16,
    pub entry_version: u16,
    pub payload_len: u32,
    pub seq_no: u64,
    pub logical_ts: u64,
    pub header_crc: u64,
    pub payload_crc: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalEntryFooter {
    pub payload_len: u32,
    pub payload_crc: u64,
}

impl WalEntryFooter {
    pub fn to_le_bytes(self) -> [u8; WAL_ENTRY_FOOTER_SIZE] {
        let mut out = [0u8; WAL_ENTRY_FOOTER_SIZE];
        out[0..4].copy_from_slice(&self.payload_len.to_le_bytes());
        out[4..12].copy_from_slice(&self.payload_crc.to_le_bytes());
        out
    }

    pub fn from_le_bytes(bytes: &[u8; WAL_ENTRY_FOOTER_SIZE]) -> Self {
        let payload_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let payload_crc = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        WalEntryFooter {
            payload_len,
            payload_crc,
        }
    }
}

#[inline]
pub fn align_up(value: usize, alignment: usize) -> usize {
    if alignment == 0 {
        return value;
    }
    let rem = value % alignment;
    if rem == 0 {
        value
    } else {
        value + (alignment - rem)
    }
}

#[inline]
pub fn wal_entry_total_len(payload_len: usize) -> usize {
    WAL_ENTRY_HEADER_SIZE + payload_len + WAL_ENTRY_FOOTER_SIZE
}

#[inline]
pub fn wal_entry_padded_len(payload_len: usize) -> usize {
    align_up(wal_entry_total_len(payload_len), WAL_ENTRY_ALIGNMENT)
}

#[inline]
pub fn wal_entry_padding_len(payload_len: usize) -> usize {
    wal_entry_padded_len(payload_len) - wal_entry_total_len(payload_len)
}

pub(crate) fn assert_layout_invariants() {
    use core::mem::size_of;

    debug_assert_eq!(FILE_MAGIC, *b"TRUTHDB\0");
    debug_assert_eq!(FILE_VERSION, 1);
    debug_assert_eq!(PAGE_SIZE, 4096);
    debug_assert_eq!(WAL_ENTRY_ALIGNMENT, 8);
    debug_assert_eq!(WAL_ENTRY_FOOTER_SIZE, 12);
    debug_assert_eq!(SUPERBLOCK_ACTIVE_A, 0);
    debug_assert_eq!(SUPERBLOCK_ACTIVE_B, 1);

    debug_assert_eq!(size_of::<FileHeader>(), FILE_HEADER_SIZE);
    debug_assert_eq!(size_of::<Superblock>(), SUPERBLOCK_SIZE);
    debug_assert_eq!(size_of::<WalEntryHeader>(), WAL_ENTRY_HEADER_SIZE);

    let footer = WalEntryFooter {
        payload_len: 0,
        payload_crc: 0,
    };
    let bytes = footer.to_le_bytes();
    let _round_trip = WalEntryFooter::from_le_bytes(&bytes);

    let _ = wal_entry_padding_len(0);
    let _ = wal_entry_padding_len(7);
    let _ = wal_entry_padding_len(8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn header_is_one_page() {
        assert_eq!(size_of::<FileHeader>(), FILE_HEADER_SIZE);
    }

    #[test]
    fn superblock_is_one_page() {
        assert_eq!(size_of::<Superblock>(), SUPERBLOCK_SIZE);
    }

    #[test]
    fn wal_header_size_matches_constant() {
        assert_eq!(size_of::<WalEntryHeader>(), WAL_ENTRY_HEADER_SIZE);
    }

    #[test]
    fn wal_footer_size_matches_constant() {
        assert_eq!(WAL_ENTRY_FOOTER_SIZE, 12);
    }

    #[test]
    fn wal_padding_is_8_byte_aligned() {
        for payload_len in [0usize, 1, 7, 8, 9, 15, 16, 1024] {
            let total = wal_entry_padded_len(payload_len);
            assert_eq!(total % WAL_ENTRY_ALIGNMENT, 0);
            assert!(total >= wal_entry_total_len(payload_len));
        }
    }
}
