use core::fmt;
use xxhash_rust::xxh64::xxh64;

pub const PAGE_SIZE: usize = 4096;
pub const FILE_MAGIC: [u8; 8] = *b"TRUTHDB\0";
pub const FILE_VERSION: u32 = 1;

pub const FILE_HEADER_SIZE: usize = PAGE_SIZE;
pub const SUPERBLOCK_SIZE: usize = PAGE_SIZE;

pub const MIB: u64 = 1024 * 1024;
pub const GIB: u64 = 1024 * 1024 * 1024;

pub const WAL_MIN_BYTES: u64 = 256 * MIB;
pub const WAL_MAX_BYTES: u64 = GIB;

pub const WAL_ENTRY_ALIGNMENT: usize = 8;
pub const WAL_ENTRY_HEADER_SIZE: usize = 40;
pub const WAL_ENTRY_FOOTER_SIZE: usize = 12;
pub const WAL_ENTRY_TYPE_RECORD: u16 = 1;
pub const WAL_ENTRY_TYPE_COMMIT: u16 = 2;

pub const SUPERBLOCK_ACTIVE_A: u8 = 0;
pub const SUPERBLOCK_ACTIVE_B: u8 = 1;

const FILE_HEADER_FIXED_SIZE: usize = 8 + 4 + 4 + 16 + 16 + 8 + 8 + 8 + (12 * 8);
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
    pub wal_offset: u64,
    pub wal_size: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub metadata_offset: u64,
    pub metadata_size: u64,
    pub allocator_offset: u64,
    pub allocator_size: u64,
    pub snapshot_offset: u64,
    pub snapshot_size: u64,
    pub reserved_offset: u64,
    pub reserved_size: u64,
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
            wal_offset: 0,
            wal_size: 0,
            data_offset: 0,
            data_size: 0,
            metadata_offset: 0,
            metadata_size: 0,
            allocator_offset: 0,
            allocator_size: 0,
            snapshot_offset: 0,
            snapshot_size: 0,
            reserved_offset: 0,
            reserved_size: 0,
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
            .field("wal_offset", &self.wal_offset)
            .field("wal_size", &self.wal_size)
            .field("data_offset", &self.data_offset)
            .field("data_size", &self.data_size)
            .field("metadata_offset", &self.metadata_offset)
            .field("metadata_size", &self.metadata_size)
            .field("allocator_offset", &self.allocator_offset)
            .field("allocator_size", &self.allocator_size)
            .field("snapshot_offset", &self.snapshot_offset)
            .field("snapshot_size", &self.snapshot_size)
            .field("reserved_offset", &self.reserved_offset)
            .field("reserved_size", &self.reserved_size)
            .finish()
    }
}

impl FileHeader {
    pub fn to_le_bytes_with_checksum(self) -> [u8; FILE_HEADER_SIZE] {
        let mut out = [0u8; FILE_HEADER_SIZE];
        out[0..8].copy_from_slice(&self.magic);
        out[8..12].copy_from_slice(&self.version.to_le_bytes());
        out[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        out[16..32].copy_from_slice(&self.file_uuid);
        out[32..48].copy_from_slice(&self.created_salt);
        out[48..56].copy_from_slice(&self.superblock_a_offset.to_le_bytes());
        out[56..64].copy_from_slice(&self.superblock_b_offset.to_le_bytes());
        out[64..72].copy_from_slice(&0u64.to_le_bytes());
        out[72..80].copy_from_slice(&self.wal_offset.to_le_bytes());
        out[80..88].copy_from_slice(&self.wal_size.to_le_bytes());
        out[88..96].copy_from_slice(&self.data_offset.to_le_bytes());
        out[96..104].copy_from_slice(&self.data_size.to_le_bytes());
        out[104..112].copy_from_slice(&self.metadata_offset.to_le_bytes());
        out[112..120].copy_from_slice(&self.metadata_size.to_le_bytes());
        out[120..128].copy_from_slice(&self.allocator_offset.to_le_bytes());
        out[128..136].copy_from_slice(&self.allocator_size.to_le_bytes());
        out[136..144].copy_from_slice(&self.snapshot_offset.to_le_bytes());
        out[144..152].copy_from_slice(&self.snapshot_size.to_le_bytes());
        out[152..160].copy_from_slice(&self.reserved_offset.to_le_bytes());
        out[160..168].copy_from_slice(&self.reserved_size.to_le_bytes());
        out[168..].copy_from_slice(&self.reserved);

        let checksum = xxh64(&out, 0);
        out[64..72].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    pub fn from_le_bytes(bytes: &[u8; FILE_HEADER_SIZE]) -> Self {
        let mut header = FileHeader::default();
        header.magic.copy_from_slice(&bytes[0..8]);
        header.version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        header.page_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        header.file_uuid.copy_from_slice(&bytes[16..32]);
        header.created_salt.copy_from_slice(&bytes[32..48]);
        header.superblock_a_offset = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
        header.superblock_b_offset = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
        header.header_checksum = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
        header.wal_offset = u64::from_le_bytes(bytes[72..80].try_into().unwrap());
        header.wal_size = u64::from_le_bytes(bytes[80..88].try_into().unwrap());
        header.data_offset = u64::from_le_bytes(bytes[88..96].try_into().unwrap());
        header.data_size = u64::from_le_bytes(bytes[96..104].try_into().unwrap());
        header.metadata_offset = u64::from_le_bytes(bytes[104..112].try_into().unwrap());
        header.metadata_size = u64::from_le_bytes(bytes[112..120].try_into().unwrap());
        header.allocator_offset = u64::from_le_bytes(bytes[120..128].try_into().unwrap());
        header.allocator_size = u64::from_le_bytes(bytes[128..136].try_into().unwrap());
        header.snapshot_offset = u64::from_le_bytes(bytes[136..144].try_into().unwrap());
        header.snapshot_size = u64::from_le_bytes(bytes[144..152].try_into().unwrap());
        header.reserved_offset = u64::from_le_bytes(bytes[152..160].try_into().unwrap());
        header.reserved_size = u64::from_le_bytes(bytes[160..168].try_into().unwrap());
        header.reserved.copy_from_slice(&bytes[168..]);
        header
    }

    pub fn compute_checksum(self) -> u64 {
        let mut out = self.to_le_bytes_with_checksum();
        out[64..72].copy_from_slice(&0u64.to_le_bytes());
        xxh64(&out, 0)
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

impl Superblock {
    pub fn to_le_bytes_with_checksum(self) -> [u8; SUPERBLOCK_SIZE] {
        let mut out = [0u8; SUPERBLOCK_SIZE];
        out[0..8].copy_from_slice(&self.generation.to_le_bytes());
        out[8] = self.active;
        out[9..16].copy_from_slice(&self.active_padding);
        out[16..24].copy_from_slice(&self.wal_head.to_le_bytes());
        out[24..32].copy_from_slice(&self.wal_tail.to_le_bytes());
        out[32..40].copy_from_slice(&self.last_committed_seq.to_le_bytes());
        out[40..48].copy_from_slice(&self.metadata_root.to_le_bytes());
        out[48..56].copy_from_slice(&self.data_root.to_le_bytes());
        out[56..64].copy_from_slice(&self.allocator_root.to_le_bytes());
        out[64..72].copy_from_slice(&self.snapshot_root.to_le_bytes());
        out[72..80].copy_from_slice(&0u64.to_le_bytes());
        out[80..].copy_from_slice(&self.reserved);

        let checksum = xxh64(&out, 0);
        out[72..80].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    pub fn from_le_bytes(bytes: &[u8; SUPERBLOCK_SIZE]) -> Self {
        let mut sb = Superblock {
            generation: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            active: bytes[8],
            active_padding: [0; 7],
            wal_head: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            wal_tail: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            last_committed_seq: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            metadata_root: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            data_root: u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
            allocator_root: u64::from_le_bytes(bytes[56..64].try_into().unwrap()),
            snapshot_root: u64::from_le_bytes(bytes[64..72].try_into().unwrap()),
            checksum: u64::from_le_bytes(bytes[72..80].try_into().unwrap()),
            reserved: [0; SUPERBLOCK_RESERVED_SIZE],
        };
        sb.active_padding.copy_from_slice(&bytes[9..16]);
        sb.reserved.copy_from_slice(&bytes[80..]);
        sb
    }

    pub fn compute_checksum(self) -> u64 {
        let mut out = self.to_le_bytes_with_checksum();
        out[72..80].copy_from_slice(&0u64.to_le_bytes());
        xxh64(&out, 0)
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

impl WalEntryHeader {
    pub fn new(
        entry_type: u16,
        entry_version: u16,
        payload_len: u32,
        seq_no: u64,
        logical_ts: u64,
        payload_crc: u64,
    ) -> Self {
        let header_crc = wal_header_crc(entry_type, entry_version, payload_len, seq_no, logical_ts);
        WalEntryHeader {
            entry_type,
            entry_version,
            payload_len,
            seq_no,
            logical_ts,
            header_crc,
            payload_crc,
        }
    }

    pub fn to_le_bytes(self) -> [u8; WAL_ENTRY_HEADER_SIZE] {
        let mut out = [0u8; WAL_ENTRY_HEADER_SIZE];
        out[0..2].copy_from_slice(&self.entry_type.to_le_bytes());
        out[2..4].copy_from_slice(&self.entry_version.to_le_bytes());
        out[4..8].copy_from_slice(&self.payload_len.to_le_bytes());
        out[8..16].copy_from_slice(&self.seq_no.to_le_bytes());
        out[16..24].copy_from_slice(&self.logical_ts.to_le_bytes());
        out[24..32].copy_from_slice(&self.header_crc.to_le_bytes());
        out[32..40].copy_from_slice(&self.payload_crc.to_le_bytes());
        out
    }

    pub fn from_le_bytes(bytes: &[u8; WAL_ENTRY_HEADER_SIZE]) -> Self {
        WalEntryHeader {
            entry_type: u16::from_le_bytes(bytes[0..2].try_into().unwrap()),
            entry_version: u16::from_le_bytes(bytes[2..4].try_into().unwrap()),
            payload_len: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            seq_no: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            logical_ts: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            header_crc: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            payload_crc: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        }
    }

    pub fn verify_header_crc(&self) -> bool {
        self.header_crc
            == wal_header_crc(
                self.entry_type,
                self.entry_version,
                self.payload_len,
                self.seq_no,
                self.logical_ts,
            )
    }
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

#[inline]
pub fn wal_header_crc(
    entry_type: u16,
    entry_version: u16,
    payload_len: u32,
    seq_no: u64,
    logical_ts: u64,
) -> u64 {
    let mut buf = [0u8; 2 + 2 + 4 + 8 + 8];
    buf[0..2].copy_from_slice(&entry_type.to_le_bytes());
    buf[2..4].copy_from_slice(&entry_version.to_le_bytes());
    buf[4..8].copy_from_slice(&payload_len.to_le_bytes());
    buf[8..16].copy_from_slice(&seq_no.to_le_bytes());
    buf[16..24].copy_from_slice(&logical_ts.to_le_bytes());
    xxh64(&buf, 0)
}

#[inline]
pub fn wal_payload_crc(payload: &[u8]) -> u64 {
    xxh64(payload, 0)
}

#[inline]
pub fn align_down(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return value;
    }
    value - (value % alignment)
}

pub const SNAPSHOT_DESCRIPTOR_SIZE: usize = PAGE_SIZE;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SnapshotDescriptor {
    pub magic: [u8; 8],
    pub generation: u64,
    pub slot: u8,
    pub slot_padding: [u8; 7],
    pub checkpoint_seq: u64,
    pub data_offset: u64,
    pub data_len: u64,
    pub data_checksum: u64,
    pub next_seq_no: u64,
    pub next_doc_id: u64,
    pub checksum: u64,
    pub reserved: [u8; SNAPSHOT_DESCRIPTOR_RESERVED_SIZE],
}

pub const SNAPSHOT_MAGIC: [u8; 8] = *b"TDBSNAP\0";

const SNAPSHOT_DESCRIPTOR_FIXED_SIZE: usize = 8 + 8 + 1 + 7 + (7 * 8);
const SNAPSHOT_DESCRIPTOR_RESERVED_SIZE: usize = SNAPSHOT_DESCRIPTOR_SIZE - SNAPSHOT_DESCRIPTOR_FIXED_SIZE;

impl Default for SnapshotDescriptor {
    fn default() -> Self {
        SnapshotDescriptor {
            magic: SNAPSHOT_MAGIC,
            generation: 0,
            slot: 0,
            slot_padding: [0; 7],
            checkpoint_seq: 0,
            data_offset: 0,
            data_len: 0,
            data_checksum: 0,
            next_seq_no: 0,
            next_doc_id: 0,
            checksum: 0,
            reserved: [0; SNAPSHOT_DESCRIPTOR_RESERVED_SIZE],
        }
    }
}

impl fmt::Debug for SnapshotDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotDescriptor")
            .field("generation", &self.generation)
            .field("slot", &self.slot)
            .field("checkpoint_seq", &self.checkpoint_seq)
            .field("data_offset", &self.data_offset)
            .field("data_len", &self.data_len)
            .field("data_checksum", &self.data_checksum)
            .field("next_seq_no", &self.next_seq_no)
            .field("next_doc_id", &self.next_doc_id)
            .field("checksum", &self.checksum)
            .finish()
    }
}

impl SnapshotDescriptor {
    pub fn to_le_bytes_with_checksum(self) -> [u8; SNAPSHOT_DESCRIPTOR_SIZE] {
        let mut out = [0u8; SNAPSHOT_DESCRIPTOR_SIZE];
        out[0..8].copy_from_slice(&self.magic);
        out[8..16].copy_from_slice(&self.generation.to_le_bytes());
        out[16] = self.slot;
        out[17..24].copy_from_slice(&self.slot_padding);
        out[24..32].copy_from_slice(&self.checkpoint_seq.to_le_bytes());
        out[32..40].copy_from_slice(&self.data_offset.to_le_bytes());
        out[40..48].copy_from_slice(&self.data_len.to_le_bytes());
        out[48..56].copy_from_slice(&self.data_checksum.to_le_bytes());
        out[56..64].copy_from_slice(&self.next_seq_no.to_le_bytes());
        out[64..72].copy_from_slice(&self.next_doc_id.to_le_bytes());
        out[72..80].copy_from_slice(&0u64.to_le_bytes());
        out[80..].copy_from_slice(&self.reserved);

        let checksum = xxh64(&out, 0);
        out[72..80].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    pub fn from_le_bytes(bytes: &[u8; SNAPSHOT_DESCRIPTOR_SIZE]) -> Self {
        let mut desc = SnapshotDescriptor::default();
        desc.magic.copy_from_slice(&bytes[0..8]);
        desc.generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        desc.slot = bytes[16];
        desc.slot_padding.copy_from_slice(&bytes[17..24]);
        desc.checkpoint_seq = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        desc.data_offset = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        desc.data_len = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
        desc.data_checksum = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
        desc.next_seq_no = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
        desc.next_doc_id = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
        desc.checksum = u64::from_le_bytes(bytes[72..80].try_into().unwrap());
        desc.reserved.copy_from_slice(&bytes[80..]);
        desc
    }

    pub fn compute_checksum(self) -> u64 {
        let mut out = self.to_le_bytes_with_checksum();
        out[72..80].copy_from_slice(&0u64.to_le_bytes());
        xxh64(&out, 0)
    }

    pub fn is_valid(&self) -> bool {
        self.magic == SNAPSHOT_MAGIC && self.checksum == self.compute_checksum() && self.data_len > 0
    }
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
    debug_assert_eq!(size_of::<SnapshotDescriptor>(), SNAPSHOT_DESCRIPTOR_SIZE);
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
