use std::path::{Path, PathBuf};

use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

use crate::allocator::PageAllocator;
use crate::direct_io::DirectFile;
use crate::storage_layout::{
    FileHeader, PAGE_SIZE, SNAPSHOT_DESCRIPTOR_SIZE, SUPERBLOCK_ACTIVE_A, SUPERBLOCK_ACTIVE_B,
    SnapshotDescriptor, Superblock, WAL_ENTRY_FOOTER_SIZE, WAL_ENTRY_HEADER_SIZE,
    WAL_ENTRY_TYPE_COMMIT, WAL_ENTRY_TYPE_RECORD, WAL_MAX_BYTES, WAL_MIN_BYTES, WalEntryFooter,
    WalEntryHeader, align_down, assert_layout_invariants, wal_entry_padded_len, wal_payload_crc,
};

#[derive(Debug, Clone, Copy)]
pub struct StorageOptions {
    pub size_gib: u64,
    pub wal_ratio: f64,
    pub metadata_ratio: f64,
    pub snapshot_ratio: f64,
    pub allocator_ratio: f64,
    pub reserved_ratio: f64,
}

impl StorageOptions {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.size_gib == 0 {
            return Err(StorageError::InvalidConfig(
                "storage.size_gib must be > 0".to_string(),
            ));
        }
        for (name, value) in [
            ("wal_ratio", self.wal_ratio),
            ("metadata_ratio", self.metadata_ratio),
            ("snapshot_ratio", self.snapshot_ratio),
            ("allocator_ratio", self.allocator_ratio),
            ("reserved_ratio", self.reserved_ratio),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(StorageError::InvalidConfig(format!(
                    "storage.{name} must be between 0.0 and 1.0"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageLayout {
    pub total_size: u64,
    pub header_offset: u64,
    pub superblock_a_offset: u64,
    pub superblock_b_offset: u64,
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
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("invalid storage file: {0}")]
    InvalidFile(String),

    #[error("wal ring full: {0}")]
    WalFull(String),
}

pub struct Storage {
    file: StorageFile,
}

impl Storage {
    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        assert_layout_invariants();
        let file = StorageFile::open_existing(path)?;
        Ok(Storage { file })
    }

    pub fn create(path: PathBuf, opts: StorageOptions) -> Result<Self, StorageError> {
        assert_layout_invariants();
        opts.validate()?;
        let file = StorageFile::create_new(path, opts)?;
        Ok(Storage { file })
    }

    pub fn path(&self) -> &Path {
        &self.file.path
    }

    pub fn append_wal_entry(
        &mut self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        logical_ts: u64,
        payload: &[u8],
    ) -> Result<u64, StorageError> {
        self.file
            .append_wal_entry(entry_type, entry_version, seq_no, logical_ts, payload)
    }

    pub fn verify_wal_entry_at(&mut self, position: u64) -> Result<bool, StorageError> {
        self.file.verify_wal_entry_at(position)
    }

    pub fn recover_wal(&mut self) -> Result<RecoveryState, StorageError> {
        self.file.recover_wal()
    }

    pub fn replay_wal_entries(&mut self) -> Result<Vec<WalRecord>, StorageError> {
        self.file.replay_wal_entries()
    }

    pub fn write_checkpoint(
        &mut self,
        data: &[u8],
        checkpoint_seq: u64,
        next_seq_no: u64,
        next_doc_id: u64,
    ) -> Result<(), StorageError> {
        self.file
            .write_checkpoint(data, checkpoint_seq, next_seq_no, next_doc_id)
    }

    pub fn load_snapshot(&mut self) -> Result<Option<SnapshotData>, StorageError> {
        self.file.load_snapshot()
    }

    pub fn wal_usage_ratio(&self) -> f64 {
        self.file.wal_usage_ratio()
    }
}

pub struct SnapshotData {
    pub data: Vec<u8>,
    pub checkpoint_seq: u64,
    pub next_seq_no: u64,
    pub next_doc_id: u64,
}

struct StorageFile {
    path: PathBuf,
    file: DirectFile,
    layout: StorageLayout,
    header: FileHeader,
    superblock_a: Superblock,
    superblock_b: Superblock,
    active_superblock: ActiveSuperblock,
    wal_state: WalRingState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveSuperblock {
    A,
    B,
}

impl ActiveSuperblock {
    fn from_superblocks(a: &Superblock, b: &Superblock, a_valid: bool, b_valid: bool) -> Self {
        match (a_valid, b_valid) {
            (true, true) => {
                if b.generation > a.generation {
                    ActiveSuperblock::B
                } else {
                    ActiveSuperblock::A
                }
            }
            (true, false) => ActiveSuperblock::A,
            (false, true) => ActiveSuperblock::B,
            (false, false) => ActiveSuperblock::A,
        }
    }
}

impl StorageFile {
    fn open_existing(path: PathBuf) -> Result<Self, StorageError> {
        let mut file = DirectFile::open_existing(path.clone())?;
        let mut header_bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
        file.read_exact_at(0, &mut header_bytes)?;
        let header = FileHeader::from_le_bytes(&header_bytes);
        let expected_header_checksum = header.compute_checksum();

        if header.magic != crate::storage_layout::FILE_MAGIC {
            return Err(StorageError::InvalidFile("bad magic".to_string()));
        }
        if header.version != crate::storage_layout::FILE_VERSION {
            return Err(StorageError::InvalidFile("unsupported version".to_string()));
        }
        if header.page_size as usize != crate::storage_layout::PAGE_SIZE {
            return Err(StorageError::InvalidFile("page size mismatch".to_string()));
        }
        if header.header_checksum != expected_header_checksum {
            return Err(StorageError::InvalidFile(
                "header checksum mismatch".to_string(),
            ));
        }

        let mut sb_a_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
        file.read_exact_at(header.superblock_a_offset, &mut sb_a_bytes)?;
        let superblock_a = Superblock::from_le_bytes(&sb_a_bytes);
        let sb_a_valid = superblock_a.checksum == superblock_a.compute_checksum();

        let mut sb_b_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
        file.read_exact_at(header.superblock_b_offset, &mut sb_b_bytes)?;
        let superblock_b = Superblock::from_le_bytes(&sb_b_bytes);
        let sb_b_valid = superblock_b.checksum == superblock_b.compute_checksum();

        if !sb_a_valid && !sb_b_valid {
            return Err(StorageError::InvalidFile(
                "both superblocks have checksum mismatch".to_string(),
            ));
        }

        let active_superblock = ActiveSuperblock::from_superblocks(
            &superblock_a,
            &superblock_b,
            sb_a_valid,
            sb_b_valid,
        );

        let layout = StorageLayout {
            total_size: file.len(),
            header_offset: 0,
            superblock_a_offset: header.superblock_a_offset,
            superblock_b_offset: header.superblock_b_offset,
            wal_offset: header.wal_offset,
            wal_size: header.wal_size,
            data_offset: header.data_offset,
            data_size: header.data_size,
            metadata_offset: header.metadata_offset,
            metadata_size: header.metadata_size,
            allocator_offset: header.allocator_offset,
            allocator_size: header.allocator_size,
            snapshot_offset: header.snapshot_offset,
            snapshot_size: header.snapshot_size,
            reserved_offset: header.reserved_offset,
            reserved_size: header.reserved_size,
        };

        let active_sb = match active_superblock {
            ActiveSuperblock::A => &superblock_a,
            ActiveSuperblock::B => &superblock_b,
        };
        let wal_state = WalRingState {
            head: active_sb.wal_head,
            tail: active_sb.wal_tail,
            offset: header.wal_offset,
            size: header.wal_size,
        };

        let file = StorageFile {
            path,
            file,
            layout,
            header,
            superblock_a,
            superblock_b,
            active_superblock,
            wal_state,
        };
        file.touch();
        Ok(file)
    }

    fn create_new(path: PathBuf, opts: StorageOptions) -> Result<Self, StorageError> {
        let layout = compute_layout(opts)?;
        let mut header = FileHeader::default();
        header.superblock_a_offset = layout.superblock_a_offset;
        header.superblock_b_offset = layout.superblock_b_offset;
        header.wal_offset = layout.wal_offset;
        header.wal_size = layout.wal_size;
        header.data_offset = layout.data_offset;
        header.data_size = layout.data_size;
        header.metadata_offset = layout.metadata_offset;
        header.metadata_size = layout.metadata_size;
        header.allocator_offset = layout.allocator_offset;
        header.allocator_size = layout.allocator_size;
        header.snapshot_offset = layout.snapshot_offset;
        header.snapshot_size = layout.snapshot_size;
        header.reserved_offset = layout.reserved_offset;
        header.reserved_size = layout.reserved_size;
        header.header_checksum = header.compute_checksum();

        let mut superblock_a = Superblock::default();
        superblock_a.checksum = superblock_a.compute_checksum();
        let mut superblock_b = Superblock::default();
        superblock_b.active = crate::storage_layout::SUPERBLOCK_ACTIVE_B;
        superblock_b.checksum = superblock_b.compute_checksum();

        let mut file = DirectFile::create_new(path.clone(), layout.total_size)?;

        file.write_all_at(layout.header_offset, &header.to_le_bytes_with_checksum())?;
        file.write_all_at(
            layout.superblock_a_offset,
            &superblock_a.to_le_bytes_with_checksum(),
        )?;
        file.write_all_at(
            layout.superblock_b_offset,
            &superblock_b.to_le_bytes_with_checksum(),
        )?;
        file.sync_data()?;

        let wal_state = WalRingState {
            head: superblock_a.wal_head,
            tail: superblock_a.wal_tail,
            offset: layout.wal_offset,
            size: layout.wal_size,
        };

        let file = StorageFile {
            path,
            file,
            layout,
            header,
            superblock_a,
            superblock_b,
            active_superblock: ActiveSuperblock::A,
            wal_state,
        };
        file.touch();
        Ok(file)
    }

    fn touch(&self) {
        let _ = self.layout.total_size;
        let _ = self.header.magic;
        let _ = self.superblock_a.generation;
        let _ = self.superblock_b.generation;
    }

    fn append_wal_entry(
        &mut self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        logical_ts: u64,
        payload: &[u8],
    ) -> Result<u64, StorageError> {
        if self.wal_state.size == 0 {
            return Err(StorageError::InvalidFile(
                "wal region size is zero".to_string(),
            ));
        }
        let payload_len = payload.len();
        let entry_len = wal_entry_padded_len(payload_len);
        if entry_len as u64 > self.wal_state.size {
            return Err(StorageError::WalFull(
                "entry larger than wal ring".to_string(),
            ));
        }

        let used = self.wal_state.tail.saturating_sub(self.wal_state.head);
        let free = self.wal_state.size.saturating_sub(used);

        let tail_offset = (self.wal_state.tail % self.wal_state.size) as usize;
        let bytes_to_end = self.wal_state.size as usize - tail_offset;
        let needs_wrap = entry_len > bytes_to_end;
        let required = if needs_wrap {
            bytes_to_end + entry_len
        } else {
            entry_len
        };

        if required as u64 > free {
            return Err(StorageError::WalFull("wal ring full".to_string()));
        }

        if needs_wrap && bytes_to_end > 0 {
            let write_pos = self.wal_state.offset + tail_offset as u64;
            let zeroes = vec![0u8; bytes_to_end];
            self.file.write_all_at(write_pos, &zeroes)?;
            self.wal_state.tail = self.wal_state.tail.saturating_add(bytes_to_end as u64);
        }

        let write_pos = self.wal_state.offset + (self.wal_state.tail % self.wal_state.size);

        let payload_crc = wal_payload_crc(payload);
        let header = WalEntryHeader::new(
            entry_type,
            entry_version,
            payload_len as u32,
            seq_no,
            logical_ts,
            payload_crc,
        );

        let footer = WalEntryFooter {
            payload_len: payload_len as u32,
            payload_crc,
        };
        let mut entry_bytes = Vec::with_capacity(entry_len);
        entry_bytes.extend_from_slice(&header.to_le_bytes());
        entry_bytes.extend_from_slice(payload);
        entry_bytes.extend_from_slice(&footer.to_le_bytes());
        entry_bytes.resize(entry_len, 0);

        self.file.write_all_at(write_pos, &entry_bytes)?;

        self.wal_state.tail = self.wal_state.tail.saturating_add(entry_len as u64);
        self.sync_active_superblock(seq_no)?;
        self.file.sync_data()?;

        debug_assert!(self.verify_wal_entry_at(write_pos).unwrap_or(false));

        Ok(write_pos)
    }

    fn verify_wal_entry_at(&mut self, position: u64) -> Result<bool, StorageError> {
        let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
        self.file.read_exact_at(position, &mut header_bytes)?;
        let header = WalEntryHeader::from_le_bytes(&header_bytes);
        if !header.verify_header_crc() {
            return Ok(false);
        }

        let payload_len = header.payload_len as usize;
        let mut payload = vec![0u8; payload_len];
        self.file
            .read_exact_at(position + WAL_ENTRY_HEADER_SIZE as u64, &mut payload)?;
        let payload_crc = wal_payload_crc(&payload);
        if payload_crc != header.payload_crc {
            return Ok(false);
        }

        let mut footer_bytes = [0u8; WAL_ENTRY_FOOTER_SIZE];
        self.file.read_exact_at(
            position + WAL_ENTRY_HEADER_SIZE as u64 + payload_len as u64,
            &mut footer_bytes,
        )?;
        let footer = WalEntryFooter::from_le_bytes(&footer_bytes);
        if footer.payload_len != header.payload_len || footer.payload_crc != header.payload_crc {
            return Ok(false);
        }

        Ok(true)
    }

    fn recover_wal(&mut self) -> Result<RecoveryState, StorageError> {
        let wal_offset = self.layout.wal_offset;
        let wal_size = self.layout.wal_size;
        let mut cursor = self.wal_state.head;
        let tail = self.wal_state.tail;

        if wal_size == 0 || tail <= cursor {
            return Ok(RecoveryState::default());
        }

        let mut last_valid_seq = None;
        let mut last_committed_seq = None;
        let mut bytes_scanned = 0u64;
        let _ = WAL_ENTRY_TYPE_RECORD;

        while cursor < tail {
            let ring_pos = cursor % wal_size;
            let bytes_to_end = wal_size - ring_pos;

            if bytes_to_end < WAL_ENTRY_HEADER_SIZE as u64 {
                cursor = cursor.saturating_add(bytes_to_end);
                bytes_scanned = bytes_scanned.saturating_add(bytes_to_end);
                continue;
            }

            let file_pos = wal_offset + ring_pos;

            let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
            self.file.read_exact_at(file_pos, &mut header_bytes)?;
            if header_bytes.iter().all(|b| *b == 0) {
                cursor = cursor.saturating_add(bytes_to_end);
                bytes_scanned = bytes_scanned.saturating_add(bytes_to_end);
                continue;
            }

            let header = WalEntryHeader::from_le_bytes(&header_bytes);
            if !header.verify_header_crc() {
                break;
            }

            let payload_len = header.payload_len as usize;
            let entry_len = wal_entry_padded_len(payload_len) as u64;
            if entry_len > wal_size {
                break;
            }

            let total_len = (WAL_ENTRY_HEADER_SIZE + payload_len + WAL_ENTRY_FOOTER_SIZE) as u64;
            if total_len > bytes_to_end {
                break;
            }

            let mut payload = vec![0u8; payload_len];
            self.file
                .read_exact_at(file_pos + WAL_ENTRY_HEADER_SIZE as u64, &mut payload)?;
            let payload_crc = wal_payload_crc(&payload);
            if payload_crc != header.payload_crc {
                break;
            }

            let mut footer_bytes = [0u8; WAL_ENTRY_FOOTER_SIZE];
            self.file.read_exact_at(
                file_pos + WAL_ENTRY_HEADER_SIZE as u64 + payload_len as u64,
                &mut footer_bytes,
            )?;
            let footer = WalEntryFooter::from_le_bytes(&footer_bytes);
            if footer.payload_len != header.payload_len || footer.payload_crc != header.payload_crc
            {
                break;
            }

            if header.entry_type == WAL_ENTRY_TYPE_COMMIT && payload_len >= 18 {
                let commit_seq = u64::from_le_bytes(payload[8..16].try_into().unwrap());
                last_committed_seq = Some(commit_seq);
            }

            last_valid_seq = Some(header.seq_no);

            cursor = cursor.saturating_add(entry_len);
            bytes_scanned = bytes_scanned.saturating_add(entry_len);
        }

        Ok(RecoveryState {
            last_valid_seq,
            last_committed_seq,
            bytes_scanned,
        })
    }

    fn replay_wal_entries(&mut self) -> Result<Vec<WalRecord>, StorageError> {
        let wal_offset = self.layout.wal_offset;
        let wal_size = self.layout.wal_size;
        let mut cursor = self.wal_state.head;
        let tail = self.wal_state.tail;
        let mut entries = Vec::new();

        if wal_size == 0 || tail <= cursor {
            return Ok(entries);
        }

        while cursor < tail {
            let ring_pos = cursor % wal_size;
            let bytes_to_end = wal_size - ring_pos;

            if bytes_to_end < WAL_ENTRY_HEADER_SIZE as u64 {
                cursor = cursor.saturating_add(bytes_to_end);
                continue;
            }

            let file_pos = wal_offset + ring_pos;

            let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
            self.file.read_exact_at(file_pos, &mut header_bytes)?;
            if header_bytes.iter().all(|b| *b == 0) {
                cursor = cursor.saturating_add(bytes_to_end);
                continue;
            }

            let header = WalEntryHeader::from_le_bytes(&header_bytes);
            if !header.verify_header_crc() {
                break;
            }

            let payload_len = header.payload_len as usize;
            let entry_len = wal_entry_padded_len(payload_len) as u64;
            if entry_len > wal_size {
                break;
            }

            let total_len = (WAL_ENTRY_HEADER_SIZE + payload_len + WAL_ENTRY_FOOTER_SIZE) as u64;
            if total_len > bytes_to_end {
                break;
            }

            let mut payload = vec![0u8; payload_len];
            self.file
                .read_exact_at(file_pos + WAL_ENTRY_HEADER_SIZE as u64, &mut payload)?;
            let payload_crc = wal_payload_crc(&payload);
            if payload_crc != header.payload_crc {
                break;
            }

            let mut footer_bytes = [0u8; WAL_ENTRY_FOOTER_SIZE];
            self.file.read_exact_at(
                file_pos + WAL_ENTRY_HEADER_SIZE as u64 + payload_len as u64,
                &mut footer_bytes,
            )?;
            let footer = WalEntryFooter::from_le_bytes(&footer_bytes);
            if footer.payload_len != header.payload_len || footer.payload_crc != header.payload_crc
            {
                break;
            }

            entries.push(WalRecord {
                entry_type: header.entry_type,
                entry_version: header.entry_version,
                seq_no: header.seq_no,
                logical_ts: header.logical_ts,
                payload,
            });

            cursor = cursor.saturating_add(entry_len);
        }

        Ok(entries)
    }

    fn sync_active_superblock(&mut self, last_committed_seq: u64) -> Result<(), StorageError> {
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);

        let (sb, offset) = match self.active_superblock {
            ActiveSuperblock::A => (&mut self.superblock_a, self.layout.superblock_a_offset),
            ActiveSuperblock::B => (&mut self.superblock_b, self.layout.superblock_b_offset),
        };

        sb.generation = generation;
        sb.active = match self.active_superblock {
            ActiveSuperblock::A => SUPERBLOCK_ACTIVE_A,
            ActiveSuperblock::B => SUPERBLOCK_ACTIVE_B,
        };
        sb.wal_head = self.wal_state.head;
        sb.wal_tail = self.wal_state.tail;
        sb.last_committed_seq = last_committed_seq;
        sb.checksum = sb.compute_checksum();

        self.file
            .write_all_at(offset, &sb.to_le_bytes_with_checksum())?;
        Ok(())
    }

    fn write_checkpoint(
        &mut self,
        data: &[u8],
        checkpoint_seq: u64,
        next_seq_no: u64,
        next_doc_id: u64,
    ) -> Result<(), StorageError> {
        let page_size = PAGE_SIZE as u64;
        let padded_len = ((data.len() as u64 + page_size - 1) / page_size) * page_size;
        let num_pages = padded_len / page_size;

        if padded_len > self.layout.data_size {
            return Err(StorageError::InvalidConfig(
                "checkpoint data exceeds data region size".to_string(),
            ));
        }

        let mut allocator = self.load_allocator()?;

        let current_snapshot = self.load_active_snapshot_descriptor()?;
        let target_slot: u8 = if let Some(ref desc) = current_snapshot {
            if desc.slot == 0 { 1 } else { 0 }
        } else {
            0
        };

        // Two-slot A/B strategy: slot 0 = first half of data region, slot 1 = second half.
        let half_pages = self.layout.data_size / page_size / 2;
        let slot_start_page = if target_slot == 0 { 0 } else { half_pages };

        if num_pages > half_pages {
            return Err(StorageError::InvalidConfig(
                "checkpoint data exceeds half of data region (slot capacity)".to_string(),
            ));
        }

        // Free the target slot region and reallocate
        allocator.free(slot_start_page, half_pages);
        let alloc_start = allocator.allocate(num_pages).ok_or_else(|| {
            StorageError::InvalidConfig("cannot allocate pages for checkpoint".to_string())
        })?;

        let data_write_offset = self.layout.data_offset + alloc_start * page_size;

        // Write the data (padded to page alignment)
        let mut padded_data = data.to_vec();
        padded_data.resize(padded_len as usize, 0);
        self.file.write_all_at(data_write_offset, &padded_data)?;
        self.file.sync_data()?;

        // Write allocator bitmap
        self.save_allocator(&allocator)?;
        self.file.sync_data()?;

        // Write snapshot descriptor
        let data_checksum = xxh64(data, 0);
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);

        let mut desc = SnapshotDescriptor::default();
        desc.generation = generation;
        desc.slot = target_slot;
        desc.checkpoint_seq = checkpoint_seq;
        desc.data_offset = data_write_offset;
        desc.data_len = data.len() as u64;
        desc.data_checksum = data_checksum;
        desc.next_seq_no = next_seq_no;
        desc.next_doc_id = next_doc_id;
        desc.checksum = desc.compute_checksum();

        let desc_offset =
            self.layout.snapshot_offset + (target_slot as u64) * SNAPSHOT_DESCRIPTOR_SIZE as u64;
        self.file
            .write_all_at(desc_offset, &desc.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;

        // Advance wal_head to reclaim entries covered by this checkpoint
        self.wal_state.head = self.wal_state.tail;

        // Flip to the inactive superblock and write it with the new state
        let new_active = match self.active_superblock {
            ActiveSuperblock::A => ActiveSuperblock::B,
            ActiveSuperblock::B => ActiveSuperblock::A,
        };
        self.active_superblock = new_active;

        // Build the new superblock state for both
        let new_sb = |active_flag: u8| -> Superblock {
            let mut sb = Superblock::default();
            sb.generation = generation;
            sb.active = active_flag;
            sb.wal_head = self.wal_state.head;
            sb.wal_tail = self.wal_state.tail;
            sb.last_committed_seq = checkpoint_seq;
            sb.snapshot_root = desc_offset;
            sb.data_root = data_write_offset;
            sb.checksum = sb.compute_checksum();
            sb
        };

        self.superblock_a = new_sb(SUPERBLOCK_ACTIVE_A);
        self.superblock_b = new_sb(SUPERBLOCK_ACTIVE_B);

        // Write primary (new active) first, then backup
        let (primary_offset, primary_sb, backup_offset, backup_sb) = match new_active {
            ActiveSuperblock::A => (
                self.layout.superblock_a_offset,
                &self.superblock_a,
                self.layout.superblock_b_offset,
                &self.superblock_b,
            ),
            ActiveSuperblock::B => (
                self.layout.superblock_b_offset,
                &self.superblock_b,
                self.layout.superblock_a_offset,
                &self.superblock_a,
            ),
        };

        self.file
            .write_all_at(primary_offset, &primary_sb.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;

        self.file
            .write_all_at(backup_offset, &backup_sb.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;

        Ok(())
    }

    fn load_active_snapshot_descriptor(
        &mut self,
    ) -> Result<Option<SnapshotDescriptor>, StorageError> {
        // Try slot 0 and slot 1, return the one with higher generation
        let mut best: Option<SnapshotDescriptor> = None;

        for slot in 0..2u8 {
            let desc_offset =
                self.layout.snapshot_offset + (slot as u64) * SNAPSHOT_DESCRIPTOR_SIZE as u64;
            if desc_offset + SNAPSHOT_DESCRIPTOR_SIZE as u64
                > self.layout.snapshot_offset + self.layout.snapshot_size
            {
                continue;
            }
            let mut desc_bytes = [0u8; SNAPSHOT_DESCRIPTOR_SIZE];
            self.file.read_exact_at(desc_offset, &mut desc_bytes)?;
            let desc = SnapshotDescriptor::from_le_bytes(&desc_bytes);
            if desc.is_valid() {
                if best
                    .as_ref()
                    .map_or(true, |b| desc.generation > b.generation)
                {
                    best = Some(desc);
                }
            }
        }

        Ok(best)
    }

    fn load_snapshot(&mut self) -> Result<Option<SnapshotData>, StorageError> {
        let desc = match self.load_active_snapshot_descriptor()? {
            Some(d) => d,
            None => return Ok(None),
        };

        let mut data = vec![0u8; desc.data_len as usize];
        self.file.read_exact_at(desc.data_offset, &mut data)?;

        let actual_checksum = xxh64(&data, 0);
        if actual_checksum != desc.data_checksum {
            return Err(StorageError::InvalidFile(
                "snapshot data checksum mismatch".to_string(),
            ));
        }

        Ok(Some(SnapshotData {
            data,
            checkpoint_seq: desc.checkpoint_seq,
            next_seq_no: desc.next_seq_no,
            next_doc_id: desc.next_doc_id,
        }))
    }

    fn load_allocator(&mut self) -> Result<PageAllocator, StorageError> {
        if self.layout.allocator_size == 0 {
            return Ok(PageAllocator::new(self.layout.data_size));
        }
        let bitmap_len = ((self.layout.data_size / PAGE_SIZE as u64 + 7) / 8) as usize;
        let read_len = bitmap_len.min(self.layout.allocator_size as usize);
        let mut bitmap = vec![0u8; bitmap_len];
        self.file
            .read_exact_at(self.layout.allocator_offset, &mut bitmap[..read_len])?;
        Ok(PageAllocator::from_bitmap(bitmap, self.layout.data_size))
    }

    fn save_allocator(&mut self, allocator: &PageAllocator) -> Result<(), StorageError> {
        let bitmap = allocator.bitmap();
        let write_len = bitmap.len().min(self.layout.allocator_size as usize);
        self.file
            .write_all_at(self.layout.allocator_offset, &bitmap[..write_len])?;
        Ok(())
    }

    fn wal_usage_ratio(&self) -> f64 {
        if self.wal_state.size == 0 {
            return 1.0;
        }
        let used = self.wal_state.tail.saturating_sub(self.wal_state.head);
        used as f64 / self.wal_state.size as f64
    }
}

#[derive(Debug, Clone, Copy)]
struct WalRingState {
    head: u64,
    tail: u64,
    offset: u64,
    size: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RecoveryState {
    pub last_valid_seq: Option<u64>,
    pub last_committed_seq: Option<u64>,
    pub bytes_scanned: u64,
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub entry_type: u16,
    pub entry_version: u16,
    pub seq_no: u64,
    pub logical_ts: u64,
    pub payload: Vec<u8>,
}

fn compute_layout(opts: StorageOptions) -> Result<StorageLayout, StorageError> {
    let total_size = opts
        .size_gib
        .checked_mul(crate::storage_layout::GIB)
        .ok_or_else(|| StorageError::InvalidConfig("storage.size_gib overflow".to_string()))?;

    let page = crate::storage_layout::PAGE_SIZE as u64;
    let fixed_size = page * 3;
    if total_size <= fixed_size {
        return Err(StorageError::InvalidConfig(
            "storage.size_gib too small for header/superblocks".to_string(),
        ));
    }

    let wal_raw = (total_size as f64 * opts.wal_ratio) as u64;
    let wal_clamped = wal_raw.clamp(WAL_MIN_BYTES, WAL_MAX_BYTES);
    let wal_size = align_down(wal_clamped, page);

    let metadata_size = align_down((total_size as f64 * opts.metadata_ratio) as u64, page);
    let snapshot_size = align_down((total_size as f64 * opts.snapshot_ratio) as u64, page);
    let allocator_size = align_down((total_size as f64 * opts.allocator_ratio) as u64, page);
    let reserved_target = align_down((total_size as f64 * opts.reserved_ratio) as u64, page);

    let mut remaining = total_size
        .saturating_sub(fixed_size)
        .saturating_sub(wal_size)
        .saturating_sub(metadata_size)
        .saturating_sub(snapshot_size)
        .saturating_sub(allocator_size)
        .saturating_sub(reserved_target);

    remaining = align_down(remaining, page);

    if remaining == 0 {
        return Err(StorageError::InvalidConfig(
            "storage ratios leave no space for data region".to_string(),
        ));
    }

    let reserved_size = total_size
        .saturating_sub(fixed_size)
        .saturating_sub(wal_size)
        .saturating_sub(metadata_size)
        .saturating_sub(snapshot_size)
        .saturating_sub(allocator_size)
        .saturating_sub(remaining);

    let header_offset = 0;
    let superblock_a_offset = page;
    let superblock_b_offset = page * 2;
    let wal_offset = page * 3;
    let data_offset = wal_offset + wal_size;
    let metadata_offset = data_offset + remaining;
    let allocator_offset = metadata_offset + metadata_size;
    let snapshot_offset = allocator_offset + allocator_size;
    let reserved_offset = snapshot_offset + snapshot_size;

    Ok(StorageLayout {
        total_size,
        header_offset,
        superblock_a_offset,
        superblock_b_offset,
        wal_offset,
        wal_size,
        data_offset,
        data_size: remaining,
        metadata_offset,
        metadata_size,
        allocator_offset,
        allocator_size,
        snapshot_offset,
        snapshot_size,
        reserved_offset,
        reserved_size,
    })
}
