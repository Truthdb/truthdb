use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::*;
use crate::relstore::recovery::LogicalUndo;
use crate::storage_layout::{
    FILE_HEADER_SIZE, FILE_VERSION, FILE_VERSION_V1, SUPERBLOCK_SIZE, WAL_ENTRY_TYPE_RECORD,
    WalEntryFooter, WalEntryHeader, wal_entry_padded_len, wal_payload_crc,
};

/// Small ring so wrap/full paths are cheap to reach.
const TEST_WAL_BYTES: u64 = 64 * 1024;

fn test_storage_options() -> StorageOptions {
    StorageOptions {
        size_bytes: 64 << 20,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
        default_collation: None,
    }
}

fn unique_temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    path.push(format!("truthdb-storage-{label}-{nanos}.db"));
    path
}

/// The length a log entry with a `payload_len`-byte payload takes in the
/// ring (sealed entries carry the writer id and tag too).
fn stored_entry_len(payload_len: usize) -> u64 {
    let sealed = if crate::encryption::test_key_source().is_some() {
        crate::encryption::WAL_SEAL_OVERHEAD
    } else {
        0
    };
    wal_entry_padded_len(payload_len + sealed) as u64
}

fn create_small(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        test_storage_options(),
        TEST_WAL_BYTES,
        TEST_WAL_BYTES,
        no_engine,
    )
    .expect("create storage")
}

mod blob_pages;
mod btree_free;
mod encryption;
mod engines;
mod recovery;
mod txn_extents;

/// The log alone, with no page-structured engine above it: these tests
/// exercise the WAL ring, checkpoints, the allocator and backups.
struct NoEngine;

struct NoLogicalUndo;

impl LogicalUndo for NoLogicalUndo {
    fn undo(
        &self,
        _ctx: &mut RelCtx<'_>,
        _mode: &mut crate::relstore::ctx::OpMode<'_>,
        object_id: u32,
        _undo: &crate::wal::records::PageOpUndo,
    ) -> Result<(), StorageError> {
        Err(StorageError::InvalidFile(format!(
            "no engine to undo tree object {object_id}"
        )))
    }
}

impl EngineParticipant for NoEngine {
    fn engine_id(&self) -> u32 {
        CATALOG_ENGINE_ID
    }
    fn owns_object(&self, _object_id: u32) -> bool {
        false
    }
    fn open(&mut self, _metadata_root: Option<u64>, _db_options: u8) {}
    fn metadata_root(&self) -> Option<u64> {
        None
    }
    fn set_metadata_root(&mut self, _page: u64) {}
    fn options_byte(&self) -> u8 {
        0
    }
    fn reload(&mut self, _core: &mut StorageFile) -> Result<(), StorageError> {
        Ok(())
    }
    fn recovery_undo(&self) -> Box<dyn LogicalUndo> {
        Box::new(NoLogicalUndo)
    }
    fn capture_standby(
        &mut self,
        _core: &mut StorageFile,
        _records: &[(u64, RelRecord)],
        _floor: u64,
    ) {
    }
    fn finish_recovery(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) fn no_engine() -> Vec<Box<dyn EngineParticipant>> {
    vec![Box::new(NoEngine)]
}

#[test]
fn whole_gib_layouts_are_unchanged_and_small_files_are_bounded() {
    use crate::storage_layout::{GIB, MIB, WAL_MAX_BYTES, WAL_MIN_BYTES};
    let options = |size_bytes: u64| StorageOptions {
        size_bytes,
        ..test_storage_options()
    };
    // The server's sizes (whole GiB, default ratios), as they were laid out
    // when sizes were given in GiB: (GiB, wal, data, metadata, allocator,
    // snapshot, reserved).
    for (gib, wal, data, metadata, allocator, snapshot, reserved) in [
        (
            1,
            268_435_456,
            493_920_256,
            85_897_216,
            21_471_232,
            21_471_232,
            182_534_144,
        ),
        (
            10,
            536_870_912,
            7_086_694_400,
            858_992_640,
            214_745_088,
            214_745_088,
            1_825_357_824,
        ),
        (
            100,
            1_073_741_824,
            75_161_915_392,
            8_589_934_592,
            2_147_483_648,
            2_147_483_648,
            18_253_611_008,
        ),
    ] {
        let layout = compute_layout(options(gib * GIB), WAL_MIN_BYTES, WAL_MAX_BYTES).unwrap();
        assert_eq!(
            (
                layout.total_size,
                layout.wal_size,
                layout.data_size,
                layout.metadata_size,
                layout.allocator_size,
                layout.snapshot_size,
                layout.reserved_size,
            ),
            (
                gib * GIB,
                wal,
                data,
                metadata,
                allocator,
                snapshot,
                reserved
            ),
            "{gib} GiB"
        );
    }
    // A small file keeps the minimum ring to a quarter of itself.
    let small = compute_layout(options(64 * MIB), WAL_MIN_BYTES, WAL_MAX_BYTES).unwrap();
    assert_eq!(small.wal_size, 16 * MIB);
    assert!(small.data_size >= crate::allocator::EXTENT_PAGES * PAGE_SIZE as u64);
    // Too small for an extent, or not whole pages: refused.
    let err = compute_layout(options(64 * PAGE_SIZE as u64), 0, 0).unwrap_err();
    assert!(err.to_string().contains("too small"), "{err}");
    assert!(options(64 * MIB + 1).validate().is_err());
    assert!(options(0).validate().is_err());
    assert!(options(64 * MIB).validate().is_ok());
}
