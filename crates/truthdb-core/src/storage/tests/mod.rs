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
        size_gib: 1,
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
