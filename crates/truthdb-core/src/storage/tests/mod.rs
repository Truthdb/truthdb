use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::*;
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

fn rows_i32(rows: &[Vec<Datum>]) -> Vec<i32> {
    rows.iter()
        .map(|row| match row[0] {
            Datum::Int(v) => v,
            ref other => panic!("expected INT, got {other:?}"),
        })
        .collect()
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
    )
    .expect("create storage")
}

mod backup_restore;
mod durability;
mod lifecycle;
mod query_paths;
mod recovery;
mod relational;
mod versioning;
