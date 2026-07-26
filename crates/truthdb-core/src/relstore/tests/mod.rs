//! Stage 2 exit-criteria tests: kill-and-recover matrix, CLR idempotence,
//! torn-page FPI repair, B+ tree vs BTreeMap oracle, split-crash, heap
//! forwarding stubs and statement rollback.

use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::relstore::row::Column;
use crate::relstore::types::{ColumnType, Datum};
use crate::storage::{Storage, StorageError, StorageOptions};

/// Room for FPIs and split images without checkpoints in most tests.
const REL_TEST_WAL_BYTES: u64 = 8 * 1024 * 1024;

fn storage_options() -> StorageOptions {
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
    path.push(format!("truthdb-rel-{label}-{nanos}.db"));
    path
}

fn create_storage(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        storage_options(),
        REL_TEST_WAL_BYTES,
        REL_TEST_WAL_BYTES,
    )
    .expect("create storage")
}

fn overwrite_bytes(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for corruption");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(bytes).expect("write");
    file.sync_all().expect("sync");
}

fn int_column(name: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        column_type: ColumnType::Int,
        nullable,
        collation: None,
    }
}

fn varchar_column(name: &str, max_len: u16) -> Column {
    Column {
        name: name.to_string(),
        column_type: ColumnType::VarChar { max_len },
        nullable: true,
        collation: None,
    }
}

fn create_tree_table(storage: &mut Storage, name: &str) {
    storage
        .rel_create_table(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            name,
            vec![int_column("id", false), varchar_column("payload", 4000)],
            &["id".to_string()],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create tree table");
}

fn create_heap_table(storage: &mut Storage, name: &str) {
    storage
        .rel_create_table(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            name,
            vec![int_column("id", false), varchar_column("payload", 4000)],
            &[],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create heap table");
}

fn row(id: i32, payload: &str) -> Vec<Datum> {
    vec![Datum::Int(id), Datum::VarChar(payload.to_string())]
}

fn scan_ids(storage: &mut Storage, table: &str) -> Vec<i32> {
    storage
        .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, table)
        .expect("scan")
        .into_iter()
        .map(|r| match r[0] {
            Datum::Int(id) => id,
            _ => panic!("expected int id"),
        })
        .collect()
}

mod btree;
mod collation;
mod databases;
mod heap;
mod recovery;
mod scan;
