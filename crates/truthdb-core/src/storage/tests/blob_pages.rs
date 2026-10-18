//! Blob body pages: written straight to the data file after their record is
//! durable, and redone on disk only when the page there is torn or older than
//! the record — across a crash, a freed-and-reused extent, a standby and a
//! restore.

use super::txn_extents::{PAGES, commit, in_txn, set_full};
use super::*;
use crate::relstore::page::BLOB_PAGE_DATA;

const OBJECT: u32 = FIRST_ENGINE_OBJECT_ID + 1;

/// A body spanning three pages, the last one partly filled.
fn body(seed: u8) -> Vec<u8> {
    (0..3 * BLOB_PAGE_DATA - 100)
        .map(|i| seed.wrapping_add(i as u8))
        .collect()
}

/// Allocates an extent and writes `data` into it, committed.
fn committed_blob(storage: &Storage, data: &[u8]) -> u64 {
    let (start, txn) = in_txn(storage, |ctx, txn| {
        let start = ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate");
        ctx.write_blob_pages(txn, OBJECT, start, data)
            .expect("write");
        start
    });
    commit(storage, txn);
    start
}

/// Reads `len` body bytes back from `start`.
fn read_blob(storage: &Storage, start: u64, len: usize) -> Vec<u8> {
    let mut guard = storage.lock();
    let mut ctx = guard.page_ctx(0);
    let mut out = Vec::new();
    let mut page = vec![0u8; BLOB_PAGE_DATA];
    for index in 0..len.div_ceil(BLOB_PAGE_DATA) {
        let header = ctx
            .read_blob_page(start + index as u64, &mut page)
            .expect("read blob page");
        assert_eq!(header.object_id, OBJECT);
        out.extend_from_slice(&page);
    }
    out.truncate(len);
    out
}

/// Overwrites a data page on disk with `fill` bytes (a torn or lost write).
fn damage_page(path: &Path, offset: u64, fill: u8) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open data file");
    file.write_all_at(&vec![fill; PAGE_SIZE], offset)
        .expect("damage page");
}

#[test]
fn a_blob_reads_back_and_survives_a_crash() {
    let path = unique_temp_path("blob-crash");
    let storage = create_small(&path);
    let data = body(1);
    let start = committed_blob(&storage, &data);
    assert_eq!(read_blob(&storage, start, data.len()), data);
    drop(storage); // crash, no checkpoint

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert_eq!(read_blob(&storage, start, data.len()), data);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn redo_repairs_a_torn_page_and_a_lost_one() {
    let path = unique_temp_path("blob-torn");
    let storage = create_small(&path);
    let data = body(2);
    let start = committed_blob(&storage, &data);
    let (torn, lost) = (
        storage.data_page_offset(start),
        storage.data_page_offset(start + 2),
    );
    drop(storage);
    damage_page(&path, torn, 0xAB);
    damage_page(&path, lost, 0);

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert_eq!(read_blob(&storage, start, data.len()), data);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_reused_extent_keeps_the_newer_blob_through_redo() {
    let path = unique_temp_path("blob-reuse");
    let storage = create_small(&path);
    let old = body(3);
    let start = committed_blob(&storage, &old);
    let ((), txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, start, PAGES).expect("free")
    });
    commit(&storage, txn);
    drop(storage);

    // Reopen so the next-fit cursor starts over and hands the extent out again.
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    let new = body(4);
    assert_eq!(
        committed_blob(&storage, &new),
        start,
        "the extent was reused"
    );
    let offsets: Vec<u64> = (0..3)
        .map(|i| storage.data_page_offset(start + i))
        .collect();
    drop(storage); // crash: both blobs' records are still in the log

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert_eq!(
        read_blob(&storage, start, new.len()),
        new,
        "redo of the older record left the newer pages alone"
    );
    drop(storage);

    // Lose every page: redo rewrites the old blob, then the new one over it.
    for offset in offsets {
        damage_page(&path, offset, 0);
    }
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert_eq!(read_blob(&storage, start, new.len()), new);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_page_that_is_not_a_blob_page_is_refused() {
    let path = unique_temp_path("blob-refuse");
    let storage = create_small(&path);
    let (start, txn) = in_txn(&storage, |ctx, txn| {
        ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate")
    });
    commit(&storage, txn);
    let mut guard = storage.lock();
    let mut out = vec![0u8; BLOB_PAGE_DATA];
    let err = guard
        .page_ctx(0)
        .read_blob_page(start, &mut out)
        .expect_err("a never-written page is not a blob page");
    assert!(err.to_string().contains("not a valid blob page"), "{err}");
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_blob_ships_to_a_standby() {
    let path = unique_temp_path("blob-standby-src");
    let bak = unique_temp_path("blob-standby-bak");
    let seed = unique_temp_path("blob-standby-seed");
    let primary = create_small(&path);
    let summary = primary.backup_full(&bak).expect("backup");
    let data = body(5);
    let start = committed_blob(&primary, &data);
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .expect("ship");

    Storage::restore_full_standby(&seed, &bak, &[], no_engine).expect("standby seed");
    let standby = Storage::open(seed.clone(), no_engine).expect("open standby");
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .expect("apply");
    assert_eq!(read_blob(&standby, start, data.len()), data);
    drop(standby);
    drop(primary);
    for p in [&path, &bak, &seed] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_blob_written_after_the_full_backup_restores_from_the_log() {
    let path = unique_temp_path("blob-restore-src");
    let bak = unique_temp_path("blob-restore-bak");
    let log = unique_temp_path("blob-restore-log");
    let dst = unique_temp_path("blob-restore-dst");
    let storage = create_small(&path);
    set_full(&storage);
    storage.backup_full(&bak).expect("full backup");
    let data = body(6);
    let start = committed_blob(&storage, &data);
    storage.backup_log(&log, true, false).expect("log backup");
    drop(storage);

    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, no_engine).expect("restore");
    let restored = Storage::open(dst.clone(), no_engine).expect("open restored");
    assert_eq!(read_blob(&restored, start, data.len()), data);
    drop(restored);
    for p in [&path, &bak, &log, &dst] {
        std::fs::remove_file(p).ok();
    }
}
