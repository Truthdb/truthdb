//! B+ tree pages a delete empties are freed: a tree drained from one end
//! gives its pages back, live, after a crash and through a rollback, and a
//! freed page reused for a blob is never overwritten by its stale cached frame.

use super::*;
use crate::relstore::btree::{BTree, TreeInsert, TreeRoots};
use crate::relstore::ctx::{OpMode, TxnLink};
use crate::relstore::page;

const OBJECT: u32 = 0xE000_0001;
/// A ring large enough for the thousands of row records these tests write.
const RING: u64 = 16 * 1024 * 1024;

fn create(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        test_storage_options(),
        RING,
        RING,
        no_engine,
    )
    .expect("create storage")
}

fn key(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

fn row(n: u64) -> Vec<u8> {
    let mut row = vec![(n % 251) as u8; 120];
    row[..8].copy_from_slice(&n.to_le_bytes());
    row
}

fn allocated_pages(storage: &Storage) -> u64 {
    let guard = storage.lock();
    (0..guard.data_pages())
        .filter(|&page| guard.is_page_allocated(page))
        .count() as u64
}

fn create_tree(storage: &Storage) -> BTree {
    let mut guard = storage.lock();
    let tree = BTree::create(&mut guard.page_ctx(0), OBJECT).expect("create tree");
    guard.wal_sync_all().expect("sync");
    tree
}

/// Runs `body` in one committed transaction.
fn committed<T>(storage: &Storage, body: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> T) -> T {
    let mut guard = storage.lock();
    let txn_id = guard.alloc_txn_id();
    let out = {
        let mut ctx = guard.page_ctx(0);
        let mut txn = ctx.begin(txn_id).expect("begin");
        let out = body(&mut ctx, &mut txn);
        ctx.commit(txn).expect("commit");
        out
    };
    guard.wal_sync_all().expect("sync");
    out
}

fn insert(storage: &Storage, tree: &BTree, keys: impl Iterator<Item = u64>) {
    committed(storage, |ctx, txn| {
        for n in keys {
            let inserted = tree
                .insert_unique(ctx, &mut OpMode::Txn(txn), &key(n), &row(n))
                .expect("insert");
            assert_eq!(inserted, TreeInsert::Inserted);
        }
    });
}

fn delete(storage: &Storage, tree: &BTree, keys: impl Iterator<Item = u64>) {
    committed(storage, |ctx, txn| {
        for n in keys {
            let removed = tree
                .delete(ctx, &mut OpMode::Txn(txn), &key(n))
                .expect("delete");
            assert_eq!(removed, Some(row(n)), "key {n}");
        }
    });
}

fn contents(storage: &Storage, tree: &BTree) -> Vec<u64> {
    let mut guard = storage.lock();
    let rows = tree.scan(&mut guard.page_ctx(0)).expect("scan");
    rows.into_iter()
        .map(|(k, r)| {
            let n = u64::from_be_bytes(k.as_slice().try_into().unwrap());
            assert_eq!(r, row(n), "row of key {n}");
            n
        })
        .collect()
}

fn checkpoint(storage: &Storage) {
    storage.write_checkpoint(b"x", 1, 2, 1).expect("checkpoint");
}

#[test]
fn a_tree_drained_from_one_end_gives_every_page_back() {
    let path = unique_temp_path("btree-free-queue");
    let storage = create(&path);
    let tree = create_tree(&storage);
    // Measured after a checkpoint, whose snapshot extent then stays put.
    checkpoint(&storage);
    let baseline = allocated_pages(&storage);
    // A queue: keys appended at the right, consumed from the left, several
    // times over — the pages in use stay bounded instead of growing with
    // every key ever written.
    let mut next = 0u64;
    let mut peak = 0;
    for round in 0..4 {
        for _ in 0..10 {
            insert(&storage, &tree, next..next + 300);
            next += 300;
        }
        peak = peak.max(allocated_pages(&storage) - baseline);
        let first = round * 3000;
        for batch in 0..10 {
            let from = first + batch * 300;
            delete(&storage, &tree, from..from + 300);
            assert_eq!(
                contents(&storage, &tree),
                (from + 300..next).collect::<Vec<_>>()
            );
        }
        checkpoint(&storage);
        assert_eq!(
            allocated_pages(&storage),
            baseline,
            "round {round}: only the root is left"
        );
    }
    assert!(peak > 20, "the tree grew several levels: {peak} pages");
    // An emptied tree takes rows again.
    insert(&storage, &tree, 0..500);
    assert_eq!(contents(&storage, &tree), (0..500).collect::<Vec<_>>());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn random_deletes_keep_the_tree_whole_and_free_what_they_empty() {
    let path = unique_temp_path("btree-free-random");
    let storage = create(&path);
    let tree = create_tree(&storage);
    // Measured after a checkpoint, whose snapshot extent then stays put.
    checkpoint(&storage);
    let baseline = allocated_pages(&storage);
    let mut model = std::collections::BTreeSet::new();
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut next_random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for _ in 0..6 {
        let fresh: Vec<u64> = (0..800)
            .map(|_| next_random() % 20_000)
            .filter(|n| model.insert(*n))
            .collect();
        insert(&storage, &tree, fresh.into_iter());
        let doomed: Vec<u64> = model
            .iter()
            .copied()
            .filter(|_| next_random() % 3 != 0)
            .collect();
        for n in &doomed {
            model.remove(n);
        }
        delete(&storage, &tree, doomed.into_iter());
        assert_eq!(
            contents(&storage, &tree),
            model.iter().copied().collect::<Vec<_>>()
        );
        checkpoint(&storage);
    }
    let rest: Vec<u64> = model.iter().copied().collect();
    delete(&storage, &tree, rest.into_iter());
    assert!(contents(&storage, &tree).is_empty());
    checkpoint(&storage);
    assert_eq!(allocated_pages(&storage), baseline, "only the root is left");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn freed_pages_stay_freed_after_a_crash_and_the_tree_reads_back() {
    let path = unique_temp_path("btree-free-crash");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    checkpoint(&storage);
    // Drained past the checkpoint: the frees and unlinkings are only in the
    // log when the process dies.
    delete(&storage, &tree, 0..2500);
    let allocated = allocated_pages(&storage);
    let root = tree.root;
    drop(storage);

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    let tree = BTree {
        object_id: OBJECT,
        root,
    };
    assert_eq!(contents(&storage, &tree), (2500..3000).collect::<Vec<_>>());
    assert_eq!(
        allocated_pages(&storage),
        allocated,
        "the allocator replayed the frees"
    );
    insert(&storage, &tree, 0..100);
    delete(&storage, &tree, 2500..3000);
    assert_eq!(contents(&storage, &tree), (0..100).collect::<Vec<_>>());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_rollback_restores_rows_whose_pages_were_freed() {
    let path = unique_temp_path("btree-free-rollback");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    let before = allocated_pages(&storage);
    {
        let mut guard = storage.lock();
        let txn_id = guard.alloc_txn_id();
        let mut ctx = guard.page_ctx(0);
        let mut txn = ctx.begin(txn_id).expect("begin");
        for n in 0..2000 {
            tree.delete(&mut ctx, &mut OpMode::Txn(&mut txn), &key(n))
                .expect("delete");
        }
        let roots = TreeRoots([(OBJECT, tree.root)].into_iter().collect());
        crate::relstore::recovery::rollback(&mut ctx, txn, &roots).expect("rollback");
    }
    assert!(
        allocated_pages(&storage) <= before + 64,
        "the rows went back in"
    );
    assert_eq!(contents(&storage, &tree), (0..3000).collect::<Vec<_>>());
    storage.lock().wal_sync_all().expect("sync");
    drop(storage);
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    let tree = BTree {
        object_id: OBJECT,
        root: tree.root,
    };
    assert_eq!(contents(&storage, &tree), (0..3000).collect::<Vec<_>>());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_freed_tree_page_reused_for_a_blob_keeps_the_blob() {
    let path = unique_temp_path("btree-free-blob");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    checkpoint(&storage);
    let in_use: Vec<u64> = {
        let guard = storage.lock();
        (0..guard.data_pages())
            .filter(|&page| guard.is_page_allocated(page))
            .collect()
    };
    // The drained leaves are dirty in the pool when they are freed.
    delete(&storage, &tree, 0..2900);
    let freed: Vec<u64> = {
        let guard = storage.lock();
        in_use
            .iter()
            .copied()
            .filter(|&page| !guard.is_page_allocated(page))
            .collect()
    };
    assert!(freed.len() > 10, "{} pages freed", freed.len());

    // Reuse a freed tree page for a blob (the next-fit allocator would only
    // come back to it after wrapping, so it is taken directly).
    let data = vec![0xAB; 100];
    let target = freed[freed.len() / 2];
    committed(&storage, |ctx, txn| {
        ctx.allocator.mark_used(target, 1);
        ctx.write_blob_pages(txn, OBJECT + 1, target, &data)
            .expect("blob");
    });
    // Flushing the pool must not put the old leaf back over the blob.
    checkpoint(&storage);
    let read_back = |storage: &Storage| {
        let mut out = vec![0u8; page::BLOB_PAGE_DATA];
        let header = storage
            .lock()
            .page_ctx(0)
            .read_blob_page(target, &mut out)
            .expect("a blob page");
        assert_eq!(header.object_id, OBJECT + 1);
        assert_eq!(&out[..data.len()], &data[..]);
    };
    read_back(&storage);
    drop(storage);
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    read_back(&storage);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_standby_replays_the_frees_and_matches_the_primary() {
    let path = unique_temp_path("btree-free-ship-src");
    let bak = unique_temp_path("btree-free-ship-bak");
    let seed = unique_temp_path("btree-free-ship-seed");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let tree = create_tree(&primary);
    insert(&primary, &tree, 0..3000);
    let summary = primary.backup_full(&bak).unwrap();
    delete(&primary, &tree, 0..2800);
    insert(&primary, &tree, 5000..5200);
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();

    Storage::restore_full_standby(&seed, &bak, &[], no_engine).unwrap();
    let standby = Storage::open(seed.clone(), no_engine).unwrap();
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    let expected: Vec<u64> = (2800..3000).chain(5000..5200).collect();
    assert_eq!(contents(&primary, &tree), expected);
    assert_eq!(contents(&standby, &tree), expected);
    assert_eq!(allocated_pages(&standby), allocated_pages(&primary));
    drop(standby);
    drop(primary);
    for p in [&path, &bak, &seed] {
        std::fs::remove_file(p).ok();
    }
}

/// Marks every free page outside `keep` used, so the next allocation lands on
/// one of `keep` (the next-fit allocator would otherwise come back to them
/// only after wrapping).
fn steer_allocations_to(storage: &Storage, keep: &[u64]) {
    let mut guard = storage.lock();
    let pages = guard.data_pages();
    let free: Vec<u64> = (0..pages)
        .filter(|&page| !guard.is_page_allocated(page) && !keep.contains(&page))
        .collect();
    let allocator = guard.allocator_mut();
    for page in free {
        allocator.mark_used(page, 1);
    }
}

fn snapshot_reads_back(storage: &Storage, data: &[u8]) {
    let snapshot = storage
        .load_snapshot()
        .expect("the snapshot reads back")
        .expect("a snapshot");
    assert_eq!(snapshot.data, data);
}

#[test]
fn a_checkpoint_snapshot_on_freed_tree_pages_is_never_overwritten() {
    let path = unique_temp_path("btree-free-snapshot");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    checkpoint(&storage);
    let in_use: Vec<u64> = {
        let guard = storage.lock();
        (0..guard.data_pages())
            .filter(|&page| guard.is_page_allocated(page))
            .collect()
    };
    delete(&storage, &tree, 0..2900);
    let freed: Vec<u64> = {
        let guard = storage.lock();
        in_use
            .iter()
            .copied()
            .filter(|&page| !guard.is_page_allocated(page))
            .collect()
    };
    assert!(freed.len() > 10);
    let data = vec![0x5A; 3 * 4096];
    // Live: the snapshot lands on freed leaves, then the pool is flushed.
    steer_allocations_to(&storage, &freed);
    storage
        .write_checkpoint(&data, 1, 2, 1)
        .expect("checkpoint");
    snapshot_reads_back(&storage, &data);
    drop(storage);
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    snapshot_reads_back(&storage, &data);
    // After a restart (redo has run): again on freed pages.
    steer_allocations_to(&storage, &freed);
    let data = vec![0xA5; 2 * 4096];
    storage
        .write_checkpoint(&data, 2, 3, 1)
        .expect("checkpoint");
    snapshot_reads_back(&storage, &data);
    drop(storage);
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    snapshot_reads_back(&storage, &data);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

fn raw_pages(path: &Path, storage: &Storage, pages: &[u64]) -> Vec<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let data_offset = storage.lock().layout.data_offset;
    let file = std::fs::File::open(path).expect("open raw");
    pages
        .iter()
        .map(|&page| {
            let mut buf = vec![0u8; PAGE_SIZE];
            file.read_exact_at(&mut buf, data_offset + page * PAGE_SIZE as u64)
                .expect("read raw page");
            buf
        })
        .collect()
}

#[test]
fn a_checkpoint_writes_no_snapshot_over_pages_whose_free_is_not_yet_durable() {
    let path = unique_temp_path("btree-free-snapshot-unsynced");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    checkpoint(&storage);
    let in_use: Vec<u64> = {
        let guard = storage.lock();
        (0..guard.data_pages())
            .filter(|&page| guard.is_page_allocated(page))
            .collect()
    };
    // Committed but not yet forced: a power loss would lose these frees,
    // and the tree would still link the pages.
    {
        let mut guard = storage.lock();
        let txn_id = guard.alloc_txn_id();
        let mut ctx = guard.page_ctx(0);
        let mut txn = ctx.begin(txn_id).expect("begin");
        for n in 0..2900 {
            tree.delete(&mut ctx, &mut OpMode::Txn(&mut txn), &key(n))
                .expect("delete");
        }
        ctx.commit(txn).expect("commit");
    }
    let freed: Vec<u64> = {
        let guard = storage.lock();
        in_use
            .iter()
            .copied()
            .filter(|&page| !guard.is_page_allocated(page))
            .collect()
    };
    assert!(freed.len() > 10);
    let before = raw_pages(&path, &storage, &freed);
    steer_allocations_to(&storage, &freed);
    storage.lock().wal.fail_next_sync();
    storage
        .write_checkpoint(&vec![0x5A; 3 * 4096], 1, 2, 1)
        .expect_err("the log could not be forced");
    assert!(
        raw_pages(&path, &storage, &freed) == before,
        "the snapshot was written over pages the durable log still links"
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn redo_skips_pages_freed_later_in_the_log() {
    let path = unique_temp_path("btree-free-redo");
    let storage = create(&path);
    storage.lock().commit_db_options(0, Some(true)).unwrap();
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    let backup = unique_temp_path("btree-free-redo-bak");
    storage.backup_full(&backup).unwrap();
    checkpoint(&storage);
    // Touch every leaf, then hold the log from here: restart redo will see
    // these page records again.
    delete(&storage, &tree, (0..3000).step_by(7));
    let log = unique_temp_path("btree-free-redo-log");
    storage.backup_log(&log, true, false).unwrap();
    let in_use: Vec<u64> = {
        let guard = storage.lock();
        (0..guard.data_pages())
            .filter(|&page| guard.is_page_allocated(page))
            .collect()
    };
    let rest: Vec<u64> = (0..2900).filter(|n| n % 7 != 0).collect();
    delete(&storage, &tree, rest.into_iter());
    let freed: Vec<u64> = {
        let guard = storage.lock();
        in_use
            .iter()
            .copied()
            .filter(|&page| !guard.is_page_allocated(page))
            .collect()
    };
    // The freed leaves now hold a checkpoint snapshot, which is not a page.
    steer_allocations_to(&storage, &freed);
    let data = vec![0x3C; 3 * 4096];
    storage
        .write_checkpoint(&data, 1, 2, 1)
        .expect("checkpoint");
    snapshot_reads_back(&storage, &data);
    drop(storage);

    let storage = Storage::open(path.clone(), no_engine).expect("restart redo succeeds");
    snapshot_reads_back(&storage, &data);
    let expected: Vec<u64> = (2900..3000).filter(|n| n % 7 != 0).collect();
    assert_eq!(contents(&storage, &tree), expected);
    drop(storage);
    for p in [&path, &backup, &log] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_scan_resumed_on_a_page_that_is_no_longer_a_page_stops() {
    let path = unique_temp_path("btree-free-resume");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..3000);
    checkpoint(&storage);
    // A sliced scan stops early in the tree…
    let mut first = Vec::new();
    let cursor = {
        let mut guard = storage.lock();
        tree.scan_from(
            &mut guard.page_ctx(0),
            crate::relstore::btree::ScanCursor::start(),
            10,
            &mut first,
        )
        .expect("first slice")
    };
    let resume_page = cursor.page().expect("a page");
    // …its page is emptied, freed and taken by a checkpoint snapshot…
    delete(&storage, &tree, 0..2900);
    assert!(!storage.lock().is_page_allocated(resume_page));
    steer_allocations_to(&storage, &[resume_page]);
    storage
        .write_checkpoint(&[0x77; 4096], 1, 2, 1)
        .expect("checkpoint");
    // …and the scan resumes: it stops rather than failing on the snapshot.
    let mut rest = Vec::new();
    let next = {
        let mut guard = storage.lock();
        tree.scan_from(&mut guard.page_ctx(0), cursor, 100, &mut rest)
            .expect("the resumed scan does not fail")
    };
    assert!(next.done());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn after_the_log_writers_fsync_failed_no_later_fsync_of_the_log_succeeds() {
    let path = unique_temp_path("log-writer-fsync-failed");
    let storage = create(&path);
    let tree = create_tree(&storage);
    insert(&storage, &tree, 0..10);
    // A commit left to the group-commit log-writer, whose fsync fails (a
    // failed device cache flush is reported to that call only).
    let tail = {
        let mut guard = storage.lock();
        let txn_id = guard.alloc_txn_id();
        let mut ctx = guard.page_ctx(0);
        let mut txn = ctx.begin(txn_id).expect("begin");
        tree.insert_unique(&mut ctx, &mut OpMode::Txn(&mut txn), &key(10), &row(10))
            .expect("insert");
        ctx.commit(txn).expect("commit");
        drop(ctx);
        guard.wal.fail_next_sync();
        guard.wal_tail()
    };
    storage
        .ensure_durable(tail)
        .expect_err("the log-writer's fsync failed");
    // Before the committer wedges the store, a checkpoint's own fsync of the
    // log must not report the lost commit durable.
    storage
        .write_checkpoint(b"x", 1, 2, 1)
        .expect_err("the log is not known durable");
    drop(storage);
    std::fs::remove_file(&path).ok();
}
