//! Extents allocated and freed inside transactions: an allocation is undone
//! when its transaction rolls back or loses at restart, and a free lands only
//! when its transaction commits — live, after a crash, at a point-in-time cut
//! and on a standby.

use super::*;
use crate::relstore::ctx::TxnLink;

const PAGES: u64 = 8;

/// Runs `body` inside a fresh transaction under the storage lock.
fn in_txn<T>(
    storage: &Storage,
    body: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink) -> T,
) -> (T, TxnLink) {
    let mut guard = storage.lock();
    let txn_id = guard.alloc_txn_id();
    let mut ctx = guard.page_ctx(0);
    let mut txn = ctx.begin(txn_id).expect("begin");
    let out = body(&mut ctx, &mut txn);
    (out, txn)
}

fn commit(storage: &Storage, txn: TxnLink) {
    let mut guard = storage.lock();
    guard.page_ctx(0).commit(txn).expect("commit");
    guard.wal_sync_all().expect("sync");
}

fn sync(storage: &Storage) {
    storage.lock().wal_sync_all().expect("sync");
}

fn allocated(storage: &Storage, start: u64) -> bool {
    let guard = storage.lock();
    (start..start + PAGES).all(|page| guard.is_page_allocated(page))
}

fn free(storage: &Storage, start: u64) -> bool {
    let guard = storage.lock();
    (start..start + PAGES).all(|page| !guard.is_page_allocated(page))
}

/// Allocates an extent in its own committed transaction.
fn committed_extent(storage: &Storage) -> u64 {
    let (start, txn) = in_txn(storage, |ctx, txn| {
        ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate")
    });
    commit(storage, txn);
    start
}

#[test]
fn rollback_frees_an_extent_allocated_in_the_transaction() {
    let path = unique_temp_path("txn-extents-rollback");
    let storage = create_small(&path);
    let (start, txn) = in_txn(&storage, |ctx, txn| {
        ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate")
    });
    assert!(allocated(&storage, start));
    {
        let mut guard = storage.lock();
        let mut ctx = guard.page_ctx(0);
        crate::relstore::recovery::rollback(&mut ctx, txn, &NoLogicalUndo).expect("rollback");
    }
    assert!(free(&storage, start), "rollback freed the extent");
    sync(&storage);
    drop(storage);

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert!(
        free(&storage, start),
        "the allocator rebuild replays the free"
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_crashed_transaction_loses_its_allocation_and_a_committed_one_keeps_it() {
    let path = unique_temp_path("txn-extents-crash");
    let storage = create_small(&path);
    let kept = committed_extent(&storage);
    let (lost, _open_txn) = in_txn(&storage, |ctx, txn| {
        ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate")
    });
    sync(&storage);
    drop(storage); // crash with the second transaction open

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert!(
        allocated(&storage, kept),
        "the committed allocation survives"
    );
    assert!(
        free(&storage, lost),
        "restart undo freed the loser's extent"
    );
    drop(storage);

    let storage = Storage::open(path.clone(), no_engine).expect("reopen again");
    assert!(allocated(&storage, kept));
    assert!(free(&storage, lost));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_interrupted_undo_never_frees_the_extent_twice() {
    let path = unique_temp_path("txn-extents-clr");
    let storage = create_small(&path);
    let ((begin_lsn, start), txn) = in_txn(&storage, |ctx, txn| {
        let begin_lsn = txn.last_lsn;
        (
            begin_lsn,
            ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate"),
        )
    });
    {
        // The undo got as far as its compensation record (which frees the
        // extent and points past the allocation), then crashed.
        let mut guard = storage.lock();
        let mut ctx = guard.page_ctx(0);
        ctx.append(
            &RelRecord::clr_free_extent(txn.txn_id, txn.last_lsn, begin_lsn, start, PAGES),
            false,
        )
        .expect("clr");
        ctx.allocator.free(start, PAGES);
        drop(ctx);
        guard.wal_sync_all().expect("sync");
    }
    drop(storage); // crash before the undo finished

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert!(free(&storage, start), "the compensation freed the extent");
    // Someone else takes the pages; a second free would hand them out again.
    let reused = committed_extent(&storage);
    assert_eq!(reused, start, "the freed pages were reused");
    drop(storage);

    let storage = Storage::open(path.clone(), no_engine).expect("reopen again");
    assert!(
        allocated(&storage, reused),
        "restart did not undo the allocation a second time"
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_free_lands_only_when_its_transaction_commits() {
    let path = unique_temp_path("txn-extents-free");
    let storage = create_small(&path);
    let committed = committed_extent(&storage);
    let rolled_back = committed_extent(&storage);
    let crashed = committed_extent(&storage);

    let ((), txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, committed, PAGES).expect("free")
    });
    assert!(
        allocated(&storage, committed),
        "not freed before the commit"
    );
    commit(&storage, txn);
    assert!(free(&storage, committed), "freed at the commit");

    let ((), txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, rolled_back, PAGES)
            .expect("free")
    });
    {
        let mut guard = storage.lock();
        let mut ctx = guard.page_ctx(0);
        crate::relstore::recovery::rollback(&mut ctx, txn, &NoLogicalUndo).expect("rollback");
    }
    assert!(
        allocated(&storage, rolled_back),
        "a rolled-back free never lands"
    );

    let ((), _open_txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, crashed, PAGES).expect("free")
    });
    sync(&storage);
    drop(storage); // crash with the third free uncommitted

    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    assert!(free(&storage, committed), "the committed free survives");
    assert!(
        allocated(&storage, rolled_back),
        "the rolled-back free stays undone"
    );
    assert!(allocated(&storage, crashed), "the loser's free never lands");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// FULL recovery model, so log backups can carry the stop point.
fn set_full(storage: &Storage) {
    storage
        .lock()
        .commit_db_options(0, Some(true))
        .expect("recovery full");
}

/// Restores `bak` plus `log` to `dst`, stopping at `stop_at`.
fn restore_at(dst: &Path, bak: &Path, log: &Path, stop_at: u64) -> Result<(), StorageError> {
    Storage::restore_full_with_logs(dst, bak, &[log.to_path_buf()], Some(stop_at), no_engine)
}

#[test]
fn a_point_in_time_cut_keeps_frees_committed_after_it() {
    let path = unique_temp_path("txn-extents-pitr");
    let bak = unique_temp_path("txn-extents-pitr-bak");
    let log = unique_temp_path("txn-extents-pitr-log");
    let dst = unique_temp_path("txn-extents-pitr-dst");
    let storage = create_small(&path);
    set_full(&storage);
    let before = committed_extent(&storage);
    let after = committed_extent(&storage);
    storage.backup_full(&bak).expect("full backup");

    let ((), txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, before, PAGES).expect("free")
    });
    commit(&storage, txn);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let stop_at = now_millis();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let ((), txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, after, PAGES).expect("free")
    });
    commit(&storage, txn);
    storage.backup_log(&log, true, false).expect("log backup");
    drop(storage);

    restore_at(&dst, &bak, &log, stop_at).expect("point-in-time restore");
    for open in ["restored", "reopened"] {
        let storage = Storage::open(dst.clone(), no_engine).expect("open");
        assert!(
            free(&storage, before),
            "{open}: a free committed before the cut lands"
        );
        assert!(
            allocated(&storage, after),
            "{open}: a free committed after the cut does not"
        );
        drop(storage);
    }
    for p in [&path, &bak, &log, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn space_freed_and_reused_after_the_cut_stays_with_its_owner_at_the_cut() {
    let path = unique_temp_path("txn-extents-pitr-reuse");
    let bak = unique_temp_path("txn-extents-pitr-reuse-bak");
    let log = unique_temp_path("txn-extents-pitr-reuse-log");
    let dst = unique_temp_path("txn-extents-pitr-reuse-dst");
    let storage = create_small(&path);
    set_full(&storage);
    let owned = committed_extent(&storage);
    storage.backup_full(&bak).expect("full backup");
    drop(storage);
    // Reopen so the next-fit cursor starts over and hands out `owned` again.
    let storage = Storage::open(path.clone(), no_engine).expect("reopen");
    let stop_at = now_millis();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let ((), txn) = in_txn(&storage, |ctx, txn| {
        ctx.free_extent_in_txn(txn, owned, PAGES).expect("free")
    });
    commit(&storage, txn);
    // A later transaction takes the freed pages and the free pages after them.
    let (reused, txn) = in_txn(&storage, |ctx, txn| {
        ctx.allocate_extent_in_txn(txn, 2 * PAGES)
            .expect("allocate")
    });
    commit(&storage, txn);
    assert_eq!(reused, owned, "the freed extent was reused after the cut");
    storage.backup_log(&log, true, false).expect("log backup");
    drop(storage);

    restore_at(&dst, &bak, &log, stop_at).expect("point-in-time restore");
    for open in ["restored", "reopened"] {
        let storage = Storage::open(dst.clone(), no_engine).expect("open");
        assert!(
            allocated(&storage, owned),
            "{open}: the extent still belongs to its first owner"
        );
        assert!(
            free(&storage, owned + PAGES),
            "{open}: the later allocation is gone"
        );
        drop(storage);
    }
    for p in [&path, &bak, &log, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_stop_point_inside_the_full_backup_is_refused() {
    let path = unique_temp_path("txn-extents-pitr-early");
    let bak = unique_temp_path("txn-extents-pitr-early-bak");
    let log = unique_temp_path("txn-extents-pitr-early-log");
    let dst = unique_temp_path("txn-extents-pitr-early-dst");
    let storage = create_small(&path);
    set_full(&storage);
    let stop_at = now_millis();
    std::thread::sleep(std::time::Duration::from_millis(5));
    committed_extent(&storage);
    // The full backup's log (and so its page images) covers that commit.
    storage.backup_full(&bak).expect("full backup");
    storage.backup_log(&log, true, false).expect("log backup");
    drop(storage);

    let err = restore_at(&dst, &bak, &log, stop_at).expect_err("refused");
    assert!(
        err.to_string().contains("before the full backup finished"),
        "unexpected error: {err}"
    );
    assert!(!dst.exists(), "the refused restore leaves no file behind");
    for p in [&path, &bak, &log] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_savepoint_rollback_cannot_take_back_a_later_free() {
    let path = unique_temp_path("txn-extents-savepoint");
    let storage = create_small(&path);
    let extent = committed_extent(&storage);
    let (savepoint, mut txn) = in_txn(&storage, |_, txn| txn.savepoint());
    {
        let mut guard = storage.lock();
        let mut ctx = guard.page_ctx(0);
        ctx.free_extent_in_txn(&mut txn, extent, PAGES)
            .expect("free");
        let err =
            crate::relstore::recovery::rollback_to(&mut ctx, &mut txn, savepoint, &NoLogicalUndo)
                .expect_err("the partial rollback is refused");
        assert!(
            err.to_string().contains("savepoint"),
            "unexpected error: {err}"
        );
        crate::relstore::recovery::rollback(&mut ctx, txn, &NoLogicalUndo).expect("rollback");
    }
    assert!(
        allocated(&storage, extent),
        "the whole rollback drops the free"
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_standby_lands_a_free_when_the_commit_arrives_even_across_a_reopen() {
    let path = unique_temp_path("txn-extents-standby-src");
    let bak = unique_temp_path("txn-extents-standby-bak");
    let seed = unique_temp_path("txn-extents-standby-seed");
    let primary = create_small(&path);
    let freed = committed_extent(&primary);
    let summary = primary.backup_full(&bak).expect("backup");

    let (lost, loser) = in_txn(&primary, |ctx, txn| {
        ctx.allocate_extent_in_txn(txn, PAGES).expect("allocate")
    });
    let ((), txn) = in_txn(&primary, |ctx, txn| {
        ctx.free_extent_in_txn(txn, freed, PAGES).expect("free")
    });
    sync(&primary);
    let mid = primary.wal_flushed_lsn();
    commit(&primary, txn);
    {
        let mut guard = primary.lock();
        let mut ctx = guard.page_ctx(0);
        crate::relstore::recovery::rollback(&mut ctx, loser, &NoLogicalUndo).expect("rollback");
        drop(ctx);
        guard.wal_sync_all().expect("sync");
    }
    let end = primary.wal_flushed_lsn();
    let first = primary
        .read_wal_range(summary.backup_end_lsn, mid)
        .expect("ship first");
    let second = primary.read_wal_range(mid, end).expect("ship second");

    Storage::restore_full_standby(&seed, &bak, &[], no_engine).expect("standby seed");
    let standby = Storage::open(seed.clone(), no_engine).expect("open standby");
    standby
        .apply_wal_stream(summary.backup_end_lsn, &first)
        .expect("apply first");
    assert!(allocated(&standby, freed), "no commit yet: the free waits");
    assert!(
        allocated(&standby, lost),
        "the in-flight allocation is held"
    );
    drop(standby);

    let standby = Storage::open(seed.clone(), no_engine).expect("reopen standby");
    assert!(allocated(&standby, freed), "still waiting after the reopen");
    standby
        .apply_wal_stream(mid, &second)
        .expect("apply second");
    assert!(free(&standby, freed), "the commit landed the free");
    assert!(
        free(&standby, lost),
        "the shipped rollback freed the allocation"
    );
    drop(standby);
    drop(primary);
    for p in [&path, &bak, &seed] {
        std::fs::remove_file(p).ok();
    }
}
