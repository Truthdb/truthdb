use super::*;

#[test]
fn backup_is_single_flight_and_releases_its_hold() {
    let path = unique_temp_path("backup-hold");
    let bak = unique_temp_path("backup-hold-bak");
    let bak2 = unique_temp_path("backup-hold-bak2");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");

    // A second backup while a hold is set is rejected, and — critically —
    // must NOT clear the in-flight backup's hold (the guard arms only after
    // begin_backup succeeds, so a rejected backup touches nothing).
    storage.lock().register_backup_hold(42);
    assert!(matches!(
        storage.backup_full(&bak),
        Err(StorageError::BackupInProgress)
    ));
    assert_eq!(
        storage.lock().truncation_gate.backup,
        Some(42),
        "a rejected backup leaves the existing hold intact"
    );
    storage.lock().release_backup_hold();

    // A successful backup releases its own hold, so a second one succeeds.
    storage.backup_full(&bak).expect("first backup");
    assert_eq!(
        storage.lock().truncation_gate.backup,
        None,
        "the hold is released after a successful backup"
    );
    storage
        .backup_full(&bak2)
        .expect("second backup after release");

    drop(storage);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(bak);
    let _ = std::fs::remove_file(bak2);
}

#[test]
fn log_backup_orphans_when_recovery_flips_to_simple_mid_flight() {
    // Reproduces the race the lock-dance opened: BACKUP LOG releases the
    // storage lock to write its archive; a concurrent ALTER ... SET RECOVERY
    // SIMPLE releases the log hold and a checkpoint advances the head; phase
    // 3 must then ORPHAN the backup rather than re-arm the hold below head.
    let path = unique_temp_path("backuplog-orphan");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    storage
        .rel_set_db_options(None, None, Some(true))
        .expect("enable FULL");
    for seq in 0..8 {
        storage
            .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, seq, b"logbytes")
            .expect("append");
    }
    // Phase 1: capture the range under the lock (marker = start).
    let (_, start, end, _log) = storage.lock().begin_log_backup(true, false).expect("begin");
    assert!(end > start);
    // The concurrent ALTER SET RECOVERY SIMPLE during the unlocked window.
    storage
        .rel_set_db_options(None, None, Some(false))
        .expect("disable FULL");
    assert_eq!(storage.log_backup_hold(), None, "SIMPLE released the hold");
    // A checkpoint now advances the head to the tail (no hold pins it).
    storage
        .write_checkpoint(b"cp", 1, 2, 1)
        .expect("checkpoint after SIMPLE");
    assert!(
        storage.wal_head() > start,
        "the head advanced past the old marker"
    );
    // Phase 3: finish must orphan (recovery_full is false) — no re-arm.
    storage
        .lock()
        .finish_log_backup(start, end)
        .expect("finish orphans cleanly");
    assert_eq!(
        storage.log_backup_hold(),
        None,
        "finish did not re-arm the hold on the now-SIMPLE database"
    );
    // The next checkpoint must not move the head backward (would panic
    // without the orphan guard).
    storage
        .write_checkpoint(b"cp2", 2, 3, 2)
        .expect("checkpoint after orphan does not panic");
    storage.lock().cancel_log_backup();
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_page_freed_since_the_backup_began_is_not_treated_as_corrupt() {
    let path = unique_temp_path("backup-freed");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    {
        let mut file = storage.lock();
        let page = file.allocator.allocate(1).expect("allocate a page");
        assert!(
            file.page_is_live_regular(page).unwrap(),
            "a live, allocated data page is regular (a checksum failure there is real corruption)"
        );
        file.allocator.free(page, 1);
        assert!(
            !file.page_is_live_regular(page).unwrap(),
            "a page freed since the backup began is tolerated, not flagged corrupt"
        );
    }
    drop(storage);
    let _ = std::fs::remove_file(path);
}
