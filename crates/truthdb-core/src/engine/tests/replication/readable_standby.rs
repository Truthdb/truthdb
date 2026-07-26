use super::*;

#[test]
fn a_readable_standby_serves_only_committed_state() {
    let primary_path = unique_temp_path("read-primary");
    let bak = unique_temp_path("read-bak");
    let standby_path = unique_temp_path("read-standby");

    let primary = new_engine(&primary_path);
    let mut ctx = TxnContext::default();
    batch(
        &primary,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
    );
    for i in 1..=10 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    let summary = primary.storage().backup_full(&bak).expect("backup");
    let backup_end = summary.backup_end_lsn;
    // Committed post-backup work...
    for i in 11..=15 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    // ...then an in-flight transaction: new rows AND an update of row 1.
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    for i in 16..=20 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    batch(&primary, &mut ctx, "UPDATE t SET v = 999 WHERE id = 1");
    let mid = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, mid)
        .expect("delta");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("seed");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply");
    assert_eq!(
        sql_rows(&standby, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("15".into())]],
        "in-flight inserts are invisible; committed ones are visible"
    );
    assert_eq!(
        sql_rows(&standby, "SELECT v FROM t WHERE id = 1").1,
        vec![vec![Some("1".into())]],
        "an in-flight update serves the committed pre-image"
    );

    // The store rebuilds at reopen from the retained ring.
    drop(standby);
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    assert_eq!(
        sql_rows(&standby, "SELECT v FROM t WHERE id = 1").1,
        vec![vec![Some("1".into())]],
        "committed-state reads survive a reopen"
    );
    assert_eq!(
        sql_rows(&standby, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("15".into())]]
    );

    // The commit ships: everything becomes visible.
    batch(&primary, &mut ctx, "COMMIT TRANSACTION");
    let committed = primary.storage().wal_flushed_lsn();
    let delta2 = primary
        .storage()
        .read_wal_range(mid, committed)
        .expect("commit range");
    standby
        .storage()
        .apply_wal_stream(mid, &delta2)
        .expect("apply commit");
    assert_eq!(
        sql_rows(&standby, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("20".into())]],
        "the committed transaction is visible"
    );
    assert_eq!(
        sql_rows(&standby, "SELECT v FROM t WHERE id = 1").1,
        vec![vec![Some("999".into())]],
        "the committed update is visible"
    );

    // An aborted shipped transaction: its effects never surface, and its
    // version chains unwind.
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    batch(&primary, &mut ctx, "UPDATE t SET v = -1 WHERE id = 2");
    batch(&primary, &mut ctx, "ROLLBACK TRANSACTION");
    let aborted = primary.storage().wal_flushed_lsn();
    let delta3 = primary
        .storage()
        .read_wal_range(committed, aborted)
        .expect("abort range");
    standby
        .storage()
        .apply_wal_stream(committed, &delta3)
        .expect("apply abort");
    assert_eq!(
        sql_rows(&standby, "SELECT v FROM t WHERE id = 2").1,
        vec![vec![Some("2".into())]],
        "an aborted shipped transaction never surfaces"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// The two review-caught wrong-results bugs, pinned: heap undo payloads
// are CELL bytes (a tag byte before the row — served raw they decode as
// garbage), and a statement rollback inside a transaction that LATER
// COMMITS ships CLRs with no TXN_END — un-captured, the rolled-back
// delete's chain head would hide a committed, physically present row.
#[test]
fn readable_standby_handles_heap_cells_and_statement_rollbacks() {
    let primary_path = unique_temp_path("edge-primary");
    let bak = unique_temp_path("edge-bak");
    let standby_path = unique_temp_path("edge-standby");

    let primary = new_engine(&primary_path);
    let mut ctx = TxnContext::default();
    // A HEAP table (no primary key) — its undo payloads are tagged cells.
    batch(&primary, &mut ctx, "CREATE TABLE h (id INT, v INT)");
    for i in 1..=3 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO h VALUES ({i}, {i})"),
        );
    }
    // A keyed table for the savepoint flow.
    batch(
        &primary,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
    );
    for i in 1..=3 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    let summary = primary.storage().backup_full(&bak).expect("backup");
    let backup_end = summary.backup_end_lsn;

    // In-flight heap update: the standby must serve the OLD row, decoded
    // correctly (the tag byte stripped).
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    batch(&primary, &mut ctx, "UPDATE h SET v = 999 WHERE id = 1");
    let mid = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, mid)
        .expect("delta");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("seed");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply");
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM h ORDER BY id").1,
        vec![
            vec![Some("1".into()), Some("1".into())],
            vec![Some("2".into()), Some("2".into())],
            vec![Some("3".into()), Some("3".into())],
        ],
        "an in-flight heap update serves the committed pre-image, decoded as a row"
    );

    // Commit the heap txn, then a mid-transaction STATEMENT rollback: a
    // multi-row re-key that collides part-way is undone with CLRs (no
    // TXN_END — the transaction continues and commits). Un-captured, the
    // undone delete's chain head would hide committed row 1 forever.
    batch(&primary, &mut ctx, "COMMIT TRANSACTION");
    batch(&primary, &mut ctx, "INSERT INTO t VALUES (10, 10)");
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    let failed = batch(&primary, &mut ctx, "UPDATE t SET id = id + 9 WHERE id <= 2");
    assert!(
        failed.error.is_some(),
        "the re-key collides with row 10 and the statement rolls back"
    );
    batch(&primary, &mut ctx, "INSERT INTO t VALUES (4, 4)");
    batch(&primary, &mut ctx, "COMMIT TRANSACTION");
    assert_eq!(
        sql_rows(&primary, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("5".into())]],
        "primary: rows 1,2,3,10,4 (the failed statement fully undone)"
    );
    let done = primary.storage().wal_flushed_lsn();
    let delta2 = primary.storage().read_wal_range(mid, done).expect("delta2");
    standby
        .storage()
        .apply_wal_stream(mid, &delta2)
        .expect("apply2");
    assert_eq!(
        sql_rows(&standby, "SELECT id FROM t ORDER BY id").1,
        sql_rows(&primary, "SELECT id FROM t ORDER BY id").1,
        "the CLR-undone statement's rows stay visible on the standby"
    );
    assert_eq!(
        sql_rows(&standby, "SELECT v FROM t WHERE id = 1").1,
        vec![vec![Some("1".into())]],
        "row 1 — deleted, CLR-restored, then committed — is served"
    );
    assert_eq!(
        sql_rows(&standby, "SELECT v FROM h WHERE id = 1").1,
        vec![vec![Some("999".into())]],
        "the committed heap update is visible"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18.6 monitoring: the replication DMVs report role, slots,
// connectedness, and lag on both sides of a link.
#[test]
fn replication_dmvs_report_slots_lag_and_role() {
    let primary_path = unique_temp_path("dmv-primary");
    let bak = unique_temp_path("dmv-bak");
    let standby_path = unique_temp_path("dmv-standby");

    let primary = new_engine(&primary_path);
    primary
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    for i in 1..=5 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    // No slots yet: the primary reports zero replica rows.
    assert_eq!(
        sql_rows(&primary, "SELECT COUNT(*) FROM sys.dm_repl_replica_states").1,
        vec![vec![Some("0".into())]],
    );
    assert_eq!(
        sql_rows(&primary, "SELECT COUNT(*) FROM sys.dm_repl_slots").1[0][0],
        Some("0".into())
    );

    // Register a slot (as the sender would) and check the reporting.
    let held = primary.storage().wal_flushed_lsn();
    primary
        .storage()
        .try_register_repl_slot(3, held)
        .expect("slot");
    for i in 6..=10 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    let (cols, rows) = sql_rows(
        &primary,
        "SELECT role, node_id, is_connected, lag_bytes, sync_state \
             FROM sys.dm_repl_replica_states",
    );
    assert_eq!(
        cols,
        vec!["role", "node_id", "is_connected", "lag_bytes", "sync_state"]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Some("PRIMARY".into()));
    assert_eq!(rows[0][1], Some("3".into()));
    assert_eq!(
        rows[0][2],
        Some("0".into()),
        "no live sender (bit renders 0)"
    );
    let lag: i64 = rows[0][3].as_deref().unwrap().parse().expect("lag");
    assert!(lag > 0, "the slot lags the new commits");
    assert_eq!(rows[0][4], Some("ASYNC".into()));
    let (_, slot_rows) = sql_rows(
        &primary,
        "SELECT slot_id, retained_bytes FROM sys.dm_repl_slots",
    );
    assert_eq!(slot_rows[0][0], Some("3".into()));
    assert!(slot_rows[0][1].as_deref().unwrap().parse::<i64>().unwrap() > 0);

    // A standby reports its own role and applied position.
    let summary = primary.storage().backup_full(&bak).expect("backup");
    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("seed");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    let _ = summary;
    let (_, srows) = sql_rows(
        &standby,
        "SELECT role, sync_state FROM sys.dm_repl_replica_states",
    );
    assert_eq!(srows.len(), 1);
    assert_eq!(srows[0][0], Some("STANDBY".into()));
    assert_eq!(srows[0][1], Some("NOT_APPLICABLE".into()));

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18.5 (D2) synchronous commit: a commit waits — after local
// durability — for a standby acknowledgement; a timeout degrades the link
// (NOT_SYNCHRONIZED) availability-first, and a caught-up acknowledgement
// re-synchronizes it.
