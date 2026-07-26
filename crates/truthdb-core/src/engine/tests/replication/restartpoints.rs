use super::*;

#[test]
fn a_standby_restartpoint_reclaims_the_ring_up_to_its_own_undo_floor() {
    let primary_path = unique_temp_path("rsp-primary");
    let bak = unique_temp_path("rsp-bak");
    let standby_path = unique_temp_path("rsp-standby");

    // A pure-relational primary (no search events, so no search floor).
    let primary = new_engine(&primary_path);
    primary
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=10 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let summary = primary.storage().backup_full(&bak).expect("backup");
    let backup_end = summary.backup_end_lsn;
    for i in 11..=30 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let flushed = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, flushed)
        .expect("delta");
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;

    // A restartpoint on the primary is a no-op (not a standby).
    assert!(
        !primary
            .standby_restartpoint_if_needed()
            .expect("primary no-op"),
        "a primary never takes a restartpoint"
    );

    // Every shipped transaction is resolved, so the floor is the applied
    // tail: the restartpoint reclaims the whole shipped range.
    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply");
    let head_before = standby.storage().wal_head();
    assert!(
        standby.standby_restartpoint().expect("restartpoint"),
        "the restartpoint runs"
    );
    assert!(standby.storage().wal_head() > head_before);
    assert_eq!(
        standby.storage().wal_head(),
        standby.storage().wal_tail(),
        "with everything resolved, the head reaches the applied tail"
    );
    assert!(
        !standby.standby_restartpoint().expect("second"),
        "nothing further to reclaim"
    );
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "relational state intact after the restartpoint"
    );

    // Durable: reopen replays from the advanced head (pages were flushed).
    drop(standby);
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("reopen")).expect("eng");
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "state survives a reopen after the restartpoint"
    );

    // The stream continues past a restartpoint.
    for i in 31..=40 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let flushed2 = primary.storage().wal_flushed_lsn();
    let delta2 = primary
        .storage()
        .read_wal_range(flushed, flushed2)
        .expect("delta2");
    standby
        .storage()
        .apply_wal_stream(flushed, &delta2)
        .expect("apply after restartpoint");
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the stream continues cleanly after a restartpoint"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// A WRITABLE restore is a NEW timeline (its history rewinds and its future
// writes diverge), so it must not present the source's epoch to standbys
// that followed the source — the epoch bumps. A --standby seed stays on
// the source's timeline verbatim.
#[test]
fn a_failed_apply_never_feeds_a_restartpoint() {
    let primary_path = unique_temp_path("rspx-primary");
    let bak = unique_temp_path("rspx-bak");
    let standby_path = unique_temp_path("rspx-standby");

    let primary = new_engine(&primary_path);
    primary
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=10 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let summary = primary.storage().backup_full(&bak).expect("backup");
    let backup_end = summary.backup_end_lsn;
    for i in 11..=20 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let f1 = primary.storage().wal_flushed_lsn();
    let delta1 = primary
        .storage()
        .read_wal_range(backup_end, f1)
        .expect("delta1");
    for i in 21..=30 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let f2 = primary.storage().wal_flushed_lsn();
    let delta2 = primary.storage().read_wal_range(f1, f2).expect("delta2");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta1)
        .expect("apply delta1");

    // A cut chunk fails the apply's coverage guard AFTER the ring bytes
    // were seeded (the live tail moved; the persisted tail did not).
    let cut = &delta2[..delta2.len() - 3];
    standby
        .storage()
        .apply_wal_stream(f1, cut)
        .expect_err("a cut chunk is refused");

    // The restartpoint reclaims only to the persisted tail.
    assert!(
        standby.standby_restartpoint().expect("restartpoint"),
        "the fully-applied prefix is reclaimable"
    );
    assert_eq!(
        standby.storage().wal_head(),
        f1,
        "the head stops at the persisted tail, not at the failed chunk's end"
    );

    // The re-shipped full chunk applies cleanly over the cut bytes, and
    // the next restartpoint reaches the new tail.
    standby
        .storage()
        .apply_wal_stream(f1, &delta2)
        .expect("re-apply the full chunk");
    assert!(
        standby.standby_restartpoint().expect("after re-apply"),
        "the re-applied range is reclaimable"
    );
    assert_eq!(standby.storage().wal_head(), f2);
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby matches the primary after the recovery"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// The restartpoint's undo floor: a shipped range ending MID-transaction
// leaves that transaction unresolved in the standby's own
// active-transaction table, and the head must stop at its BEGIN — the
// undo log above it is what promotion's analysis+undo will need. Once the
// commit ships, the floor lifts.
#[test]
fn a_standby_restartpoint_stops_at_an_unresolved_transactions_begin() {
    let primary_path = unique_temp_path("rspf-primary");
    let bak = unique_temp_path("rspf-bak");
    let standby_path = unique_temp_path("rspf-standby");

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
    let begin_area = primary.storage().wal_flushed_lsn();

    // An explicit transaction, durable but uncommitted: the shipped range
    // ends mid-transaction.
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    for i in 11..=20 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    let mid = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, mid)
        .expect("mid-transaction range");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply mid-txn range");
    assert!(
        standby.standby_restartpoint().expect("restartpoint"),
        "the resolved prefix is reclaimable"
    );
    let head = standby.storage().wal_head();
    assert_eq!(
        head, begin_area,
        "the head advanced exactly to the unresolved transaction's BEGIN \
             (reclaiming everything before it, retaining all of its undo log)"
    );
    assert!(
        head < mid,
        "the head stopped below the applied tail (the open transaction pins it)"
    );

    // The floor survives a reopen (the ATT is re-derived from the ring).
    drop(standby);
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    assert!(
        !standby.standby_restartpoint().expect("after reopen"),
        "still pinned by the same unresolved transaction after a reopen"
    );

    // The commit ships: the floor lifts and the next restartpoint reaches
    // the applied tail.
    batch(&primary, &mut ctx, "COMMIT");
    let flushed = primary.storage().wal_flushed_lsn();
    let delta2 = primary
        .storage()
        .read_wal_range(mid, flushed)
        .expect("commit range");
    standby
        .storage()
        .apply_wal_stream(mid, &delta2)
        .expect("apply commit");
    assert!(
        standby.standby_restartpoint().expect("after commit"),
        "the commit lifted the floor"
    );
    assert_eq!(
        standby.storage().wal_head(),
        standby.storage().wal_tail(),
        "everything resolved: full reclaim"
    );
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby matches the primary"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// The restartpoint's search floor: the seed carries no search snapshot, so
// every shipped search record must stay in the ring for the reopen replay
// — the head clamps below the first one, and the reopened standby still
// serves the search state.
#[test]
fn a_standby_restartpoint_never_truncates_unsnapshotted_search_records() {
    let primary_path = unique_temp_path("rsps-primary");
    let bak = unique_temp_path("rsps-bak");
    let standby_path = unique_temp_path("rsps-standby");

    let primary = new_engine(&primary_path);
    primary
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    for i in 1..=10 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    let summary = primary.storage().backup_full(&bak).expect("backup");
    let backup_end = summary.backup_end_lsn;
    // Post-backup: relational rows FIRST (a reclaimable prefix), then the
    // search records (which pin the head), then more rows.
    for i in 11..=15 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    primary
            .execute(r#"create index products { "mappings": { "properties": { "name": { "type": "text" } } } }"#)
            .expect("create search index");
    primary
        .execute(r#"insert document products { "name": "red shoes" }"#)
        .expect("insert doc");
    for i in 16..=20 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    let flushed = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, flushed)
        .expect("delta");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply");
    assert!(
        standby.standby_restartpoint().expect("restartpoint"),
        "the pre-search prefix is reclaimable"
    );
    let head = standby.storage().wal_head();
    assert!(
        head > backup_end,
        "the relational prefix before the search records was reclaimed"
    );
    assert!(
        head < standby.storage().wal_tail(),
        "the head stopped below the tail: the search records are retained"
    );

    // Reopen: the ring still holds every search record, so the replayed
    // search state serves the streamed doc.
    drop(standby);
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    let response = standby
        .execute(r#"search products { "query": { "match": { "name": "red" } } }"#)
        .expect("standby search after reopen");
    let response: Value = serde_json::from_str(&response).expect("json");
    assert_eq!(
        response["hits"]["total"].as_u64(),
        Some(1),
        "the streamed search doc survives the restartpoint + reopen"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// A FULL-recovery-model primary's backup seeds the standby with the FULL
// bit and a log-backup marker — but the standby must NOT hold ring
// truncation there (its log chain belongs to the primary; holding would
// cap every restartpoint at the seed point forever and run the ring
// full). BACKUP LOG on the standby is refused for the same reason.
#[test]
fn a_full_model_standby_still_reclaims_and_refuses_backup_log() {
    let primary_path = unique_temp_path("rspl-primary");
    let bak = unique_temp_path("rspl-bak");
    let standby_path = unique_temp_path("rspl-standby");

    let primary = new_engine(&primary_path);
    primary
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    primary
        .execute("ALTER DATABASE truthdb SET RECOVERY FULL")
        .expect("full model");
    for i in 1..=10 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    let summary = primary.storage().backup_full(&bak).expect("backup");
    let backup_end = summary.backup_end_lsn;
    for i in 11..=20 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    let flushed = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, flushed)
        .expect("delta");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply");
    assert!(
        standby.standby_restartpoint().expect("restartpoint"),
        "a FULL-model standby still reclaims (no log-backup hold)"
    );
    assert!(
        standby.storage().wal_head() > backup_end,
        "the head advanced past the seeded log-backup marker"
    );
    let err = sql(&standby, "BACKUP LOG truthdb TO DISK = '/tmp/never.trn'");
    assert!(
        !err["error"].is_null(),
        "BACKUP LOG on a standby is refused: {err}"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18 slice 4d (review fix): a shipped stream can end MID-transaction
// (the primary's durable watermark can land between an in-flight txn's page
// ops and its commit). A standby's reopen must REPEAT history (redo-only) —
// a full ARIES undo would roll back the in-flight rows, and since the primary
// commits them and resumes shipping ABOVE the standby's applied point, they
// would be lost forever (silent replica divergence).
#[test]
fn a_standby_reopen_is_redo_only_and_keeps_in_flight_stream_data() {
    let primary_path = unique_temp_path("redo-primary");
    let bak = unique_temp_path("redo-bak");
    let standby_path = unique_temp_path("redo-standby");

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

    // An explicit transaction whose inserts are made durable (fsynced) but NOT
    // committed — the durable watermark now lands mid-transaction.
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    for i in 11..=20 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    let mid = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, mid)
        .expect("ship the mid-transaction range");

    let count = |eng: &Engine| -> String {
        sql_rows(eng, "SELECT COUNT(*) FROM t").1[0][0]
            .clone()
            .unwrap_or_default()
    };

    // Standby applies the mid-transaction stream live: the in-flight rows
    // are applied PHYSICALLY via redo (proven by the post-commit equality
    // below) but hidden from reads — a readable standby serves only
    // committed state.
    Storage::restore_full(&standby_path, &bak).expect("restore");
    {
        let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
        standby
            .storage()
            .apply_wal_stream(backup_end, &delta)
            .expect("apply mid-txn stream");
        assert_eq!(
            count(&standby),
            "10",
            "in-flight rows are applied but not visible"
        );
    }
    // THE FIX: a standby reopen is redo-only, so the applied rows survive. A
    // full ARIES undo here would drop the 10 in-flight rows permanently.
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("reopen")).expect("eng");
    assert_eq!(
        count(&standby),
        "10",
        "redo-only reopen keeps the streamed in-flight rows physically, still hidden"
    );

    // The primary commits and ships the continuation; the standby stays
    // consistent with the primary.
    batch(&primary, &mut ctx, "COMMIT TRANSACTION");
    let after = primary.storage().wal_flushed_lsn();
    let delta2 = primary
        .storage()
        .read_wal_range(mid, after)
        .expect("ship the commit");
    standby
        .storage()
        .apply_wal_stream(mid, &delta2)
        .expect("apply the commit");
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1,
        "the standby matches the primary once the commit is shipped"
    );

    drop(primary);
    drop(standby);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18 slice 3: a replication slot holds WAL-ring truncation at a
// standby's received LSN, survives a restart, and is invalidated once it
// lags past `max_slot_retain_bytes`.
