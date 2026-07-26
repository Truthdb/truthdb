use crate::engine::test_support::*;

use crate::engine::*;

// Stage 18 slice 2: a standby seeded from a full backup, fed the primary's
// raw shipped WAL ring bytes (`read_wal_range`), recovers to a state that
// matches the primary — the physical-replication apply path, offline.
#[test]
fn standby_applies_shipped_wal_ranges_and_matches_the_primary() {
    let src = unique_temp_path("repl-src");
    let bak = unique_temp_path("repl-bak");
    let standby = unique_temp_path("repl-standby");
    let standby_idem = unique_temp_path("repl-standby-idem");

    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    // Full backup captures rows 1..=20 up to `backup_end`.
    let summary = engine.storage().backup_full(&bak).expect("full backup");
    let backup_end = summary.backup_end_lsn;
    // Committed changes AFTER the backup — the log a standby must catch up on.
    for i in 21..=40 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    engine
        .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
        .expect("update");
    // Ship the primary's raw ring bytes `[backup_end, durable tail)`.
    let flushed = engine.storage().wal_flushed_lsn();
    assert!(flushed > backup_end, "there is post-backup log to ship");
    let delta = engine
        .storage()
        .read_wal_range(backup_end, flushed)
        .expect("ship the WAL range");
    let expected = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1;
    drop(engine);

    // Standby = the full backup + the shipped raw WAL range, recovered.
    Storage::restore_full_with_wal_ranges(&standby, &bak, &[(backup_end, delta.clone())])
        .expect("apply the shipped range to the standby");
    let s = Engine::new(Storage::open(standby.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&s, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby matches the primary after applying the shipped WAL"
    );
    // The replication restartpoint persisted = the end of the applied range.
    assert_eq!(
        s.storage().applied_lsn(),
        backup_end + delta.len() as u64,
        "applied_lsn is the end of the applied range and survives the reopen"
    );

    // Idempotent: applying the SAME range twice yields the identical state
    // (seed overwrites identical bytes; redo is page-LSN-gated).
    Storage::restore_full_with_wal_ranges(
        &standby_idem,
        &bak,
        &[(backup_end, delta.clone()), (backup_end, delta.clone())],
    )
    .expect("re-applying the same range is accepted");
    let s2 = Engine::new(Storage::open(standby_idem.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&s2, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "re-applying the same range is idempotent"
    );

    drop(s);
    drop(s2);
    for p in [src, bak, standby, standby_idem] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18 slice 4d: an OPEN standby applies a shipped WAL range LIVE (no
// reopen) and its state matches the primary; the apply is idempotent and
// survives a standby restart.
#[test]
fn a_standby_applies_a_live_wal_stream_and_matches_the_primary() {
    let primary_path = unique_temp_path("live-primary");
    let bak = unique_temp_path("live-bak");
    let standby_path = unique_temp_path("live-standby");

    // Primary: rows 1..=10, a backup, then post-backup changes to ship.
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
    for i in 11..=25 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    primary
        .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
        .expect("update");
    let flushed = primary.storage().wal_flushed_lsn();
    let delta = primary
        .storage()
        .read_wal_range(backup_end, flushed)
        .expect("ship the delta");
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;

    // Standby: restore the backup as an OPEN engine (only the 10 backed-up
    // rows), then apply the shipped delta LIVE — no reopen.
    Storage::restore_full(&standby_path, &bak).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    assert_eq!(
        sql_rows(&standby, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("10".into())]],
        "the standby starts at the backup point"
    );
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("live apply");
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby matches the primary after applying the live stream (no reopen)"
    );

    // Idempotent: re-applying the same range changes nothing.
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("re-apply");
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "re-applying the same range is idempotent"
    );

    // Durable: the applied state survives a standby restart.
    drop(standby);
    let standby2 = Engine::new(Storage::open(standby_path.clone()).expect("reopen")).expect("eng");
    assert_eq!(
        sql_rows(&standby2, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the live-applied state persists across a standby restart"
    );

    // A backup taken ON the standby captures the FULL applied state (the
    // in-memory WAL tail is resynced, so the backup is not truncated to the
    // pre-apply point).
    let standby_bak = unique_temp_path("live-standby-bak");
    let restored = unique_temp_path("live-standby-restored");
    standby2
        .storage()
        .backup_full(&standby_bak)
        .expect("backup on the standby");
    Storage::restore_full(&restored, &standby_bak).expect("restore the standby backup");
    let r = Engine::new(Storage::open(restored.clone()).expect("open")).expect("eng");
    assert_eq!(
        sql_rows(&r, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "a backup of the standby restores the full applied state"
    );

    // A checkpoint on a standby is refused (it reclaims ring space only at
    // promotion, to keep the in-flight undo log).
    assert!(
        standby2.checkpoint().is_err(),
        "checkpoint is refused on a standby"
    );

    // A standby is read-only: a local client write is rejected (it would
    // append to the replica's own WAL and diverge it from the primary).
    let write = sql(&standby2, "INSERT INTO t VALUES (999, 999)");
    assert!(
        !write["error"].is_null(),
        "a local write on a standby is rejected: {write}"
    );
    assert_eq!(
        sql_rows(&standby2, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("25".into())]],
        "the rejected write left the standby unchanged"
    );

    drop(primary);
    drop(standby2);
    drop(r);
    for p in [primary_path, bak, standby_path, standby_bak, restored] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18 slice 4c: the full transport, end to end over a real TCP+TLS
// socket — listener + handshake + per-standby sender on the primary,
// reconnecting receiver on the standby. A backup-seeded standby catches up,
// follows live commits (woken by the flushed watch, no polling), its
// FlushAcks advance the primary's slot, and a receiver restart resumes from
// the standby's persisted watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_replication_transport_streams_live_writes_to_a_standby() {
    use crate::repl::listener::{PrimaryReplContext, run_repl_listener};
    use crate::repl::receiver::{ReceiverConfig, run_standby_receiver};
    use crate::repl::tls::server_config_from_pem;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio_rustls::TlsAcceptor;

    const SECRET: &[u8] = b"transport-secret";
    const UUID: [u8; 16] = [7u8; 16];

    let primary_path = unique_temp_path("xport-primary");
    let bak = unique_temp_path("xport-bak");
    let standby_path = unique_temp_path("xport-standby");

    // Primary: rows 1..=10, a backup to seed the standby, then post-backup
    // rows the transport must catch the standby up on.
    let primary = new_engine(&primary_path);
    primary
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=10 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    primary.storage().backup_full(&bak).expect("backup");
    // A catch-up backlog spanning MANY sender chunks (the context below
    // sets 512-byte chunks), so entry-boundary chunking is exercised end
    // to end — a mid-entry cut would fail the apply's coverage check.
    for i in 11..=120 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }

    // The primary's replication listener on an ephemeral port.
    let (cert_pem, key_pem) = {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        (c.cert.pem(), c.key_pair.serialize_pem())
    };
    let acceptor =
        TlsAcceptor::from(server_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let ctx = PrimaryReplContext {
        shared_secret: Arc::new(SECRET.to_vec()),
        cluster_uuid: UUID,
        storage: primary.storage_arc(),
        heartbeat: Duration::from_millis(200),
        stall_timeout: Duration::from_secs(30),
        chunk_bytes: 512,
    };
    let listener_task = tokio::spawn(run_repl_listener(
        listener,
        acceptor,
        ctx,
        shutdown_rx.clone(),
    ));

    // Standby: seeded with a --standby restore (stamped redo-only +
    // read-only before its first open), opened live, then its receiver
    // dials the primary.
    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    assert!(
        standby.storage().is_standby(),
        "a --standby restore stamps the standby mode before the first open"
    );
    let receiver_cfg = ReceiverConfig {
        primary_addr: addr.to_string(),
        server_name: "localhost".to_string(),
        tls_ca_pem: cert_pem.as_bytes().to_vec(),
        shared_secret: SECRET.to_vec(),
        cluster_uuid: UUID,
        node_id: 7,
        reconnect_delay: Duration::from_millis(100),
        stall_timeout: Duration::from_secs(30),
    };
    let (rx_shutdown_tx, rx_shutdown_rx) = watch::channel(false);
    let receiver_task = tokio::spawn(run_standby_receiver(
        receiver_cfg.clone(),
        standby.storage_arc(),
        rx_shutdown_rx,
    ));

    async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !cond() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    // Catch-up: the standby reaches the primary's durable watermark.
    let target = primary.storage().wal_flushed_lsn();
    wait_until("catch-up", || standby.storage().wal_tail() >= target).await;
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby matches the primary after catch-up"
    );

    // Live follow: new commits arrive without any reconnect.
    for i in 21..=30 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    primary
        .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
        .expect("update");
    let target = primary.storage().wal_flushed_lsn();
    wait_until("live follow", || standby.storage().wal_tail() >= target).await;
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby follows live commits"
    );

    // The standby's FlushAcks advance the primary's slot to its watermark,
    // so the primary can reclaim the shipped log.
    wait_until("slot advance", || {
        primary.storage().repl_slot_lsn(7) == Some(target)
    })
    .await;

    // Reconnect: stop the receiver, commit more, restart it — the stream
    // resumes from the standby's persisted watermark (the slot held the
    // log in between).
    rx_shutdown_tx.send(true).unwrap();
    let _ = receiver_task.await;
    for i in 31..=40 {
        primary
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let (rx_shutdown_tx2, rx_shutdown_rx2) = watch::channel(false);
    let receiver_task2 = tokio::spawn(run_standby_receiver(
        receiver_cfg,
        standby.storage_arc(),
        rx_shutdown_rx2,
    ));
    let target = primary.storage().wal_flushed_lsn();
    wait_until("resume after reconnect", || {
        standby.storage().wal_tail() >= target
    })
    .await;
    let expected = sql_rows(&primary, "SELECT id, v FROM t ORDER BY id").1;
    assert_eq!(
        sql_rows(&standby, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "the standby resumes and matches after a receiver restart"
    );

    rx_shutdown_tx2.send(true).unwrap();
    let _ = receiver_task2.await;
    shutdown_tx.send(true).unwrap();
    let _ = listener_task.await;
    drop(standby);
    drop(primary);
    for p in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(p);
    }
}

// Stage 18 (18.3 restartpoints): a standby cannot checkpoint, so the
// restartpoint is what reclaims its WAL ring — flush redone pages, persist
// the allocator, advance the head to the standby's OWN undo floor (its
// active-transaction table over the shipped log, plus the first search
// record the seed snapshot does not cover). Storage-only: no WAL append,
// no snapshot write (a locally allocated snapshot extent would collide
// with the primary's future logged allocations).
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
fn a_writable_restore_is_a_new_timeline_a_standby_seed_is_not() {
    let src = unique_temp_path("tl-src");
    let bak = unique_temp_path("tl-bak");
    let writable = unique_temp_path("tl-writable");
    let seed = unique_temp_path("tl-seed");

    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    engine.storage().backup_full(&bak).expect("backup");
    drop(engine);

    Storage::restore_full(&writable, &bak).expect("plain restore");
    let restored = Engine::new(Storage::open(writable.clone()).expect("open")).expect("eng");
    assert_eq!(
        restored.storage().epoch(),
        1,
        "a writable restore bumps the epoch (new timeline)"
    );
    drop(restored);

    Storage::restore_full_standby(&seed, &bak, &[]).expect("standby seed");
    let standby = Engine::new(Storage::open(seed.clone()).expect("open")).expect("eng");
    assert_eq!(
        standby.storage().epoch(),
        0,
        "a standby seed carries the source's timeline"
    );
    drop(standby);
    for p in [src, bak, writable, seed] {
        let _ = std::fs::remove_file(p);
    }
}

// `ALTER DATABASE <name> FAILOVER` online mirrors RESTORE DATABASE: the
// answer is the pointer at the offline CLI.
#[test]
fn alter_database_failover_points_at_the_offline_cli() {
    let path = unique_temp_path("failover-online");
    let engine = new_engine(&path);
    assert_eq!(
        sql_error_number(&engine, "ALTER DATABASE CURRENT FAILOVER"),
        3101
    );
    let response = sql(&engine, "ALTER DATABASE CURRENT FAILOVER");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("truthdb-cli promote"),
        "{response}"
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

// Stage 18.5 (D2) readable standby: reads resolve through the version
// store at the last-applied-commit snapshot — shipped in-flight changes
// (inserts AND updates of pre-existing rows) are invisible until their
// commit ships; an aborted shipped transaction unwinds; the store is
// rebuilt at reopen from the retained ring.
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
#[test]
fn synchronous_commit_waits_for_acks_and_degrades_on_timeout() {
    let path = unique_temp_path("sync-commit");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine
        .storage()
        .arm_sync_commit(std::time::Duration::from_millis(500));

    // A healthy standby: an acker thread mirrors the durable watermark.
    let storage = engine.storage_arc();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = std::sync::Arc::clone(&stop);
    let acker = std::thread::spawn(move || {
        while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
            storage.publish_sync_ack(storage.wal_flushed_lsn());
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    assert!(
        !engine.storage().sync_commit_degraded(),
        "an acked commit keeps the link synchronized"
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    acker.join().expect("acker");

    // The standby goes silent: the next commit stalls out the timeout,
    // degrades the link, and still succeeds.
    let t0 = std::time::Instant::now();
    engine.execute("INSERT INTO t VALUES (2)").expect("insert");
    assert!(
        t0.elapsed() >= std::time::Duration::from_millis(500),
        "the degrading commit waited out the timeout"
    );
    assert!(
        engine.storage().sync_commit_degraded(),
        "the timed-out link is NOT_SYNCHRONIZED"
    );

    // Degraded: commits pass straight through (bounded by well under the
    // timeout — generous margin against CI jitter).
    let t1 = std::time::Instant::now();
    engine.execute("INSERT INTO t VALUES (3)").expect("insert");
    assert!(
        t1.elapsed() < std::time::Duration::from_millis(400),
        "a degraded link does not delay commits"
    );

    // A caught-up acknowledgement re-synchronizes.
    engine
        .storage()
        .publish_sync_ack(engine.storage().wal_flushed_lsn());
    assert!(
        !engine.storage().sync_commit_degraded(),
        "a caught-up standby re-synchronizes the link"
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

// Stage 18.4 (D1 manual failover): offline promotion turns a standby into
// a read-write primary — finish recovery (undo the shipped in-flight
// transactions), clear the standby mode, bump the epoch. The epoch fences
// the old timeline: only an EQUAL epoch may follow the new primary, so
// the old primary and pre-failover seeds must reseed.
#[test]
fn promotion_finishes_recovery_bumps_the_epoch_and_fences_the_old_timeline() {
    use crate::repl::handshake::{HandshakeParams, compute_auth, evaluate_hello};

    let primary_path = unique_temp_path("promo-primary");
    let bak = unique_temp_path("promo-bak");
    let standby_path = unique_temp_path("promo-standby");
    let reseed_bak = unique_temp_path("promo-reseed-bak");
    let reseed_path = unique_temp_path("promo-reseed");

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
    for i in 11..=20 {
        batch(
            &primary,
            &mut ctx,
            &format!("INSERT INTO t VALUES ({i}, {i})"),
        );
    }
    // An in-flight transaction, durable but uncommitted: the primary dies
    // here; promotion must roll it back.
    batch(&primary, &mut ctx, "BEGIN TRANSACTION");
    for i in 21..=25 {
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
        .expect("delta");

    Storage::restore_full_standby(&standby_path, &bak, &[]).expect("restore");
    let standby = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    standby
        .storage()
        .apply_wal_stream(backup_end, &delta)
        .expect("apply");

    // A running server holds the advisory lock: promote refuses.
    assert!(
        Storage::promote(&standby_path).is_err(),
        "promote refuses a file a live server holds"
    );
    drop(standby);

    // A non-standby cannot be promoted.
    drop(primary);
    let err = Storage::promote(&primary_path).expect_err("primary is not a standby");
    assert!(
        err.to_string().contains("not a replication standby"),
        "{err}"
    );

    let epoch = Storage::promote(&standby_path).expect("promote");
    assert_eq!(epoch, 1, "the first failover bumps epoch 0 -> 1");

    let promoted = Engine::new(Storage::open(standby_path.clone()).expect("open")).expect("eng");
    assert!(
        !promoted.storage().is_standby(),
        "the promoted node is a normal primary"
    );
    assert_eq!(promoted.storage().epoch(), 1);
    assert_eq!(
        sql_rows(&promoted, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("20".into())]],
        "the shipped in-flight transaction was undone at promotion"
    );
    promoted
        .execute("INSERT INTO t VALUES (999, 999)")
        .expect("the promoted node accepts writes");

    // Epoch fencing: the OLD timeline (epoch 0 — the dead primary, or any
    // pre-failover seed) cannot follow the new primary.
    const SECRET: &[u8] = b"promo-secret";
    const UUID: [u8; 16] = [9u8; 16];
    let params = HandshakeParams {
        shared_secret: SECRET,
        cluster_uuid: UUID,
        primary_epoch: promoted.storage().epoch(),
        primary_flushed_lsn: promoted.storage().wal_flushed_lsn(),
    };
    let old_auth = compute_auth(SECRET, 3, &UUID, 0, mid);
    let old_hello = crate::repl::Hello {
        protocol_version: crate::repl::REPL_PROTOCOL_VERSION,
        node_id: 3,
        cluster_uuid: UUID,
        epoch: 0,
        last_received_lsn: mid,
        auth: old_auth,
    };
    let ack = evaluate_hello(&old_hello, &params);
    assert!(!ack.accepted, "the old timeline is fenced off");
    assert!(ack.message.contains("reseed"), "{}", ack.message);

    // A FRESH seed from the new primary carries epoch 1 and is accepted.
    promoted
        .storage()
        .backup_full(&reseed_bak)
        .expect("backup of the promoted node");
    Storage::restore_full_standby(&reseed_path, &reseed_bak, &[]).expect("reseed");
    let reseeded = Engine::new(Storage::open(reseed_path.clone()).expect("open")).expect("eng");
    assert_eq!(
        reseeded.storage().epoch(),
        1,
        "the seed carries the new timeline's epoch"
    );
    let new_auth = compute_auth(SECRET, 3, &UUID, 1, reseeded.storage().wal_tail());
    let new_hello = crate::repl::Hello {
        protocol_version: crate::repl::REPL_PROTOCOL_VERSION,
        node_id: 3,
        cluster_uuid: UUID,
        epoch: 1,
        last_received_lsn: reseeded.storage().wal_tail(),
        auth: new_auth,
    };
    assert!(
        evaluate_hello(&new_hello, &params).accepted,
        "an equal-epoch seed follows the new primary"
    );

    drop(promoted);
    drop(reseeded);
    for p in [primary_path, bak, standby_path, reseed_bak, reseed_path] {
        let _ = std::fs::remove_file(p);
    }
}

// A FAILED apply (a cut or corrupt chunk) advances the live ring tail but
// not the persisted one, and folds nothing into the restartpoint floors.
// The restartpoint must reclaim only up to the persisted tail — the last
// fully decoded + redone + committed range — never over bytes whose redo
// did not run.
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
#[test]
fn replication_slots_hold_truncation_persist_and_reap() {
    let path = unique_temp_path("repl-slots");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=10 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let l1 = engine.storage().wal_flushed_lsn();
    assert!(l1 > 0, "there is log to pin");

    // A slot pinned at l1 holds the WAL head there even as the tail advances.
    engine
        .storage()
        .try_register_repl_slot(7, l1)
        .expect("register");
    for i in 11..=30 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    engine.checkpoint().expect("checkpoint");
    assert_eq!(
        engine.storage().wal_head(),
        l1,
        "the slot pins the truncation floor at its LSN"
    );

    // Advancing the slot lets the floor follow.
    let l2 = engine.storage().wal_flushed_lsn();
    engine.storage().advance_repl_slot(7, l2);
    engine.checkpoint().expect("checkpoint");
    assert_eq!(
        engine.storage().wal_head(),
        l2,
        "advancing the slot advances the floor"
    );

    // The slot's hold survives a restart (re-seeded from the superblock).
    drop(engine);
    let engine = Engine::new(Storage::open(path.clone()).expect("open")).expect("engine");
    assert_eq!(
        engine.storage().repl_slot_lsn(7),
        Some(l2),
        "the slot survives the restart"
    );

    // An explicit drop removes a slot (a standby that deregisters).
    engine
        .storage()
        .try_register_repl_slot(8, l2)
        .expect("register");
    engine.storage().drop_repl_slot(8);
    assert_eq!(
        engine.storage().repl_slot_lsn(8),
        None,
        "an explicitly dropped slot is gone"
    );

    // Lagging past max_slot_retain invalidates the slot at the next checkpoint.
    engine
        .storage()
        .set_max_slot_retain_bytes(64)
        .expect("set cap");
    for i in 31..=60 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    engine.checkpoint().expect("checkpoint");
    assert_eq!(
        engine.storage().repl_slot_lsn(7),
        None,
        "an over-lagging slot is invalidated"
    );
    assert!(
        engine.storage().wal_head() > l2,
        "the reclaimed floor advances past the dropped slot"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
}

// A replication sender wakes on the group-commit flushed watch instead of
// polling: a committed write advances the watch past the subscriber's last
// seen value, and the durable watermark it re-reads covers the commit.
#[test]
fn the_flushed_watch_wakes_on_a_committed_write() {
    let path = unique_temp_path("repl-watch");
    let engine = new_engine(&path);
    let mut rx = engine.storage().subscribe_wal_flushed();
    let before = *rx.borrow_and_update();
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    assert!(
        rx.has_changed().expect("sender alive"),
        "a durable commit signals the watch"
    );
    let hint = *rx.borrow_and_update();
    assert!(hint > before, "the watch value advances");
    assert!(
        engine.storage().wal_flushed_lsn() >= hint,
        "the re-read watermark covers the hint"
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

// Slot registration is atomically fenced against truncation: a slot below
// the WAL head is refused (the log it needs is gone — reseed), and the
// table is bounded so a slot never silently fails to persist.
#[test]
fn slot_registration_rejects_below_head_and_a_full_table() {
    let path = unique_temp_path("repl-slot-guards");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    for i in 1..=10 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    engine.checkpoint().expect("checkpoint");
    let head = engine.storage().wal_head();
    assert!(head > 0, "the checkpoint truncated some log");
    let err = engine
        .storage()
        .try_register_repl_slot(1, head - 1)
        .expect_err("a below-head slot is refused");
    assert!(
        err.to_string().contains("reseed"),
        "the error tells the operator to reseed: {err}"
    );

    let lsn = engine.storage().wal_flushed_lsn();
    for id in 1..=8 {
        engine
            .storage()
            .try_register_repl_slot(id, lsn)
            .expect("register");
    }
    engine
        .storage()
        .try_register_repl_slot(9, lsn)
        .expect_err("a ninth slot exceeds the persisted table");
    // Re-registering an existing id is a reset, not a new slot.
    engine
        .storage()
        .try_register_repl_slot(8, lsn)
        .expect("re-register");
    // A mid-entry LSN off the wire is refused: it would become the
    // checkpoint truncation floor and, at the next restart, a WAL head the
    // scan cannot decode (entries are >= 48 bytes and 8-aligned, so
    // `fresh - 1` is never a boundary). Fresh writes keep it above the
    // head so the boundary check — not the head fence — is what fires.
    for i in 11..=13 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("insert");
    }
    let fresh = engine.storage().wal_flushed_lsn();
    let err = engine
        .storage()
        .try_register_repl_slot(8, fresh - 1)
        .expect_err("a mid-entry slot LSN is refused");
    assert!(
        err.to_string().contains("boundary"),
        "the error names the misalignment: {err}"
    );
    // An ack for a never-registered (or reaped) slot must not create one.
    engine.storage().advance_repl_slot(42, lsn);
    assert_eq!(engine.storage().repl_slot_lsn(42), None);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

// The replication epoch persists across a checkpoint (whose superblock
// rebuild zeroes the reserved area — the checkpoint-wipe carry) and a
// restart, and the retention-cap setter refuses a cap the ring cannot
// honor.
#[test]
fn the_replication_epoch_persists_and_the_retain_cap_is_bounded() {
    let path = unique_temp_path("repl-epoch");
    let engine = new_engine(&path);
    assert_eq!(engine.storage().epoch(), 0, "a fresh file is epoch 0");
    engine.storage().set_epoch(5).expect("set epoch");
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.checkpoint().expect("checkpoint");
    drop(engine);
    let engine = Engine::new(Storage::open(path.clone()).expect("open")).expect("engine");
    assert_eq!(
        engine.storage().epoch(),
        5,
        "the epoch survives the checkpoint and the restart"
    );
    engine
        .storage()
        .set_max_slot_retain_bytes(u64::MAX - 1)
        .expect_err("a cap at or above the usable ring is refused");
    drop(engine);
    let _ = std::fs::remove_file(path);
}
