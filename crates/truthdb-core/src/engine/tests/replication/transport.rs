use super::*;

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
