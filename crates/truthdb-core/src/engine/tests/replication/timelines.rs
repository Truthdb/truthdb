use super::*;

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
