use super::*;

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
