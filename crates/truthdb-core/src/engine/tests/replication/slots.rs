use super::*;

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
