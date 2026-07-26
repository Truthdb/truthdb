use crate::engine::test_support::*;

use crate::engine::*;

#[test]
fn recovery_redo_is_idempotent_across_repeated_reopens() {
    // Redo is the resumable core of replication (a standby applies it as
    // records arrive): re-running it must be a no-op, gated by each page's
    // LSN. Reopening the same database repeatedly re-runs redo over the whole
    // log each time; the committed state must survive unchanged.
    let path = unique_temp_path("redo-idempotent");
    {
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
            .expect("create");
        for i in 1..=30 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        engine
            .execute("UPDATE t SET v = v + 100 WHERE id <= 10")
            .expect("update");
    }
    let expected = {
        let engine = Engine::new(Storage::open(path.clone()).expect("open")).expect("engine");
        sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1
    };
    // Reopen several more times: each reopen re-runs redo over the full log.
    // The state is invariant — proving redo re-application is idempotent.
    for _ in 0..3 {
        let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("engine");
        assert_eq!(
            sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1,
            expected,
            "redo re-application over the whole log is a no-op"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn commit_records_carry_a_recent_wall_clock_timestamp() {
    use crate::storage_layout::WAL_ENTRY_TYPE_REL;
    use crate::wal::records::{REL_KIND_TXN_COMMIT, RelRecord};
    let now_ms = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    };
    let path = unique_temp_path("commit-ts");
    let before = now_ms();
    {
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
            .expect("create");
        engine.execute("INSERT INTO t VALUES (1)").expect("insert"); // autocommits
    }
    let after = now_ms();

    // Reopen so recovery's ring scan populates the replay cache; some
    // committed transaction's record carries a v2 entry with a wall-clock
    // timestamp in [before, after] (for point-in-time restore).
    let storage = Storage::open(path.clone()).expect("reopen");
    let records = storage.replay_wal_entries().expect("replay");
    let mut found = false;
    for r in records {
        if r.entry_type != WAL_ENTRY_TYPE_REL {
            continue;
        }
        let rec = RelRecord::decode(&r.payload).expect("decode");
        if rec.kind == REL_KIND_TXN_COMMIT && rec.redo.len() >= 8 {
            assert_eq!(r.entry_version, 2, "new commit records are entry version 2");
            let ts = u64::from_le_bytes(rec.redo[..8].try_into().unwrap());
            if before <= ts && ts <= after {
                found = true;
            }
        }
    }
    assert!(
        found,
        "a commit record carried a timestamp in [{before}, {after}]"
    );
    drop(storage);
    let _ = std::fs::remove_file(path);
}
#[test]
fn full_recovery_ring_full_reports_9002() {
    let path = unique_temp_path("backuplog-9002");
    // A small ring so the un-backed-up log fills it quickly.
    let storage = Storage::create_with_wal_bounds(
        path.clone(),
        test_storage_options(),
        128 * 1024,
        128 * 1024,
    )
    .expect("create");
    let engine = Engine::new(storage).expect("engine");
    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("full");
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v NVARCHAR(120))")
        .expect("create");
    // FULL pins the head at the enable point, so with no BACKUP LOG the ring
    // fills and a write eventually reports 9002 (log full).
    let mut hit_9002 = false;
    for i in 0..4000 {
        let env = sql(
            &engine,
            &format!("INSERT INTO t VALUES ({i}, '{}')", "x".repeat(100)),
        );
        if let Some(n) = env["error"]["number"].as_i64() {
            assert_eq!(n, 9002, "ring-full under FULL reports 9002, got {env}");
            hit_9002 = true;
            break;
        }
    }
    assert!(hit_9002, "the ring filled and reported 9002");
    drop(engine);
    let _ = std::fs::remove_file(path);
}
