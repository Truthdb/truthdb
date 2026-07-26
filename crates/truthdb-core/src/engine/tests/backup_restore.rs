use crate::engine::test_support::*;

use crate::engine::*;

#[test]
fn full_backup_and_offline_restore_round_trips_relational_data() {
    let src = unique_temp_path("backup-src");
    let bak = unique_temp_path("backup-bak");
    let restored = unique_temp_path("backup-restored");

    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
        .expect("create t");
    engine
        .execute("CREATE INDEX ix_name ON t (name)")
        .expect("create index");
    // Enough rows to spread the heap and the secondary index over several
    // B+tree pages, so the copy is more than a single catalog page.
    for i in 1..=200 {
        engine
            .execute(&format!("INSERT INTO t (id, name) VALUES ({i}, 'row{i}')"))
            .expect("insert into t");
    }
    engine
        .execute("CREATE TABLE u (k INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create u");
    engine
        .execute("INSERT INTO u (k, v) VALUES (1, 10), (2, 20)")
        .expect("insert into u");

    let expected_t = sql_rows(&engine, "SELECT id, name FROM t ORDER BY id").1;

    // Online backup while the engine is live.
    let summary = engine
        .storage()
        .backup_full(&bak)
        .expect("online full backup");
    assert!(summary.pages_copied > 0, "the backup copied data pages");
    assert!(
        summary.backup_end_lsn >= summary.redo_start_lsn,
        "the log bracket is well-formed"
    );
    drop(engine);

    // Offline restore into a fresh file, then open it and compare.
    Storage::restore_full(&restored, &bak).expect("offline restore");
    let engine2 = Engine::new(Storage::open(restored.clone()).expect("open restored"))
        .expect("engine on restored file");

    assert_eq!(
        sql_rows(&engine2, "SELECT id, name FROM t ORDER BY id").1,
        expected_t,
        "table t round-trips row-for-row"
    );
    assert_eq!(
        sql_rows(&engine2, "SELECT k, v FROM u ORDER BY k").1,
        vec![
            vec![Some("1".into()), Some("10".into())],
            vec![Some("2".into()), Some("20".into())],
        ],
        "table u round-trips"
    );
    // The secondary index round-trips: a seek by name finds the row.
    assert_eq!(
        sql_rows(&engine2, "SELECT id FROM t WHERE name = 'row150'").1,
        vec![vec![Some("150".into())]],
        "the secondary index is intact after restore"
    );
    // The catalog round-trips: further DML on the restored database works.
    engine2
        .execute("INSERT INTO u (k, v) VALUES (3, 30)")
        .expect("insert into restored u");
    assert_eq!(
        sql_rows(&engine2, "SELECT v FROM u WHERE k = 3").1,
        vec![vec![Some("30".into())]]
    );
    drop(engine2);

    let _ = std::fs::remove_file(src);
    let _ = std::fs::remove_file(bak);
    let _ = std::fs::remove_file(restored);
}

#[test]
fn backup_database_statement_backs_up_and_restores() {
    let src = unique_temp_path("backup-stmt-src");
    let bak = unique_temp_path("backup-stmt-bak");
    let restored = unique_temp_path("backup-stmt-restored");

    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
        .expect("create");
    for i in 1..=50 {
        engine
            .execute(&format!("INSERT INTO t (id, name) VALUES ({i}, 'r{i}')"))
            .expect("insert");
    }
    let expected = sql_rows(&engine, "SELECT id, name FROM t ORDER BY id").1;

    // The T-SQL BACKUP statement drives the online backup.
    let path_lit = bak.to_str().unwrap().replace('\'', "''");
    let env = sql(
        &engine,
        &format!("BACKUP DATABASE truthdb TO DISK = '{path_lit}' WITH CHECKSUM, COPY_ONLY"),
    );
    assert!(env["error"].is_null(), "BACKUP DATABASE failed: {env}");
    drop(engine);

    Storage::restore_full(&restored, &bak).expect("restore");
    let engine2 =
        Engine::new(Storage::open(restored.clone()).expect("open restored")).expect("engine");
    assert_eq!(
        sql_rows(&engine2, "SELECT id, name FROM t ORDER BY id").1,
        expected,
        "the BACKUP-statement backup restores row-for-row"
    );
    drop(engine2);

    let _ = std::fs::remove_file(src);
    let _ = std::fs::remove_file(bak);
    let _ = std::fs::remove_file(restored);
}

#[test]
fn backup_database_is_rejected_inside_a_transaction() {
    let path = unique_temp_path("backup-in-txn");
    let engine = new_engine(&path);
    // BACKUP manages its own per-chunk locking and cannot run inside an
    // explicit transaction (3021).
    assert_eq!(
        sql_error_number(
            &engine,
            "BEGIN TRANSACTION; BACKUP DATABASE d TO DISK = '/tmp/truthdb-never.bak'"
        ),
        3021
    );
    // A side-effecting BACKUP is illegal inside a function body (156);
    // otherwise a per-row SELECT would run a backup per row.
    assert_eq!(
        sql_error_number(
            &engine,
            "CREATE FUNCTION dbo.f() RETURNS INT AS BEGIN \
                 BACKUP DATABASE d TO DISK = '/tmp/truthdb-never.bak'; RETURN 1 END"
        ),
        156
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn recovery_model_sets_persists_and_reports() {
    let path = unique_temp_path("recovery-model");
    let engine = new_engine(&path);
    let model = |e: &Engine| sql_rows(e, "SELECT recovery_model_desc FROM sys.databases").1;

    // SIMPLE is the default.
    assert_eq!(model(&engine), vec![vec![Some("SIMPLE".into())]]);

    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("set full");
    assert_eq!(model(&engine), vec![vec![Some("FULL".into())]]);

    // An unrelated option in the same statement family leaves it untouched.
    engine
        .execute("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON")
        .expect("rcsi on");
    assert_eq!(model(&engine), vec![vec![Some("FULL".into())]]);
    drop(engine);

    // FULL persists across a reopen (the set is itself durable).
    let engine2 = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("engine");
    assert_eq!(model(&engine2), vec![vec![Some("FULL".into())]]);
    assert_eq!(
        sql_rows(
            &engine2,
            "SELECT is_read_committed_snapshot_on FROM sys.databases"
        )
        .1,
        vec![vec![Some("1".into())]],
        "RCSI survived alongside the recovery model"
    );

    engine2
        .execute("ALTER DATABASE CURRENT SET RECOVERY SIMPLE")
        .expect("set simple");
    assert_eq!(model(&engine2), vec![vec![Some("SIMPLE".into())]]);
    drop(engine2);
    let _ = std::fs::remove_file(path);
}

#[test]
fn backup_log_ships_the_log_advances_the_marker_and_chains() {
    use crate::backup::{BackupReader, BlockType};
    let src = unique_temp_path("backuplog-src");
    let trn1 = unique_temp_path("backuplog-1");
    let trn2 = unique_temp_path("backuplog-2");
    let engine = new_engine(&src);
    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("full");
    // Enabling FULL starts the log chain at the current tail and pins it.
    let chain_start = engine.storage().last_log_backup_lsn();
    assert_eq!(engine.storage().log_backup_hold(), Some(chain_start));
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }

    let read_log_archive = |path: &std::path::Path| -> (crate::backup::BackupHeader, u64) {
        let (mut r, header) =
            BackupReader::new(std::io::BufReader::new(std::fs::File::open(path).unwrap())).unwrap();
        let mut log_bytes = 0u64;
        while let Some((bt, payload)) = r.next_block().unwrap() {
            if bt == BlockType::LogChunk {
                log_bytes += payload.len().saturating_sub(8) as u64; // minus the start-LSN prefix
            }
        }
        (header, log_bytes)
    };

    // First BACKUP LOG ships [chain_start, tail) and advances the marker.
    let lit1 = trn1.to_str().unwrap().replace('\'', "''");
    assert!(
        sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit1}'"))["error"].is_null(),
        "BACKUP LOG succeeded"
    );
    let marker1 = engine.storage().last_log_backup_lsn();
    assert!(marker1 > chain_start, "the marker advanced");
    assert_eq!(
        engine.storage().log_backup_hold(),
        Some(marker1),
        "the hold moved to the new marker"
    );
    let (header1, bytes1) = read_log_archive(&trn1);
    assert!(header1.flags.log_backup, "flagged as a log-only archive");
    assert_eq!(header1.redo_start_lsn, chain_start);
    assert_eq!(
        chain_start + bytes1,
        marker1,
        "the shipped range ends at the marker"
    );

    // A second BACKUP LOG chains contiguously from the first.
    engine
        .execute("INSERT INTO t VALUES (99, 99)")
        .expect("more");
    let lit2 = trn2.to_str().unwrap().replace('\'', "''");
    assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit2}'"))["error"].is_null());
    let (header2, _) = read_log_archive(&trn2);
    assert_eq!(
        header2.redo_start_lsn, marker1,
        "the second archive starts where the first ended"
    );
    drop(engine);
    for p in [src, trn1, trn2] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn restoring_a_full_recovery_database_opens_and_checkpoints_cleanly() {
    let src = unique_temp_path("restore-full-src");
    let bak = unique_temp_path("restore-full-bak");
    let restored = unique_temp_path("restore-full-restored");
    let trn = unique_temp_path("restore-full-trn");
    let engine = new_engine(&src);
    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("full");
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=30 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let expected = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1;
    engine
        .storage()
        .backup_full(&bak)
        .expect("full backup of a FULL-model db");
    drop(engine);

    Storage::restore_full(&restored, &bak).expect("restore");
    let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&engine2, "SELECT id, v FROM t ORDER BY id").1,
        expected
    );
    // The restored DB is FULL and its log-backup floor is seeded at the
    // restore point (backup_end), so the on-open hold sits at/above wal_head.
    assert_eq!(
        sql_rows(&engine2, "SELECT recovery_model_desc FROM sys.databases").1,
        vec![vec![Some("FULL".into())]]
    );
    assert!(
        engine2.storage().log_backup_hold().is_some(),
        "FULL-model hold re-registered on the restored db"
    );
    // A checkpoint would panic (set_head with a floor below the head) if the
    // marker had been left at 0 — the Fix-3 regression guard.
    engine2
        .storage()
        .write_checkpoint(b"cp", 1, 2, 1)
        .expect("checkpoint on the restored db");
    // And BACKUP LOG works on the restored (fresh) log chain.
    let lit = trn.to_str().unwrap().replace('\'', "''");
    assert!(sql(&engine2, &format!("BACKUP LOG truthdb TO DISK = '{lit}'"))["error"].is_null());
    drop(engine2);
    for p in [src, bak, restored, trn] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn restore_full_plus_log_chain_recovers_post_backup_changes() {
    let src = unique_temp_path("restlog-src");
    let bak = unique_temp_path("restlog-bak");
    let trn1 = unique_temp_path("restlog-1");
    let trn2 = unique_temp_path("restlog-2");
    let restored = unique_temp_path("restlog-restored");
    let restored_full_only = unique_temp_path("restlog-fullonly");
    let restored_gap = unique_temp_path("restlog-gap");

    let engine = new_engine(&src);
    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("full");
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    // Full backup captures rows 1..=20.
    engine.storage().backup_full(&bak).expect("full backup");
    // Changes AFTER the full backup, then a first log backup.
    for i in 21..=40 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let lit1 = trn1.to_str().unwrap().replace('\'', "''");
    assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit1}'"))["error"].is_null());
    // More changes, then a second (chained) log backup.
    engine
        .execute("UPDATE t SET v = v + 100 WHERE id <= 5")
        .expect("update");
    for i in 41..=50 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let lit2 = trn2.to_str().unwrap().replace('\'', "''");
    assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit2}'"))["error"].is_null());
    let expected = sql_rows(&engine, "SELECT id, v FROM t ORDER BY id").1;
    drop(engine);

    // Full + the whole log chain recovers EVERY committed change.
    Storage::restore_full_with_logs(&restored, &bak, &[trn1.clone(), trn2.clone()], None)
        .expect("restore full + log chain");
    let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&engine2, "SELECT id, v FROM t ORDER BY id").1,
        expected,
        "restore + log chain recovers all post-full-backup changes"
    );

    // The full backup alone recovers only the 20 rows at its point.
    Storage::restore_full(&restored_full_only, &bak).expect("restore full only");
    let engine3 =
        Engine::new(Storage::open(restored_full_only.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&engine3, "SELECT COUNT(*) FROM t").1,
        vec![vec![Some("20".into())]],
        "the full backup alone is the point-in-time it was taken"
    );

    // A gap in the chain (apply only the second log) is rejected (4305), and
    // the partial destination is removed so a retry can reuse the path.
    assert!(
        Storage::restore_full_with_logs(&restored_gap, &bak, &[trn2.clone()], None).is_err(),
        "a log-chain gap is rejected"
    );
    assert!(
        !restored_gap.exists(),
        "the partial destination is cleaned up on error"
    );
    Storage::restore_full_with_logs(&restored_gap, &bak, &[trn1.clone(), trn2.clone()], None)
        .expect("retry with the full chain to the same path succeeds after cleanup");

    drop(engine2);
    drop(engine3);
    for p in [
        src,
        bak,
        trn1,
        trn2,
        restored,
        restored_full_only,
        restored_gap,
    ] {
        let _ = std::fs::remove_file(p);
    }
}
#[test]
fn point_in_time_restore_stops_at_a_commit_timestamp() {
    use crate::storage_layout::WAL_ENTRY_TYPE_REL;
    use crate::wal::records::{REL_KIND_TXN_COMMIT, RelRecord};

    let src = unique_temp_path("pitr-src");
    let bak = unique_temp_path("pitr-bak");
    let restored = unique_temp_path("pitr-restored");

    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    // Three autocommitted inserts, spaced so their commit records land in
    // distinct milliseconds; the sleeps make the timestamps separable.
    engine.execute("INSERT INTO t VALUES (1)").expect("ins 1");
    std::thread::sleep(std::time::Duration::from_millis(15));
    engine.execute("INSERT INTO t VALUES (2)").expect("ins 2");
    std::thread::sleep(std::time::Duration::from_millis(15));
    engine.execute("INSERT INTO t VALUES (3)").expect("ins 3");
    // The online full backup captures all three commits in its embedded log.
    engine.storage().backup_full(&bak).expect("full backup");
    drop(engine);

    // Reopen the source so recovery's ring scan fills the replay cache, then
    // read the commit timestamps in LSN order: create, insert-1, insert-2,
    // insert-3. Stopping at insert-1's timestamp must keep row 1 (ts == stop
    // is not "after") and undo rows 2 and 3 (ts > stop).
    let storage = Storage::open(src.clone()).expect("reopen src");
    let mut commit_ts: Vec<u64> = Vec::new();
    for r in storage.replay_wal_entries().expect("replay") {
        if r.entry_type != WAL_ENTRY_TYPE_REL {
            continue;
        }
        let rec = RelRecord::decode(&r.payload).expect("decode");
        if let Some(ts) = rec.commit_timestamp_millis() {
            commit_ts.push(ts);
        }
    }
    drop(storage);
    assert!(
        commit_ts.len() >= 4,
        "create + three inserts commit; got {commit_ts:?}"
    );
    let stop_at = commit_ts[1]; // insert-1's commit timestamp
    assert!(
        commit_ts[2] > stop_at,
        "insert-2 must commit strictly after insert-1 for a clean stop point: {commit_ts:?}"
    );

    // Point-in-time restore of the full backup, stopping after insert-1.
    Storage::restore_full_with_logs(&restored, &bak, &[], Some(stop_at)).expect("pitr restore");
    let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&engine2, "SELECT id FROM t ORDER BY id").1,
        vec![vec![Some("1".into())]],
        "only the commit at-or-before the stop point survives"
    );
    drop(engine2);

    // The undo persists: a plain reopen (no stop point) still shows only
    // row 1 — the losers were rolled back durably via CLRs, not re-derived
    // from stop_at.
    let engine3 = Engine::new(Storage::open(restored.clone()).expect("reopen")).expect("engine");
    assert_eq!(
        sql_rows(&engine3, "SELECT id FROM t ORDER BY id").1,
        vec![vec![Some("1".into())]],
        "point-in-time state survives a normal restart"
    );
    drop(engine3);

    for p in [src, bak, restored] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn restore_inspect_verbs_read_a_backup_without_restoring() {
    let src = unique_temp_path("restinspect-src");
    let bak = unique_temp_path("restinspect-bak");

    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=10 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    let baklit = bak.to_str().unwrap().replace('\'', "''");
    assert!(
        sql(
            &engine,
            &format!("BACKUP DATABASE truthdb TO DISK = '{baklit}'")
        )["error"]
            .is_null()
    );

    // HEADERONLY: exactly one metadata row; a full backup is BackupType 1.
    let (cols, rows) = sql_rows(
        &engine,
        &format!("RESTORE HEADERONLY FROM DISK = '{baklit}'"),
    );
    assert_eq!(rows.len(), 1, "one header row");
    assert!(cols.contains(&"BackupType".to_string()));
    assert!(cols.contains(&"Checksum".to_string()));
    let col = |name: &str| rows[0][cols.iter().position(|c| c == name).unwrap()].clone();
    assert_eq!(col("BackupType"), Some("1".to_string()), "full backup");
    assert_eq!(col("FormatVersion"), Some("2".to_string()));
    assert_eq!(col("PageSize"), Some("4096".to_string()));

    // FILELISTONLY: a data row ('D') and a log row ('L').
    let (fcols, frows) = sql_rows(
        &engine,
        &format!("RESTORE FILELISTONLY FROM DISK = '{baklit}'"),
    );
    assert_eq!(fcols, vec!["LogicalName", "Type", "Size"]);
    let types: Vec<Option<String>> = frows.iter().map(|r| r[1].clone()).collect();
    assert_eq!(types, vec![Some("D".to_string()), Some("L".to_string())]);

    // VERIFYONLY: a valid backup verifies with no error and opens no rowset.
    let env = sql(
        &engine,
        &format!("RESTORE VERIFYONLY FROM DISK = '{baklit}'"),
    );
    assert!(env["error"].is_null(), "valid backup verifies: {env}");

    // RESTORE DATABASE is offline-only: online it errors (3101).
    assert_eq!(
        sql_error_number(
            &engine,
            &format!("RESTORE DATABASE truthdb FROM DISK = '{baklit}'")
        ),
        3101
    );

    // Inside a transaction, restore is refused (3021), like BACKUP.
    assert_eq!(
        sql_error_number(
            &engine,
            &format!("BEGIN TRANSACTION; RESTORE VERIFYONLY FROM DISK = '{baklit}'")
        ),
        3021
    );
    drop(engine);

    for p in [src, bak] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn restore_verifyonly_rejects_a_corrupt_or_missing_backup() {
    let src = unique_temp_path("restverify-src");
    let bak = unique_temp_path("restverify-bak");
    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    let baklit = bak.to_str().unwrap().replace('\'', "''");
    assert!(
        sql(
            &engine,
            &format!("BACKUP DATABASE truthdb TO DISK = '{baklit}'")
        )["error"]
            .is_null()
    );

    let pristine = std::fs::read(&bak).expect("read bak");
    let verify = |bytes: &[u8]| {
        std::fs::write(&bak, bytes).expect("write");
        sql_error_number(
            &engine,
            &format!("RESTORE VERIFYONLY FROM DISK = '{baklit}'"),
        )
    };

    // Flip a payload byte mid-file: it no longer matches its xxh64, so
    // VERIFYONLY reports the restore terminating abnormally (3013).
    let mut payload_flip = pristine.clone();
    let mid = payload_flip.len() / 2;
    payload_flip[mid] ^= 0xFF;
    assert_eq!(verify(&payload_flip), 3013, "a flipped payload byte");

    // Corrupt the header block's length field (its high byte, outside the
    // checksum): recovery must report 3013, not allocate ~u64::MAX and crash.
    let mut len_flip = pristine.clone();
    len_flip[19] = 0xFF;
    assert_eq!(
        verify(&len_flip),
        3013,
        "a corrupt block length is not a crash"
    );

    // A missing file errors cleanly, not a panic.
    assert_eq!(
        sql_error_number(
            &engine,
            "RESTORE VERIFYONLY FROM DISK = '/nonexistent/nope.bak'"
        ),
        3013
    );
    drop(engine);
    for p in [src, bak] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn backup_under_concurrent_write_load_restores_to_a_consistent_prefix() {
    use std::sync::Arc;
    let src = unique_temp_path("bul-src");
    let bak = unique_temp_path("bul-bak");
    let restored = unique_temp_path("bul-restored");

    let engine = Arc::new(new_engine(&src));
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    // A baseline committed BEFORE the backup starts: the restore must contain
    // at least these 20 rows.
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i})"))
            .expect("baseline insert");
    }

    // A writer thread commits ids 21..=80 while the backup runs, so the fuzzy
    // page copy interleaves with live commits (online backup under load). The
    // count is kept modest so the test stays short and does not hold io_uring
    // resources long enough to pressure other tests running in parallel.
    let writer = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || {
            for i in 21..=80 {
                engine
                    .execute(&format!("INSERT INTO t VALUES ({i})"))
                    .expect("concurrent insert");
            }
        })
    };
    engine
        .storage()
        .backup_full(&bak)
        .expect("online backup under write load");
    writer.join().expect("writer thread");
    drop(engine);

    // The restore recovers to a single consistent LSN (backup_end). Commits
    // are serialized in id order, so the restored ids must be a CONTIGUOUS
    // prefix 1..=k — a gap would mean a torn page, an id past k an
    // uncommitted write leaked in. k lies between the 20 baseline rows and
    // the writer's max (80).
    Storage::restore_full(&restored, &bak).expect("restore");
    let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
    let (_, rows) = sql_rows(&engine2, "SELECT id FROM t ORDER BY id");
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_ref().unwrap().parse().unwrap())
        .collect();
    assert!(
        (20..=80).contains(&ids.len()),
        "restored count is between the baseline and the writer's max, got {}",
        ids.len()
    );
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(
            *id,
            i as i64 + 1,
            "restored ids are a contiguous prefix (no gaps, no torn/phantom rows): {ids:?}"
        );
    }
    drop(engine2);
    for p in [src, bak, restored] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn a_failed_backup_leaves_the_database_writable_and_backuppable() {
    let src = unique_temp_path("killmid-src");
    let good = unique_temp_path("killmid-good");
    let restored = unique_temp_path("killmid-restored");
    let engine = new_engine(&src);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");

    // A backup to a nonexistent directory fails mid-flight: the hold is armed
    // (begin_backup succeeded), then write_backup's File::create errors. The
    // RAII hold guard must still release, or WAL truncation freezes and writes
    // eventually wedge.
    assert_eq!(
        sql_error_number(
            &engine,
            "BACKUP DATABASE truthdb TO DISK = '/nonexistent-truthdb-dir/b.bak'"
        ),
        3013
    );

    // The database is unharmed: writes still work...
    engine
        .execute("INSERT INTO t VALUES (2)")
        .expect("insert after a failed backup");
    // ...and a fresh backup to a good path succeeds — which it could not if
    // the failed backup's hold or single-flight guard were still set.
    let goodlit = good.to_str().unwrap().replace('\'', "''");
    assert!(
        sql(
            &engine,
            &format!("BACKUP DATABASE truthdb TO DISK = '{goodlit}'")
        )["error"]
            .is_null()
    );
    drop(engine);

    // And that good backup restores the surviving rows.
    Storage::restore_full(&restored, &good).expect("restore");
    let engine2 = Engine::new(Storage::open(restored.clone()).expect("open")).expect("engine");
    assert_eq!(
        sql_rows(&engine2, "SELECT id FROM t ORDER BY id").1,
        vec![vec![Some("1".into())], vec![Some("2".into())]],
        "the post-failure database backs up and restores intact"
    );
    drop(engine2);
    for p in [src, good, restored] {
        let _ = std::fs::remove_file(p);
    }
}
#[test]
fn backup_log_requires_the_full_recovery_model() {
    let path = unique_temp_path("backuplog-simple");
    let engine = new_engine(&path);
    // SIMPLE (the default): BACKUP LOG is 4208.
    assert_eq!(
        sql_error_number(
            &engine,
            "BACKUP LOG truthdb TO DISK = '/tmp/truthdb-never.trn'"
        ),
        4208
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn full_recovery_holds_the_log_until_backup_log() {
    let path = unique_temp_path("backuplog-hold");
    let trn = unique_temp_path("backuplog-hold-trn");
    let engine = new_engine(&path);
    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("full");
    let marker = engine.storage().last_log_backup_lsn();
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    for i in 1..=50 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .expect("insert");
    }
    // A checkpoint cannot truncate past the log-backup floor under FULL.
    engine
        .storage()
        .write_checkpoint(b"cp", 1, 2, 1)
        .expect("checkpoint");
    assert!(
        engine.storage().wal_head() <= marker,
        "FULL pins the head at the log-backup floor"
    );

    // BACKUP LOG advances the floor, so a later checkpoint reclaims the log.
    let lit = trn.to_str().unwrap().replace('\'', "''");
    assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit}'"))["error"].is_null());
    assert!(engine.storage().last_log_backup_lsn() > marker);
    engine
        .storage()
        .write_checkpoint(b"cp2", 2, 3, 2)
        .expect("checkpoint");
    assert!(
        engine.storage().wal_head() > marker,
        "after BACKUP LOG the checkpoint reclaims past the old floor"
    );
    drop(engine);
    for p in [path, trn] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn log_backup_marker_and_hold_survive_reopen() {
    let path = unique_temp_path("backuplog-reopen");
    let trn = unique_temp_path("backuplog-reopen-trn");
    let engine = new_engine(&path);
    engine
        .execute("ALTER DATABASE CURRENT SET RECOVERY FULL")
        .expect("full");
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .expect("create");
    engine.execute("INSERT INTO t VALUES (1)").expect("insert");
    let lit = trn.to_str().unwrap().replace('\'', "''");
    assert!(sql(&engine, &format!("BACKUP LOG truthdb TO DISK = '{lit}'"))["error"].is_null());
    let marker = engine.storage().last_log_backup_lsn();
    drop(engine);

    let engine2 = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("engine");
    assert_eq!(
        engine2.storage().last_log_backup_lsn(),
        marker,
        "the marker persisted"
    );
    assert_eq!(
        engine2.storage().log_backup_hold(),
        Some(marker),
        "the hold re-registered on open"
    );
    assert_eq!(
        sql_rows(&engine2, "SELECT recovery_model_desc FROM sys.databases").1,
        vec![vec![Some("FULL".into())]]
    );
    drop(engine2);
    for p in [path, trn] {
        let _ = std::fs::remove_file(p);
    }
}
