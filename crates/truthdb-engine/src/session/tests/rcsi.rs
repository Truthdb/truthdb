use super::*;

#[tokio::test]
async fn rcsi_select_reads_committed_state_without_waiting_for_the_writer() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 99 WHERE id = 1;".into())
        .await
        .unwrap();
    // B reads the pre-update value while A's transaction holds its X
    // lock: the value is the proof there was no wait (a lock-based
    // reader could only return 99, after A commits).
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![99]);
}

#[tokio::test]
async fn without_rcsi_the_same_reader_waits_for_the_commit() {
    // The control for the test above — and the teeth of the
    // `rcsi_enabled` gate: with the option off, the reader parks on the
    // writer's lock and can only ever see the committed value.
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(
            a,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT); \
             INSERT INTO t VALUES (1, 10);"
                .into(),
        )
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 99 WHERE id = 1;".into())
        .await
        .unwrap();
    let reader = {
        let handle = h.handle.clone();
        tokio::spawn(async move { handle.run_batch(b, "SELECT v FROM t".into()).await })
    };
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = reader.await.unwrap().unwrap();
    assert_eq!(
        values(&reply),
        vec![99],
        "a lock-based reader sees only the commit"
    );
}

#[tokio::test]
async fn rcsi_reader_sees_a_deleted_row_until_the_delete_commits() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(a, "BEGIN TRAN; DELETE FROM t WHERE id = 1;".into())
        .await
        .unwrap();
    // The row is physically gone from the page; the scan's merge pass
    // serves it from its version image.
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(got, vec![1, 2]);
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    assert_eq!(values(&reply), vec![2]);
}

#[tokio::test]
async fn rcsi_insert_is_invisible_until_it_commits() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (3, 30);".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(
        got,
        vec![1, 2],
        "an uncommitted insert does not exist for the snapshot"
    );
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

#[tokio::test]
async fn rcsi_rekey_update_shows_the_old_row_until_commit() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET id = 9 WHERE id = 1;".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(
        got,
        vec![1, 2],
        "the re-key is invisible: old identity present, new absent"
    );
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(got, vec![2, 9]);
}

#[tokio::test]
async fn rcsi_index_seek_serves_the_snapshot_row() {
    let (h, a, b) = rcsi_harness().await;
    // Enough rows that the planner takes the seek (tiny tables demote to
    // scans), plus an index on v.
    let mut fill = String::from("INSERT INTO t VALUES ");
    for i in 3..=25 {
        fill.push_str(&format!("({}, {}),", i, i * 100));
    }
    fill.pop();
    h.handle.run_batch(a, fill).await.unwrap();
    h.handle
        .run_batch(a, "CREATE INDEX ix_v ON t (v)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 4242 WHERE id = 1;".into())
        .await
        .unwrap();
    // The uncommitted update moved the index entry from 10 to 4242. The
    // snapshot must still find id=1 under v=10 (its entry is gone — the
    // merge pass restores it) and must not find it under v=4242 (the
    // entry exists, but the visible image fails the predicate).
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 10").await;
    assert_eq!(values(&reply), vec![1]);
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 4242").await;
    assert_eq!(values(&reply), Vec::<i64>::new());
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 4242").await;
    assert_eq!(values(&reply), vec![1]);
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 10").await;
    assert_eq!(values(&reply), Vec::<i64>::new());
}

#[tokio::test]
async fn rcsi_reader_sees_its_own_uncommitted_writes() {
    let (h, a, _b) = rcsi_harness().await;
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 55 WHERE id = 1;".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, a, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![55],
        "a session always sees its own writes"
    );
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    let reply = must_not_block(&h, a, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
}

#[tokio::test]
async fn rcsi_same_batch_write_then_read_sees_the_write() {
    // The durable-prefix rule must not hide the session's own
    // just-committed statement from a later statement in the same batch
    // (the flush-before-capture path).
    let (h, a, _b) = rcsi_harness().await;
    let reply = must_not_block(
        &h,
        a,
        "UPDATE t SET v = 11 WHERE id = 1; SELECT v FROM t WHERE id = 1",
    )
    .await;
    assert_eq!(values(&reply), vec![11]);
}

#[tokio::test]
async fn rollback_after_publish_keeps_chains_sound_for_reuse() {
    // A rolled-back insert must unpublish its version entries: the next
    // insert of the same key builds a fresh chain (the debug asserts on
    // chain-head invariants fire here if it does not), and readers see
    // only the committed state.
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (5, 50);".into())
        .await
        .unwrap();
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    let reply = h
        .handle
        .run_batch(a, "INSERT INTO t VALUES (5, 55)".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 5").await;
    assert_eq!(values(&reply), vec![55]);
}

#[tokio::test]
async fn savepoint_rollback_unpublishes_the_rolled_back_statements() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; UPDATE t SET v = 100 WHERE id = 1; \
             SAVE TRANSACTION s; UPDATE t SET v = 200 WHERE id = 2; \
             ROLLBACK TRANSACTION s;"
                .into(),
        )
        .await
        .unwrap();
    // While A is open, B sees both originals (100 and 200 are invisible
    // either way — the discriminator is after the commit).
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 2").await;
    assert_eq!(values(&reply), vec![20]);
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![100], "the kept update committed");
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 2").await;
    assert_eq!(
        values(&reply),
        vec![20],
        "the savepoint-rolled-back update did not"
    );
}

#[tokio::test]
async fn rcsi_assignment_select_sees_the_same_batch_commit() {
    // An assignment SELECT opens no rowset, so it skips `run_block`'s
    // pre-rowset durability flush — the snapshot capture must flush on
    // its own or the durable-prefix rule hides the batch's own
    // just-committed UPDATE from it.
    let (h, a, _b) = rcsi_harness().await;
    let reply = must_not_block(
        &h,
        a,
        "UPDATE t SET v = 42 WHERE id = 1; \
         DECLARE @x INT; SELECT @x = v FROM t WHERE id = 1; SELECT @x",
    )
    .await;
    assert_eq!(values(&reply), vec![42]);
}

#[tokio::test]
async fn repeatable_read_still_blocks_under_rcsi() {
    // RCSI changes READ COMMITTED only. A REPEATABLE READ reader keeps
    // its Table S request and parks behind the writer.
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(b, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 99 WHERE id = 1;".into())
        .await
        .unwrap();
    let reader = {
        let handle = h.handle.clone();
        tokio::spawn(async move {
            handle
                .run_batch(b, "SELECT v FROM t WHERE id = 1".into())
                .await
        })
    };
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = reader.await.unwrap().unwrap();
    assert_eq!(
        values(&reply),
        vec![99],
        "an RR reader sees only committed state, by waiting"
    );
}

#[tokio::test]
async fn rcsi_heap_table_serves_snapshot_rows() {
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE hp (v INT)",
        "INSERT INTO hp VALUES (10), (20)",
    ] {
        h.handle.run_batch(a, sql.into()).await.unwrap();
    }
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; UPDATE hp SET v = 99 WHERE v = 10; DELETE FROM hp WHERE v = 20;".into(),
        )
        .await
        .unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM hp").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(
        got,
        vec![10, 20],
        "heap identities (RIDs) version like tree keys"
    );
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM hp").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(got, vec![10, 20]);
}

#[tokio::test]
async fn alter_database_rejects_wrong_names_and_open_transactions() {
    let h = start(Duration::from_secs(30));
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let reply = h
        .handle
        .run_batch(
            s,
            "ALTER DATABASE nosuch SET READ_COMMITTED_SNAPSHOT ON".into(),
        )
        .await
        .unwrap();
    assert_eq!(error_number(&reply), Some(911));
    let reply = h
        .handle
        .run_batch(
            s,
            "BEGIN TRAN; ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;".into(),
        )
        .await
        .unwrap();
    assert_eq!(error_number(&reply), Some(226));
    h.handle.run_batch(s, "ROLLBACK".into()).await.unwrap();
    // The session's own database by name works, and the option sticks.
    let reply = h
        .handle
        .run_batch(
            s,
            "ALTER DATABASE truthdb SET READ_COMMITTED_SNAPSHOT ON".into(),
        )
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
}

#[tokio::test]
async fn serializable_exec_literal_select_must_not_dirty_read_under_rcsi() {
    // A SERIALIZABLE session EXECs a literal SELECT while another
    // session's transaction holds an uncommitted UPDATE. The reader must
    // wait for the writer (SERIALIZABLE reads lock) and, after the
    // writer's ROLLBACK, see the original value 10. A dirty read
    // returns 99 — a value that never committed.
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(b, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 99 WHERE id = 1;".into())
        .await
        .unwrap();
    let reader = {
        let handle = h.handle.clone();
        tokio::spawn(async move {
            handle
                .run_batch(
                    b,
                    "EXEC sp_executesql N'SELECT v FROM t WHERE id = 1'".into(),
                )
                .await
        })
    };
    // Give the reader time to (incorrectly) run without blocking.
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    let reply = reader.await.unwrap().unwrap();
    assert_eq!(
        values(&reply),
        vec![10],
        "a SERIALIZABLE reader must never see the uncommitted 99"
    );
}

#[tokio::test]
async fn a_parked_reader_is_reanalyzed_when_rcsi_flips_off() {
    // A reader analyzed while RCSI is ON (Database IS only, no Table S)
    // parks behind a pending ALTER ... OFF; when granted, RCSI is OFF so
    // it executes lock-based — with the stale (versioned) lock set. A
    // concurrent uncommitted writer is then readable: dirty read at
    // READ COMMITTED with RCSI off.
    for round in 0..5 {
        let (h, a, b) = rcsi_harness().await;
        let c = h
            .handle
            .open_session(String::new(), String::new(), 0)
            .await
            .expect("open session")
            .0;
        // Writer A holds Database IX -> the ALTER (Database X) parks.
        h.handle
            .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 55 WHERE id = 2;".into())
            .await
            .unwrap();
        let admin = {
            let handle = h.handle.clone();
            let s = h
                .handle
                .open_session(String::new(), String::new(), 0)
                .await
                .expect("open session")
                .0;
            tokio::spawn(async move {
                handle
                    .run_batch(
                        s,
                        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT OFF".into(),
                    )
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Writer C parks behind the ALTER (fairness on Database).
        let writer = {
            let handle = h.handle.clone();
            tokio::spawn(async move {
                handle
                    .run_batch(c, "BEGIN TRAN; UPDATE t SET v = 777 WHERE id = 1;".into())
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Reader B analyzed NOW (RCSI still on -> no Table S), parks.
        let reader = {
            let handle = h.handle.clone();
            tokio::spawn(async move {
                handle
                    .run_batch(b, "SELECT v FROM t WHERE id = 1".into())
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        // A commits: the ALTER runs and flips RCSI off. B's lock set was
        // analyzed under RCSI-on (no Table S); the grant path must
        // re-analyze it, so B now blocks behind C's open transaction
        // instead of reading its uncommitted 777 lock-free.
        h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
        // C's UPDATE batch completes (transaction stays open, holding
        // its locks); B must still be waiting on them.
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .expect("writer hung")
            .unwrap()
            .unwrap();
        h.handle.run_batch(c, "ROLLBACK".into()).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("reader hung")
            .unwrap()
            .unwrap();
        let seen = values(&reply);
        assert_eq!(
            seen,
            vec![10],
            "round {round}: the reader may only see committed state \
             (777 never committed — seeing it is the dirty read)"
        );
        admin.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn rcsi_insert_after_delete_in_one_txn_serves_the_old_image() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; DELETE FROM t WHERE id = 1; INSERT INTO t VALUES (1, 111);".into(),
        )
        .await
        .unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10], "pre-txn image, not the re-insert");
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; DELETE FROM t WHERE id = 1; INSERT INTO t VALUES (1, 222);".into(),
        )
        .await
        .unwrap();
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![222]);
}

#[tokio::test]
async fn rcsi_aggregates_and_subqueries_read_the_snapshot() {
    let (h, a, b) = rcsi_harness().await;
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; DELETE FROM t WHERE id = 2; UPDATE t SET v = 99 WHERE id = 1;".into(),
        )
        .await
        .unwrap();
    // Aggregate over the collecting path (build_table_source).
    let reply = must_not_block(&h, b, "SELECT COUNT(*) FROM t").await;
    assert_eq!(
        values(&reply),
        vec![2],
        "the uncommitted delete is invisible"
    );
    let reply = must_not_block(&h, b, "SELECT SUM(v) FROM t").await;
    assert_eq!(values(&reply), vec![30], "10 + 20, not 99 + (deleted)");
    // Uncorrelated subquery: reads t under the same statement snapshot.
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = (SELECT MAX(id) FROM t)").await;
    assert_eq!(values(&reply), vec![20], "MAX(id) = 2 in the snapshot");
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT COUNT(*) FROM t").await;
    assert_eq!(values(&reply), vec![1]);
}

#[tokio::test]
async fn rcsi_range_seek_does_not_double_count_a_moved_entry() {
    let (h, a, b) = rcsi_harness().await;
    let mut fill = String::from("INSERT INTO t VALUES ");
    for i in 3..=25 {
        fill.push_str(&format!("({}, {}),", i, i * 100));
    }
    fill.pop();
    h.handle.run_batch(a, fill).await.unwrap();
    h.handle
        .run_batch(a, "CREATE INDEX ix_v ON t (v)".into())
        .await
        .unwrap();
    // Uncommitted: moves id=1's entry from v=10 to v=15 — both inside
    // the seek range below.
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 15 WHERE id = 1;".into())
        .await
        .unwrap();
    let reply = must_not_block(
        &h,
        b,
        "SELECT id FROM t WHERE v >= 5 AND v <= 20 ORDER BY id",
    )
    .await;
    assert_eq!(
        values(&reply),
        vec![1, 2],
        "id=1 exactly once (image v=10), id=2 (v=20); no double count"
    );
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
}

#[tokio::test]
async fn rcsi_covering_seek_serves_a_deleted_row_image() {
    let (h, a, b) = rcsi_harness().await;
    let mut fill = String::from("INSERT INTO t VALUES ");
    for i in 3..=25 {
        fill.push_str(&format!("({}, {}),", i, i * 100));
    }
    fill.pop();
    h.handle.run_batch(a, fill).await.unwrap();
    h.handle
        .run_batch(a, "CREATE INDEX ix_vc ON t (v) INCLUDE (v, id)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; DELETE FROM t WHERE id = 1;".into())
        .await
        .unwrap();
    // The deleted row's index entry is gone; the covering seek must
    // append its image from the version store.
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 10").await;
    assert_eq!(values(&reply), vec![1], "the deleted row's image is served");
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 10").await;
    assert_eq!(values(&reply), Vec::<i64>::new());
}
