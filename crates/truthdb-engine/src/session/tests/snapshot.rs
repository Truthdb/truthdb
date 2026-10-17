use super::*;

#[tokio::test]
async fn snapshot_reads_are_repeatable_and_block_nobody() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![10],
        "first access establishes the view"
    );
    // A writer neither blocks the snapshot reader nor is blocked by it.
    let reply = must_not_block(&h, a, "UPDATE t SET v = 99 WHERE id = 1").await;
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![10],
        "the committed 99 postdates the transaction's snapshot — repeatable read"
    );
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![99],
        "a new transaction sees the new state"
    );
}

#[tokio::test]
async fn snapshot_autocommit_statements_take_fresh_snapshots() {
    let (h, a, b) = si_harness().await;
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle
        .run_batch(a, "UPDATE t SET v = 99 WHERE id = 1".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![99],
        "outside a transaction each statement is its own snapshot"
    );
}

#[tokio::test]
async fn snapshot_update_conflict_is_3960_and_rolls_the_transaction_back() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    // A commits a change B's snapshot cannot see...
    h.handle
        .run_batch(a, "UPDATE t SET v = 99 WHERE id = 1".into())
        .await
        .unwrap();
    // ...so B writing the same row is the classic update conflict.
    let reply = h
        .handle
        .run_batch(b, "UPDATE t SET v = 100 WHERE id = 1".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3960),
        "{:?}",
        reply.outcome.error
    );
    // SQL Server ABORTS the transaction (not merely dooms it): a COMMIT
    // now has nothing to commit.
    let reply = h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), Some(3902), "the transaction is gone");
    // The conflicting write never landed; A's did.
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![99]);
}

#[tokio::test]
async fn snapshot_delete_of_a_row_deleted_since_the_snapshot_is_3960() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 2").await;
    assert_eq!(values(&reply), vec![20]);
    h.handle
        .run_batch(a, "DELETE FROM t WHERE id = 2".into())
        .await
        .unwrap();
    // The row is physically gone, but B's snapshot still sees it — and
    // targeting it must conflict, not silently affect zero rows.
    let reply = h
        .handle
        .run_batch(b, "DELETE FROM t WHERE id = 2".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3960),
        "{:?}",
        reply.outcome.error
    );
}

#[tokio::test]
async fn snapshot_dml_targets_snapshot_rows_not_current_ones() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    // A moves the row INTO the predicate's range after B's snapshot.
    h.handle
        .run_batch(a, "UPDATE t SET v = 77 WHERE id = 1".into())
        .await
        .unwrap();
    // B's WHERE v = 77 matches the CURRENT row but not the snapshot's
    // version: nothing is targeted, no conflict — current-based targeting
    // would wrongly update it.
    let reply = h
        .handle
        .run_batch(b, "UPDATE t SET v = 1000 WHERE v = 77".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    match reply.outcome.results.as_slice() {
        [StatementResult::RowsAffected(n)] => {
            assert_eq!(*n, 0, "the snapshot's rows decide targeting")
        }
        other => panic!("expected RowsAffected, got {other:?}"),
    }
    h.handle.run_batch(b, "ROLLBACK".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![77], "A's committed value is untouched");
}

#[tokio::test]
async fn snapshot_without_allow_option_is_3952_at_access() {
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(
            a,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY); INSERT INTO t VALUES (1);".into(),
        )
        .await
        .unwrap();
    // The SET itself succeeds (SQL Server defers the check to access).
    let reply = h
        .handle
        .run_batch(a, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3952),
        "{:?}",
        reply.outcome.error
    );
    // Inside a transaction the failure dooms it: COMMIT is refused until
    // the rollback.
    h.handle.run_batch(a, "BEGIN TRAN".into()).await.unwrap();
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), Some(3952));
    let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3930),
        "doomed: only ROLLBACK is allowed"
    );
    let reply = h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
}

#[tokio::test]
async fn snapshot_schema_change_since_the_snapshot_is_3961() {
    let (h, a, b) = si_harness().await;
    h.handle
        .run_batch(a, "CREATE TABLE other (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    // The ALTER re-encodes every row of t; B's images predate it.
    h.handle
        .run_batch(a, "ALTER TABLE t ADD extra INT DEFAULT 5".into())
        .await
        .unwrap();
    let reply = h
        .handle
        .run_batch(b, "SELECT v FROM t WHERE id = 1".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3961),
        "{:?}",
        reply.outcome.error
    );
    // 3961 fails the statement, not the transaction: other tables still
    // read, and the transaction can end normally.
    let reply = must_not_block(&h, b, "SELECT id FROM other").await;
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
}

#[tokio::test]
async fn snapshot_inserts_do_not_conflict_and_stay_isolated() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(got, vec![1, 2]);
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (3, 30)".into())
        .await
        .unwrap();
    // B's own insert succeeds (insert-insert is a key collision question,
    // not a version conflict) and B still does not see A's row.
    let reply = must_not_block(&h, b, "INSERT INTO t VALUES (4, 40)").await;
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(
        got,
        vec![1, 2, 4],
        "own insert visible, A's post-snapshot one not"
    );
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT id FROM t").await;
    let mut got = values(&reply);
    got.sort();
    assert_eq!(got, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn snapshot_rekeyed_row_conflicts_on_old_key_and_misses_on_new_key() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 2").await;
    assert_eq!(values(&reply), vec![20]);
    // A re-keys row 2 -> 5 after B's snapshot.
    let reply = h
        .handle
        .run_batch(a, "UPDATE t SET id = 5 WHERE id = 2".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    // Targeting by the NEW key: the snapshot has no row with id = 5.
    let reply = h
        .handle
        .run_batch(b, "DELETE FROM t WHERE id = 5".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    match reply.outcome.results.as_slice() {
        [StatementResult::RowsAffected(n)] => assert_eq!(*n, 0, "snapshot has no id = 5"),
        other => panic!("expected RowsAffected, got {other:?}"),
    }
    // Targeting by the OLD key: the snapshot row exists but its current
    // state was produced by an invisible writer - update conflict.
    let reply = h
        .handle
        .run_batch(b, "UPDATE t SET v = 200 WHERE id = 2".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3960),
        "{:?}",
        reply.outcome.error
    );
    // The 3960 rolled B back; the current row was never touched by the
    // no-op DELETE or the conflicting UPDATE.
    let reply = must_not_block(&h, a, "SELECT v FROM t WHERE id = 5").await;
    assert_eq!(values(&reply), vec![20], "A's re-keyed row survived");
    let reply = must_not_block(&h, a, "SELECT COUNT(*) FROM t").await;
    assert_eq!(values(&reply), vec![2]);
}

#[tokio::test]
async fn snapshot_reads_merge_through_an_index_created_after_the_snapshot() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    // Post-snapshot: an index on v, then an update that moves row 1's
    // entry from 10 to 99 (the new index only ever contains 99).
    let reply = h
        .handle
        .run_batch(a, "CREATE INDEX ix_v ON t (v)".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = h
        .handle
        .run_batch(a, "UPDATE t SET v = 99 WHERE id = 1".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    // Seek for the snapshot's value: nothing physical in range, so the
    // row must come from its chain image.
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 10").await;
    assert_eq!(
        values(&reply),
        vec![1],
        "the snapshot's v = 10 row must be found through the new index"
    );
    // Seek for the current value: the physical entry resolves to an
    // image with v = 10, which the predicate must filter out.
    let reply = must_not_block(&h, b, "SELECT id FROM t WHERE v = 99").await;
    assert_eq!(
        values(&reply),
        Vec::<i64>::new(),
        "the current v = 99 row postdates the snapshot"
    );
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
}

#[tokio::test]
async fn exec_literal_shares_the_transaction_snapshot() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    // First data access happens INSIDE the EXEC: it must establish the
    // transaction's snapshot.
    let reply = must_not_block(&h, b, "EXEC sp_executesql N'SELECT v FROM t WHERE id = 1'").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle
        .run_batch(a, "UPDATE t SET v = 99 WHERE id = 1".into())
        .await
        .unwrap();
    // Both a plain statement and another EXEC read the SAME view.
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![10],
        "captured inside EXEC, reused outside"
    );
    let reply = must_not_block(&h, b, "EXEC sp_executesql N'SELECT v FROM t WHERE id = 1'").await;
    assert_eq!(values(&reply), vec![10], "reused inside EXEC");
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
}

#[tokio::test]
async fn exec_literal_under_snapshot_without_allow_is_3952() {
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(
            a,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY); INSERT INTO t VALUES (1);".into(),
        )
        .await
        .unwrap();
    h.handle
        .run_batch(a, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT".into())
        .await
        .unwrap();
    h.handle.run_batch(a, "BEGIN TRAN".into()).await.unwrap();
    let reply = h
        .handle
        .run_batch(a, "EXEC sp_executesql N'SELECT id FROM t'".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3952),
        "{:?}",
        reply.outcome.error
    );
    let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), Some(3930), "doomed");
    let reply = h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    // The session is usable again (autocommit statements still 3952
    // until the level changes, but the SET path works).
    h.handle
        .run_batch(a, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, a, "SELECT id FROM t").await;
    assert_eq!(values(&reply), vec![1]);
}

#[tokio::test]
async fn nested_begin_and_savepoint_rollback_keep_the_snapshot() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle
        .run_batch(a, "UPDATE t SET v = 99 WHERE id = 1".into())
        .await
        .unwrap();
    // Nested BEGIN + inner COMMIT: trancount 2 -> 1, transaction alive.
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![10],
        "inner COMMIT must not drop the view"
    );
    // Savepoint rollback: own write undone, transaction and view alive.
    h.handle.run_batch(b, "SAVE TRAN sp1".into()).await.unwrap();
    let reply = h
        .handle
        .run_batch(b, "UPDATE t SET v = 500 WHERE id = 2".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    h.handle
        .run_batch(b, "ROLLBACK TRAN sp1".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 2").await;
    assert_eq!(
        values(&reply),
        vec![20],
        "own write rolled back to the savepoint"
    );
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(
        values(&reply),
        vec![10],
        "the view survives the savepoint rollback"
    );
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![99]);
}

#[tokio::test]
async fn alter_options_refuse_while_a_snapshot_transaction_lives() {
    // A SNAPSHOT transaction idle between batches holds no locks, so the
    // ALTER's Database X grants — but flipping the options under its
    // registered snapshot would reset the store its reads depend on
    // (the review's HIGH: a debug panic, silent wrong data in release).
    // The ALTER refuses with 5061 instead; SQL Server would wait.
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10], "snapshot registered");
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        h.handle.run_batch(
            a,
            "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF".into(),
        ),
    )
    .await
    .expect("the ALTER must not hang")
    .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(5061),
        "refused while the snapshot lives: {:?}",
        reply.outcome.error
    );
    // The transaction is untouched — still repeatable.
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle.run_batch(b, "COMMIT".into()).await.unwrap();
    // Snapshot released: the retry succeeds, and SNAPSHOT access is gone.
    let reply = h
        .handle
        .run_batch(
            a,
            "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION OFF".into(),
        )
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = h
        .handle
        .run_batch(b, "SELECT v FROM t WHERE id = 1".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), Some(3952));
}

#[tokio::test]
async fn autocommit_3952_leaves_the_session_usable() {
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(
            a,
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY); INSERT INTO t VALUES (1);".into(),
        )
        .await
        .unwrap();
    h.handle
        .run_batch(a, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT".into())
        .await
        .unwrap();
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), Some(3952));
    // A table-free SELECT is not data access: no 3952, and it does not
    // establish a snapshot (SQL Server defers both to the first read of
    // an actual object) — the review's finding 3.
    let reply = h.handle.run_batch(a, "SELECT 1".into()).await.unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    h.handle
        .run_batch(a, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED".into())
        .await
        .unwrap();
    let reply = must_not_block(&h, a, "SELECT id FROM t").await;
    assert_eq!(values(&reply), vec![1]);
}

#[tokio::test]
async fn snapshot_drop_and_recreate_between_batches_is_3961() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    for sql in [
        "DROP TABLE t",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (7, 70)",
    ] {
        let reply = h.handle.run_batch(a, sql.into()).await.unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "{sql}: {:?}",
            reply.outcome.error
        );
    }
    let reply = h
        .handle
        .run_batch(b, "SELECT v FROM t".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3961),
        "SQL Server: DDL since the snapshot is 3961, got {:?} / rows {:?}",
        reply.outcome.error,
        reply.outcome.results
    );
    h.handle.run_batch(b, "ROLLBACK".into()).await.unwrap();
}

#[tokio::test]
async fn snapshot_heap_dml_conflicts_via_rid_identities() {
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
        "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON",
        "CREATE TABLE hp (v INT)",
        "INSERT INTO hp VALUES (10), (20)",
    ] {
        let reply = h.handle.run_batch(a, sql.into()).await.unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "{sql}: {:?}",
            reply.outcome.error
        );
    }
    h.handle
        .run_batch(b, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT".into())
        .await
        .unwrap();
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM hp WHERE v = 10").await;
    assert_eq!(values(&reply), vec![10]);
    // A deletes the row B's snapshot still sees.
    let reply = h
        .handle
        .run_batch(a, "DELETE FROM hp WHERE v = 10".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    // Targeting it via its synthesized RID locator is the conflict.
    let reply = h
        .handle
        .run_batch(b, "DELETE FROM hp WHERE v = 10".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3960),
        "{:?}",
        reply.outcome.error
    );
    // B was rolled back; the surviving row is intact.
    let reply = must_not_block(&h, b, "SELECT v FROM hp").await;
    assert_eq!(values(&reply), vec![20]);
}

#[tokio::test]
async fn snapshot_full_table_delete_conflicts_when_any_row_changed() {
    let (h, a, b) = si_harness().await;
    h.handle.run_batch(b, "BEGIN TRAN".into()).await.unwrap();
    let reply = must_not_block(&h, b, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(values(&reply), vec![10]);
    h.handle
        .run_batch(a, "UPDATE t SET v = 99 WHERE id = 2".into())
        .await
        .unwrap();
    let reply = h.handle.run_batch(b, "DELETE FROM t".into()).await.unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3960),
        "{:?}",
        reply.outcome.error
    );
}
