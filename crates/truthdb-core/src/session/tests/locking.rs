use super::*;

#[tokio::test]
async fn point_writers_to_different_rows_run_concurrently() {
    // The Stage 12 row-lock win: two transactions updating *different* rows
    // of one table no longer serialize (Table IX + distinct Row X locks).
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
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT); INSERT INTO t VALUES (1,0),(2,0);".into(),
        )
        .await
        .unwrap();

    // A holds Row X on id = 1 inside an open transaction.
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 10 WHERE id = 1;".into())
        .await
        .unwrap();

    // B updates id = 2 — a different row — and must complete without waiting
    // for A's commit.
    let out = tokio::time::timeout(
        Duration::from_secs(3),
        h.handle
            .run_batch(b, "UPDATE t SET v = 20 WHERE id = 2".into()),
    )
    .await
    .expect("a point write to a different row must not block")
    .unwrap();
    assert!(error_number(&out).is_none(), "{:?}", out.outcome.error);

    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
}

#[tokio::test]
async fn point_writers_to_the_same_row_serialize() {
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
            "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT); INSERT INTO t VALUES (1,0);"
                .into(),
        )
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET v = 10 WHERE id = 1;".into())
        .await
        .unwrap();

    // B updates the *same* row (id = 1): it must block on A's Row X.
    let handle_b = h.handle.clone();
    let write = tokio::spawn(async move {
        handle_b
            .run_batch(b, "UPDATE t SET v = 20 WHERE id = 1".into())
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!write.is_finished(), "same-row writer must block");

    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("writer unblocks after commit")
        .unwrap();
    assert!(error_number(&out).is_none(), "{:?}", out.outcome.error);
}

#[tokio::test]
async fn read_uncommitted_sees_uncommitted_rows_without_blocking() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (7);".into())
        .await
        .unwrap();

    // B under READ UNCOMMITTED takes no read lock → dirty-reads A's row.
    h.handle
        .run_batch(b, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED".into())
        .await
        .unwrap();
    let out = tokio::time::timeout(
        Duration::from_secs(2),
        h.handle.run_batch(b, "SELECT id FROM t".into()),
    )
    .await
    .expect("READ UNCOMMITTED must not block")
    .unwrap();
    assert_eq!(ids(&out), vec![7], "dirty read sees the uncommitted row");

    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
}

#[tokio::test]
async fn disconnect_releases_locks_and_wakes_waiter() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
        .await
        .unwrap();

    let handle_b = h.handle.clone();
    let read = tokio::spawn(async move {
        handle_b
            .run_batch(b, "SELECT id FROM t".into())
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!read.is_finished(), "reader blocked by open writer txn");

    // A disconnects mid-transaction: rollback releases the lock, waking B,
    // which now sees an empty table (the insert was undone).
    h.handle.close_session(a);
    let out = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("reader should unblock on disconnect")
        .unwrap();
    assert_eq!(ids(&out), Vec::<i64>::new());
}

#[tokio::test]
async fn deadlock_is_broken_by_timeout_with_1205() {
    // Wide enough that BOTH conflicting batches park (forming the cycle)
    // before any deadline expires: since the 1222 split, an expired
    // waiter with no cycle is reaped as a lock timeout, and a loaded
    // runner delaying the second park past the deadline would otherwise
    // turn this test's deadlock into a 1222.
    let h = start(Duration::from_secs(2));
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

    for stmt in [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (1)",
    ] {
        h.handle.run_batch(a, stmt.into()).await.unwrap();
    }

    // A locks table a; B locks table b (each in its own transaction).
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE a SET id = id".into())
        .await
        .unwrap();
    h.handle
        .run_batch(b, "BEGIN TRAN; UPDATE b SET id = id".into())
        .await
        .unwrap();

    // Now each waits for the other's table → deadlock.
    let ha = h.handle.clone();
    let a_waits = tokio::spawn(async move { ha.run_batch(a, "UPDATE b SET id = id".into()).await });
    let hb = h.handle.clone();
    let b_waits = tokio::spawn(async move { hb.run_batch(b, "UPDATE a SET id = id".into()).await });

    let a_out = tokio::time::timeout(Duration::from_secs(5), a_waits)
        .await
        .expect("a_waits resolved")
        .unwrap()
        .unwrap();
    let b_out = tokio::time::timeout(Duration::from_secs(5), b_waits)
        .await
        .expect("b_waits resolved")
        .unwrap()
        .unwrap();

    // Exactly one is the deadlock victim (1205); the other succeeds.
    let victims = [&a_out, &b_out]
        .iter()
        .filter(|o| error_number(o) == Some(1205))
        .count();
    assert_eq!(victims, 1, "exactly one transaction is the deadlock victim");
}

#[tokio::test]
async fn deadlock_is_broken_by_the_waits_for_graph_not_the_timeout() {
    // A 30 s wait timeout: if the deadlock were only broken by the timeout
    // backstop this would not resolve for 30 s. The waits-for-graph detector
    // must break it the instant the cycle closes, so the whole thing
    // finishes well under the timeout.
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
    for stmt in [
        "CREATE TABLE a (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE b (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (1)",
    ] {
        h.handle.run_batch(a, stmt.into()).await.unwrap();
    }
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE a SET id = id".into())
        .await
        .unwrap();
    h.handle
        .run_batch(b, "BEGIN TRAN; UPDATE b SET id = id".into())
        .await
        .unwrap();

    let ha = h.handle.clone();
    let a_waits = tokio::spawn(async move { ha.run_batch(a, "UPDATE b SET id = id".into()).await });
    let hb = h.handle.clone();
    let b_waits = tokio::spawn(async move { hb.run_batch(b, "UPDATE a SET id = id".into()).await });

    // Both resolve far sooner than the 30 s timeout — proving graph detection.
    let a_out = tokio::time::timeout(Duration::from_secs(3), a_waits)
        .await
        .expect("graph must break the deadlock well under the timeout")
        .unwrap()
        .unwrap();
    let b_out = tokio::time::timeout(Duration::from_secs(3), b_waits)
        .await
        .expect("graph must break the deadlock well under the timeout")
        .unwrap()
        .unwrap();

    let victims = [&a_out, &b_out]
        .iter()
        .filter(|o| error_number(o) == Some(1205))
        .count();
    assert_eq!(victims, 1, "exactly one transaction is the deadlock victim");
}

#[tokio::test]
async fn deadlock_through_a_fifo_yield_is_detected_by_the_graph() {
    // A deadlock whose cycle passes through a FIFO anti-barging yield (not a
    // held-lock conflict): A holds t1; C parks wanting t1+t2; A then wants
    // the *free* t2 but yields to C, which is queued ahead for it. The graph
    // must model that yield edge and break the cycle under the 30 s timeout.
    let h = start(Duration::from_secs(30));
    let a = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    let c = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    for stmt in [
        "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
        "CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY)",
        "INSERT INTO t1 VALUES (1)",
        "INSERT INTO t2 VALUES (1)",
    ] {
        h.handle.run_batch(a, stmt.into()).await.unwrap();
    }
    // A holds X(t1).
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t1 SET id = id".into())
        .await
        .unwrap();
    // C wants t2 then t1 (held by A) → parks, now queued ahead for t2.
    let hc = h.handle.clone();
    let c_waits = tokio::spawn(async move {
        hc.run_batch(
            c,
            "BEGIN TRAN; UPDATE t2 SET id = id; UPDATE t1 SET id = id".into(),
        )
        .await
    });
    // Ensure C is parked before A asks for t2, so A queues behind it.
    tokio::time::sleep(Duration::from_millis(250)).await;
    // A wants the free t2 but yields to C (ahead) → parks → FIFO cycle.
    let ha = h.handle.clone();
    let a_waits =
        tokio::spawn(async move { ha.run_batch(a, "UPDATE t2 SET id = id".into()).await });

    let a_out = tokio::time::timeout(Duration::from_secs(3), a_waits)
        .await
        .expect("graph must break the FIFO deadlock well under the timeout")
        .unwrap()
        .unwrap();
    let c_out = tokio::time::timeout(Duration::from_secs(3), c_waits)
        .await
        .expect("graph must break the FIFO deadlock well under the timeout")
        .unwrap()
        .unwrap();
    let victims = [&a_out, &c_out]
        .iter()
        .filter(|o| error_number(o) == Some(1205))
        .count();
    assert_eq!(victims, 1, "exactly one transaction is the deadlock victim");
}

#[tokio::test]
async fn repeatable_read_holds_shared_lock_and_blocks_a_writer() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (1)".into())
        .await
        .unwrap();

    // A reads under REPEATABLE READ inside a transaction → holds S on t.
    h.handle
        .run_batch(a, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "BEGIN TRAN; SELECT id FROM t;".into())
        .await
        .unwrap();

    // B's write must block on A's retained shared lock.
    let hb = h.handle.clone();
    let write = tokio::spawn(async move {
        hb.run_batch(b, "UPDATE t SET id = id".into())
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !write.is_finished(),
        "REPEATABLE READ keeps the shared lock, blocking the writer"
    );

    // A commits → releases S → B proceeds.
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("writer unblocks after reader commits")
        .unwrap();
    assert!(out.outcome.error.is_none(), "{:?}", out.outcome.error);
}

#[tokio::test]
async fn read_committed_releases_shared_lock_so_a_writer_proceeds() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (1)".into())
        .await
        .unwrap();

    // A reads under READ COMMITTED (the default) inside a transaction; its
    // shared lock is dropped at statement end even though the txn stays open.
    h.handle
        .run_batch(a, "BEGIN TRAN; SELECT id FROM t;".into())
        .await
        .unwrap();

    // B's write is NOT blocked — unlike REPEATABLE READ above.
    let out = tokio::time::timeout(
        Duration::from_secs(2),
        h.handle.run_batch(b, "UPDATE t SET id = id".into()),
    )
    .await
    .expect("READ COMMITTED releases the shared lock, so the writer runs")
    .unwrap();
    assert!(out.outcome.error.is_none(), "{:?}", out.outcome.error);

    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();
}

#[tokio::test]
async fn isolation_escalation_within_a_batch_locks_the_read() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (1)".into())
        .await
        .unwrap();

    // A holds X on t.
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET id = id;".into())
        .await
        .unwrap();

    // B is READ UNCOMMITTED, so a plain read would take no lock and could
    // dirty-read. But B raises the level to SERIALIZABLE in the SAME batch
    // as the read, which must lock the read → it blocks on A's writer.
    h.handle
        .run_batch(b, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED".into())
        .await
        .unwrap();
    let hb = h.handle.clone();
    let read = tokio::spawn(async move {
        hb.run_batch(
            b,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SELECT id FROM t;".into(),
        )
        .await
        .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !read.is_finished(),
        "an escalated read must lock and block on the uncommitted writer"
    );

    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("escalated read unblocks after commit")
        .unwrap();
    assert_eq!(ids(&out), vec![1]);
}

#[tokio::test]
async fn holder_is_not_blocked_by_a_waiter_on_its_own_lock() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (1)".into())
        .await
        .unwrap();

    // A holds X on t via an open transaction.
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET id = id;".into())
        .await
        .unwrap();

    // B blocks on t and parks in the queue.
    let hb = h.handle.clone();
    let b_read =
        tokio::spawn(async move { hb.run_batch(b, "SELECT id FROM t".into()).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!b_read.is_finished(), "B parks on A's lock");

    // A re-touches t in a new batch. Because A already holds the lock, it
    // must NOT yield to the queued waiter B — doing so would deadlock A on
    // its own lock. This completes promptly.
    let again = tokio::time::timeout(
        Duration::from_secs(2),
        h.handle.run_batch(a, "UPDATE t SET id = id".into()),
    )
    .await
    .expect("holder must not self-deadlock on a waiter behind its own lock")
    .unwrap();
    assert!(again.outcome.error.is_none(), "{:?}", again.outcome.error);

    // A commits → B finally proceeds.
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), b_read)
        .await
        .expect("B unblocks after A commits")
        .unwrap();
    assert_eq!(ids(&out), vec![1]);
}

#[tokio::test]
async fn autocommit_reads_run_concurrently() {
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (1)".into())
        .await
        .unwrap();

    // Two shared readers never block each other.
    let out_a = h
        .handle
        .run_batch(a, "SELECT id FROM t".into())
        .await
        .unwrap();
    let out_b = tokio::time::timeout(
        Duration::from_secs(2),
        h.handle.run_batch(b, "SELECT id FROM t".into()),
    )
    .await
    .expect("concurrent shared reads must not block")
    .unwrap();
    assert_eq!(ids(&out_a), vec![1]);
    assert_eq!(ids(&out_b), vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transfers_conserve_the_total() {
    const ACCOUNTS: i64 = 8;
    const TASKS: usize = 16;
    const TRANSFERS: usize = 25;
    const INITIAL: i64 = 1000;

    let h = start(Duration::from_secs(30));
    let setup = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(
            setup,
            "CREATE TABLE accounts (id INT NOT NULL PRIMARY KEY, balance INT NOT NULL)".into(),
        )
        .await
        .unwrap();
    for id in 1..=ACCOUNTS {
        h.handle
            .run_batch(
                setup,
                format!("INSERT INTO accounts VALUES ({id}, {INITIAL})"),
            )
            .await
            .unwrap();
    }

    let mut tasks = Vec::new();
    for t in 0..TASKS {
        let handle = h.handle.clone();
        tasks.push(tokio::spawn(async move {
            let session = handle
                .open_session(String::new(), String::new(), 0)
                .await
                .expect("open session")
                .0;
            // A deterministic per-task PRNG (an LCG) — reproducible, no dep.
            let mut rng: u64 =
                0x9E37_79B9_7F4A_7C15 ^ (t as u64).wrapping_mul(0x2545_F491_4F6C_DD1D);
            let mut next = move || {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                rng >> 33
            };
            for _ in 0..TRANSFERS {
                let a = (next() % ACCOUNTS as u64) as i64 + 1;
                // Force a distinct counterparty: (a % N) + 1 is never a.
                let b = (a % ACCOUNTS) + 1;
                let amount = (next() % 50) as i64 + 1;
                // One in ten transactions rolls back; conservation must hold
                // either way.
                let close = if next() % 10 == 0 {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                };
                let sql = format!(
                    "BEGIN TRAN; \
                     UPDATE accounts SET balance = balance - {amount} WHERE id = {a}; \
                     UPDATE accounts SET balance = balance + {amount} WHERE id = {b}; \
                     {close};"
                );
                // A deadlock victim (1205) rolls back cleanly; just retry it.
                loop {
                    let reply = handle.run_batch(session, sql.clone()).await.unwrap();
                    match error_number(&reply) {
                        Some(1205) => continue,
                        Some(other) => panic!("unexpected error {other} on transfer"),
                        None => break,
                    }
                }
            }
            handle.close_session(session);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let reply = h
        .handle
        .run_batch(setup, "SELECT balance FROM accounts".into())
        .await
        .unwrap();
    let total: i64 = ids(&reply).iter().sum();
    assert_eq!(
        total,
        ACCOUNTS * INITIAL,
        "money was created or destroyed under concurrency"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_multi_table_transfers_survive_deadlocks() {
    const ACCOUNTS: i64 = 3;
    const TASKS: usize = 10;
    const TRANSFERS: usize = 12;
    const INITIAL: i64 = 1000;
    const TABLES: [&str; 2] = ["checking", "savings"];

    let h = start(Duration::from_secs(30));
    let setup = h
        .handle
        .open_session(String::new(), String::new(), 0)
        .await
        .expect("open session")
        .0;
    for table in TABLES {
        h.handle
            .run_batch(
                setup,
                format!("CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, balance INT NOT NULL)"),
            )
            .await
            .unwrap();
        for id in 1..=ACCOUNTS {
            h.handle
                .run_batch(
                    setup,
                    format!("INSERT INTO {table} VALUES ({id}, {INITIAL})"),
                )
                .await
                .unwrap();
        }
    }

    let mut tasks = Vec::new();
    for t in 0..TASKS {
        let handle = h.handle.clone();
        tasks.push(tokio::spawn(async move {
            let session = handle
                .open_session(String::new(), String::new(), 0)
                .await
                .expect("open session")
                .0;
            let mut rng: u64 =
                0xDEAD_BEEF_0000_0001 ^ (t as u64).wrapping_mul(0x2545_F491_4F6C_DD1D);
            let mut next = move || {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                rng >> 33
            };
            for _ in 0..TRANSFERS {
                let src = TABLES[(next() % 2) as usize];
                let dst = TABLES[(next() % 2) as usize];
                let a = (next() % ACCOUNTS as u64) as i64 + 1;
                let b = (next() % ACCOUNTS as u64) as i64 + 1;
                let amount = (next() % 40) as i64 + 1;
                let close = if next() % 8 == 0 {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                };
                // Deadlock victims only ever land on the two UPDATEs; BEGIN
                // and COMMIT/ROLLBACK take no new locks. Retry the whole
                // transaction (already rolled back) from the top.
                'attempt: loop {
                    let steps = [
                        "BEGIN TRAN".to_string(),
                        format!("UPDATE {src} SET balance = balance - {amount} WHERE id = {a}"),
                        format!("UPDATE {dst} SET balance = balance + {amount} WHERE id = {b}"),
                        close.to_string(),
                    ];
                    for step in steps {
                        let reply = handle.run_batch(session, step).await.unwrap();
                        match error_number(&reply) {
                            Some(1205) => continue 'attempt,
                            Some(other) => panic!("unexpected error {other}"),
                            None => {}
                        }
                    }
                    break;
                }
            }
            handle.close_session(session);
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let mut total = 0i64;
    for table in TABLES {
        let reply = h
            .handle
            .run_batch(setup, format!("SELECT balance FROM {table}"))
            .await
            .unwrap();
        total += ids(&reply).iter().sum::<i64>();
    }
    assert_eq!(
        total,
        2 * ACCOUNTS * INITIAL,
        "money not conserved across tables under deadlock retries"
    );
}

#[tokio::test]
async fn lone_waiter_is_reaped_by_timeout_when_pool_goes_idle() {
    let h = start(Duration::from_millis(300));
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
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(a, "INSERT INTO t VALUES (1)".into())
        .await
        .unwrap();
    // A holds X on t and stays idle inside its transaction (never commits).
    h.handle
        .run_batch(a, "BEGIN TRAN; UPDATE t SET id = id".into())
        .await
        .unwrap();

    // B's read conflicts with A's uncommitted write and parks. There is no
    // cycle (A waits on nothing), so only the timeout backstop can free it,
    // and no further calls arrive to wake a worker.
    let out = tokio::time::timeout(
        Duration::from_secs(5),
        h.handle.run_batch(b, "SELECT id FROM t".into()),
    )
    .await
    .expect("lone waiter must be reaped by the timeout, not hang forever")
    .unwrap();
    assert_eq!(
        error_number(&out),
        Some(1222),
        "a lone waiter behind a live holder times out as 1222 — there is \
         no cycle, so 1205 would report a deadlock that never happened"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_search_is_answered_while_a_large_scan_runs() {
    let h = start(LOCK_WAIT_TIMEOUT);
    let s = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    fill(&h, s, 60_000).await;
    h.handle
        .run_native(
            r#"create index bench { "mappings": { "properties": { "title": { "type": "text" } } } }"#
                .to_string(),
        )
        .await
        .expect("create index");
    h.handle
        .run_native(r#"insert document bench { "title": "hello world" }"#.to_string())
        .await
        .expect("insert doc");

    let mut events = h
        .handle
        .stream_batch(s, "SELECT id FROM t".into(), no_cancel());
    // The first Columns event: the worker is inside the batch, gate held.
    loop {
        match events.recv().await.expect("stream lives") {
            BatchEvent::Columns(_) => break,
            BatchEvent::Failed(err) => panic!("scan failed to start: {err}"),
            _ => {}
        }
    }

    let search = h
        .handle
        .run_native(r#"search bench {"query": {"match": {"title": "hello"}}}"#.to_string())
        .await
        .expect("search");
    assert!(search.contains("hello world"), "search answered: {search}");
    // The scan must have STILL BEEN RUNNING when the search returned.
    // The channel is unbounded, so "the next event" proves nothing —
    // instead drain everything already buffered at this instant without
    // blocking: if the batch's terminal event is among it, the batch
    // finished before the search did (an exclusive gate makes the search
    // wait out the whole scan, and this assertion catches exactly that).
    let mut already_finished = false;
    while let Ok(event) = events.try_recv() {
        if matches!(event, BatchEvent::Complete { .. } | BatchEvent::Failed(_)) {
            already_finished = true;
        }
    }
    assert!(
        !already_finished,
        "the search must complete while the scan is mid-stream, not after it"
    );
    drain_events(events).await;
}
