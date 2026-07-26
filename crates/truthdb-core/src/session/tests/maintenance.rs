use super::*;

#[test]
fn find_cycle_detects_and_ignores_cycles() {
    // No edges / acyclic chain / DAG -> None.
    assert!(find_cycle(&graph(&[])).is_none());
    assert!(find_cycle(&graph(&[(1, &[2]), (2, &[3]), (3, &[])])).is_none());
    assert!(find_cycle(&graph(&[(1, &[2, 3]), (2, &[3]), (3, &[])])).is_none());

    // A 2-cycle where the second node is reached as a neighbor before it is
    // colored — the case that regressed when unvisited nodes defaulted to
    // "done" instead of "unvisited".
    let c2 = find_cycle(&graph(&[(1, &[2]), (2, &[1])])).expect("2-cycle");
    assert_eq!(c2.iter().copied().collect::<HashSet<_>>(), [1, 2].into());

    // A 3-cycle with a dead-end branch (4 holds a lock but is not waiting).
    let c3 = find_cycle(&graph(&[(1, &[2, 4]), (2, &[3]), (3, &[1])])).expect("3-cycle");
    assert_eq!(c3.iter().copied().collect::<HashSet<_>>(), [1, 2, 3].into());

    // A self-loop (a transaction waiting on itself should never happen, but
    // the detector must not miss it).
    assert!(find_cycle(&graph(&[(1, &[1])])).is_some());
}

#[tokio::test]
async fn idle_transaction_is_reaped_and_its_locks_released() {
    // A client that opens a transaction and goes silent without
    // disconnecting must not hold its locks forever.
    let h = start_with_idle(Some(Duration::from_millis(150)));
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    // Session A takes a write lock and then abandons the transaction.
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
        .await
        .unwrap();

    // B blocks on A's lock while A still holds it, so wait for the reaper.
    // Once it fires, A's write is rolled back and B sees an empty table.
    let reply = h
        .handle
        .run_batch(b, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        None,
        "B must proceed once the abandoned transaction is reaped: {:?}",
        reply.outcome.error
    );
    assert_eq!(ids(&reply), Vec::<i64>::new(), "the reaped write is undone");

    // A is told its transaction was reaped rather than left to discover it
    // at a COMMIT that fails for a confusing reason.
    let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), Some(1205));
    // The signal fires once; the now-transactionless COMMIT then reports the
    // ordinary 3902.
    let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), Some(3902));
}

#[tokio::test]
async fn the_sweep_runs_on_time_even_with_a_batch_parked_further_out() {
    // A worker must not sleep past the sweep just because the nearest parked
    // deadline is further out: the abandoned transaction the parked batch is
    // waiting on would not be reaped until that deadline, and the waiter
    // would die of its own timeout (1205) first — the very lock it was
    // waiting for having been reclaimable the whole time.
    //
    // Single-worker, because with several workers a sibling holding an
    // earlier deadline snapshot can wake and sweep anyway, hiding this.
    let h = start_single_worker(Some(Duration::from_millis(150)));
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    // A abandons a transaction holding a write lock.
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
        .await
        .unwrap();
    // B parks on A's lock with a 5 s deadline — far beyond the 150 ms sweep.
    let reply = h
        .handle
        .run_batch(b, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        None,
        "B must be unblocked by the sweep, not killed at its own deadline: {:?}",
        reply.outcome.error
    );
    assert_eq!(ids(&reply), Vec::<i64>::new(), "A's write was rolled back");
}

#[tokio::test]
async fn active_transaction_is_not_reaped() {
    // The reaper must only touch *idle* sessions: a session that keeps
    // working keeps its transaction, however long it stays open.
    let h = start_with_idle(Some(Duration::from_millis(150)));
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
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
    // Keep touching the session across more than the idle timeout.
    for i in 2..=6 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let reply = h
            .handle
            .run_batch(a, format!("INSERT INTO t VALUES ({i})"))
            .await
            .unwrap();
        assert_eq!(
            error_number(&reply),
            None,
            "an active session must keep its transaction"
        );
    }
    // The transaction survived and commits everything it wrote.
    let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t ORDER BY id".into())
        .await
        .unwrap();
    assert_eq!(ids(&reply), vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn idle_reaper_can_be_disabled() {
    // With the reaper off, an idle transaction is left alone.
    let h = start_with_idle(None);
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
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
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The transaction is still open and still commits.
    let reply = h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(ids(&reply), vec![1]);
}

#[test]
fn a_running_batch_is_never_reaped_however_idle_the_session_looks() {
    // The reaper's whole safety argument: while a batch runs its context has
    // been moved out by take_ctx, so the session reports no open
    // transaction and the sweep cannot select it. Pinned directly, with a
    // zero idle timeout so that guard is the *only* thing protecting it.
    let (engine, mut sched, path) = bare("reap-running", Some(Duration::ZERO));
    let id = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    {
        let ctx = &mut sched.sessions.get_mut(id).expect("session").txn_ctx;
        engine
            .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", ctx)
            .expect("create");
        engine
            .sql_batch("BEGIN TRAN; INSERT INTO t VALUES (1);", ctx)
            .expect("begin");
    }
    assert!(
        sched
            .sessions
            .get(id)
            .expect("session")
            .txn_ctx
            .has_open_transaction(),
        "precondition: the session holds an open transaction"
    );

    // Simulate the batch being dispatched: the context moves to the worker.
    let ctx = sched.take_ctx(id);
    assert!(
        !sched
            .sessions
            .get(id)
            .expect("session")
            .txn_ctx
            .has_open_transaction(),
        "take_ctx must leave the session reporting no open transaction"
    );
    assert!(
        !sched.reap_idle_txns(&engine),
        "a session whose batch is running must never be reaped"
    );

    // Restoring it makes the very same session reapable — proving the test
    // above is not passing merely because nothing is ever reapable.
    sched.finish(&engine, id, ctx);
    assert!(
        sched.reap_idle_txns(&engine),
        "once idle again, the transaction is reaped"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_session_with_a_parked_batch_is_not_reaped() {
    // A parked batch is waiting on locks, not abandoned; its own deadline
    // reaps it. Reaping its transaction underneath it would run the batch
    // against a rolled-back transaction.
    let (engine, mut sched, path) = bare("reap-parked", Some(Duration::ZERO));
    let id = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    {
        let ctx = &mut sched.sessions.get_mut(id).expect("session").txn_ctx;
        engine
            .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", ctx)
            .expect("create");
        engine
            .sql_batch("BEGIN TRAN; INSERT INTO t VALUES (1);", ctx)
            .expect("begin");
    }
    let (tx, _rx) = mpsc::unbounded_channel();
    let reply = BatchSink::new(tx);
    sched.parked.push_back(Parked {
        session: id,
        sql: "SELECT id FROM t".into(),
        params: Vec::new(),
        proc_tail: None,
        cancel: Arc::new(AtomicBool::new(false)),
        reply,
        needs: Vec::new(),
        deadline: Instant::now() + Duration::from_secs(5),
        epoch: 0,
    });
    assert!(
        !sched.reap_idle_txns(&engine),
        "a session with a parked batch must not be reaped"
    );

    // Drop the parked entry and the same session is reaped — the guard, not
    // some other condition, is what protected it.
    sched.parked.clear();
    assert!(
        sched.reap_idle_txns(&engine),
        "with nothing parked, the idle transaction is reaped"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn review_poc_alter_procedure_reanalyzes_a_parked_exec() {
    let (engine, mut sched, path) = bare("epoch-proc-alter", None);
    let blocker = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    let waiter = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    {
        let mut ctx = crate::engine::TxnContext::default();
        engine
            .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", &mut ctx)
            .expect("create table");
        engine
            .sql_batch("CREATE PROCEDURE p AS SELECT id FROM t", &mut ctx)
            .expect("create proc");
    }
    // The blocker holds Database X so nothing becomes grantable.
    assert!(sched.try_acquire(
        blocker.raw(),
        &[(Resource::Database, LockMode::Exclusive)],
        true
    ));
    // Park an EXEC analyzed against the OLD (read-only) body.
    let epoch = engine.lock_analysis_epoch();
    let needs = engine.analyze_locks(
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC p",
        crate::engine::Isolation::ReadCommitted,
    );
    assert!(
        !needs
            .iter()
            .any(|(_, m)| matches!(m, LockMode::IntentExclusive | LockMode::Exclusive)),
        "precondition: the old body takes no write locks: {needs:?}"
    );
    let (tx, _rx) = mpsc::unbounded_channel();
    sched.parked.push_back(Parked {
        session: waiter,
        sql: "EXEC p".into(),
        params: Vec::new(),
        proc_tail: None,
        cancel: Arc::new(AtomicBool::new(false)),
        reply: BatchSink::new(tx),
        needs,
        deadline: Instant::now() + Duration::from_secs(5),
        epoch,
    });
    // The body is replaced while the batch is parked.
    {
        let mut ctx = crate::engine::TxnContext::default();
        engine
            .sql_batch("ALTER PROCEDURE p AS INSERT INTO t VALUES (1)", &mut ctx)
            .expect("alter proc");
    }
    // The grant path must re-analyze (the blocker still prevents the
    // grant itself, so the refreshed needs stay inspectable).
    assert!(
        sched.next_grantable(&engine).is_none(),
        "Database X is still held; nothing may be granted"
    );
    let refreshed = &sched.parked.front().expect("still parked").needs;
    assert!(
        refreshed
            .iter()
            .any(|(_, m)| matches!(m, LockMode::IntentExclusive | LockMode::Exclusive)),
        "the parked EXEC's lock set was re-analyzed against the NEW \
         body (INSERT): {refreshed:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn the_pool_actually_spawns_a_maintenance_thread() {
    // `housekeeping_runs_with_no_worker_free_to_do_it` builds `Shared` by
    // hand and starts the thread itself, so it would pass just as happily
    // if the supervisor never spawned one. This covers the wiring: run the
    // real `spawn_engine` path and wait for a maintenance thread to report
    // in. (A sibling test's pool satisfying this is fine — it is the same
    // supervisor code either way; what fails is nobody spawning one at all.)
    let h = start_with_idle(Some(Duration::from_millis(150)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while MAINTENANCE_STARTS.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        MAINTENANCE_STARTS.load(Ordering::Relaxed) > 0,
        "the pool spawns a maintenance thread"
    );
    drop(h);
}

#[test]
fn both_ways_of_shutting_the_pool_down_stop_every_thread() {
    // The server calls `shutdown` and then joins; the tests just drop the
    // handle. Only the first closes the inbox by itself — the second relies
    // on the handle token, since the `Arc<Inbox>` the workers hold can never
    // reach zero. Joining the supervisor is what proves it: it joins the
    // workers and the maintenance thread first, so it only returns if every
    // one of them noticed.
    for explicit in [true, false] {
        let path = unique_temp_path("shutdown");
        let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
        let engine = Engine::new(storage).expect("engine");
        let (handle, join) = spawn_engine(engine);
        if explicit {
            handle.shutdown();
            drop(handle);
        } else {
            drop(handle);
        }
        join.join().expect("the pool shut down");
        let _ = std::fs::remove_file(path);
    }
}

#[tokio::test]
async fn a_batch_the_idle_reaper_unblocks_is_handed_to_a_worker() {
    // The reaper runs off-worker now, so releasing a lock and running what
    // that unblocks happen on different threads. Workers block on the inbox
    // indefinitely — there is no timeout to fall back on — so a reap that
    // did not nudge would leave this batch parked forever rather than late.
    let h = start_with_idle(Some(Duration::from_millis(150)));
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    let b = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    // A abandons a write lock; B parks behind it and can only be rescued by
    // the maintenance thread reaping A and then nudging a worker.
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
        .await
        .unwrap();
    let reply = tokio::time::timeout(
        Duration::from_secs(10),
        h.handle.run_batch(b, "SELECT id FROM t".into()),
    )
    .await
    .expect("the rescued batch was handed to a worker, not left parked")
    .unwrap();
    assert_eq!(error_number(&reply), None, "{:?}", reply.outcome.error);
    assert_eq!(ids(&reply), Vec::<i64>::new(), "the reaped write is undone");
}

#[test]
fn the_maintenance_thread_sleeps_between_sweeps_whatever_is_parked() {
    // An expired waiter that `reap_expired` must NOT reap (its locks are
    // free, so it is queued for a worker rather than blocked) keeps a
    // deadline in the past for as long as it sits there. A sweeper that
    // derived its sleep from that deadline would compute zero and spin,
    // taking the scheduler mutex thousands of times a second — and would do
    // it precisely while every worker was busy, which is the case this
    // thread exists for.
    let (engine, mut sched, path) = bare("no-spin", Some(Duration::from_millis(50)));
    let id = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    let (tx, _rx) = mpsc::unbounded_channel();
    let reply = BatchSink::new(tx);
    sched.parked.push_back(Parked {
        session: id,
        sql: "SELECT 1".into(),
        params: Vec::new(),
        proc_tail: None,
        cancel: Arc::new(AtomicBool::new(false)),
        reply,
        needs: vec![(Resource::Table(1), LockMode::Shared)],
        deadline: Instant::now() - Duration::from_secs(60),
        epoch: 0,
    });
    let shared = Arc::new(Shared {
        engine: Arc::new(engine),
        scheduler: Mutex::new(sched),
        inbox: Arc::new(Inbox::new()),
        stop: AtomicBool::new(false),
        idle: Mutex::new(()),
        wake: Condvar::new(),
        sweeps: std::sync::atomic::AtomicUsize::new(0),
    });
    let keeper = Arc::clone(&shared);
    let maintenance = std::thread::spawn(move || maintenance_loop(&keeper));
    std::thread::sleep(Duration::from_millis(200));
    let sweeps = shared.sweeps.load(Ordering::Relaxed);
    {
        let _idle = shared.idle.lock().expect("idle mutex poisoned");
        shared.stop.store(true, Ordering::Release);
    }
    shared.wake.notify_all();
    maintenance.join().expect("maintenance thread");

    // 200ms at a 50ms interval is ~4 sweeps. A spin measures in thousands,
    // so the bound is loose enough not to be a timing test.
    assert!(
        sweeps <= 20,
        "the thread slept between sweeps; it ran {sweeps} in 200ms"
    );
    assert!(
        shared
            .scheduler
            .lock()
            .expect("scheduler poisoned")
            .parked
            .len()
            == 1,
        "and it left the grantable waiter for a worker, rather than reaping it"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn housekeeping_runs_with_no_worker_free_to_do_it() {
    // The reapers are the engine's safety valves and must not be hostage to
    // the pool: a worker only sweeps between batches, so a busy pool used to
    // mean no sweep. Pinned in its strongest form — a pool with *no workers
    // at all*, which no amount of load can distinguish itself from — where
    // only the maintenance thread can do it.
    let (engine, mut sched, path) = bare("maintenance", Some(Duration::from_millis(50)));
    let id = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    {
        let ctx = &mut sched.sessions.get_mut(id).expect("session").txn_ctx;
        engine
            .sql_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)", ctx)
            .expect("create");
        engine
            .sql_batch("BEGIN TRAN; INSERT INTO t VALUES (1);", ctx)
            .expect("begin");
        assert!(
            ctx.has_open_transaction(),
            "the session starts with an open txn"
        );
    }
    let shared = Arc::new(Shared {
        engine: Arc::new(engine),
        scheduler: Mutex::new(sched),
        inbox: Arc::new(Inbox::new()),
        stop: AtomicBool::new(false),
        idle: Mutex::new(()),
        wake: Condvar::new(),
        sweeps: std::sync::atomic::AtomicUsize::new(0),
    });
    let keeper = Arc::clone(&shared);
    let maintenance = std::thread::spawn(move || maintenance_loop(&keeper));

    let deadline = Instant::now() + Duration::from_secs(5);
    let reaped = loop {
        {
            let sched = shared.scheduler.lock().expect("scheduler poisoned");
            if !sched
                .sessions
                .get(id)
                .expect("session")
                .txn_ctx
                .has_open_transaction()
            {
                break true;
            }
        }
        if Instant::now() > deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    {
        let _idle = shared.idle.lock().expect("idle mutex poisoned");
        shared.stop.store(true, Ordering::Release);
    }
    shared.wake.notify_all();
    maintenance.join().expect("maintenance thread");
    assert!(
        reaped,
        "the idle transaction was reaped with no worker to do it"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_parked_batch_whose_locks_are_free_is_never_reaped_as_a_victim() {
    // The deadline is a backstop for a batch stuck behind someone else's
    // lock. A waiter whose locks are free is not stuck — it is queued for a
    // worker — so reaping it would report a conflict (1205) that never
    // existed.
    let (engine, mut sched, path) = bare("reap-grantable", None);
    let id = sched.sessions.open(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        0,
        "dbo".into(),
        0,
    );
    let (tx, _rx) = mpsc::unbounded_channel();
    let reply = BatchSink::new(tx);
    // Parked, deadline long gone, and nothing holds the lock it wants.
    sched.parked.push_back(Parked {
        session: id,
        sql: "SELECT 1".into(),
        params: Vec::new(),
        proc_tail: None,
        cancel: Arc::new(AtomicBool::new(false)),
        reply,
        needs: vec![(Resource::Table(1), LockMode::Shared)],
        deadline: Instant::now() - Duration::from_secs(60),
        epoch: 0,
    });
    sched.reap_expired(&engine);
    assert_eq!(
        sched.parked.len(),
        1,
        "an expired waiter whose locks are free is not a deadlock victim"
    );

    // The same waiter, once something actually blocks it, is reaped — so it
    // is grantability doing the work above, not a dead reaper.
    sched
        .locks
        .grant(999, Resource::Table(1), LockMode::Exclusive);
    sched.reap_expired(&engine);
    assert!(
        sched.parked.is_empty(),
        "an expired waiter that is genuinely blocked is still the victim"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn reaped_session_is_told_instead_of_silently_autocommitting() {
    // A client that comes back believing it is still in a transaction must
    // not have its statements silently autocommit.
    let h = start_with_idle(Some(Duration::from_millis(150)));
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
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
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The next batch is told the transaction is gone, and does not run.
    let reply = h
        .handle
        .run_batch(a, "INSERT INTO t VALUES (2)".into())
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(1205),
        "the reap must be reported, not swallowed"
    );

    // The signal fires once; the session is usable again afterwards.
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t".into())
        .await
        .unwrap();
    assert_eq!(error_number(&reply), None);
    assert_eq!(
        ids(&reply),
        Vec::<i64>::new(),
        "the reaped write is undone, and the rejected INSERT never applied"
    );
}

#[tokio::test]
async fn a_reaped_transaction_leaves_no_savepoints_behind() {
    // A savepoint holds the undo-log offset of the transaction that recorded
    // it. One surviving a reap would let ROLLBACK TRANSACTION find a stale
    // entry in the session's NEXT transaction and hand a dead offset to the
    // undo log — silently discarding committed work, or panicking.
    let h = start_with_idle(Some(Duration::from_millis(150)));
    let a = h
        .handle
        .open_session("truthdb".into(), "sa".into(), 0)
        .await
        .expect("open session")
        .0;
    h.handle
        .run_batch(a, "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)".into())
        .await
        .unwrap();
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; INSERT INTO t VALUES (1); SAVE TRANSACTION sp1;".into(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Drain the one-shot reap signal.
    let reply = h.handle.run_batch(a, "SELECT 1".into()).await.unwrap();
    assert_eq!(error_number(&reply), Some(1205));

    // sp1 belonged to the reaped transaction: rolling back to it in a new
    // transaction must error 3908, not silently truncate this one's work.
    let reply = h
        .handle
        .run_batch(
            a,
            "BEGIN TRAN; INSERT INTO t VALUES (7); INSERT INTO t VALUES (8); ROLLBACK TRANSACTION sp1;"
                .into(),
        )
        .await
        .unwrap();
    assert_eq!(
        error_number(&reply),
        Some(3908),
        "a savepoint from the reaped transaction must not survive"
    );
    h.handle.run_batch(a, "ROLLBACK".into()).await.unwrap();

    // And the new transaction's work was never silently discarded.
    h.handle
        .run_batch(
            a,
            "BEGIN TRAN; INSERT INTO t VALUES (7); INSERT INTO t VALUES (8); COMMIT;".into(),
        )
        .await
        .unwrap();
    let reply = h
        .handle
        .run_batch(a, "SELECT id FROM t ORDER BY id".into())
        .await
        .unwrap();
    assert_eq!(ids(&reply), vec![7, 8], "both committed rows survive");
}

#[tokio::test]
async fn writer_blocks_reader_until_commit() {
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
    // A opens a transaction and writes, holding X on t.
    h.handle
        .run_batch(a, "BEGIN TRAN; INSERT INTO t VALUES (1);".into())
        .await
        .unwrap();

    // B's read must block (READ COMMITTED cannot read A's uncommitted row).
    let handle_b = h.handle.clone();
    let read = tokio::spawn(async move {
        handle_b
            .run_batch(b, "SELECT id FROM t".into())
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !read.is_finished(),
        "reader should be blocked by the writer"
    );

    // A commits → releases X → B unblocks and sees the committed row.
    h.handle.run_batch(a, "COMMIT".into()).await.unwrap();
    let out = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("reader should unblock after commit")
        .unwrap();
    assert_eq!(ids(&out), vec![1]);
}
