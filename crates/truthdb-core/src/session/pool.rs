use super::*;

/// How long a batch may wait on a lock before it is treated as a deadlock
/// victim and rolled back (SQL Server-style, plan: "5 s wait timeout →
/// abort youngest").
pub(super) const LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a session may sit idle with a transaction still open before the
/// engine rolls it back and releases its locks.
///
/// This is a deliberate divergence from SQL Server, which never reaps an idle
/// transaction: a client that dies without closing its TCP connection would
/// otherwise hold its locks until the OS notices, which can take hours, and
/// every conflicting batch fails with 1205 in the meantime. Ten minutes is far
/// longer than any interactive transaction should stay idle, so a legitimate
/// client is never reaped. `spawn_engine_pool` takes it as `Option`, so it can
/// be disabled outright.
pub(super) const IDLE_TXN_TIMEOUT: Duration = Duration::from_secs(600);

/// Floor on the maintenance thread's sleep, so no configuration of the timeouts
/// can turn its loop into a spin.
pub(super) const MIN_SWEEP_INTERVAL: Duration = Duration::from_millis(10);

/// Worker-thread count for the pool: one per core (minus a couple reserved for
/// the async listeners), at least two so reads can genuinely overlap.
pub(super) fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(2))
        .unwrap_or(2)
        .max(2)
}

/// Spawns the engine worker pool and returns a handle plus a join handle for a
/// supervisor thread that outlives every worker.
pub fn spawn_engine(engine: Engine) -> (EngineHandle, JoinHandle<()>) {
    spawn_engine_pool(
        engine,
        LOCK_WAIT_TIMEOUT,
        Some(IDLE_TXN_TIMEOUT),
        worker_count(),
    )
}

/// Like [`spawn_engine`] but with a custom lock-wait timeout, so tests can
/// exercise the deadlock reaper without a real 5 s wait.
#[cfg(test)]
pub(super) fn spawn_engine_with_timeout(
    engine: Engine,
    timeout: Duration,
) -> (EngineHandle, JoinHandle<()>) {
    spawn_engine_pool(engine, timeout, Some(IDLE_TXN_TIMEOUT), worker_count())
}

/// Like [`spawn_engine`] but with a custom idle-transaction timeout, so tests
/// can exercise the idle reaper without a real 10 min wait.
#[cfg(test)]
pub(super) fn spawn_engine_with_idle_timeout(
    engine: Engine,
    idle: Option<Duration>,
) -> (EngineHandle, JoinHandle<()>) {
    spawn_engine_pool(engine, LOCK_WAIT_TIMEOUT, idle, worker_count())
}

pub(super) fn spawn_engine_pool(
    engine: Engine,
    timeout: Duration,
    idle_txn_timeout: Option<Duration>,
    workers: usize,
) -> (EngineHandle, JoinHandle<()>) {
    let inbox = Arc::new(Inbox::new());
    let shared = Arc::new(Shared {
        engine: Arc::new(engine),
        scheduler: Mutex::new(Scheduler::new(timeout, idle_txn_timeout)),
        inbox: Arc::clone(&inbox),
        stop: AtomicBool::new(false),
        idle: Mutex::new(()),
        wake: Condvar::new(),
        #[cfg(test)]
        sweeps: std::sync::atomic::AtomicUsize::new(0),
    });
    // A supervisor thread spawns the workers and joins them; its handle is what
    // callers join at shutdown. When all workers have exited, any batch still
    // parked is failed so its caller unblocks.
    let supervisor = Arc::clone(&shared);
    let join = std::thread::Builder::new()
        .name("truthdb-engine".to_string())
        .spawn(move || {
            let keeper = Arc::clone(&supervisor);
            let maintenance = std::thread::Builder::new()
                .name("truthdb-maintenance".to_string())
                .spawn(move || maintenance_loop(&keeper))
                .expect("spawn maintenance thread");
            let handles: Vec<_> = (0..workers)
                .map(|i| {
                    let shared = Arc::clone(&supervisor);
                    std::thread::Builder::new()
                        .name(format!("truthdb-worker-{i}"))
                        .spawn(move || worker_loop(&shared))
                        .expect("spawn worker thread")
                })
                .collect();
            for handle in handles {
                let _ = handle.join();
            }
            // The workers are gone, and neither way of getting here sets the
            // flag: `shutdown` and the last handle dropping both just close the
            // inbox. Tell the maintenance thread, or it would outlive the pool.
            // Setting the flag under `idle` is what makes the wake reliable
            // rather than a race against its next sleep.
            {
                let _idle = supervisor.idle.lock().expect("idle mutex poisoned");
                supervisor.stop.store(true, Ordering::Release);
            }
            supervisor.wake.notify_all();
            let _ = maintenance.join();
            let mut sched = supervisor.scheduler.lock().expect("scheduler poisoned");
            for parked in sched.parked.drain(..) {
                parked
                    .reply
                    .send(BatchEvent::Failed(EngineError::Unavailable));
            }
        })
        .expect("spawn engine supervisor");
    (
        EngineHandle {
            _token: Arc::new(HandleToken(Arc::clone(&inbox))),
            inbox,
        },
        join,
    )
}

/// State shared by every worker thread.
pub(super) struct Shared {
    /// The database engine. `&self` throughout, so the pool shares one `Arc`.
    pub(super) engine: Arc<Engine>,
    /// Sessions + lock table + parked queue. Held only for lock decisions.
    pub(super) scheduler: Mutex<Scheduler>,
    /// Inbound calls, plus the drain nudge a releaser uses to hand parked work
    /// to whichever worker is free.
    pub(super) inbox: Arc<Inbox>,
    /// Set when a `Shutdown` is seen, so a worker between calls exits promptly
    /// rather than picking up more work.
    pub(super) stop: AtomicBool,
    /// Companion mutex for [`Self::wake`]. Guards nothing — a `Condvar` needs
    /// one.
    pub(super) idle: Mutex<()>,
    /// Wakes the maintenance thread out of its sleep at shutdown, so the pool
    /// does not wait out a whole sweep interval before exiting.
    pub(super) wake: Condvar,
    /// This pool's maintenance sweeps, so a test can prove the thread sleeps
    /// between them rather than spinning. Per-pool, not global: the tests run
    /// in parallel in one binary, and a global counter measures every other
    /// pool's sweeps too.
    #[cfg(test)]
    pub(super) sweeps: std::sync::atomic::AtomicUsize,
}

// The pool shares `Arc<Engine>` across worker threads, so the engine — and thus
// the whole shared state — must be Send + Sync. Assert it here rather than
// discovering it via a distant `thread::spawn` error.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Shared>();
};

/// Counts maintenance threads that have started, so a test can prove the pool
/// actually spawns one — the reaping itself is pinned against a hand-built
/// `Shared`, which would not notice the supervisor forgetting to wire it up.
#[cfg(test)]
pub(super) static MAINTENANCE_STARTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counts maintenance sweeps, so a test can prove the thread sleeps between
/// them rather than spinning.
#[cfg(test)]
pub(super) static MAINTENANCE_SWEEPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// The engine's housekeeping, on a thread that never runs a batch: the deadlock
/// backstop and the idle-transaction reaper.
///
/// Both used to run only on the workers, between calls, which made them exactly
/// as punctual as the pool was free — and the pool is `cores-2` threads, two on
/// a four-core box. A few long scans deferred them for as long as they ran,
/// which is backwards: the idle reaper exists to release the locks of a client
/// that has stopped responding, and a loaded engine is when that matters most.
/// Nothing a client does can delay this thread, because it never executes
/// anything on anyone's behalf.
///
/// It only *releases* locks. Running what that unblocks still needs a worker,
/// so it nudges the [`Inbox`] rather than doing it here — the pairing the old
/// worker sweep got for free by simply calling `drain_ready` next.
pub(super) fn maintenance_loop(shared: &Arc<Shared>) {
    #[cfg(test)]
    MAINTENANCE_STARTS.fetch_add(1, Ordering::Relaxed);
    while !shared.stop.load(Ordering::Acquire) {
        // Sleep until the nearest deadline the reaper could act on, capped by
        // the idle sweep's interval and floored so no arrangement of parked
        // work or tuning knobs can turn this loop into a spin.
        let wait = {
            let sched = shared.scheduler.lock().expect("scheduler poisoned");
            let cap = sched.sweep_interval();
            match sched.earliest_reapable_deadline() {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()).min(cap),
                None => cap,
            }
            .max(MIN_SWEEP_INTERVAL)
        };
        {
            // `stop` is read and written under this mutex, so a shutdown that
            // lands between the check and the wait cannot be missed — it would
            // otherwise be slept through for a whole interval, and the
            // supervisor is joining this thread.
            let idle = shared.idle.lock().expect("idle mutex poisoned");
            if shared.stop.load(Ordering::Acquire) {
                break;
            }
            let _ = shared
                .wake
                .wait_timeout(idle, wait)
                .expect("idle mutex poisoned");
        }
        #[cfg(test)]
        shared.sweeps.fetch_add(1, Ordering::Relaxed);
        {
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            // One victim per sweep, and the floor on the sleep means a worker
            // has a chance to run what that release rescued before the next
            // one. The ordering matters: reaping a victim makes the waiter
            // behind it grantable, and sweeping again without letting that
            // happen would take a second victim where one would do.
            sched.reap_expired(&shared.engine);
            sched.reap_idle_txns(&shared.engine);
        }
        // Version-store upkeep (Stage 13): drop row-version history no live
        // snapshot can need. Outside the scheduler lock — it takes the
        // storage lock, and nothing here depends on lock-table state.
        shared.engine.version_prune();
        // Standby ring upkeep (Stage 18): a replication standby cannot
        // checkpoint, so this periodic restartpoint is what reclaims its WAL
        // ring (up to the primary-shipped undo floor). A no-op elsewhere.
        if let Err(err) = shared.engine.standby_restartpoint_if_needed() {
            eprintln!("standby restartpoint failed: {err}");
        }
        // Unconditionally, not just when something was released. Workers now
        // block indefinitely on the inbox, so a nudge that should have been
        // sent and was not is a batch parked forever rather than a batch
        // started late — this periodic one is the backstop the workers' old
        // `recv_timeout(wake_cap)` used to be, at the same cost: one wakeup per
        // interval on an idle engine, which finds nothing and sleeps again.
        shared.inbox.nudge();
    }
}

/// One worker thread: pull a call, dispatch it, repeat until shutdown.
pub(super) fn worker_loop(shared: &Arc<Shared>) {
    while !shared.stop.load(Ordering::Acquire) {
        // Block until there is a call to run, or a releaser nudged us to look
        // at the parked queue. `None` means the inbox is closed and drained.
        let work = match shared.inbox.next() {
            Some(work) => work,
            None => break,
        };
        {
            // A call we just dequeued proves its session is not idle. Stamp it
            // before anything else looks: the maintenance thread sweeps at
            // arbitrary times and would otherwise reap the transaction of a
            // session whose next batch is already in hand.
            let mut sched = shared.scheduler.lock().expect("scheduler poisoned");
            match &work {
                Work::Call(EngineCall::RunBatch { session, .. })
                | Work::Call(EngineCall::RunRpc { session, .. }) => {
                    sched.sessions.touch(*session);
                }
                _ => {}
            }
        }
        drain_ready(shared);
        match work {
            Work::Drain => {}
            Work::Call(EngineCall::OpenSession {
                database,
                login,
                login_sid,
                reply,
            }) => {
                // The requested database must exist (the caller answers 4060
                // otherwise); the session records the CATALOG's spelling and
                // the id, resolved once, here — the same derivation USE runs.
                // NOT an early return: this arm runs inside the worker loop,
                // and returning would kill the thread.
                if let Some((db_id, canonical)) = shared.engine.resolve_database(&database) {
                    // Resolve the login to its database user here (the worker
                    // has the catalog); the session records both for
                    // USER_NAME() and role membership.
                    let (user, user_sid) = shared.engine.resolve_session_user(&login, login_sid);
                    let id = shared
                        .scheduler
                        .lock()
                        .expect("scheduler poisoned")
                        .sessions
                        .open(canonical.clone(), db_id, login, login_sid, user, user_sid);
                    let _ = reply.send(Ok((id, canonical)));
                } else {
                    let _ = reply.send(Err(()));
                }
            }
            Work::Call(EngineCall::RunBatch {
                session,
                sql,
                params,
                proc_tail,
                cancel,
                reply,
            }) => dispatch_batch(shared, session, sql, params, proc_tail, cancel, reply),
            Work::Call(EngineCall::RunRpc {
                session,
                command,
                cancel,
                reply,
            }) => dispatch_rpc(shared, session, command, cancel, reply),
            Work::Call(EngineCall::RunNative { command, reply }) => {
                let _ = reply.send(shared.engine.execute(&command));
            }
            Work::Call(EngineCall::LookupLogin { name, reply }) => {
                let _ = reply.send(shared.engine.lookup_login(&name));
            }
            Work::Call(EngineCall::CloseSession { session }) => {
                shared
                    .scheduler
                    .lock()
                    .expect("scheduler poisoned")
                    .close_session(&shared.engine, session);
                drain_ready(shared);
            }
        }
    }
}
