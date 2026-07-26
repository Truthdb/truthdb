use super::*;

/// A parked batch that has become grantable: its locks are already held and its
/// session's transaction context has been taken out for the worker to run with.
pub(super) struct Runnable {
    pub(super) session: SessionId,
    pub(super) sql: String,
    pub(super) params: Vec<crate::engine::RpcParam>,
    pub(super) proc_tail: Option<ProcRpcTail>,
    pub(super) cancel: Arc<AtomicBool>,
    pub(super) reply: BatchSink,
    pub(super) txn_ctx: TxnContext,
}

/// A SQL batch waiting for locks: its request, the locks it needs, and the
/// deadline past which it is treated as a deadlock victim.
pub(super) struct Parked {
    pub(super) session: SessionId,
    pub(super) sql: String,
    pub(super) params: Vec<crate::engine::RpcParam>,
    pub(super) proc_tail: Option<ProcRpcTail>,
    pub(super) cancel: Arc<AtomicBool>,
    pub(super) reply: BatchSink,
    pub(super) needs: Vec<(Resource, LockMode)>,
    pub(super) deadline: Instant,
    /// The lock-analysis epoch `needs` was computed under. A pending `ALTER
    /// DATABASE` option flip bumps the epoch; a parked batch whose epoch is
    /// stale is re-analyzed before it can be granted, so the lock set it
    /// runs with always matches the versioning regime it will execute under
    /// (analyzed-versioned-but-executes-lock-based was a dirty read).
    pub(super) epoch: u64,
}

/// The scheduler's mutable world: the sessions, the lock manager, and the FIFO
/// queue of batches parked on locks. One [`Mutex`] guards all three; a worker
/// holds it only to make lock decisions, never while running a batch.
pub(super) struct Scheduler {
    pub(super) sessions: SessionManager,
    pub(super) locks: LockManager,
    pub(super) parked: VecDeque<Parked>,
    pub(super) lock_wait_timeout: Duration,
    /// How long a session may sit idle *with a transaction open* before that
    /// transaction is rolled back and its locks released. `None` disables the
    /// reaper.
    pub(super) idle_txn_timeout: Option<Duration>,
}

impl Scheduler {
    pub(super) fn new(lock_wait_timeout: Duration, idle_txn_timeout: Option<Duration>) -> Self {
        Scheduler {
            sessions: SessionManager::new(),
            locks: LockManager::new(),
            parked: VecDeque::new(),
            lock_wait_timeout,
            idle_txn_timeout,
        }
    }

    /// The earliest deadline the reaper could actually act on.
    ///
    /// A waiter that is grantable is skipped, and that is load-bearing rather
    /// than an optimisation: [`Self::reap_expired`] refuses to reap one (it is
    /// queued for a worker, not blocked), so its deadline stays in the past for
    /// as long as it sits there. Letting that drive the sleep computes zero and
    /// spins a core against the scheduler mutex — and only while every worker
    /// is busy, since a free one drains the waiter away in microseconds.
    pub(super) fn earliest_reapable_deadline(&self) -> Option<Instant> {
        (0..self.parked.len())
            .filter(|i| !self.grantable(*i))
            .map(|i| self.parked[i].deadline)
            .min()
    }

    /// The longest the maintenance thread may sleep: often enough to notice an
    /// idle transaction, and floored so no setting of a tuning knob can turn
    /// its loop into a spin (a test already passes `Duration::ZERO`).
    pub(super) fn sweep_interval(&self) -> Duration {
        match self.idle_txn_timeout {
            Some(idle) => idle.min(self.lock_wait_timeout),
            // The reaper is disabled; there is nothing to be prompt for.
            None => self.lock_wait_timeout,
        }
        .max(MIN_SWEEP_INTERVAL)
    }

    /// A session's current isolation level (default if the session is unknown).
    pub(super) fn isolation(&self, session: SessionId) -> Isolation {
        self.sessions
            .get(session)
            .map(|s| s.txn_ctx.isolation())
            .unwrap_or_default()
    }

    /// The session's current database id — the namespace its batch's lock
    /// analysis must resolve names in (the same one execution will use). The
    /// default database for an unknown session or one whose context is
    /// momentarily taken (a session has at most one in-flight batch, so its
    /// own analysis never observes the placeholder).
    pub(super) fn session_db(&self, session: SessionId) -> u32 {
        self.sessions
            .get(session)
            .map(|s| s.txn_ctx.database_id())
            .unwrap_or(crate::relstore::catalog::DEFAULT_DATABASE_ID)
    }

    /// Takes a session's transaction context out for a worker to run a batch
    /// with (a `Default` placeholder is left behind; [`Self::finish`] returns
    /// the real one). A session has at most one in-flight batch and no close
    /// races it — the connection is request/response — so the placeholder is
    /// never observed. Unknown session: a transient context, rolled back after.
    pub(super) fn take_ctx(&mut self, session: SessionId) -> TxnContext {
        self.sessions
            .get_mut(session)
            .map(|state| std::mem::take(&mut state.txn_ctx))
            .unwrap_or_default()
    }

    /// Tries to grant every lock in `needs` to `owner` atomically. When
    /// `respect_queue` is set, an incoming batch also yields to any resource a
    /// parked waiter (of another owner) is already queued for — FIFO fairness,
    /// no barging. A resource the owner ALREADY holds is exempt from that
    /// yield: re-acquiring or upgrading a held lock is not queue-jumping, and
    /// yielding there would make a transaction wait on its own lock (a waiter
    /// parked behind that lock can never release it), a false self-deadlock.
    /// Returns whether all locks were granted.
    pub(super) fn try_acquire(
        &mut self,
        owner: u64,
        needs: &[(Resource, LockMode)],
        respect_queue: bool,
    ) -> bool {
        let blocked = needs.iter().any(|(resource, mode)| {
            let queued = respect_queue
                && !self.locks.holds(owner, *resource)
                && self.parked.iter().any(|p| {
                    p.session.raw() != owner && p.needs.iter().any(|(r, _)| r == resource)
                });
            queued || self.locks.conflict(owner, *resource, *mode).is_some()
        });
        if blocked {
            return false;
        }
        for (resource, mode) in needs {
            self.locks.grant(owner, *resource, *mode);
        }
        true
    }

    /// Returns a finished batch's transaction context to its session and
    /// releases the locks that do not outlive it: all of them once the
    /// transaction closes; read locks after each statement under READ
    /// COMMITTED. Returns whether the connection is still in a transaction.
    /// (Execution ran in [`run_and_finish`], with the scheduler lock released.)
    pub(super) fn finish(
        &mut self,
        engine: &Engine,
        session: SessionId,
        txn_ctx: TxnContext,
    ) -> bool {
        let owner = session.raw();
        match self.sessions.get_mut(session) {
            Some(state) => {
                state.txn_ctx = txn_ctx;
                // The idle clock restarts when the batch finishes: time spent
                // running is not time spent idle.
                state.last_activity = Instant::now();
                let open = state.txn_ctx.has_open_transaction();
                // READ COMMITTED shared locks do not survive the batch, and a
                // SNAPSHOT transaction holds no read locks between batches at
                // all (its Database IS is the running batch's DDL fence; the
                // snapshot itself is what protects its reads).
                let releases_read_locks = matches!(
                    state.txn_ctx.isolation(),
                    Isolation::ReadCommitted | Isolation::Snapshot
                );
                if open {
                    // Transaction still open: keep write locks.
                    if releases_read_locks {
                        self.locks.release_read_locks(owner);
                    }
                    true
                } else {
                    // Transaction closed (autocommit or COMMIT/ROLLBACK): drop
                    // every lock the batch acquired.
                    self.locks.release_all(owner);
                    false
                }
            }
            None => {
                // Session closed while the batch ran, or unknown: roll back the
                // taken context and hold no locks.
                let mut txn_ctx = txn_ctx;
                engine.abort_session_txn(&mut txn_ctx);
                self.locks.release_all(owner);
                false
            }
        }
    }

    /// Whether the parked batch at `i` could take every lock it needs right
    /// now — i.e. it is waiting for a worker to pick it up, not for a lock.
    pub(super) fn grantable(&self, i: usize) -> bool {
        let owner = self.parked[i].session.raw();
        // Only waiters ahead in the queue have priority (FIFO); a waiter never
        // yields to itself or to those behind it.
        let ahead: Vec<(Resource, LockMode)> = self
            .parked
            .iter()
            .take(i)
            .filter(|p| p.session.raw() != owner)
            .flat_map(|p| p.needs.iter().copied())
            .collect();
        self.parked[i].needs.iter().all(|(resource, mode)| {
            // A resource the waiter already holds is exempt from the FIFO yield
            // (it is not jumping the queue for it), matching try_acquire.
            (self.locks.holds(owner, *resource) || !ahead.iter().any(|(r, _)| r == resource))
                && self.locks.conflict(owner, *resource, *mode).is_none()
        })
    }

    /// Removes and returns the first parked batch (FIFO) whose locks are now
    /// grantable, having granted them and taken its session's transaction
    /// context out to run with. `None` if none can proceed. The caller runs it
    /// with the scheduler lock released, then calls again.
    pub(super) fn next_grantable(&mut self, engine: &Engine) -> Option<Runnable> {
        let current_epoch = engine.lock_analysis_epoch();
        let mut i = 0;
        while i < self.parked.len() {
            // An ALTER DATABASE option flip since this batch was analyzed may
            // have changed which locks its reads need (versioned vs Table S):
            // re-analyze before considering the grant, so the lock set always
            // matches the regime the batch will execute under. The deadlock
            // graph may see one sweep of stale needs; the grant path never
            // does.
            if self.parked[i].epoch != current_epoch {
                let isolation = self.isolation(self.parked[i].session);
                self.parked[i].needs = engine.analyze_locks(
                    self.session_db(self.parked[i].session),
                    &self.parked[i].sql,
                    isolation,
                );
                self.parked[i].epoch = current_epoch;
            }
            let owner = self.parked[i].session.raw();
            if self.grantable(i) {
                let parked = self.parked.remove(i).expect("index in bounds");
                for (resource, mode) in &parked.needs {
                    self.locks.grant(owner, *resource, *mode);
                }
                let txn_ctx = self.take_ctx(parked.session);
                return Some(Runnable {
                    session: parked.session,
                    sql: parked.sql,
                    params: parked.params,
                    proc_tail: parked.proc_tail,
                    cancel: parked.cancel,
                    reply: parked.reply,
                    txn_ctx,
                });
            }
            i += 1;
        }
        None
    }

    /// Rolls back the single earliest-deadline batch whose wait has expired
    /// (the deadlock backstop victim). The caller then drains anyone its
    /// released locks unblock — typically rescuing its deadlock partner before
    /// that partner is itself reaped. Any further expired waiters are handled on
    /// the next loop iteration.
    ///
    /// A waiter whose locks are already grantable is never a victim, however
    /// long it has sat there: it is not blocked on anyone, it is waiting for a
    /// worker to run it, and killing it would report a lock conflict (1205)
    /// that does not exist. The gap is narrow today — the worker that releases
    /// the locks drains the queue microseconds later, so only an unlucky
    /// deschedule between the two exposes it — but it widens as soon as
    /// anything can delay a worker between releasing locks and draining, which
    /// is exactly what pushing result rows to a client will do. The reaper's
    /// contract is about lock waits either way.
    pub(super) fn reap_expired(&mut self, engine: &Engine) {
        let now = Instant::now();
        let victim_idx = self
            .parked
            .iter()
            .enumerate()
            // `grantable` is only consulted for a waiter that has actually
            // expired, so the common case (nothing expired) does no extra work.
            .filter(|(i, p)| p.deadline <= now && !self.grantable(*i))
            .min_by_key(|(_, p)| p.deadline)
            .map(|(i, _)| i);
        if let Some(idx) = victim_idx {
            // An expired wait behind a LIVE holder is not a deadlock: SQL
            // Server raises 1205 only for real cycles and 1222 for a lock
            // wait that timed out. Reporting a false deadlock sends drivers
            // into retry loops for a condition retrying cannot fix.
            self.abort_parked_victim(engine, idx, lock_timeout_error());
        }
    }

    /// Rolls back transactions abandoned by idle sessions, releasing their
    /// locks; returns whether anything was released (so the caller drains the
    /// batches those locks unblock).
    ///
    /// A connection that opens a transaction and then goes silent *without
    /// disconnecting* — a crashed client, a severed network — would otherwise
    /// hold its locks indefinitely: the connection-drop path only covers a
    /// connection that actually closed, and TCP may not notice for hours.
    ///
    /// Only genuinely idle sessions are candidates. A session running a batch
    /// has had its context moved out by [`Self::take_ctx`], so it reports no
    /// open transaction and cannot be reaped mid-batch; a session with a parked
    /// batch is skipped explicitly, since that batch is only waiting on locks
    /// (and its own deadline reaps it) rather than being abandoned.
    pub(super) fn reap_idle_txns(&mut self, engine: &Engine) -> bool {
        let Some(timeout) = self.idle_txn_timeout else {
            return false;
        };
        let now = Instant::now();
        let parked: Vec<SessionId> = self.parked.iter().map(|p| p.session).collect();
        let victims: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(id, state)| {
                state.txn_ctx.has_open_transaction()
                    && now.duration_since(state.last_activity) >= timeout
                    && !parked.contains(id)
            })
            .map(|(id, _)| *id)
            .collect();
        for session in &victims {
            if let Some(state) = self.sessions.get_mut(*session) {
                // The session survives, so the rollback is recorded: its next
                // batch is told the transaction is gone rather than silently
                // autocommitting statements the client means to be
                // transactional.
                engine.abort_idle_session_txn(&mut state.txn_ctx);
                state.last_activity = now;
            }
            self.locks.release_all(session.raw());
        }
        !victims.is_empty()
    }

    /// Aborts the parked batch at `idx` as a deadlock victim: rolls back its
    /// transaction, releases its locks, and replies with error 1205. The caller
    /// drains any batches the released locks unblock.
    pub(super) fn abort_parked_victim(&mut self, engine: &Engine, idx: usize, error: SqlError) {
        let victim = self.parked.remove(idx).expect("index in bounds");
        if let Some(state) = self.sessions.get_mut(victim.session) {
            engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(victim.session.raw());
        victim.reply.send_outcome(
            BatchOutcome {
                results: Vec::new(),
                error: Some(error),
            },
            false,
        );
    }

    /// Detects lock-wait *cycles* among the parked batches — a waits-for graph
    /// over the lock manager — and aborts the youngest transaction in each cycle
    /// (error 1205). A cycle can only form when a batch parks, so this runs the
    /// instant one does, breaking the deadlock immediately rather than after the
    /// wait-timeout backstop. Aborts victims until the graph is acyclic.
    pub(super) fn detect_deadlock(&mut self, engine: &Engine) {
        while let Some(idx) = self.find_deadlock_victim() {
            self.abort_parked_victim(engine, idx, deadlock_victim_error());
        }
    }

    /// The parked-queue index of a deadlock victim, or `None` if no cycle exists.
    /// Edge O -> H: a parked owner O waits for every current holder H of a
    /// resource O needs but cannot acquire. The victim is the cycle member that
    /// parked most recently (the youngest wait — the least work to roll back).
    pub(super) fn find_deadlock_victim(&self) -> Option<usize> {
        use std::collections::{HashMap, HashSet};
        // Assumes at most one parked batch per session (a session is
        // request/response, so it has at most one in-flight batch). If pipelining
        // is ever added, the per-owner edge merge and single-index abort below
        // must be revisited.
        let mut waits_for: HashMap<u64, HashSet<u64>> = HashMap::new();
        for (index, parked) in self.parked.iter().enumerate() {
            let owner = parked.session.raw();
            let edges = waits_for.entry(owner).or_default();
            for (resource, mode) in &parked.needs {
                // Held-conflict edges: owners holding a conflicting lock.
                for holder in self.locks.conflicting_holders(owner, *resource, *mode) {
                    edges.insert(holder);
                }
                // FIFO anti-barging edges: a batch yields a free resource to any
                // waiter parked ahead of it that needs the same resource (the
                // `wake_parked` grant rule), unless it already holds it. Without
                // these a deadlock routed through a queue yield would be missed.
                if !self.locks.holds(owner, *resource) {
                    for ahead in self.parked.iter().take(index) {
                        if ahead.session.raw() != owner
                            && ahead.needs.iter().any(|(r, _)| r == resource)
                        {
                            edges.insert(ahead.session.raw());
                        }
                    }
                }
            }
        }
        let cycle = find_cycle(&waits_for)?;
        self.parked
            .iter()
            .enumerate()
            .filter(|(_, p)| cycle.contains(&p.session.raw()))
            .max_by_key(|(_, p)| p.deadline)
            .map(|(i, _)| i)
    }

    /// Handles a disconnect: roll back any open transaction and release the
    /// session's locks. The caller drains anyone that was waiting on them.
    pub(super) fn close_session(&mut self, engine: &Engine, session: SessionId) {
        if let Some(mut state) = self.sessions.close(session)
            && state.txn_ctx.has_open_transaction()
        {
            engine.abort_session_txn(&mut state.txn_ctx);
        }
        self.locks.release_all(session.raw());
    }
}

/// The reply delivered to a deadlock victim: no results, error 1205, and the
/// transaction is over (it was rolled back).
/// Finds one cycle in a waits-for graph (owner -> owners it waits for), or
/// `None` if acyclic. Iterative colored DFS (white/gray/black); a back-edge to a
/// gray node on the current path is a cycle, returned as the owners composing
/// it. Nodes with no outgoing edges (a lock holder that is not itself waiting)
/// are dead ends and cannot close a cycle.
pub(super) fn find_cycle(
    graph: &std::collections::HashMap<u64, std::collections::HashSet<u64>>,
) -> Option<Vec<u64>> {
    const WHITE: u8 = 0;
    const GRAY: u8 = 1;
    const BLACK: u8 = 2;
    // Pre-seed every graph node WHITE. A neighbor absent from this map is a lock
    // holder that is not itself waiting (no outgoing edges) — a dead end, so it
    // defaults to BLACK below and cannot extend a path.
    let mut color: std::collections::HashMap<u64, u8> = graph.keys().map(|&k| (k, WHITE)).collect();
    for &root in graph.keys() {
        if color.get(&root).copied().unwrap_or(WHITE) != WHITE {
            continue;
        }
        let mut path: Vec<u64> = vec![root];
        let neighbors = |n: u64| -> std::vec::IntoIter<u64> {
            graph
                .get(&n)
                .map(|s| s.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default()
                .into_iter()
        };
        let mut iters: Vec<std::vec::IntoIter<u64>> = vec![neighbors(root)];
        color.insert(root, GRAY);
        while !iters.is_empty() {
            let next = iters.last_mut().expect("non-empty").next();
            match next {
                Some(next) => match color.get(&next).copied().unwrap_or(BLACK) {
                    WHITE => {
                        color.insert(next, GRAY);
                        path.push(next);
                        iters.push(neighbors(next));
                    }
                    GRAY => {
                        let start = path.iter().position(|&x| x == next).expect("gray on path");
                        return Some(path[start..].to_vec());
                    }
                    _ => {}
                },
                None => {
                    let done = path.pop().expect("path non-empty");
                    color.insert(done, BLACK);
                    iters.pop();
                }
            }
        }
    }
    None
}

pub(super) fn deadlock_victim_error() -> SqlError {
    SqlError::new(
        1205,
        13,
        51,
        "Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.",
    )
}

/// A lock wait that outlived the timeout behind a LIVE holder — no cycle.
/// SQL Server's number for an expired lock wait is 1222.
pub(super) fn lock_timeout_error() -> SqlError {
    SqlError::new(1222, 16, 56, "Lock request time out period exceeded.")
}
