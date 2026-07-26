use super::*;

/// The DONE acknowledging an autocommit write is never emitted before the
/// write's commit record is fsync-durable: the executor's deferred DONEs
/// flush — fsyncing first — before the next result set opens, so a client
/// can never act on an acknowledgment a crash then silently revokes.
#[test]
fn a_commit_acknowledgement_never_precedes_its_fsync() {
    use crate::engine::{
        BatchEmitter, ResultColumn, TxnContext, execute_batch, execute_batch_streamed,
    };
    use crate::relstore::types::Datum;

    /// Logs each emitted event alongside the fsync count at that moment.
    struct Probe<'a> {
        storage: &'a Storage,
        log: Vec<(&'static str, u64)>,
    }
    impl Probe<'_> {
        fn note(&mut self, what: &'static str) {
            self.log.push((what, self.storage.group_commit_fsyncs()));
        }
    }
    impl BatchEmitter for Probe<'_> {
        fn columns(&mut self, _columns: Vec<ResultColumn>) {
            self.note("columns");
        }
        fn rows(&mut self, _rows: Vec<Vec<Datum>>) {
            self.note("rows");
        }
        fn statement_done(
            &mut self,
            _count: Option<u64>,
            _in_transaction: bool,
            _command: crate::engine::DoneCommand,
        ) {
            self.note("done");
        }
        fn statement_aborted(&mut self, _in_transaction: bool) {
            self.note("aborted");
        }
    }

    let path = unique_temp_path("stream-durability");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut setup = TxnContext::default();
    let create = execute_batch(&storage, "CREATE TABLE t (v INT NOT NULL)", &mut setup);
    assert!(create.error.is_none(), "create table: {:?}", create.error);
    let baseline = storage.group_commit_fsyncs();

    let mut ctx = TxnContext::default();
    let mut probe = Probe {
        storage: &storage,
        log: Vec::new(),
    };
    let error = execute_batch_streamed(
        &storage,
        "INSERT INTO t VALUES (1); SELECT v FROM t",
        &mut ctx,
        &[],
        &mut probe,
    );
    assert!(error.is_none(), "{error:?}");
    // The INSERT's DONE comes first — already past its fsync — and only
    // then does the SELECT's result set open.
    assert_eq!(
        probe.log.first(),
        Some(&("done", baseline + 1)),
        "the write's acknowledgment waits for its fsync: {:?}",
        probe.log
    );
    assert_eq!(
        probe.log.get(1).map(|(what, _)| *what),
        Some("columns"),
        "the rowset opens after the acknowledgment: {:?}",
        probe.log
    );
    // One fsync total: the mid-batch flush covered the batch's only commit.
    assert_eq!(storage.group_commit_fsyncs(), baseline + 1);

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// An identity value reserved by an in-transaction INSERT (a mini-commit,
/// durable independently of the transaction) must not stream to the client
/// before its reservation is fsynced: a value the client has seen must
/// never be reissued after a crash, and it escapes through the *rows* of a
/// following SELECT, not through any commit-acknowledging DONE. This is
/// why the mid-batch flush gates on the kind-based `committed` flag rather
/// than on "some DONE acknowledges a commit".
#[test]
fn an_identity_value_never_streams_before_its_reservation_fsync() {
    use crate::engine::{
        BatchEmitter, ResultColumn, TxnContext, execute_batch, execute_batch_streamed,
    };
    use crate::relstore::types::Datum;

    struct Probe<'a> {
        storage: &'a Storage,
        log: Vec<(&'static str, u64)>,
    }
    impl Probe<'_> {
        fn note(&mut self, what: &'static str) {
            self.log.push((what, self.storage.group_commit_fsyncs()));
        }
    }
    impl BatchEmitter for Probe<'_> {
        fn columns(&mut self, _columns: Vec<ResultColumn>) {
            self.note("columns");
        }
        fn rows(&mut self, _rows: Vec<Vec<Datum>>) {
            self.note("rows");
        }
        fn statement_done(
            &mut self,
            _count: Option<u64>,
            _in_transaction: bool,
            _command: crate::engine::DoneCommand,
        ) {
            self.note("done");
        }
        fn statement_aborted(&mut self, _in_transaction: bool) {
            self.note("aborted");
        }
    }

    let path = unique_temp_path("stream-identity");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut setup = TxnContext::default();
    let create = execute_batch(
        &storage,
        "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, v INT NOT NULL)",
        &mut setup,
    );
    assert!(create.error.is_none(), "create table: {:?}", create.error);
    let baseline = storage.group_commit_fsyncs();

    let mut ctx = TxnContext::default();
    let mut probe = Probe {
        storage: &storage,
        log: Vec::new(),
    };
    // The INSERT's own DONE promises nothing durable (the transaction is
    // open), but its identity reservation is already a mini-commit — and
    // the SELECT's rows carry the reserved value out of the server.
    let error = execute_batch_streamed(
        &storage,
        "BEGIN TRANSACTION; INSERT INTO t (v) VALUES (10); SELECT id FROM t; ROLLBACK",
        &mut ctx,
        &[],
        &mut probe,
    );
    assert!(error.is_none(), "{error:?}");
    for (what, fsyncs) in &probe.log {
        if *what == "columns" || *what == "rows" {
            assert!(
                *fsyncs > baseline,
                "the rowset streamed before the reservation's fsync: {:?}",
                probe.log
            );
        }
    }

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// A batch of autocommit writes with nothing to stream between them still
/// coalesces to a single fsync at the end of the batch: the DONEs are
/// deferred to that durability point rather than each buying an fsync.
#[test]
fn a_write_only_batch_still_fsyncs_once() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("stream-one-fsync");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut setup = TxnContext::default();
    let create = execute_batch(&storage, "CREATE TABLE t (v INT NOT NULL)", &mut setup);
    assert!(create.error.is_none(), "create table: {:?}", create.error);
    let baseline = storage.group_commit_fsyncs();

    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        storage.group_commit_fsyncs(),
        baseline + 1,
        "three autocommit writes, one batch-end fsync"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Group commit: many transactions whose commit records are already in the
/// WAL, all waiting for durability up to the same point, are made durable by
/// a single log-writer fsync. Deterministic (independent of fsync latency):
/// the commit records are appended WITHOUT fsyncing first — `rel_insert`
/// appends its commit record but does not force the log — then every
/// committer waits on the same tail, so exactly one fsync serves them all.
#[test]
fn group_commit_coalesces_many_commits_into_one_fsync() {
    use crate::engine::{TxnContext, execute_batch};
    use std::sync::Arc;

    const THREADS: usize = 16;

    let path = unique_temp_path("group-commit");
    let storage = Arc::new(Storage::create(path.clone(), test_storage_options()).expect("create"));

    let mut setup = TxnContext::default();
    let create = execute_batch(&storage, "CREATE TABLE t (v INT NOT NULL)", &mut setup);
    assert!(create.error.is_none(), "create table: {:?}", create.error);
    let baseline = storage.group_commit_fsyncs();

    // Raw autocommit inserts append a commit record each with `sync=false`
    // and never call `ensure_durable`, so the WAL tail advances while
    // `flushed` stays put — nothing fsyncs.
    for i in 0..THREADS {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                vec![Datum::Int(i as i32)],
            )
            .expect("insert");
    }
    let target = storage.wal_tail();
    assert_eq!(
        storage.group_commit_fsyncs(),
        baseline,
        "appending commit records must not fsync"
    );

    // Every committer waits for durability up to the same tail; one fsync
    // covers them all.
    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let storage = Arc::clone(&storage);
        handles.push(std::thread::spawn(move || {
            storage.ensure_durable(target).expect("durable")
        }));
    }
    for handle in handles {
        handle.join().expect("thread panicked");
    }

    let fsyncs = storage.group_commit_fsyncs() - baseline;
    assert!(
        (1..=2).contains(&fsyncs),
        "{THREADS} commits should coalesce into a single fsync, got {fsyncs}"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// An identity value is consumed permanently (SQL Server semantics), so its
/// reservation — a mini-commit made even inside an open transaction — must be
/// fsynced. Regression: group commit skipped `ensure_durable` for a batch
/// whose only durable effect was the identity reservation (an INSERT inside
/// a still-open transaction), so a crash would revert and reuse the value.
#[test]
fn identity_reservation_is_made_durable_even_inside_a_transaction() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("identity-durable");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");

    let mut ctx = TxnContext::default();
    let create = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY IDENTITY(1,1), v INT NOT NULL)",
        &mut ctx,
    );
    assert!(create.error.is_none(), "create: {:?}", create.error);

    let before = storage.group_commit_fsyncs();
    // INSERT inside an open transaction: no row commits (the COMMIT is a
    // later batch), but the identity reservation does and must be fsynced.
    let out = execute_batch(
        &storage,
        "BEGIN TRAN; INSERT INTO t (v) VALUES (1)",
        &mut ctx,
    );
    assert!(out.error.is_none(), "insert: {:?}", out.error);
    assert!(
        storage.group_commit_fsyncs() > before,
        "identity reservation inside a transaction must be made durable"
    );

    let _ = execute_batch(&storage, "ROLLBACK", &mut ctx);
    drop(storage);
    let _ = std::fs::remove_file(&path);
}
