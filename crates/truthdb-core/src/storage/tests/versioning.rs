use super::*;

/// A covering (INCLUDE) seek must serve the snapshot's image, not the
/// index leaf's freshly-updated include payload — pinned on the covering
/// path itself via the covering-scan counter.
#[test]
fn covering_seek_serves_the_snapshot_image() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("rcsi-covering");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut writer = TxnContext::default();
    let mut fill = String::from("INSERT INTO c VALUES ");
    for i in 1..=25 {
        fill.push_str(&format!("({}, {}, {}),", i, i * 10, i * 1000));
    }
    fill.pop();
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE c (id INT NOT NULL PRIMARY KEY, v INT, w INT)",
        fill.as_str(),
        "CREATE INDEX ix_cv ON c (v) INCLUDE (v, w)",
        // The open transaction updates only the INCLUDE column: the
        // seek still finds the entry under v = 10, now carrying the NEW
        // include bytes.
        "BEGIN TRAN; UPDATE c SET w = 777 WHERE id = 1;",
    ] {
        let outcome = execute_batch(&storage, sql, &mut writer);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }

    let select = |label: &str| {
        let mut reader = TxnContext::default();
        let outcome = execute_batch(&storage, "SELECT w FROM c WHERE v = 10", &mut reader);
        assert!(outcome.error.is_none(), "{label}: {:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => rowset.rows.clone(),
            other => panic!("{label}: expected rows, got {other:?}"),
        }
    };

    let covering_before = storage.covering_scans();
    let rows = select("during the writer's transaction");
    assert!(
        storage.covering_scans() > covering_before,
        "the read must have gone down the covering path for this to test anything"
    );
    assert_eq!(
        rows,
        vec![vec![Datum::Int(1000)]],
        "the snapshot image, not the new include payload"
    );

    let outcome = execute_batch(&storage, "COMMIT", &mut writer);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(rows_i32(&select("after the commit")), vec![777]);

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Rollback unpublishes: a rolled-back transaction leaves no version
/// chains behind (pruning would otherwise treat its entries as an open
/// transaction's and pin them forever), and pruning drops the history of
/// committed transactions once no snapshot is live.
#[test]
fn rollback_unpublishes_and_prune_drops_settled_history() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("rcsi-prune");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }

    // The seed INSERTs published their own (committed) chains; settle
    // them first so the rollback assertion sees only the rollback's work.
    storage
        .ensure_durable(storage.wal_tail())
        .expect("durability");
    storage.version_prune();
    assert_eq!(storage.version_chain_count("t"), 0);

    // Rolled back: the chains its statements published are reversed.
    let outcome = execute_batch(
        &storage,
        "BEGIN TRAN; UPDATE t SET v = 99 WHERE id = 1; ROLLBACK",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        storage.version_chain_count("t"),
        0,
        "a rolled-back transaction leaves no chains"
    );

    // Committed: the chain exists until pruning decides nothing can need
    // it (no live snapshot, commit durable).
    let outcome = execute_batch(&storage, "UPDATE t SET v = 11 WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(storage.version_chain_count("t"), 1);
    storage
        .ensure_durable(storage.wal_tail())
        .expect("durability");
    storage.version_prune();
    assert_eq!(
        storage.version_chain_count("t"),
        0,
        "settled history is dropped by the maintenance prune"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Version cleanup under load (Stage 13 exit): a live snapshot pins
/// exactly the history it may still read through sustained churn — and
/// keeps reading its own consistent view — while releasing it lets the
/// maintenance prune drop everything.
#[test]
fn version_cleanup_under_load_pins_then_drops_history() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("cleanup-load");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let mut seed = String::from("INSERT INTO t VALUES ");
    for i in 0..100 {
        seed.push_str(&format!("({i}, 0),"));
    }
    seed.pop();
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        seed.as_str(),
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    storage
        .ensure_durable(storage.wal_tail())
        .expect("durability");
    storage.version_prune();
    assert_eq!(storage.version_chain_count("t"), 0, "settled baseline");

    // A long-lived snapshot (an idle SNAPSHOT transaction's view) while
    // a writer churns every row, five rounds, pruning between rounds.
    let pinned = storage.capture_read_snapshot(None);
    for round in 1..=5 {
        let outcome = execute_batch(&storage, &format!("UPDATE t SET v = {round}"), &mut ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        storage
            .ensure_durable(storage.wal_tail())
            .expect("durability");
        storage.version_prune();
    }
    assert_eq!(
        storage.version_chain_count("t"),
        100,
        "the live snapshot pins one chain per churned row"
    );
    // The pinned view still reads its consistent state through all of it.
    let rows = storage
        .rel_scan_snapshot(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            Some(&[1]),
            pinned,
        )
        .expect("snapshot scan");
    assert_eq!(rows.len(), 100);
    assert!(
        rows.iter().all(|r| r == &vec![Datum::Int(0)]),
        "the snapshot sees the pre-churn value on every row"
    );

    // Released: the next prune drops the whole store.
    storage.release_read_snapshot(pinned.seq);
    storage.version_prune();
    assert_eq!(
        storage.version_chain_count("t"),
        0,
        "released history is dropped, the store is bounded"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// SI review PoC: the 3960 auto-abort path must release the
/// transaction's snapshot registration - a leak would pin the prune
/// watermark forever. Observable through pruning: while the snapshot is
/// registered the conflicting chain must survive a prune; after the 3960
/// rolled the transaction back, the same prune must drop it.
#[test]
fn a_3960_abort_releases_the_snapshot_registration() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("si-3960-release");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut setup = TxnContext::default();
    for sql in [
        "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON",
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
        "INSERT INTO t VALUES (1, 10)",
    ] {
        let outcome = execute_batch(&storage, sql, &mut setup);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    storage
        .ensure_durable(storage.wal_tail())
        .expect("durability");
    storage.version_prune();
    assert_eq!(storage.version_chain_count("t"), 0);

    // B: SNAPSHOT transaction, snapshot captured at first access.
    let mut b = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; BEGIN TRAN; SELECT v FROM t",
        &mut b,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // A: a conflicting committed write B's snapshot cannot see.
    let mut a = TxnContext::default();
    let outcome = execute_batch(&storage, "UPDATE t SET v = 99 WHERE id = 1", &mut a);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // While B's snapshot is registered, its history is pinned.
    storage
        .ensure_durable(storage.wal_tail())
        .expect("durability");
    storage.version_prune();
    assert_eq!(
        storage.version_chain_count("t"),
        1,
        "a registered snapshot pins the chain"
    );

    // B writes the same row: 3960, and the whole transaction (with its
    // snapshot registration) must be gone.
    let outcome = execute_batch(&storage, "UPDATE t SET v = 100 WHERE id = 1", &mut b);
    assert_eq!(
        outcome.error.as_ref().map(|e| e.number),
        Some(3960),
        "{:?}",
        outcome.error
    );

    storage.version_prune();
    assert_eq!(
        storage.version_chain_count("t"),
        0,
        "the 3960 abort must release the snapshot registration"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The Stage 14 exit criterion: a 10 MiB value round-trips through an
/// overflow chain, survives a kill-and-reopen, and a mid-transaction
/// update of it recovers to the committed value.
#[test]
fn ten_mib_value_round_trips_and_survives_a_crash() {
    use crate::engine::RpcParam;
    use crate::engine::{StatementResult, TxnContext, execute_batch, execute_batch_with_params};

    let path = unique_temp_path("max-10mib");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE big (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))",
        "INSERT INTO big VALUES (1, N'seed')",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    // 10 MiB of UTF-16 payload = 5 * 1024 * 1024 characters.
    let big: String = "abcdefgh".repeat(5 * 1024 * 1024 / 8);
    assert_eq!(big.encode_utf16().count() * 2, 10 * 1024 * 1024);
    let outcome = execute_batch_with_params(
        &storage,
        "UPDATE big SET body = @v WHERE id = 1",
        &mut ctx,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: crate::relstore::types::Datum::NVarChar(big.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    let fetch = |storage: &Storage, ctx: &mut TxnContext| -> String {
        let outcome = execute_batch(storage, "SELECT body FROM big WHERE id = 1", ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => match &rowset.rows[0][0] {
                Datum::NVarChar(s) => s.clone(),
                other => panic!("expected NVARCHAR, got {other:?}"),
            },
            other => panic!("expected rows, got {other:?}"),
        }
    };
    assert_eq!(
        fetch(&storage, &mut ctx),
        big,
        "round-trip before the crash"
    );

    // An in-flight update dies with the crash; the committed 10 MiB
    // value must recover intact.
    let outcome = execute_batch(
        &storage,
        "BEGIN TRAN; UPDATE big SET body = N'doomed' WHERE id = 1;",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let _ = ctx;
    drop(storage);

    let storage = Storage::open(path.clone()).expect("recovery");
    let mut ctx = TxnContext::default();
    let recovered = fetch(&storage, &mut ctx);
    assert_eq!(recovered.len(), big.len(), "length after recovery");
    assert_eq!(
        recovered, big,
        "the committed chain survives, the loser is undone"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Versioning over (MAX): an RCSI reader sees the pre-update big value
/// through the version image's overflow REFERENCE (images carry raw row
/// bytes; chains are immutable and never freed, so the ref stays valid).
#[test]
fn rcsi_reads_the_old_big_value_through_the_image_reference() {
    use crate::engine::{
        RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
    };

    let path = unique_temp_path("max-rcsi");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut writer = TxnContext::default();
    let big_old: String = "old-value".repeat(20_000); // ~180 KB
    let big_new: String = "new-value".repeat(20_000);
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE big (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))",
    ] {
        let outcome = execute_batch(&storage, sql, &mut writer);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let outcome = execute_batch_with_params(
        &storage,
        "INSERT INTO big VALUES (1, @v)",
        &mut writer,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big_old.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // Writer holds an uncommitted update to the big value...
    let outcome = execute_batch(&storage, "BEGIN TRAN", &mut writer);
    assert!(outcome.error.is_none());
    let outcome = execute_batch_with_params(
        &storage,
        "UPDATE big SET body = @v WHERE id = 1",
        &mut writer,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big_new.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // ...and a snapshot reader gets the OLD value, resolved through the
    // image's overflow reference.
    let mut reader = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT body FROM big WHERE id = 1", &mut reader);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::NVarChar(big_old.clone()));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let outcome = execute_batch(&storage, "COMMIT", &mut writer);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let outcome = execute_batch(&storage, "SELECT body FROM big WHERE id = 1", &mut reader);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::NVarChar(big_new.clone()));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Review PoC: HEAP tables' version priors come from `heap.read_row`
/// pre-images. An RCSI reader must see the pre-update big value of a heap
/// row (resolved through the image's overflow reference), and a deleted
/// heap row must stay visible to the open snapshot.
#[test]
fn heap_rcsi_reads_old_big_value_through_preimage() {
    use crate::engine::{
        RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
    };

    let path = unique_temp_path("review-heap-rcsi");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut writer = TxnContext::default();
    let big_old: String = "old-heap".repeat(10_000); // 80k chars -> chain
    let big_new: String = "new-heap".repeat(10_000);
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        // No PRIMARY KEY: a heap.
        "CREATE TABLE hbig (id INT NOT NULL, body NVARCHAR(MAX))",
    ] {
        let outcome = execute_batch(&storage, sql, &mut writer);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let outcome = execute_batch_with_params(
        &storage,
        "INSERT INTO hbig VALUES (1, @v)",
        &mut writer,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big_old.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    let fetch = |ctx: &mut TxnContext| -> Option<Datum> {
        let outcome = execute_batch(&storage, "SELECT body FROM hbig WHERE id = 1", ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => rowset.rows.first().map(|r| r[0].clone()),
            other => panic!("expected rows, got {other:?}"),
        }
    };

    // Uncommitted UPDATE: the reader sees the old value via the image.
    let outcome = execute_batch(&storage, "BEGIN TRAN", &mut writer);
    assert!(outcome.error.is_none());
    let outcome = execute_batch_with_params(
        &storage,
        "UPDATE hbig SET body = @v WHERE id = 1",
        &mut writer,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big_new.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let mut reader = TxnContext::default();
    assert_eq!(
        fetch(&mut reader),
        Some(Datum::NVarChar(big_old.clone())),
        "heap RCSI reader must get the pre-update value"
    );
    let outcome = execute_batch(&storage, "COMMIT", &mut writer);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(fetch(&mut reader), Some(Datum::NVarChar(big_new.clone())));

    // Uncommitted DELETE: the reader still sees the (new) value.
    let outcome = execute_batch(
        &storage,
        "BEGIN TRAN; DELETE FROM hbig WHERE id = 1;",
        &mut writer,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        fetch(&mut reader),
        Some(Datum::NVarChar(big_new.clone())),
        "heap RCSI reader must see the row an open txn deleted"
    );
    let outcome = execute_batch(&storage, "COMMIT", &mut writer);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(fetch(&mut reader), None);

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Review PoC: a heap row that has MOVED (forwarding stub at its home
/// RID) must still read correctly, version correctly (the pre-image is
/// read through the stub), and keep its overflow value.
#[test]
fn moved_heap_row_versions_and_resolves_through_the_stub() {
    use crate::engine::{
        RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
    };

    let path = unique_temp_path("review-heap-moved");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut writer = TxnContext::default();
    let big_old: String = "moved-old".repeat(5_000); // 45k chars -> chain
    let big_new: String = "moved-new".repeat(5_000);
    for sql in [
        "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON",
        "CREATE TABLE hm (id INT NOT NULL, pad VARCHAR(3000), body NVARCHAR(MAX))",
    ] {
        let outcome = execute_batch(&storage, sql, &mut writer);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    // Row 1 small; row 2 fills most of the first heap page, so growing
    // row 1's pad to 3000 bytes cannot fit and must move the row.
    let outcome = execute_batch_with_params(
        &storage,
        "INSERT INTO hm VALUES (1, 'a', @v)",
        &mut writer,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big_old.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let filler = "f".repeat(3000);
    let outcome = execute_batch(
        &storage,
        &format!("INSERT INTO hm VALUES (2, '{filler}', NULL)"),
        &mut writer,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let grown = "g".repeat(3000);
    let outcome = execute_batch(
        &storage,
        &format!("UPDATE hm SET pad = '{grown}' WHERE id = 1"),
        &mut writer,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    // The moved row still resolves its chain.
    let fetch_body = |ctx: &mut TxnContext| -> Option<Datum> {
        let outcome = execute_batch(&storage, "SELECT body FROM hm WHERE id = 1", ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => rowset.rows.first().map(|r| r[0].clone()),
            other => panic!("expected rows, got {other:?}"),
        }
    };
    assert_eq!(
        fetch_body(&mut writer),
        Some(Datum::NVarChar(big_old.clone())),
        "moved row reads its chain"
    );

    // Version an update of the moved row: the prior must be read through
    // the forwarding stub, and the reader must get the old big value.
    let outcome = execute_batch(&storage, "BEGIN TRAN", &mut writer);
    assert!(outcome.error.is_none());
    let outcome = execute_batch_with_params(
        &storage,
        "UPDATE hm SET body = @v WHERE id = 1",
        &mut writer,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big_new.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let mut reader = TxnContext::default();
    assert_eq!(
        fetch_body(&mut reader),
        Some(Datum::NVarChar(big_old.clone())),
        "RCSI reader must get the pre-update value of a MOVED heap row"
    );
    // The pad read through the same image must be the grown one.
    let outcome = execute_batch(&storage, "SELECT pad FROM hm WHERE id = 1", &mut reader);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::VarChar(grown.clone()));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let outcome = execute_batch(&storage, "COMMIT", &mut writer);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(fetch_body(&mut reader), Some(Datum::NVarChar(big_new)));

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Review PoC: a statement that spills a chain and THEN fails (duplicate
/// key on a later row) must roll back cleanly — the chain leaks, the rows
/// do not land, the store stays usable, and recovery after a kill agrees.
#[test]
fn failed_statement_after_spill_rolls_back_cleanly() {
    use crate::engine::{
        RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
    };

    let path = unique_temp_path("review-spill-fail");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE TABLE sf (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))",
        "INSERT INTO sf VALUES (7, N'anchor')",
    ] {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let big: String = "spilled".repeat(10_000); // 70k chars -> chain
    // Row 1 spills its chain, row 2 hits the duplicate key: the whole
    // statement must fail and undo row 1.
    let outcome = execute_batch_with_params(
        &storage,
        "INSERT INTO sf VALUES (1, @v), (7, N'dup')",
        &mut ctx,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big.clone()),
        }],
    );
    assert!(outcome.error.is_some(), "duplicate key must fail");

    let count = |ctx: &mut TxnContext| -> Datum {
        let outcome = execute_batch(&storage, "SELECT COUNT(*) FROM sf", ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => rowset.rows[0][0].clone(),
            other => panic!("expected rows, got {other:?}"),
        }
    };
    assert_eq!(count(&mut ctx), Datum::BigInt(1), "only the anchor row");

    // The store is still usable: the same big value inserts fine now.
    let outcome = execute_batch_with_params(
        &storage,
        "INSERT INTO sf VALUES (1, @v)",
        &mut ctx,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::NVarCharMax,
            value: Datum::NVarChar(big.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(count(&mut ctx), Datum::BigInt(2));

    // Kill and reopen: recovery replays the leaked chain's images and
    // the committed rows; the value survives.
    drop(storage);
    let storage = Storage::open(path.clone()).expect("recovery");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT body FROM sf WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::NVarChar(big));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Review PoC: ALTER TABLE ADD on a table with spilled (MAX) values.
/// The rewrite resolves every row and re-encodes it WITHOUT re-spilling,
/// so a chain value small enough for the row cap is silently re-inlined
/// (and must survive), while a big one fails the whole ALTER — which must
/// fail CLEANLY, leaving the table intact and the store usable.
#[test]
fn alter_add_column_respills_max_values() {
    use crate::engine::{
        RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
    };

    let path = unique_temp_path("review-alter-max");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();

    // Case A: a 300-byte chain value fits back inline; ALTER succeeds.
    let small_chain = "s".repeat(300); // VARCHAR: 300 bytes > 256 -> chain
    for sql in [
        "CREATE TABLE amax (id INT NOT NULL PRIMARY KEY, body VARCHAR(MAX))".to_string(),
        format!("INSERT INTO amax VALUES (1, '{small_chain}')"),
        "ALTER TABLE amax ADD extra INT NULL".to_string(),
    ] {
        let outcome = execute_batch(&storage, &sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let outcome = execute_batch(&storage, "SELECT body, extra FROM amax", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::VarChar(small_chain));
            assert_eq!(rowset.rows[0][1], Datum::Null);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // Case B: a 10k value re-spills to a fresh chain during the ALTER's
    // rewrite (the review found the original rewrite re-inlined and
    // failed; the re-encode now runs inside the statement with
    // spill_max_values, like every other write path).
    let big: String = "b".repeat(10_000);
    let outcome = execute_batch_with_params(
        &storage,
        "CREATE TABLE bmax (id INT NOT NULL PRIMARY KEY, body VARCHAR(MAX)); \
         INSERT INTO bmax VALUES (1, @v);",
        &mut ctx,
        &[RpcParam {
            name: "@v".into(),
            column_type: ColumnType::VarCharMax,
            value: Datum::VarChar(big.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let outcome = execute_batch(&storage, "ALTER TABLE bmax ADD extra INT NULL", &mut ctx);
    assert!(
        outcome.error.is_none(),
        "the rewrite must spill, not re-inline: {:?}",
        outcome.error
    );
    let outcome = execute_batch(
        &storage,
        "SELECT body, extra FROM bmax WHERE id = 1",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::VarChar(big.clone()));
            assert_eq!(rowset.rows[0][1], Datum::Null, "the frozen fill");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    // ...and the widened row survives a reopen (the fresh chain is
    // durable with the ALTER's statement).
    drop(ctx);
    drop(storage);
    let storage = Storage::open(path.clone()).expect("reopen");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(&storage, "SELECT body FROM bmax WHERE id = 1", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows[0][0], Datum::VarChar(big));
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let outcome = execute_batch(&storage, "INSERT INTO bmax VALUES (2, 'ok', 5)", &mut ctx);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Review PoC: codec edges — empty string, the 256/257 inline threshold,
/// NULL, and a VARBINARY(MAX) value — all round-trip, including across a
/// reopen.
#[test]
fn max_codec_edges_round_trip() {
    use crate::engine::{
        RpcParam, StatementResult, TxnContext, execute_batch, execute_batch_with_params,
    };

    let path = unique_temp_path("review-max-edges");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let at_threshold = "t".repeat(256); // inline boundary
    let over_threshold = "u".repeat(257); // first chained length
    for sql in [
        "CREATE TABLE edges (id INT NOT NULL PRIMARY KEY, v VARCHAR(MAX), b VARBINARY(MAX))"
            .to_string(),
        "INSERT INTO edges VALUES (1, '', NULL)".to_string(),
        format!("INSERT INTO edges VALUES (2, '{at_threshold}', NULL)"),
        format!("INSERT INTO edges VALUES (3, '{over_threshold}', NULL)"),
        "INSERT INTO edges VALUES (4, NULL, NULL)".to_string(),
    ] {
        let outcome = execute_batch(&storage, &sql, &mut ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    }
    let blob = vec![0xABu8; 300]; // > 256 -> chain
    let outcome = execute_batch_with_params(
        &storage,
        "INSERT INTO edges VALUES (5, NULL, @b)",
        &mut ctx,
        &[RpcParam {
            name: "@b".into(),
            column_type: ColumnType::VarBinaryMax,
            value: Datum::VarBinary(blob.clone()),
        }],
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);

    let check = |storage: &Storage, ctx: &mut TxnContext| {
        let outcome = execute_batch(storage, "SELECT v, b FROM edges ORDER BY id", ctx);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => {
                assert_eq!(rowset.rows[0][0], Datum::VarChar(String::new()), "empty");
                assert_eq!(rowset.rows[1][0], Datum::VarChar(at_threshold.clone()));
                assert_eq!(rowset.rows[2][0], Datum::VarChar(over_threshold.clone()));
                assert_eq!(rowset.rows[3][0], Datum::Null);
                assert_eq!(rowset.rows[4][1], Datum::VarBinary(blob.clone()));
            }
            other => panic!("expected rows, got {other:?}"),
        }
    };
    check(&storage, &mut ctx);
    drop(storage);
    let storage = Storage::open(path.clone()).expect("reopen");
    let mut ctx = TxnContext::default();
    check(&storage, &mut ctx);
    drop(storage);
    let _ = std::fs::remove_file(&path);
}
