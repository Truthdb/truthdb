//! Stage 2 exit-criteria tests: kill-and-recover matrix, CLR idempotence,
//! torn-page FPI repair, B+ tree vs BTreeMap oracle, split-crash, heap
//! forwarding stubs and statement rollback.

use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::relstore::row::Column;
use crate::relstore::types::{ColumnType, Datum};
use crate::storage::{Storage, StorageError, StorageOptions};

/// Room for FPIs and split images without checkpoints in most tests.
const REL_TEST_WAL_BYTES: u64 = 8 * 1024 * 1024;

fn storage_options() -> StorageOptions {
    StorageOptions {
        size_gib: 1,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
    }
}

fn unique_temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    path.push(format!("truthdb-rel-{label}-{nanos}.db"));
    path
}

fn create_storage(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        storage_options(),
        REL_TEST_WAL_BYTES,
        REL_TEST_WAL_BYTES,
    )
    .expect("create storage")
}

fn overwrite_bytes(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for corruption");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(bytes).expect("write");
    file.sync_all().expect("sync");
}

fn int_column(name: &str, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        column_type: ColumnType::Int,
        nullable,
        collation: None,
    }
}

fn varchar_column(name: &str, max_len: u16) -> Column {
    Column {
        name: name.to_string(),
        column_type: ColumnType::VarChar { max_len },
        nullable: true,
        collation: None,
    }
}

fn create_tree_table(storage: &mut Storage, name: &str) {
    storage
        .rel_create_table(
            name,
            vec![int_column("id", false), varchar_column("payload", 4000)],
            &["id".to_string()],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create tree table");
}

fn create_heap_table(storage: &mut Storage, name: &str) {
    storage
        .rel_create_table(
            name,
            vec![int_column("id", false), varchar_column("payload", 4000)],
            &[],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create heap table");
}

fn row(id: i32, payload: &str) -> Vec<Datum> {
    vec![Datum::Int(id), Datum::VarChar(payload.to_string())]
}

fn scan_ids(storage: &mut Storage, table: &str) -> Vec<i32> {
    storage
        .rel_scan(table)
        .expect("scan")
        .into_iter()
        .map(|r| match r[0] {
            Datum::Int(id) => id,
            _ => panic!("expected int id"),
        })
        .collect()
}

#[test]
fn committed_statements_survive_crash_without_checkpoint() {
    let path = unique_temp_path("committed-durable");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    create_heap_table(&mut storage, "h");
    for i in 0..20 {
        storage
            .rel_insert("t", row(i, &format!("tree-{i}")))
            .expect("insert");
        storage
            .rel_insert("h", row(i, &format!("heap-{i}")))
            .expect("insert");
    }
    storage
        .rel_delete_where("t", "id", &Datum::Int(3))
        .expect("delete");
    storage
        .rel_update_where(
            "t",
            "id",
            &Datum::Int(4),
            &[("payload".to_string(), Datum::VarChar("updated".to_string()))],
        )
        .expect("update");
    drop(storage); // crash: nothing checkpointed, pool never flushed

    let mut storage = Storage::open(path.clone()).expect("reopen");
    let ids = scan_ids(&mut storage, "t");
    assert_eq!(ids, (0..20).filter(|i| *i != 3).collect::<Vec<_>>());
    let updated = storage
        .rel_get("t", &[Datum::Int(4)])
        .expect("get")
        .expect("row 4 exists");
    assert_eq!(updated[1], Datum::VarChar("updated".to_string()));
    assert_eq!(scan_ids(&mut storage, "h").len(), 20);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn active_transaction_count_tracks_open_transactions() {
    // The active-transaction set (which a fuzzy checkpoint clamps the WAL head
    // to) must track explicit transactions across both the commit and rollback
    // paths, so `has_active_transactions` flips on begin and off on end.
    let path = unique_temp_path("active-txn-gate");
    let mut storage = create_storage(&path);
    assert!(!storage.has_active_transactions());

    let txn = storage.rel_begin().expect("begin");
    assert!(
        storage.has_active_transactions(),
        "open transaction is active"
    );
    storage.rel_commit(txn).expect("commit");
    assert!(
        !storage.has_active_transactions(),
        "commit clears the active transaction"
    );

    let txn = storage.rel_begin().expect("begin");
    assert!(storage.has_active_transactions());
    storage.rel_rollback(txn).expect("rollback");
    assert!(
        !storage.has_active_transactions(),
        "rollback clears the active transaction"
    );

    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn fuzzy_checkpoint_with_open_txn_then_crash_undoes_it() {
    // A checkpoint may now run WHILE an explicit transaction is open. It flushes
    // the txn's uncommitted page but clamps the WAL head to the txn's begin LSN,
    // so its undo survives. On crash the open txn is rolled back; a row committed
    // before the checkpoint survives.
    use crate::storage::TxnScope;
    let path = unique_temp_path("fuzzy-ckpt-crash");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    storage
        .rel_insert("t", row(1, "committed"))
        .expect("insert 1");

    let mut txn = storage.rel_begin().expect("begin");
    storage
        .rel_insert_many(
            "t",
            vec![row(99, "uncommitted")],
            &mut TxnScope::Explicit(&mut txn),
        )
        .expect("insert 99 under txn");
    assert!(storage.has_active_transactions());

    // Fuzzy checkpoint with the transaction still open (previously refused).
    storage
        .write_checkpoint(b"fuzzy", 1, 2, 1)
        .expect("checkpoint runs with an open transaction");

    drop(txn); // crash before commit
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        scan_ids(&mut storage, "t"),
        vec![1],
        "open txn rolled back after the fuzzy checkpoint; committed row survives"
    );
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn fuzzy_checkpoint_then_commit_survives_crash() {
    // Work done both before AND after a fuzzy checkpoint, then committed, must
    // survive a crash — the checkpoint clamped the WAL head to the txn's begin,
    // so redo replays everything the commit made durable.
    use crate::storage::TxnScope;
    let path = unique_temp_path("fuzzy-ckpt-commit");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");

    let mut txn = storage.rel_begin().expect("begin");
    storage
        .rel_insert_many(
            "t",
            vec![row(50, "before-ckpt")],
            &mut TxnScope::Explicit(&mut txn),
        )
        .expect("insert 50");
    storage
        .write_checkpoint(b"fuzzy", 1, 2, 1)
        .expect("checkpoint with open txn");
    storage
        .rel_insert_many(
            "t",
            vec![row(51, "after-ckpt")],
            &mut TxnScope::Explicit(&mut txn),
        )
        .expect("insert 51");
    storage.rel_commit(txn).expect("commit forces the log");

    drop(storage); // crash after commit

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        scan_ids(&mut storage, "t"),
        vec![50, 51],
        "both pre- and post-checkpoint rows of the committed txn survive"
    );
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn uncommitted_statement_is_undone_and_recovery_rerun_is_clean() {
    let path = unique_temp_path("loser-undo");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    create_heap_table(&mut storage, "h");
    storage
        .rel_insert("t", row(1, "committed"))
        .expect("insert");
    storage
        .rel_insert("h", row(1, "committed"))
        .expect("insert");
    // Crash mid-statement: ops durable, commit record never written.
    storage
        .rel_insert_without_commit("t", row(2, "uncommitted"))
        .expect("uncommitted tree insert");
    storage
        .rel_insert_without_commit("h", row(2, "uncommitted"))
        .expect("uncommitted heap insert");
    drop(storage);

    // First recovery: losers undone via CLRs.
    let mut storage = Storage::open(path.clone()).expect("reopen with losers");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1], "loser insert undone");
    assert_eq!(scan_ids(&mut storage, "h"), vec![1], "loser insert undone");
    drop(storage); // crash again before any checkpoint: CLRs replay

    // Second recovery re-runs redo over the CLRs (idempotence) and must not
    // resurrect or double-undo anything.
    let mut storage = Storage::open(path.clone()).expect("reopen after recovery crash");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1]);
    assert_eq!(scan_ids(&mut storage, "h"), vec![1]);
    // The store stays fully usable.
    storage
        .rel_insert("t", row(2, "fresh"))
        .expect("insert after recovery");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1, 2]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn torn_page_is_repaired_from_full_page_image() {
    let path = unique_temp_path("torn-page-fpi");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    for i in 0..5 {
        storage.rel_insert("t", row(i, "payload")).expect("insert");
    }
    // Flush dirty pages to disk without advancing the WAL head (the
    // mid-checkpoint crash window), then tear the table's root page.
    storage.rel_flush_pool_only().expect("flush");
    let root_page = storage.rel_table("t").expect("def").root_page;
    let offset = storage.data_page_offset(root_page);
    drop(storage);
    overwrite_bytes(&path, offset + 1000, &[0xDBu8; 2000]);

    let mut storage = Storage::open(path.clone()).expect("reopen after tear");
    assert_eq!(scan_ids(&mut storage, "t"), vec![0, 1, 2, 3, 4]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Deterministic xorshift so the oracle test needs no new deps.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[test]
fn btree_matches_btreemap_oracle_through_splits_and_crash() {
    let path = unique_temp_path("btree-oracle");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    let mut oracle: BTreeMap<i32, String> = BTreeMap::new();
    let mut rng = Rng(0x5EED_5EED_5EED_5EED);

    for step in 0..900 {
        let id = (rng.next() % 300) as i32;
        match rng.next() % 4 {
            // Insert (duplicate key must be rejected and change nothing).
            0 | 1 => {
                let payload = format!("{id}-{}", "x".repeat(180 + (rng.next() % 200) as usize));
                let result = storage.rel_insert("t", row(id, &payload));
                match oracle.entry(id) {
                    std::collections::btree_map::Entry::Occupied(_) => assert!(
                        matches!(result, Err(StorageError::Constraint(_))),
                        "duplicate insert must fail"
                    ),
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        result.expect("insert");
                        slot.insert(payload);
                    }
                }
            }
            2 => {
                let expected = oracle.remove(&id).is_some();
                let count = storage
                    .rel_delete_where("t", "id", &Datum::Int(id))
                    .expect("delete");
                assert_eq!(count == 1, expected, "delete count diverged");
            }
            _ => {
                let payload = format!("{id}-upd-{}", "y".repeat(100 + (rng.next() % 500) as usize));
                let count = storage
                    .rel_update_where(
                        "t",
                        "id",
                        &Datum::Int(id),
                        &[("payload".to_string(), Datum::VarChar(payload.clone()))],
                    )
                    .expect("update");
                if let std::collections::btree_map::Entry::Occupied(mut entry) = oracle.entry(id) {
                    assert_eq!(count, 1);
                    entry.insert(payload);
                } else {
                    assert_eq!(count, 0);
                }
            }
        }
        // Periodic point-lookup and checkpoint (fresh FPI epochs).
        if step % 97 == 0 {
            let got = storage.rel_get("t", &[Datum::Int(id)]).expect("get");
            assert_eq!(got.is_some(), oracle.contains_key(&id));
            storage
                .write_checkpoint(b"oracle-checkpoint", 1, 2, 1)
                .expect("checkpoint");
        }
    }

    let verify = |storage: &mut Storage, oracle: &BTreeMap<i32, String>| {
        let rows = storage.rel_scan("t").expect("scan");
        assert_eq!(rows.len(), oracle.len(), "row count diverged");
        for (row, (id, payload)) in rows.iter().zip(oracle.iter()) {
            assert_eq!(row[0], Datum::Int(*id), "scan must be in key order");
            assert_eq!(row[1], Datum::VarChar(payload.clone()));
        }
    };
    verify(&mut storage, &oracle);
    drop(storage); // crash without a final checkpoint

    let mut storage = Storage::open(path.clone()).expect("reopen");
    verify(&mut storage, &oracle);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn multi_level_splits_survive_crash() {
    let path = unique_temp_path("split-crash");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    // ~400-byte rows: ~9 per leaf; 300 rows forces a multi-level tree.
    // Insert in descending order to exercise inserts at position 0.
    for i in (0..300).rev() {
        let payload = format!("{i}-{}", "z".repeat(380));
        storage.rel_insert("t", row(i, &payload)).expect("insert");
    }
    drop(storage); // crash: splits only exist as WAL images

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(scan_ids(&mut storage, "t"), (0..300).collect::<Vec<_>>());
    // The recovered tree keeps working (routing, further splits).
    for i in 300..340 {
        let payload = format!("{i}-{}", "z".repeat(380));
        storage
            .rel_insert("t", row(i, &payload))
            .expect("insert after recovery");
    }
    assert_eq!(scan_ids(&mut storage, "t"), (0..340).collect::<Vec<_>>());
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn heap_updates_move_rows_with_forwarding_stubs() {
    let path = unique_temp_path("heap-stubs");
    let mut storage = create_storage(&path);
    create_heap_table(&mut storage, "h");
    // Fill the first page almost completely.
    for i in 0..3 {
        storage
            .rel_insert("h", row(i, &"a".repeat(1200)))
            .expect("insert");
    }
    // Growing row 0 beyond the page's free space forces a move + stub.
    let count = storage
        .rel_update_where(
            "h",
            "id",
            &Datum::Int(0),
            &[("payload".to_string(), Datum::VarChar("B".repeat(3000)))],
        )
        .expect("update");
    assert_eq!(count, 1);
    let mut ids = scan_ids(&mut storage, "h");
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2], "moved row appears exactly once");
    drop(storage); // crash

    let mut storage = Storage::open(path.clone()).expect("reopen");
    let rows = storage.rel_scan("h").expect("scan");
    let moved = rows
        .iter()
        .find(|r| r[0] == Datum::Int(0))
        .expect("moved row survives");
    assert_eq!(moved[1], Datum::VarChar("B".repeat(3000)));
    // Deleting through the stub removes the row entirely.
    assert_eq!(
        storage
            .rel_delete_where("h", "id", &Datum::Int(0))
            .expect("delete"),
        1
    );
    let mut ids = scan_ids(&mut storage, "h");
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn failing_statement_rolls_back_all_its_rows() {
    let path = unique_temp_path("statement-rollback");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    for i in 0..5 {
        storage.rel_insert("t", row(i, "small")).expect("insert");
    }
    // A multi-row update where the grown rows exceed the tree cell cap: the
    // first rows update fine, then the statement fails and must roll back.
    let err = storage
        .rel_update_where(
            "t",
            "payload",
            &Datum::VarChar("small".to_string()),
            &[("payload".to_string(), Datum::VarChar("W".repeat(3000)))],
        )
        .expect_err("oversized tree rows must fail the statement");
    assert!(matches!(err, StorageError::InvalidConfig(_)), "got: {err}");
    let rows = storage.rel_scan("t").expect("scan");
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(
            row[1],
            Datum::VarChar("small".to_string()),
            "no partial update may survive rollback"
        );
    }
    // And the same holds across a crash (the rollback CLRs replay).
    drop(storage);
    let mut storage = Storage::open(path.clone()).expect("reopen");
    let rows = storage.rel_scan("t").expect("scan");
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row[1], Datum::VarChar("small".to_string()));
    }
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn create_table_crash_before_commit_rolls_back_catalog() {
    let path = unique_temp_path("create-table-loser");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "keep");
    storage.rel_insert("keep", row(1, "x")).expect("insert");
    drop(storage);

    // Committed table survives; the catalog itself recovered.
    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert!(storage.rel_table("keep").is_some());
    assert_eq!(scan_ids(&mut storage, "keep"), vec![1]);

    // NOT NULL constraint failures roll the whole insert statement back.
    let err = storage
        .rel_insert("keep", vec![Datum::Null, Datum::VarChar("x".to_string())])
        .expect_err("null pk must fail");
    assert!(matches!(err, StorageError::Constraint(_)), "got: {err}");
    assert_eq!(scan_ids(&mut storage, "keep"), vec![1]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn checkpoint_persists_relational_pages_and_truncates_wal() {
    let path = unique_temp_path("rel-checkpoint");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    for i in 0..50 {
        storage
            .rel_insert("t", row(i, &"c".repeat(200)))
            .expect("insert");
    }
    storage
        .write_checkpoint(b"combined", 1, 2, 1)
        .expect("checkpoint");
    // Post-checkpoint work lands in a fresh WAL epoch.
    for i in 50..60 {
        storage
            .rel_insert("t", row(i, &"c".repeat(200)))
            .expect("insert");
    }
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(scan_ids(&mut storage, "t"), (0..60).collect::<Vec<_>>());
    // The catalog root survived via the superblock (not just the WAL).
    assert!(storage.rel_table("t").is_some());
    drop(storage);
    let _ = std::fs::remove_file(path);
}

// TEMPORARY VERIFIER REPRO — remove after review verification.

/// Review finding: a statement failing mid-way (after some ops applied)
/// must roll back every applied op via CLRs — live, and across a crash.
#[test]
fn mid_statement_failure_rolls_back_applied_ops() {
    let path = unique_temp_path("fault-rollback");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    for i in 0..5 {
        storage.rel_insert("t", row(i, "original")).expect("insert");
    }
    // Let 3 update ops apply, then fail the 4th (simulated WAL failure).
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(Some(3)));
    let err = storage
        .rel_update_where(
            "t",
            "payload",
            &Datum::VarChar("original".to_string()),
            &[("payload".to_string(), Datum::VarChar("changed".to_string()))],
        )
        .expect_err("injected fault must fail the statement");
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(None));
    assert!(matches!(err, StorageError::InvalidConfig(_)), "got: {err}");

    let verify = |storage: &mut Storage| {
        let rows = storage.rel_scan("t").expect("scan");
        assert_eq!(rows.len(), 5);
        for row in &rows {
            assert_eq!(
                row[1],
                Datum::VarChar("original".to_string()),
                "no partially-applied update may survive"
            );
        }
    };
    verify(&mut storage);
    drop(storage); // crash: the rollback CLRs must replay
    let mut storage = Storage::open(path.clone()).expect("reopen");
    verify(&mut storage);
    // Store is not wedged (rollback succeeded) and stays writable.
    storage
        .rel_insert("t", row(100, "after"))
        .expect("insert after rollback");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Exit criterion: a crash DURING recovery undo re-runs cleanly. Two losers
/// are undone one after the other; the injected fault kills recovery after
/// the first loser completed (its CLRs and TXN_END durable) and before the
/// second — the rerun must finish the job exactly once.
#[test]
fn crash_during_recovery_undo_reruns_cleanly() {
    let path = unique_temp_path("crash-mid-undo");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    storage
        .rel_insert("t", row(1, "committed"))
        .expect("insert");
    storage
        .rel_insert_without_commit("t", row(2, "loser-a"))
        .expect("loser a");
    storage
        .rel_insert_without_commit("t", row(3, "loser-b"))
        .expect("loser b");
    drop(storage); // crash with two losers

    // Recovery undoes the higher-LSN loser first; fail on the second's op.
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(Some(1)));
    let result = Storage::open(path.clone());
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(None));
    assert!(
        result.is_err(),
        "injected fault must abort recovery mid-undo"
    );
    drop(result);

    // Re-run: the first loser's durable CLRs replay (no double-undo), the
    // second loser is undone now.
    let mut storage = Storage::open(path.clone()).expect("recovery rerun");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1], "only committed data");
    storage
        .rel_insert("t", row(2, "fresh"))
        .expect("insert after rerun");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1, 2]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Review finding: a growing update of a tiny row on a page too full to
/// hold a forwarding stub must fail cleanly (constraint error), not with an
/// internal logging error or partial application.
#[test]
fn heap_update_on_stub_starved_page_fails_cleanly() {
    let path = unique_temp_path("stub-starved");
    let mut storage = create_storage(&path);
    storage
        .rel_create_table(
            "h",
            vec![
                int_column("id", false),
                Column {
                    name: "v".to_string(),
                    column_type: ColumnType::VarBinary { max_len: 200 },
                    nullable: true,
                    collation: None,
                },
            ],
            &[],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create heap");
    // Fill page 1 to exactly 2 free bytes: 336 null-v rows (12 bytes each
    // with slot) + one row with a 2-byte value (14 bytes with slot).
    for i in 0..336 {
        storage
            .rel_insert("h", vec![Datum::Int(i), Datum::Null])
            .expect("filler");
    }
    storage
        .rel_insert("h", vec![Datum::Int(999), Datum::VarBinary(vec![7u8; 2])])
        .expect("pad row");

    // Row id=0's cell is 8 bytes; a stub needs 11 and the page has 1 free.
    let err = storage
        .rel_update_where(
            "h",
            "id",
            &Datum::Int(0),
            &[("v".to_string(), Datum::VarBinary(vec![9u8; 100]))],
        )
        .expect_err("stub-starved page must reject the growing update");
    assert!(matches!(err, StorageError::Constraint(_)), "got: {err}");

    // Nothing changed and the store keeps working.
    let rows = storage.rel_scan("h").expect("scan");
    assert_eq!(rows.len(), 337);
    let row0 = rows.iter().find(|r| r[0] == Datum::Int(0)).expect("row 0");
    assert_eq!(row0[1], Datum::Null);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

// ---- batched scans (ScanCursor) -------------------------------------------

/// Walks a table in slices of `budget` rows, as a streaming reader would.
fn scan_batched_ids(storage: &mut Storage, table: &str, budget: usize) -> Vec<i32> {
    use crate::relstore::btree::{BTree, ScanCursor};
    use crate::relstore::heap::Heap;
    let (def, schema) = storage.rel_def_for_test(table).expect("def");
    let mut raw: Vec<Vec<u8>> = Vec::new();
    let mut cursor = ScanCursor::start();
    let mut slices = 0;
    while !cursor.done() {
        // One lock acquisition per slice, released between: what lets a large
        // scan stop holding the storage mutex for its whole duration.
        let (next, got) = storage.with_rel_ctx_for_test(|ctx| {
            let mut got = Vec::new();
            let next = if def.is_tree() {
                let tree = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                let mut keyed = Vec::new();
                let next = tree
                    .scan_from(ctx, cursor, budget, &mut keyed)
                    .expect("scan_from");
                got.extend(keyed.into_iter().map(|(_, row)| row));
                next
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                let mut located = Vec::new();
                let next = heap
                    .scan_from(ctx, cursor, budget, &mut located)
                    .expect("scan_from");
                got.extend(located.into_iter().map(|(_, row)| row));
                next
            };
            (next, got)
        });
        assert!(got.len() <= budget, "a slice must respect its budget");
        raw.extend(got);
        cursor = next;
        slices += 1;
        assert!(slices < 100_000, "the cursor must always advance");
    }
    raw.iter()
        .map(
            |r| match crate::relstore::row::decode_row(&schema, r).expect("decode")[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected int id, got {other:?}"),
            },
        )
        .collect()
}

#[test]
fn batched_scan_matches_a_whole_scan_at_every_budget() {
    // A slice boundary must fall anywhere without losing or repeating a row —
    // including mid-page and exactly on a page boundary.
    let path = unique_temp_path("scan-batched");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    create_heap_table(&mut storage, "h");
    // Enough rows, each large, to span many pages.
    for i in 0..200 {
        storage
            .rel_insert("t", row(i, &"x".repeat(200)))
            .expect("insert t");
        storage
            .rel_insert("h", row(i, &"x".repeat(200)))
            .expect("insert h");
    }
    for table in ["t", "h"] {
        let whole = scan_ids(&mut storage, table);
        assert_eq!(whole.len(), 200, "{table}: precondition");
        for budget in [1, 2, 7, 199, 200, 201, 1000] {
            assert_eq!(
                scan_batched_ids(&mut storage, table, budget),
                whole,
                "{table}: budget {budget} must agree with a whole scan"
            );
        }
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn batched_scan_of_an_empty_table_terminates() {
    let path = unique_temp_path("scan-empty");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    create_heap_table(&mut storage, "h");
    for table in ["t", "h"] {
        assert_eq!(scan_batched_ids(&mut storage, table, 4), Vec::<i32>::new());
    }
    let _ = std::fs::remove_file(path);
}
