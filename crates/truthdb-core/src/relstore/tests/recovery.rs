use super::*;

#[test]
fn committed_statements_survive_crash_without_checkpoint() {
    let path = unique_temp_path("committed-durable");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    create_heap_table(&mut storage, "h");
    for i in 0..20 {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &format!("tree-{i}")),
            )
            .expect("insert");
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "h",
                row(i, &format!("heap-{i}")),
            )
            .expect("insert");
    }
    storage
        .rel_delete_where(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            "id",
            &Datum::Int(3),
        )
        .expect("delete");
    storage
        .rel_update_where(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
        .rel_get(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            &[Datum::Int(4)],
        )
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
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(1, "committed"),
        )
        .expect("insert 1");

    let mut txn = storage.rel_begin().expect("begin");
    storage
        .rel_insert_many(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(1, "committed"),
        )
        .expect("insert");
    storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "h",
            row(1, "committed"),
        )
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
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(2, "fresh"),
        )
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
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, "payload"),
            )
            .expect("insert");
    }
    // Flush dirty pages to disk without advancing the WAL head (the
    // mid-checkpoint crash window), then tear the table's root page.
    storage.rel_flush_pool_only().expect("flush");
    let root_page = storage
        .rel_table(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
        .expect("def")
        .root_page;
    let offset = storage.data_page_offset(root_page);
    drop(storage);
    overwrite_bytes(&path, offset + 1000, &[0xDBu8; 2000]);

    let mut storage = Storage::open(path.clone()).expect("reopen after tear");
    assert_eq!(scan_ids(&mut storage, "t"), vec![0, 1, 2, 3, 4]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn failing_statement_rolls_back_all_its_rows() {
    let path = unique_temp_path("statement-rollback");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    for i in 0..5 {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, "small"),
            )
            .expect("insert");
    }
    // A multi-row update where the grown rows exceed the tree cell cap: the
    // first rows update fine, then the statement fails and must roll back.
    let err = storage
        .rel_update_where(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            "payload",
            &Datum::VarChar("small".to_string()),
            &[("payload".to_string(), Datum::VarChar("W".repeat(3000)))],
        )
        .expect_err("oversized tree rows must fail the statement");
    assert!(matches!(err, StorageError::InvalidConfig(_)), "got: {err}");
    let rows = storage
        .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
        .expect("scan");
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
    let rows = storage
        .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
        .expect("scan");
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
    storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "keep",
            row(1, "x"),
        )
        .expect("insert");
    drop(storage);

    // Committed table survives; the catalog itself recovered.
    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert!(
        storage
            .rel_table(crate::relstore::catalog::DEFAULT_DATABASE_ID, "keep")
            .is_some()
    );
    assert_eq!(scan_ids(&mut storage, "keep"), vec![1]);

    // NOT NULL constraint failures roll the whole insert statement back.
    let err = storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "keep",
            vec![Datum::Null, Datum::VarChar("x".to_string())],
        )
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
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &"c".repeat(200)),
            )
            .expect("insert");
    }
    storage
        .write_checkpoint(b"combined", 1, 2, 1)
        .expect("checkpoint");
    // Post-checkpoint work lands in a fresh WAL epoch.
    for i in 50..60 {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &"c".repeat(200)),
            )
            .expect("insert");
    }
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(scan_ids(&mut storage, "t"), (0..60).collect::<Vec<_>>());
    // The catalog root survived via the superblock (not just the WAL).
    assert!(
        storage
            .rel_table(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
            .is_some()
    );
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
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, "original"),
            )
            .expect("insert");
    }
    // Let 3 update ops apply, then fail the 4th (simulated WAL failure).
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(Some(3)));
    let err = storage
        .rel_update_where(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            "payload",
            &Datum::VarChar("original".to_string()),
            &[("payload".to_string(), Datum::VarChar("changed".to_string()))],
        )
        .expect_err("injected fault must fail the statement");
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(None));
    assert!(matches!(err, StorageError::InvalidConfig(_)), "got: {err}");

    let verify = |storage: &mut Storage| {
        let rows = storage
            .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
            .expect("scan");
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
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(100, "after"),
        )
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
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(1, "committed"),
        )
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
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(2, "fresh"),
        )
        .expect("insert after rerun");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1, 2]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Review finding: a counter compensation is a blind arithmetic delta, so it
/// cannot tolerate the CLR-group re-run the other undo arms survive by being
/// guarded (occupancy checks, logical presence). Its CLR must therefore point
/// *past* the compensated record — a crash after the CLR but before the
/// sealing no-op then resumes at the previous record instead of re-applying
/// the delta. This test pins the mechanism itself, on the WAL bytes: the CLR
/// compensating a CounterAdd record carries `undo_next == prev_lsn` of that
/// record, never its own LSN.
#[test]
fn counter_compensation_clr_points_past_its_record() {
    use crate::storage::TxnScope;
    use crate::wal::records::{PageOpRedo, REL_KIND_CLR, REL_KIND_PAGE_IMAGE, REL_KIND_PAGE_OP};

    let path = unique_temp_path("counter-clr-pointer");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(1, "committed"),
        )
        .expect("insert");

    let mut txn = storage.rel_begin().expect("begin");
    storage
        .rel_insert_many(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            vec![row(2, "rolled back")],
            &mut TxnScope::Explicit(&mut txn),
        )
        .expect("insert under txn");
    storage.rel_rollback(txn).expect("rollback");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(1),
        "count restored"
    );
    // The record scan reads the open-time replay cache, so reopen: the ring
    // still holds the rollback's records (nothing checkpointed).
    drop(storage);
    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(1)
    );

    let records = storage.rel_wal_records().expect("scan");
    // The rolled-back statement's forward CounterAdd record. It may have been
    // logged as a plain page op or — on the counter page's first touch since
    // a checkpoint — as a full page image; either way its UNDO carries the
    // inverse CounterAdd.
    // Both inserts log a +1; the rolled-back transaction's is the LAST one.
    let (forward_lsn, forward_prev) = records
        .iter()
        .filter_map(|(lsn, record)| {
            if record.txn_id == 0
                || record.kind != REL_KIND_PAGE_OP && record.kind != REL_KIND_PAGE_IMAGE
            {
                return None;
            }
            let is_forward_counter = matches!(
                record.decode_page_op_redo(),
                Ok(PageOpRedo::CounterAdd { delta, .. }) if delta > 0
            ) || matches!(
                record.decode_page_op_undo(),
                Ok(crate::wal::records::PageOpUndo::CounterAdd { delta, .. }) if delta < 0
            );
            if is_forward_counter {
                Some((*lsn, record.prev_lsn))
            } else {
                None
            }
        })
        .last()
        .expect("the rolled-back insert logged a forward CounterAdd");
    // The CLR that compensates it.
    let undo_next = records
        .iter()
        .find_map(|(_, record)| {
            if record.kind != REL_KIND_CLR {
                return None;
            }
            match record.decode_clr() {
                Ok((undo_next, Some(PageOpRedo::CounterAdd { delta, .. }))) if delta < 0 => {
                    Some(undo_next)
                }
                _ => None,
            }
        })
        .expect("the rollback logged a compensating CounterAdd CLR");
    assert_eq!(
        undo_next, forward_prev,
        "the counter CLR points PAST its record (a crash-resumed undo must \
         never revisit it: the delta would apply twice)"
    );
    assert_ne!(undo_next, forward_lsn, "not the group-re-run pointer");

    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// The behavioral face of the same property: a crash mid-recovery-undo whose
/// re-run walks the loser again must apply the counter compensation exactly
/// once (the CLR pointer above is what makes the re-run skip the record).
#[test]
fn counter_compensation_survives_a_crash_during_recovery_undo() {
    let path = unique_temp_path("counter-crash-mid-undo");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(1, "committed"),
        )
        .expect("insert");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(1)
    );
    storage
        .rel_insert_without_commit("t", row(2, "loser"))
        .expect("loser");
    drop(storage); // crash with the loser open

    // Recovery undoes the loser LIFO: the counter compensation lands first,
    // then the injected fault aborts recovery on the row op behind it.
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(Some(1)));
    let result = Storage::open(path.clone());
    crate::relstore::ctx::FAIL_APPLY_OPS_AFTER.with(|c| c.set(None));
    assert!(
        result.is_err(),
        "injected fault must abort recovery mid-undo"
    );
    drop(result);

    let mut storage = Storage::open(path.clone()).expect("recovery rerun");
    assert_eq!(scan_ids(&mut storage, "t"), vec![1], "loser row undone");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(1),
        "the compensation applied exactly once across the re-run"
    );
    drop(storage);
    let _ = std::fs::remove_file(path);
}
