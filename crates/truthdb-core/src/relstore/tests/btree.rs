use super::*;

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
                let result = storage.rel_insert(
                    crate::relstore::catalog::DEFAULT_DATABASE_ID,
                    "t",
                    row(id, &payload),
                );
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
                    .rel_delete_where(
                        crate::relstore::catalog::DEFAULT_DATABASE_ID,
                        "t",
                        "id",
                        &Datum::Int(id),
                    )
                    .expect("delete");
                assert_eq!(count == 1, expected, "delete count diverged");
            }
            _ => {
                let payload = format!("{id}-upd-{}", "y".repeat(100 + (rng.next() % 500) as usize));
                let count = storage
                    .rel_update_where(
                        crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
            let got = storage
                .rel_get(
                    crate::relstore::catalog::DEFAULT_DATABASE_ID,
                    "t",
                    &[Datum::Int(id)],
                )
                .expect("get");
            assert_eq!(got.is_some(), oracle.contains_key(&id));
            storage
                .write_checkpoint(b"oracle-checkpoint", 1, 2, 1)
                .expect("checkpoint");
        }
    }

    let verify = |storage: &mut Storage, oracle: &BTreeMap<i32, String>| {
        let rows = storage
            .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
            .expect("scan");
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
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &payload),
            )
            .expect("insert");
    }
    drop(storage); // crash: splits only exist as WAL images

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(scan_ids(&mut storage, "t"), (0..300).collect::<Vec<_>>());
    // The recovered tree keeps working (routing, further splits).
    for i in 300..340 {
        let payload = format!("{i}-{}", "z".repeat(380));
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &payload),
            )
            .expect("insert after recovery");
    }
    assert_eq!(scan_ids(&mut storage, "t"), (0..340).collect::<Vec<_>>());
    drop(storage);
    let _ = std::fs::remove_file(path);
}
