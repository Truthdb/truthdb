use super::*;

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
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &"x".repeat(200)),
            )
            .expect("insert t");
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "h",
                row(i, &"x".repeat(200)),
            )
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

// ---- the database's default collation ------------------------------------

#[test]
fn a_sliced_scan_reads_the_same_rows_as_a_whole_one() {
    // The SELECT paths read through rel_scan_slice (ScanStream pulls it lazily
    // since the streaming-scans leg; rel_scan_sliced composes the same slices
    // eagerly); either way the slices must agree with the atomic rel_scan the
    // integrity checks still use, at any slice size.
    let path = unique_temp_path("scan-sliced");
    let mut storage = create_storage(&path);
    create_tree_table(&mut storage, "t");
    create_heap_table(&mut storage, "h");
    for i in 0..300 {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &"x".repeat(150)),
            )
            .expect("insert t");
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "h",
                row(i, &"x".repeat(150)),
            )
            .expect("insert h");
    }
    for table in ["t", "h"] {
        let whole = scan_ids(&mut storage, table);
        assert_eq!(whole.len(), 300, "{table}: precondition");
        for budget in [1, 3, 64, 299, 300, 301, 4096] {
            let sliced: Vec<i32> = storage
                .rel_scan_sliced(crate::relstore::catalog::DEFAULT_DATABASE_ID, table, budget)
                .expect("sliced scan")
                .iter()
                .map(|r| match r[0] {
                    Datum::Int(v) => v,
                    ref other => panic!("expected int id, got {other:?}"),
                })
                .collect();
            assert_eq!(sliced, whole, "{table}: slice budget {budget}");
        }
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_sliced_scan_takes_the_storage_lock_once_per_slice_not_once_per_table() {
    // The point of slicing (#96): one large read must stop blocking every other
    // session for its whole duration.
    //
    // This used to assert that a probe thread spinning on `rel_get` won the
    // storage lock at least once while the scan ran. That is not a property the
    // code has: `std::sync::Mutex` is not fair, so the scanning thread may
    // release and immediately re-acquire, and the probe is never guaranteed a
    // turn — under CPU starvation it may not even be scheduled. It failed ~35%
    // of the time on a loaded box and intermittently in CI, which is a test
    // asserting the scheduler's behaviour rather than this module's.
    //
    // The mechanism *is* deterministic, and it implies the property for any
    // mutex that is not pathologically unfair: the scan acquires the lock once
    // per slice. A non-reentrant mutex acquired N times must have been released
    // N-1 times, so counting the acquisitions proves the releases. A scan that
    // hoisted the guard out of the loop — the exact regression this guards, and
    // an easy one to write — takes the count to zero and fails here.
    let path = unique_temp_path("scan-sliced-lock");
    let mut storage = Storage::create_with_wal_bounds(
        path.clone(),
        storage_options(),
        64 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .expect("create");
    create_tree_table(&mut storage, "t");
    for i in 0..1500 {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "t",
                row(i, &"x".repeat(400)),
            )
            .expect("insert");
    }

    let before = storage.scan_slices();
    let rows = storage
        .rel_scan_sliced(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t", 8)
        .expect("sliced scan");
    let slices = storage.scan_slices() - before;

    assert_eq!(rows.len(), 1500, "the scan still reads everything");
    assert!(
        slices >= 1500 / 8,
        "1500 rows at 8 per slice is at least 187 separate lock holds, not one; got {slices}"
    );

    // The counter means what the assertion above assumes: a whole-table scan
    // takes the lock once. Without this, `slices` could be counting something
    // else entirely and the test would still pass.
    let before = storage.scan_slices();
    storage
        .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
        .expect("whole scan");
    assert_eq!(
        storage.scan_slices() - before,
        0,
        "rel_scan is the single-hold path and takes no slices"
    );
    let _ = std::fs::remove_file(path);
}

// ---- Multiple databases (A1: the catalog layer) ----
