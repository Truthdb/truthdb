use super::*;

#[test]
fn heap_updates_move_rows_with_forwarding_stubs() {
    let path = unique_temp_path("heap-stubs");
    let mut storage = create_storage(&path);
    create_heap_table(&mut storage, "h");
    // Fill the first page almost completely.
    for i in 0..3 {
        storage
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "h",
                row(i, &"a".repeat(1200)),
            )
            .expect("insert");
    }
    // Growing row 0 beyond the page's free space forces a move + stub.
    let count = storage
        .rel_update_where(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
    let rows = storage
        .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "h")
        .expect("scan");
    let moved = rows
        .iter()
        .find(|r| r[0] == Datum::Int(0))
        .expect("moved row survives");
    assert_eq!(moved[1], Datum::VarChar("B".repeat(3000)));
    // Deleting through the stub removes the row entirely.
    assert_eq!(
        storage
            .rel_delete_where(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "h",
                "id",
                &Datum::Int(0)
            )
            .expect("delete"),
        1
    );
    let mut ids = scan_ids(&mut storage, "h");
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2]);
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
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
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
            .rel_insert(
                crate::relstore::catalog::DEFAULT_DATABASE_ID,
                "h",
                vec![Datum::Int(i), Datum::Null],
            )
            .expect("filler");
    }
    storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "h",
            vec![Datum::Int(999), Datum::VarBinary(vec![7u8; 2])],
        )
        .expect("pad row");

    // Row id=0's cell is 8 bytes; a stub needs 11 and the page has 1 free.
    let err = storage
        .rel_update_where(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "h",
            "id",
            &Datum::Int(0),
            &[("v".to_string(), Datum::VarBinary(vec![9u8; 100]))],
        )
        .expect_err("stub-starved page must reject the growing update");
    assert!(matches!(err, StorageError::Constraint(_)), "got: {err}");

    // Nothing changed and the store keeps working.
    let rows = storage
        .rel_scan(crate::relstore::catalog::DEFAULT_DATABASE_ID, "h")
        .expect("scan");
    assert_eq!(rows.len(), 337);
    let row0 = rows.iter().find(|r| r[0] == Datum::Int(0)).expect("row 0");
    assert_eq!(row0[1], Datum::Null);
    drop(storage);
    let _ = std::fs::remove_file(path);
}
