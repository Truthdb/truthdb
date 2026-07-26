use super::*;

fn storage_options_with_collation(name: &str) -> StorageOptions {
    StorageOptions {
        default_collation: Some(name.to_string()),
        ..storage_options()
    }
}

#[test]
fn the_database_default_collation_survives_a_reopen() {
    // It decides the sort-key bytes of every column that inherits it, so it
    // belongs to the file, not to the config of whoever opens it next.
    let path = unique_temp_path("default-collation");
    {
        let storage = Storage::create_with_wal_bounds(
            path.clone(),
            storage_options_with_collation("Finnish_Swedish_CI_AS"),
            REL_TEST_WAL_BYTES,
            REL_TEST_WAL_BYTES,
        )
        .expect("create");
        assert_eq!(
            storage.default_collation().as_deref(),
            Some("Finnish_Swedish_CI_AS")
        );
    }
    let storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        storage.default_collation().as_deref(),
        Some("Finnish_Swedish_CI_AS"),
        "the default must be read back from the file"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_column_keeps_the_default_it_was_created_under() {
    // The column stores the collation by name at CREATE TABLE. A database
    // created later with a different default must not re-key this one.
    let path = unique_temp_path("default-collation-column");
    let mut storage = Storage::create_with_wal_bounds(
        path.clone(),
        storage_options_with_collation("Finnish_Swedish_CI_AS"),
        REL_TEST_WAL_BYTES,
        REL_TEST_WAL_BYTES,
    )
    .expect("create");
    storage
        .rel_create_table(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            vec![
                Column {
                    nullable: false,
                    ..varchar_column("w", 20)
                },
                int_column("n", true),
            ],
            &["w".to_string()],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create table");
    let (_, schema) = storage.rel_def_for_test("t").expect("def");
    assert_eq!(
        schema.columns[0].collation.as_deref(),
        Some("Finnish_Swedish_CI_AS"),
        "a character column inherits the database default, by name"
    );
    assert_eq!(
        schema.columns[1].collation, None,
        "a non-character column has no collation to inherit"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn no_configured_default_leaves_columns_on_the_builtin() {
    let path = unique_temp_path("default-collation-none");
    let mut storage = create_storage(&path);
    assert_eq!(storage.default_collation(), None);
    storage
        .rel_create_table(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            vec![Column {
                nullable: false,
                ..varchar_column("w", 20)
            }],
            &["w".to_string()],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("create table");
    let (_, schema) = storage.rel_def_for_test("t").expect("def");
    assert_eq!(
        schema.columns[0].collation, None,
        "built-in default applies"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_collation_name_too_long_for_the_header_is_refused() {
    // Better to refuse than to stamp a truncated name and key columns under a
    // collation nobody asked for.
    let path = unique_temp_path("default-collation-long");
    let result = Storage::create_with_wal_bounds(
        path.clone(),
        storage_options_with_collation(&"x".repeat(9000)),
        REL_TEST_WAL_BYTES,
        REL_TEST_WAL_BYTES,
    );
    match result {
        Ok(_) => panic!("a name that cannot fit the header must be refused"),
        Err(err) => assert!(
            format!("{err:?}").contains("too long"),
            "unexpected error: {err:?}"
        ),
    }
    let _ = std::fs::remove_file(path);
}
