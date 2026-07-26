use super::*;

#[test]
fn databases_create_list_resolve_and_survive_reopen() {
    let path = unique_temp_path("multidb-create");
    let storage = create_storage(&path);

    assert_eq!(
        storage.rel_databases(),
        vec![(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "truthdb".to_string()
        )],
        "a fresh instance has exactly the synthesized default database"
    );
    assert_eq!(storage.rel_create_database("hr").expect("create hr"), 2);
    assert_eq!(
        storage.rel_create_database("sales").expect("create sales"),
        3
    );
    assert_eq!(
        storage.rel_database_id_by_name("HR"),
        Some(2),
        "case-insensitive"
    );
    assert_eq!(storage.rel_database_id_by_name("TruthDB"), Some(1));
    assert_eq!(storage.rel_database_id_by_name("nope"), None);

    drop(storage);
    let storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        storage.rel_databases(),
        vec![
            (1, "truthdb".to_string()),
            (2, "hr".to_string()),
            (3, "sales".to_string()),
        ],
        "database rows reload from the catalog"
    );
    // The id allocator continues past the surviving maximum.
    assert_eq!(storage.rel_create_database("audit").expect("create"), 4);
    let _ = std::fs::remove_file(path);
}

#[test]
fn duplicate_database_names_are_refused_case_insensitively() {
    let path = unique_temp_path("multidb-dup");
    let storage = create_storage(&path);
    storage.rel_create_database("hr").expect("create");
    assert!(matches!(
        storage.rel_create_database("HR"),
        Err(StorageError::Constraint(_))
    ));
    assert!(
        matches!(
            storage.rel_create_database("TRUTHDB"),
            Err(StorageError::Constraint(_))
        ),
        "the synthesized default database's name is reserved"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn same_table_name_resolves_per_database_and_drop_database_is_scoped() {
    let path = unique_temp_path("multidb-scope");
    let mut storage = create_storage(&path);
    let hr = storage.rel_create_database("hr").expect("create hr");

    create_tree_table(&mut storage, "t");
    storage
        .rel_create_table(
            hr,
            "t",
            vec![int_column("id", false), varchar_column("payload", 4000)],
            &["id".to_string()],
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
        )
        .expect("same name in another database");

    storage
        .rel_insert(
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            "t",
            row(1, "default"),
        )
        .expect("insert default");
    storage
        .rel_insert(hr, "t", row(7, "hr"))
        .expect("insert hr");
    storage
        .rel_insert(hr, "t", row(8, "hr2"))
        .expect("insert hr");

    assert_eq!(
        scan_ids(&mut storage, "t"),
        vec![1],
        "default db sees its own rows"
    );
    let hr_rows = storage.rel_scan(hr, "t").expect("scan hr");
    assert_eq!(hr_rows.len(), 2, "hr sees its own rows");
    let (d1, d2) = (
        storage
            .rel_table(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t")
            .expect("default t"),
        storage.rel_table(hr, "t").expect("hr t"),
    );
    assert_ne!(d1.object_id, d2.object_id, "distinct objects");

    assert!(storage.rel_drop_database("hr").expect("drop hr"));
    assert!(
        storage.rel_table(hr, "t").is_none(),
        "hr's objects are gone"
    );
    assert_eq!(
        scan_ids(&mut storage, "t"),
        vec![1],
        "the default database's same-named table is untouched"
    );
    assert_eq!(storage.rel_database_id_by_name("hr"), None);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_default_database_cannot_be_dropped_and_missing_drop_is_false() {
    let path = unique_temp_path("multidb-drop-default");
    let storage = create_storage(&path);
    assert!(matches!(
        storage.rel_drop_database("truthdb"),
        Err(StorageError::Constraint(_))
    ));
    assert!(!storage.rel_drop_database("ghost").expect("missing drop"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn drop_database_redo_survives_reopen() {
    // DROP DATABASE is the first statement deleting many catalog rows in one
    // transaction; a reopen must recover to the dropped state (redo of the
    // committed multi-delete), and recreating the name allocates a fresh id.
    let path = unique_temp_path("multidb-drop-reopen");
    let mut storage = create_storage(&path);
    let hr = storage.rel_create_database("hr").expect("create hr");
    create_tree_table(&mut storage, "keep");
    for name in ["a", "b", "c"] {
        storage
            .rel_create_table(
                hr,
                name,
                vec![int_column("id", false)],
                &["id".to_string()],
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
            )
            .expect("hr table");
    }
    assert!(storage.rel_drop_database("hr").expect("drop"));
    drop(storage);

    let storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        storage.rel_database_id_by_name("hr"),
        None,
        "drop is durable"
    );
    for name in ["a", "b", "c"] {
        assert!(
            storage.rel_table(hr, name).is_none(),
            "objects gone after redo"
        );
    }
    assert!(
        storage
            .rel_table(crate::relstore::catalog::DEFAULT_DATABASE_ID, "keep")
            .is_some(),
        "other databases untouched"
    );
    let recreated = storage.rel_create_database("hr").expect("recreate");
    assert_eq!(
        recreated, 3,
        "the dropped database's id is tombstoned across reopen — never reused"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn configured_default_database_name_must_not_shadow_a_stored_database() {
    let path = unique_temp_path("multidb-default-collision");
    let storage = create_storage(&path);
    storage.rel_create_database("prod").expect("create prod");
    assert!(matches!(
        storage.set_default_database_name("prod"),
        Err(StorageError::InvalidConfig(_))
    ));
    storage
        .set_default_database_name("main")
        .expect("a fresh name is fine");
    assert_eq!(storage.rel_database_id_by_name("main"), Some(1));
    assert_eq!(storage.rel_database_id_by_name("truthdb"), None, "renamed");
    let _ = std::fs::remove_file(path);
}
