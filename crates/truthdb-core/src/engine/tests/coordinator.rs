use crate::engine::test_support::*;

use crate::engine::*;

/// Stage 2 exit criterion: search events and relational records share
/// one WAL ring; a crash must recover both, each through its own
/// mechanism, regardless of interleaving.
#[test]
fn mixed_search_and_relational_wal_replays_in_order() {
    let path = unique_temp_path("mixed-wal");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("storage create");
    let engine = Engine::new(storage).expect("engine create");

    engine
        .execute(
            r#"create index docs { "mappings": { "properties": { "body": { "type": "text" } } } }"#,
        )
        .expect("create index");
    engine
        .execute("CREATE TABLE items (id INT NOT NULL PRIMARY KEY, label NVARCHAR(50))")
        .expect("create table");
    // Interleave the two subsystems in one ring.
    for i in 0..10 {
        engine
            .execute(&format!(
                r#"insert document docs {{ "body": "search event {i}" }}"#
            ))
            .expect("insert doc");
        engine
            .execute(&format!("INSERT INTO items VALUES ({i}, 'row {i}')"))
            .expect("insert row");
    }
    drop(engine); // crash: everything lives in the shared WAL only

    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("recover both subsystems");

    let response = engine
        .execute(r#"search docs { "query": { "match": { "body": "search" } } }"#)
        .expect("search");
    let response: Value = serde_json::from_str(&response).expect("json");
    assert_eq!(response["hits"]["total"].as_u64(), Some(10));

    let ids = sql_column_i64(&engine, "SELECT id FROM items ORDER BY id", 0);
    assert_eq!(
        ids,
        (0..10).collect::<Vec<_>>(),
        "all rows recovered in key order"
    );

    // Both surfaces stay writable after recovery.
    engine
        .execute("INSERT INTO items VALUES (10, 'after recovery')")
        .expect("insert after recovery");
    let ids = sql_column_i64(&engine, "SELECT id FROM items WHERE id > 8 ORDER BY id", 0);
    assert_eq!(ids, vec![9, 10]);
    let _ = std::fs::remove_file(path);
}
#[test]
fn engine_replay_ignores_relational_wal_records() {
    let path = unique_temp_path("rel-coexistence");
    let mut storage =
        Storage::create(path.clone(), test_storage_options()).expect("storage create");
    // Relational records land in the same ring before and between search
    // events; search replay must skip them.
    let extent = storage.allocate_extent(false).expect("extent");
    let engine = Engine::new(storage).expect("engine create");
    engine
        .execute(
            r#"
                create index notes {
                  "mappings": { "properties": { "body": { "type": "text" } } }
                }
                "#,
        )
        .expect("create index");
    engine
        .execute(r#"insert document notes { "body": "relational coexistence" }"#)
        .expect("insert");
    drop(engine);

    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine replay with rel records");
    let response = engine
        .execute(r#"search notes { "query": { "match": { "body": "coexistence" } } }"#)
        .expect("search after replay");
    let response: Value = serde_json::from_str(&response).expect("valid json");
    assert_eq!(response["hits"]["total"].as_u64(), Some(1));
    let _ = extent;

    let _ = std::fs::remove_file(path);
}
