use super::super::test_support::{test_storage_options, unique_temp_path};
use super::super::*;
use super::*;

#[test]
fn parses_multiline_create_index_command() {
    let cmd = parse_command(
        r#"
            create index products {
              "mappings": {
                "properties": {
                  "name": { "type": "text" },
                  "category": { "type": "keyword" }
                }
              }
            }
            "#,
    )
    .expect("command should parse");

    match cmd {
        Some(Command::CreateIndex { name, mappings }) => {
            assert_eq!(name, "products");
            assert_eq!(mappings["name"], FieldType::Text);
            assert_eq!(mappings["category"], FieldType::Keyword);
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn create_insert_search_and_replay() {
    let path = unique_temp_path("basic-search");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("storage create");
    let engine = Engine::new(storage).expect("engine create");

    engine
        .execute(
            r#"
                create index products {
                  "mappings": {
                    "properties": {
                      "name": { "type": "text" },
                      "category": { "type": "keyword" },
                      "price": { "type": "float" },
                      "description": { "type": "text" }
                    }
                  }
                }
                "#,
        )
        .expect("create index");

    engine
        .execute(
            r#"
                insert document products {
                  "name": "Red Running Shoes",
                  "category": "shoes",
                  "price": 79.99,
                  "description": "Lightweight shoes for road running"
                }
                "#,
        )
        .expect("insert first doc");

    engine
        .execute(
            r#"
                insert document products {
                  "name": "Blue Hiking Boots",
                  "category": "boots",
                  "price": 129.99,
                  "description": "Durable boots for mountain trails"
                }
                "#,
        )
        .expect("insert second doc");

    let response = engine
        .execute(
            r#"
                search products {
                  "query": {
                    "match": {
                      "description": "running shoes"
                    }
                  }
                }
                "#,
        )
        .expect("search");
    let response: Value = serde_json::from_str(&response).expect("valid json search response");
    assert_eq!(response["hits"]["total"].as_u64(), Some(1));
    assert_eq!(
        response["hits"]["hits"][0]["_source"]["name"].as_str(),
        Some("Red Running Shoes")
    );

    drop(engine);

    let storage = Storage::open(path.clone()).expect("storage reopen");
    let engine = Engine::new(storage).expect("engine replay");
    let response = engine
        .execute(
            r#"
                search products {
                  "query": {
                    "bool": {
                      "must": [
                        { "match": { "description": "running" } }
                      ],
                      "filter": [
                        { "term": { "category": "shoes" } }
                      ]
                    }
                  }
                }
                "#,
        )
        .expect("replayed search");
    let response: Value = serde_json::from_str(&response).expect("valid replayed search json");
    assert_eq!(response["hits"]["total"].as_u64(), Some(1));
    assert_eq!(
        response["hits"]["hits"][0]["_source"]["name"].as_str(),
        Some("Red Running Shoes")
    );

    let _ = std::fs::remove_file(path);
}

/// Regression (review finding, pre-existing): snapshots holding
/// documents could never be decoded again (bincode cannot deserialize
/// serde_json::Value). Round-trip a checkpoint with real documents plus
/// a post-checkpoint WAL event.
#[test]
fn checkpoint_with_documents_survives_restart() {
    let path = unique_temp_path("checkpoint-roundtrip");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("storage create");
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
        .execute(r#"insert document notes { "body": "first snapshot doc" }"#)
        .expect("insert 1");
    engine
        .execute(r#"insert document notes { "body": "second snapshot doc" }"#)
        .expect("insert 2");
    engine.checkpoint().expect("checkpoint with documents");
    engine
        .execute(r#"insert document notes { "body": "post checkpoint doc" }"#)
        .expect("insert 3");
    drop(engine);

    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine restart after checkpoint");
    let response = engine
        .execute(r#"search notes { "query": { "match": { "body": "doc" } } }"#)
        .expect("search");
    let response: Value = serde_json::from_str(&response).expect("valid json");
    assert_eq!(
        response["hits"]["total"].as_u64(),
        Some(3),
        "snapshot docs and post-checkpoint doc must all survive"
    );
    // Doc-id continuity: the next insert must not collide.
    engine
        .execute(r#"insert document notes { "body": "post restart doc" }"#)
        .expect("insert after restart");
    let _ = std::fs::remove_file(path);
}

/// Regression (review finding): a crash between the snapshot descriptor
/// becoming durable and the WAL head advancing leaves snapshot-covered
/// events in the ring; replay must skip them instead of failing on
/// duplicate applies.
#[test]
fn replay_skips_events_already_covered_by_snapshot() {
    let path = unique_temp_path("covered-replay");
    let mut storage =
        Storage::create(path.clone(), test_storage_options()).expect("storage create");

    // Snapshot state: index "notes" with one document, next_seq_no = 3.
    let mut mappings = BTreeMap::new();
    mappings.insert("body".to_string(), FieldType::Text);
    let create_event = WalEvent::CreateIndex {
        name: "notes".to_string(),
        mappings: mappings.clone(),
    };
    let mut doc = Document::new();
    doc.insert("body".to_string(), Value::String("covered".to_string()));
    let insert_event = WalEvent::InsertDocument {
        index: "notes".to_string(),
        id: "1".to_string(),
        document: doc,
    };
    let mut state = EngineState::default();
    let mut index = IndexState::new(mappings);
    if let WalEvent::InsertDocument { id, document, .. } = &insert_event {
        index.insert_document(id, document).expect("apply insert");
    }
    state.indices.insert("notes".to_string(), index);
    let snapshot = serde_json::to_vec(&state).expect("encode state");
    storage
        .write_checkpoint(&snapshot, 2, 3, 2)
        .expect("checkpoint");

    // The crash window: events 1 and 2 (already in the snapshot) sit in
    // the ring after the checkpoint.
    for (seq, event) in [(1u64, &create_event), (2u64, &insert_event)] {
        let payload = serde_json::to_vec(event).expect("encode event");
        storage
            .append_wal_entry(
                ENGINE_WAL_ENTRY_TYPE,
                ENGINE_WAL_ENTRY_VERSION,
                seq,
                &payload,
            )
            .expect("append covered event");
    }
    drop(storage);

    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("open must skip covered events");
    let response = engine
        .execute(r#"search notes { "query": { "match": { "body": "covered" } } }"#)
        .expect("search");
    let response: Value = serde_json::from_str(&response).expect("valid json");
    assert_eq!(response["hits"]["total"].as_u64(), Some(1));
    let _ = std::fs::remove_file(path);
}
