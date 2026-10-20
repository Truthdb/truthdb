//! The vector engine beside the relational and object engines in one
//! database: each keeps its data through a reopen, a failed vector
//! transaction rolls back through the shared log's routed undo, and the
//! maintenance sweep purges deleted indexes.

use crate::engine::test_support::*;
use crate::engine::*;
use serde_json::json;
use truthdb_core::storage::StorageError;
use truthdb_vector::{DistanceMetric, NewIndex, Query, VectorInput};

fn vectors(n: usize) -> Vec<VectorInput> {
    (0..n)
        .map(|i| VectorInput {
            key: format!("v{i}"),
            data: vec![i as f32, 1.0],
            metadata: Some(json!({"i": i})),
        })
        .collect()
}

fn search(engine: &Engine, index: &truthdb_vector::IndexInfo, top_k: usize) -> Vec<String> {
    let mut query = Query::new(index.clone(), vec![0.0, 1.0], top_k, None).unwrap();
    loop {
        let done = engine
            .vector(|file| Ok(query.step(file, 16).unwrap()))
            .unwrap();
        if done {
            break;
        }
    }
    engine
        .vector(|file| query.finish(file, false))
        .unwrap()
        .into_iter()
        .map(|hit| hit.key)
        .collect()
}

#[test]
fn vectors_live_beside_objects_and_tables_and_survive_a_reopen() {
    let path = unique_temp_path("vectors-engine");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();
    engine
        .object(|file| file.transaction(|tx| tx.create_bucket("objects", 1)))
        .unwrap()
        .unwrap();
    let index = engine
        .vector(|file| {
            file.transaction(|tx| {
                tx.create_bucket("vb1", 1)?.unwrap();
                tx.create_index(
                    "vb1",
                    &NewIndex {
                        name: "idx".into(),
                        dimension: 2,
                        metric: DistanceMetric::Euclidean,
                        non_filterable: Vec::new(),
                    },
                    1,
                )
            })
        })
        .unwrap()
        .unwrap();
    let batch = vectors(40);
    let mut from = 0;
    while from < batch.len() {
        from = engine
            .vector(|file| file.transaction(|tx| tx.put_vectors(&index, &batch, from)))
            .unwrap()
            .unwrap();
    }
    // A transaction that fails after writing rolls its writes back: the
    // undo of the vector trees goes to the vector engine.
    let failed: Result<(), EngineError> = engine.vector(|file| {
        file.transaction(|tx| {
            tx.put_vectors(&index, &vectors(45)[40..], 0)?.unwrap();
            Err(StorageError::InvalidConfig("abort".into()))
        })
    });
    assert!(failed.is_err());
    assert_eq!(search(&engine, &index, 3), ["v0", "v1", "v2"]);
    drop(engine);

    let engine =
        Engine::new(RelStorage::open_with(path.clone(), crate::database_engines).expect("open"))
            .expect("engine");
    assert_eq!(
        sql_rows(&engine, "SELECT id FROM t").1,
        [[Some("1".to_string())]]
    );
    assert!(
        engine
            .object(|file| file.bucket("objects"))
            .unwrap()
            .is_some()
    );
    let reopened = engine
        .vector(|file| file.index("vb1", "idx"))
        .unwrap()
        .unwrap();
    assert_eq!(reopened, index);
    assert_eq!(
        engine
            .vector(|file| file.list_vectors(&index, None, 100, false, false))
            .unwrap()
            .len(),
        40
    );
    assert_eq!(search(&engine, &index, 2), ["v0", "v1"]);

    // Deleted: the maintenance sweep purges it.
    engine
        .vector(|file| file.transaction(|tx| tx.delete_index("vb1", "idx")))
        .unwrap()
        .unwrap();
    while engine.vector(|file| file.purge_pending()).unwrap() {
        engine.vector_purge().unwrap();
    }
    assert!(
        engine
            .vector(|file| file.list_vectors(&index, None, 100, false, false))
            .unwrap()
            .is_empty()
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}
