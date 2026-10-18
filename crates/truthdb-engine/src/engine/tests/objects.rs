//! The object engine through the combined engine: operations run under the
//! storage lock (directly or on a worker), object writes trigger checkpoints,
//! the maintenance collection frees unreferenced bodies, and every database
//! file runs the object engine.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{ObjFile, SEGMENT_BYTES};

fn bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 253) as u8))
        .collect()
}

/// Writes `data` as one sealed body, each segment its own engine call.
fn put(engine: &Engine, data: &[u8]) -> u64 {
    let body = engine.object(|file| file.begin_body(1)).expect("begin");
    for chunk in data.chunks(SEGMENT_BYTES) {
        engine
            .object(|file| file.append_segment(body, chunk, 1))
            .expect("segment");
    }
    engine
        .object(|file| file.transaction(|tx| tx.seal_body(body, [0; 16])))
        .expect("seal");
    body
}

fn read(engine: &Engine, body: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    engine
        .object(|file| file.read_body(body, 0, &mut out))
        .expect("read");
    out
}

#[test]
fn a_body_written_through_the_engine_survives_a_reopen() {
    let path = unique_temp_path("objects-reopen");
    let engine = new_engine(&path);
    let data = bytes(1, SEGMENT_BYTES + 99);
    let body = put(&engine, &data);
    assert_eq!(read(&engine, body, data.len()), data);
    drop(engine);

    let err = RelStorage::open(path.clone())
        .err()
        .expect("a relational-only open refuses a file with object data");
    assert!(err.to_string().contains("holds engine 1"), "{err}");
    let engine =
        Engine::new(RelStorage::open_with(path.clone(), crate::database_engines).expect("open"))
            .expect("engine");
    assert_eq!(read(&engine, body, data.len()), data);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_maintenance_collection_frees_an_unreferenced_body() {
    let path = unique_temp_path("objects-gc");
    let engine = new_engine(&path);
    let body = put(&engine, &bytes(2, 1000));
    engine
        .object(|file| file.transaction(|tx| tx.drop_ref(body, 0, 0)))
        .expect("drop the last reference");
    engine.object_gc().expect("collect");
    let err = engine
        .object(|file| file.body_info(body))
        .expect_err("collected");
    assert!(err.to_string().contains("no body"), "{err}");
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn object_writes_take_checkpoints_as_the_log_fills() {
    let path = unique_temp_path("objects-checkpoint");
    let storage = RelStorage::create_with_wal_bounds_with(
        path.clone(),
        test_storage_options(),
        8 * 1024 * 1024,
        8 * 1024 * 1024,
        crate::database_engines,
    )
    .expect("create");
    let engine = Engine::new(storage).expect("engine");
    // Twelve 1 MiB segments through an 8 MiB ring: without checkpoints the
    // ring would fill and the writes fail.
    let data = bytes(3, 12 * SEGMENT_BYTES);
    let body = put(&engine, &data);
    assert!(
        engine.wal_usage_ratio() < 0.75,
        "a checkpoint reclaimed the ring"
    );
    assert_eq!(read(&engine, body, data.len()), data);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn an_object_operation_runs_on_a_worker() {
    let path = unique_temp_path("objects-worker");
    let engine = new_engine(&path);
    let (handle, join) = crate::session::spawn_engine(engine);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let info = runtime.block_on(async {
        let body = handle
            .run_object(|file: &mut ObjFile<'_>| file.begin_body(1))
            .await
            .expect("begin");
        handle
            .run_object(move |file: &mut ObjFile<'_>| file.append_segment(body, b"hello", 1))
            .await
            .expect("segment");
        handle
            .run_object(move |file: &mut ObjFile<'_>| {
                file.transaction(|tx| tx.seal_body(body, [7; 16]))
            })
            .await
            .expect("seal")
    });
    assert_eq!(info.len, 5);
    handle.shutdown();
    join.join().expect("pool exits");
    let _ = std::fs::remove_file(path);
}
